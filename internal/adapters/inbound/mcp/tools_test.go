package mcp

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// harness builds Deps over in-memory repos, seeding through the real
// EnqueueWorkUnit use case, with a FixedClock so timing is deterministic.
type harness struct {
	deps      Deps
	pools     *memory.WorkPoolRepo
	workUnits *memory.WorkUnitRepo
	enqueue   *usecases.EnqueueWorkUnit
	clock     memory.FixedClock
	seq       int
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	pools := memory.NewWorkPoolRepo()
	workUnits := memory.NewWorkUnitRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: base}

	return &harness{
		deps: Deps{
			SampleBacklog:     usecases.NewSampleBacklog(pools, publisher, clock),
			RebalanceDecision: usecases.NewRebalanceDecision(pools, publisher, clock),
			ReleaseNextWork:   usecases.NewReleaseNextWork(pools, workUnits, publisher, clock),
		},
		pools:     pools,
		workUnits: workUnits,
		enqueue:   usecases.NewEnqueueWorkUnit(workUnits, pools, publisher, clock),
		clock:     clock,
	}
}

// pathId builds a valid PathId or fails the test.
func pathId(t *testing.T, s string) shared.PathId {
	t.Helper()
	p, err := shared.NewPathId(s)
	if err != nil {
		t.Fatalf("pathId(%q): %v", s, err)
	}
	return p
}

// seedPending enqueues one pending work unit on path (auto-creating a
// release-fed pool the first time), with the given CPT offset and reference.
// It returns the generated work-unit id.
func (h *harness) seedPending(t *testing.T, path string, cptOffset time.Duration, ref string) string {
	t.Helper()
	h.seq++
	id := "wu-" + ref
	_, err := h.enqueue.Execute(context.Background(), usecases.EnqueueWorkUnitRequest{
		WorkUnitId: id,
		PathId:     pathId(t, path),
		CPT:        shared.NewCPT(base.Add(cptOffset)),
		Reference:  ref,
	})
	if err != nil {
		t.Fatalf("seedPending: %v", err)
	}
	return id
}

// seedFlowFedPool provisions a flow-fed pool directly with backlog above its
// alarm threshold, for telemetry/rebalance paths the release-fed default
// cannot reach.
func (h *harness) seedFlowFedPool(t *testing.T, path string, backlog, alarmThreshold int) {
	t.Helper()
	pool := release.NewWorkPool(pathId(t, path), release.FlowFed, 0, alarmThreshold)
	for i := 0; i < backlog; i++ {
		if err := pool.Enqueue("ff-"+path+"-"+string(rune('a'+i)), shared.NewCPT(base.Add(time.Duration(i)*time.Minute))); err != nil {
			t.Fatalf("seedFlowFedPool enqueue: %v", err)
		}
	}
	if err := h.pools.Save(context.Background(), pool); err != nil {
		t.Fatalf("seedFlowFedPool save: %v", err)
	}
}

// backlogTelemetryCase is one row of TestGetBacklogTelemetry's table.
type backlogTelemetryCase struct {
	name      string
	in        string
	wantDepth int
	wantMode  string
	wantOver  bool
	wantErr   bool
}

// assertBacklogTelemetry checks the happy-path output fields against the
// case's expectations.
func assertBacklogTelemetry(t *testing.T, out backlogTelemetry, tc backlogTelemetryCase) {
	t.Helper()
	if out.BacklogDepth != tc.wantDepth {
		t.Fatalf("backlogDepth = %d, want %d", out.BacklogDepth, tc.wantDepth)
	}
	if out.Mode != tc.wantMode {
		t.Fatalf("mode = %q, want %q", out.Mode, tc.wantMode)
	}
	if out.OverAlarmThreshold != tc.wantOver {
		t.Fatalf("overAlarmThreshold = %v, want %v", out.OverAlarmThreshold, tc.wantOver)
	}
	if out.PathId != tc.in {
		t.Fatalf("pathId = %q, want %q", out.PathId, tc.in)
	}
}

func TestGetBacklogTelemetry(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.seedPending(t, "pick-a", time.Hour, "o1")
	h.seedPending(t, "pick-a", 2*time.Hour, "o2")
	h.seedFlowFedPool(t, "pack-a", 5, 2) // backlog 5 > threshold 2 => over alarm

	tests := []backlogTelemetryCase{
		{"release-fed backlog 2", "pick-a", 2, "ReleaseFed", false, false},
		{"flow-fed over alarm", "pack-a", 5, "FlowFed", true, false},
		{"empty pathId rejected", "", 0, "", false, true},
		{"unknown path errors", "no-such-path", 0, "", false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := h.deps.getBacklogTelemetry(ctx, backlogTelemetryInput{PathId: tc.in})
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertBacklogTelemetry(t, out, tc)
		})
	}
}

func TestGetRebalanceRecommendation(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// Flow-fed path over alarm -> ThrottleUpstream.
	h.seedFlowFedPool(t, "pack-a", 5, 2)
	// Release-fed path with backlog but under WIP -> NoActionNeeded.
	h.seedPending(t, "pick-a", time.Hour, "o1")

	tests := []struct {
		name       string
		in         string
		wantAction string
		wantErr    bool
	}{
		{"flow-fed over alarm throttles", "pack-a", "ThrottleUpstream", false},
		{"release-fed healthy no action", "pick-a", "NoActionNeeded", false},
		{"empty pathId rejected", "", "", true},
		{"unknown path errors", "no-such-path", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := h.deps.getRebalanceRecommendation(ctx, rebalanceInput{PathId: tc.in})
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if out.Action != tc.wantAction {
				t.Fatalf("action = %q, want %q", out.Action, tc.wantAction)
			}
		})
	}
}

func TestReleaseNextWork(t *testing.T) {
	ctx := context.Background()

	t.Run("releases earliest-CPT unit", func(t *testing.T) {
		h := newHarness(t)
		h.seedPending(t, "pick-a", 2*time.Hour, "late")
		h.seedPending(t, "pick-a", time.Hour, "early")

		out, err := h.deps.releaseNextWork(ctx, releaseNextWorkInput{PathId: "pick-a"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if out.Ref != "early" {
			t.Fatalf("released ref = %q, want the earliest-CPT unit 'early'", out.Ref)
		}
		if out.WorkUnitId != "wu-early" {
			t.Fatalf("workUnitId = %q, want wu-early", out.WorkUnitId)
		}
		if out.CPT != base.Add(time.Hour).UTC().Format(time.RFC3339) {
			t.Fatalf("cpt = %q, unexpected", out.CPT)
		}
	})

	t.Run("empty pool is rejected", func(t *testing.T) {
		h := newHarness(t)
		h.seedPending(t, "pick-a", time.Hour, "only")
		if _, err := h.deps.releaseNextWork(ctx, releaseNextWorkInput{PathId: "pick-a"}); err != nil {
			t.Fatalf("first release failed: %v", err)
		}
		// Now the pool has no pending entries left.
		if _, err := h.deps.releaseNextWork(ctx, releaseNextWorkInput{PathId: "pick-a"}); err == nil {
			t.Fatal("releasing from an empty pool must be rejected")
		}
	})

	t.Run("empty pathId rejected", func(t *testing.T) {
		h := newHarness(t)
		if _, err := h.deps.releaseNextWork(ctx, releaseNextWorkInput{PathId: ""}); err == nil {
			t.Fatal("empty pathId must be rejected")
		}
	})

	t.Run("unknown path is rejected", func(t *testing.T) {
		h := newHarness(t)
		if _, err := h.deps.releaseNextWork(ctx, releaseNextWorkInput{PathId: "no-such-path"}); err == nil {
			t.Fatal("releasing on an unknown path must error")
		}
	})
}

func TestParsePathId(t *testing.T) {
	if _, err := parsePathId(""); err == nil {
		t.Fatal("empty pathId must be rejected")
	}
	p, err := parsePathId("pick-a")
	if err != nil {
		t.Fatalf("valid pathId rejected: %v", err)
	}
	if p.String() != "pick-a" {
		t.Fatalf("pathId round-trip = %q, want pick-a", p.String())
	}
}

func TestParseBacklogURI(t *testing.T) {
	tests := []struct {
		uri     string
		want    string
		wantErr bool
	}{
		{"telemetry://pick-a/backlog", "pick-a", false},
		{"telemetry://pack-station-3/backlog", "pack-station-3", false},
		{"telemetry:///backlog", "", true},
		{"telemetry://a/b/backlog", "", true},
		{"queue://pick-a/backlog", "", true},
		{"telemetry://pick-a/status", "", true},
	}
	for _, tc := range tests {
		got, err := parseBacklogURI(tc.uri)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseBacklogURI(%q) expected error", tc.uri)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("parseBacklogURI(%q) = (%q, %v), want (%q, nil)", tc.uri, got, err, tc.want)
		}
	}
}
