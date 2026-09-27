// Shared harness for the MCP eval suites (E1-E3): one place that builds a
// real Streamable HTTP server over in-memory adapters and connects a real
// SDK client to it, so schema evals, wire conformance evals, and the
// Gherkin behavioral evals all exercise exactly the surface a model host
// would.
package mcp_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/mcp"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// evalBase is the fixed instant every eval runs against, so CPTs and
// timestamps in the pinned structured results are deterministic.
var evalBase = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// evalHarness is a fully wired MCP server over in-memory repos plus the
// client session talking to it, with the knobs the evals assert on.
type evalHarness struct {
	session         *sdk.ClientSession
	server          *httptest.Server
	publisher       *events.LogPublisher
	lastCallResult  *sdk.CallToolResult
	lastCallErr     error
	lastCallContent string
}

// newEvalDeps builds the DEFAULT tool surface (no reports client) over
// empty in-memory repos — enough for schema and conformance evals that do
// not seed state. The conditional report tool is intentionally absent here;
// eval_governance_test.go pins that conditionality explicitly.
func newEvalDeps() inboundmcp.Deps {
	pools := memory.NewWorkPoolRepo()
	workUnits := memory.NewWorkUnitRepo()
	publisher := events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: evalBase}
	return inboundmcp.Deps{
		SampleBacklog:     usecases.NewSampleBacklog(pools, publisher, clock),
		RebalanceDecision: usecases.NewRebalanceDecision(pools, publisher, clock),
		ReleaseNextWork:   usecases.NewReleaseNextWork(pools, workUnits, publisher, clock),
	}
}

// newEvalHarness seeds the canonical eval state over a real Streamable HTTP
// server and connects a client session to it. The canonical state gives
// every tool something meaningful to read or admit:
//   - pick-a: release-fed pool (default provisioning) with two pending units
//     o1 (CPT +1h) and o2 (CPT +2h) — the release happy path and the
//     healthy-telemetry path;
//   - pack-a: flow-fed pool with backlog 5 against alarm threshold 2 — the
//     over-alarm telemetry and ThrottleUpstream rebalance paths;
//   - sat-a: release-fed pool with WIP limit 1 already occupied by sat-w1
//     and sat-w2 still pending — the ReassignLabor rebalance path and the
//     WIP-invariant rejection of release_next_work.
//
// Saturation is seeded at the domain aggregate level (like the package's
// own seedFlowFedPool helper) because the REST surface cannot provision a
// pool with a non-default WIP limit.
func newEvalHarness(t *testing.T) *evalHarness {
	t.Helper()
	return newEvalHarnessWithReports(t, nil)
}

// newEvalHarnessWithReports is newEvalHarness with an optional reports
// client: when non-nil the conditional get_release_throughput_report tool
// is registered, so the behavioral evals can drive it too.
func newEvalHarnessWithReports(t *testing.T, reports inboundmcp.ReportsClient) *evalHarness {
	t.Helper()

	h := &evalHarness{}
	pools := memory.NewWorkPoolRepo()
	workUnits := memory.NewWorkUnitRepo()
	h.publisher = events.NewLogPublisher(nil)
	clock := memory.FixedClock{At: evalBase}

	ctx := context.Background()
	enqueue := usecases.NewEnqueueWorkUnit(workUnits, pools, h.publisher, clock)
	pickA, err := shared.NewPathId("pick-a")
	if err != nil {
		t.Fatalf("seed pick-a path id: %v", err)
	}
	for i, ref := range []string{"o1", "o2"} {
		if _, err := enqueue.Execute(ctx, usecases.EnqueueWorkUnitRequest{
			WorkUnitId: "wu-" + ref,
			PathId:     pickA,
			CPT:        shared.NewCPT(evalBase.Add(time.Duration(i+1) * time.Hour)),
			Reference:  ref,
		}); err != nil {
			t.Fatalf("seed pick-a unit %s: %v", ref, err)
		}
	}

	packA, err := shared.NewPathId("pack-a")
	if err != nil {
		t.Fatalf("seed pack-a path id: %v", err)
	}
	flowFed := release.NewWorkPool(packA, release.FlowFed, 0, 2)
	for i := 0; i < 5; i++ {
		id := "ff-pack-a-" + string(rune('a'+i))
		if err := flowFed.Enqueue(id, shared.NewCPT(evalBase.Add(time.Duration(i)*time.Minute))); err != nil {
			t.Fatalf("seed pack-a entry %s: %v", id, err)
		}
	}
	if err := pools.Save(ctx, flowFed); err != nil {
		t.Fatalf("seed pack-a pool: %v", err)
	}

	satA, err := shared.NewPathId("sat-a")
	if err != nil {
		t.Fatalf("seed sat-a path id: %v", err)
	}
	saturated := release.NewWorkPool(satA, release.ReleaseFed, 1, 0)
	for i, id := range []string{"sat-w1", "sat-w2"} {
		if err := saturated.Enqueue(id, shared.NewCPT(evalBase.Add(time.Duration(i+1)*time.Hour))); err != nil {
			t.Fatalf("seed sat-a entry %s: %v", id, err)
		}
	}
	if err := saturated.Release("sat-w1"); err != nil {
		t.Fatalf("seed sat-a occupancy: %v", err)
	}
	if err := pools.Save(ctx, saturated); err != nil {
		t.Fatalf("seed sat-a pool: %v", err)
	}

	server := inboundmcp.NewServer(inboundmcp.Deps{
		SampleBacklog:     usecases.NewSampleBacklog(pools, h.publisher, clock),
		RebalanceDecision: usecases.NewRebalanceDecision(pools, h.publisher, clock),
		ReleaseNextWork:   usecases.NewReleaseNextWork(pools, workUnits, h.publisher, clock),
		Reports:           reports,
	})
	h.server = httptest.NewServer(inboundmcp.Handler(server))
	t.Cleanup(h.server.Close)

	client := sdk.NewClient(&sdk.Implementation{Name: "eval-client", Version: "0.0.1"}, nil)
	transport := &sdk.StreamableClientTransport{Endpoint: h.server.URL}
	session, err := client.Connect(context.Background(), transport, nil)
	if err != nil {
		t.Fatalf("eval harness connect: %v", err)
	}
	h.session = session
	t.Cleanup(func() { _ = session.Close() })
	return h
}

// wireSession builds a real Streamable HTTP server over the given deps and
// connects a client session to it, for evals that do not need seeded
// state.
func wireSession(t *testing.T, deps inboundmcp.Deps) *sdk.ClientSession {
	t.Helper()
	server := inboundmcp.NewServer(deps)
	httpSrv := httptest.NewServer(inboundmcp.Handler(server))
	t.Cleanup(httpSrv.Close)

	client := sdk.NewClient(&sdk.Implementation{Name: "eval-client", Version: "0.0.1"}, nil)
	transport := &sdk.StreamableClientTransport{Endpoint: httpSrv.URL}
	session, err := client.Connect(context.Background(), transport, nil)
	if err != nil {
		t.Fatalf("wire session connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

// callTool invokes a tool and records the result for the Then steps.
func (h *evalHarness) callTool(ctx context.Context, name string, args map[string]any) error {
	res, err := h.session.CallTool(ctx, &sdk.CallToolParams{Name: name, Arguments: args})
	h.lastCallResult, h.lastCallErr = res, err
	h.lastCallContent = ""
	if res != nil {
		for _, c := range res.Content {
			if text, ok := c.(*sdk.TextContent); ok {
				h.lastCallContent += text.Text
			}
		}
	}
	return err
}
