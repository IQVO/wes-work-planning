package kafkacatalog

import (
	"context"
	"slices"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
)

// ADR-0040 / process-path-management ADR 0034 (fleet decision E5): the
// catalogue cache carries each path's declared `intake` verbatim, so
// ApplySlotPlanApproved can refuse a RELOCATE path that does not admit
// SLOT_RELOCATION. The payload below is process-path-management's own
// RELOCATE golden (its relocate_golden_test.go) byte for byte.
func TestConsumer_Created_DecodesIntake(t *testing.T) {
	relocate := []byte(`{
		"specversion": "1.0",
		"id": "7b3e4c90-1f53-4c8d-a02e-8d4f3a7c9b12",
		"source": "/warehouse/process-path-management",
		"type": "com.warehouse.wes.process-path-management.processpath.ProcessPathCreated",
		"subject": "RELOCATE",
		"time": "2026-10-14T00:00:00Z",
		"datacontenttype": "application/json",
		"dataschema": "urn:warehouse:process-path-management:events:ProcessPathCreated:v1",
		"data": {"path_id":"RELOCATE","match_prefix":"relocate","direct":true,"required_capabilities":["relocate"],"cycle_time_p95":"2h0m0s","eligibility":{},"intake":["SLOT_RELOCATION"]}
	}`)
	pack := []byte(`{
		"specversion": "1.0",
		"id": "7b3e4c90-1f53-4c8d-a02e-8d4f3a7c9b13",
		"source": "/warehouse/process-path-management",
		"type": "com.warehouse.wes.process-path-management.processpath.ProcessPathCreated",
		"subject": "PACK",
		"time": "2026-10-14T00:00:00Z",
		"datacontenttype": "application/json",
		"dataschema": "urn:warehouse:process-path-management:events:ProcessPathCreated:v1",
		"data": {"path_id":"PACK","match_prefix":"pack","required_capabilities":["pack"],"cycle_time_p95":"0s","eligibility":{},"intake":[]}
	}`)
	reader := &fakeReader{messages: []kafkago.Message{{Partition: 0, Offset: 0, Value: relocate}, {Partition: 0, Offset: 1, Value: pack}}}
	c := newTestConsumer(reader, targetOffsets{0: 2})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()
	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready before timeout, got: %v", err)
	}

	def, err := c.Lookup("RELOCATE")
	if err != nil {
		t.Fatalf("RELOCATE: %v", err)
	}
	if !slices.Equal(def.Intake, []string{pathcatalog.IntakeSlotRelocation}) || !def.Admits(pathcatalog.IntakeSlotRelocation) {
		t.Fatalf("RELOCATE intake = %v", def.Intake)
	}
	if def.Admits("CUSTOMER_ORDER") {
		t.Fatal("RELOCATE must not admit customer orders")
	}
	packDef, err := c.Lookup("pack")
	if err != nil || len(packDef.Intake) != 0 {
		t.Fatalf("PACK = (%+v, %v), want no intake", packDef, err)
	}
}
