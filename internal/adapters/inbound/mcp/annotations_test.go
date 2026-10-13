package mcp

import (
	"strings"
	"testing"
)

// eventRaisingTools are the tools whose use case publishes a domain event
// as part of the call (ADR-0037, fleet gap fix D17):
//
//   - get_backlog_telemetry runs SampleBacklog, which publishes
//     BacklogThresholdBreached when the pool is over its alarm threshold.
//   - get_rebalance_recommendation runs RebalanceDecision, which publishes
//     PathThrottled (flow-fed, over threshold) or LaborReassignmentFlagged
//     (release-fed, saturated at its WIP limit with work backlogged).
//
// Those events go through the transactional outbox onto both topics, where
// other contexts act on them; a published event cannot be recalled.
var eventRaisingTools = map[string][]string{
	"get_backlog_telemetry":        {"BacklogThresholdBreached"},
	"get_rebalance_recommendation": {"PathThrottled", "LaborReassignmentFlagged"},
}

// TestTools_EventRaisingToolsAreNotAnnotatedReadOnly pins D17: a host that
// trusts readOnlyHint to skip a confirmation must not be told a tool is
// side-effect free when it publishes events. Under the governance charter
// (§4.2) a non-read-only tool is annotated destructive; neither tool is
// idempotent (every over-threshold call publishes again).
func TestTools_EventRaisingToolsAreNotAnnotatedReadOnly(t *testing.T) {
	seen := map[string]bool{}
	for _, tool := range governanceTools(t) {
		events, ok := eventRaisingTools[tool.Name]
		if !ok {
			continue
		}
		seen[tool.Name] = true
		if tool.Annotations == nil {
			t.Fatalf("tool %q has no annotations", tool.Name)
		}
		if tool.Annotations.ReadOnlyHint {
			t.Errorf("tool %q publishes %v but is annotated readOnlyHint=true", tool.Name, events)
		}
		if tool.Annotations.DestructiveHint == nil || !*tool.Annotations.DestructiveHint {
			t.Errorf("tool %q is non-read-only, so charter §4.2 requires destructiveHint=true", tool.Name)
		}
		if tool.Annotations.IdempotentHint {
			t.Errorf("tool %q publishes again on every qualifying call; idempotentHint must be false", tool.Name)
		}
		for _, ev := range events {
			if !strings.Contains(tool.Description, ev) {
				t.Errorf("tool %q description must name its side effect %s (charter §4.4): %q", tool.Name, ev, tool.Description)
			}
		}
	}
	for name := range eventRaisingTools {
		if !seen[name] {
			t.Errorf("tool %q is not advertised", name)
		}
	}
}
