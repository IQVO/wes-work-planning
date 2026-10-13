package mcp_test

import (
	"slices"
	"testing"
)

// TestReportTool_OnlyTheWindowIsRequired pins gap fix D16: the report tool's
// handler treats pathId and granularity as optional (all paths, hourly), so
// the advertised input schema must require only from and to. Without
// `omitempty` the SDK marked every field required and a model that left the
// optional filters out had its call rejected before the handler ran.
func TestReportTool_OnlyTheWindowIsRequired(t *testing.T) {
	for _, tool := range reportsSurfaceTools(t) {
		if tool.Name != "get_release_throughput_report" {
			continue
		}
		s := schemaOf(t, tool.InputSchema)
		required := slices.Clone(s.Required)
		slices.Sort(required)
		if !slices.Equal(required, []string{"from", "to"}) {
			t.Fatalf("get_release_throughput_report requires %v, want only [from to] (pathId and granularity are optional)", s.Required)
		}
		for _, optional := range []string{"pathId", "granularity"} {
			if _, ok := s.Properties[optional]; !ok {
				t.Errorf("optional input %q must still be declared", optional)
			}
		}
		return
	}
	t.Fatal("get_release_throughput_report is not advertised on the reports-wired surface")
}
