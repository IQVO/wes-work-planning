// E3 — behavioral eval suite: Gherkin scenarios in
// testdata/features/mcp_tools.feature driven through godog, the same
// Cucumber-for-Go engine the repo-root REST acceptance suite uses. The
// suite lives inside the mcp package so the evals ship with the adapter
// they evaluate and run in the existing CI test job (`go test ./...`) with
// zero new infrastructure.
package mcp_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cucumber/godog"

	inboundmcp "github.com/claudioed/wes-work-planning/internal/adapters/inbound/mcp"
)

// currentEvalT carries the running *testing.T into the scenario world;
// godog's ScenarioInitializer API does not hand it to step contexts, and
// the shared harness needs it for t.Cleanup. Safe here because the suite
// runs scenarios sequentially inside one test function.
var currentEvalT *testing.T

// TestMCPEvalSuite runs every Gherkin scenario under testdata/features
// against a freshly wired MCP server + client session.
func TestMCPEvalSuite(t *testing.T) {
	currentEvalT = t
	suite := godog.TestSuite{
		ScenarioInitializer: initializeMCPEvalScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"testdata/features"},
			Strict:   true,
			TestingT: t,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("non-zero status returned, failed to run MCP eval scenarios")
	}
}

// mcpEvalWorld is the per-scenario state: one harness (server + session +
// seeded repos) per scenario, rebuilt by the Background step (and again by
// the reports-client Given, which swaps in a server that registers the
// conditional report tool).
type mcpEvalWorld struct {
	h *evalHarness
}

func initializeMCPEvalScenario(sc *godog.ScenarioContext) {
	w := &mcpEvalWorld{}

	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		return ctx, nil
	})

	// Given
	sc.Step(`^the MCP server is running with the canonical eval state \(two pending units on pick-a, a flow-fed pack-a pool over its alarm threshold, and a saturated release-fed sat-a pool\)$`, w.serverRunning)
	sc.Step(`^the MCP server is running with the reports client returning one throughput row$`, w.serverRunningWithReports)

	// When
	sc.Step(`^I call the tool "([^"]*)" with argument "([^"]*)" = "([^"]*)"$`, w.callWithStringArg)
	sc.Step(`^I call the tool "([^"]*)" with argument "([^"]*)" = (\d+)$`, w.callWithNumberArg)
	sc.Step(`^I call the tool "([^"]*)" with arguments$`, w.callWithTableArgs)

	// Then
	sc.Step(`^the tool call succeeds$`, w.callSucceeded)
	sc.Step(`^the tool call does not succeed silently$`, w.callDidNotSucceedSilently)
	sc.Step(`^the tool call reports a problem mentioning "([^"]*)"$`, w.callErroredMentioning)
	sc.Step(`^the structured result field "([^"]*)" is "([^"]*)"$`, w.fieldIsString)
	sc.Step(`^the structured result field "([^"]*)" is (\d+)$`, w.fieldIsNumber)
	sc.Step(`^the structured result field "([^"]*)" is true$`, w.fieldIsTrue)
	sc.Step(`^the structured result field "([^"]*)" is false$`, w.fieldIsFalse)
	sc.Step(`^the structured result field "([^"]*)" has (\d+) entries$`, w.fieldHasEntries)
	sc.Step(`^the structured result field "([^"]*)" entry (\d+) has "([^"]*)" = "([^"]*)"$`, w.entryFieldIsString)
	sc.Step(`^the structured result field "([^"]*)" entry (\d+) has "([^"]*)" = (\d+)$`, w.entryFieldIsNumber)
	sc.Step(`^the domain event "([^"]*)" was published$`, w.eventPublished)
	sc.Step(`^the domain event "([^"]*)" was not published$`, w.eventNotPublished)
}

func (w *mcpEvalWorld) serverRunning() error {
	// The harness binds its own lifecycle to the running *testing.T via
	// newEvalHarness; the world only carries the pointer.
	w.h = newEvalHarness(currentEvalT)
	return nil
}

func (w *mcpEvalWorld) serverRunningWithReports() error {
	w.h = newEvalHarnessWithReports(currentEvalT, &fakeReportsClient{
		report: inboundmcp.ThroughputReportView{
			Rows: []inboundmcp.ThroughputRowView{
				{
					PathId:                   "pick-a",
					HourBucket:               "2026-01-01T13:00:00Z",
					WorkReleased:             4,
					WorkUnitCompleted:        3,
					BacklogThresholdBreached: 1,
				},
			},
		},
	})
	return nil
}

// The When steps RECORD the call outcome (harness lastCall* fields) rather
// than failing the step on it: unlike the pilot repo's surface, this server
// also produces protocol-level rejections (calling the conditionally
// registered report tool on a default deployment is an unknown-tool JSON-RPC
// error), and those are outcomes for the Then steps to pin, not step
// failures. Happy paths are still guarded by "the tool call succeeds".
func (w *mcpEvalWorld) callWithStringArg(tool, arg, value string) error {
	_ = w.h.callTool(context.Background(), tool, map[string]any{arg: value})
	return nil
}

func (w *mcpEvalWorld) callWithNumberArg(tool, arg string, value int64) error {
	_ = w.h.callTool(context.Background(), tool, map[string]any{arg: value})
	return nil
}

func (w *mcpEvalWorld) callWithTableArgs(tool string, table *godog.Table) error {
	args := map[string]any{}
	for _, row := range table.Rows {
		cells := row.Cells
		if len(cells) != 2 {
			return fmt.Errorf("arguments table needs exactly two columns, got %d", len(cells))
		}
		key, raw := cells[0].Value, cells[1].Value
		if n, err := strconv.Atoi(raw); err == nil {
			args[key] = n
			continue
		}
		args[key] = raw
	}
	_ = w.h.callTool(context.Background(), tool, args)
	return nil
}

func (w *mcpEvalWorld) callSucceeded() error {
	if w.h.lastCallErr != nil {
		return fmt.Errorf("tool call failed: %w", w.h.lastCallErr)
	}
	if w.h.lastCallResult == nil || w.h.lastCallResult.IsError {
		return fmt.Errorf("tool call returned an error result: %s", w.h.lastCallContent)
	}
	return nil
}

func (w *mcpEvalWorld) callDidNotSucceedSilently() error {
	if w.h.lastCallErr != nil {
		return nil // protocol-level rejection
	}
	if w.h.lastCallResult != nil && w.h.lastCallResult.IsError {
		return nil // tool-level rejection
	}
	return fmt.Errorf("the call succeeded silently — wrong-typed arguments must not be coerced")
}

func (w *mcpEvalWorld) callErroredMentioning(fragment string) error {
	if w.h.lastCallErr == nil && (w.h.lastCallResult == nil || !w.h.lastCallResult.IsError) {
		return fmt.Errorf("expected a tool error, got success: %s", w.h.lastCallContent)
	}
	if !containsFold(w.h.lastCallContent, fragment) && w.h.lastCallErr != nil && !containsFold(w.h.lastCallErr.Error(), fragment) {
		return fmt.Errorf("expected the tool error to mention %q, got %q / %v", fragment, w.h.lastCallContent, w.h.lastCallErr)
	}
	return nil
}

func (w *mcpEvalWorld) structuredField(field string) (any, error) {
	if w.h.lastCallResult == nil {
		return nil, fmt.Errorf("no tool result recorded")
	}
	obj, ok := w.h.lastCallResult.StructuredContent.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("structured result is not an object: %+v", w.h.lastCallResult.StructuredContent)
	}
	got, ok := obj[field]
	if !ok {
		return nil, fmt.Errorf("structured result has no field %q: %+v", field, obj)
	}
	return got, nil
}

func (w *mcpEvalWorld) fieldIsString(field, want string) error {
	got, err := w.structuredField(field)
	if err != nil {
		return err
	}
	if s, ok := got.(string); ok && s == want {
		return nil
	}
	return fmt.Errorf("structured result field %q = %v, want %q", field, got, want)
}

func (w *mcpEvalWorld) fieldIsNumber(field string, want int64) error {
	got, err := w.structuredField(field)
	if err != nil {
		return err
	}
	switch v := got.(type) {
	case float64:
		if int64(v) == want {
			return nil
		}
	case int:
		if int64(v) == want {
			return nil
		}
	case int64:
		if v == want {
			return nil
		}
	}
	return fmt.Errorf("structured result field %q = %v, want %d", field, got, want)
}

func (w *mcpEvalWorld) fieldIsTrue(field string) error {
	got, err := w.structuredField(field)
	if err != nil {
		return err
	}
	if b, ok := got.(bool); ok && b {
		return nil
	}
	return fmt.Errorf("structured result field %q = %v, want true", field, got)
}

func (w *mcpEvalWorld) fieldIsFalse(field string) error {
	got, err := w.structuredField(field)
	if err != nil {
		return err
	}
	if b, ok := got.(bool); ok && !b {
		return nil
	}
	return fmt.Errorf("structured result field %q = %v, want false", field, got)
}

func (w *mcpEvalWorld) fieldHasEntries(field string, want int) error {
	got, err := w.structuredField(field)
	if err != nil {
		return err
	}
	list, ok := got.([]any)
	if !ok {
		return fmt.Errorf("structured result field %q is not a list: %v", field, got)
	}
	if len(list) != want {
		return fmt.Errorf("structured result field %q has %d entries, want %d", field, len(list), want)
	}
	return nil
}

func (w *mcpEvalWorld) entryField(entryField string, index int, field string) (any, error) {
	got, err := w.structuredField(entryField)
	if err != nil {
		return nil, err
	}
	list, ok := got.([]any)
	if !ok {
		return nil, fmt.Errorf("structured result field %q is not a list: %v", entryField, got)
	}
	if index < 0 || index >= len(list) {
		return nil, fmt.Errorf("structured result field %q has no entry %d (len %d)", entryField, index, len(list))
	}
	obj, ok := list[index].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("entry %d of %q is not an object: %v", index, entryField, list[index])
	}
	value, ok := obj[field]
	if !ok {
		return nil, fmt.Errorf("entry %d of %q has no field %q: %+v", index, entryField, field, obj)
	}
	return value, nil
}

func (w *mcpEvalWorld) entryFieldIsString(entryField string, index int, field, want string) error {
	got, err := w.entryField(entryField, index, field)
	if err != nil {
		return err
	}
	if s, ok := got.(string); ok && s == want {
		return nil
	}
	return fmt.Errorf("entry %d of %q field %q = %v, want %q", index, entryField, field, got, want)
}

func (w *mcpEvalWorld) entryFieldIsNumber(entryField string, index int, field string, want int64) error {
	got, err := w.entryField(entryField, index, field)
	if err != nil {
		return err
	}
	if v, ok := got.(float64); ok && int64(v) == want {
		return nil
	}
	return fmt.Errorf("entry %d of %q field %q = %v, want %d", index, entryField, field, got, want)
}

func (w *mcpEvalWorld) eventPublished(name string) error {
	for _, e := range w.h.publisher.Events() {
		if e.EventName() == name {
			return nil
		}
	}
	return fmt.Errorf("expected domain event %q to be published", name)
}

func (w *mcpEvalWorld) eventNotPublished(name string) error {
	for _, e := range w.h.publisher.Events() {
		if e.EventName() == name {
			return fmt.Errorf("domain event %q was published, want it not to be", name)
		}
	}
	return nil
}

func containsFold(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
