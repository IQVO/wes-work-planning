package main_test

// BDD wave 3: step definitions for the documentation-derived feature files
// (features/*_rules.feature). Everything here is additive vocabulary on top
// of the world and the wave-2 steps: generic HTTP requests with paged reads,
// problem-slug and event-payload assertions, and the process-path catalogue
// variants ADR-0040 section 7 needs. All type names carry a docRules prefix
// so they cannot clash with an identifier in an integration-tagged file of
// this package.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// docRulesProblemBase is the type-URI namespace of this service's RFC 7807
// problems (ADR-0005).
const docRulesProblemBase = "https://errors.wes-work-planning.warehouse-systems.dev/"

type docRulesSteps struct {
	w     *world
	wave2 *wave2

	// listPath is the path (with its query) of the last collection request,
	// so "the next page" can be fetched with the cursor of the last answer.
	listPath string
}

func (s *docRulesSteps) reset() { s.listPath = "" }

// --- generic requests and paged reads --------------------------------------

func (s *docRulesSteps) requestSent(method, path string) error {
	s.listPath = path
	return s.w.do(method, path, nil)
}

func (s *docRulesSteps) requestSentWithBody(method, path, body string) error {
	return s.wave2.doRaw(method, path, body)
}

func (s *docRulesSteps) pageOf() (items []map[string]any, nextCursor string, err error) {
	var out struct {
		Items      *[]map[string]any `json:"items"`
		NextCursor string            `json:"nextCursor"`
	}
	if err := json.Unmarshal(s.w.lastBody, &out); err != nil {
		return nil, "", fmt.Errorf("decode page %q: %w", string(s.w.lastBody), err)
	}
	if out.Items == nil {
		return nil, "", fmt.Errorf("page carries no items array (null or absent): %s", string(s.w.lastBody))
	}
	return *out.Items, out.NextCursor, nil
}

func (s *docRulesSteps) pageHasNextCursor() error {
	_, cursor, err := s.pageOf()
	if err != nil {
		return err
	}
	if cursor == "" {
		return fmt.Errorf("page has no nextCursor: %s", string(s.w.lastBody))
	}
	return nil
}

func (s *docRulesSteps) pageHasNoNextCursor() error {
	_, cursor, err := s.pageOf()
	if err != nil {
		return err
	}
	if cursor != "" {
		return fmt.Errorf("page carries nextCursor %q, want none: %s", cursor, string(s.w.lastBody))
	}
	return nil
}

func (s *docRulesSteps) nextPageFetched() error {
	return s.fetchNextPage(1)
}

// fetchNextPage requests the next page with the cursor of the last answer
// repeated `times` times (a repeated parameter is a different request).
func (s *docRulesSteps) fetchNextPage(times int) error {
	_, cursor, err := s.pageOf()
	if err != nil {
		return err
	}
	if cursor == "" {
		return errors.New("there is no next page to fetch")
	}
	sep := "?"
	if strings.Contains(s.listPath, "?") {
		sep = "&"
	}
	query := strings.TrimSuffix(strings.Repeat("cursor="+cursor+"&", times), "&")
	return s.w.do(http.MethodGet, s.listPath+sep+query, nil)
}

var docRulesQuoted = regexp.MustCompile(`"([^"]*)"`)

func (s *docRulesSteps) pageLists(quoted string) error {
	var want []string
	for _, m := range docRulesQuoted.FindAllStringSubmatch(quoted, -1) {
		want = append(want, m[1])
	}
	items, _, err := s.pageOf()
	if err != nil {
		return err
	}
	got := make([]string, len(items))
	for i, it := range items {
		got[i], _ = it["id"].(string)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		return fmt.Errorf("page lists work units %v, want %v (in this order)", got, want)
	}
	return nil
}

func (s *docRulesSteps) pageIsEmptyArray() error {
	items, cursor, err := s.pageOf()
	if err != nil {
		return err
	}
	if len(items) != 0 || cursor != "" {
		return fmt.Errorf("want an empty items array and no cursor, got %s", string(s.w.lastBody))
	}
	return nil
}

func (s *docRulesSteps) problemSlug(slug string) error {
	return s.w.problemDetailType(docRulesProblemBase + slug)
}

// --- work unit read-back ----------------------------------------------------

func (s *docRulesSteps) workUnitWaitsOnNothing() error {
	obj, err := s.w.decodeLast()
	if err != nil {
		return err
	}
	rel, ok := obj["relocation"].(map[string]any)
	if !ok {
		return fmt.Errorf("no relocation in %s", string(s.w.lastBody))
	}
	if blocker, present := rel["blockedBy"]; present {
		return fmt.Errorf("work unit still waits on %v: %s", blocker, string(s.w.lastBody))
	}
	return nil
}

func (s *docRulesSteps) workUnitCancelledAt(at string) error {
	got, err := s.w.stringField("cancelledAt")
	if err != nil {
		return err
	}
	if got != at {
		return fmt.Errorf("got cancelledAt %q, want %q", got, at)
	}
	return nil
}

func (s *docRulesSteps) workUnitSite(siteId string) error {
	obj, err := s.w.decodeLast()
	if err != nil {
		return err
	}
	rel, ok := obj["relocation"].(map[string]any)
	if !ok || rel["siteId"] != siteId {
		return fmt.Errorf("want relocation at site %q, got %s", siteId, string(s.w.lastBody))
	}
	return nil
}

// --- slot-plan catalogue variants (ADR-0040 section 7) ---------------------

func (s *docRulesSteps) useCatalogue(relocate *pathcatalog.PathDefinition) {
	defs := []pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick"},
		{Id: "PACK", MatchPrefix: "pack"},
		{Id: "REBIN", MatchPrefix: "rebin"},
		{Id: "SLAM", MatchPrefix: "slam"},
	}
	if relocate != nil {
		defs = append(defs, *relocate)
	}
	s.w.h.inbound.slotPlan = s.w.h.inbound.slotPlanWith(pathcatalog.New(defs))
}

func (s *docRulesSteps) catalogueWithoutRelocate() error {
	s.useCatalogue(nil)
	return nil
}

func (s *docRulesSteps) catalogueRelocateWithoutIntake() error {
	s.useCatalogue(&pathcatalog.PathDefinition{Id: "RELOCATE", MatchPrefix: "relocate"})
	return nil
}

func (s *docRulesSteps) catalogueRelocateWithIntakes(list string) error {
	var intake []string
	for _, m := range docRulesQuoted.FindAllStringSubmatch(list, -1) {
		intake = append(intake, m[1])
	}
	s.useCatalogue(&pathcatalog.PathDefinition{Id: "RELOCATE", MatchPrefix: "relocate", Intake: intake})
	return nil
}

// --- consumed events ---------------------------------------------------------

func (s *docRulesSteps) taskCompletedSkippedAlreadyCompleted() error {
	return s.wave2.taskCompletedOutcomeIs(usecases.TaskCompletedAlreadyCompleted, "already completed")
}

func (s *docRulesSteps) orderAllocatedSingleLine(eventId, orderId, promise string, lineNo int, sku, pathId string) error {
	promiseDate, err := time.Parse(time.RFC3339, promise)
	if err != nil {
		return fmt.Errorf("promise date %q: %w", promise, err)
	}
	s.wave2.consumerDuplicate, s.wave2.consumerErr = s.w.h.inbound.orderAllocated.Execute(context.Background(), usecases.ApplyOrderAllocatedRequest{
		EventId:     eventId,
		OccurredAt:  fixedNow,
		OrderId:     orderId,
		PromiseDate: promiseDate,
		Lines:       []usecases.OrderAllocatedLine{{LineNo: lineNo, SKU: sku, PathId: pathId}},
	})
	return nil
}

// --- published events --------------------------------------------------------

func (s *docRulesSteps) lastEvent(name string) (shared.DomainEvent, error) {
	events := s.w.h.published.events
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].EventName() == name {
			return events[i], nil
		}
	}
	return nil, fmt.Errorf("no %s event was published (published: %v)", name, s.w.h.published.names)
}

func (s *docRulesSteps) lastEventIsFor(name, workUnitId, pathId string) error {
	ev, err := s.lastEvent(name)
	if err != nil {
		return err
	}
	var gotId string
	var gotPath shared.PathId
	switch e := ev.(type) {
	case shared.WorkReleased:
		gotId, gotPath = e.WorkUnitId, e.PathId
	case shared.WorkUnitCompleted:
		gotId, gotPath = e.WorkUnitId, e.PathId
	case shared.WorkUnitCreated:
		gotId, gotPath = e.WorkUnitId, e.PathId
	default:
		return fmt.Errorf("%s events are not work-unit events", name)
	}
	if gotId != workUnitId || gotPath.String() != pathId {
		return fmt.Errorf("last %s event is for %s on %s, want %s on %s", name, gotId, gotPath, workUnitId, pathId)
	}
	return nil
}

func registerRulesSteps(sc *godog.ScenarioContext, w *world, wv2 *wave2) {
	s := &docRulesSteps{w: w, wave2: wv2}
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		s.reset()
		return ctx, nil
	})

	// Generic requests and paged reads.
	sc.Step(`^a (GET|POST|PUT) request is sent to "([^"]*)"$`, s.requestSent)
	sc.Step(`^a (POST|PUT) request is sent to "([^"]*)" with the body (.+)$`, s.requestSentWithBody)
	sc.Step(`^the page has a next page cursor$`, s.pageHasNextCursor)
	sc.Step(`^the page has no next page cursor$`, s.pageHasNoNextCursor)
	sc.Step(`^the next page of the last list request is fetched$`, s.nextPageFetched)
	sc.Step(`^the next page of the last list request is fetched with its cursor given twice$`, func() error { return s.fetchNextPage(2) })
	sc.Step(`^the page lists work units (.+)$`, s.pageLists)
	sc.Step(`^the page is an empty array of items$`, s.pageIsEmptyArray)
	sc.Step(`^the problem type slug is "([^"]*)"$`, s.problemSlug)

	// Work unit read-back.
	sc.Step(`^the WorkUnit in the response waits on nothing$`, s.workUnitWaitsOnNothing)
	sc.Step(`^the WorkUnit in the response was cancelled at "([^"]*)"$`, s.workUnitCancelledAt)
	sc.Step(`^the WorkUnit in the response belongs to site "([^"]*)"$`, s.workUnitSite)

	// Slot-plan catalogue variants.
	sc.Step(`^the process-path catalogue does not declare RELOCATE$`, s.catalogueWithoutRelocate)
	sc.Step(`^the process-path catalogue declares RELOCATE without an intake$`, s.catalogueRelocateWithoutIntake)
	sc.Step(`^the process-path catalogue declares RELOCATE with the intakes (.+)$`, s.catalogueRelocateWithIntakes)

	// Consumed events.
	sc.Step(`^the TaskCompleted event is skipped because the work unit was already completed$`, s.taskCompletedSkippedAlreadyCompleted)
	sc.Step(`^order-management publishes OrderAllocated event "([^"]*)" for order "([^"]*)" promised for "([^"]*)" with the single line (-?\d+) of SKU "([^"]*)" on process path "([^"]*)"$`, s.orderAllocatedSingleLine)

	// Published events.
	sc.Step(`^the last "([^"]*)" event is for WorkUnit "([^"]*)" on process path "([^"]*)"$`, s.lastEventIsFor)
}
