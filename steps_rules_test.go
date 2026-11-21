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
	"strconv"
	"strings"
	"time"

	"github.com/cucumber/godog"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
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

	// Outcomes of the last consumed-event use case calls and their errors,
	// so Then steps can assert them.
	classificationOutcome usecases.ProductClassificationOutcome
	packLevelOutcome      usecases.PackLevelOutcome
	replenishOutcome      usecases.ReplenishmentOutcome
	consumerErr           error

	// lastWireKey is the Kafka key of the last encoded wire message.
	lastWireKey string
}

func (s *docRulesSteps) reset() {
	s.listPath = ""
	s.classificationOutcome = usecases.ProductClassificationApplied
	s.packLevelOutcome = usecases.PackLevelApplied
	s.replenishOutcome = usecases.ReplenishmentPlanned
	s.consumerErr = nil
	s.lastWireKey = ""
}

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

// --- documentation wave: wire-payload assertions -----------------------------
//
// The outbound Kafka publisher (internal/adapters/outbound/kafka) is what
// enriches WorkReleased/WorkUnitCreated with the documented data payload.
// These steps re-encode a captured domain event through that production
// Publisher over the SAME in-memory repos and local classification/pack
// copy, so a scenario asserts the exact wire bytes the relay would publish.

// docRulesVoidWriter is the Writer seam NewPublisherWithWriter accepts; the
// wire steps only ever call Encode, which never touches the broker.
type docRulesVoidWriter struct{}

func (docRulesVoidWriter) WriteMessages(context.Context, ...kafkago.Message) error { return nil }
func (docRulesVoidWriter) Close() error                                            { return nil }

// wireEventOf encodes the last domain event called name and returns its
// CloudEvents envelope and data payload as decoded JSON objects.
func (s *docRulesSteps) wireEventOf(name string) (envelope, data map[string]any, err error) {
	ev, err := s.lastEvent(name)
	if err != nil {
		return nil, nil, err
	}
	pub := kafka.NewPublisherWithWriter(docRulesVoidWriter{}, s.w.h.inbound.workUnits,
		s.w.h.inbound.classificationStore, func() string { return "bdd-wire-check" })
	encoded, err := pub.Encode(context.Background(), ev)
	if err != nil {
		return nil, nil, fmt.Errorf("encode %s: %w", name, err)
	}
	if err := json.Unmarshal(encoded[0].Value, &envelope); err != nil {
		return nil, nil, fmt.Errorf("decode %s CloudEvent %s: %w", name, encoded[0].Value, err)
	}
	data, _ = envelope["data"].(map[string]any)
	if data == nil {
		return nil, nil, fmt.Errorf("%s CloudEvent carries no data object: %s", name, encoded[0].Value)
	}
	s.lastWireKey = string(encoded[0].Key)
	return envelope, data, nil
}

func (s *docRulesSteps) wireDataFieldIs(name, field, want string) error {
	_, data, err := s.wireEventOf(name)
	if err != nil {
		return err
	}
	got, present := data[field]
	if !present {
		return fmt.Errorf("%s wire data has no field %q: %v", name, field, data)
	}
	if fmt.Sprint(got) != want {
		return fmt.Errorf("%s wire data %s = %v, want %q", name, field, got, want)
	}
	return nil
}

func (s *docRulesSteps) wireDataOmits(name, field string) error {
	_, data, err := s.wireEventOf(name)
	if err != nil {
		return err
	}
	if value, present := data[field]; present {
		return fmt.Errorf("%s wire data carries %s = %v, want it omitted", name, field, value)
	}
	return nil
}

func (s *docRulesSteps) wireDataRequiresCapabilities(name, want string) error {
	_, data, err := s.wireEventOf(name)
	if err != nil {
		return err
	}
	raw, ok := data["required_capabilities"].([]any)
	if !ok {
		return fmt.Errorf("%s wire data carries no required_capabilities array: %v", name, data)
	}
	var got []string
	for _, item := range raw {
		got = append(got, fmt.Sprint(item))
	}
	if strings.Join(got, ",") != want {
		return fmt.Errorf("%s wire data requires %v, want [%s]", name, got, want)
	}
	return nil
}

func (s *docRulesSteps) wireDataReplenishes(name, requestId string, quantity int, sku, from, to, priority string) error {
	_, data, err := s.wireEventOf(name)
	if err != nil {
		return err
	}
	obj, ok := data["replenishment"].(map[string]any)
	if !ok {
		return fmt.Errorf("%s wire data carries no replenishment object: %v", name, data)
	}
	want := map[string]any{
		"request_id": requestId, "sku": sku, "from_bin": from,
		"to_bin": to, "quantity": float64(quantity), "priority": priority,
	}
	for field, wantValue := range want {
		if got := obj[field]; got != wantValue {
			return fmt.Errorf("%s wire data replenishment.%s = %v, want %v", name, field, got, wantValue)
		}
	}
	return nil
}

func (s *docRulesSteps) wireDataPicks(name string, quantity int, unit, sku string) error {
	_, data, err := s.wireEventOf(name)
	if err != nil {
		return err
	}
	obj, ok := data["pick"].(map[string]any)
	if !ok {
		return fmt.Errorf("%s wire data carries no pick object: %v", name, data)
	}
	if got := obj["sku"]; got != sku {
		return fmt.Errorf("%s wire data pick.sku = %v, want %q", name, got, sku)
	}
	if got, _ := obj["quantity"].(float64); int(got) != quantity {
		return fmt.Errorf("%s wire data pick.quantity = %v, want %d", name, obj["quantity"], quantity)
	}
	if got := obj["unit"]; got != unit {
		return fmt.Errorf("%s wire data pick.unit = %v, want %q", name, got, unit)
	}
	return nil
}

func (s *docRulesSteps) wireDataPicksFromBin(name, bin string) error {
	_, data, err := s.wireEventOf(name)
	if err != nil {
		return err
	}
	obj, ok := data["pick"].(map[string]any)
	if !ok {
		return fmt.Errorf("%s wire data carries no pick object: %v", name, data)
	}
	if got := obj["from_bin"]; got != bin {
		return fmt.Errorf("%s wire data pick.from_bin = %v, want %q", name, got, bin)
	}
	return nil
}

func (s *docRulesSteps) wireDataPicksFromNoBin(name string) error {
	_, data, err := s.wireEventOf(name)
	if err != nil {
		return err
	}
	obj, ok := data["pick"].(map[string]any)
	if !ok {
		return fmt.Errorf("%s wire data carries no pick object: %v", name, data)
	}
	if got, present := obj["from_bin"]; present {
		return fmt.Errorf("%s wire data pick.from_bin = %v, want it omitted", name, got)
	}
	return nil
}

func (s *docRulesSteps) wireDataBreaksCase(name, lpn, sku, bin, task string) error {
	_, data, err := s.wireEventOf(name)
	if err != nil {
		return err
	}
	obj, ok := data["case_break"].(map[string]any)
	if !ok {
		return fmt.Errorf("%s wire data carries no case_break object: %v", name, data)
	}
	want := map[string]any{"lpn": lpn, "sku": sku, "loose_bin": bin, "pick_task_id": task}
	for field, wantValue := range want {
		if got := obj[field]; got != wantValue {
			return fmt.Errorf("%s wire data case_break.%s = %v, want %v", name, field, got, wantValue)
		}
	}
	return nil
}

func (s *docRulesSteps) wireMessageIsCloudEvents(name, eventType, dataschema, subject string) error {
	envelope, _, err := s.wireEventOf(name)
	if err != nil {
		return err
	}
	if got := envelope["specversion"]; got != "1.0" {
		return fmt.Errorf("%s wire message specversion = %v, want 1.0", name, got)
	}
	if got := envelope["type"]; got != eventType {
		return fmt.Errorf("%s wire message type = %v, want %q", name, got, eventType)
	}
	if got := envelope["dataschema"]; got != dataschema {
		return fmt.Errorf("%s wire message dataschema = %v, want %q", name, got, dataschema)
	}
	if got := envelope["subject"]; got != subject {
		return fmt.Errorf("%s wire message subject = %v, want %q", name, got, subject)
	}
	if s.lastWireKey != subject {
		return fmt.Errorf("%s wire message key = %q, want the subject %q (partition affinity, ADR-0024)", name, s.lastWireKey, subject)
	}
	return nil
}

// --- documentation wave: product classification copy (ADR-0035) --------------

func (s *docRulesSteps) classifySKU(eventId, sku, tags string, version int64) error {
	var handling []string
	if strings.TrimSpace(tags) != "" {
		for _, tag := range strings.Split(tags, ",") {
			handling = append(handling, strings.TrimSpace(tag))
		}
	}
	s.classificationOutcome, s.consumerErr = s.w.h.inbound.classification.Execute(context.Background(),
		usecases.ObserveProductClassificationRequest{
			EventId: eventId, SKU: sku, HandlingTags: handling,
			Version: version, ObservedAt: fixedNow,
		})
	return nil
}
func (s *docRulesSteps) classificationOutcomeIs(want usecases.ProductClassificationOutcome, label string) error {
	if s.consumerErr != nil {
		return fmt.Errorf("classification event failed: %w", s.consumerErr)
	}
	if s.classificationOutcome != want {
		return fmt.Errorf("classification event outcome %s, want %s", s.classificationOutcome, label)
	}
	return nil
}

// --- documentation wave: pack level copy (ADR-0045) ---------------------------

func (s *docRulesSteps) declarePackLevels(doc *godog.Table) error {
	// Rows[0] is the header row; the data rows follow.
	for _, row := range doc.Rows[1:] {
		cell := func(name string) string { return row.Cells[columnIndex(doc, name)].Value }
		units, err := strconv.Atoi(cell("units_per_pack"))
		if err != nil {
			return fmt.Errorf("units_per_pack %q: %w", cell("units_per_pack"), err)
		}
		version, err := strconv.ParseInt(cell("version"), 10, 64)
		if err != nil {
			return fmt.Errorf("version %q: %w", cell("version"), err)
		}
		s.packLevelOutcome, s.consumerErr = s.w.h.inbound.packLevel.Execute(context.Background(),
			usecases.ObservePackLevelRequest{
				EventId: fmt.Sprintf("pack-%s-%s", cell("sku"), cell("version")),
				Event:   usecases.PackLevelDeclared,
				Record: productclassificationview.PackLevelRecord{
					SKU: cell("sku"), Level: productclassificationview.PackLevel(cell("level")),
					GTIN: cell("gtin"), UnitsPerPack: units, Version: version,
				},
				ObservedAt: fixedNow,
			})
		if s.consumerErr != nil {
			return fmt.Errorf("declaring %s/%s: %w", cell("sku"), cell("level"), s.consumerErr)
		}
		if s.packLevelOutcome != usecases.PackLevelApplied {
			return fmt.Errorf("declaring %s/%s: outcome %s, want applied", cell("sku"), cell("level"), s.packLevelOutcome)
		}
	}
	return nil
}

func columnIndex(doc *godog.Table, name string) int {
	for i, cell := range doc.Rows[0].Cells {
		if cell.Value == name {
			return i
		}
	}
	return -1
}

func (s *docRulesSteps) retirePackLevel(gtin string, version int64) error {
	s.packLevelOutcome, s.consumerErr = s.w.h.inbound.packLevel.Execute(context.Background(),
		usecases.ObservePackLevelRequest{
			EventId: fmt.Sprintf("retire-%s-%d", gtin, version), Event: usecases.PackLevelRetired,
			Record: productclassificationview.PackLevelRecord{
				GTIN: gtin, Version: version, EffectiveTo: fixedNow,
			},
			ObservedAt: fixedNow,
		})
	return nil
}

func (s *docRulesSteps) measurePackLevel(gtin string, length, width, height, weight, version int) error {
	s.packLevelOutcome, s.consumerErr = s.w.h.inbound.packLevel.Execute(context.Background(),
		usecases.ObservePackLevelRequest{
			EventId: fmt.Sprintf("measure-%s-%d", gtin, version), Event: usecases.PackLevelMeasured,
			Record: productclassificationview.PackLevelRecord{
				GTIN: gtin, Version: int64(version),
				Measured: productclassificationview.Dims{LengthMm: length, WidthMm: width, HeightMm: height, WeightG: weight},
			},
			ObservedAt: fixedNow,
		})
	return nil
}

func (s *docRulesSteps) packLevelOutcomeIs(want usecases.PackLevelOutcome, label string) error {
	if s.consumerErr != nil {
		return fmt.Errorf("pack level event failed: %w", s.consumerErr)
	}
	if s.packLevelOutcome != want {
		return fmt.Errorf("pack level event outcome %s, want %s", s.packLevelOutcome, label)
	}
	return nil
}

// --- documentation wave: pick-face replenishment (ADR-0042) -------------------

func (s *docRulesSteps) replenishmentRequested(eventId, requestId string, quantity int, sku, from, to, priority, site, requestedAt string) error {
	// An empty requested_at stands for the field the event never carried.
	var ts time.Time
	if requestedAt != "" {
		parsed, err := time.Parse(time.RFC3339, requestedAt)
		if err != nil {
			return fmt.Errorf("requested_at %q: %w", requestedAt, err)
		}
		ts = parsed
	}
	s.replenishOutcome, s.consumerErr = s.w.h.inbound.replenishRequested.Execute(context.Background(),
		usecases.ApplyReplenishmentRequestedRequest{
			EventId: eventId, OccurredAt: fixedNow, RequestId: requestId, SiteId: site,
			SKU: sku, FromBin: from, ToBin: to, Quantity: quantity,
			Priority: priority, RequestedAt: ts,
		})
	return nil
}

func (s *docRulesSteps) replenishmentCancelled(eventId, requestId string) error {
	s.replenishOutcome, s.consumerErr = s.w.h.inbound.replenishCancelled.Execute(context.Background(),
		usecases.ApplyReplenishmentCancelledRequest{
			EventId: eventId, OccurredAt: fixedNow, RequestId: requestId, Reason: "OPERATOR",
		})
	return nil
}

func (s *docRulesSteps) replenishmentCancelledBlank(eventId string) error {
	return s.replenishmentCancelled(eventId, "  ")
}

func (s *docRulesSteps) replenishOutcomeIs(want usecases.ReplenishmentOutcome, label string) error {
	if s.consumerErr != nil {
		return fmt.Errorf("replenishment event failed: %w (this run's outcome %s)", s.consumerErr, s.replenishOutcome)
	}
	if s.replenishOutcome != want {
		return fmt.Errorf("replenishment event outcome %s, want %s", s.replenishOutcome, label)
	}
	return nil
}

func (s *docRulesSteps) replenishmentRefusedAsPoison() error {
	if s.consumerErr == nil {
		return fmt.Errorf("replenishment event was accepted (outcome %s), want a deterministic refusal", s.replenishOutcome)
	}
	return nil
}

// --- documentation wave: pack-aware REST submits (ADR-0045) -------------------

func (s *docRulesSteps) submitPick(id, path string, quantity int, sku, unit string) error {
	return s.w.do(http.MethodPost, "/paths/"+path+"/work-units", map[string]any{
		"workUnitId": id, "cpt": defaultCPT, "reference": "order-9001",
		"sku": sku, "quantity": quantity, "pickUnit": unit,
	})
}

func (s *docRulesSteps) submitPickFromBin(id, path string, quantity int, sku, unit, bin string) error {
	return s.w.do(http.MethodPost, "/paths/"+path+"/work-units", map[string]any{
		"workUnitId": id, "cpt": defaultCPT, "reference": "order-9001",
		"sku": sku, "quantity": quantity, "pickUnit": unit, "fromBin": bin,
	})
}

func (s *docRulesSteps) submitCaseBreak(id, path, lpn, sku, bin, task string) error {
	return s.w.do(http.MethodPost, "/paths/"+path+"/work-units", map[string]any{
		"workUnitId": id, "cpt": defaultCPT, "reference": "order-9001", "sku": sku,
		"caseBreak": map[string]any{"lpn": lpn, "looseBin": bin, "pickTaskId": task},
	})
}

func (s *docRulesSteps) workUnitWorkKind(kind string) error {
	got, err := s.w.stringField("workKind")
	if err != nil {
		return err
	}
	if got != kind {
		return fmt.Errorf("work unit workKind %q, want %q: %s", got, kind, string(s.w.lastBody))
	}
	return nil
}

func (s *docRulesSteps) workUnitPicks(quantity int, unit string) error {
	obj, err := s.w.decodeLast()
	if err != nil {
		return err
	}
	pick, ok := obj["pick"].(map[string]any)
	if !ok {
		return fmt.Errorf("work unit carries no pick: %s", string(s.w.lastBody))
	}
	gotQty, _ := pick["quantity"].(float64)
	if int(gotQty) != quantity || pick["unit"] != unit {
		return fmt.Errorf("work unit picks %v %v, want %d %q", pick["quantity"], pick["unit"], quantity, unit)
	}
	return nil
}

func (s *docRulesSteps) workUnitPicksFrom(bin string) error {
	obj, err := s.w.decodeLast()
	if err != nil {
		return err
	}
	pick, ok := obj["pick"].(map[string]any)
	if !ok {
		return fmt.Errorf("work unit carries no pick: %s", string(s.w.lastBody))
	}
	if got := pick["fromBin"]; got != bin {
		return fmt.Errorf("work unit picks from %v, want %q", got, bin)
	}
	return nil
}

func (s *docRulesSteps) workUnitPicksFromNoBin() error {
	obj, err := s.w.decodeLast()
	if err != nil {
		return err
	}
	pick, ok := obj["pick"].(map[string]any)
	if !ok {
		return fmt.Errorf("work unit carries no pick: %s", string(s.w.lastBody))
	}
	if got, present := pick["fromBin"]; present {
		return fmt.Errorf("work unit picks from %v, want no from bin", got)
	}
	return nil
}

func (s *docRulesSteps) workUnitBreaks(lpn, bin, task string) error {
	obj, err := s.w.decodeLast()
	if err != nil {
		return err
	}
	brk, ok := obj["caseBreak"].(map[string]any)
	if !ok {
		return fmt.Errorf("work unit carries no case break: %s", string(s.w.lastBody))
	}
	if brk["lpn"] != lpn || brk["looseBin"] != bin || brk["pickTaskId"] != task {
		return fmt.Errorf("work unit breaks %v, want LPN %s into %s for %s", brk, lpn, bin, task)
	}
	return nil
}

// --- documentation wave: catalogue variants -----------------------------------

// installCatalogue rebuilds every catalogue-dependent piece of the live
// harness (router path validation, the enqueue use case's canonical-path
// checks, the REPLENISH use case) over the base families plus extra.
func (s *docRulesSteps) installCatalogue(extra ...pathcatalog.PathDefinition) {
	defs := []pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick"},
		{Id: "PACK", MatchPrefix: "pack"},
		{Id: "REBIN", MatchPrefix: "rebin"},
		{Id: "SLAM", MatchPrefix: "slam"},
		{Id: "RELOCATE", MatchPrefix: "relocate", Intake: []string{pathcatalog.IntakeSlotRelocation}},
	}
	defs = append(defs, extra...)
	catalogue := pathcatalog.New(defs)
	in := s.w.h.inbound
	in.handlers.Catalogue = catalogue
	in.handlers.EnqueueWorkUnit = in.enqueueWithCatalogue(catalogue)
	in.replenishRequested = in.replenishWith(catalogue)
}

func (s *docRulesSteps) catalogueDeclaresReplenish() error {
	s.installCatalogue(pathcatalog.PathDefinition{Id: "REPLENISH", MatchPrefix: "replenish"})
	return nil
}

func (s *docRulesSteps) catalogueWithoutReplenish() error {
	s.installCatalogue()
	return nil
}

func (s *docRulesSteps) catalogueDeclaresBreakCase() error {
	s.installCatalogue(pathcatalog.PathDefinition{Id: "BREAK_CASE", MatchPrefix: "break_case"})
	return nil
}

// --- documentation wave: an allocated order with a fulfillment site (ADR-0048)

func (s *docRulesSteps) orderAllocatedAtSite(eventId, orderId, promise, site string, lineNo int, sku, pathId string) error {
	promiseDate, err := time.Parse(time.RFC3339, promise)
	if err != nil {
		return fmt.Errorf("promise date %q: %w", promise, err)
	}
	s.wave2.consumerDuplicate, s.wave2.consumerErr = s.w.h.inbound.orderAllocated.Execute(context.Background(), usecases.ApplyOrderAllocatedRequest{
		EventId: eventId, OccurredAt: fixedNow, OrderId: orderId, PromiseDate: promiseDate,
		SiteId: site,
		Lines:  []usecases.OrderAllocatedLine{{LineNo: lineNo, SKU: sku, PathId: pathId}},
	})
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

	// Documentation wave: wire payloads (the outbound Kafka publisher's
	// exact CloudEvents bytes for a captured domain event).
	sc.Step(`^the "([^"]*)" wire data field "([^"]*)" is "([^"]*)"$`, s.wireDataFieldIs)
	sc.Step(`^the "([^"]*)" wire data omits "([^"]*)"$`, s.wireDataOmits)
	sc.Step(`^the "([^"]*)" wire data requires capabilities "([^"]*)"$`, s.wireDataRequiresCapabilities)
	sc.Step(`^the "([^"]*)" wire data replenishes request "([^"]*)" with (\d+) units of SKU "([^"]*)" from bin "([^"]*)" to bin "([^"]*)" with priority "([^"]*)"$`, s.wireDataReplenishes)
	sc.Step(`^the "([^"]*)" wire data picks (\d+) "([^"]*)" of SKU "([^"]*)"$`, s.wireDataPicks)
	sc.Step(`^the "([^"]*)" wire data picks from bin "([^"]*)"$`, s.wireDataPicksFromBin)
	sc.Step(`^the "([^"]*)" wire data picks from no bin$`, s.wireDataPicksFromNoBin)
	sc.Step(`^the "([^"]*)" wire data breaks LPN "([^"]*)" of SKU "([^"]*)" into bin "([^"]*)" for pick task "([^"]*)"$`, s.wireDataBreaksCase)
	sc.Step(`^the "([^"]*)" wire message is CloudEvents 1.0 typed "([^"]*)" with dataschema "([^"]*)" and subject "([^"]*)"$`, s.wireMessageIsCloudEvents)

	// Documentation wave: the local product classification copy (ADR-0035).
	sc.Step(`^product-master publishes classification event "([^"]*)" for SKU "([^"]*)" with tags "([^"]*)" at version (\d+)$`, s.classifySKU)
	sc.Step(`^the classification event is applied$`, func() error {
		return s.classificationOutcomeIs(usecases.ProductClassificationApplied, "applied")
	})
	sc.Step(`^the classification event is a stale no-op$`, func() error {
		return s.classificationOutcomeIs(usecases.ProductClassificationStale, "stale")
	})
	sc.Step(`^the classification event is ignored as a redelivery$`, func() error {
		return s.classificationOutcomeIs(usecases.ProductClassificationRedelivered, "redelivered")
	})

	// Documentation wave: the local pack level copy (ADR-0045).
	sc.Step(`^product-master declares pack levels:$`, s.declarePackLevels)
	sc.Step(`^product-master retires the pack level of GTIN "([^"]*)" at version (\d+)$`, s.retirePackLevel)
	sc.Step(`^product-master measures the pack level of GTIN "([^"]*)" at (\d+)x(\d+)x(\d+) mm and (\d+) g at version (\d+)$`, s.measurePackLevel)
	sc.Step(`^the pack level event is applied$`, func() error {
		return s.packLevelOutcomeIs(usecases.PackLevelApplied, "applied")
	})
	sc.Step(`^the pack level event is a stale no-op$`, func() error {
		return s.packLevelOutcomeIs(usecases.PackLevelStale, "stale")
	})
	sc.Step(`^the pack level event is ignored as a redelivery$`, func() error {
		return s.packLevelOutcomeIs(usecases.PackLevelRedelivered, "redelivered")
	})

	// Documentation wave: pick-face replenishment (ADR-0042).
	sc.Step(`^pick-face-replenishment publishes ReplenishmentRequested event "([^"]*)" for request "([^"]*)" of (-?\d+) units of SKU "([^"]*)" from bin "([^"]*)" to bin "([^"]*)" with priority "([^"]*)" at site "([^"]*)" requested at "([^"]*)"$`, s.replenishmentRequested)
	sc.Step(`^pick-face-replenishment publishes ReplenishmentCancelled event "([^"]*)" for request "([^"]*)"$`, s.replenishmentCancelled)
	sc.Step(`^pick-face-replenishment publishes ReplenishmentCancelled event "([^"]*)" with a blank request id$`, s.replenishmentCancelledBlank)
	sc.Step(`^the ReplenishmentRequested event is applied$`, func() error {
		return s.replenishOutcomeIs(usecases.ReplenishmentPlanned, "planned")
	})
	sc.Step(`^the ReplenishmentRequested event is a no-op because the request is already planned$`, func() error {
		return s.replenishOutcomeIs(usecases.ReplenishmentAlreadyPlanned, "already planned")
	})
	sc.Step(`^the ReplenishmentRequested event is ignored as a redelivery$`, func() error {
		return s.replenishOutcomeIs(usecases.ReplenishmentAlreadyProcessed, "already processed")
	})
	sc.Step(`^the ReplenishmentRequested event is refused as poison$`, s.replenishmentRefusedAsPoison)
	sc.Step(`^the ReplenishmentCancelled event cancels the pending unit$`, func() error {
		return s.replenishOutcomeIs(usecases.ReplenishmentPlanned, "planned")
	})
	sc.Step(`^the ReplenishmentCancelled event leaves the unit running$`, func() error {
		return s.replenishOutcomeIs(usecases.ReplenishmentNotCancellable, "not cancellable")
	})
	sc.Step(`^the ReplenishmentCancelled event is a no-op for a request this context never planned$`, func() error {
		return s.replenishOutcomeIs(usecases.ReplenishmentUnknownRequest, "unknown request")
	})
	sc.Step(`^the ReplenishmentCancelled event is ignored as a redelivery$`, func() error {
		return s.replenishOutcomeIs(usecases.ReplenishmentAlreadyProcessed, "already processed")
	})
	sc.Step(`^the ReplenishmentCancelled event is refused as poison$`, s.replenishmentRefusedAsPoison)

	// Documentation wave: pack-aware REST submits (ADR-0045).
	sc.Step(`^a WorkUnit "([^"]*)" is submitted to process path "([^"]*)" for (\d+) eaches of SKU "([^"]*)" picked as "([^"]*)"$`, s.submitPick)
	sc.Step(`^a WorkUnit "([^"]*)" is submitted to process path "([^"]*)" for (\d+) eaches of SKU "([^"]*)" picked as "([^"]*)" from bin "([^"]*)"$`, s.submitPickFromBin)
	sc.Step(`^a WorkUnit "([^"]*)" is submitted to process path "([^"]*)" to break LPN "([^"]*)" of SKU "([^"]*)" into bin "([^"]*)" for pick task "([^"]*)"$`, s.submitCaseBreak)
	sc.Step(`^the WorkUnit in the response has workKind "([^"]*)"$`, s.workUnitWorkKind)
	sc.Step(`^the WorkUnit in the response picks (\d+) "([^"]*)"$`, s.workUnitPicks)
	sc.Step(`^the WorkUnit in the response picks from bin "([^"]*)"$`, s.workUnitPicksFrom)
	sc.Step(`^the WorkUnit in the response picks from no bin$`, s.workUnitPicksFromNoBin)
	sc.Step(`^the WorkUnit in the response breaks LPN "([^"]*)" into bin "([^"]*)" for pick task "([^"]*)"$`, s.workUnitBreaks)

	// Documentation wave: catalogue variants (REPLENISH, BREAK_CASE).
	sc.Step(`^the process-path catalogue declares REPLENISH$`, s.catalogueDeclaresReplenish)
	sc.Step(`^the process-path catalogue does not declare REPLENISH$`, s.catalogueWithoutReplenish)
	sc.Step(`^the process-path catalogue declares BREAK_CASE$`, s.catalogueDeclaresBreakCase)

	// Documentation wave: an allocated order with a fulfillment site (ADR-0048).
	sc.Step(`^order-management publishes OrderAllocated event "([^"]*)" for order "([^"]*)" promised for "([^"]*)" fulfilled at site "([^"]*)" with the single line (\d+) of SKU "([^"]*)" on process path "([^"]*)"$`, s.orderAllocatedAtSite)
}
