//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/claudioed/wes-work-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ADR-0010 regression: gift_wrap was carried on the WorkUnit aggregate but
// the Postgres repo never wrote or read it, so every unit loaded back from
// the database reported false and WorkReleased never carried gift_wrap=true.

func newGiftWrapUnit(t *testing.T, id, ref string, giftWrap bool) *workunit.WorkUnit {
	t.Helper()
	cpt := shared.NewCPT(time.Now().Add(time.Hour).Truncate(time.Microsecond))
	unit, err := workunit.NewWorkUnit(id, mustPath(t, "pick-gw"), cpt, ref)
	if err != nil {
		t.Fatalf("new work unit: %v", err)
	}
	unit.SetSKU("sku-gw")
	unit.SetGiftWrap(giftWrap)
	return unit
}

func TestWorkUnitRepo_GiftWrapRoundTripsThroughEveryFinder(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewWorkUnitRepo(pool)

	if err := repo.Save(ctx, newGiftWrapUnit(t, "wu-gw-yes", "ref-gw", true)); err != nil {
		t.Fatalf("save gift-wrapped unit: %v", err)
	}
	if err := repo.Save(ctx, newGiftWrapUnit(t, "wu-gw-no", "ref-gw", false)); err != nil {
		t.Fatalf("save plain unit: %v", err)
	}

	byId, err := repo.FindById(ctx, "wu-gw-yes")
	if err != nil || !byId.GiftWrap() {
		t.Fatalf("FindById: giftWrap=%v err=%v, want true", byId != nil && byId.GiftWrap(), err)
	}
	plain, err := repo.FindById(ctx, "wu-gw-no")
	if err != nil || plain.GiftWrap() {
		t.Fatalf("FindById plain: giftWrap must default to false (err=%v)", err)
	}

	got := map[string]bool{}
	byPath, err := repo.FindByPathId(ctx, mustPath(t, "pick-gw"))
	if err != nil {
		t.Fatalf("FindByPathId: %v", err)
	}
	for _, u := range byPath {
		got["path:"+u.Id()] = u.GiftWrap()
	}
	byRef, err := repo.FindByReference(ctx, "ref-gw")
	if err != nil {
		t.Fatalf("FindByReference: %v", err)
	}
	for _, u := range byRef {
		got["ref:"+u.Id()] = u.GiftWrap()
	}
	want := map[string]bool{"path:wu-gw-yes": true, "path:wu-gw-no": false, "ref:wu-gw-yes": true, "ref:wu-gw-no": false}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s giftWrap = %v, want %v (all: %v)", k, got[k], v, got)
		}
	}

	// A later Save of the same id updates the column (ON CONFLICT path).
	upd := newGiftWrapUnit(t, "wu-gw-no", "ref-gw", true)
	if err := repo.Save(ctx, upd); err != nil {
		t.Fatalf("re-save: %v", err)
	}
	if again, _ := repo.FindById(ctx, "wu-gw-no"); again == nil || !again.GiftWrap() {
		t.Fatal("ON CONFLICT update did not persist gift_wrap")
	}
}

// WorkReleased is encoded from a unit read back through FindById; it must
// carry gift_wrap=true when the unit was gift-wrapped, and omit it otherwise.
func TestWorkReleased_BuiltFromPostgresFindById_CarriesGiftWrap(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewWorkUnitRepo(pool)
	unit := newGiftWrapUnit(t, "wu-gw-evt", "ref-gw-evt", true)
	if err := unit.Release(time.Now().UTC()); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := repo.Save(ctx, unit); err != nil {
		t.Fatalf("save: %v", err)
	}

	integration, _ := encoders(pool)
	released := shared.NewWorkReleased("wu-gw-evt", mustPath(t, "pick-gw"), time.Now().UTC())
	msgs, err := integration.Encode(ctx, released)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("encode: msgs=%d err=%v", len(msgs), err)
	}
	if msgs[0].Topic != cloudevents.TopicWorkPlanningEvents {
		t.Fatalf("topic = %s", msgs[0].Topic)
	}
	var ce struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(msgs[0].Value, &ce); err != nil {
		t.Fatalf("decode CloudEvent: %v", err)
	}
	if ce.Data["gift_wrap"] != true || ce.Data["ref"] != "ref-gw-evt" {
		t.Fatalf("WorkReleased data = %v, want gift_wrap=true and ref", ce.Data)
	}
}
