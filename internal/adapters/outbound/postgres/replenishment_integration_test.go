//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5"

	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/events"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/wes-work-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ADR-0042 on a real Postgres (testcontainers, migrations 0001-0015): the
// replenishment columns round-trip, migration 0015 is additive and its down
// removes only its own columns, the memory adapter behaves like Postgres for
// the same unit, and a request commits (or rolls back) atomically with its
// processed-event mark, pool entry and outbox rows.

var replAt = time.Date(2026, 10, 17, 9, 30, 0, 0, time.UTC)

func replUnit(t *testing.T, requestId, priority string) *workunit.WorkUnit {
	t.Helper()
	cpt := shared.NewCPT(replAt.Add(8 * time.Hour))
	unit, err := workunit.NewWorkUnit(workunit.ReplenishmentWorkUnitId(requestId), mustPath(t, "REPLENISH"), cpt, requestId)
	if err != nil {
		t.Fatalf("NewWorkUnit: %v", err)
	}
	move, err := workunit.NewReplenishmentMove(requestId, "SKU-PG", "WH1-RSV-A07-03-02-B", "WH1-FWD-A01-01-01-A", 24, priority)
	if err != nil {
		t.Fatalf("NewReplenishmentMove: %v", err)
	}
	unit.SetWorkKind(workunit.WorkKindReplenish)
	unit.SetSiteId("WH1")
	unit.SetReplenishment(move)
	return unit
}

func TestWorkUnitRepo_ReplenishmentColumnsRoundTrip(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewWorkUnitRepo(pool)

	if err := repo.Save(ctx, replUnit(t, "repl-pg-1", "URGENT")); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := repo.FindById(ctx, "replenish-repl-pg-1")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	m, ok := got.Replenishment()
	want := workunit.ReplenishmentMove{RequestId: "repl-pg-1", SKU: "SKU-PG", FromBin: "WH1-RSV-A07-03-02-B", ToBin: "WH1-FWD-A01-01-01-A", Quantity: 24, Priority: workunit.ReplenishmentPriorityUrgent}
	if !ok || m != want {
		t.Fatalf("replenishment = (%+v, %v), want %+v", m, ok, want)
	}
	if got.WorkKind() != workunit.WorkKindReplenish || got.SiteId() != "WH1" || got.SKU() != "SKU-PG" || got.Quantity() != 24 || got.Reference() != "repl-pg-1" {
		t.Fatalf("kind=%q site=%q sku=%q qty=%d ref=%q", got.WorkKind(), got.SiteId(), got.SKU(), got.Quantity(), got.Reference())
	}

	at := replAt.Add(time.Hour)
	if err := got.Cancel(at); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := repo.Save(ctx, got); err != nil {
		t.Fatalf("save cancelled: %v", err)
	}
	cancelled, err := repo.FindById(ctx, "replenish-repl-pg-1")
	if err != nil || cancelled.State() != workunit.Cancelled || !cancelled.CancelledAt().Equal(at) {
		t.Fatalf("cancelled round trip = (%v, %v)", cancelled, err)
	}
	if again, ok := cancelled.Replenishment(); !ok || again != want {
		t.Fatalf("the cancelled unit lost its replenishment: %+v", again)
	}
}

// The memory adapter and Postgres hand back the same unit for the same save
// (the repo's parity discipline: the memory clone rebuilds exactly like the
// Postgres rehydration).
func TestWorkUnitRepo_MemoryAndPostgresAgreeOnReplenishment(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	pg := postgres.NewWorkUnitRepo(pool)
	mem := memory.NewWorkUnitRepo()
	for _, repo := range []ports.WorkUnitRepo{pg, mem} {
		if err := repo.Save(ctx, replUnit(t, "repl-parity", "NORMAL")); err != nil {
			t.Fatalf("save: %v", err)
		}
	}
	fromPg, err := pg.FindById(ctx, "replenish-repl-parity")
	if err != nil {
		t.Fatalf("pg FindById: %v", err)
	}
	fromMem, err := mem.FindById(ctx, "replenish-repl-parity")
	if err != nil {
		t.Fatalf("mem FindById: %v", err)
	}
	pgMove, _ := fromPg.Replenishment()
	memMove, _ := fromMem.Replenishment()
	if pgMove != memMove || fromPg.SKU() != fromMem.SKU() || fromPg.Quantity() != fromMem.Quantity() || fromPg.WorkKind() != fromMem.WorkKind() || !fromPg.CPT().Time().Equal(fromMem.CPT().Time()) {
		t.Fatalf("adapters disagree: pg %+v / mem %+v", pgMove, memMove)
	}
}

// Migration 0015 is additive and reversible: rows written before it keep
// reading, its columns default to ”, and its down drops only its own four
// columns.
func TestMigration0015_UpAndDownAreAdditive(t *testing.T) {
	dsn := freshDatabase(t)
	ctx := context.Background()
	m, err := migrate.New("file://"+migrationsDir(t), dsn)
	if err != nil {
		t.Fatalf("open migrations: %v", err)
	}
	defer func() { _, _ = m.Close() }()
	if err := m.Migrate(14); err != nil {
		t.Fatalf("migrate to 14: %v", err)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, `INSERT INTO work_units (id, path_id, cpt, reference, state) VALUES ('wu-old', 'pick-a', now(), 'order-1', 'Pending')`); err != nil {
		t.Fatalf("insert a pre-0015 row: %v", err)
	}
	if columnsOf(t, conn, "replenishment_") != 0 {
		t.Fatal("the replenishment columns exist before 0015")
	}

	if err := m.Steps(1); err != nil {
		t.Fatalf("up 0015: %v", err)
	}
	if got := columnsOf(t, conn, "replenishment_"); got != 4 {
		t.Fatalf("replenishment columns after up = %d, want 4", got)
	}
	var priority string
	if err := conn.QueryRow(ctx, `SELECT replenishment_priority FROM work_units WHERE id = 'wu-old'`).Scan(&priority); err != nil || priority != "" {
		t.Fatalf("an old row must default to '' (got %q, %v)", priority, err)
	}

	if err := m.Steps(-1); err != nil {
		t.Fatalf("down 0015: %v", err)
	}
	if got := columnsOf(t, conn, "replenishment_"); got != 0 {
		t.Fatalf("replenishment columns after down = %d, want 0", got)
	}
	var kept int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM work_units WHERE id = 'wu-old'`).Scan(&kept); err != nil || kept != 1 {
		t.Fatalf("the down must not touch rows (%d, %v)", kept, err)
	}
	if got := columnsOf(t, conn, "relocation_"); got != 3 {
		t.Fatalf("relocation columns after down = %d, want the 0014 ones intact", got)
	}
}

func columnsOf(t *testing.T, conn *pgx.Conn, prefix string) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(context.Background(),
		`SELECT count(*) FROM information_schema.columns WHERE table_name = 'work_units' AND column_name LIKE $1`, prefix+"%").Scan(&n); err != nil {
		t.Fatalf("count columns: %v", err)
	}
	return n
}

type replPgFixture struct {
	requested *usecases.ApplyReplenishmentRequested
	cancelled *usecases.ApplyReplenishmentCancelled
	release   *usecases.ReleaseNextWork
	workUnits *postgres.WorkUnitRepo
}

func newReplPgFixture(t *testing.T, publisher ports.EventPublisher) replPgFixture {
	t.Helper()
	pool := outboxDB(t)
	uow := postgres.NewUnitOfWork(pool)
	workUnits := postgres.NewWorkUnitRepo(pool)
	pools := postgres.NewWorkPoolRepo(pool)
	processed := postgres.NewProcessedEventRepo(pool)
	clock := memory.FixedClock{At: replAt.Add(time.Minute)}
	catalogue := pathcatalog.New([]pathcatalog.PathDefinition{{Id: "REPLENISH", MatchPrefix: "replenish", RequiredCapabilities: []string{"replenish"}}})
	return replPgFixture{
		requested: usecases.NewApplyReplenishmentRequested(workUnits, pools, processed, catalogue, publisher, clock).WithUnitOfWork(uow),
		cancelled: usecases.NewApplyReplenishmentCancelled(workUnits, pools, processed, clock).WithUnitOfWork(uow),
		release:   usecases.NewReleaseNextWork(pools, workUnits, publisher, clock).WithUnitOfWork(uow),
		workUnits: workUnits,
	}
}

func replPgRequest(eventId, requestId, priority string) usecases.ApplyReplenishmentRequestedRequest {
	return usecases.ApplyReplenishmentRequestedRequest{
		EventId: eventId, OccurredAt: replAt, RequestId: requestId, SiteId: "WH1", SKU: "SKU-PG",
		FromBin: "WH1-RSV-A07-03-02-B", ToBin: "WH1-FWD-A01-01-01-A", Quantity: 24, Priority: priority, RequestedAt: replAt,
	}
}

// One unit per request id on a real database: a redelivery is caught by the
// processed mark, a DIFFERENT event for the same request by the unit's
// existence; URGENT is released ahead of NORMAL; a Pending unit is cancelled
// and leaves the pool, a released one is not.
func TestApplyReplenishment_OnPostgres(t *testing.T) {
	f := newReplPgFixture(t, events.NewLogPublisher(nil))
	ctx := context.Background()
	path := mustPath(t, "REPLENISH")

	if o, err := f.requested.Execute(ctx, replPgRequest("evt-n", "repl-n", "NORMAL")); err != nil || o != usecases.ReplenishmentPlanned {
		t.Fatalf("plan NORMAL = (%v, %v)", o, err)
	}
	if o, err := f.requested.Execute(ctx, replPgRequest("evt-u", "repl-u", "URGENT")); err != nil || o != usecases.ReplenishmentPlanned {
		t.Fatalf("plan URGENT = (%v, %v)", o, err)
	}
	if o, err := f.requested.Execute(ctx, replPgRequest("evt-n", "repl-n", "NORMAL")); err != nil || o != usecases.ReplenishmentAlreadyProcessed {
		t.Fatalf("redelivery = (%v, %v)", o, err)
	}
	if o, err := f.requested.Execute(ctx, replPgRequest("evt-n2", "repl-n", "URGENT")); err != nil || o != usecases.ReplenishmentAlreadyPlanned {
		t.Fatalf("same request, new event = (%v, %v)", o, err)
	}

	first, err := f.release.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: path})
	if err != nil || first.Id() != "replenish-repl-u" {
		t.Fatalf("first release = (%v, %v), want the URGENT request", first, err)
	}
	if o, err := f.cancelled.Execute(ctx, usecases.ApplyReplenishmentCancelledRequest{EventId: "evt-cu", OccurredAt: replAt, RequestId: "repl-u"}); err != nil || o != usecases.ReplenishmentNotCancellable {
		t.Fatalf("cancel released = (%v, %v)", o, err)
	}
	if o, err := f.cancelled.Execute(ctx, usecases.ApplyReplenishmentCancelledRequest{EventId: "evt-cn", OccurredAt: replAt, RequestId: "repl-n"}); err != nil || o != usecases.ReplenishmentPlanned {
		t.Fatalf("cancel pending = (%v, %v)", o, err)
	}
	for id, want := range map[string]workunit.State{"replenish-repl-u": workunit.Released, "replenish-repl-n": workunit.Cancelled} {
		if got, err := f.workUnits.FindById(ctx, id); err != nil || got.State() != want {
			t.Fatalf("%s = (%v, %v), want %v", id, got, err, want)
		}
	}
	if _, err := f.release.Execute(ctx, usecases.ReleaseNextWorkRequest{PathId: path}); err == nil {
		t.Fatal("the cancelled request must not be releasable")
	}
}

// The processed-event mark, the work unit, the pool entry and the outbox
// rows commit together or not at all: a failing encode rolls ALL of them
// back, and the retry then plans the request for real.
func TestApplyReplenishment_AtomicWithMarkPoolAndOutbox(t *testing.T) {
	pool := outboxDB(t)
	uow := postgres.NewUnitOfWork(pool)
	workUnits := postgres.NewWorkUnitRepo(pool)
	pools := postgres.NewWorkPoolRepo(pool)
	processed := postgres.NewProcessedEventRepo(pool)
	clock := memory.FixedClock{At: replAt.Add(time.Minute)}
	catalogue := pathcatalog.New([]pathcatalog.PathDefinition{{Id: "REPLENISH", MatchPrefix: "replenish"}})
	ctx := context.Background()

	failing := postgres.NewOutboxPublisher(pool, failingEncoder{err: errors.New("encode exploded")})
	bad := usecases.NewApplyReplenishmentRequested(workUnits, pools, processed, catalogue, failing, clock).WithUnitOfWork(uow)
	if _, err := bad.Execute(ctx, replPgRequest("evt-rb", "repl-rb", "NORMAL")); err == nil {
		t.Fatal("the encode failure must surface")
	}
	for table, where := range map[string]string{
		"processed_events":  "event_id = 'evt-rb'",
		"work_units":        "id = 'replenish-repl-rb'",
		"work_pool_entries": "work_unit_id = 'replenish-repl-rb'",
		"work_pools":        "path_id = 'REPLENISH'",
	} {
		if got := countRows(t, pool, table, where); got != 0 {
			t.Fatalf("%s kept %d row(s) of a rolled-back request", table, got)
		}
	}
	if got := countOutbox(t, pool, "true"); got != 0 {
		t.Fatalf("outbox has %d row(s), want 0", got)
	}

	integration, analytics := encoders(pool)
	good := usecases.NewApplyReplenishmentRequested(workUnits, pools, processed, catalogue, postgres.NewOutboxPublisher(pool, integration, analytics), clock).WithUnitOfWork(uow)
	if o, err := good.Execute(ctx, replPgRequest("evt-rb", "repl-rb", "NORMAL")); err != nil || o != usecases.ReplenishmentPlanned {
		t.Fatalf("retry = (%v, %v), want a real plan (the failed attempt left no mark)", o, err)
	}
	if countRows(t, pool, "processed_events", "event_id = 'evt-rb'") != 1 || countRows(t, pool, "work_units", "id = 'replenish-repl-rb'") != 1 || countOutbox(t, pool, "event_type LIKE '%WorkUnitCreated%'") == 0 {
		t.Fatal("the retry did not commit the mark, the unit and the outbox rows together")
	}
}
