package release

import "github.com/claudioed/wes-work-planning/internal/domain/shared"

// FeedMode distinguishes release-fed pools (WES controls admission volume via
// a WIP limit) from flow-fed pools (priority only; backlog is watched against
// an alarm threshold instead).
type FeedMode int

const (
	ReleaseFed FeedMode = iota
	FlowFed
)

// String is the persisted form of a FeedMode (inverse of ParseFeedMode).
func (m FeedMode) String() string {
	switch m {
	case ReleaseFed:
		return "ReleaseFed"
	case FlowFed:
		return "FlowFed"
	default:
		return "Unknown"
	}
}

// ParseFeedMode validates and converts the persisted string form of a
// FeedMode. Anything other than exactly "ReleaseFed" or "FlowFed" —
// including "" — is rejected with ErrUnknownFeedMode rather than silently
// defaulting to a mode.
func ParseFeedMode(value string) (FeedMode, error) {
	switch value {
	case ReleaseFed.String():
		return ReleaseFed, nil
	case FlowFed.String():
		return FlowFed, nil
	default:
		return ReleaseFed, ErrUnknownFeedMode
	}
}

type entryState int

const (
	pending entryState = iota
	released
	completed
)

// Persisted forms of a pool entry's state.
const (
	entryStatePending   = "pending"
	entryStateReleased  = "released"
	entryStateCompleted = "completed"
)

// ParseEntryState validates the persisted string form of a pool entry state
// ("pending", "released", "completed") and returns it as the
// (isReleased, isCompleted) pair RestoreEntry takes. Anything else —
// including "" — is rejected with ErrUnknownEntryState rather than being
// silently rehydrated as pending.
func ParseEntryState(value string) (isReleased, isCompleted bool, err error) {
	switch value {
	case entryStatePending:
		return false, false, nil
	case entryStateReleased:
		return true, false, nil
	case entryStateCompleted:
		return true, true, nil
	default:
		return false, false, ErrUnknownEntryState
	}
}

type poolEntry struct {
	workUnitId string
	cpt        shared.CPT
	state      entryState
}

// WorkPool is the queue for one process path: backlog depth, arrival rate,
// service rate live here as behavior over enqueued entries. It hands out
// work in priority order (earliest CPT first), at most once per entry. The
// WIP limit is an enforceable invariant on release-fed pools; it is only an
// alarm threshold on flow-fed pools.
type WorkPool struct {
	pathId         shared.PathId
	mode           FeedMode
	wipLimit       int // only enforced when mode == ReleaseFed
	alarmThreshold int // only informative when mode == FlowFed
	entries        []poolEntry
	// version is the persisted optimistic-concurrency version (0 = never
	// saved). A repository's Save must only succeed against the version it
	// loaded -- see ports.ErrConcurrentModification.
	version int64
}

// NewWorkPool constructs an empty WorkPool for a path.
func NewWorkPool(pathId shared.PathId, mode FeedMode, wipLimit, alarmThreshold int) *WorkPool {
	return &WorkPool{pathId: pathId, mode: mode, wipLimit: wipLimit, alarmThreshold: alarmThreshold}
}

func (p *WorkPool) PathId() shared.PathId { return p.pathId }
func (p *WorkPool) Mode() FeedMode        { return p.mode }
func (p *WorkPool) WIPLimit() int         { return p.wipLimit }
func (p *WorkPool) AlarmThreshold() int   { return p.alarmThreshold }

// PoolEntrySnapshot is a read-only projection of one pool entry, for
// adapters that need to persist or display pool contents.
type PoolEntrySnapshot struct {
	WorkUnitId string
	CPT        shared.CPT
	Released   bool
	Completed  bool
}

// Entries returns a snapshot of every entry currently in the pool.
func (p *WorkPool) Entries() []PoolEntrySnapshot {
	out := make([]PoolEntrySnapshot, len(p.entries))
	for i, e := range p.entries {
		out[i] = PoolEntrySnapshot{
			WorkUnitId: e.workUnitId,
			CPT:        e.cpt,
			Released:   e.state == released || e.state == completed,
			Completed:  e.state == completed,
		}
	}
	return out
}

// Enqueue adds a work unit to the pool as pending. Fails if already present.
func (p *WorkPool) Enqueue(workUnitId string, cpt shared.CPT) error {
	for _, e := range p.entries {
		if e.workUnitId == workUnitId {
			return ErrDuplicateEntry
		}
	}
	p.entries = append(p.entries, poolEntry{workUnitId: workUnitId, cpt: cpt, state: pending})
	return nil
}

// Configure sets the pool's feed mode and WIP limit (the ConfigurePool
// command, ADR-0034). It reports whether anything changed, so an identical
// repeat is a no-op success (idempotent).
//
// The limit must be a positive integer and the mode a known FeedMode;
// otherwise the pool is left untouched. Configure NEVER evicts or cancels
// work: entries are not touched at all. Lowering the limit below the
// current WIP simply leaves the pool saturated -- Release/ReleaseNext
// already refuse while WIP() >= wipLimit -- so releases pause until enough
// work completes for WIP < limit. Raising the limit takes effect on the
// very next release. The alarm threshold is not part of this command.
func (p *WorkPool) Configure(mode FeedMode, wipLimit int) (changed bool, err error) {
	if mode != ReleaseFed && mode != FlowFed {
		return false, ErrUnknownFeedMode
	}
	if wipLimit <= 0 {
		return false, ErrInvalidWIPLimit
	}
	if p.mode == mode && p.wipLimit == wipLimit {
		return false, nil
	}
	p.mode = mode
	p.wipLimit = wipLimit
	return true, nil
}

// BacklogDepth is the count of pending (not yet released) entries.
func (p *WorkPool) BacklogDepth() int {
	count := 0
	for _, e := range p.entries {
		if e.state == pending {
			count++
		}
	}
	return count
}

// WIP is the count of released-but-not-yet-completed (outstanding) entries.
// A completed entry no longer occupies a WIP slot — see Complete below —
// so a release-fed pool's admission ceiling reflects live outstanding work,
// not the lifetime total ever released.
func (p *WorkPool) WIP() int {
	count := 0
	for _, e := range p.entries {
		if e.state == released {
			count++
		}
	}
	return count
}

// IsOverAlarmThreshold reports whether backlog depth exceeds the flow-fed
// alarm threshold. Meaningless for release-fed pools, which enforce the WIP
// limit as a hard invariant instead.
func (p *WorkPool) IsOverAlarmThreshold() bool {
	return p.BacklogDepth() > p.alarmThreshold
}

// ReleaseNext hands out the highest-priority pending entry (earliest CPT).
// On a release-fed pool, releasing beyond the WIP limit fails — this is the
// enforced invariant. Each entry can be released at most once.
func (p *WorkPool) ReleaseNext() (string, error) {
	idx := p.nextPendingIndex()
	if idx == -1 {
		return "", ErrEmptyPool
	}
	if p.mode == ReleaseFed && p.WIP() >= p.wipLimit {
		return "", ErrWIPLimitReached
	}
	p.entries[idx].state = released
	return p.entries[idx].workUnitId, nil
}

// nextPendingIndex finds the pending entry with the earliest (most urgent)
// CPT.
func (p *WorkPool) nextPendingIndex() int {
	best := -1
	for i, e := range p.entries {
		if e.state != pending {
			continue
		}
		if best == -1 || e.cpt.Before(p.entries[best].cpt) {
			best = i
		}
	}
	return best
}

// Release marks a specific entry as released. Fails if unknown or already
// released — enforcing at-most-once handout.
func (p *WorkPool) Release(workUnitId string) error {
	for i, e := range p.entries {
		if e.workUnitId != workUnitId {
			continue
		}
		if e.state == released || e.state == completed {
			return ErrAlreadyReleased
		}
		if p.mode == ReleaseFed && p.WIP() >= p.wipLimit {
			return ErrWIPLimitReached
		}
		p.entries[i].state = released
		return nil
	}
	return ErrUnknownEntry
}

// RemainingCapacity reports how many more units this pool can admit right
// now, and whether that figure means anything (see ADR-0018).
//
// Known is false — and remaining is always 0 — for a FlowFed pool: FlowFed
// pools have no hard admission ceiling, only an alarmThreshold, which is a
// backlog alarm, not a capacity figure (see ADR-0003's flow-balancing
// rationale: a flow-fed path "cannot refuse arrivals — a conveyor does not
// ask permission"). Reporting a number derived from alarmThreshold here
// would misrepresent a soft alarm as a hard ceiling to a downstream
// consumer (order-management's promise-window calculation) that treats
// "known" capacity as a real constraint to plan against.
//
// Known is also false when wipLimit is unset/zero on a ReleaseFed pool
// (never provisioned for admission control), since a "remaining capacity
// of 0" would be indistinguishable from a genuinely saturated pool.
//
// remaining is never negative (ADR-0018: max(0, wipLimit - WIP)). Release
// and ReleaseNext refuse to admit past wipLimit (ErrWIPLimitReached), but
// RestoreEntry deliberately rehydrates a pool exactly as stored even when
// the limit has since been lowered below the current WIP — so WIP() CAN
// exceed wipLimit, and the figure is clamped to 0 ("saturated") rather than
// reported as a negative number of admissible units.
//
// The WIP limit is enforced pool-wide, not sub-allocated per CPT bucket
// (WorkPool has no notion of a per-CPT admission ceiling — see
// ChargeForecast for the CPT-bucketed side of the picture, which models
// demand, not supply). RemainingCapacity therefore answers "how much more
// can this path admit right now" independent of which CPT the caller is
// asking about; the caller supplies the CPT purely to identify which
// cutoff window the reported figure is being correlated against
// (ADR-0018), not to select a different capacity number per CPT.
func (p *WorkPool) RemainingCapacity() (remaining int, known bool) {
	if p.mode != ReleaseFed || p.wipLimit <= 0 {
		return 0, false
	}
	return max(0, p.wipLimit-p.WIP()), true
}

// Complete marks a released entry as completed, freeing its WIP slot on a
// release-fed pool. This is the missing half of the release/complete cycle:
// without it, a release-fed pool's WIP count only ever rises (each entry is
// released at most once and never leaves WIP), so the pool permanently
// wedges shut once wipLimit entries have EVER been released — regardless of
// how much of that work downstream has since finished. Idempotent: calling
// Complete twice on an already-completed entry is a no-op success, since a
// redelivered TaskCompleted event must not error the consumer.
func (p *WorkPool) Complete(workUnitId string) error {
	for i, e := range p.entries {
		if e.workUnitId != workUnitId {
			continue
		}
		if e.state == completed {
			return nil
		}
		if e.state != released {
			return ErrNotReleased
		}
		p.entries[i].state = completed
		return nil
	}
	return ErrUnknownEntry
}

// Version is the persisted optimistic-concurrency version this pool was
// loaded at (0 for a pool that has never been saved).
func (p *WorkPool) Version() int64 { return p.version }

// SetVersion is for repositories only: it records the version a pool was
// rehydrated at, or the new version after a successful Save.
func (p *WorkPool) SetVersion(v int64) { p.version = v }

// RestoreEntry re-adds a persisted entry exactly as stored, for
// repositories rehydrating a pool. Unlike Enqueue + Release it does not
// re-check the WIP limit: rehydration must reproduce what is stored, even
// if the limit has since been lowered below the current WIP.
func (p *WorkPool) RestoreEntry(workUnitId string, cpt shared.CPT, isReleased, isCompleted bool) error {
	for _, e := range p.entries {
		if e.workUnitId == workUnitId {
			return ErrDuplicateEntry
		}
	}
	st := pending
	switch {
	case isCompleted:
		st = completed
	case isReleased:
		st = released
	}
	p.entries = append(p.entries, poolEntry{workUnitId: workUnitId, cpt: cpt, state: st})
	return nil
}

// Reconcile moves an entry FORWARD to match its work unit's authoritative
// lifecycle (the WorkUnit aggregate, not the pool, is the source of truth
// for a unit's state). It never moves an entry backwards and is a no-op
// when the entry already agrees. Used to heal a pool entry left behind by
// a lost update, and by completion to finish an entry that missed its
// release transition.
func (p *WorkPool) Reconcile(workUnitId string, unitReleased, unitCompleted bool) error {
	for i, e := range p.entries {
		if e.workUnitId != workUnitId {
			continue
		}
		p.entries[i].state = reconciledState(e.state, unitReleased, unitCompleted)
		return nil
	}
	return ErrUnknownEntry
}

// reconciledState is the forward-only merge of an entry's stored state with
// its work unit's lifecycle: completion always wins, a pending entry follows
// a released unit, and nothing ever moves backwards.
func reconciledState(stored entryState, unitReleased, unitCompleted bool) entryState {
	if unitCompleted {
		return completed
	}
	if unitReleased && stored == pending {
		return released
	}
	return stored
}
