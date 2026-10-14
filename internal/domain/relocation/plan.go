// Package relocation plans the floor work of an approved slot plan
// (ADR-0040): one RELOCATE work unit per move, the order the moves must run
// in, and whether an incoming plan supersedes the site's current one. It is
// pure policy; the work units themselves are workunit.WorkUnit aggregates.
package relocation

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ErrInvalidPlan marks an approved slot plan that cannot be planned: no plan
// id, no site, no approval time, or a move without a SKU. Deterministic: a
// retry can never fix it.
var ErrInvalidPlan = errors.New("relocation: invalid slot plan")

// MoveInput is one move as slotting-optimization publishes it.
type MoveInput struct {
	SKU      string
	Kind     string
	FromSlot string
	ToSlot   string
}

// Move is one validated move of a plan: the SKU and the move value its
// RELOCATE work unit carries.
type Move struct {
	SKU  string
	Move workunit.RelocationMove
}

// Plan is an approved slot plan, validated, with its moves numbered by
// their 0-based position in the published `moves` array.
type Plan struct {
	id               string
	siteId           string
	approvedAt       time.Time
	supersedesPlanId string
	moves            []Move
}

// NewPlan validates an approved plan and its moves.
func NewPlan(planId, siteId string, approvedAt time.Time, supersedesPlanId string, inputs []MoveInput) (*Plan, error) {
	p := &Plan{
		id:               strings.TrimSpace(planId),
		siteId:           strings.TrimSpace(siteId),
		approvedAt:       approvedAt,
		supersedesPlanId: strings.TrimSpace(supersedesPlanId),
	}
	if err := p.validateHeader(); err != nil {
		return nil, err
	}
	moves, err := buildMoves(p.id, inputs)
	if err != nil {
		return nil, err
	}
	p.moves = moves
	return p, nil
}

func (p *Plan) validateHeader() error {
	if p.id == "" {
		return fmt.Errorf("%w: plan_id is required", ErrInvalidPlan)
	}
	if p.siteId == "" {
		return fmt.Errorf("%w: site_id is required", ErrInvalidPlan)
	}
	if p.approvedAt.IsZero() {
		return fmt.Errorf("%w: approved_at is required", ErrInvalidPlan)
	}
	return nil
}

func buildMoves(planId string, inputs []MoveInput) ([]Move, error) {
	moves := make([]Move, 0, len(inputs))
	for seq, in := range inputs {
		sku := strings.TrimSpace(in.SKU)
		if sku == "" {
			return nil, fmt.Errorf("%w: move %d has no sku", ErrInvalidPlan, seq)
		}
		m, err := workunit.NewRelocationMove(planId, seq, in.Kind, in.FromSlot, in.ToSlot)
		if err != nil {
			return nil, fmt.Errorf("move %d: %w", seq, err)
		}
		moves = append(moves, Move{SKU: sku, Move: m})
	}
	return moves, nil
}

func (p *Plan) Id() string               { return p.id }
func (p *Plan) SiteId() string           { return p.siteId }
func (p *Plan) ApprovedAt() time.Time    { return p.approvedAt }
func (p *Plan) SupersedesPlanId() string { return p.supersedesPlanId }

// Moves returns a copy of the plan's moves in move_seq order.
func (p *Plan) Moves() []Move {
	out := make([]Move, len(p.moves))
	copy(out, p.moves)
	return out
}

// Receipt is what this context records about an applied plan.
func (p *Plan) Receipt() Receipt {
	return Receipt{
		PlanId:           p.id,
		SiteId:           p.siteId,
		ApprovedAt:       p.approvedAt,
		SupersedesPlanId: p.supersedesPlanId,
		MoveCount:        len(p.moves),
	}
}

// WorkUnitId is the deterministic id of a plan's move work unit:
// "<plan_id>-move-<move_seq>".
func WorkUnitId(planId string, moveSeq int) string {
	return fmt.Sprintf("%s-move-%d", planId, moveSeq)
}

// Predecessors returns, for every move that must wait, the move_seq of the
// move it waits on (ADR-0040): move B waits on move A when A vacates the
// slot B moves into (A.from_slot == B.to_slot, A != B). When several moves
// vacate one slot, the lowest move_seq is the one waited on. A dependency
// cycle (a swap) is broken at its lowest move_seq, which then waits on
// nothing. Every move has at most one predecessor.
func (p *Plan) Predecessors() map[int]int {
	vacaters := lowestVacaterBySlot(p.moves)
	pred := make(map[int]int)
	for _, m := range p.moves {
		a, ok := vacaters[m.Move.ToSlot]
		if ok && a != m.Move.MoveSeq {
			pred[m.Move.MoveSeq] = a
		}
	}
	breakCycles(len(p.moves), pred)
	return pred
}

// lowestVacaterBySlot maps every from_slot to the lowest move_seq leaving it.
func lowestVacaterBySlot(moves []Move) map[string]int {
	out := make(map[string]int)
	for _, m := range moves {
		slot := m.Move.FromSlot
		if slot == "" {
			continue
		}
		if _, seen := out[slot]; !seen {
			out[slot] = m.Move.MoveSeq
		}
	}
	return out
}

// Visit states of the cycle search over the predecessor graph.
const (
	unvisited = iota
	onPath
	done
)

// breakCycles removes, from every cycle of the functional graph pred (each
// node has at most one outgoing edge), the edge of the cycle's lowest node.
func breakCycles(n int, pred map[int]int) {
	state := make([]int, n)
	for start := 0; start < n; start++ {
		if state[start] != unvisited {
			continue
		}
		path := walk(start, pred, state)
		if entry, ok := cycleEntry(path, pred); ok {
			delete(pred, slices.Min(path[entry:]))
		}
		for _, node := range path {
			state[node] = done
		}
	}
}

// walk follows pred from start through unvisited nodes, marking each onPath,
// and returns the nodes visited in order (never empty: start is unvisited).
func walk(start int, pred map[int]int, state []int) []int {
	var path []int
	cur := start
	for state[cur] == unvisited {
		state[cur] = onPath
		path = append(path, cur)
		next, ok := pred[cur]
		if !ok {
			break
		}
		cur = next
	}
	return path
}

// cycleEntry reports where a cycle closes on path: the last node's
// predecessor is a node of path itself. A walk that ended on a node without
// a predecessor, or ran into a node an earlier walk finished, has no cycle.
func cycleEntry(path []int, pred map[int]int) (int, bool) {
	next, ok := pred[path[len(path)-1]]
	if !ok {
		return 0, false
	}
	for i, node := range path {
		if node == next {
			return i, true
		}
	}
	return 0, false
}

// Receipt records one applied plan of a site.
type Receipt struct {
	PlanId           string
	SiteId           string
	ApprovedAt       time.Time
	SupersedesPlanId string
	MoveCount        int
}

// Supersedes reports whether r replaces current as the site's plan: a later
// approval time wins; on a tie the greater plan id wins, so the order is
// total and independent of delivery order.
func (r Receipt) Supersedes(current Receipt) bool {
	if !r.ApprovedAt.Equal(current.ApprovedAt) {
		return r.ApprovedAt.After(current.ApprovedAt)
	}
	return r.PlanId > current.PlanId
}

// Decision is what to do with an incoming plan.
type Decision int

const (
	// Apply: plan the incoming plan's moves, cancelling every not-yet-
	// released move of the site's other plans.
	Apply Decision = iota
	// AlreadyApplied: the same plan was applied before (another event id).
	AlreadyApplied
	// Stale: the site already has a newer plan; record and plan nothing.
	Stale
)

// Decide compares an incoming plan with the latest plan recorded for its
// site (nil when the site has none).
func Decide(incoming Receipt, latest *Receipt) Decision {
	if latest == nil {
		return Apply
	}
	if latest.PlanId == incoming.PlanId {
		return AlreadyApplied
	}
	if incoming.Supersedes(*latest) {
		return Apply
	}
	return Stale
}
