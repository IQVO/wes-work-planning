package workunit

import (
	"fmt"
	"strings"
)

// MaxReplenishmentRequestIdLen bounds the request id: fulfillment-execution
// reports it as TaskCompleted.request_ref, which inventory-storage stores as
// the RelocateStock source_ref (at most 200 characters, ADR-0042).
const MaxReplenishmentRequestIdLen = 200

// ReplenishmentPriority is the urgency pick-face-replenishment stamped on a
// request, spelled exactly like its ReplenishmentRequested `priority`
// (ADR-0042) and like WorkReleased.data.replenishment.priority.
type ReplenishmentPriority string

const (
	// ReplenishmentPriorityNormal is a request detected while the pick face
	// still had usable stock.
	ReplenishmentPriorityNormal ReplenishmentPriority = "NORMAL"
	// ReplenishmentPriorityUrgent is a request detected with no usable
	// stock left in the pick face.
	ReplenishmentPriorityUrgent ReplenishmentPriority = "URGENT"
)

// String returns the exact wire form.
func (p ReplenishmentPriority) String() string { return string(p) }

// ParseReplenishmentPriority validates the wire form of a priority (exact,
// case-sensitive). Anything else is ErrUnknownReplenishmentPriority.
func ParseReplenishmentPriority(value string) (ReplenishmentPriority, error) {
	p := ReplenishmentPriority(value)
	if p == ReplenishmentPriorityNormal || p == ReplenishmentPriorityUrgent {
		return p, nil
	}
	return "", fmt.Errorf("%w: %q", ErrUnknownReplenishmentPriority, value)
}

// ReplenishmentMove is the pick-face replenishment a REPLENISH work unit
// executes (ADR-0042): move Quantity units of SKU from the reserve bin
// FromBin to the pick face ToBin, for the pick-face-replenishment request
// RequestId. Unlike a slot-plan RelocationMove no side is left open: both
// bins and the quantity are planned, the operator only confirms them.
type ReplenishmentMove struct {
	RequestId string
	SKU       string
	FromBin   string
	ToBin     string
	Quantity  int
	Priority  ReplenishmentPriority
}

// NewReplenishmentMove validates and builds a move. Every string is trimmed.
// The request id, SKU and both bins are required, the bins must differ, the
// quantity is at least 1 and the priority is NORMAL or URGENT.
func NewReplenishmentMove(requestId, sku, fromBin, toBin string, quantity int, priority string) (ReplenishmentMove, error) {
	p, err := ParseReplenishmentPriority(strings.TrimSpace(priority))
	if err != nil {
		return ReplenishmentMove{}, err
	}
	m := ReplenishmentMove{
		RequestId: strings.TrimSpace(requestId),
		SKU:       strings.TrimSpace(sku),
		FromBin:   strings.TrimSpace(fromBin),
		ToBin:     strings.TrimSpace(toBin),
		Quantity:  quantity,
		Priority:  p,
	}
	if err := m.validate(); err != nil {
		return ReplenishmentMove{}, err
	}
	return m, nil
}

func (m ReplenishmentMove) validate() error {
	if m.RequestId == "" {
		return fmt.Errorf("%w: request_id is required", ErrInvalidReplenishmentMove)
	}
	if len(m.RequestId) > MaxReplenishmentRequestIdLen {
		return fmt.Errorf("%w: request_id is longer than %d characters", ErrInvalidReplenishmentMove, MaxReplenishmentRequestIdLen)
	}
	if m.SKU == "" {
		return fmt.Errorf("%w: sku is required", ErrInvalidReplenishmentMove)
	}
	if m.FromBin == "" {
		return fmt.Errorf("%w: source bin is required", ErrInvalidReplenishmentMove)
	}
	if m.ToBin == "" {
		return fmt.Errorf("%w: destination bin is required", ErrInvalidReplenishmentMove)
	}
	if m.FromBin == m.ToBin {
		return fmt.Errorf("%w: source and destination bin are both %q", ErrInvalidReplenishmentMove, m.FromBin)
	}
	if m.Quantity < 1 {
		return fmt.Errorf("%w: quantity must be at least 1, got %d", ErrInvalidReplenishmentMove, m.Quantity)
	}
	return nil
}

// ReplenishmentWorkUnitId is the deterministic id of the work unit planned
// for a request: "replenish-<request_id>". One request, one id: a second
// event for the same request can only ever address the same unit.
func ReplenishmentWorkUnitId(requestId string) string {
	return "replenish-" + strings.TrimSpace(requestId)
}
