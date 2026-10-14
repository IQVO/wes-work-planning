// Package pathcatalog models the fleet's process-path catalogue as a
// small, in-memory lookup: which process-path FAMILIES are declared, and
// what capability set each one requires. The catalogue's CONTENT is
// configuration data (see warehouse-infra/config/process-paths/*.yaml) —
// this package only models its shape and the Lookup behavior every
// consumer needs. Loading the YAML itself is a separate concern (see the
// filecatalog adapter), kept out of the domain layer per this
// repository's hexagonal discipline (ADR-0001).
//
// This package mirrors fulfillment-execution's own pathcatalog package
// byte-for-byte in its matching semantics (both read the SAME published-
// language YAML file, so they must agree). Lookup is a case-insensitive
// PREFIX match, not an exact match: real path_id values across this
// fleet are not the bare canonical id ("PICK") — they carry a
// station/zone/scenario suffix chosen by whichever caller supplies them
// (order-management's shared.DefaultPathId is the bare "pick"; e2e test
// fixtures use "pick-zone-a", "pick-soak", "pick-t5-imbalance", and so
// on). A path's declared MatchPrefix (e.g. "pick") matches every one of
// those forms, because each one either equals the prefix exactly or
// starts with prefix + "-". See fulfillment-execution's ADR-0017 (and
// its addendum) for the full history of why an earlier exact-match
// version of this idea was wrong.
package pathcatalog

import (
	"errors"
	"slices"
	"strings"
)

// ErrUnknownPath is returned by Lookup when id does not match any
// declared path's prefix. Every consumer of the catalogue must treat
// this as a real error.
var ErrUnknownPath = errors.New("pathcatalog: unknown process path id")

// PathDefinition is one declared process path: its canonical id, the
// lower-cased prefix family of real path_id values it recognizes, the
// capability set a station must hold to work it, and its optional
// declared destination.
type PathDefinition struct {
	Id                   string
	MatchPrefix          string
	RequiredCapabilities []string
	// DestinationLocationRole is process-path-management's optional,
	// Define-time-only declaration of what kind of facility-layout
	// LocationRole this path's completed work is destined for (Drop,
	// WorkCenter, or Shipping — see process-path-management's own
	// ADR-0009). The empty string means "not declared", the default and
	// most common case — mirrored here verbatim from the upstream event
	// payload, never validated or defaulted by this service, exactly
	// like MatchPrefix and RequiredCapabilities are carried as declared.
	// Available for a future consumer (e.g. destination-aware release
	// ordering) to use; this field alone adds no such logic.
	DestinationLocationRole string
	// Intake is process-path-management's declared set of demand sources
	// allowed to route work onto this path (its ADR 0034, fleet decision
	// E5): CUSTOMER_ORDER, STOCK_TRANSFER, SLOT_RELOCATION,
	// INVENTORY_CONTROL. Empty means "not declared" (an older producer, or
	// a path reached only by explicit id). Mirrored verbatim.
	Intake []string
}

// IntakeSlotRelocation is the intake value of approved slot plans
// (ADR-0040).
const IntakeSlotRelocation = "SLOT_RELOCATION"

// Admits reports whether the path accepts work from demand source: true
// when the path declares no intake at all (routed by explicit id) or when
// its intake lists source.
func (d PathDefinition) Admits(source string) bool {
	return len(d.Intake) == 0 || slices.Contains(d.Intake, source)
}

// Catalogue is the validated, in-memory set of a building's declared
// process paths.
type Catalogue struct {
	defs []PathDefinition
}

// New builds a Catalogue from defs. Callers (typically a loader adapter)
// are responsible for ensuring defs is non-empty and each definition is
// well-formed — New itself does not re-validate, since the loader is
// expected to reject a malformed source before ever calling this
// constructor (see filecatalog.Load's doc comment for the boot-time
// failure contract).
func New(defs []PathDefinition) *Catalogue {
	out := make([]PathDefinition, len(defs))
	copy(out, defs)
	return &Catalogue{defs: out}
}

// Lookup returns the declared definition whose MatchPrefix matches id
// (case-insensitively: id equals the prefix, or id starts with
// prefix + "-"), or ErrUnknownPath if no declared path recognizes id.
// When more than one declared prefix would match, the LONGEST matching
// prefix wins, so a more specific declaration always takes precedence
// over a more general one.
func (c *Catalogue) Lookup(id string) (PathDefinition, error) {
	lower := strings.ToLower(id)

	var best PathDefinition
	bestLen := -1
	for _, d := range c.defs {
		prefix := strings.ToLower(d.MatchPrefix)
		if prefix == "" {
			continue
		}
		if !matchesPrefix(lower, prefix) {
			continue
		}
		if len(prefix) > bestLen {
			best = d
			bestLen = len(prefix)
		}
	}
	if bestLen == -1 {
		return PathDefinition{}, ErrUnknownPath
	}
	return best, nil
}

// matchesPrefix reports whether id (already lower-cased) belongs to
// prefix's family: either id equals prefix exactly, or id starts with
// prefix followed by a "-" separator. Deliberately does NOT match a bare
// substring prefix without the separator.
func matchesPrefix(id, prefix string) bool {
	if id == prefix {
		return true
	}
	return strings.HasPrefix(id, prefix+"-")
}

// Ids returns every declared path's canonical id, in no particular
// order.
func (c *Catalogue) Ids() []string {
	out := make([]string, 0, len(c.defs))
	for _, d := range c.defs {
		out = append(out, d.Id)
	}
	return out
}
