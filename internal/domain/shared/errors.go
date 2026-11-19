// Package shared holds value objects, domain events, and domain errors
// shared across the WES domain: CPT, PathId, Quantity.
package shared

import "errors"

var (
	ErrInvalidQuantity = errors.New("quantity must be non-negative")
	ErrInvalidPathId   = errors.New("path id must not be empty")
)
