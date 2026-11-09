package workunit

import "time"

// Cursor is the keyset position a paged work-unit query resumes after: the
// last returned work unit's CPT and id (ordering is (cpt, id)).
type Cursor struct {
	CPT time.Time
	ID  string
}
