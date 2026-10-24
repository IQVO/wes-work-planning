package productclassificationview

import "time"

// PackLevelRecord is one row of the local pack-level copy: product-master's
// level exactly as announced, keyed by GTIN (a GTIN identifies one (sku,
// level) fleet-wide, contracts G7). A retired level keeps its row with
// EffectiveTo set, so its GTIN stays known; only an ACTIVE row is surfaced
// as PackFacts.
type PackLevelRecord struct {
	SKU          string
	Level        PackLevel
	GTIN         string
	ChildGTIN    string
	QtyOfChild   int
	UnitsPerPack int
	Declared     Dims
	// Measured is the latest ProductPackLevelMeasured dimensions; the zero
	// value means none was received.
	Measured      Dims
	IsOrderable   bool
	IsDespatch    bool
	EffectiveFrom time.Time
	// EffectiveTo is the zero time while the level is active.
	EffectiveTo time.Time
	Version     int64
}

// Active reports whether the level has not been end-dated.
func (r PackLevelRecord) Active() bool { return r.EffectiveTo.IsZero() }

// Facts maps an active record to the facts this context surfaces: the
// measured dimensions win over the declared ones when a measurement exists.
func (r PackLevelRecord) Facts() PackFacts {
	dims, measured := r.Declared, false
	if r.Measured.Known() {
		dims, measured = r.Measured, true
	}
	return PackFacts{Level: r.Level, GTIN: r.GTIN, UnitsPerPack: r.UnitsPerPack, Dims: dims, Measured: measured}
}
