package productclassificationview

// PackLevel is a product-master pack level (contracts G6). This context uses
// EACH and CASE only; INNER and PALLET rows are stored but never surfaced.
type PackLevel string

const (
	PackLevelEach   PackLevel = "EACH"
	PackLevelInner  PackLevel = "INNER"
	PackLevelCase   PackLevel = "CASE"
	PackLevelPallet PackLevel = "PALLET"
)

// Valid reports whether l is one of the four product-master pack levels.
func (l PackLevel) Valid() bool {
	switch l {
	case PackLevelEach, PackLevelInner, PackLevelCase, PackLevelPallet:
		return true
	default:
		return false
	}
}

// Dims is the size and weight of ONE pack of a level, in millimetres and
// grams, exactly as product-master publishes them. A zero member means the
// dimension is not known.
type Dims struct {
	LengthMm int
	WidthMm  int
	HeightMm int
	WeightG  int
}

// Known reports whether every dimension is present (cube needs all three).
func (d Dims) Known() bool {
	return d.LengthMm > 0 && d.WidthMm > 0 && d.HeightMm > 0
}

// CubeMm3 is length x width x height in cubic millimetres; 0 when any side
// is unknown.
func (d Dims) CubeMm3() int64 {
	if !d.Known() {
		return 0
	}
	return int64(d.LengthMm) * int64(d.WidthMm) * int64(d.HeightMm)
}

// PackFacts is what this context holds about one ACTIVE pack level of a SKU
// (ADR-0045): the factor to eaches and the size of one pack. Dims are the
// measured dimensions when product-master published a measurement, else the
// declared ones (the declared-versus-measured split of product-master ADR
// 0002: a measurement wins).
type PackFacts struct {
	Level        PackLevel
	GTIN         string
	UnitsPerPack int
	Dims         Dims
	// Measured reports that Dims come from a ProductPackLevelMeasured event.
	Measured bool
}

// CubeMm3 is the cube of one pack, 0 when unknown.
func (f PackFacts) CubeMm3() int64 { return f.Dims.CubeMm3() }

// WeightG is the weight of one pack in grams, 0 when unknown.
func (f PackFacts) WeightG() int { return f.Dims.WeightG }
