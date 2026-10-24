package productclassificationcopy

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
)

// selectActivePacksSQL reads the active EACH / CASE levels of a SKU, the
// highest version last so it wins if an out-of-order history left two active
// rows for one level. Served by idx_product_pack_level_copy_active.
const selectActivePacksSQL = `
	SELECT sku, level, gtin, units_per_pack,
		length_mm, width_mm, height_mm, weight_g,
		measured_length_mm, measured_width_mm, measured_height_mm, measured_weight_g,
		version
	FROM product_pack_level_copy
	WHERE sku = $1 AND level IN ('EACH', 'CASE') AND effective_to IS NULL
	ORDER BY version`

const upsertPackDeclaredSQL = `
	INSERT INTO product_pack_level_copy (gtin, sku, level, child_gtin, qty_of_child, units_per_pack,
		length_mm, width_mm, height_mm, weight_g, is_orderable, is_despatch_unit,
		effective_from, effective_to, version, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, now())
	ON CONFLICT (gtin) DO UPDATE SET
		sku = EXCLUDED.sku, level = EXCLUDED.level, child_gtin = EXCLUDED.child_gtin,
		qty_of_child = EXCLUDED.qty_of_child, units_per_pack = EXCLUDED.units_per_pack,
		length_mm = EXCLUDED.length_mm, width_mm = EXCLUDED.width_mm,
		height_mm = EXCLUDED.height_mm, weight_g = EXCLUDED.weight_g,
		is_orderable = EXCLUDED.is_orderable, is_despatch_unit = EXCLUDED.is_despatch_unit,
		effective_from = EXCLUDED.effective_from, effective_to = EXCLUDED.effective_to,
		version = EXCLUDED.version, updated_at = now()
	WHERE product_pack_level_copy.version < EXCLUDED.version`

const retirePackSQL = `
	UPDATE product_pack_level_copy
	SET effective_to = $2, version = $3, updated_at = now()
	WHERE gtin = $1 AND version < $3`

const measurePackSQL = `
	UPDATE product_pack_level_copy
	SET measured_length_mm = $2, measured_width_mm = $3, measured_height_mm = $4,
		measured_weight_g = $5, version = $6, updated_at = now()
	WHERE gtin = $1 AND version < $6`

// ApplyDeclared implements ports.PackLevelCopyRepo with ONE statement: insert
// when absent, overwrite only when the stored version is lower. The measured
// columns are not in the SET list, so a re-declaration keeps the measurement.
func (s *Store) ApplyDeclared(ctx context.Context, rec productclassificationview.PackLevelRecord) (bool, error) {
	tag, err := s.querier(ctx).Exec(ctx, upsertPackDeclaredSQL,
		rec.GTIN, rec.SKU, string(rec.Level), nullableText(rec.ChildGTIN), nullableInt(rec.QtyOfChild), rec.UnitsPerPack,
		nullableInt(rec.Declared.LengthMm), nullableInt(rec.Declared.WidthMm), nullableInt(rec.Declared.HeightMm), nullableInt(rec.Declared.WeightG),
		rec.IsOrderable, rec.IsDespatch, rec.EffectiveFrom.UTC(), nullableTime(rec.EffectiveTo), rec.Version)
	if err != nil {
		return false, fmt.Errorf("pack level copy: declare %s: %w", rec.GTIN, err)
	}
	return tag.RowsAffected() == 1, nil
}

// ApplyRetired implements ports.PackLevelCopyRepo.
func (s *Store) ApplyRetired(ctx context.Context, gtin string, effectiveTo time.Time, version int64) (bool, error) {
	tag, err := s.querier(ctx).Exec(ctx, retirePackSQL, gtin, effectiveTo.UTC(), version)
	if err != nil {
		return false, fmt.Errorf("pack level copy: retire %s: %w", gtin, err)
	}
	return tag.RowsAffected() == 1, nil
}

// ApplyMeasured implements ports.PackLevelCopyRepo.
func (s *Store) ApplyMeasured(ctx context.Context, gtin string, dims productclassificationview.Dims, version int64) (bool, error) {
	tag, err := s.querier(ctx).Exec(ctx, measurePackSQL, gtin,
		dims.LengthMm, dims.WidthMm, dims.HeightMm, dims.WeightG, version)
	if err != nil {
		return false, fmt.Errorf("pack level copy: measure %s: %w", gtin, err)
	}
	return tag.RowsAffected() == 1, nil
}

// scanPackLevel maps one selectActivePacksSQL row. NULL dimensions rehydrate as
// 0 (unknown).
func scanPackLevel(row pgx.Row) (productclassificationview.PackLevelRecord, error) {
	var (
		rec                           productclassificationview.PackLevelRecord
		level                         string
		l, wd, h, wg, ml, mw, mh, mwg *int
	)
	if err := row.Scan(&rec.SKU, &level, &rec.GTIN, &rec.UnitsPerPack, &l, &wd, &h, &wg, &ml, &mw, &mh, &mwg, &rec.Version); err != nil {
		return productclassificationview.PackLevelRecord{}, fmt.Errorf("scan pack level: %w", err)
	}
	rec.Level = productclassificationview.PackLevel(level)
	rec.Declared = productclassificationview.Dims{LengthMm: deref(l), WidthMm: deref(wd), HeightMm: deref(h), WeightG: deref(wg)}
	rec.Measured = productclassificationview.Dims{LengthMm: deref(ml), WidthMm: deref(mw), HeightMm: deref(mh), WeightG: deref(mwg)}
	return rec, nil
}

func deref(v *int) int {
	if v == nil {
		return 0
	}
	return *v
}

func nullableInt(v int) *int {
	if v == 0 {
		return nil
	}
	return &v
}

func nullableTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}
