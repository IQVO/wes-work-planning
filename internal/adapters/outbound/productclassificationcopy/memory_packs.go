package productclassificationcopy

import (
	"context"
	"time"

	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
)

// ApplyDeclared implements ports.PackLevelCopyRepo.
func (s *MemoryStore) ApplyDeclared(_ context.Context, rec productclassificationview.PackLevelRecord) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.packs[rec.GTIN]
	if ok && cur.Version >= rec.Version {
		return false, nil
	}
	rec.Measured = cur.Measured // a re-declaration keeps the measurement
	s.packs[rec.GTIN] = rec
	return true, nil
}

// ApplyRetired implements ports.PackLevelCopyRepo.
func (s *MemoryStore) ApplyRetired(_ context.Context, gtin string, effectiveTo time.Time, version int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.packs[gtin]
	if !ok || cur.Version >= version {
		return false, nil
	}
	cur.EffectiveTo, cur.Version = effectiveTo, version
	s.packs[gtin] = cur
	return true, nil
}

// ApplyMeasured implements ports.PackLevelCopyRepo.
func (s *MemoryStore) ApplyMeasured(_ context.Context, gtin string, dims productclassificationview.Dims, version int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.packs[gtin]
	if !ok || cur.Version >= version {
		return false, nil
	}
	cur.Measured, cur.Version = dims, version
	s.packs[gtin] = cur
	return true, nil
}

// attachPacks sets view.Each / view.Case from the active rows of view.SKU, the
// highest version winning per level. The caller holds s.mu.
func (s *MemoryStore) attachPacks(view *productclassificationview.ProductClassificationView) {
	var each, kase *productclassificationview.PackLevelRecord
	for _, rec := range s.packs {
		if rec.SKU != view.SKU || !rec.Active() {
			continue
		}
		rec := rec
		switch rec.Level {
		case productclassificationview.PackLevelEach:
			if each == nil || rec.Version > each.Version {
				each = &rec
			}
		case productclassificationview.PackLevelCase:
			if kase == nil || rec.Version > kase.Version {
				kase = &rec
			}
		}
	}
	if each != nil {
		f := each.Facts()
		view.Each = &f
	}
	if kase != nil {
		f := kase.Facts()
		view.Case = &f
	}
}
