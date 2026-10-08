package productclassificationcopy

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/productclassificationview"
)

// memoryRow is one SKU's entry in MemoryStore.
type memoryRow struct {
	handlingTags     []string
	temperatureClass string
	dotHazardClass   int
	version          int64
	updatedAt        time.Time
}

// MemoryStore is the in-memory local copy used when DATABASE_URL is unset
// (local runs, tests). It lives only as long as the process (ADR-0035,
// "In-memory runs").
type MemoryStore struct {
	mu    sync.RWMutex
	bySKU map[string]memoryRow
}

var (
	_ ports.ProductClassificationLookup   = (*MemoryStore)(nil)
	_ ports.ProductClassificationCopyRepo = (*MemoryStore)(nil)
)

// NewMemoryStore constructs an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{bySKU: make(map[string]memoryRow)}
}

// ApplyIfNewer implements ports.ProductClassificationCopyRepo.
func (s *MemoryStore) ApplyIfNewer(_ context.Context, view productclassificationview.ProductClassificationView, dotHazardClass int, version int64, updatedAt time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, ok := s.bySKU[view.SKU]; ok && current.version >= version {
		return false, nil
	}
	s.bySKU[view.SKU] = memoryRow{
		handlingTags:     slices.Clone(view.HandlingTags),
		temperatureClass: view.TemperatureClass,
		dotHazardClass:   dotHazardClass,
		version:          version,
		updatedAt:        updatedAt,
	}
	return true, nil
}

// GetClassification implements ports.ProductClassificationLookup.
func (s *MemoryStore) GetClassification(_ context.Context, sku string) (productclassificationview.ProductClassificationView, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	row, ok := s.bySKU[sku]
	if !ok {
		return unknown(sku), nil
	}
	return productclassificationview.ProductClassificationView{
		SKU:              sku,
		HandlingTags:     slices.Clone(row.handlingTags),
		TemperatureClass: row.temperatureClass,
		Known:            true,
	}, nil
}

// Version reports the stored version for sku (0 when absent). Test and
// diagnostics helper; not part of either port.
func (s *MemoryStore) Version(sku string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bySKU[sku].version
}
