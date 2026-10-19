package main

import "github.com/claudioed/wes-work-planning/internal/adapters/outbound/memory"

// memoryRepositories is a TEST fixture only (ADR-0044): repositories over the
// in-memory doubles, for the unit tests that start or stop one component
// without a database. It lives in a _test.go file on purpose: no non-test file
// in cmd/** may use the memory package (internal/architecture
// TestRuntimeBinaryHasNoInMemoryAdapters), and the binary has no in-memory
// runtime path.
func memoryRepositories() repositories {
	workUnits := memory.NewWorkUnitRepo()
	return repositories{
		charges:        memory.NewChargeRepo(),
		plans:          memory.NewPlanRepo(),
		pools:          memory.NewWorkPoolRepo(),
		workUnits:      workUnits,
		laborPlanViews: memory.NewLaborPlanViewRepo(),
		inventoryViews: memory.NewInventoryViewRepo(),
		processedEvts:  memory.NewProcessedEventRepo(),
		relocations:    workUnits,
		slotPlans:      memory.NewSlotPlanRepo(),
	}
}
