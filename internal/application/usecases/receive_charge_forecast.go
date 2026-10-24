// Package usecases implements the application's use cases: one struct per
// use case, orchestrating the domain against outbound ports.
package usecases

import (
	"context"
	"fmt"
	"strings"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/charge"
	"github.com/claudioed/wes-work-planning/internal/domain/pickload"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// ReceiveChargeForecast records the volume due by CPT for a process path.
type ReceiveChargeForecast struct {
	charges   ports.ChargeRepo
	publisher ports.EventPublisher
	clock     ports.Clock
	uow       ports.UnitOfWork
	packs     ports.ProductClassificationLookup
}

func NewReceiveChargeForecast(charges ports.ChargeRepo, publisher ports.EventPublisher, clock ports.Clock) *ReceiveChargeForecast {
	return &ReceiveChargeForecast{charges: charges, publisher: publisher, clock: clock}
}

// WithUnitOfWork brackets Save + Publish in one atomic scope (ADR-0014).
// Optional: nil keeps the two calls running back to back.
func (uc *ReceiveChargeForecast) WithUnitOfWork(u ports.UnitOfWork) *ReceiveChargeForecast {
	uc.uow = u
	return uc
}

// WithPackLookup wires the local pack copy (ADR-0045) the pack-aware charge
// estimate reads. Optional: without it a bucket that asks for an estimate is
// refused.
func (uc *ReceiveChargeForecast) WithPackLookup(l ports.ProductClassificationLookup) *ReceiveChargeForecast {
	uc.packs = l
	return uc
}

// CPTBucketInput is one CPT/quantity pair in the incoming forecast. Quantity
// is eaches. SKU and PickUnit are the optional pack-aware estimate of
// ADR-0045: when SKU is given, the bucket's handles (cases for a CASE pick),
// cube and weight are estimated from the SKU's local pack copy; PickUnit
// defaults to EACH.
type CPTBucketInput struct {
	CPT      shared.CPT
	Quantity shared.Quantity
	SKU      string
	PickUnit workunit.PickUnit
}

type ReceiveChargeForecastRequest struct {
	PathId  shared.PathId
	Buckets []CPTBucketInput
}

func (uc *ReceiveChargeForecast) Execute(ctx context.Context, req ReceiveChargeForecastRequest) (*charge.ChargeForecast, error) {
	buckets := make([]charge.CPTBucket, len(req.Buckets))
	for i, b := range req.Buckets {
		bucket, err := uc.bucketOf(ctx, b)
		if err != nil {
			return nil, fmt.Errorf("bucket %d: %w", i, err)
		}
		buckets[i] = bucket
	}

	forecast, err := charge.NewChargeForecast(req.PathId, buckets, uc.clock.Now())
	if err != nil {
		return nil, err
	}

	event := shared.NewChargeForecastReceived(req.PathId, uc.clock.Now())
	err = atomically(ctx, uc.uow, func(ctx context.Context) error {
		if err := uc.charges.Save(ctx, forecast); err != nil {
			return err
		}
		return uc.publisher.Publish(ctx, event)
	})
	if err != nil {
		return nil, err
	}

	return forecast, nil
}

// bucketOf maps one input bucket, adding the pack-aware estimate when it names
// a SKU. A CASE bucket needs an active CASE level and a whole number of cases
// (pickload's errors); an EACH bucket needs nothing, its cube and weight are
// simply unknown without an EACH size on record.
func (uc *ReceiveChargeForecast) bucketOf(ctx context.Context, b CPTBucketInput) (charge.CPTBucket, error) {
	sku := strings.TrimSpace(b.SKU)
	if sku == "" {
		if b.PickUnit != "" {
			return charge.CPTBucket{}, fmt.Errorf("%w: pickUnit needs a sku", workunit.ErrInvalidPickPlan)
		}
		return charge.CPTBucket{CPT: b.CPT, Quantity: b.Quantity}, nil
	}
	unit := b.PickUnit
	if unit == "" {
		unit = workunit.PickUnitEach
	}
	view := productClassificationFor(ctx, uc.packs, sku)
	load, err := pickload.Estimate(unit, b.Quantity.Value(), view)
	if err != nil {
		return charge.CPTBucket{}, err
	}
	return charge.CPTBucket{CPT: b.CPT, Quantity: b.Quantity, SKU: sku, Load: &load}, nil
}
