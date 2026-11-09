// Package postgres provides pgxpool-backed implementations of every
// outbound port.
package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/charge"
	"github.com/claudioed/wes-work-planning/internal/domain/pickload"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

type ChargeRepo struct {
	pool *pgxpool.Pool
}

func NewChargeRepo(pool *pgxpool.Pool) *ChargeRepo {
	return &ChargeRepo{pool: pool}
}

type bucketDTO struct {
	CPTUnixNano int64 `json:"cpt_unix_nano"`
	Quantity    int   `json:"quantity"`
	// The pack-aware estimate of ADR-0045, all omitted for a plain bucket
	// (so an old row and a new plain row are byte-identical).
	SKU      string `json:"sku,omitempty"`
	PickUnit string `json:"pick_unit,omitempty"`
	Handles  int    `json:"handles,omitempty"`
	CubeMm3  *int64 `json:"cube_mm3,omitempty"`
	WeightG  *int64 `json:"weight_g,omitempty"`
}

func (r *ChargeRepo) Save(ctx context.Context, forecast *charge.ChargeForecast) error {
	buckets := forecast.Buckets()
	dtos := make([]bucketDTO, len(buckets))
	for i, b := range buckets {
		dtos[i] = bucketDTO{CPTUnixNano: b.CPT.Time().UnixNano(), Quantity: b.Quantity.Value()}
		if b.Load != nil {
			dtos[i].SKU, dtos[i].PickUnit, dtos[i].Handles = b.SKU, b.Load.Unit.String(), b.Load.Handles
			if b.Load.CubeKnown {
				dtos[i].CubeMm3 = &b.Load.CubeMm3
			}
			if b.Load.WeightKnown {
				dtos[i].WeightG = &b.Load.WeightG
			}
		}
	}
	payload, err := json.Marshal(dtos)
	if err != nil {
		return err
	}

	_, err = querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO charge_forecasts (path_id, received_at, buckets)
		VALUES ($1, $2, $3)
		ON CONFLICT (path_id) DO UPDATE SET received_at = $2, buckets = $3
	`, forecast.PathId().String(), forecast.ReceivedAt(), payload)
	return err
}

func (r *ChargeRepo) FindByPathId(ctx context.Context, pathId shared.PathId) (*charge.ChargeForecast, error) {
	var receivedAt = forecastRow{}
	row := querierFrom(ctx, r.pool).QueryRow(ctx, `SELECT received_at, buckets FROM charge_forecasts WHERE path_id = $1`, pathId.String())
	if err := row.Scan(&receivedAt.ReceivedAt, &receivedAt.Buckets); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ports.ErrNotFound
		}
		return nil, err
	}

	var dtos []bucketDTO
	if err := json.Unmarshal(receivedAt.Buckets, &dtos); err != nil {
		return nil, err
	}

	cptBuckets := make([]charge.CPTBucket, len(dtos))
	for i, d := range dtos {
		qty, err := shared.NewQuantity(d.Quantity)
		if err != nil {
			return nil, err
		}
		cptBuckets[i] = charge.CPTBucket{CPT: shared.NewCPT(unixNanoToTime(d.CPTUnixNano)), Quantity: qty}
		if d.PickUnit != "" {
			unit, err := workunit.ParsePickUnit(d.PickUnit)
			if err != nil {
				return nil, err
			}
			load := pickload.Load{Unit: unit, Eaches: d.Quantity, Handles: d.Handles}
			if d.CubeMm3 != nil {
				load.CubeMm3, load.CubeKnown = *d.CubeMm3, true
			}
			if d.WeightG != nil {
				load.WeightG, load.WeightKnown = *d.WeightG, true
			}
			cptBuckets[i].SKU, cptBuckets[i].Load = d.SKU, &load
		}
	}

	return charge.NewChargeForecast(pathId, cptBuckets, receivedAt.ReceivedAt)
}
