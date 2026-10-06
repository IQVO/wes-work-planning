package usecases

import (
	"context"
	"errors"

	"github.com/claudioed/wes-work-planning/internal/application/ports"
	"github.com/claudioed/wes-work-planning/internal/domain/release"
	"github.com/claudioed/wes-work-planning/internal/domain/shared"
)

// ConfigurePool is the explicit operator command that sets a path's pool
// feed mode and WIP limit, creating the pool when none exists yet
// (ADR-0033). It is the only way to obtain a flow-fed pool.
//
// It never evicts or cancels work: lowering the limit below the current
// WIP just pauses releases until WIP < limit (release.WorkPool.Configure).
// An unconfigured path keeps EnqueueWorkUnit's ReleaseFed 1000/1000
// fallback. It raises no event: the configuration is operational state,
// not a business fact other contexts react to.
type ConfigurePool struct {
	pools ports.WorkPoolRepo
}

func NewConfigurePool(pools ports.WorkPoolRepo) *ConfigurePool {
	return &ConfigurePool{pools: pools}
}

type ConfigurePoolRequest struct {
	PathId   shared.PathId
	Mode     release.FeedMode
	WIPLimit int
}

// Execute applies the configuration and returns the pool as saved. It is
// idempotent: an identical repeat on an existing pool writes nothing. The
// read-modify-write runs in retryOnPoolConflict like every other pool
// writer, so it can never overwrite a newer pool.
func (uc *ConfigurePool) Execute(ctx context.Context, req ConfigurePoolRequest) (*release.WorkPool, error) {
	var pool *release.WorkPool
	err := retryOnPoolConflict(ctx, func(ctx context.Context) error {
		var err error
		pool, err = uc.configureOnce(ctx, req)
		return err
	})
	if err != nil {
		return nil, err
	}
	return pool, nil
}

func (uc *ConfigurePool) configureOnce(ctx context.Context, req ConfigurePoolRequest) (*release.WorkPool, error) {
	pool, err := uc.pools.FindByPathId(ctx, req.PathId)
	created := false
	if errors.Is(err, ports.ErrNotFound) {
		pool = release.NewWorkPool(req.PathId, release.ReleaseFed, defaultWIPLimit, defaultAlarmThreshold)
		created = true
	} else if err != nil {
		return nil, err
	}
	changed, err := pool.Configure(req.Mode, req.WIPLimit)
	if err != nil {
		return nil, err
	}
	if created || changed {
		if err := uc.pools.Save(ctx, pool); err != nil {
			return nil, err
		}
	}
	return pool, nil
}
