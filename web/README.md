# planning-mfe

The `wes-work-planning` Module Federation remote. Exposes `./App` (mounted
by `warehouse-console` at `/planning/*` as `planning_mfe`) and is also
runnable standalone for local development:

```bash
npm install
npm run dev   # http://localhost:5183, standalone (no shell)
```

Talks directly to wes-work-planning's own REST API (`WES_HTTP_PORT=8083`
per `e2e-tests/env.sh`) via `src/config.ts`'s `WES_API_BASE`.

See `order-management/web/README.md` and the top-level `CLAUDE.md` for the
shared MFE conventions this remote follows (ui-kit, module federation
shape, per-service port map).

## Disclaimer

This project is a domain-driven design study. It is not associated with, endorsed by or affiliated with any company, and it uses no company's code, data, logos or trademarks. All names, figures and examples are illustrative or synthetic.
