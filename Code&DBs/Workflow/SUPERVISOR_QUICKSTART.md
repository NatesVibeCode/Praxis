# Supervisor Quick Start (Legacy)

Use this only as a compatibility pointer. The supported quickstart is:

```bash
cd <repo-root>
./scripts/praxis launch
./scripts/praxis doctor --json
```

That command path:

- installs or starts launchd services if needed
- bootstraps schema authority
- rebuilds the TypeScript launcher app when stale
- verifies `8420 /api/health`
- verifies `8421 /orient`
- verifies `8421 /mcp`
- verifies `8420 /app`

Primary URLs:

- Launcher: `http://127.0.0.1:8420/app`
- Helm: `http://127.0.0.1:8420/app/helm`
- Legacy `/ui`: redirects to `http://127.0.0.1:8420/app`
- API docs: `http://127.0.0.1:8420/docs`

Do not use `workflow supervisor ...` as the primary setup path anymore.
