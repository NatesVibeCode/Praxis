# Supervisor Quick Start (Legacy)

Use this only as a compatibility pointer. The supported launcher quickstart is:

```bash
cd <repo-root>
./scripts/praxis launch
./scripts/praxis doctor --json
```

That command path:

- starts the Docker runtime if needed
- probes schema authority and launcher readiness
- verifies `8420 /api/health`
- verifies `8420 /orient`
- verifies `8420 /mcp`
- verifies `8420 /app`

Native launchd install/setup control has been removed. Use `./scripts/praxis start|stop|restart|status|logs`
for Docker runtime control.

Primary URLs:

- Launcher: `http://127.0.0.1:8420/app`
- Helm: `http://127.0.0.1:8420/app/helm`
- API docs: `http://127.0.0.1:8420/docs`

Do not use `workflow supervisor ...` as the primary setup path anymore.
