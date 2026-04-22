# Praxis Cloudflare Sandbox Bridge

This surface is the HTTP bridge that makes Praxis `cloudflare_remote` sandbox execution real.

It matches the runtime contract in `Code&DBs/Workflow/runtime/sandbox_runtime.py`:

- `POST /sessions/create`
- `POST /sessions/{id}/hydrate`
- `POST /sessions/{id}/exec`
- `POST /sessions/{id}/artifacts`
- `POST /sessions/{id}/destroy`

The bridge is designed for a `workers.dev` URL first. You do not need a hosted site or a custom route to use it.

## What It Does

- Provisions one Cloudflare Sandbox per Praxis session.
- Hydrates the workspace archive into the configured container workspace root.
- Executes commands with stdin, env vars, cwd, and timeout.
- Captures changed artifacts and returns them as base64 content.
- Optionally protects the bridge with a bearer token.

## Important Constraints

- This bridge currently supports only `network_policy="provider_only"`.
- This bridge currently supports only `workspace_materialization="copy"`.
- The current Praxis runtime uploads the hydrated workspace as base64 JSON. Large workspaces may hit Worker request size limits.
- Artifact sync returns changed and newly written files. File deletions are not mirrored back.

## Deploy

1. `cd Code&DBs/Workflow/surfaces/cloudflare_sandbox_bridge`
2. `npm install`
3. `npx wrangler login`
4. Configure `PRAXIS_CONTAINER_WORKSPACE_ROOT` and `PRAXIS_BRIDGE_TMP_ROOT` from `config/workspace_layout.json` in the deployment environment.
5. Optional but recommended: `npx wrangler secret put BRIDGE_TOKEN`
6. `npx wrangler deploy`

The checked-in `wrangler.jsonc` intentionally does not duplicate container path
values. The bridge fails closed when the deployment environment omits those
registry-owned values.

Wrangler will print a URL shaped like:

```text
https://praxis-sandbox-bridge.<your-subdomain>.workers.dev
```

Use that as the Praxis runtime base URL.

## Runtime Wiring

Set these where Praxis runs:

```bash
export PRAXIS_CLOUDFLARE_SANDBOX_URL="https://praxis-sandbox-bridge.<your-subdomain>.workers.dev"
export PRAXIS_CLOUDFLARE_SANDBOX_TOKEN="<same value as BRIDGE_TOKEN>"
```

`PRAXIS_CLOUDFLARE_SANDBOX_TOKEN` is optional only if you intentionally deploy the bridge without `BRIDGE_TOKEN`.

## Smoke Check

Unauthenticated health:

```bash
curl https://praxis-sandbox-bridge.<your-subdomain>.workers.dev/healthz
```

Authenticated create-session probe:

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $PRAXIS_CLOUDFLARE_SANDBOX_TOKEN" \
  https://praxis-sandbox-bridge.<your-subdomain>.workers.dev/sessions/create \
  -d '{
    "sandbox_session_id": "sandbox_session:smoke:cloudflare",
    "sandbox_group_id": "group:smoke",
    "network_policy": "provider_only",
    "workspace_materialization": "copy",
    "timeout_seconds": 30,
    "metadata": {}
  }'
```

## Why The Dockerfile Looks Familiar

The container image intentionally mirrors the local Praxis worker lane:

- `bash`
- `python3`
- `git`
- `ripgrep`
- `@openai/codex`

That keeps the remote lane closer to `docker_local` instead of becoming a weird alternate runtime.
