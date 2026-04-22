# Windows WSL Postgres Runtime Target

This guide describes a supported Praxis runtime target shape: a Windows host
running WSL2, Ubuntu, Postgres 16+, and pgvector, reachable by other Praxis
machines over a trusted LAN.

The authority model is simple:

- Praxis.db is the durable state authority.
- `WORKFLOW_DATABASE_URL` is the database connection authority for each runtime.
- The Windows/WSL host is one possible runtime target, not a special product
  boundary.
- Workers remain explicit execution nodes; a database host should not claim
  workflow work unless it is intentionally configured as a worker.

## Target Shape

Use this topology when a Windows machine is the most convenient always-on
database host:

```text
Praxis cockpit or worker
  -> TCP 5432 on a LAN address
  -> Windows firewall allow rule
  -> Windows port proxy
  -> WSL2 Ubuntu
  -> Postgres 16+ with pgvector
  -> Praxis.db
```

Required services:

- WSL2 with a Linux distribution supported by your package manager.
- Postgres 16 or newer.
- pgvector enabled in the target database.
- A LAN-only firewall rule for the Postgres port.
- A boot or login task that starts WSL/Postgres and refreshes the port proxy
  when the WSL IP changes.
- A backup job that writes compressed `pg_dump` output outside the WSL virtual
  disk.

## Configuration Contract

Each Praxis runtime that uses this database must set:

```bash
WORKFLOW_DATABASE_URL='postgresql://<user>:<password>@<lan-host>:5432/<database>'
```

Do not bake this URL into source files, compose files, launchers, or docs as a
local default. For public examples, use placeholders. For operator machines,
load the value from the runtime registry, shell, or local `.env` created by
`./scripts/bootstrap`.

Recommended environment variables for the helper scripts:

| Variable | Purpose | Default |
|---|---|---|
| `PRAXIS_WSL_DISTRO` | WSL distribution name | `Ubuntu` |
| `PRAXIS_DB_NAME` | Postgres database to check or back up | `praxis` |
| `PRAXIS_DB_USER` | Local Postgres user for health checks | `postgres` |
| `PRAXIS_DB_PORT` | Windows-facing Postgres port | `5432` |
| `PRAXIS_BACKUP_DIR` | Windows directory for compressed dumps | user profile `PraxisBackups` |
| `PRAXIS_BACKUP_RETAIN_DAYS` | Backup retention window | `14` |
| `PRAXIS_FIREWALL_RULE` | Windows firewall rule display name | `Praxis Postgres (LAN)` |
| `PRAXIS_STATUS_PORT` | Health HTTP port for the optional status server | `8480` |

## Helper Scripts

Portable helper scripts live under:

```text
scripts/runtime-targets/windows-wsl-postgres/
```

They are support tooling, not core authority:

- `health-check.ps1` checks WSL reachability, Postgres readiness, pgvector,
  Windows port listening, firewall rule presence, disk space, backup freshness,
  and the optional status endpoint.
- `backup-postgres.ps1` streams `pg_dump` from WSL to a compressed backup file
  on the Windows filesystem and prunes old backups.
- `status-server.py` exposes a small read-only JSON `/health` endpoint from
  inside WSL.

These scripts must stay parameterized. If an operator needs a fixed host path,
IP address, or credential location, put it in local environment or scheduler
configuration outside the repo.

## Backup And Recovery

Minimum recovery posture:

- Keep backups outside the WSL virtual disk.
- Verify compressed dump integrity after every backup.
- Retain enough history to recover from silent corruption.
- Practice restoring into a scratch database before trusting the backup plan.

If the Windows host fails, restore the latest dump onto any reachable Postgres
16+ server with pgvector, then update `WORKFLOW_DATABASE_URL` for the Praxis
runtimes that use it.

## Security Notes

- Expose Postgres only on trusted LAN ranges.
- Do not port-forward the database to the public internet.
- Use password auth suitable for your Postgres version, such as SCRAM.
- Keep database credentials out of the repository.
- Treat Windows scheduled tasks, firewall rules, and WSL service configuration
  as runtime-target setup, not Praxis source authority.

## Agent Skill Authority

Do not import filesystem skill copies directly into Praxis.db from this runtime
target. Agent skills are governed by DB-backed skill authority. Filesystem
exports for Claude, Codex, Cursor, Gemini, or other harnesses are derived
compatibility artifacts and should be regenerated from the registry-backed
writer when that path is available.

The older one-off skill ingestion script from the Dell setup is intentionally
not recovered as a public runnable helper because it wrote directly from local
files into the database. The durable shape is a registry-backed skill writer
with receipts.

## Non-Authoritative Example Host

The original build used a consumer Windows laptop with WSL2, Ubuntu 24.04,
Postgres 16, pgvector, a LAN port proxy, daily compressed backups, and a small
health endpoint. That proved the target shape works, but those machine details
are evidence only. They are not defaults, contracts, or public authority.
