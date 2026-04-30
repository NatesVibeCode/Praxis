# Client System Discovery Phase 1

Status: implemented as a minimal durable substrate in this checkout on 2026-04-30.

## Scope

Phase 1 establishes the lowest-level authority needed for client system
discovery before broader Object Truth or Virtual Lab workflows run:

- typed system census records
- typed connector census records
- durable evidence for capability, object, API, and event surfaces
- credential-health references without secret material
- deterministic automation-bearing tool classification
- typed discovery gaps emitted through `authority_events`

## Local Constraints

This checkout does not include the packet file requested at
`artifacts/workflow/client_operating_model/packets/phase_01_client_system_discovery/PLAN.md`
or the broader migration-authority generator tree that the full Praxis repo
normally uses. The implementation therefore lands the Phase 1 substrate in the
canonical local paths without trying to backfill the missing backlog.

## Contracts

Runtime contracts live in:

- `runtime/client_system_discovery/models.py`
- `storage/postgres/client_system_discovery_repository.py`
- `surfaces/mcp/tools/client_system_discovery.py`

The discovery surface is CQRS-ready in shape:

- `discover` persists a full census snapshot and child evidence rows
- `list`, `search`, and `describe` are read-oriented query actions
- `record_gap` emits a typed gap event into `authority_events`

## Persistence

Migration:

- `Code&DBs/Databases/migrations/workflow/314_client_system_discovery_authority.sql`

Authority metadata:

- `Code&DBs/Workflow/system_authority/client_system_discovery_phase_01_authority.json`

## Deliberate Omissions

- no broad migration-authority regeneration
- no external API crawling or connector execution
- no raw credential storage
- no new typed-gap table
- no dependency on absent packet/backlog files
