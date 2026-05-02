# Phase 10 Implementation Report

Date: 2026-04-30

## Summary

Phase 10 is complete. The optional managed runtime layer is no longer just a
bounded accounting substrate: it is now a DB-backed CQRS authority with gateway
operations, MCP tools, migration registration, generated docs, live authority
proof, cost readback, heartbeat-derived pool health, and roadmap closeout.

The architectural line stays intact: Praxis can run workflows for a customer,
export them, or operate in hybrid mode, but the recurring runtime is not the
only value path. Discovery, build, proof, deployment contract, and drift
authority remain separable from managed compute charges.

## Authority Discovery

Used Praxis standing orders, `praxis workflow` roadmap/readback authority,
federated search, local code inspection, and the CQRS gateway doctrine. Discovery
confirmed there was no existing managed-runtime accounting authority, so Phase
10 added a new authority instead of hiding runtime billing or health state inside
worker internals.

## Changed Files

- `Code&DBs/Workflow/runtime/managed_runtime/__init__.py`
- `Code&DBs/Workflow/runtime/managed_runtime/accounting.py`
- `Code&DBs/Workflow/runtime/operations/commands/managed_runtime.py`
- `Code&DBs/Workflow/runtime/operations/queries/managed_runtime.py`
- `Code&DBs/Workflow/storage/postgres/managed_runtime_repository.py`
- `Code&DBs/Workflow/surfaces/mcp/tools/managed_runtime.py`
- `Code&DBs/Workflow/surfaces/mcp/cli_metadata.py`
- `Code&DBs/Databases/migrations/workflow/382_managed_runtime_authority.sql`
- `Code&DBs/Workflow/system_authority/workflow_migration_authority.json`
- `Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py`
- `Code&DBs/Workflow/tests/unit/test_managed_runtime_accounting.py`
- `Code&DBs/Workflow/tests/unit/test_managed_runtime_operations.py`
- `Code&DBs/Workflow/tests/unit/test_managed_runtime_repository.py`
- `Code&DBs/Workflow/tests/unit/test_managed_runtime_mcp_tool.py`
- `Code&DBs/Workflow/tests/unit/test_operation_catalog_bindings.py`
- `Code&DBs/Workflow/tests/unit/test_operation_catalog_mounting.py`
- `Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py`
- `docs/architecture/object-truth-trust-toolbelt/managed-runtime-accounting-2026-04-30.md`
- `docs/MCP.md`
- `docs/CLI.md`
- `docs/API.md`
- `artifacts/workflow/client_operating_model/build_reports/phase_10_IMPLEMENTATION.md`
- `artifacts/workflow/client_operating_model/build_reports/chain_EXECUTION_SUMMARY.md`

## Implemented Authority

- CQRS command: `authority.managed_runtime.record`
- CQRS query: `authority.managed_runtime.read`
- MCP write tool: `praxis_authority_managed_runtime_record`
- MCP read tool: `praxis_authority_managed_runtime_read`
- Migration: `382_managed_runtime_authority.sql`
- Storage authority for managed runtime records, meter events, pricing
  schedules, run heartbeats, pool health snapshots, and audit events.

## Implemented Contracts

- managed, exported, and hybrid mode selection
- policy-level fail-fast reason codes for disabled, denied, unsupported, or
  scoped-out managed execution
- idempotent meter events with deterministic run receipt linkage
- billable usage totals separated from diagnostic/runtime metadata
- pricing schedule version references as explicit cost basis
- deterministic final run receipts with correction references
- heartbeat-derived pool health and dispatch eligibility
- internal audit contracts separate from customer-safe observability summaries

## Live Proof

Live migration application succeeded for
`382_managed_runtime_authority.sql`.

The live CQRS smoke proof recorded
`managed_runtime_record.phase_10_live_proof` for
`run.managed.phase10.live_proof` with final run receipt
`run_receipt.8ac939d2ab1da24ec7fc`.

Proof details:

- write receipt: `473ca58b-8fbb-493b-acde-0b54b96b7509`
- emitted event: `d1cff397-f774-4859-b615-5795440a266b`
- meter-event readback receipt: `634b64c8-0b4e-47bb-b237-cb1777c92b9e`
- cost: `0.240000 USD`
- meter events: `3`, all linked to
  `run_receipt.8ac939d2ab1da24ec7fc`
- pool health: `healthy`
- dispatch allowed: `true`

The first live smoke intentionally caught real system friction:

- failed receipt: `990f873e-3acb-4cb5-bcbc-97d337c6043e`
- failure: parent managed-runtime record referenced a pricing schedule version
  before the version row existed
- fix: insert pricing schedule versions before parent managed-runtime records
- hardening: persist the finalized run receipt id into every meter-event row

That is exactly why this layer exists: if the model cannot survive the gateway,
storage, receipt, and readback path, it is not authority yet.

## Validation

- Phase 10 domain plus CQRS operation/repository/MCP gate:
  `16 passed in 0.40s`
- Phase 10 new operation/repository/MCP gate:
  `8 passed in 0.38s`
- Post-fix repository/operation gate:
  `6 passed in 0.31s`
- Catalog route/binding gate:
  `56 passed in 0.55s`
- Catalog, migration-authority, and generated-docs unit gate:
  `78 passed in 0.79s`
- Migration-authority contract gate:
  `13 passed in 0.33s`
- Generated docs metadata gate:
  `9 passed in 0.56s`
- `git diff --check`: passed

DB-backed migration integration collection still hits the existing local
`praxis_test` bootstrap trigger issue around OpenRouter/CLI task routing. That
failure is outside the Phase 10 managed-runtime authority and remains separate
from the live workflow authority proof above.

## Roadmap Closeout

- closeout preview receipt: `b25548fe-e162-41ad-a599-7ec06d7a8c5b`
- closeout preview event: `aa28bdde-04ff-4d0f-85df-3998490d83d4`
- closeout command receipt: `6c3f2981-9b29-4d80-bd18-50ee3708669a`
- closeout event: `7633a6ac-7ee4-4a2b-a560-e21ad0ea4cff`
- Phase 10 roadmap readback receipt:
  `52385c1b-80f2-475a-8192-5fbd05c14a99`
- root roadmap readback after Phase 10:
  `add3cbe1-d700-4569-a055-e09cab40a232`

Roadmap state after closeout:

- Phase 10:
  `roadmap_item.object.truth.trust.toolbelt.authority.managed.runtime.compute.observability`
  is `completed` / `completed`.
- Root remains `active` / `claimed`.
- Phase 11 remains the active unfinished slice:
  `roadmap_item.object.truth.trust.toolbelt.authority.operator.inspection.canvas.workflow.surfaces`.

## Remaining Authority Work

Phase 11 should turn the completed substrate into operator-facing inspection and
workflow-builder surfaces. The surface must consume the durable authority now in
place instead of reassembling proof payloads by hand.
