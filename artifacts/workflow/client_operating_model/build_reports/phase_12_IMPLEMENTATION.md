# Phase 12 Implementation Report

Date: 2026-04-30

## Scope

Promote the Phase 11 operator read-model substrate into the first CQRS/MCP-visible operator surface.

This slice is deliberately read-only. It exposes derived operator views from supplied evidence payloads and does not persist projection snapshots, mutate client systems, run live sandbox promotions, or file bugs/gaps automatically.

The remaining projection-storage and HTTP-route verification gates were split into a follow-up roadmap item so this slice could be closed without pretending those gates were done.

## Changed Files

- `Code&DBs/Workflow/runtime/operations/queries/client_operating_model.py`
- `Code&DBs/Workflow/surfaces/mcp/tools/client_operating_model.py`
- `Code&DBs/Workflow/surfaces/mcp/cli_metadata.py`
- `Code&DBs/Databases/migrations/workflow/356_register_client_operating_model_operator_view.sql`
- `Code&DBs/Databases/migrations/workflow/357_client_system_discovery_authority.sql`
- `Code&DBs/Workflow/system_authority/workflow_migration_authority.json`
- `Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py`
- `Code&DBs/Workflow/tests/unit/test_client_operating_model_operation.py`
- `Code&DBs/Workflow/tests/unit/test_client_operating_model_mcp_tool.py`
- `docs/MCP.md`
- `docs/CLI.md`
- `docs/API.md`

## Delivered

- Added `client_operating_model_operator_view`, a read-only CQRS query handler.
- Added `praxis_client_operating_model`, a thin MCP wrapper that dispatches through the operation catalog gateway.
- Added CLI metadata and regenerated shared MCP/CLI/API docs.
- Registered the authority domain live as `authority.client_operating_model`.
- Registered the operation live through `praxis_register_operation`.
- Added migration 356 using `register_operation_atomic` so the registration is also durable in the migration lane.
- Regenerated workflow migration authority after adding the migration.
- Preserved the Phase 11 read-model contract: stable view id, generated timestamp, freshness, permission scope, correlation ids, evidence refs, explicit state, and JSON-ready payload.

## Validation

Focused validation passed:

- Full focused Client Operating Model chain: `112 passed in 1.01s`
- Phase 12/docs authority recheck: `26 passed in 0.58s`
- `test_mcp_docs_and_metadata.py`: passed.
- `test_workflow_migration_authority_contract.py`: passed.
- py_compile passed for the new query and MCP tool modules.
- Live authority-domain registration receipt: `dac71b97-b855-46a2-a365-0257ef0e27dd`.
- Live operation registration receipt: `62f49271-31a8-4aea-80c9-c0cfb47dd9bb`.
- Live gateway execution receipt: `ee294403-cf1b-40b8-ad63-3f655b07fff6`.
- Roadmap update receipts: `28fba056-3654-458e-88ca-e462a175c9d5`, `6b06ac79-d8ef-46c1-81dd-ccd0c0a8f695`, `bd4735d1-f401-49a9-af7d-29f07d065d10`.
- Follow-up roadmap item: `roadmap_item.object.truth.trust.toolbelt.authority.phase.13.client.operating.model.projection.storage.and.http.route.verification`.
- Follow-up roadmap commit receipt: `3ab74cbf-f459-4dc3-8137-6e7fd111f2ae`.
- Phase 12 proof-backed closeout receipt: `adcc3bb3-10ec-4700-a354-972b575d47b1`.
- Roadmap readback receipt: `ebb1146e-282b-4b71-a791-b80c6d319180`.
- Operation-forge readback reports `state: existing_operation`, API route `GET /api/operator/client-operating-model/view`, and catalog-bound tool `praxis_client_operating_model`.

## Known Blockers

- A pre-existing catalog warning remains: `structured_documents.context_assemble` references a missing query model attribute.

## Remaining Phase 12 Work

None. Phase 12 is closed as the CQRS/MCP operator-view registration slice.

## Follow-Up Work

Tracked and completed by `roadmap_item.object.truth.trust.toolbelt.authority.phase.13.client.operating.model.projection.storage.and.http.route.verification`:

- Add durable projection/storage for operator-view snapshots if the product wants historical readback instead of request-time derivation.
- Decide whether migration 356 should remain as the durable replay artifact now that live registration has already written the catalog triple.
- Add end-to-end API route verification once the HTTP server lane is running.
