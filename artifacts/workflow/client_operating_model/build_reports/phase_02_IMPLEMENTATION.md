# Phase 02 Implementation Report

Date: 2026-04-30

## Summary

Promoted Object Truth ingestion and evidence capture from a local primitive
layer into DB-backed CQRS authority.

Phase 2 now records queryable client-system snapshots, source-query evidence,
sample captures, raw-payload references, redacted previews, object-version
refs, and replay fixtures through the operation catalog, MCP tools, HTTP
routes, and Postgres tables.

## Authority Added

- Migration: `Code&DBs/Databases/migrations/workflow/362_object_truth_ingestion_sample_authority.sql`
- Command operation: `object_truth_ingestion_sample_record`
- Query operation: `object_truth_ingestion_sample_read`
- MCP tools:
  - `praxis_object_truth_ingestion_sample_record`
  - `praxis_object_truth_ingestion_sample_read`
- HTTP routes:
  - `POST /api/object-truth/ingestion/samples`
  - `GET /api/object-truth/ingestion/samples`

## Changed Files

- `Code&DBs/Databases/migrations/workflow/362_object_truth_ingestion_sample_authority.sql`
- `Code&DBs/Workflow/runtime/operations/commands/object_truth_ingestion.py`
- `Code&DBs/Workflow/runtime/operations/queries/object_truth_ingestion.py`
- `Code&DBs/Workflow/storage/postgres/object_truth_repository.py`
- `Code&DBs/Workflow/surfaces/mcp/tools/object_truth.py`
- `Code&DBs/Workflow/surfaces/mcp/cli_metadata.py`
- `Code&DBs/Workflow/system_authority/workflow_migration_authority.json`
- `Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py`
- `Code&DBs/Workflow/tests/unit/test_object_truth_ingestion_operation.py`
- `Code&DBs/Workflow/tests/unit/test_object_truth_ingestion_repository.py`
- `Code&DBs/Workflow/tests/unit/test_object_truth_mcp_tool.py`
- `Code&DBs/Workflow/tests/unit/test_operation_catalog_mounting.py`
- `artifacts/workflow/client_operating_model/build_reports/phase_02_IMPLEMENTATION.md`

## Key Correction

Live proof exposed one weak path before closeout: object-version evidence and
source metadata could carry raw sample values even though raw payload storage
was disabled.

Fixed in the ingestion command:

- confidential/restricted object-version field previews are replaced with
  redaction markers
- identity values for classified identity fields are replaced with digest
  markers
- source metadata raw identifiers are replaced with stable redacted refs
- leftover source metadata is stored as digest-backed redacted metadata
- object-version digests are recalculated over the sanitized packet

This keeps replay fixtures and stored object-version evidence useful for
comparison without exposing raw client payload values.

## Validation

Focused local gate:

```bash
PYTHONPATH='Code&DBs/Workflow' .venv/bin/python -m py_compile \
  Code\&DBs/Workflow/runtime/operations/commands/object_truth_ingestion.py \
  Code\&DBs/Workflow/runtime/operations/queries/object_truth_ingestion.py \
  Code\&DBs/Workflow/storage/postgres/object_truth_repository.py \
  Code\&DBs/Workflow/surfaces/mcp/tools/object_truth.py \
  Code\&DBs/Workflow/surfaces/mcp/cli_metadata.py

PYTHONPATH='Code&DBs/Workflow' .venv/bin/python -m pytest \
  Code\&DBs/Workflow/tests/unit/test_object_truth_ingestion.py \
  Code\&DBs/Workflow/tests/unit/test_object_truth_ingestion_operation.py \
  Code\&DBs/Workflow/tests/unit/test_object_truth_ingestion_repository.py \
  Code\&DBs/Workflow/tests/unit/test_object_truth_mcp_tool.py \
  Code\&DBs/Workflow/tests/unit/test_operation_catalog_mounting.py \
  Code\&DBs/Workflow/tests/unit/test_workflow_migration_authority_contract.py \
  Code\&DBs/Workflow/tests/unit/test_mcp_docs_and_metadata.py -q
```

Result: `65 passed in 0.80s`.

The API server was restarted and health checked successfully after the code
change.

## Live Proof

Operation forge previews:

- record command: `f232483b-445b-460e-b4ad-6bf5cc6f1fb6`
- read query: `831189bc-8710-4c4e-a440-e8ee5fc41d5c`

Schema snapshots used as ingestion anchors:

- Salesforce Account: `ba334f59-f980-488d-b269-367226b60e6a`
- HubSpot Company: `4e4b2091-6ac0-4118-b8bb-a8609fde48b9`

MCP writes:

- Salesforce sample write receipt: `5fefc683-612f-4dda-a800-267e80849e1c`
- Salesforce event: `bf07f533-af2f-4d21-8c2e-aed4926663ff`
- Salesforce sample: `object_truth_sample.312007d95199270d`
- Salesforce fixture digest: `c676126626ebf5ae210dcdbf2e6303e7cd818a738f48e61f572fc0de5ab1b676`
- HubSpot sample write receipt: `1d3a0b8c-432b-4668-8e9c-446ff8e91098`
- HubSpot event: `9d19e3e3-9427-435e-b3c6-2dc1e05eb9b7`
- HubSpot sample: `object_truth_sample.0f7ebcc039986e68`
- HubSpot fixture digest: `237494ebc3ec76f7b699dcba561efbfddc62375bdfa641e1e488119646dcb89d`

MCP readbacks:

- list receipt: `ba90deae-0bbb-4a1f-9137-5f5d01cebaae`
- Salesforce describe receipt: `c8d4e2b8-0ac6-4171-a128-61834eb3c4eb`
- HubSpot describe receipt: `309632af-6f09-4238-bdf9-8168f5746a4e`

HTTP proof:

- route catalog showed one GET and one POST under
  `/api/object-truth/ingestion/samples`
- HTTP GET list receipt: `1df6302b-1ba1-4eef-ae96-b240bcfe668f`
- HTTP GET describe receipt: `634fc384-a9eb-4f5f-9f69-b8ca9dc84075`
- HTTP POST write receipt: `d2c4bed8-5d86-4b8b-ac9d-bd2a74471064`
- HTTP POST event: `c7e7a792-ceb1-46e4-89b0-edec65a17528`
- HTTP sample: `object_truth_sample.5e4628bfacd5fe8b`
- HTTP fixture digest: `dfcc1738e640548b00af5d8b1d0f2aeb2a70df250a1d943bc97577a20bef1e0f`
- HTTP describe receipt: `3dc30bcf-485f-4362-841d-87e77ad1470b`

Redaction readback checks confirmed that live MCP and HTTP responses did not
return the raw proof values used in the samples:

- `owner@example.com`
- `private renewal terms`
- `sf-redact-002`
- `hs-redact-001`
- `http-owner@example.com`
- `http private renewal terms`
- `sf-http-001`

## Known Separate Warning

The route/docs checks still surface the pre-existing
`structured_documents.context_assemble` catalog warning. It is unrelated to
Object Truth ingestion and was not caused by Phase 2.

## Closeout Judgment

Phase 2 is closed in roadmap authority.

- closeout preview receipt: `bb6eb84e-d631-4b6f-b4b4-16b860232ffc`
- closeout commit receipt: `65b928df-b63d-4ae7-93b9-dfa3bb93d87d`
- closeout event: `144580ab-f523-4bb2-b853-e6cff73c0588`
- roadmap readback: status `completed`, lifecycle `completed` as of
  `2026-04-30T16:54:56.527988+00:00`

The ingestion layer is no longer a local-only helper; it is a receipt-backed
authority path with durable writes, queryable readbacks, redaction-safe replay
fixtures, MCP tools, HTTP routes, and focused test coverage.
