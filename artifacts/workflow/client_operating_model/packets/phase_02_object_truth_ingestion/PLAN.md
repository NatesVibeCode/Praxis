# Phase 2: Object Truth Ingestion and Evidence Capture

## Verdict

`READY FOR BOUNDED BUILD DESIGN`

The generated Phase 2 packet incorrectly claimed no Object Truth implementation exists. That is false. The repo already has deterministic object-truth primitives, Postgres persistence, migrations, CQRS command/query handlers, CLI/MCP/API surfaces, and operation registrations.

This corrected packet treats the existing implementation as substrate and defines what must be extended to support full client-system ingestion.

## Authority

Parent roadmap item:

- `roadmap_item.object.truth.trust.toolbelt.authority`

Phase 0 gate:

- `artifacts/workflow/client_operating_model/packets/phase_00_authority_readiness/PLAN.md`

Phase 1 dependency:

- `artifacts/workflow/client_operating_model/packets/phase_01_client_system_discovery/PLAN.md`

Authority boundary:

- Object Truth is the evidence authority for observed client-system facts.
- It observes, normalizes, hashes, compares, persists, and exposes evidence.
- It does not decide business truth without evidence and does not execute automations.
- Virtual Lab consumes Object Truth evidence later to emulate consequences.

## Existing Implementation

Deterministic core:

- `Code&DBs/Workflow/core/object_truth_ops.py`

Current core capabilities:

- canonical JSON value normalization
- purpose-scoped SHA-256 digests
- schema snapshot normalization
- object-version packet construction
- identity digest construction from required field paths
- source metadata normalization
- field observation extraction
- hierarchy/flattening signal detection
- object-version comparison
- sensitive-field detection for obvious secret fields

Storage:

- `Code&DBs/Workflow/storage/postgres/object_truth_repository.py`

Current storage capabilities:

- persist object versions into `object_truth_object_versions`
- persist field observations into `object_truth_field_observations`
- persist schema snapshots into `object_truth_schema_snapshots`
- load object versions by digest
- persist comparison runs into `object_truth_comparison_runs`

CQRS handlers:

- `Code&DBs/Workflow/runtime/operations/queries/object_truth.py`
- `Code&DBs/Workflow/runtime/operations/commands/object_truth.py`

Current operations:

- `object_truth_observe_record`
- `object_truth_store_observed_record`
- `object_truth_store_schema_snapshot`
- `object_truth_compare_versions`
- `object_truth_record_comparison_run`

Registered migrations:

- `318_register_object_truth_observe_record_operation.sql`
- `320_object_truth_evidence_store.sql`
- `321_register_object_truth_store_observed_record_operation.sql`
- `322_object_truth_schema_snapshot_store.sql`
- `323_register_object_truth_schema_and_compare_operations.sql`
- `324_object_truth_comparison_run_store.sql`
- `325_register_object_truth_record_comparison_run_operation.sql`

Operator surfaces:

- `praxis workflow object-truth`
- `praxis workflow object-truth-store`
- `praxis workflow object-truth-store-schema`
- `praxis workflow object-truth-compare`
- `praxis workflow object-truth-record-comparison`

API surfaces:

- `POST /api/object-truth/observe-record`
- `POST /api/object-truth/store-observed-record`
- `POST /api/object-truth/store-schema-snapshot`
- `GET /api/object-truth/compare-versions`
- `POST /api/object-truth/record-comparison-run`

## Existing Gaps

The current substrate handles inline record/schema evidence. It does not yet provide the full ingestion layer required for client operating model discovery.

Missing capabilities:

- client-scoped system snapshot records
- connector/discovery-run linkage to Phase 1 system census
- sample capture orchestration across connectors
- source query/cursor/window evidence
- raw payload storage references with privacy classification
- redacted preview policy beyond obvious sensitive field names
- record sample batches and sample strategy tracking
- source-created/source-updated/source-actor/source-version metadata normalization
- identity clusters across multiple systems
- lineage edges into data dictionary and task environment contracts
- object-truth readiness read model
- ingestion replay fixtures by system/object/sample strategy
- promotion path from observed evidence to Virtual Lab state

## Target Data Flow

Phase 2 should extend the current primitives into this flow:

```text
client system census
  -> connector/object catalog
  -> schema snapshot
  -> sample capture
  -> object version packets
  -> field observations
  -> identity/source metadata evidence
  -> comparison runs
  -> typed gaps
  -> Object Truth readiness/read models
```

## Required Extensions

### 1. System Snapshot

Add durable records for one observed state of a client system.

Required fields:

- `system_snapshot_id`
- `client_ref`
- `system_ref`
- `integration_id`
- `connector_ref`
- `environment_ref`
- `auth_context_hash`
- `captured_at`
- `capture_receipt_id`
- `schema_snapshot_count`
- `sample_count`
- `metadata_json`

### 2. Sample Capture

Add durable records for sampled external objects.

Required fields:

- `sample_id`
- `system_snapshot_id`
- `schema_snapshot_digest`
- `system_ref`
- `object_ref`
- `sample_strategy`
- `source_query_json`
- `cursor_ref`
- `sample_size_requested`
- `sample_size_returned`
- `sample_hash`
- `status`
- `receipt_id`

Sample strategies:

- `recent`
- `claimed_source_truth`
- `matching_ids`
- `random_window`
- `operator_supplied`
- `fixture`

### 3. Object Version Metadata

Extend object-version evidence with source-aware fields where available:

- `external_record_id`
- `source_created_at`
- `source_updated_at`
- `source_actor_ref`
- `source_version_ref`
- `raw_payload_hash`
- `normalized_payload_hash`
- `raw_payload_ref`
- `privacy_classification`
- `retention_policy_ref`

Keep raw payload content out of ordinary workflow artifacts unless explicitly approved.

### 4. Redacted Preview Policy

The existing sensitive-field pattern catches obvious secret names. Phase 2 needs a stronger policy:

- classify fields as `public`, `internal`, `confidential`, or `restricted`
- redact tokens, secrets, credentials, personal identifiers, free-text sensitive content, and policy-marked fields
- store redacted preview JSON separately from raw payload references
- preserve structure and field presence while hiding sensitive values
- test redaction deterministically

### 5. Readiness Query

Add `object_truth.readiness` as a read-only gateway operation.

It should answer:

- are required Object Truth tables present
- are operation catalog entries registered
- is DB pressure safe for the requested fanout
- are privacy policies available
- are connector/source references valid
- are open typed gaps blocking ingestion
- is the current evidence fresh enough for downstream phases

Downstream waves should fail closed on readiness values `blocked`, `unknown`, or `revoked`.

### 6. Typed Gaps

Do not create a private gap table unless the broader typed-gap authority demands it. Emit durable `typed_gap.created` events for:

- missing schema
- missing identity field
- missing credential scope
- unsafe payload classification
- sample too small
- source metadata unavailable
- connector limit unknown
- comparison input missing
- source freshness ambiguous

## CQRS Work

New query operations:

- `object_truth.readiness`
- `object_truth.system_snapshot_get`
- `object_truth.sample_get`
- `object_truth.sample_list`
- `object_truth.object_version_list`
- `object_truth.field_observation_list`

New command operations:

- `object_truth.capture_system_snapshot`
- `object_truth.capture_sample`
- `object_truth.attach_raw_payload_ref`
- `object_truth.classify_payload`
- `object_truth.emit_ingestion_gaps`

All new operations must be registered in:

- `operation_catalog_registry`
- `authority_object_registry`
- `data_dictionary_objects`

Command operations that mutate authority must emit events and receipts through the CQRS gateway.

## Tests

Required unit tests:

- deterministic schema digest is stable across key order changes
- object version digest is stable for canonical-equivalent records
- identity digest fails closed when required identity fields are missing
- field observation extraction handles nested objects, arrays, nulls, empty strings, and flattened keys
- redaction hides restricted values while preserving field paths
- source metadata normalization handles timestamps and unknowns

Required integration tests:

- `object_truth_store_schema_snapshot` persists schema snapshot and emits event
- `object_truth_store_observed_record` persists object version and field observations
- `object_truth_compare_versions` loads persisted versions and reports field-level differences
- `object_truth_record_comparison_run` persists comparison evidence
- `object_truth.readiness` reports blocked when required tables or privacy policy are missing

Fixture tests:

- Salesforce account sample
- HubSpot company sample
- NetSuite customer sample
- sparse object
- malformed object
- redaction-heavy object
- same object observed across two systems

## Validation Commands

Use these before build:

- `praxis workflow tools describe praxis_object_truth`
- `praxis workflow tools describe praxis_object_truth_store`
- `praxis workflow tools describe praxis_object_truth_store_schema_snapshot`
- `praxis workflow tools describe praxis_object_truth_compare_versions`
- `praxis workflow tools describe praxis_object_truth_record_comparison_run`
- `rg -n "object_truth_" Code&DBs/Databases/migrations/workflow Code&DBs/Workflow`

Use these after build:

- focused unit tests for `core/object_truth_ops.py`
- focused repository tests for `storage/postgres/object_truth_repository.py`
- gateway operation tests for every new command/query
- one fixture end-to-end ingest from schema snapshot to object version to comparison run

## Failure Containment

Stop Phase 2 build if:

- raw client payload storage is proposed without classification and retention policy
- a surface writes directly without gateway receipt/event authority
- an LLM is asked to decide truth before deterministic parse/compare evidence exists
- connector discovery is inferred rather than linked to Phase 1 evidence
- schema/sample/object evidence cannot be replayed from stored inputs

## Downstream Handoff

Phase 3 may consume this packet only for MDM/source authority planning. It should assume:

- existing Object Truth primitives are useful but incomplete
- identity clusters and source authority are not yet fully built
- normalization must remain deterministic before LLM reasoning
- source authority must be evidence-backed, not hand-authored precedence sprawl
