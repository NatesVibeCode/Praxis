# Object Truth Trust Toolbelt Build Plan

Status: standalone build plan, grounded in live Praxis authority on 2026-04-28.

## Verdict

Build this as a new authority domain:

```text
authority.object_truth
```

The system should create durable object truth from cross-system evidence, then
materialize task environment contracts that define what success means for
business work.

The LLM is allowed to reason over structured evidence. It is not allowed to be
the parser, comparator, source-of-truth resolver, or hidden state machine.

## Existing Substrate To Reuse

| Capability | Reuse path | Role in this project |
| --- | --- | --- |
| Deterministic data operations | `/Users/nate/Praxis/Code&DBs/Workflow/core/data_ops.py` | Normalize, profile, reconcile, sync, validate, and compare record sets. |
| Data job contracts | `/Users/nate/Praxis/Code&DBs/Workflow/contracts/data_contracts.py` | Fail-closed job shape normalization and operation allowlists. |
| Data runtime | `/Users/nate/Praxis/Code&DBs/Workflow/runtime/data_plane.py` | Workspace-safe IO, registry-backed manifests, receipts, and workflow specs. |
| Integration manifests | `/Users/nate/Praxis/Code&DBs/Workflow/runtime/integration_manifest.py` | Starting point for connector capabilities and HTTP action definitions. |
| Object lifecycle | `/Users/nate/Praxis/Code&DBs/Workflow/runtime/object_lifecycle.py` | Projection/display layer only, not object truth authority. |
| Data dictionary | `/Users/nate/Praxis/Code&DBs/Workflow/storage/postgres/data_dictionary_repository.py` | Publish inferred schemas and fields into layered `auto` / `inferred` / `operator` authority. |
| Dictionary lineage | `/Users/nate/Praxis/Code&DBs/Workflow/storage/postgres/data_dictionary_lineage_repository.py` | Capture `same_as`, `derives_from`, `ingests_from`, `projects_to`, and field-level edges. |
| Typed gaps | `/Users/nate/Praxis/Code&DBs/Workflow/runtime/typed_gap_events.py` | Emit unresolved ambiguity as durable events. |
| Pattern authority | `/Users/nate/Praxis/Code&DBs/Workflow/runtime/platform_patterns.py` | Feed repeated failures and anti-patterns back into contract evolution. |
| CQRS gateway | `/Users/nate/Praxis/Code&DBs/Workflow/runtime/operation_catalog_gateway.py` | One authoritative operation front door with receipts and events. |
| Operation bindings | `/Users/nate/Praxis/Code&DBs/Workflow/runtime/operation_catalog_bindings.py` | Import-resolved handler and Pydantic model binding. |
| Primitive catalog | `/Users/nate/Praxis/Code&DBs/Workflow/runtime/primitive_authority.py` | Declare this authority as a reusable platform primitive and scan for drift. |
| Provider routing | `/Users/nate/Praxis/Code&DBs/Workflow/runtime/task_type_router.py` | Route only model-backed task types through DB authority. |

## New Runtime Package

Create:

```text
/Users/nate/Praxis/Code&DBs/Workflow/core/object_truth_ops.py
/Users/nate/Praxis/Code&DBs/Workflow/runtime/object_truth/__init__.py
/Users/nate/Praxis/Code&DBs/Workflow/runtime/object_truth/models.py
/Users/nate/Praxis/Code&DBs/Workflow/runtime/object_truth/schema_normalizer.py
/Users/nate/Praxis/Code&DBs/Workflow/runtime/object_truth/sample_capture.py
/Users/nate/Praxis/Code&DBs/Workflow/runtime/object_truth/value_normalizer.py
/Users/nate/Praxis/Code&DBs/Workflow/runtime/object_truth/field_observation.py
/Users/nate/Praxis/Code&DBs/Workflow/runtime/object_truth/identity_resolution.py
/Users/nate/Praxis/Code&DBs/Workflow/runtime/object_truth/comparison.py
/Users/nate/Praxis/Code&DBs/Workflow/runtime/object_truth/contract_compiler.py
/Users/nate/Praxis/Code&DBs/Workflow/runtime/object_truth/task_environment_contracts.py
/Users/nate/Praxis/Code&DBs/Workflow/runtime/object_truth/gaps.py
/Users/nate/Praxis/Code&DBs/Workflow/storage/postgres/object_truth_repository.py
```

Rule:

| Layer | Owns |
| --- | --- |
| `core/object_truth_ops.py` | Pure deterministic parse, path flattening, hashing, comparison, scoring, and contract digest helpers. No IO. |
| `runtime/object_truth/*` | Orchestration, integration calls, receipts, typed gaps, dictionary projection, and workflow behavior. |
| `storage/postgres/object_truth_repository.py` | All object truth table writes and reads. |
| `surfaces/*` | Parse operator input, dispatch gateway operation, render result. No domain writes. |

## New DB Migrations

Use the next migration numbers at implementation time. The current planned
shape is:

```text
/Users/nate/Praxis/Code&DBs/Databases/migrations/workflow/314_object_truth_authority.sql
/Users/nate/Praxis/Code&DBs/Databases/migrations/workflow/315_task_environment_contract_authority.sql
/Users/nate/Praxis/Code&DBs/Databases/migrations/workflow/316_object_truth_operation_registry.sql
```

Before writing migration files, run the migration authority generator/checker
path used by the repo so numbers and generated authority stay canonical.

## Authority Tables

### `external_system_snapshots`

Purpose: one observed state of a connected system.

Required fields:

| Field | Purpose |
| --- | --- |
| `system_snapshot_id` | Stable primary key. |
| `integration_id` | Integration authority reference. |
| `connector_ref` | Connector or manifest reference. |
| `tenant_ref` | Client or workspace tenant boundary. |
| `auth_context_hash` | Hash of credential context, not credential material. |
| `captured_at` | Observation time. |
| `capture_receipt_id` | Gateway receipt proving capture. |
| `schema_snapshot_count` | Count of schema snapshots in this system view. |
| `metadata` | Source-specific metadata. |

### `external_object_schema_snapshots`

Purpose: versioned schema evidence from an external system.

Required fields:

| Field | Purpose |
| --- | --- |
| `schema_snapshot_id` | Stable primary key. |
| `system_snapshot_id` | Parent system snapshot. |
| `integration_id` | Integration authority reference. |
| `external_object_name` | Native object/table/API resource name. |
| `schema_hash` | Deterministic hash of normalized schema. |
| `raw_schema` | Raw schema payload, redacted if needed. |
| `normalized_schema` | Canonical field/type/path representation. |
| `observed_at` | Observation time. |
| `receipt_id` | Gateway receipt. |

### `external_object_samples`

Purpose: one sample pull from an external object.

Required fields:

| Field | Purpose |
| --- | --- |
| `sample_id` | Stable primary key. |
| `schema_snapshot_id` | Schema used when sampling. |
| `integration_id` | Integration authority reference. |
| `business_object_ref` | Candidate business object, such as account, company, contact, order. |
| `external_object_name` | Native object name. |
| `sample_strategy` | `recent`, `claimed_source_truth`, `matching_ids`, `random_window`, `operator_supplied`, or `fixture`. |
| `sample_size_requested` | Requested row count. |
| `sample_size_returned` | Actual row count. |
| `source_query` | Filter/query/cursor metadata. |
| `sample_hash` | Deterministic hash over normalized sample envelope. |
| `status` | `captured`, `empty`, `partial`, `failed`, `superseded`. |
| `receipt_id` | Gateway receipt. |

### `external_object_versions`

Purpose: one observed version of one external record.

Required fields:

| Field | Purpose |
| --- | --- |
| `object_version_id` | Stable primary key. |
| `sample_id` | Sample that produced it. |
| `integration_id` | Source system. |
| `external_object_name` | Native object name. |
| `external_record_id` | Native record id when present. |
| `identity_key_digest` | Deterministic candidate identity hash. |
| `raw_payload_hash` | Hash of raw payload. |
| `normalized_payload_hash` | Hash of normalized payload. |
| `raw_payload_ref` | Optional pointer to encrypted/redacted payload storage. |
| `normalized_payload` | Canonical normalized JSON. |
| `source_created_at` | Source-created timestamp when known. |
| `source_updated_at` | Source-updated timestamp when known. |
| `source_actor_ref` | Last actor/automation/user when known. |
| `source_version_ref` | ETag, revision id, sequence, or updated token. |

### `external_field_observations`

Purpose: one field-path observation from one object version.

Required fields:

| Field | Purpose |
| --- | --- |
| `field_observation_id` | Stable primary key. |
| `object_version_id` | Parent object version. |
| `field_path` | Canonical dot/bracket path. |
| `field_kind` | Canonical type. |
| `value_presence` | `present`, `null`, `missing`, `empty`. |
| `normalized_value_hash` | Hash of normalized value. |
| `redacted_value_preview` | Safe preview, never secret/full PII by default. |
| `cardinality_kind` | `scalar`, `array`, `object`, `flattened`, `reference`. |
| `source_updated_at` | Field/source freshness signal when available. |
| `metadata` | Field-level source hints. |

### `external_object_identity_clusters`

Purpose: inferred same-real-world-object groups.

Required fields:

| Field | Purpose |
| --- | --- |
| `identity_cluster_id` | Stable primary key. |
| `business_object_ref` | Object kind being resolved. |
| `canonical_subject_ref` | Optional operator-confirmed subject. |
| `confidence` | 0.0 to 1.0. |
| `status` | `candidate`, `confirmed`, `rejected`, `split`, `superseded`. |
| `created_by_run_ref` | Comparison or resolver run. |

### `external_object_identity_links`

Purpose: evidence behind cluster membership.

Required fields:

| Field | Purpose |
| --- | --- |
| `identity_link_id` | Stable primary key. |
| `identity_cluster_id` | Parent cluster. |
| `object_version_id` | Member object version. |
| `match_method` | `external_id`, `email`, `domain`, `composite_key`, `operator`, `llm_suggested_reviewed`, etc. |
| `confidence` | 0.0 to 1.0. |
| `evidence` | Machine-readable match evidence. |
| `status` | `candidate`, `accepted`, `rejected`, `superseded`. |

### `object_truth_comparison_runs`

Purpose: one deterministic comparison across sampled evidence.

Required fields:

| Field | Purpose |
| --- | --- |
| `comparison_run_id` | Stable primary key. |
| `business_object_ref` | Object kind being compared. |
| `sample_ids` | Included samples. |
| `strategy_ref` | Comparison strategy. |
| `thresholds` | Required confidence/freshness thresholds. |
| `input_hash` | Hash of all inputs and strategy. |
| `result_hash` | Hash of output. |
| `status` | `completed`, `completed_with_gaps`, `failed`, `superseded`. |
| `receipt_id` | Gateway receipt. |

### `object_truth_field_comparisons`

Purpose: field-level comparison results.

Required fields:

| Field | Purpose |
| --- | --- |
| `field_comparison_id` | Stable primary key. |
| `comparison_run_id` | Parent run. |
| `identity_cluster_id` | Object group compared. |
| `field_path` | Canonical field path. |
| `agreement_state` | `same`, `different`, `missing_left`, `missing_right`, `incomparable`, `ambiguous`. |
| `freshness_state` | `left_newer`, `right_newer`, `equal`, `unknown`. |
| `transform_signal` | Detected transform, mapping, normalization, or none. |
| `hierarchy_signal` | Flattening/nesting/reference evidence. |
| `source_authority_signal` | Evidence toward who currently owns field truth. |
| `confidence` | 0.0 to 1.0. |
| `evidence` | Machine-readable supporting observations. |

### `object_truth_contracts`

Purpose: durable truth contract for one business object.

Required fields:

| Field | Purpose |
| --- | --- |
| `object_truth_contract_id` | Stable primary key. |
| `business_object_ref` | Object kind/process subject. |
| `status` | `draft`, `observing`, `confirmed`, `deprecated`, `rejected`. |
| `current_revision_id` | Current append-only revision. |
| `contract_hash` | Hash of current contract body. |
| `confidence` | Overall confidence. |
| `created_from_comparison_run_id` | Evidence source. |
| `verifier_refs` | Required verifiers. |
| `metadata` | Contract metadata. |

### `object_truth_contract_revisions`

Purpose: append-only revision history.

Required fields:

| Field | Purpose |
| --- | --- |
| `contract_revision_id` | Stable primary key. |
| `object_truth_contract_id` | Contract head. |
| `revision_no` | Monotonic revision number. |
| `parent_revision_id` | Prior revision. |
| `contract_hash` | Hash of revision body. |
| `contract_body` | Identity, field, source authority, transform, hierarchy, and verifier rules. |
| `source_comparison_run_id` | Evidence source. |
| `change_reason` | Why revision exists. |
| `created_by` | `praxis_object_truth`, operator, or workflow run. |

### `task_environment_contracts`

Purpose: current head for task success meaning.

Required fields:

| Field | Purpose |
| --- | --- |
| `task_environment_contract_id` | Stable primary key. |
| `task_environment_ref` | Process/task/environment subject. |
| `status` | `draft`, `active`, `superseded`, `revoked`, `expired`. |
| `current_revision_id` | Current revision. |
| `current_contract_hash` | Hash of current revision. |
| `object_truth_contract_refs` | Bound object truth contracts. |
| `sop_refs` | Bound SOPs. |
| `pattern_refs` | Bound patterns and anti-patterns. |
| `verifier_refs` | Required verifier set. |
| `model_policy_ref` | Allowed model/task routing policy. |

### `task_environment_contract_revisions`

Purpose: append-only task contract revisions.

Required fields:

| Field | Purpose |
| --- | --- |
| `task_contract_revision_id` | Stable primary key. |
| `task_environment_contract_id` | Contract head. |
| `revision_no` | Monotonic revision number. |
| `parent_revision_id` | Prior revision. |
| `contract_hash` | Hash of revision body. |
| `dependency_hash` | Hash of SOPs, patterns, truth contracts, model policy, verifier refs, and sampled evidence refs. |
| `contract_body` | Machine-readable success contract. |
| `source_run_id` | Workflow run or operation that materialized it. |
| `change_reason` | What changed. |

## CQRS Operations

Run `praxis_operation_forge` before adding each operation. Register with
`praxis_register_operation` after handlers and Pydantic inputs import cleanly.

Use:

```text
/Users/nate/Praxis/Code&DBs/Workflow/runtime/operations/models/object_truth.py
/Users/nate/Praxis/Code&DBs/Workflow/runtime/operations/queries/object_truth.py
/Users/nate/Praxis/Code&DBs/Workflow/runtime/operations/commands/object_truth.py
```

### Query Operations

| Operation name | Operation ref | Handler | Input model | Policy |
| --- | --- | --- | --- | --- |
| `object_truth.schema_snapshot_get` | `object-truth-schema-snapshot-get` | `runtime.operations.queries.object_truth.handle_schema_snapshot_get` | `runtime.operations.models.object_truth.SchemaSnapshotGetQuery` | `read_only` |
| `object_truth.sample_list` | `object-truth-sample-list` | `runtime.operations.queries.object_truth.handle_sample_list` | `runtime.operations.models.object_truth.SampleListQuery` | `read_only` |
| `object_truth.object_version_list` | `object-truth-object-version-list` | `runtime.operations.queries.object_truth.handle_object_version_list` | `runtime.operations.models.object_truth.ObjectVersionListQuery` | `read_only` |
| `object_truth.identity_cluster_get` | `object-truth-identity-cluster-get` | `runtime.operations.queries.object_truth.handle_identity_cluster_get` | `runtime.operations.models.object_truth.IdentityClusterGetQuery` | `read_only` |
| `object_truth.comparison_run_get` | `object-truth-comparison-run-get` | `runtime.operations.queries.object_truth.handle_comparison_run_get` | `runtime.operations.models.object_truth.ComparisonRunGetQuery` | `read_only` |
| `object_truth.contract_get` | `object-truth-contract-get` | `runtime.operations.queries.object_truth.handle_contract_get` | `runtime.operations.models.object_truth.ContractGetQuery` | `read_only` |
| `object_truth.task_environment_contract_get` | `object-truth-task-environment-contract-get` | `runtime.operations.queries.object_truth.handle_task_environment_contract_get` | `runtime.operations.models.object_truth.TaskEnvironmentContractGetQuery` | `read_only` |
| `object_truth.gaps` | `object-truth-gaps` | `runtime.operations.queries.object_truth.handle_gaps` | `runtime.operations.models.object_truth.ObjectTruthGapsQuery` | `read_only` |
| `object_truth.readiness` | `object-truth-readiness` | `runtime.operations.queries.object_truth.handle_readiness` | `runtime.operations.models.object_truth.ObjectTruthReadinessQuery` | `read_only` |

### Command Operations

| Operation name | Operation ref | Handler | Input model | Idempotency | Event |
| --- | --- | --- | --- | --- | --- |
| `object_truth.capture_schema` | `object-truth-capture-schema` | `runtime.operations.commands.object_truth.handle_capture_schema` | `CaptureSchemaCommand` | `idempotent` by integration, object, schema hash | `object_truth.schema_captured` |
| `object_truth.capture_sample` | `object-truth-capture-sample` | `runtime.operations.commands.object_truth.handle_capture_sample` | `CaptureSampleCommand` | `idempotent` by integration, object, source query, cursor window | `object_truth.sample_captured` |
| `object_truth.normalize_sample` | `object-truth-normalize-sample` | `runtime.operations.commands.object_truth.handle_normalize_sample` | `NormalizeSampleCommand` | `idempotent` by sample hash and normalizer version | `object_truth.sample_normalized` |
| `object_truth.extract_field_observations` | `object-truth-extract-field-observations` | `runtime.operations.commands.object_truth.handle_extract_field_observations` | `ExtractFieldObservationsCommand` | `idempotent` by object versions and extractor version | `object_truth.field_observations_extracted` |
| `object_truth.resolve_identity` | `object-truth-resolve-identity` | `runtime.operations.commands.object_truth.handle_resolve_identity` | `ResolveIdentityCommand` | `idempotent` by sample set and resolver version | `object_truth.identity_resolved` |
| `object_truth.compare_fields` | `object-truth-compare-fields` | `runtime.operations.commands.object_truth.handle_compare_fields` | `CompareFieldsCommand` | `idempotent` by comparison input hash | `object_truth.comparison_completed` |
| `object_truth.propose_contract` | `object-truth-propose-contract` | `runtime.operations.commands.object_truth.handle_propose_contract` | `ProposeContractCommand` | `idempotent` by comparison run and prompt contract hash | `object_truth.contract_proposed` |
| `object_truth.append_contract_revision` | `object-truth-append-contract-revision` | `runtime.operations.commands.object_truth.handle_append_contract_revision` | `AppendContractRevisionCommand` | `non_idempotent` unless explicit idempotency key | `object_truth.contract_revision_appended` |
| `object_truth.materialize_task_environment_contract` | `object-truth-materialize-task-environment-contract` | `runtime.operations.commands.object_truth.handle_materialize_task_environment_contract` | `MaterializeTaskEnvironmentContractCommand` | `idempotent` by dependency hash | `task_environment_contract.materialized` |
| `object_truth.emit_typed_gaps` | `object-truth-emit-typed-gaps` | `runtime.operations.commands.object_truth.handle_emit_typed_gaps` | `EmitObjectTruthGapsCommand` | `idempotent` by gap fingerprint | `typed_gap.created` via helper |
| `object_truth.run_discovery` | `object-truth-run-discovery` | `runtime.operations.commands.object_truth.handle_run_discovery` | `RunObjectTruthDiscoveryCommand` | `non_idempotent`, orchestrates child receipts | `object_truth.discovery_run_started` |

## Registry Rows

Every operation must be registered through `praxis_register_operation`, not
manual triple-row SQL.

Required registry surfaces:

| Registry | Required rows |
| --- | --- |
| `authority_domains` | `authority.object_truth` |
| `operation_catalog_registry` | One row per operation above. |
| `authority_object_registry` | Tables, operations, events, projections. |
| `data_dictionary_objects` | Tables, command/query operation objects, event payload objects, projections. |
| `authority_event_contracts` | All command events listed above. |
| `primitive_catalog` | One primitive row for object truth authority and one for task environment contract authority. |

Important convention:

Even query operations have historically needed companion dictionary/object rows
using the existing operation convention. Do not guess `query` vs `command`
categories manually. Let `praxis_register_operation` write the triple and add a
regression test for whatever it produces.

## MCP and CLI

Create a thin MCP wrapper:

```text
/Users/nate/Praxis/Code&DBs/Workflow/surfaces/mcp/tools/object_truth.py
```

Update catalog metadata:

```text
/Users/nate/Praxis/Code&DBs/Workflow/surfaces/mcp/cli_metadata.py
```

Optional CLI wrapper:

```text
/Users/nate/Praxis/Code&DBs/Workflow/surfaces/cli/commands/object_truth.py
```

Tool name:

```text
praxis_object_truth
```

Allowed actions:

| Action | Dispatches to |
| --- | --- |
| `inspect` | `object_truth.readiness` and related read operations |
| `capture_schema` | `object_truth.capture_schema` |
| `capture_sample` | `object_truth.capture_sample` |
| `normalize` | `object_truth.normalize_sample` |
| `extract_fields` | `object_truth.extract_field_observations` |
| `resolve_identity` | `object_truth.resolve_identity` |
| `compare` | `object_truth.compare_fields` |
| `propose_contract` | `object_truth.propose_contract` |
| `append_contract_revision` | `object_truth.append_contract_revision` |
| `materialize_task_contract` | `object_truth.materialize_task_environment_contract` |
| `gaps` | `object_truth.gaps` |
| `run` | `object_truth.run_discovery` |

The wrapper should only validate action shape, call
`execute_operation_from_subsystems`, and return the receipt-backed result.

## HTTP Surface

Do not add static object truth HTTP handlers as a second authority.

If UI needs HTTP access, use operation catalog mounted routes. If a temporary
handler is unavoidable, place it under:

```text
/Users/nate/Praxis/Code&DBs/Workflow/surfaces/api/handlers/object_truth.py
```

and make it a gateway-dispatch wrapper only.

This depends on the existing roadmap item:

```text
roadmap_item.gateway.dispatch.static.integration.and.picker.http.surfaces
```

## Scripts

Scripts are smoke and bootstrap helpers only. No script owns business behavior.

Create:

```text
/Users/nate/Praxis/Code&DBs/Workflow/scripts/object_truth_smoke.py
```

Responsibilities:

| Script responsibility | Rule |
| --- | --- |
| Fixture end-to-end run | Calls gateway operations only. |
| Smoke verification | Asserts receipts, events, tables, and hashes exist. |
| No direct DB writes | Repository writes happen through operation handlers. |
| No real client data | Fixtures only. |

## Test Plan

Unit tests:

```text
/Users/nate/Praxis/tests/unit/test_object_truth_schema_normalizer.py
/Users/nate/Praxis/tests/unit/test_object_truth_field_observations.py
/Users/nate/Praxis/tests/unit/test_object_truth_identity_resolution.py
/Users/nate/Praxis/tests/unit/test_object_truth_comparison.py
/Users/nate/Praxis/tests/unit/test_object_truth_contract_compiler.py
/Users/nate/Praxis/tests/unit/test_task_environment_contracts.py
/Users/nate/Praxis/tests/unit/test_object_truth_mcp_tool.py
```

Integration tests:

```text
/Users/nate/Praxis/tests/integration/test_object_truth_authority_schema.py
/Users/nate/Praxis/tests/integration/test_object_truth_gateway_receipts.py
/Users/nate/Praxis/tests/integration/test_object_truth_workflow_task_types.py
/Users/nate/Praxis/tests/integration/test_object_truth_end_to_end_fixture.py
```

Fixtures:

```text
/Users/nate/Praxis/tests/fixtures/object_truth/salesforce_account_sample.json
/Users/nate/Praxis/tests/fixtures/object_truth/hubspot_company_sample.json
/Users/nate/Praxis/tests/fixtures/object_truth/netsuite_customer_sample.json
```

Validation command family:

```text
./scripts/test.sh suite focus
./scripts/test.sh check-affected
./scripts/test.sh validate <queue-file>
```

Parse the JSON envelope exactly:

```text
ok, command, duration_s, results, errors, warnings
```

## Rollout Phases

| Phase | Name | Outcome |
| --- | --- | --- |
| 0 | Gating cleanup | Confirm DB connection health, provider route truth, pattern materialization status, and gateway-only integration path. |
| 1 | Authority schema | Add DB tables, repository, authority domain, event contracts, and primitive catalog rows. |
| 2 | Deterministic observation | Capture schema/sample fixtures, normalize object versions, extract field observations, and hash everything. |
| 3 | Identity and comparison | Build deterministic identity clusters, field comparisons, freshness scoring, hierarchy/flattening signals. |
| 4 | Object truth contracts | Compile draft contracts from comparison evidence, emit typed gaps for ambiguity, append revisions. |
| 5 | Task environment contracts | Materialize hashed task contracts from SOPs, object truth, patterns, verifiers, and model/tool policy. |
| 6 | Workflow integration | Add task types, route model-backed tasks, verify deterministic task gates. |
| 7 | MCP/CLI/UI | Expose `praxis_object_truth`, operation catalog routes, and minimal UI read surfaces. |
| 8 | Pattern feedback loop | Link verifier failures, typed gaps, and repeated object truth failures into pattern authority. |
| 9 | Client pilot | Use fixtures first, then one low-risk real object/process with operator approval. |

## Definition Of Done

The first release is done when:

| Proof | Required result |
| --- | --- |
| Fixture E2E | Two or more mock systems can be sampled, normalized, compared, and contracted. |
| Receipts | Every command produces `authority_operation_receipts`. |
| Events | Every command event appears in `authority_events`. |
| Hashes | Samples, normalized records, comparison inputs, comparison outputs, contracts, and task contracts are hash-addressed. |
| Typed gaps | Ambiguity becomes `typed_gap.created`, not prose residue. |
| Provider truth | LLM tasks use only currently runnable provider routes. |
| Verifiers | Contract verification can fail closed and link proof. |
| Registry | Primitive consistency scan includes object truth rows. |

