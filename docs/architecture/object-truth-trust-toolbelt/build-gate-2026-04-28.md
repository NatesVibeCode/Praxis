# Object Truth Trust Toolbelt - Manual Build Gate Receipt - 2026-04-28

## Scope

Build the first deterministic substrate slice for Object Truth while workflow automation is being repaired in another lane.

This receipt covers:

- deterministic schema snapshot normalization
- deterministic record identity construction
- deterministic field observation extraction
- hierarchy and flattening signal capture
- deterministic object-version comparison
- append-only task environment contract candidate hashing

This receipt now covers the first runtime and persistence slice: deterministic object-version evidence can be observed, persisted, receipt-backed, evented, and called through MCP. It still does not cover connector ingestion, cross-system comparison runs, workflow task types, or model execution.

## Gate Results

| Gate | Result | Evidence |
| --- | --- | --- |
| Standing orders | Passed | Queried active `architecture_policy` rows through `praxis_operator_decisions`. Relevant policies require deterministic substrate before LLM judgment, append-only task environment contracts, CQRS gateway registration for runtime operations, and DB-backed authority for durable state. |
| Discovery | Passed | Queried `praxis_search` for object truth, schema normalization, identity, comparison, and contracts. The canonical decision found was `object_truth_requires_deterministic_parse_compare_substrate`. |
| Workspace boundary | Passed | All writes stayed under `/Users/nate/Praxis`. |
| Dirty worktree isolation | Passed | Existing dirty files and untracked runtime/migration work from another lane were left untouched. |
| CQRS gate | Deferred | No new operation, command, query, registry row, or MCP tool was created. Runtime operations must go through `praxis_operation_forge` / `praxis_register_operation` after the workflow/CQRS repair lane is stable. |
| DB gate | Passed for object-version evidence | Added durable `object_truth_object_versions` and `object_truth_field_observations` tables, registered both as data dictionary objects and authority objects under `authority.object_truth`, and applied migration `320_object_truth_evidence_store.sql`. |
| Workflow gate | Deferred | No workflow task type was registered because workflow repair is active elsewhere. This slice is pure deterministic core logic that can later back task types. |
| Model gate | Passed by non-use | No LLM judgment is involved. The only model-route shape created is a hashable allowed-route list inside the contract candidate. |
| Security/redaction gate | Passed for first slice | Sensitive field names are detected and previews are redacted while value digests remain available for equality comparison. |
| Verification gate | Passed for current slice | Object-truth focused tests passed with 7 tests. Registry-focused tests passed with 41 tests. `./scripts/test.sh selftest` returned JSON `ok: true` with no errors or warnings. |
| Discovery index gate | Passed | `praxis workflow discover reindex --yes` returned `ok: true`, indexed 7 changes, skipped 4518 unchanged items, and reported no errors. |
| Registry gate | Passed for observe-record query | `praxis_operation_forge` returned `ok_to_register: true`; `praxis_register_operation` registered `object_truth.query.observe_record` as query/read-only under `authority.object_truth` with receipt `d87d45a9-f7b3-4120-9aa4-de1f5168a030`. Forge readback now reports `state: existing_operation`. |
| Authority-domain wizard gate | Passed for first implementation | Added `authority_domain_forge` and `authority_domain_register` as gateway operations under `authority.cqrs`. Register receipts: `9fd00428-ab27-4588-925d-37afd913158b` and `4184fbe8-4b6b-4789-93f1-d812f1095837`. Gateway proof completed for both operations; `authority_domain_register` emitted event `017a9a0e-d964-483d-8eaf-37f824c0b4f3`. |
| Authority-domain MCP gate | Passed | Added `praxis_authority_domain_forge` and `praxis_register_authority_domain` as thin MCP wrappers over gateway operations. `praxis workflow tools describe` resolves both tools with aliases `authority-domain-forge` and `register-authority-domain`. Live MCP call receipt `8d1e3736-99be-415c-851f-61d46b3ceafb` completed for `authority_domain_forge`. |
| Object-truth MCP gate | Passed | Added `praxis_object_truth` as a thin read-only MCP wrapper over `object_truth_observe_record`. `praxis workflow tools describe praxis_object_truth` resolves alias `object-truth`; live MCP call completed with receipt `ad4baafc-46ca-47ff-a87d-0ff1dcd66275` under `authority.object_truth`. |
| Object-truth persistence gate | Passed | Added `object_truth_store_observed_record` as an idempotent CQRS command with event `object_truth.object_version_stored`, then exposed it as write MCP tool `praxis_object_truth_store` with alias `object-truth-store`. Live MCP call persisted object version `25908b008cd1920c5c6260afeddf62d1acc7178e8bbb4cdb4cd6ac44c2385a6a`, receipt `ef646603-9774-47bc-b1d6-38bf21599b66`, and event `8956617d-69d4-4c6a-be8d-f21401704e7b`. |
| Schema snapshot persistence gate | Passed | Added durable `object_truth_schema_snapshots`, registered it as data dictionary + authority object, and exposed `object_truth_store_schema_snapshot` as write MCP tool `praxis_object_truth_store_schema_snapshot` with alias `object-truth-store-schema`. Live call persisted schema snapshot `07e5ee3ce4797f484df5a01ad7e31257940e168b77ac72280f2a587db2ec663c`, receipt `02e9f7fa-3e85-4b3f-9f6f-c1426073bf1f`, and event `a4f2398b-a30b-42c4-abdb-5fc8a295364e`. |
| Persisted comparison gate | Passed | Added read-only CQRS query `object_truth_compare_versions` and MCP tool `praxis_object_truth_compare_versions` with alias `object-truth-compare`. Live comparison between Salesforce account and HubSpot company samples returned same identity, 1 matching field, 4 different fields, and `right_newer` freshness with receipt `4e29549e-9b3a-413e-b6ae-228887b5f3a0`. |
| Comparison-run authority gate | Passed | Added durable `object_truth_comparison_runs`, registered it as data dictionary + authority object, and exposed `object_truth_record_comparison_run` as write MCP tool `praxis_object_truth_record_comparison_run` with alias `object-truth-record-comparison`. Live call persisted comparison run `91986b96209a20e268eb5ddd0e80133fcd5139a8c334f2cfa1976331bf2b3709`, receipt `85a6cab9-d68e-4142-9296-8adf8633ebe8`, and event `f165cca1-c26b-4430-827e-21c7b4ddd14f`. |

## Authority Model

The new code is not the source of truth for client object truth. It is the deterministic evidence builder that later runtime authority can call.

The durable authority now has first-slice database backing for schema snapshots, object versions, field observations, and comparison runs. These parts still need to become database-backed:

- `object_truth.task_environment_contracts`
- `object_truth.failure_patterns`

The future runtime authority should expose query and command operations through the CQRS gateway only. MCP tools should remain thin wrappers over registered operations.

## Files Added

- `Code&DBs/Workflow/core/object_truth_ops.py`
- `Code&DBs/Workflow/runtime/operations/queries/object_truth.py`
- `Code&DBs/Workflow/runtime/operations/queries/authority_domain_forge.py`
- `Code&DBs/Workflow/runtime/operations/commands/authority_domain_register.py`
- `Code&DBs/Workflow/runtime/operations/commands/object_truth.py`
- `Code&DBs/Workflow/storage/postgres/object_truth_repository.py`
- `Code&DBs/Workflow/tests/unit/test_object_truth_ops.py`
- `Code&DBs/Workflow/tests/unit/test_object_truth_operation.py`
- `Code&DBs/Workflow/tests/unit/test_authority_domain_wizard.py`
- `Code&DBs/Workflow/tests/unit/test_authority_domain_mcp_tools.py`
- `Code&DBs/Workflow/tests/unit/test_object_truth_mcp_tool.py`
- `Code&DBs/Workflow/tests/unit/test_object_truth_store_operation.py`
- `Code&DBs/Workflow/tests/unit/test_object_truth_schema_and_compare_operations.py`
- `Code&DBs/Databases/migrations/workflow/318_register_object_truth_observe_record_operation.sql`
- `Code&DBs/Databases/migrations/workflow/319_register_authority_domain_wizard_operations.sql`
- `Code&DBs/Databases/migrations/workflow/320_object_truth_evidence_store.sql`
- `Code&DBs/Databases/migrations/workflow/321_register_object_truth_store_observed_record_operation.sql`
- `Code&DBs/Databases/migrations/workflow/322_object_truth_schema_snapshot_store.sql`
- `Code&DBs/Databases/migrations/workflow/323_register_object_truth_schema_and_compare_operations.sql`
- `Code&DBs/Databases/migrations/workflow/324_object_truth_comparison_run_store.sql`
- `Code&DBs/Databases/migrations/workflow/325_register_object_truth_record_comparison_run_operation.sql`
- `Code&DBs/Workflow/surfaces/mcp/tools/catalog.py`
- `Code&DBs/Workflow/surfaces/mcp/tools/object_truth.py`
- `Code&DBs/Workflow/surfaces/mcp/cli_metadata.py`
- `docs/MCP.md`
- `docs/CLI.md`
- `docs/API.md`
- `docs/architecture/object-truth-trust-toolbelt/build-gate-2026-04-28.md`

## Verification Evidence

Focused deterministic slice:

```text
7 passed in 0.34s
```

Registry-focused slice:

```text
41 passed in 0.43s
```

Authority-domain wizard slice:

```text
4 passed in 0.43s
```

Authority-domain MCP/docs slice:

```text
6 passed in 0.40s
47 passed in 0.61s
57 passed in 0.66s
```

Object-truth MCP/docs slice:

```text
8 passed in 0.38s
58 passed in 0.59s
14 passed in 0.43s
```

Object-truth persistence/MCP/docs slice:

```text
3 passed in 0.14s
14 passed in 0.52s
```

Object-truth schema/compare slice:

```text
6 passed in 0.21s
13 passed in 0.53s
19 passed in 0.56s
18 passed in 0.54s
```

Canonical test front door selftest:

```json
{
  "ok": true,
  "command": "./scripts/test.sh selftest",
  "errors": [],
  "warnings": []
}
```

Discovery reindex:

```json
{
  "ok": true,
  "indexed": 7,
  "skipped": 4518,
  "errors": []
}
```

Gateway execution proof:

```json
{
  "ok": true,
  "operation": "object_truth_observe_record",
  "receipt_execution_status": "completed",
  "receipt_authority_domain_ref": "authority.object_truth"
}
```

Authority-domain wizard gateway proof:

```json
{
  "forge": {
    "ok": true,
    "state": "existing_domain",
    "receipt_execution_status": "completed"
  },
  "register": {
    "ok": true,
    "action": "register",
    "receipt_execution_status": "completed",
    "event_ids": ["017a9a0e-d964-483d-8eaf-37f824c0b4f3"]
  }
}
```

Authority-domain MCP live proof:

```json
{
  "tool": "praxis_authority_domain_forge",
  "ok": true,
  "state": "existing_domain",
  "attached_operations": 1,
  "receipt_id": "8d1e3736-99be-415c-851f-61d46b3ceafb",
  "receipt_execution_status": "completed"
}
```

Object-truth MCP live proof:

```json
{
  "tool": "praxis_object_truth",
  "ok": true,
  "operation": "object_truth_observe_record",
  "field_observation_count": 5,
  "has_nested_objects": true,
  "receipt_id": "ad4baafc-46ca-47ff-a87d-0ff1dcd66275",
  "receipt_authority_domain_ref": "authority.object_truth",
  "receipt_execution_status": "completed"
}
```

Object-truth persistence live proof:

```json
{
  "tool": "praxis_object_truth_store",
  "ok": true,
  "operation": "object_truth_store_observed_record",
  "object_version_digest": "25908b008cd1920c5c6260afeddf62d1acc7178e8bbb4cdb4cd6ac44c2385a6a",
  "field_observation_count": 5,
  "receipt_id": "ef646603-9774-47bc-b1d6-38bf21599b66",
  "event_ids": ["8956617d-69d4-4c6a-be8d-f21401704e7b"]
}
```

Object-truth table proof:

```text
object_truth_object_version:25908b008cd1920c5c6260afeddf62d1acc7178e8bbb4cdb4cd6ac44c2385a6a|salesforce|account|operator:nate|sample:object-truth:001
api_token|text|true|"[REDACTED]"
billing|object|false|{"keys": ["city"]}
billing.city|text|false|"Denver"
id|text|false|"001"
name|text|false|"Acme"
```

Object-truth schema snapshot live proof:

```json
{
  "tool": "praxis_object_truth_store_schema_snapshot",
  "ok": true,
  "operation": "object_truth_store_schema_snapshot",
  "schema_snapshot_digest": "07e5ee3ce4797f484df5a01ad7e31257940e168b77ac72280f2a587db2ec663c",
  "field_count": 3,
  "receipt_id": "02e9f7fa-3e85-4b3f-9f6f-c1426073bf1f",
  "event_ids": ["a4f2398b-a30b-42c4-abdb-5fc8a295364e"]
}
```

Object-truth persisted comparison live proof:

```json
{
  "tool": "praxis_object_truth_compare_versions",
  "ok": true,
  "operation": "object_truth_compare_versions",
  "summary": {
    "matching_fields": 1,
    "different_fields": 4,
    "missing_left_fields": 0,
    "missing_right_fields": 0
  },
  "freshness": {
    "state": "right_newer",
    "left_updated_at": "2026-04-28T10:00:00Z",
    "right_updated_at": "2026-04-28T11:00:00Z"
  },
  "receipt_id": "4e29549e-9b3a-413e-b6ae-228887b5f3a0"
}
```

Object-truth schema table proof:

```text
object_truth_schema_snapshot:07e5ee3ce4797f484df5a01ad7e31257940e168b77ac72280f2a587db2ec663c|salesforce|account|3|operator:nate|schema:salesforce:account:demo
```

Object-truth comparison-run live proof:

```json
{
  "tool": "praxis_object_truth_record_comparison_run",
  "ok": true,
  "operation": "object_truth_record_comparison_run",
  "comparison_run_digest": "91986b96209a20e268eb5ddd0e80133fcd5139a8c334f2cfa1976331bf2b3709",
  "comparison_run_ref": "object_truth_comparison_run:91986b96209a20e268eb5ddd0e80133fcd5139a8c334f2cfa1976331bf2b3709",
  "summary": {
    "matching_fields": 1,
    "different_fields": 4,
    "missing_left_fields": 0,
    "missing_right_fields": 0
  },
  "freshness": {
    "state": "right_newer",
    "left_updated_at": "2026-04-28T10:00:00Z",
    "right_updated_at": "2026-04-28T11:00:00Z"
  },
  "receipt_id": "85a6cab9-d68e-4142-9296-8adf8633ebe8",
  "event_ids": ["f165cca1-c26b-4430-827e-21c7b4ddd14f"]
}
```

Object-truth comparison-run table proof:

```text
object_truth_comparison_run:91986b96209a20e268eb5ddd0e80133fcd5139a8c334f2cfa1976331bf2b3709|25908b008cd1920c5c6260afeddf62d1acc7178e8bbb4cdb4cd6ac44c2385a6a|9583a6e3e09b56867bf91a199b5b61cb46c06b7bd62de2d076a1f2b81f271e64|{"matching_fields": 1, "different_fields": 4, "missing_left_fields": 0, "missing_right_fields": 0}|{"state": "right_newer", "left_updated_at": "2026-04-28T10:00:00Z", "right_updated_at": "2026-04-28T11:00:00Z"}|operator:nate|comparison:object-truth:demo
```

## Next Build Slice

The next safe slice is latest-version lookup authority, not UX:

1. Add query surfaces for “latest versions by identity” so operators do not have to paste digests.
2. Add query surfaces for comparison-run history by identity/system/object.
3. Add append-only task-environment-contract persistence.
4. Add failure-pattern links from comparison ambiguity to typed gaps.
5. Add workflow task types after the repaired workflow lane exposes stable task registration again.
