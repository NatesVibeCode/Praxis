# Phase 10 Operator Control Authority

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `10` (`Operator Control Authority`), status `historical_foundation`, predecessor phase `9`, with mandatory closeout sequence `review -> healer -> human_approval`.

Grounding note: this packet is based on the current checked-out repo snapshot under `/workspace` and the supplied verification database `postgresql://nate@127.0.0.1:5432/praxis`. The execution shard says compile authority inputs are ready, but verification coverage and fully proved coverage are both still `0.0`, with write-manifest coverage only `0.1195`. This sprint therefore stays narrow and proof-first.

## 1. Objective in repo terms

- Prove one real Phase 10 public write seam that already exists in the repo: native-primary cutover gate admission through the shared operator-control frontdoor.
- Keep the sprint bounded to the exported sync and async admission path in [Code&DBs/Workflow/surfaces/api/operator_write.py](/workspace/Code&DBs/Workflow/surfaces/api/operator_write.py), not a broad operator-control cleanup.
- Repo-level target for this sprint: one successful admission call persists canonical `operator_decisions` and `cutover_gates` rows, and those rows round-trip through the canonical Phase 10 read authority in [Code&DBs/Workflow/authority/operator_control.py](/workspace/Code&DBs/Workflow/authority/operator_control.py).

## 2. Current evidence in the repo

- The authority map defines phase `10` as `Operator Control Authority`, in arc `10-19 durable state and retrieval`, with predecessor `9` and mandatory closeout sequence `review -> healer -> human_approval` in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json).
- The canonical schema source already exists in [Code&DBs/Databases/migrations/workflow/010_operator_control_authority.sql](/workspace/Code&DBs/Databases/migrations/workflow/010_operator_control_authority.sql). It creates `operator_decisions`, `cutover_gates`, and `work_item_workflow_bindings`, and enforces `cutover_gates_target_exactly_one`.
- The generated migration authority also knows those objects today in [Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py](/workspace/Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py) and [Code&DBs/Workflow/system_authority/workflow_migration_authority.json](/workspace/Code&DBs/Workflow/system_authority/workflow_migration_authority.json).
- Migration-contract coverage already proves the Phase 10 tables and indexes are in the manifest in [Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py).
- The canonical read-side authority already exists in [Code&DBs/Workflow/authority/operator_control.py](/workspace/Code&DBs/Workflow/authority/operator_control.py), including `load_operator_control_authority(...)`, decision ordering, gate ordering, and fail-closed row normalization.
- The dedicated Postgres write repository already exists in [Code&DBs/Workflow/storage/postgres/operator_control_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/operator_control_repository.py), including `record_operator_decision(...)`, `record_cutover_gate(...)`, and `load_operator_control_authority(...)`.
- The bounded runtime seam already exists in [Code&DBs/Workflow/policy/native_primary_cutover.py](/workspace/Code&DBs/Workflow/policy/native_primary_cutover.py). `NativePrimaryCutoverRuntime.admit_gate(...)` validates exactly one target, constructs canonical authority records, and writes through the repository adapter.
- The public frontdoor already exists in [Code&DBs/Workflow/surfaces/api/operator_write.py](/workspace/Code&DBs/Workflow/surfaces/api/operator_write.py) as `_admit_native_primary_cutover_gate(...)`, `admit_native_primary_cutover_gate(...)`, and `aadmit_native_primary_cutover_gate(...)`.
- Additional product surfaces already route to that same seam:
- [Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py](/workspace/Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py) exposes `/api/operator/native-primary-cutover-gate`
- [Code&DBs/Workflow/surfaces/cli/native_operator.py](/workspace/Code&DBs/Workflow/surfaces/cli/native_operator.py) parses and dispatches `native-primary-cutover-gate`
- [Code&DBs/Workflow/surfaces/mcp/tools/operator.py](/workspace/Code&DBs/Workflow/surfaces/mcp/tools/operator.py) routes the MCP operator tool through the same frontdoor
- A sibling Phase 10 seam already has direct proof. [Code&DBs/Workflow/tests/integration/test_work_item_workflow_bindings.py](/workspace/Code&DBs/Workflow/tests/integration/test_work_item_workflow_bindings.py) proves write -> persistence -> readback for `work_item_workflow_bindings` through `operator_write`.
- Read-side Phase 10 surfaces already depend on `operator_decisions` and `cutover_gates`, but several tests still seed those rows directly with SQL instead of proving the admission seam:
- [Code&DBs/Workflow/tests/integration/test_cutover_graph_status_surface.py](/workspace/Code&DBs/Workflow/tests/integration/test_cutover_graph_status_surface.py)
- [Code&DBs/Workflow/tests/integration/test_native_operator_query_surface.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_operator_query_surface.py)
- [Code&DBs/Workflow/tests/integration/test_operator_graph_projection.py](/workspace/Code&DBs/Workflow/tests/integration/test_operator_graph_projection.py)
- There is currently no dedicated test file under [Code&DBs/Workflow/tests](/workspace/Code&DBs/Workflow/tests) that exercises `admit_native_primary_cutover_gate`, `aadmit_native_primary_cutover_gate`, or `NativePrimaryCutoverRuntime` directly.

## 3. Gap or ambiguity still remaining

- Phase 10 has schema, repository, runtime, exported frontdoor, and downstream readers, but it does not yet have direct proof that the native-primary cutover admission seam works end to end.
- That leaves one important ambiguity in the current repo: read surfaces can look healthy because they are fed by direct SQL fixtures, while the actual operator-control write path could drift or break unnoticed.
- The exact-one-target rule is implemented twice today, once in runtime validation and once in the database constraint, but there is no explicit regression proof for the caller-visible failure path on invalid multi-target input.
- The first sprint should remove that ambiguity for exactly one Phase 10 seam. It should not claim that all operator-control authority is now fully proved.

## 4. One bounded first sprint only

- Add one dedicated integration proof for native-primary cutover admission through the shared `operator_write` frontdoor.
- In that sprint, prove three behaviors only:
- one successful sync admission call persists one canonical decision row and one canonical gate row
- one successful async admission call persists the same authority shape and returns the same top-level payload contract
- one invalid multi-target submission fails and leaves no partial row pair behind
- Read back persisted state through `load_operator_control_authority(...)` and assert that the gate links to the decision through `opened_by_decision_id`.
- Allow only minimal repair in the existing seam if the new proof exposes a real defect.
- Stop after this seam is proved. Do not widen into gate closing, gate supersession, `work_item_workflow_bindings`, query-surface fixture replacement, CLI redesign, MCP redesign, or schema changes.

## 5. Exact file or subsystem scope

- Primary implementation scope:
- [Code&DBs/Workflow/tests/integration/test_native_primary_cutover_gate.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_primary_cutover_gate.py) as a new integration proof file
- Repair-only scope if the new proof finds a defect:
- [Code&DBs/Workflow/surfaces/api/operator_write.py](/workspace/Code&DBs/Workflow/surfaces/api/operator_write.py)
- [Code&DBs/Workflow/policy/native_primary_cutover.py](/workspace/Code&DBs/Workflow/policy/native_primary_cutover.py)
- [Code&DBs/Workflow/storage/postgres/operator_control_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/operator_control_repository.py)
- Read-only authority and neighboring-proof references:
- [Code&DBs/Workflow/authority/operator_control.py](/workspace/Code&DBs/Workflow/authority/operator_control.py)
- [Code&DBs/Workflow/tests/integration/test_work_item_workflow_bindings.py](/workspace/Code&DBs/Workflow/tests/integration/test_work_item_workflow_bindings.py)
- [Code&DBs/Workflow/tests/integration/test_cutover_graph_status_surface.py](/workspace/Code&DBs/Workflow/tests/integration/test_cutover_graph_status_surface.py)
- [Code&DBs/Workflow/tests/integration/test_native_operator_query_surface.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_operator_query_surface.py)
- [Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py](/workspace/Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py)
- [Code&DBs/Workflow/surfaces/cli/native_operator.py](/workspace/Code&DBs/Workflow/surfaces/cli/native_operator.py)
- [Code&DBs/Workflow/surfaces/mcp/tools/operator.py](/workspace/Code&DBs/Workflow/surfaces/mcp/tools/operator.py)
- [Code&DBs/Databases/migrations/workflow/010_operator_control_authority.sql](/workspace/Code&DBs/Databases/migrations/workflow/010_operator_control_authority.sql)
- Explicitly out of scope:
- [Code&DBs/Workflow/runtime/work_item_workflow_bindings.py](/workspace/Code&DBs/Workflow/runtime/work_item_workflow_bindings.py) behavior changes
- gate close, gate supersede, or historical gate lifecycle work
- query-surface payload redesign
- CLI parsing redesign
- MCP tool redesign
- new migrations
- repo-wide replacement of direct SQL fixtures in unrelated operator tests

## 6. Done criteria

- A dedicated integration test file exists for native-primary cutover admission.
- The sync frontdoor proves write -> persistence -> authority readback for one valid target.
- The async frontdoor proves the same seam and the same top-level payload shape `{"native_primary_cutover": ...}`.
- The persisted gate links back to the persisted decision through `opened_by_decision_id`.
- The invalid multi-target path fails with one explicit error path and does not create partial `operator_decisions` or `cutover_gates` rows.
- Existing adjacent proofs still pass in [test_work_item_workflow_bindings.py](/workspace/Code&DBs/Workflow/tests/integration/test_work_item_workflow_bindings.py), [test_cutover_graph_status_surface.py](/workspace/Code&DBs/Workflow/tests/integration/test_cutover_graph_status_surface.py), [test_native_operator_query_surface.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_operator_query_surface.py), and [test_workflow_migration_contracts.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py).
- No schema migration is added or modified.
- No new Phase 10 write owner is introduced outside the existing runtime/repository seam.

## 7. Verification commands

- `export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'`
- `export PYTHONPATH='/workspace/Code&DBs/Workflow'`
- `python -m pytest /workspace/Code\&DBs/Workflow/tests/integration/test_native_primary_cutover_gate.py -q`
- `python -m pytest /workspace/Code\&DBs/Workflow/tests/integration/test_work_item_workflow_bindings.py -q`
- `python -m pytest /workspace/Code\&DBs/Workflow/tests/integration/test_cutover_graph_status_surface.py -q`
- `python -m pytest /workspace/Code\&DBs/Workflow/tests/integration/test_native_operator_query_surface.py -q`
- `python -m pytest /workspace/Code\&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py -q`
- `rg -n "admit_native_primary_cutover_gate|aadmit_native_primary_cutover_gate|NativePrimaryCutoverRuntime|record_cutover_gate" /workspace/Code\\&DBs/Workflow/surfaces/api/operator_write.py /workspace/Code\\&DBs/Workflow/policy/native_primary_cutover.py /workspace/Code\\&DBs/Workflow/storage/postgres/operator_control_repository.py`

Expected verification outcome:

- the native-primary cutover admission seam is directly proved instead of inferred from seeded SQL fixtures
- the same shared write path remains responsible for sync, async, HTTP, CLI, and MCP entrypoints
- downstream read surfaces still consume the newly persisted Phase 10 rows without contract drift

## 8. Review -> healer -> human approval gate

- Review:
- confirm the new proof exercises the real `operator_write` admission seam rather than inserting `operator_decisions` or `cutover_gates` directly
- confirm both success and invalid-input cases are covered
- confirm authority readback is part of the assertions, not only API payload checks
- confirm the sprint stayed inside the cutover-admission seam and did not widen into bindings, gate-close flows, or surface redesign
- Healer:
- if review finds contract drift, partial-write behavior, or target-validation defects, repair only the scoped seam in the files listed above
- do not widen healer work into gate lifecycle expansion, query-surface rewrites, CLI redesign, MCP redesign, or migration edits
- Human approval gate:
- require explicit human approval after review and any healer pass before opening a second Phase 10 sprint
- if a later Phase 10 sprint is approved, target exactly one adjacent seam, most likely gate-close authority or one read surface that still depends on direct SQL fixtures
