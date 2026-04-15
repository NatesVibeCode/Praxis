# Phase 10 Operator Control Authority

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) declares phase `10` as `Operator Control Authority`, in arc `10-19 durable state and retrieval`, with predecessor `9`, status `historical_foundation`, and mandatory closeout `review -> healer -> human_approval`.

Grounding note: this packet is grounded in the mounted repo snapshot at `/workspace`. The prompt declares the execution root as `/Users/nate/Praxis`, so repo evidence below is taken from `/workspace` while verification commands target `/Users/nate/Praxis`. The execution shard says `execution_packets_ready=true`, `repo_snapshots_ready=true`, `verification_registry_ready=true`, and `verify_refs_ready=true`, while proof coverage is still effectively unproved (`verification_coverage=0.0`, `fully_proved_verification_coverage=0.0`) and `write_manifest_coverage=0.2579`. The first sprint must therefore add one narrow proof instead of widening Phase 10.

## 1. Objective in repo terms

- Prove one canonical Phase 10 write-to-read authority path that already exists in the repo: native primary cutover gate admission through the shared operator-control frontdoor.
- Keep the sprint bounded to the existing path in [Code&DBs/Workflow/surfaces/api/operator_write.py](/workspace/Code&DBs/Workflow/surfaces/api/operator_write.py), [Code&DBs/Workflow/policy/native_primary_cutover.py](/workspace/Code&DBs/Workflow/policy/native_primary_cutover.py), [Code&DBs/Workflow/storage/postgres/operator_control_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/operator_control_repository.py), and [Code&DBs/Workflow/authority/operator_control.py](/workspace/Code&DBs/Workflow/authority/operator_control.py).
- In concrete repo terms, one valid cutover admission must persist canonical `operator_decisions` and `cutover_gates` rows, and those rows must round-trip through `load_operator_control_authority(...)` without any direct SQL fixture insertion for the proof itself.

## 2. Current evidence in the repo

- Phase `10` is declared in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) as `Operator Control Authority` with predecessor `9` and required closeout `review -> healer -> human_approval`.
- The canonical schema already exists in [Code&DBs/Databases/migrations/workflow/010_operator_control_authority.sql](/workspace/Code&DBs/Databases/migrations/workflow/010_operator_control_authority.sql). It creates:
- `operator_decisions`
- `cutover_gates`
- `work_item_workflow_bindings`
- It also enforces the bounded gate invariant with `cutover_gates_target_exactly_one`.
- The migration manifest already registers those objects in [Code&DBs/Workflow/system_authority/workflow_migration_authority.json](/workspace/Code&DBs/Workflow/system_authority/workflow_migration_authority.json).
- The canonical read authority already exists in [Code&DBs/Workflow/authority/operator_control.py](/workspace/Code&DBs/Workflow/authority/operator_control.py), including:
- `OperatorDecisionAuthorityRecord`
- `CutoverGateAuthorityRecord`
- `OperatorControlAuthority`
- `resolve_decision(...)`
- `resolve_gate(...)`
- `load_operator_control_authority(...)`
- The canonical Postgres persistence seam already exists in [Code&DBs/Workflow/storage/postgres/operator_control_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/operator_control_repository.py), including:
- `record_operator_decision(...)`
- `record_cutover_gate(...)`
- `load_operator_control_authority(...)`
- The bounded runtime write seam already exists in [Code&DBs/Workflow/policy/native_primary_cutover.py](/workspace/Code&DBs/Workflow/policy/native_primary_cutover.py). `NativePrimaryCutoverRuntime.admit_gate(...)` already:
- requires exactly one target across `roadmap_item_id`, `workflow_class_id`, and `schedule_definition_id`
- constructs canonical decision and gate records
- routes writes through the repository adapter
- The public frontdoor already exists in [Code&DBs/Workflow/surfaces/api/operator_write.py](/workspace/Code&DBs/Workflow/surfaces/api/operator_write.py), including:
- `admit_native_primary_cutover_gate(...)`
- `aadmit_native_primary_cutover_gate(...)`
- `OperatorControlFrontdoor.admit_native_primary_cutover_gate(...)`
- Multiple surfaces already depend on that same seam:
- [Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py](/workspace/Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py) exposes `/api/operator/native-primary-cutover-gate`
- [Code&DBs/Workflow/surfaces/cli/native_operator.py](/workspace/Code&DBs/Workflow/surfaces/cli/native_operator.py) dispatches the native-operator command for cutover-gate admission
- [Code&DBs/Workflow/surfaces/mcp/tools/operator.py](/workspace/Code&DBs/Workflow/surfaces/mcp/tools/operator.py) routes the operator MCP tool through shared operator surfaces
- There is already one adjacent Phase 10 persistence proof in [Code&DBs/Workflow/tests/integration/test_work_item_workflow_bindings.py](/workspace/Code&DBs/Workflow/tests/integration/test_work_item_workflow_bindings.py), but it proves the sibling `work_item_workflow_bindings` seam rather than cutover-gate admission.
- Migration-contract coverage already exists in [Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py).
- Read-side operator tests still seed Phase 10 rows directly instead of proving the cutover admission seam:
- [Code&DBs/Workflow/tests/integration/test_native_operator_query_surface.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_operator_query_surface.py)
- [Code&DBs/Workflow/tests/integration/test_cutover_graph_status_surface.py](/workspace/Code&DBs/Workflow/tests/integration/test_cutover_graph_status_surface.py)
- [Code&DBs/Workflow/tests/integration/test_operator_graph_projection.py](/workspace/Code&DBs/Workflow/tests/integration/test_operator_graph_projection.py)
- There is currently no dedicated [Code&DBs/Workflow/tests/integration/test_native_primary_cutover_gate.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_primary_cutover_gate.py); that missing file is the clearest proof gap for this phase packet.

## 3. Gap or ambiguity still remaining

- Phase 10 already has schema, read authority, repository, runtime validation, and public entrypoints, but the repo still lacks one focused integration proof for the actual native cutover admission path.
- That leaves a live ambiguity: downstream operator read surfaces can pass because they insert `operator_decisions` and `cutover_gates` directly, while the real write seam could drift unnoticed.
- The exact-one-target rule exists twice today:
- in `NativePrimaryCutoverRuntime._normalized_targets(...)`
- in the database constraint `cutover_gates_target_exactly_one`
- but there is no dedicated proof that the caller-visible invalid multi-target path fails cleanly and leaves no partial rows behind.
- The first sprint must resolve exactly that ambiguity for one seam only. It must not claim that all operator-control authority is fully proved.

## 4. One bounded first sprint only

- Add one dedicated integration proof for native primary cutover gate admission through the shared `operator_write` frontdoor.
- In that sprint, prove three behaviors only:
- one successful sync admission call persists one canonical decision row and one canonical gate row
- one successful async admission call persists the same authority shape and returns the same top-level payload contract
- one invalid multi-target submission fails and leaves no partial `operator_decisions` or `cutover_gates` rows behind
- Read the persisted state back through `load_operator_control_authority(...)` and assert that the admitted gate links to the admitted decision via `opened_by_decision_id`.
- If the new proof exposes a defect, repair only the narrow seam needed to make the proof pass.
- Stop after this seam is proved. Do not widen into gate closing, gate supersession, work-item binding changes, read-surface fixture replacement, CLI redesign, MCP redesign, or schema edits.

## 5. Exact file or subsystem scope

- Primary proof scope:
- [Code&DBs/Workflow/tests/integration/test_native_primary_cutover_gate.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_primary_cutover_gate.py) as one new focused integration proof
- Repair-only implementation scope if the proof exposes a defect:
- [Code&DBs/Workflow/surfaces/api/operator_write.py](/workspace/Code&DBs/Workflow/surfaces/api/operator_write.py)
- [Code&DBs/Workflow/policy/native_primary_cutover.py](/workspace/Code&DBs/Workflow/policy/native_primary_cutover.py)
- [Code&DBs/Workflow/storage/postgres/operator_control_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/operator_control_repository.py)
- [Code&DBs/Workflow/authority/operator_control.py](/workspace/Code&DBs/Workflow/authority/operator_control.py)
- Read-only grounding references:
- [Code&DBs/Databases/migrations/workflow/010_operator_control_authority.sql](/workspace/Code&DBs/Databases/migrations/workflow/010_operator_control_authority.sql)
- [Code&DBs/Workflow/system_authority/workflow_migration_authority.json](/workspace/Code&DBs/Workflow/system_authority/workflow_migration_authority.json)
- [Code&DBs/Workflow/tests/integration/test_work_item_workflow_bindings.py](/workspace/Code&DBs/Workflow/tests/integration/test_work_item_workflow_bindings.py)
- [Code&DBs/Workflow/tests/integration/test_native_operator_query_surface.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_operator_query_surface.py)
- [Code&DBs/Workflow/tests/integration/test_cutover_graph_status_surface.py](/workspace/Code&DBs/Workflow/tests/integration/test_cutover_graph_status_surface.py)
- [Code&DBs/Workflow/tests/integration/test_operator_graph_projection.py](/workspace/Code&DBs/Workflow/tests/integration/test_operator_graph_projection.py)
- [Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py)
- [Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py](/workspace/Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py)
- [Code&DBs/Workflow/surfaces/cli/native_operator.py](/workspace/Code&DBs/Workflow/surfaces/cli/native_operator.py)
- [Code&DBs/Workflow/surfaces/mcp/tools/operator.py](/workspace/Code&DBs/Workflow/surfaces/mcp/tools/operator.py)
- Explicitly out of scope:
- [Code&DBs/Workflow/runtime/work_item_workflow_bindings.py](/workspace/Code&DBs/Workflow/runtime/work_item_workflow_bindings.py)
- any existing direct-SQL seeding in read-surface tests
- gate close or gate supersession work
- CLI parsing redesign
- MCP tool redesign
- new migrations
- repo-wide Phase 10 cleanup

## 6. Done criteria

- A dedicated integration proof file exists for native primary cutover gate admission.
- The sync frontdoor proves write -> persistence -> authority readback for one valid target.
- The async frontdoor proves the same seam and the same top-level payload contract shape.
- The persisted gate links back to the persisted decision through `opened_by_decision_id`.
- The invalid multi-target path fails through one explicit error path and leaves no partial `operator_decisions` or `cutover_gates` rows behind.
- Existing adjacent proofs still pass in:
- [Code&DBs/Workflow/tests/integration/test_work_item_workflow_bindings.py](/workspace/Code&DBs/Workflow/tests/integration/test_work_item_workflow_bindings.py)
- [Code&DBs/Workflow/tests/integration/test_native_operator_query_surface.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_operator_query_surface.py)
- [Code&DBs/Workflow/tests/integration/test_cutover_graph_status_surface.py](/workspace/Code&DBs/Workflow/tests/integration/test_cutover_graph_status_surface.py)
- [Code&DBs/Workflow/tests/integration/test_operator_graph_projection.py](/workspace/Code&DBs/Workflow/tests/integration/test_operator_graph_projection.py)
- [Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py)
- No migration file is added or modified.
- No new Phase 10 write owner is introduced outside the existing runtime and repository seam.

## 7. Verification commands

- `cd /Users/nate/Praxis`
- `export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'`
- `export PYTHONPATH='Code&DBs/Workflow'`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_native_primary_cutover_gate.py' -q`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_work_item_workflow_bindings.py' -q`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_native_operator_query_surface.py' -q`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_cutover_graph_status_surface.py' -q`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_operator_graph_projection.py' -q`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py' -q`
- `rg -n "admit_native_primary_cutover_gate|aadmit_native_primary_cutover_gate|NativePrimaryCutoverRuntime|record_cutover_gate|load_operator_control_authority|cutover_gates_target_exactly_one" 'Code&DBs/Workflow/surfaces/api/operator_write.py' 'Code&DBs/Workflow/policy/native_primary_cutover.py' 'Code&DBs/Workflow/storage/postgres/operator_control_repository.py' 'Code&DBs/Workflow/authority/operator_control.py' 'Code&DBs/Databases/migrations/workflow/010_operator_control_authority.sql'`

Expected verification outcome:

- the native primary cutover admission seam is directly proved instead of being inferred from SQL-seeded read tests
- the same shared write path remains responsible for sync and async cutover admission
- adjacent operator read surfaces still consume canonical Phase 10 rows without contract drift

## 8. Review -> healer -> human approval gate

- Review:
- confirm the new proof exercises the real `operator_write` cutover-admission seam rather than inserting `operator_decisions` or `cutover_gates` directly
- confirm both success paths and the invalid multi-target failure path are covered
- confirm authority readback through `load_operator_control_authority(...)` is part of the assertions
- confirm the sprint stayed inside the cutover-admission seam and did not widen into bindings, gate-lifecycle work, or surface redesign
- Healer:
- if review finds contract drift, partial-write behavior, or target-validation defects, repair only the files listed in this packet's in-scope set
- rerun the full verification command set after any repair
- do not widen healer work into gate lifecycle expansion, read-surface rewrites, CLI redesign, MCP redesign, or migration edits
- Human approval gate:
- require explicit human approval after review and any healer pass before opening a second Phase 10 sprint
- any later Phase 10 sprint must target exactly one adjacent seam instead of claiming full Phase 10 completion
