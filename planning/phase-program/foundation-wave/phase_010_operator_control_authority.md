# Phase 10 Operator Control Authority

Status: execution_ready

## 1. Objective in repo terms
- Registry authority: Phase `10` is `Operator Control Authority` in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json). The registry currently marks it as `historical_foundation`, so this packet is a bounded repo-convergence sprint, not a request to redesign the phase.
- In the current repo, Phase 10 already has durable schema and read-side authority. The execution objective is to make one real operator write path authoritative by replacing the native-primary cutover stub with a canonical write flow that persists both `operator_decisions` and `cutover_gates`.
- Repo-level target for this sprint: one frontdoor admission call creates canonical Phase 10 rows, and those rows are then readable through `authority.operator_control` without direct test seeding.

## 2. Current evidence in the repo
- The canonical Phase 10 schema already exists in [Code&DBs/Databases/migrations/workflow/010_operator_control_authority.sql](/workspace/Code&DBs/Databases/migrations/workflow/010_operator_control_authority.sql). It defines `operator_decisions`, `cutover_gates`, and `work_item_workflow_bindings`.
- Migration-contract coverage already proves those objects exist in [Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py).
- Canonical read-side validation already exists in [Code&DBs/Workflow/authority/operator_control.py](/workspace/Code&DBs/Workflow/authority/operator_control.py), including `load_operator_control_authority(...)`, `resolve_decision(...)`, and `resolve_gate(...)`.
- The Postgres authority repository already exists in [Code&DBs/Workflow/storage/postgres/operator_control_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/operator_control_repository.py). It can bootstrap schema, record `operator_decisions`, and load validated authority snapshots.
- Real downstream surfaces already read Phase 10 authority, including [Code&DBs/Workflow/surfaces/api/native_operator_surface.py](/workspace/Code&DBs/Workflow/surfaces/api/native_operator_surface.py) and integration coverage such as [Code&DBs/Workflow/tests/integration/test_cutover_graph_status_surface.py](/workspace/Code&DBs/Workflow/tests/integration/test_cutover_graph_status_surface.py).
- The write-side admission seam is still a stub in [Code&DBs/Workflow/policy/native_primary_cutover.py](/workspace/Code&DBs/Workflow/policy/native_primary_cutover.py):
- `NativePrimaryCutoverRepository` is an empty protocol.
- `PostgresNativePrimaryCutoverRepository` has no write methods.
- `NativePrimaryCutoverRuntime.admit_gate(...)` returns a synthetic `gate_id="stub"` record and does not persist authority rows.
- The public write frontdoor in [Code&DBs/Workflow/surfaces/api/operator_write.py](/workspace/Code&DBs/Workflow/surfaces/api/operator_write.py) already exposes `admit_native_primary_cutover_gate` and `admit_native_primary_cutover_gate_async`, so the repo presents this as a real operator-control API even though the implementation is not authoritative yet.
- Existing integration tests that exercise Phase 10 read surfaces still seed `operator_decisions` and `cutover_gates` with direct SQL inserts instead of proving the frontdoor can create those rows.

## 3. Gap or ambiguity still remaining
- Phase 10 has durable schema and read-side authority, but no canonical write path yet proves that one operator action can create a valid decision-plus-gate pair.
- Authority ownership is still ambiguous at the seam that matters most for execution: `operator_write.py` routes to `policy/native_primary_cutover.py`, but that module is still a boot-time stub rather than an authority-backed runtime.
- [Code&DBs/Workflow/storage/postgres/operator_control_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/operator_control_repository.py) currently records operator decisions but does not expose a matching cutover-gate write helper, so the packet cannot assume the write repository already exists end to end.
- There is no existing dedicated integration contract for the native-primary cutover frontdoor; the current packet must account for adding one instead of naming a nonexistent test file as if it already exists.

## 4. One bounded first sprint only
- Implement one canonical admission flow for `admit_native_primary_cutover_gate`.
- Add the smallest missing write seam to [Code&DBs/Workflow/storage/postgres/operator_control_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/operator_control_repository.py) so the runtime can persist one `cutover_gates` row alongside the existing `operator_decisions` write path.
- Replace the stub behavior in [Code&DBs/Workflow/policy/native_primary_cutover.py](/workspace/Code&DBs/Workflow/policy/native_primary_cutover.py) so one admission call:
- validates exactly one target among `roadmap_item_id`, `workflow_class_id`, or `schedule_definition_id`
- records one canonical operator decision row
- records one canonical cutover gate row linked by `opened_by_decision_id`
- returns a stable payload for the existing `operator_write` frontdoor
- Add one focused integration test for the frontdoor admission path, then load authority through `load_operator_control_authority(...)` and prove the created decision/gate pair is visible there.
- Stop after this one admission seam works. Do not expand into gate-closing flows, work-item workflow bindings, broader operator UX, or repo-wide direct-SQL cleanup.

## 5. Exact file or subsystem scope
- Primary implementation scope:
- [Code&DBs/Workflow/policy/native_primary_cutover.py](/workspace/Code&DBs/Workflow/policy/native_primary_cutover.py)
- [Code&DBs/Workflow/storage/postgres/operator_control_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/operator_control_repository.py)
- [Code&DBs/Workflow/surfaces/api/operator_write.py](/workspace/Code&DBs/Workflow/surfaces/api/operator_write.py)
- Test scope:
- [Code&DBs/Workflow/tests/integration/test_native_primary_cutover_gate.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_primary_cutover_gate.py) (new file expected)
- Existing read-side regression scope:
- [Code&DBs/Workflow/tests/integration/test_cutover_graph_status_surface.py](/workspace/Code&DBs/Workflow/tests/integration/test_cutover_graph_status_surface.py)
- [Code&DBs/Workflow/tests/integration/test_native_operator_query_surface.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_operator_query_surface.py)
- [Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py)
- Read for context only:
- [Code&DBs/Workflow/authority/operator_control.py](/workspace/Code&DBs/Workflow/authority/operator_control.py)
- Explicitly out of scope:
- [Code&DBs/Databases/migrations/workflow/010_operator_control_authority.sql](/workspace/Code&DBs/Databases/migrations/workflow/010_operator_control_authority.sql)
- gate-closing or supersession flows
- `work_item_workflow_bindings` write behavior
- native-operator UI or handler redesign
- broad replacement of direct SQL in existing read-surface integration tests

## 6. Done criteria
- `admit_native_primary_cutover_gate` and `admit_native_primary_cutover_gate_async` no longer return stub-only data.
- One successful admission persists exactly one canonical `operator_decisions` row and one canonical `cutover_gates` row in Phase 10 tables.
- The persisted gate is linked to the persisted decision through `opened_by_decision_id`.
- The admission path enforces the schema rule that exactly one target is populated.
- The frontdoor response remains stable enough for existing callers to keep using the `native_primary_cutover` payload without a parallel API redesign.
- A dedicated integration test proves frontdoor write -> repository persistence -> `load_operator_control_authority(...)` readback.
- Existing read-side regression tests for cutover/operator authority still pass.
- No schema migration or unrelated authority subsystem is changed in this sprint.

## 7. Verification commands
- `WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis' python -m pytest /workspace/Code\&DBs/Workflow/tests/integration/test_native_primary_cutover_gate.py -q`
- `WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis' python -m pytest /workspace/Code\&DBs/Workflow/tests/integration/test_cutover_graph_status_surface.py -q`
- `WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis' python -m pytest /workspace/Code\&DBs/Workflow/tests/integration/test_native_operator_query_surface.py -q`
- `WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis' python -m pytest /workspace/Code\&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py -q`
- Authority grep after implementation:
- `rg -n "gate_id=\\\"stub\\\"|return NativePrimaryCutoverGateRecord\\(|INSERT INTO cutover_gates|record_cutover_gate|admit_native_primary_cutover_gate" /workspace/Code\\&DBs/Workflow/policy/native_primary_cutover.py /workspace/Code\\&DBs/Workflow/storage/postgres/operator_control_repository.py /workspace/Code\\&DBs/Workflow/surfaces/api/operator_write.py`

## 8. Review -> healer -> human approval gate
- Review:
- confirm the frontdoor admission seam writes through the declared Phase 10 authority path rather than returning synthetic data
- confirm the new write logic stays inside the scoped files and does not introduce a second operator-control repository
- confirm the new integration test proves real persistence and authority readback instead of direct SQL seeding
- Healer:
- if review finds payload drift, broken target validation, or decision/gate linkage errors, repair only the bounded native-primary cutover admission seam
- do not widen healer work into gate-closing flows, work-item bindings, or operator surface redesign
- Human approval gate:
- require explicit human approval after review and any healer pass before starting any second Phase 10 sprint
- the next Phase 10 sprint, if approved later, should take one adjacent seam only, likely gate-closing or binding authority, not “finish operator control” in one pass
