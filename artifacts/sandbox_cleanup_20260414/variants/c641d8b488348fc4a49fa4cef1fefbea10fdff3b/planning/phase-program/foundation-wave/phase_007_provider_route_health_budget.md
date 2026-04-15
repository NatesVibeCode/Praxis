# Phase 7 Provider Route Health Budget

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `7` (`Provider Route Health Budget`), status `historical_foundation`, predecessor phase `6`, with mandatory closeout sequence `review -> healer -> human_approval`.

Grounding note: this packet is based on the mounted checkout at `/workspace`. The platform-context repo root `/Users/nate/Praxis` is not present in this execution environment, so all repo evidence below is grounded in the current workspace snapshot.

## 1. Objective in repo terms

- Re-establish one canonical Phase 7 write boundary for provider-route control-tower authority in the current repo.
- Keep the first sprint bounded to the missing `provider_route_health_windows` write seam rather than reopening routing design across every health, ranking, or failover surface.
- Repo-level target for this sprint: the Phase 7 control tower should have one repo-owned way to persist canonical provider-route health windows, so current runtime and operator readers stop depending on direct SQL test fixtures for the health half of the authority model.

## 2. Current evidence in the repo

- Phase `7` is declared as `Provider Route Health Budget` in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json), and the registry requires `review -> healer -> human_approval` before later phases.
- The canonical schema origin already exists in [Code&DBs/Databases/migrations/workflow/007_provider_route_health_budget.sql](/workspace/Code&DBs/Databases/migrations/workflow/007_provider_route_health_budget.sql). It creates:
- `provider_route_health_windows`
- `provider_budget_windows`
- `route_eligibility_states`
- Manifest and expected-object coverage already exist in [Code&DBs/Workflow/tests/integration/test_provider_route_health_budget_schema.py](/workspace/Code&DBs/Workflow/tests/integration/test_provider_route_health_budget_schema.py) and [Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py).
- Canonical read-side authority already exists in [Code&DBs/Workflow/registry/provider_routing.py](/workspace/Code&DBs/Workflow/registry/provider_routing.py). It bootstraps schema and loads all three Phase 7 tables into explicit authority records and snapshots.
- Runtime adoption already exists:
- [Code&DBs/Workflow/runtime/provider_route_runtime.py](/workspace/Code&DBs/Workflow/runtime/provider_route_runtime.py) resolves bounded runtime wiring from Phase 7 authority plus route catalog rows.
- [Code&DBs/Workflow/runtime/default_path_pilot.py](/workspace/Code&DBs/Workflow/runtime/default_path_pilot.py) reads `provider_route_health_windows`, `provider_budget_windows`, and `route_eligibility_states` when deciding pilot/default-path behavior.
- [Code&DBs/Workflow/surfaces/api/native_operator_surface.py](/workspace/Code&DBs/Workflow/surfaces/api/native_operator_surface.py) exposes provider-route control-tower snapshots to operator-facing surfaces.
- Read-path and runtime-path proofs already exist:
- [Code&DBs/Workflow/tests/integration/test_provider_route_control_tower.py](/workspace/Code&DBs/Workflow/tests/integration/test_provider_route_control_tower.py)
- [Code&DBs/Workflow/tests/integration/test_provider_route_authority.py](/workspace/Code&DBs/Workflow/tests/integration/test_provider_route_authority.py)
- [Code&DBs/Workflow/tests/integration/test_provider_route_runtime_wiring.py](/workspace/Code&DBs/Workflow/tests/integration/test_provider_route_runtime_wiring.py)
- [Code&DBs/Workflow/tests/integration/test_default_path_route_runtime_adoption.py](/workspace/Code&DBs/Workflow/tests/integration/test_default_path_route_runtime_adoption.py)
- [Code&DBs/Workflow/tests/integration/test_default_path_pilot_wiring.py](/workspace/Code&DBs/Workflow/tests/integration/test_default_path_pilot_wiring.py)
- The repo already has app-owned writers for two of the three Phase 7 tables inside [Code&DBs/Workflow/registry/native_runtime_profile_sync.py](/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py):
- `_sync_budget_window_sync(...)` and `_sync_budget_window_async(...)` write `provider_budget_windows`
- `_sync_route_states_sync(...)` and `_sync_route_states_async(...)` write `route_eligibility_states`
- The same file does not write `provider_route_health_windows`. A repo-wide search in non-test code shows no `INSERT`, `UPDATE`, or `DELETE` statements for that table outside integration fixtures.
- Existing mutable route-health behavior still lives elsewhere:
- [Code&DBs/Workflow/runtime/task_type_router.py](/workspace/Code&DBs/Workflow/runtime/task_type_router.py) updates `task_type_routing.route_health_score` and related counters in `record_outcome(...)` and `record_review_feedback(...)`
- [Code&DBs/Workflow/runtime/workflow/_routing.py](/workspace/Code&DBs/Workflow/runtime/workflow/_routing.py) and other runtime seams still read route-health metadata from `task_type_routing`
- Current Phase 7 integration tests seed `provider_route_health_windows` directly with SQL helper inserts rather than through a repo-owned write surface.

## 3. Gap or ambiguity still remaining

- The Phase 7 control tower is real on the read side, but its health-window authority is not owned by a repo write seam.
- That leaves two competing health worlds in the current checkout:
- Phase 7 readers consume `provider_route_health_windows`
- mutable runtime health still accumulates in `task_type_routing.route_health_score` and related counters
- Because non-test code does not write `provider_route_health_windows`, the current provider-route runtime and operator snapshot proofs are strong only after direct SQL fixture seeding.
- The missing decision is not whether Phase 7 should exist. It already does.
- The unresolved question is which repo seam should own translation from live route-health facts into canonical `provider_route_health_windows` rows for the current native runtime path.
- This first sprint should not widen into:
- replacing `task_type_routing`
- redesigning ranking, demotion, or composite scoring
- broad failover policy work
- multi-surface operator UX changes
- a new end-to-end routing architecture for every provider surface

## 4. One bounded first sprint only

- Add one explicit Postgres write seam for `provider_route_health_windows` and prove one native bounded caller uses it.
- The bounded caller should be the existing native-profile sync path in [Code&DBs/Workflow/registry/native_runtime_profile_sync.py](/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py), because that file already owns the adjacent Phase 7 writes for budget and eligibility.
- In this sprint, do three things only:
- introduce one explicit repository/helper that upserts canonical `provider_route_health_windows` rows
- derive one bounded health snapshot for each synced native candidate from current repo-local routing facts already available in the native path, without redesigning routing policy
- add one integration proof that runs the native sync path and then reads back the resulting control-tower health window through `load_provider_route_authority(...)`
- The first sprint may derive the health window from the current `task_type_routing` state for the matching provider/model pair if that is the smallest repo-local bridge to canonical Phase 7 authority.
- Stop once one real app path writes `provider_route_health_windows` and one integration proof no longer needs direct SQL inserts for that table.

## 5. Exact file or subsystem scope

- Primary implementation scope:
- [Code&DBs/Workflow/registry/native_runtime_profile_sync.py](/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py)
- one new explicit Postgres write owner beside the existing write repositories, preferably under [Code&DBs/Workflow/storage/postgres](/workspace/Code&DBs/Workflow/storage/postgres)
- Repair-only scope if the new proof exposes a defect in the read side:
- [Code&DBs/Workflow/registry/provider_routing.py](/workspace/Code&DBs/Workflow/registry/provider_routing.py)
- Primary test scope:
- one new focused integration test file under [Code&DBs/Workflow/tests/integration](/workspace/Code&DBs/Workflow/tests/integration), or a tightly scoped extension of [test_provider_route_control_tower.py](/workspace/Code&DBs/Workflow/tests/integration/test_provider_route_control_tower.py)
- Read-only context:
- [Code&DBs/Workflow/runtime/task_type_router.py](/workspace/Code&DBs/Workflow/runtime/task_type_router.py)
- [Code&DBs/Workflow/runtime/provider_route_runtime.py](/workspace/Code&DBs/Workflow/runtime/provider_route_runtime.py)
- [Code&DBs/Workflow/runtime/default_path_pilot.py](/workspace/Code&DBs/Workflow/runtime/default_path_pilot.py)
- [Code&DBs/Workflow/tests/integration/test_provider_route_runtime_wiring.py](/workspace/Code&DBs/Workflow/tests/integration/test_provider_route_runtime_wiring.py)
- [Code&DBs/Workflow/tests/integration/test_default_path_route_runtime_adoption.py](/workspace/Code&DBs/Workflow/tests/integration/test_default_path_route_runtime_adoption.py)
- Explicitly out of scope:
- schema or migration changes to `007_provider_route_health_budget.sql`
- redesign of `task_type_routing` scoring, demotion, or ranking
- changes to `provider_budget_windows` or `route_eligibility_states` beyond minimal compatibility adjustments
- HTTP, CLI, MCP, or operator UI redesign
- broad failover or endpoint-routing work

## 6. Done criteria

- There is one explicit repo-owned write path for `provider_route_health_windows`.
- The native profile sync path writes or refreshes canonical health-window rows for its bounded candidate set without direct SQL fixture seeding.
- A focused integration proof exercises the repo write path and then reads the resulting row through `load_provider_route_authority(...)` or `load_provider_route_authority_snapshot(...)`.
- The proof asserts canonical Phase 7 fields, including:
- `candidate_ref`
- `provider_ref`
- `health_status`
- `health_score`
- `observation_ref`
- ordered window timestamps
- Existing provider-route control-tower and runtime wiring tests still pass after the change.
- No new migration is added.
- No broader routing-policy redesign is introduced.

## 7. Verification commands

- `export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'`
- `export PYTHONPATH='/workspace/Code&DBs/Workflow'`
- `python -m pytest /workspace/Code\\&DBs/Workflow/tests/integration/test_provider_route_health_window_write_path.py -q`
- `python -m pytest /workspace/Code\\&DBs/Workflow/tests/integration/test_provider_route_control_tower.py -q`
- `python -m pytest /workspace/Code\\&DBs/Workflow/tests/integration/test_provider_route_runtime_wiring.py -q`
- `python -m pytest /workspace/Code\\&DBs/Workflow/tests/integration/test_default_path_route_runtime_adoption.py -q`
- `rg -n 'provider_route_health_windows' /workspace/Code\\&DBs/Workflow/registry/native_runtime_profile_sync.py /workspace/Code\\&DBs/Workflow/storage/postgres /workspace/Code\\&DBs/Workflow/tests/integration`
- `rg -n 'INSERT INTO provider_route_health_windows|UPDATE provider_route_health_windows|DELETE FROM provider_route_health_windows' /workspace/Code\\&DBs/Workflow | rg -v '/tests/'`

Expected verification outcome:

- one non-test repo seam now owns health-window writes
- provider-route control-tower reads can be proved from repo code rather than only from fixture SQL
- existing control-tower runtime adoption still works without contract drift

## 8. Review -> healer -> human approval gate

- Review:
- confirm the sprint stays inside the missing Phase 7 health-window write boundary
- confirm the new proof uses repo code to create `provider_route_health_windows` rows rather than seeding them directly with test SQL
- confirm no broader routing-policy or operator-surface redesign slipped in
- confirm the repo still has one bounded first sprint, not a “finish all provider routing” plan
- Healer:
- if review finds drift, repair only the scoped health-window writer seam, the native sync caller, and the focused integration proof
- do not widen healer work into `task_type_routing` replacement, score-model changes, failover redesign, or UI work
- Human approval gate:
- require explicit human approval after review and any healer pass before opening a second Phase 7 sprint
- the next Phase 7 sprint, if approved later, should take one adjacent seam only, most likely elimination of one remaining `task_type_routing` health dependency or tightening the budget/eligibility bridge, not “complete provider routing authority” in one pass
