# Phase 7 Provider Route Health Budget

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `7` (`Provider Route Health Budget`), status `historical_foundation`, predecessor phase `6`, required closeout sequence `review -> healer -> human_approval`.

Grounding note: this packet is grounded in the mounted repo snapshot at `/workspace`. The execution target named in platform context is the Praxis repository root, so evidence cites `/workspace` and verification commands are written for that root. The execution shard says packets and repo snapshots are ready while verification coverage remains `0.0`, so this packet keeps Phase 7 to one proofable write seam only.

## 1. Objective in repo terms

- Give Phase 7 one canonical repo-owned write path for `provider_route_health_windows`.
- Keep the first sprint attached to the existing control-tower family already consumed by:
- [Code&DBs/Workflow/registry/provider_routing.py](/workspace/Code&DBs/Workflow/registry/provider_routing.py)
- [Code&DBs/Workflow/runtime/provider_route_runtime.py](/workspace/Code&DBs/Workflow/runtime/provider_route_runtime.py)
- [Code&DBs/Workflow/runtime/default_path_pilot.py](/workspace/Code&DBs/Workflow/runtime/default_path_pilot.py)
- [Code&DBs/Workflow/observability/operator_dashboard.py](/workspace/Code&DBs/Workflow/observability/operator_dashboard.py)
- Repo outcome for this sprint:
- one repo-owned writer must upsert canonical provider-route health windows
- the native runtime profile sync path must invoke that writer in both sync and async flows
- one integration proof must exercise the writer through repo code and then read the canonical result back through the Phase 7 authority loader

## 2. Current evidence in the repo

- Phase `7` is declared in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) as `Provider Route Health Budget`, with predecessor `6` and mandatory closeout `review -> healer -> human_approval`.
- [Code&DBs/Databases/migrations/workflow/007_provider_route_health_budget.sql](/workspace/Code&DBs/Databases/migrations/workflow/007_provider_route_health_budget.sql) already creates the canonical Phase 7 control-tower tables:
- `provider_route_health_windows`
- `provider_budget_windows`
- `route_eligibility_states`
- The migration defines the exact health-window contract already consumed elsewhere:
- `candidate_ref`
- `provider_ref`
- `health_status`
- `health_score`
- `sample_count`
- `failure_rate`
- `latency_p95_ms`
- `observed_window_started_at`
- `observed_window_ended_at`
- `observation_ref`
- [Code&DBs/Workflow/registry/provider_routing.py](/workspace/Code&DBs/Workflow/registry/provider_routing.py) already treats `provider_route_health_windows` as canonical read authority via:
- `bootstrap_provider_route_authority_schema(...)`
- `fetch_provider_route_health_windows(...)`
- `load_provider_route_authority(...)`
- `load_provider_route_authority_snapshot(...)`
- Read-side runtime and operator consumers already depend on health windows being present:
- [Code&DBs/Workflow/runtime/default_path_pilot.py](/workspace/Code&DBs/Workflow/runtime/default_path_pilot.py) fail-closes when matching health windows are missing or ambiguous
- [Code&DBs/Workflow/observability/operator_dashboard.py](/workspace/Code&DBs/Workflow/observability/operator_dashboard.py) reports `route:health_windows_missing` when they are absent
- [Code&DBs/Workflow/registry/native_runtime_profile_sync.py](/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py) already owns adjacent Phase 7 writes:
- `_sync_budget_window_sync(...)` and `_sync_budget_window_async(...)` upsert `provider_budget_windows`
- `_sync_route_states_sync(...)` and `_sync_route_states_async(...)` upsert `route_eligibility_states`
- `sync_native_runtime_profile_authority(...)` and `sync_native_runtime_profile_authority_async(...)` already call those adjacent writers as part of one repo-native authority projection loop
- The same native sync module does not define any `_sync_*health*` writer and does not reference `provider_route_health_windows`.
- Repo-wide search shows no non-test `INSERT INTO`, `UPDATE`, or `DELETE FROM provider_route_health_windows` in runtime or registry code outside migration/bootstrap authority.
- The mutable live health state still sits elsewhere:
- [Code&DBs/Workflow/runtime/task_type_router.py](/workspace/Code&DBs/Workflow/runtime/task_type_router.py) updates `task_type_routing.route_health_score`, `recent_successes`, `recent_failures`, `observed_completed_count`, and `observed_execution_failure_count` in `record_outcome(...)`
- the same file updates `task_type_routing.route_health_score` in `record_review_feedback(...)`
- Existing integration tests still seed `provider_route_health_windows` directly with SQL fixtures instead of producing them through repo code, including:
- [Code&DBs/Workflow/tests/integration/test_provider_route_control_tower.py](/workspace/Code&DBs/Workflow/tests/integration/test_provider_route_control_tower.py)
- [Code&DBs/Workflow/tests/integration/test_provider_route_runtime_wiring.py](/workspace/Code&DBs/Workflow/tests/integration/test_provider_route_runtime_wiring.py)
- [Code&DBs/Workflow/tests/integration/test_default_path_route_runtime_adoption.py](/workspace/Code&DBs/Workflow/tests/integration/test_default_path_route_runtime_adoption.py)
- [Code&DBs/Workflow/tests/integration/test_default_path_pilot_wiring.py](/workspace/Code&DBs/Workflow/tests/integration/test_default_path_pilot_wiring.py)
- There is no existing dedicated Postgres writer module under [Code&DBs/Workflow/storage/postgres](/workspace/Code&DBs/Workflow/storage/postgres) for provider-route health windows.

## 3. Gap or ambiguity still remaining

- Phase 7 is real on schema and read paths, but the repo has no canonical writer for `provider_route_health_windows`.
- The missing piece is not table design; it is write ownership and derivation ownership.
- The repo currently has two different health surfaces with different semantics:
- canonical routing authority expects `provider_route_health_windows`
- mutable operational health is still maintained in `task_type_routing`
- [Code&DBs/Workflow/registry/native_runtime_profile_sync.py](/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py) is the cleanest adjacent sync seam, but today it has no health derivation fields of its own. It loads live candidates, latest budget windows, and latest eligibility states, not health-window metrics.
- That leaves one bounded ambiguity for this sprint:
- the packet needs one first-bridge derivation source for canonical health windows
- the only concrete non-test source named in current repo evidence is the existing `task_type_routing` counters and score for matching provider/model pairs
- This packet therefore treats `task_type_routing` as the first-bridge source unless a narrower repo-backed source is found during implementation.
- This sprint must not widen into:
- redesigning `task_type_routing` policy or ranking
- replacing route-health scoring formulas
- changing default-path selection rules
- redesigning operator surfaces
- solving all provider failover or observability gaps

## 4. One bounded first sprint only

- Implement exactly one bridge from current live route-health facts into canonical Phase 7 health windows.
- Preferred seam:
- add one explicit health-window writer helper or repository
- call it from [Code&DBs/Workflow/registry/native_runtime_profile_sync.py](/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py) beside the existing budget and eligibility writers
- Keep the derivation narrow:
- derive one canonical health window per synced live candidate
- use currently available repo facts only, with `task_type_routing` as the allowed first-bridge source if needed
- produce deterministic IDs and observation refs tied to the runtime profile and candidate
- add one focused integration proof that:
- prepares the minimum prerequisite rows
- runs `sync_native_runtime_profile_authority(...)` or `sync_native_runtime_profile_authority_async(...)`
- reads back the result through `load_provider_route_authority(...)` or `load_provider_route_authority_snapshot(...)`
- asserts the health window was created by repo code, not by direct SQL fixture insertion
- Stop after one real repo path writes canonical health windows and one proof exercises that path end to end.

## 5. Exact file or subsystem scope

- Primary implementation scope:
- [Code&DBs/Workflow/registry/native_runtime_profile_sync.py](/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py)
- one new dedicated writer module under [Code&DBs/Workflow/storage/postgres](/workspace/Code&DBs/Workflow/storage/postgres) if that is the cleanest fit
- or one tightly scoped adjacent repository/helper module under [Code&DBs/Workflow/registry](/workspace/Code&DBs/Workflow/registry) if introducing a storage module is unnecessary overhead for this first bridge
- Allowed compatibility-repair scope only if the new proof exposes a defect:
- [Code&DBs/Workflow/registry/provider_routing.py](/workspace/Code&DBs/Workflow/registry/provider_routing.py)
- [Code&DBs/Workflow/registry/model_routing.py](/workspace/Code&DBs/Workflow/registry/model_routing.py)
- Primary proof scope:
- one new focused integration test under [Code&DBs/Workflow/tests/integration](/workspace/Code&DBs/Workflow/tests/integration)
- or one tightly scoped extension of [Code&DBs/Workflow/tests/integration/test_provider_route_control_tower.py](/workspace/Code&DBs/Workflow/tests/integration/test_provider_route_control_tower.py)
- Read-only context:
- [Code&DBs/Workflow/runtime/task_type_router.py](/workspace/Code&DBs/Workflow/runtime/task_type_router.py)
- [Code&DBs/Workflow/runtime/default_path_pilot.py](/workspace/Code&DBs/Workflow/runtime/default_path_pilot.py)
- [Code&DBs/Workflow/runtime/provider_route_runtime.py](/workspace/Code&DBs/Workflow/runtime/provider_route_runtime.py)
- [Code&DBs/Workflow/observability/operator_dashboard.py](/workspace/Code&DBs/Workflow/observability/operator_dashboard.py)
- [Code&DBs/Databases/migrations/workflow/048_task_type_route_health.sql](/workspace/Code&DBs/Databases/migrations/workflow/048_task_type_route_health.sql)
- Explicitly out of scope:
- edits to [Code&DBs/Databases/migrations/workflow/007_provider_route_health_budget.sql](/workspace/Code&DBs/Databases/migrations/workflow/007_provider_route_health_budget.sql)
- broad refactors of [Code&DBs/Workflow/runtime/task_type_router.py](/workspace/Code&DBs/Workflow/runtime/task_type_router.py)
- budget-window redesign
- eligibility-state redesign
- operator UI or API changes
- generic provider-routing cleanup outside the health-window writer seam

## 6. Done criteria

- The repo contains one explicit non-test write owner for `provider_route_health_windows`.
- `sync_native_runtime_profile_authority(...)` and `sync_native_runtime_profile_authority_async(...)` both invoke that writer directly or through one shared helper.
- The implemented bridge produces canonical rows with, at minimum:
- `candidate_ref`
- `provider_ref`
- `health_status`
- `health_score`
- `sample_count`
- `failure_rate`
- `observed_window_started_at`
- `observed_window_ended_at`
- `observation_ref`
- A focused integration proof exercises the repo writer path and reads the resulting row back through `load_provider_route_authority(...)` or `load_provider_route_authority_snapshot(...)`.
- At least one proof that previously depended on direct `provider_route_health_windows` fixture SQL is replaced or supplemented by the repo-owned write path.
- Existing Phase 7 read-side tests still pass.
- No new migration is added.
- No ranking, demotion, or fallback-policy redesign is introduced.

## 7. Verification commands

- `cd` to the Praxis repository root (the directory that contains `scripts/_workflow_env.sh`)
- `. ./scripts/_workflow_env.sh && workflow_load_repo_env`
- `export PYTHONPATH='Code&DBs/Workflow'`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_provider_route_health_window_write_path.py' -q`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_provider_route_control_tower.py' -q`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_provider_route_runtime_wiring.py' -q`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_default_path_route_runtime_adoption.py' -q`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_default_path_pilot_wiring.py' -q`
- `rg -n 'provider_route_health_windows|health_status|health_score|observation_ref' 'Code&DBs/Workflow/registry/native_runtime_profile_sync.py' 'Code&DBs/Workflow/storage/postgres' 'Code&DBs/Workflow/registry' 'Code&DBs/Workflow/tests/integration'`
- `rg -n 'INSERT INTO provider_route_health_windows|UPDATE provider_route_health_windows|DELETE FROM provider_route_health_windows' 'Code&DBs/Workflow' | rg -v '/tests/'`

Expected verification outcome:

- one non-test repo seam now owns canonical health-window writes
- the native runtime profile sync path materializes Phase 7 health authority without direct fixture SQL
- control-tower, runtime, and operator consumers still load the same canonical health records without contract drift

## 8. Review -> healer -> human approval gate

- Review:
- confirm the change stayed inside the missing Phase 7 health-window writer seam
- confirm the implementation used repo code to create `provider_route_health_windows`, not direct fixture SQL inside the proof
- confirm the derivation bridge stayed narrow and did not redesign route-health policy
- confirm sync and async paths stayed aligned
- Healer:
- if review finds drift, repair only:
- the health-window writer seam
- the native runtime profile sync caller
- the focused integration proof
- do not widen healer work into task-routing redesign, budget redesign, eligibility redesign, or operator-surface work
- Human approval gate:
- require explicit human approval after review and any healer pass before a second Phase 7 sprint opens
- if a later Phase 7 sprint is approved, it should take exactly one adjacent seam next, such as replacing one remaining dependency on mutable `task_type_routing` health with stronger canonical authority
