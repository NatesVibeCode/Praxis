# Phase 3 Registry Authority

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `3` (`Registry Authority`), status `historical_foundation`, predecessor `2`, required closeout sequence `review -> healer -> human_approval`.

Grounding note: this packet is grounded in the live checkout at `/workspace`. The supplied platform root `/Users/nate/Praxis` is not present in this execution environment, so repo evidence and verification commands use `/workspace` while keeping the provided database target `postgresql://nate@127.0.0.1:5432/praxis`. The execution shard also says `execution_packets_ready=true`, `repo_snapshots_ready=true`, `verification_registry_ready=true`, `verify_refs_ready=true`, but `fully_proved_verification_coverage=0.0` and `verification_coverage=0.0`, so the first sprint must add one real proof rather than expand scope.

## 1. Objective in repo terms

- Reassert one explicit Phase 3 authority seam in the current repo: repo-local native runtime profile config must be able to populate canonical registry authority rows and then drive `load_registry_resolver(...)` without manual authority-row seeding.
- Keep the sprint bounded to the native workspace/runtime-profile path only:
- `config/runtime_profiles.json`
- `registry_workspace_authority`
- `registry_runtime_profile_authority`
- `load_registry_resolver(...)`
- First-sprint target: prove one end-to-end chain in repo terms:
- temp repo-local `config/runtime_profiles.json`
- native config resolution
- canonical registry sync
- Postgres-backed resolver load
- downstream intake-ready authority context

## 2. Current evidence in the repo

- Phase `3` is declared as `Registry Authority` in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json), with predecessor `2` and mandatory closeout sequence `review -> healer -> human_approval`.
- The Phase 3 schema authority already exists in [Code&DBs/Databases/migrations/workflow/002_registry_authority.sql](/workspace/Code&DBs/Databases/migrations/workflow/002_registry_authority.sql):
- `registry_workspace_authority`
- `registry_runtime_profile_authority`
- The repo also contains a later repair migration in [Code&DBs/Databases/migrations/workflow/056_native_runtime_profile_authority_repair.sql](/workspace/Code&DBs/Databases/migrations/workflow/056_native_runtime_profile_authority_repair.sql), which is evidence that native runtime-profile authority is still a live seam, not dead historical schema.
- The canonical Postgres repository seam already exists in [Code&DBs/Workflow/registry/repository.py](/workspace/Code&DBs/Workflow/registry/repository.py):
- `bootstrap_registry_authority_schema(...)`
- `PostgresRegistryAuthorityRepository`
- `load_registry_resolver(...)`
- `PostgresRegistryAuthorityRepository.load_resolver(...)` already auto-calls `sync_native_runtime_profile_authority_async(...)` when any requested runtime profile is native
- The native sync implementation already exists in [Code&DBs/Workflow/registry/native_runtime_profile_sync.py](/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py):
- `NativeRuntimeProfileConfig.workspace_record()`
- `NativeRuntimeProfileConfig.runtime_profile_record()`
- `load_native_runtime_profile_configs()`
- `resolve_native_runtime_profile_config(...)`
- `sync_native_runtime_profile_authority_async(...)`
- that sync path writes both registry authority tables and also refreshes related model/provider/route-side authority rows used by runtime-profile admission
- The repo-local native instance contract already exists separately in [Code&DBs/Workflow/runtime/instance.py](/workspace/Code&DBs/Workflow/runtime/instance.py):
- it accepts `PRAXIS_RUNTIME_PROFILES_CONFIG`
- it enforces the canonical `config/runtime_profiles.json` boundary
- it has focused proof for alternate repo-local config roots in [Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py)
- Current integration proof for Phase 3 still relies on hand-seeded authority rows instead of the native sync path:
- [Code&DBs/Workflow/tests/integration/test_registry_authority_path.py](/workspace/Code&DBs/Workflow/tests/integration/test_registry_authority_path.py) manually calls `upsert_workspace_authority(...)` and `upsert_runtime_profile_authority(...)`
- [Code&DBs/Workflow/tests/integration/test_context_bundle_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_context_bundle_repository.py) manually inserts the same authority rows before loading a resolver
- [Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py) resolves a native instance from repo-local config, then still seeds registry rows manually before `load_registry_resolver(...)`
- The live mismatch is concrete in code:
- `runtime.instance` can resolve a repo-local alternate `config/runtime_profiles.json`
- `registry.native_runtime_profile_sync` still reads its config via `_config_path()` rooted at `Path(__file__).resolve().parents[3]`
- `load_registry_resolver(...)` does not accept a config-path or env override
- [Code&DBs/Workflow/surfaces/api/_smoke_service.py](/workspace/Code&DBs/Workflow/surfaces/api/_smoke_service.py) passes `env` into DB setup, but `_load_smoke_registry_async(...)` still relies on `load_registry_resolver(...)` without any matching native-config authority input

## 3. Gap or ambiguity still remaining

- The repo has Phase 3 schema, repository code, native sync code, and native instance resolution, but the proof does not converge on one authority path.
- Today there are effectively two different native truths:
- `runtime.instance` can honor an explicit repo-local config path
- `registry.native_runtime_profile_sync` is pinned to the checked-out repo path
- Because of that split, existing tests prove pieces in isolation but do not prove the live Phase 3 chain:
- repo-local config override
- native registry sync
- canonical registry rows
- resolver load
- intake-ready authority payload
- The first sprint should remove that ambiguity with one end-to-end contract.
- Do not widen into:
- integration or connector registry work
- route-policy redesign
- context-bundle redesign
- repo-wide registry cleanup
- migration churn

## 4. One bounded first sprint only

- Add one focused integration proof that creates a temporary repo root containing a canonical `config/runtime_profiles.json` and exercises the native registry path without manual `upsert_*_authority(...)` setup.
- The sprint should prove:
- `resolve_native_instance(...)` can resolve the temp repo-local config
- registry sync can read the same temp repo-local config
- `load_registry_resolver(...)` loads canonical workspace/runtime-profile authority from Postgres after that sync
- the loaded resolver preserves explicit `repo_root`, nested `workdir`, `model_profile_id`, and `provider_policy_id`
- Prefer extending [Code&DBs/Workflow/tests/integration/test_registry_authority_path.py](/workspace/Code&DBs/Workflow/tests/integration/test_registry_authority_path.py) or adding one adjacent focused test file if that keeps the proof narrower.
- If the proof exposes drift, fix only the narrow seam needed to give registry sync the same repo-local config authority contract as `runtime.instance`.
- Stop once one real native config -> registry sync -> resolver proof exists and the existing registry/context smoke-adjacent tests still pass.

## 5. Exact file or subsystem scope

- Primary implementation scope:
- [Code&DBs/Workflow/registry/native_runtime_profile_sync.py](/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py)
- [Code&DBs/Workflow/registry/repository.py](/workspace/Code&DBs/Workflow/registry/repository.py)
- Primary regression scope:
- [Code&DBs/Workflow/tests/integration/test_registry_authority_path.py](/workspace/Code&DBs/Workflow/tests/integration/test_registry_authority_path.py)
- optionally one new focused integration test beside it under [Code&DBs/Workflow/tests/integration](/workspace/Code&DBs/Workflow/tests/integration)
- Read-only grounding references:
- [Code&DBs/Workflow/runtime/instance.py](/workspace/Code&DBs/Workflow/runtime/instance.py)
- [Code&DBs/Databases/migrations/workflow/002_registry_authority.sql](/workspace/Code&DBs/Databases/migrations/workflow/002_registry_authority.sql)
- [Code&DBs/Databases/migrations/workflow/056_native_runtime_profile_authority_repair.sql](/workspace/Code&DBs/Databases/migrations/workflow/056_native_runtime_profile_authority_repair.sql)
- [Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py)
- [Code&DBs/Workflow/tests/integration/test_context_bundle_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_context_bundle_repository.py)
- [Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py)
- [Code&DBs/Workflow/surfaces/api/_smoke_service.py](/workspace/Code&DBs/Workflow/surfaces/api/_smoke_service.py)
- Explicitly out of scope:
- [Code&DBs/Workflow/registry/runtime_profile_admission.py](/workspace/Code&DBs/Workflow/registry/runtime_profile_admission.py)
- [Code&DBs/Workflow/runtime/task_type_router.py](/workspace/Code&DBs/Workflow/runtime/task_type_router.py)
- [Code&DBs/Workflow/runtime/workflow/_routing.py](/workspace/Code&DBs/Workflow/runtime/workflow/_routing.py)
- integration registry, connector registry, reference catalog sync, or persona authority
- any new migration or schema expansion
- broad replacement of every manual registry setup in the repo

## 6. Done criteria

- One focused integration test proves the native registry authority path without manual `upsert_workspace_authority(...)` or `upsert_runtime_profile_authority(...)`.
- The proof uses a temp repo-local `config/runtime_profiles.json` and demonstrates parity between:
- `resolve_native_instance(...)`
- native registry sync
- `load_registry_resolver(...)`
- The resulting resolver preserves at minimum:
- `workspace_ref`
- `runtime_profile_ref`
- `repo_root`
- `workdir`
- `model_profile_id`
- `provider_policy_id`
- Existing integration proof still passes in [Code&DBs/Workflow/tests/integration/test_context_bundle_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_context_bundle_repository.py).
- Existing native-instance isolation proof still passes in [Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py).
- No migration is added and no unrelated registry family is changed.

## 7. Verification commands

- `cd /workspace`
- `export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'`
- `export PYTHONPATH='/workspace/Code&DBs/Workflow'`
- `python -m pytest '/workspace/Code&DBs/Workflow/tests/integration/test_registry_authority_path.py' -q`
- `python -m pytest '/workspace/Code&DBs/Workflow/tests/integration/test_context_bundle_repository.py' -q`
- `python -m pytest '/workspace/Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py' -q`
- `python -m pytest '/workspace/Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py' -q`
- `rg -n "_config_path\\(|load_native_runtime_profile_configs|sync_native_runtime_profile_authority_async|PRAXIS_RUNTIME_PROFILES_CONFIG|load_registry_resolver\\(" '/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py' '/workspace/Code&DBs/Workflow/registry/repository.py' '/workspace/Code&DBs/Workflow/runtime/instance.py' '/workspace/Code&DBs/Workflow/tests/integration/test_registry_authority_path.py'`

Expected verification outcome:

- the new proof no longer depends on manual registry authority-row setup for the native path
- registry sync and native instance resolution visibly depend on the same repo-local config contract
- existing context-bundle and native-smoke-adjacent tests still pass against canonical registry authority

## 8. Review -> healer -> human approval gate

- Review:
- confirm the sprint stayed on native workspace/runtime-profile authority parity
- confirm the new proof uses native sync plus `load_registry_resolver(...)` rather than manual `upsert_*_authority(...)`
- confirm the proof would have failed under the old hard-wired config-path behavior
- confirm no migration work, route-policy redesign, or unrelated registry-family cleanup leaked in
- Healer:
- if review finds drift, repair only:
- [Code&DBs/Workflow/registry/native_runtime_profile_sync.py](/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py)
- [Code&DBs/Workflow/registry/repository.py](/workspace/Code&DBs/Workflow/registry/repository.py)
- the one focused integration proof under [Code&DBs/Workflow/tests/integration](/workspace/Code&DBs/Workflow/tests/integration)
- rerun all verification commands
- Human approval gate:
- require explicit human approval after review and any healer pass before opening a second Phase 3 sprint
- if a later Phase 3 sprint is approved, take exactly one adjacent seam next, most likely `runtime_profile_admission.py` duplicate authority reads, not broad registry cleanup
