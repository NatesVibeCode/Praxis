# Phase 3 Registry Authority

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `3` (`Registry Authority`), status `historical_foundation`, predecessor phase `2`, with mandatory closeout sequence `review -> healer -> human_approval`.

Grounding note: this packet is based on the current checked-out repo snapshot under `/workspace`. The platform context supplied for downstream execution is `/Users/nate/Praxis` with database `postgresql://nate@127.0.0.1:5432/praxis`. The execution shard also shows proof coverage is still immature (`fully_proved_verification_coverage=0.0`, `verification_coverage=0.0`, `write_manifest_coverage=0.125`), so the first sprint must stay narrow and add one explicit runnable proof at a live registry seam.

## 1. Objective in repo terms

- Converge one real Phase 3 authority seam in the current repo: the DB-backed registry resolver must be able to derive native workspace and runtime-profile authority from the same repo-local runtime profile contract that `runtime.instance` resolves, instead of depending on hand-seeded Postgres rows in tests and bootstrap flows.
- Keep the sprint bounded to native workspace/runtime-profile authority for request intake and resolver loading. Do not widen Phase 3 into every other registry in the repo.
- Repo-level target for this sprint: a repo-local `config/runtime_profiles.json` should be sufficient to populate `registry_workspace_authority` and `registry_runtime_profile_authority` through the canonical native sync path, then drive `load_registry_resolver(...)` without manual `upsert_*_authority(...)` setup.

## 2. Current evidence in the repo

- The authority map declares phase `3` as `Registry Authority`, predecessor `2`, with mandatory closeout sequence `review -> healer -> human_approval` in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json).
- The Phase 3 schema seam already exists in [Code&DBs/Databases/migrations/workflow/002_registry_authority.sql](/workspace/Code&DBs/Databases/migrations/workflow/002_registry_authority.sql):
- it creates `registry_workspace_authority`
- it creates `registry_runtime_profile_authority`
- both tables are explicitly marked as canonical authority owned by `registry/`
- Later repo history already amends this seam. [Code&DBs/Databases/migrations/workflow/056_native_runtime_profile_authority_repair.sql](/workspace/Code&DBs/Databases/migrations/workflow/056_native_runtime_profile_authority_repair.sql) repairs stale native runtime-profile rows, proving the native authority path is a live concern rather than dead historical schema.
- The Postgres repository seam already exists in [Code&DBs/Workflow/registry/repository.py](/workspace/Code&DBs/Workflow/registry/repository.py):
- `bootstrap_registry_authority_schema(...)`
- `PostgresRegistryAuthorityRepository`
- `load_registry_resolver(...)`
- `load_resolver(...)` already auto-invokes `sync_native_runtime_profile_authority_async(...)` when a native runtime profile ref is requested
- The repo-local native source of truth already exists in [Code&DBs/Workflow/registry/native_runtime_profile_sync.py](/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py):
- `NativeRuntimeProfileConfig.workspace_record()`
- `NativeRuntimeProfileConfig.runtime_profile_record()`
- `sync_native_runtime_profile_authority(...)`
- `sync_native_runtime_profile_authority_async(...)`
- that sync path already writes both `registry_workspace_authority` and `registry_runtime_profile_authority`
- that sync path also refreshes the related `model_profiles`, `provider_policies`, binding rows, and route state needed for runtime-profile admission
- The native instance contract already exists separately in [Code&DBs/Workflow/runtime/instance.py](/workspace/Code&DBs/Workflow/runtime/instance.py):
- it resolves one repo-local `config/runtime_profiles.json`
- it supports an explicit `PRAXIS_RUNTIME_PROFILES_CONFIG` override as long as the file is still the canonical `config/runtime_profiles.json` inside a repo
- it already has focused unit proof in [Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py) for alternate repo-local config roots and nested `workdir`
- The current Phase 3 integration proofs bypass the native sync path instead of proving it:
- [Code&DBs/Workflow/tests/integration/test_registry_authority_path.py](/workspace/Code&DBs/Workflow/tests/integration/test_registry_authority_path.py) manually calls `upsert_workspace_authority(...)` and `upsert_runtime_profile_authority(...)`
- [Code&DBs/Workflow/tests/integration/test_context_bundle_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_context_bundle_repository.py) manually inserts both authority rows before building a resolver
- [Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py) resolves a native instance from repo-local config, then still manually inserts registry authority rows before calling `load_registry_resolver(...)`
- The live drift is concrete:
- `runtime.instance` can resolve a repo-local alternate `config/runtime_profiles.json`
- `registry.native_runtime_profile_sync` reads its config through a file-local `_config_path()` rooted at `Path(__file__).resolve().parents[3]`
- `load_registry_resolver(...)` has no way to accept the same explicit config path or env mapping that `runtime.instance` can use
- `surfaces/api/_smoke_service.py` passes `env` into database connection setup, but `_load_smoke_registry_async(...)` still relies on `load_registry_resolver(...)` without any config-path authority input

## 3. Gap or ambiguity still remaining

- The Phase 3 schema and repository are present. The unresolved gap is authority-path parity.
- Right now the repo has two different ways to decide native registry truth:
- `runtime.instance` can resolve from an explicit repo-local config path
- `registry.native_runtime_profile_sync` is pinned to the checked-out repo path and cannot be pointed at the same alternate repo-local config contract
- Because of that mismatch, the integration tests that should prove the native authority path instead seed registry rows manually, which means current proof does not actually cover:
- `config/runtime_profiles.json -> native sync -> registry tables -> load_registry_resolver(...) -> intake/context consumers`
- The first sprint should not widen into:
- integration registry or connector registry work
- provider-route policy redesign
- context-bundle redesign
- workspace-product UX
- repo-wide replacement of every direct registry read
- There is a second real duplication seam in [Code&DBs/Workflow/registry/runtime_profile_admission.py](/workspace/Code&DBs/Workflow/registry/runtime_profile_admission.py), which re-syncs native profiles and reads `registry_runtime_profile_authority` directly. That is Phase 3 relevant, but it is not the first sprint here. First prove the native sync/resolver path against the correct repo-local config contract.

## 4. One bounded first sprint only

- Add one focused integration contract that uses a temporary repo-local `config/runtime_profiles.json` and proves the registry authority path can sync from that repo-local contract without manual authority-row inserts.
- The sprint should:
- create a temp repo layout with canonical `config/runtime_profiles.json`
- define one native runtime profile where `workspace_ref`, `runtime_profile_ref`, `repo_root`, and nested `workdir` are all explicit
- resolve the native instance through `runtime.instance`
- bootstrap registry authority schema
- load the registry resolver through the canonical repository path
- assert that the resulting resolver contains the synced workspace/runtime-profile authority from the temp repo-local config
- assert that downstream intake or resolver consumers see the same `repo_root`, `workdir`, `model_profile_id`, and `provider_policy_id`
- If the new contract exposes drift, repair only the native sync/repository path needed to let `load_registry_resolver(...)` honor the same repo-local config authority as `runtime.instance`.
- Stop once the native config path is proved end to end and existing registry/context proofs still pass. Do not widen into broader registry cleanup, runtime-profile routing redesign, or additional registry families.

## 5. Exact file or subsystem scope

- Primary implementation scope:
- [Code&DBs/Workflow/registry/native_runtime_profile_sync.py](/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py)
- [Code&DBs/Workflow/registry/repository.py](/workspace/Code&DBs/Workflow/registry/repository.py)
- Primary regression scope:
- [Code&DBs/Workflow/tests/integration/test_registry_authority_path.py](/workspace/Code&DBs/Workflow/tests/integration/test_registry_authority_path.py) or one new focused integration test beside it
- [Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py) if needed to remove manual authority seeding for this seam
- Read-only authority references:
- [Code&DBs/Workflow/runtime/instance.py](/workspace/Code&DBs/Workflow/runtime/instance.py)
- [Code&DBs/Databases/migrations/workflow/002_registry_authority.sql](/workspace/Code&DBs/Databases/migrations/workflow/002_registry_authority.sql)
- [Code&DBs/Databases/migrations/workflow/056_native_runtime_profile_authority_repair.sql](/workspace/Code&DBs/Databases/migrations/workflow/056_native_runtime_profile_authority_repair.sql)
- [Code&DBs/Workflow/tests/integration/test_context_bundle_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_context_bundle_repository.py)
- [Code&DBs/Workflow/surfaces/api/_smoke_service.py](/workspace/Code&DBs/Workflow/surfaces/api/_smoke_service.py)
- Explicitly out of scope:
- [Code&DBs/Workflow/registry/runtime_profile_admission.py](/workspace/Code&DBs/Workflow/registry/runtime_profile_admission.py)
- [Code&DBs/Workflow/runtime/task_type_router.py](/workspace/Code&DBs/Workflow/runtime/task_type_router.py)
- [Code&DBs/Workflow/runtime/workflow/_routing.py](/workspace/Code&DBs/Workflow/runtime/workflow/_routing.py)
- integration registry, connector registry, or reference catalog sync
- any migration renumbering or schema expansion
- any broader refactor of every registry consumer

## 6. Done criteria

- A focused integration test proves the native registry path from repo-local runtime profile config into canonical registry rows without manual `upsert_workspace_authority(...)` or `upsert_runtime_profile_authority(...)`.
- The test proves `load_registry_resolver(...)` can resolve the same native workspace/runtime-profile authority contract that `runtime.instance` resolves for the same repo-local config.
- The synced resolver preserves explicit `repo_root` and nested `workdir` values from the temp repo-local config.
- The synced resolver preserves explicit `model_profile_id` and `provider_policy_id` values from the temp repo-local config.
- Existing registry-path and context-bundle integration proofs still pass after the change.
- No new registry family, no new schema objects, and no repo-wide direct-read cleanup lands in this sprint.

## 7. Verification commands

- `cd /Users/nate/Praxis`
- `export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'`
- `export PYTHONPATH='Code&DBs/Workflow'`
- `python -m pytest Code\&DBs/Workflow/tests/integration/test_registry_authority_path.py -q`
- `python -m pytest Code\&DBs/Workflow/tests/integration/test_context_bundle_repository.py -q`
- `python -m pytest Code\&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py -q`
- `python -m pytest Code\&DBs/Workflow/tests/integration/test_native_instance_isolation.py -q`
- `rg -n "_config_path\\(|PRAXIS_RUNTIME_PROFILES_CONFIG|load_registry_resolver\\(|sync_native_runtime_profile_authority_async\\(" Code\&DBs/Workflow/registry/native_runtime_profile_sync.py Code\&DBs/Workflow/registry/repository.py Code\&DBs/Workflow/runtime/instance.py Code\&DBs/Workflow/tests/integration/test_registry_authority_path.py`

Expected verification outcome:

- the registry resolver path is visibly tied to the same repo-local runtime profile contract as native instance resolution
- the new Phase 3 proof no longer depends on hand-seeded authority rows for the native path
- existing registry/context consumers still pass against the canonical authority seam

## 8. Review -> healer -> human approval gate

- Review:
- confirm the sprint stayed on native workspace/runtime-profile authority parity and did not widen into general registry cleanup
- confirm the new proof uses native sync plus `load_registry_resolver(...)` rather than manual `upsert_*_authority(...)` setup
- confirm the proof would have failed under the old hardwired config-path behavior
- confirm no migration, provider-route redesign, or integration-registry work leaked into the change set
- Healer:
- if review finds drift, repair only the scoped native sync/repository/test seam
- do not widen healer work into `runtime_profile_admission.py`, task routing, context-bundle redesign, or additional registry families
- Human approval gate:
- require explicit human approval after review and any healer pass before opening a second Phase 3 sprint
- the next Phase 3 sprint, if approved later, should take exactly one adjacent seam, most likely the `runtime_profile_admission.py` duplicate authority read path, not “finish registry authority” in one pass
