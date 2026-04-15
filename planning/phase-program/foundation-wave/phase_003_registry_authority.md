# Phase 3 Registry Authority

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) declares phase `3` as `Registry Authority`, predecessor phase `2`, status `historical_foundation`, and mandatory closeout `review -> healer -> human_approval`.

Grounding note: this packet is grounded in the live checkout at `/workspace` and intended for execution in the declared platform root `/Users/nate/Praxis` against `postgresql://nate@127.0.0.1:5432/praxis`. The execution shard says `execution_packets_ready=true`, `repo_snapshots_ready=true`, `verification_registry_ready=true`, and `verify_refs_ready=true`, but `verification_coverage=0.0` and `fully_proved_verification_coverage=0.0`, so the first sprint must add one narrow proof instead of widening Phase 3.

## 1. Objective in repo terms

- Prove one canonical authority path for native registry resolution in the current repo.
- In concrete repo terms, the path must be:
- `config/runtime_profiles.json`
- repo-local native instance resolution
- native registry sync
- `registry_workspace_authority`
- `registry_runtime_profile_authority`
- `load_registry_resolver(...)`
- The bounded Phase 3 objective is to make the native config authority used by [Code&DBs/Workflow/runtime/instance.py](/workspace/Code&DBs/Workflow/runtime/instance.py) and the native config authority used by [Code&DBs/Workflow/registry/native_runtime_profile_sync.py](/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py) converge on the same repo-local contract, then prove that `load_registry_resolver(...)` can resolve the canonical rows without manual authority-row seeding in the test.

## 2. Current evidence in the repo

- Phase `3` is declared as `Registry Authority` in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json), with predecessor `2` and required closeout `review -> healer -> human_approval`.
- [Code&DBs/Databases/migrations/workflow/002_registry_authority.sql](/workspace/Code&DBs/Databases/migrations/workflow/002_registry_authority.sql) already defines the canonical Phase 3 tables:
- `registry_workspace_authority`
- `registry_runtime_profile_authority`
- [Code&DBs/Workflow/registry/README.md](/workspace/Code&DBs/Workflow/registry/README.md) already states the boundary in plain repo terms:
- registry owns workspace identity
- config resolution
- path resolution
- resource lookup
- registry does not own runtime state, workflow execution, or receipt writing
- [Code&DBs/Workflow/registry/repository.py](/workspace/Code&DBs/Workflow/registry/repository.py) already provides the canonical Postgres seam:
- `bootstrap_registry_authority_schema(...)`
- `PostgresRegistryAuthorityRepository`
- `load_registry_resolver(...)`
- `PostgresRegistryAuthorityRepository.load_resolver(...)` auto-runs `sync_native_runtime_profile_authority_async(...)` when the requested runtime profile is native
- [Code&DBs/Workflow/registry/native_runtime_profile_sync.py](/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py) already defines the native sync surface:
- `load_native_runtime_profile_configs()`
- `resolve_native_runtime_profile_config(...)`
- `is_native_runtime_profile_ref(...)`
- `sync_native_runtime_profile_authority_async(...)`
- that module currently hard-wires `_config_path()` to `Path(__file__).resolve().parents[3] / "config" / "runtime_profiles.json"`
- [config/runtime_profiles.json](/workspace/config/runtime_profiles.json) is present in the repo and currently declares:
- `default_runtime_profile = "praxis"`
- runtime profile `praxis`
- `workspace_ref = "praxis"`
- repo-local `repo_root = "."`
- repo-local `workdir = "."`
- [Code&DBs/Workflow/runtime/instance.py](/workspace/Code&DBs/Workflow/runtime/instance.py) already enforces the native repo-local instance contract:
- `PRAXIS_RUNTIME_PROFILES_CONFIG`
- canonical `config/runtime_profiles.json` boundary
- repo-root derivation from the config path
- [Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py) already proves that `runtime.instance` can resolve a temporary repo-local `config/runtime_profiles.json` and reject legacy grammar or boundary drift.
- Current integration proof for registry authority still depends on manual row seeding instead of the native sync path:
- [Code&DBs/Workflow/tests/integration/test_registry_authority_path.py](/workspace/Code&DBs/Workflow/tests/integration/test_registry_authority_path.py) manually calls `upsert_workspace_authority(...)` and `upsert_runtime_profile_authority(...)`
- [Code&DBs/Workflow/tests/integration/test_context_bundle_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_context_bundle_repository.py) manually inserts the same authority rows before `load_registry_resolver(...)`
- [Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py) resolves a native instance, then still seeds registry rows manually before `load_registry_resolver(...)`
- There is also live historical drift inside the repo:
- [Code&DBs/Databases/migrations/workflow/056_native_runtime_profile_authority_repair.sql](/workspace/Code&DBs/Databases/migrations/workflow/056_native_runtime_profile_authority_repair.sql) still repairs `dag-project` rows even though the current checked-in config authority is `praxis`
- that mismatch is evidence that native registry authority has not yet been proved end to end from the current repo truth

## 3. Gap or ambiguity still remaining

- The repo already has schema, repository code, native sync code, and native instance resolution, but the authority chain is not proved as one path.
- Today there are two different native config authorities:
- `runtime.instance` can honor an explicit repo-local `config/runtime_profiles.json` via `PRAXIS_RUNTIME_PROFILES_CONFIG`
- `registry.native_runtime_profile_sync` reads only its own checked-out `_config_path()`
- Because of that split, the repo still relies on manual authority-row seeding in the tests that matter most for Phase 3.
- The missing proof is narrow:
- one temporary repo-local `config/runtime_profiles.json`
- one native instance resolved from that config
- one registry sync sourced from that same config
- one `load_registry_resolver(...)` call reading the resulting canonical rows
- one downstream assertion that the resolver contains the expected workspace and runtime-profile payload
- Do not widen this sprint into:
- route-catalog redesign
- provider-policy redesign
- context-bundle redesign
- registry family cleanup outside workspace/runtime-profile authority
- migration renumbering or broad historical cleanup

## 4. One bounded first sprint only

- Add one focused integration proof for native config parity and resolver loading.
- The sprint should:
- create a temporary repo root with a canonical `config/runtime_profiles.json`
- point native instance resolution at that config
- make native registry sync read that same repo-local config authority
- call `load_registry_resolver(...)` without manual `upsert_workspace_authority(...)` or `upsert_runtime_profile_authority(...)`
- assert the loaded resolver contains the expected:
- `workspace_ref`
- `runtime_profile_ref`
- `repo_root`
- `workdir`
- `model_profile_id`
- `provider_policy_id`
- Prefer extending [Code&DBs/Workflow/tests/integration/test_registry_authority_path.py](/workspace/Code&DBs/Workflow/tests/integration/test_registry_authority_path.py) unless a new adjacent focused test file is cleaner.
- If the new proof exposes a defect, fix only the narrow seam required to give `registry.native_runtime_profile_sync` the same repo-local config contract already enforced by `runtime.instance`.
- Stop after one real `config -> sync -> canonical rows -> resolver` proof exists and the nearby registry/native-instance tests stay green.

## 5. Exact file or subsystem scope

- Primary implementation scope:
- [Code&DBs/Workflow/registry/native_runtime_profile_sync.py](/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py)
- [Code&DBs/Workflow/registry/repository.py](/workspace/Code&DBs/Workflow/registry/repository.py)
- Primary proof scope:
- [Code&DBs/Workflow/tests/integration/test_registry_authority_path.py](/workspace/Code&DBs/Workflow/tests/integration/test_registry_authority_path.py)
- optional one new adjacent focused integration test under [Code&DBs/Workflow/tests/integration](/workspace/Code&DBs/Workflow/tests/integration)
- Read-only grounding references:
- [config/runtime_profiles.json](/workspace/config/runtime_profiles.json)
- [Code&DBs/Workflow/runtime/instance.py](/workspace/Code&DBs/Workflow/runtime/instance.py)
- [Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py)
- [Code&DBs/Workflow/tests/integration/test_context_bundle_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_context_bundle_repository.py)
- [Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py)
- [Code&DBs/Databases/migrations/workflow/002_registry_authority.sql](/workspace/Code&DBs/Databases/migrations/workflow/002_registry_authority.sql)
- [Code&DBs/Databases/migrations/workflow/056_native_runtime_profile_authority_repair.sql](/workspace/Code&DBs/Databases/migrations/workflow/056_native_runtime_profile_authority_repair.sql)
- Explicitly out of scope:
- [Code&DBs/Workflow/registry/runtime_profile_admission.py](/workspace/Code&DBs/Workflow/registry/runtime_profile_admission.py)
- [Code&DBs/Workflow/registry/model_routing.py](/workspace/Code&DBs/Workflow/registry/model_routing.py)
- [Code&DBs/Workflow/registry/provider_routing.py](/workspace/Code&DBs/Workflow/registry/provider_routing.py)
- [Code&DBs/Workflow/registry/context_bundle_repository.py](/workspace/Code&DBs/Workflow/registry/context_bundle_repository.py), except as regression coverage
- migration edits
- broad replacement of every manual registry fixture in the repo

## 6. Done criteria

- One focused integration proof exists for the native Phase 3 authority path without manual authority-row seeding.
- That proof uses a temporary repo-local `config/runtime_profiles.json` and demonstrates parity between:
- `resolve_native_instance(...)`
- native registry sync
- `load_registry_resolver(...)`
- The proof asserts at minimum:
- `workspace_ref`
- `runtime_profile_ref`
- `repo_root`
- `workdir`
- `model_profile_id`
- `provider_policy_id`
- Existing native-instance boundary coverage in [Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py) still passes.
- Existing registry authority proof in [Code&DBs/Workflow/tests/integration/test_registry_authority_path.py](/workspace/Code&DBs/Workflow/tests/integration/test_registry_authority_path.py) still passes after being converted or extended to the canonical native path.
- Existing adjacent registry consumer coverage in [Code&DBs/Workflow/tests/integration/test_context_bundle_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_context_bundle_repository.py) still passes.
- No migration file is added and no out-of-scope registry family is changed.

## 7. Verification commands

- `cd /Users/nate/Praxis`
- `export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'`
- `export PYTHONPATH='Code&DBs/Workflow'`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_registry_authority_path.py' -q`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py' -q`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_context_bundle_repository.py' -q`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py' -q`
- `rg -n "_config_path\\(|load_native_runtime_profile_configs|sync_native_runtime_profile_authority_async|PRAXIS_RUNTIME_PROFILES_CONFIG|load_registry_resolver\\(|upsert_workspace_authority|upsert_runtime_profile_authority" 'Code&DBs/Workflow/registry/native_runtime_profile_sync.py' 'Code&DBs/Workflow/registry/repository.py' 'Code&DBs/Workflow/runtime/instance.py' 'Code&DBs/Workflow/tests/integration/test_registry_authority_path.py' 'Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py'`

Expected verification outcome:

- the native registry proof no longer depends on manual `upsert_workspace_authority(...)` or `upsert_runtime_profile_authority(...)`
- registry sync and native instance resolution visibly depend on the same repo-local config contract
- adjacent registry consumers still pass against canonical registry authority rows

## 8. Review -> healer -> human approval gate

- Review:
- confirm the sprint stayed on workspace/runtime-profile registry authority only
- confirm the new proof uses native config authority plus `load_registry_resolver(...)`, not manual row insertion under a helper wrapper
- confirm the proof would have failed under the old split between `runtime.instance` config resolution and `registry.native_runtime_profile_sync` config resolution
- confirm no migration edits, route-catalog work, provider-routing work, or broad registry cleanup leaked in
- Healer:
- if review finds drift, repair only:
- [Code&DBs/Workflow/registry/native_runtime_profile_sync.py](/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py)
- [Code&DBs/Workflow/registry/repository.py](/workspace/Code&DBs/Workflow/registry/repository.py)
- the one focused integration proof under [Code&DBs/Workflow/tests/integration](/workspace/Code&DBs/Workflow/tests/integration)
- rerun the full verification command set
- Human approval gate:
- require explicit human approval after review and any healer pass before opening another Phase 3 sprint
- if a later Phase 3 sprint is approved, take one adjacent seam only, likely a remaining duplicate authority consumer such as [Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py), not broad registry cleanup
