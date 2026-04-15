# Phase 1 Workspace Boundary Contract

Status: execution_ready

Authority map:
- `planning/phase-program/praxis_0_100_registry.json` declares phase `1` title `Workspace Boundary Contract`
- phase `1` predecessor = `0`
- governance requires `one_phase_one_thing = true`
- governance requires `mandatory_review_healer_between_phases = true`
- governance requires `human_approval_between_phases = true`
- `config/cascade/specs/W_phase_001_010_foundation_wave_20260414.queue.json` defines job `phase_001_workspace_boundary_contract` and requires one bounded execution packet for this phase
- execution context shard for `phase_001_workspace_boundary_contract` says `execution_packets_ready=true`, `repo_snapshots_ready=true`, `verification_registry_ready=true`, `verify_refs_ready=true`, while `fully_proved_verification_coverage=0.0`

Grounding note:
- repo evidence below was read from the mounted checkout at `/workspace`
- platform execution root for later implementation is `/Users/nate/Praxis`
- database for later verification is `postgresql://nate@127.0.0.1:5432/praxis`

## 1. Objective in repo terms

- Preserve the repo-local native workspace contract where `repo_root` and `workdir` are separate authority fields and may resolve to different paths.
- The concrete Phase 1 seam in this repo is the handoff from runtime-profile config into workflow runtime registry builders:
- `Code&DBs/Workflow/registry/native_runtime_profile_sync.py`
- `Code&DBs/Workflow/runtime/workflow/_admission.py`
- `Code&DBs/Workflow/runtime/workflow/runtime_setup.py`
- First sprint target: prove that workflow runtime builders preserve `config.repo_root` when a runtime profile uses a nested repo-local `workdir`.
- Stop at that seam. Do not widen Phase 1 into multi-repo policy, workspace UX, operator controls, or registry redesign.

## 2. Current evidence in the repo

- `planning/phase-program/praxis_0_100_registry.json` marks phase `1` as `Workspace Boundary Contract` and requires closeout sequence `review -> healer -> human_approval`.
- `config/cascade/specs/W_phase_001_010_foundation_wave_20260414.queue.json` asks for one bounded execution packet for this phase.
- `config/runtime_profiles.json` is the checked-in native authority source for runtime profiles and currently defines:
- `workspace_ref`
- `repo_root`
- `workdir`
- `receipts_dir`
- `topology_dir`
- The checked-in default profile currently sets `"repo_root": "."` and `"workdir": "."`, so default execution does not expose `repo_root != workdir`.
- `Code&DBs/Workflow/registry/native_runtime_profile_sync.py` resolves config values into `NativeRuntimeProfileConfig` and `workspace_record()` emits `WorkspaceAuthorityRecord(repo_root=self.repo_root, workdir=self.workdir)`.
- `Code&DBs/Workflow/runtime/instance.py` already enforces the repo-local native boundary:
- canonical config file name must be `config/runtime_profiles.json`
- config location determines `repo_root`
- `workdir`, `receipts_dir`, and `topology_dir` must stay inside that repo root
- `Code&DBs/Workflow/runtime/workflow/_admission.py` currently collapses the boundary in `_graph_registry_for_request(...)` by building `WorkspaceAuthorityRecord(..., repo_root=workdir, workdir=workdir)`.
- `Code&DBs/Workflow/runtime/workflow/runtime_setup.py` currently collapses the boundary in `_build_registry(...)` with `repo_root = workdir`.
- Existing nearby proof already covers adjacent parts of the contract:
- `Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py` proves native instance resolution allows a nested `workdir` under a repo-local config
- `Code&DBs/Workflow/tests/integration/test_bounded_native_primary_proof.py` proves wrapper and CLI surfaces expose the native instance contract
- `Code&DBs/Workflow/tests/integration/test_registry_authority_path.py` proves intake preserves `repo_root` and `workdir` if a correct `WorkspaceAuthorityRecord` already exists
- There is no dedicated integration proof at `Code&DBs/Workflow/tests/integration/test_workspace_boundary_contract.py`.

## 3. Gap or ambiguity still remaining

- The unresolved gap is narrow: there is no focused proof for the exact builder seam where native runtime-profile authority becomes a `WorkspaceAuthorityRecord` inside workflow runtime code.
- Because the checked-in default profile uses identical paths for `repo_root` and `workdir`, the collapse bug can survive normal local runs.
- The unresolved decision is specific:
- preserve `config.repo_root`
- do not re-derive `repo_root` from `workdir`
- do not redesign the workspace model
- The proof-metric shard reports `fully_proved_verification_coverage=0.0`, which matches the absence of a direct Phase 1 boundary proof.

## 4. One bounded first sprint only

- Sprint label: preserve native `repo_root` / `workdir` separation through workflow runtime builders.
- Add one focused integration proof that creates a repo-local runtime profile with a nested `workdir` and exercises both builder seams:
- `_graph_registry_for_request(...)` in `Code&DBs/Workflow/runtime/workflow/_admission.py`
- `_build_registry(...)` in `Code&DBs/Workflow/runtime/workflow/runtime_setup.py`
- The proof should assert that both builders preserve:
- `workspace_ref`
- resolved native `repo_root`
- resolved nested `workdir`
- If the proof fails, fix only the two builder seams needed to make it pass.
- Stop once:
- the new proof passes
- `test_native_instance_isolation.py` still passes
- `test_bounded_native_primary_proof.py` still passes
- `test_registry_authority_path.py` still passes
- Explicitly out of scope for this sprint:
- changing the checked-in default profile in `config/runtime_profiles.json`
- changing schema or migrations
- changing registry persistence shape
- changing CLI, wrapper, operator, API, or control-plane behavior
- general workspace abstraction cleanup

## 5. Exact file or subsystem scope

- Read scope:
- `planning/phase-program/praxis_0_100_registry.json`
- `config/cascade/specs/W_phase_001_010_foundation_wave_20260414.queue.json`
- `config/runtime_profiles.json`
- `Code&DBs/Workflow/registry/native_runtime_profile_sync.py`
- `Code&DBs/Workflow/runtime/instance.py`
- `Code&DBs/Workflow/runtime/workflow/_admission.py`
- `Code&DBs/Workflow/runtime/workflow/runtime_setup.py`
- `Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py`
- `Code&DBs/Workflow/tests/integration/test_bounded_native_primary_proof.py`
- `Code&DBs/Workflow/tests/integration/test_registry_authority_path.py`
- Write scope:
- `Code&DBs/Workflow/runtime/workflow/_admission.py`
- `Code&DBs/Workflow/runtime/workflow/runtime_setup.py`
- `Code&DBs/Workflow/tests/integration/test_workspace_boundary_contract.py`
- Subsystem boundary:
- native runtime-profile authority flowing into workflow runtime intake/setup registry construction
- `WorkspaceAuthorityRecord` construction inside workflow admission/setup only
- Explicitly out of scope:
- `Code&DBs/Workflow/registry/**`
- `Code&DBs/Workflow/surfaces/**`
- `Code&DBs/Workflow/runtime/operator_*`
- database migrations
- product workspace UX
- fork, worktree, or multi-repo policy

## 6. Done criteria

- A focused integration test exists for a repo-local runtime profile where resolved `repo_root` and resolved `workdir` are different.
- That test covers both `_graph_registry_for_request(...)` and `_build_registry(...)`.
- The test would fail against the current collapse behavior where `repo_root` is set from `workdir`.
- `_graph_registry_for_request(...)` preserves `config.repo_root`.
- `_build_registry(...)` preserves `config.repo_root`.
- Existing adjacent proofs still pass:
- `Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py`
- `Code&DBs/Workflow/tests/integration/test_bounded_native_primary_proof.py`
- `Code&DBs/Workflow/tests/integration/test_registry_authority_path.py`
- No new schema, config grammar, or workspace abstraction is introduced.

## 7. Verification commands

```bash
cd /Users/nate/Praxis
export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'
export PYTHONPATH='Code&DBs/Workflow'
python -m pytest 'Code&DBs/Workflow/tests/integration/test_workspace_boundary_contract.py' -q
python -m pytest 'Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py' -q
python -m pytest 'Code&DBs/Workflow/tests/integration/test_bounded_native_primary_proof.py' -q
python -m pytest 'Code&DBs/Workflow/tests/integration/test_registry_authority_path.py' -q
rg -n 'repo_root=workdir|repo_root = workdir' \
  'Code&DBs/Workflow/runtime/workflow/_admission.py' \
  'Code&DBs/Workflow/runtime/workflow/runtime_setup.py'
rg -n 'test_workspace_boundary_contract|_graph_registry_for_request|_build_registry' \
  'Code&DBs/Workflow/tests/integration' \
  'Code&DBs/Workflow/runtime/workflow/_admission.py' \
  'Code&DBs/Workflow/runtime/workflow/runtime_setup.py'
```

Expected verification result:
- the new integration proof demonstrates distinct `repo_root` and `workdir` survive both workflow builder seams
- adjacent native-boundary proofs still pass
- direct `repo_root <- workdir` collapse is absent from the two targeted builder functions
- the proof and code references still stay confined to the two builder seams plus the focused integration test

## 8. Review -> healer -> human approval gate

- Review:
- confirm the packet stays inside Phase 1 workspace-boundary preservation and does not drift into general workspace architecture
- confirm the first sprint is one seam wide: two builder functions plus one focused integration proof
- confirm the proof would have caught the current `repo_root <- workdir` collapse
- confirm no registry, API, operator, or migration work leaked into scope
- Healer:
- if review finds drift or undercoverage, repair only:
- `Code&DBs/Workflow/runtime/workflow/_admission.py`
- `Code&DBs/Workflow/runtime/workflow/runtime_setup.py`
- `Code&DBs/Workflow/tests/integration/test_workspace_boundary_contract.py`
- rerun all verification commands
- Human approval gate:
- require explicit human approval after review and any healer pass
- do not begin Phase 2 `Control Plane Core` until approval is recorded
- do not widen Phase 1 into broader workspace architecture until approval is recorded
