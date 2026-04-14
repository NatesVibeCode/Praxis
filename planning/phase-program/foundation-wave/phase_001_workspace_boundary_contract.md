# Phase 1 Workspace Boundary Contract

Status: execution_ready

Authority map:
- [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `1` title = `Workspace Boundary Contract`
- [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) governance requires `one_phase_one_thing = true`
- [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) governance requires `mandatory_review_healer_between_phases = true`
- [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) governance requires `human_approval_between_phases = true`
- [config/cascade/specs/W_phase_001_010_foundation_wave_20260414.queue.json](/workspace/config/cascade/specs/W_phase_001_010_foundation_wave_20260414.queue.json) job `phase_001_workspace_boundary_contract` requires one bounded first sprint with explicit boundaries, files, and verification

Grounding note:
- Repo evidence in this packet was read from the mounted checkout at `/workspace`
- Execution commands below target the declared platform repo root `/Users/nate/Praxis`
- The packet is grounded in current files and current defects in this repo, not in a generic workspace model

## 1. Objective in repo terms

- Preserve the native runtime-profile boundary where `repo_root` and `workdir` are separate fields and may resolve to different repo-local paths.
- In current repo terms, the Phase 1 seam is:
- [config/runtime_profiles.json](/workspace/config/runtime_profiles.json)
- [Code&DBs/Workflow/registry/native_runtime_profile_sync.py](/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py)
- [Code&DBs/Workflow/runtime/workflow/_admission.py](/workspace/Code&DBs/Workflow/runtime/workflow/_admission.py)
- [Code&DBs/Workflow/runtime/workflow/runtime_setup.py](/workspace/Code&DBs/Workflow/runtime/workflow/runtime_setup.py)
- First-sprint target: prove that workflow runtime registry builders preserve the resolved native `repo_root` instead of collapsing it to `workdir` when a profile declares a nested repo-local working directory.
- This phase is one boundary only: native workspace contract -> `WorkspaceAuthorityRecord` creation in workflow builders.

## 2. Current evidence in the repo

- Phase `1` is declared in the registry as `Workspace Boundary Contract` in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json), with required closeout sequence `review -> healer -> human_approval`.
- [config/cascade/specs/W_phase_001_010_foundation_wave_20260414.queue.json](/workspace/config/cascade/specs/W_phase_001_010_foundation_wave_20260414.queue.json) says this job must produce one bounded execution packet for Phase 1.
- [config/runtime_profiles.json](/workspace/config/runtime_profiles.json) defines the native runtime-profile contract fields:
- `workspace_ref`
- `repo_root`
- `workdir`
- `receipts_dir`
- `topology_dir`
- The checked-in default profile currently sets `"repo_root": "."` and `"workdir": "."`, so the default config does not expose a `repo_root != workdir` regression by itself.
- [Code&DBs/Workflow/registry/native_runtime_profile_sync.py](/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py) resolves profile paths relative to the repo root and emits `WorkspaceAuthorityRecord(repo_root=self.repo_root, workdir=self.workdir)` through `NativeRuntimeProfileConfig.workspace_record()`.
- [Code&DBs/Workflow/runtime/instance.py](/workspace/Code&DBs/Workflow/runtime/instance.py) enforces the repo-local native contract:
- canonical config path must be `config/runtime_profiles.json`
- `repo_root` is the repo owning that config
- `workdir`, `receipts_dir`, and `topology_dir` must stay inside that repo
- [Code&DBs/Workflow/runtime/workflow/_admission.py](/workspace/Code&DBs/Workflow/runtime/workflow/_admission.py) currently constructs `WorkspaceAuthorityRecord(..., repo_root=workdir, workdir=workdir)` in `_graph_registry_for_request(...)`.
- [Code&DBs/Workflow/runtime/workflow/runtime_setup.py](/workspace/Code&DBs/Workflow/runtime/workflow/runtime_setup.py) currently sets `repo_root = workdir` in `_build_registry(...)`.
- Existing tests already cover adjacent authority surfaces:
- [Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py) proves native instance resolution accepts a nested `workdir` and rejects boundary drift.
- [Code&DBs/Workflow/tests/integration/test_bounded_native_primary_proof.py](/workspace/Code&DBs/Workflow/tests/integration/test_bounded_native_primary_proof.py) proves wrapper and CLI surfaces expose the repo-local native contract.
- [Code&DBs/Workflow/tests/integration/test_registry_authority_path.py](/workspace/Code&DBs/Workflow/tests/integration/test_registry_authority_path.py) proves intake preserves distinct `repo_root` and `workdir` once a correct `WorkspaceAuthorityRecord` already exists.

## 3. Gap or ambiguity still remaining

- There is no focused integration proof for the exact builder seam where native runtime-profile data becomes a `WorkspaceAuthorityRecord` inside workflow runtime code.
- Because the default runtime profile uses `"repo_root": "."` and `"workdir": "."`, the current collapse bug can survive normal default-path testing.
- The active ambiguity is not “what is a workspace” in general. The active ambiguity is narrower:
- should workflow builder code preserve `config.repo_root`
- or is it allowed to derive `repo_root` from `workdir`
- Current repo evidence says the native contract keeps those fields distinct, but two workflow builder seams do not.
- Phase drift risk is high because `workspace` appears across operator, product, and runtime surfaces. This sprint must stay pinned to the two builder seams above and not expand into broader workspace architecture.

## 4. One bounded first sprint only

- Sprint label: preserve native `repo_root` / `workdir` separation through workflow runtime builders.
- Build one new integration proof that creates a repo-local runtime profile with nested `workdir` and exercises both builder seams:
- `_graph_registry_for_request(...)` in [Code&DBs/Workflow/runtime/workflow/_admission.py](/workspace/Code&DBs/Workflow/runtime/workflow/_admission.py)
- `_build_registry(...)` in [Code&DBs/Workflow/runtime/workflow/runtime_setup.py](/workspace/Code&DBs/Workflow/runtime/workflow/runtime_setup.py)
- The new proof should assert that both builders preserve:
- `workspace_ref`
- canonical resolved `repo_root`
- canonical resolved nested `workdir`
- If the proof fails, fix only the two builder seams needed to make it pass.
- Stop after the new proof passes and the existing adjacent native-boundary tests still pass.
- Explicitly not in this sprint:
- changing the checked-in default profile values in [config/runtime_profiles.json](/workspace/config/runtime_profiles.json)
- redesigning workspace abstractions
- adding database schema or migration work
- changing registry repository shape
- changing CLI, wrapper, operator, or control-plane surfaces

## 5. Exact file or subsystem scope

- Files to read:
- [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json)
- [config/cascade/specs/W_phase_001_010_foundation_wave_20260414.queue.json](/workspace/config/cascade/specs/W_phase_001_010_foundation_wave_20260414.queue.json)
- [config/runtime_profiles.json](/workspace/config/runtime_profiles.json)
- [Code&DBs/Workflow/registry/native_runtime_profile_sync.py](/workspace/Code&DBs/Workflow/registry/native_runtime_profile_sync.py)
- [Code&DBs/Workflow/runtime/instance.py](/workspace/Code&DBs/Workflow/runtime/instance.py)
- [Code&DBs/Workflow/runtime/workflow/_admission.py](/workspace/Code&DBs/Workflow/runtime/workflow/_admission.py)
- [Code&DBs/Workflow/runtime/workflow/runtime_setup.py](/workspace/Code&DBs/Workflow/runtime/workflow/runtime_setup.py)
- [Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py)
- [Code&DBs/Workflow/tests/integration/test_bounded_native_primary_proof.py](/workspace/Code&DBs/Workflow/tests/integration/test_bounded_native_primary_proof.py)
- [Code&DBs/Workflow/tests/integration/test_registry_authority_path.py](/workspace/Code&DBs/Workflow/tests/integration/test_registry_authority_path.py)
- Files to modify:
- [Code&DBs/Workflow/runtime/workflow/_admission.py](/workspace/Code&DBs/Workflow/runtime/workflow/_admission.py)
- [Code&DBs/Workflow/runtime/workflow/runtime_setup.py](/workspace/Code&DBs/Workflow/runtime/workflow/runtime_setup.py)
- [Code&DBs/Workflow/tests/integration/test_workspace_boundary_contract.py](/workspace/Code&DBs/Workflow/tests/integration/test_workspace_boundary_contract.py)
- Subsystem boundary:
- native runtime-profile resolution feeding workflow runtime registry builders
- `WorkspaceAuthorityRecord` construction inside workflow admission/setup only
- Out of scope:
- `Code&DBs/Workflow/surfaces/api/**`
- `config/helm_human_layer/seeds/**`
- database migrations
- registry repository contracts
- workspace product UX
- fork/worktree policy
- any cleanup outside the two named builder functions

## 6. Done criteria

- A focused integration test exists for a native runtime profile where resolved `repo_root` and resolved `workdir` are different.
- The test covers both workflow builder seams and would fail against the current `repo_root <- workdir` collapse behavior.
- `_graph_registry_for_request(...)` preserves `config.repo_root` instead of substituting `workdir`.
- `_build_registry(...)` preserves the resolved native `repo_root` instead of assigning `repo_root = workdir`.
- Existing adjacent boundary proofs still pass:
- [Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py)
- [Code&DBs/Workflow/tests/integration/test_bounded_native_primary_proof.py](/workspace/Code&DBs/Workflow/tests/integration/test_bounded_native_primary_proof.py)
- No new schema, config shape, or workspace abstraction is introduced.

## 7. Verification commands

- `export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'`
- `export PYTHONPATH='/Users/nate/Praxis/Code&DBs/Workflow'`
- `cd /Users/nate/Praxis`
- `python -m pytest Code\&DBs/Workflow/tests/integration/test_workspace_boundary_contract.py -q`
- `python -m pytest Code\&DBs/Workflow/tests/integration/test_native_instance_isolation.py -q`
- `python -m pytest Code\&DBs/Workflow/tests/integration/test_bounded_native_primary_proof.py -q`
- `python -m pytest Code\&DBs/Workflow/tests/integration/test_registry_authority_path.py -q`
- `rg -n 'repo_root=workdir|repo_root = workdir' Code\&DBs/Workflow/runtime/workflow/_admission.py Code\&DBs/Workflow/runtime/workflow/runtime_setup.py`

Expected verification outcome:

- the new integration proof demonstrates that workflow builders keep `repo_root` and `workdir` distinct when the runtime profile makes them distinct
- adjacent native-boundary proofs still pass
- the direct collapse assignment is absent from both known builder seams

## 8. Review -> healer -> human approval gate

- Review:
- confirm the packet stays inside Phase 1 workspace-boundary preservation and does not drift into general workspace architecture
- confirm the sprint is one bounded builder-seam sprint, not a roadmap for multi-workspace design
- confirm both runtime builder entrypoints are covered by the new proof
- confirm the proof would have caught the current `repo_root <- workdir` collapse
- Healer:
- if review finds drift or undercoverage, repair only:
- [Code&DBs/Workflow/runtime/workflow/_admission.py](/workspace/Code&DBs/Workflow/runtime/workflow/_admission.py)
- [Code&DBs/Workflow/runtime/workflow/runtime_setup.py](/workspace/Code&DBs/Workflow/runtime/workflow/runtime_setup.py)
- [Code&DBs/Workflow/tests/integration/test_workspace_boundary_contract.py](/workspace/Code&DBs/Workflow/tests/integration/test_workspace_boundary_contract.py)
- rerun all verification commands
- Human approval gate:
- require explicit human approval after review and any healer pass
- do not start Phase 2 `Control Plane Core` before approval is recorded
- do not widen Phase 1 into broader workspace architecture before approval is recorded
