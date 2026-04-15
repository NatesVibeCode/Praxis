# Phase 1 Workspace Boundary Contract

Status: execution_ready

Authority map:
- `planning/phase-program/praxis_0_100_registry.json` phase `1` title = `Workspace Boundary Contract`
- `planning/phase-program/praxis_0_100_registry.json` governance requires `one_phase_one_thing = true`
- `planning/phase-program/praxis_0_100_registry.json` governance requires `mandatory_review_healer_between_phases = true`
- `planning/phase-program/praxis_0_100_registry.json` governance requires `human_approval_between_phases = true`
- `config/cascade/specs/W_phase_001_010_foundation_wave_20260414.queue.json` job `phase_001_workspace_boundary_contract` requires one bounded first sprint with explicit boundaries, files, and verification

Execution environment note:
- repo evidence was gathered from the mounted checkout at `/workspace`
- the platform-context path `/Users/nate/Praxis` is not present in this execution environment

## 1. Objective in repo terms

Preserve the checked-in native workspace contract when workflow runtime builders create `WorkspaceAuthorityRecord`.

In repo terms:
- `config/runtime_profiles.json` declares the native contract inputs
- `registry/native_runtime_profile_sync.py` resolves `repo_root` and `workdir` as separate canonical repo-local paths
- Phase 1 should make the workflow runtime builders preserve that distinction instead of collapsing `repo_root` to `workdir`

This phase is about one boundary only:
- native runtime profile -> workflow runtime builder -> `WorkspaceAuthorityRecord`

This phase is not:
- multi-workspace product design
- worktree or fork ownership policy
- operator UX
- registry schema redesign
- Phase 2 control-plane work

## 2. Current evidence in the repo

- `planning/phase-program/praxis_0_100_registry.json` declares Phase `1` as `Workspace Boundary Contract` and requires closeout through `review`, `healer`, then `human_approval`
- `config/cascade/specs/W_phase_001_010_foundation_wave_20260414.queue.json` says this job must produce one bounded execution packet
- `config/runtime_profiles.json` defines runtime-profile fields `workspace_ref`, `repo_root`, `workdir`, `receipts_dir`, and `topology_dir`
- the checked-in default profile currently sets `"repo_root": "."` and `"workdir": "."`, which hides any collapse bug in default-path testing
- `Code&DBs/Workflow/registry/native_runtime_profile_sync.py`
- `NativeRuntimeProfileConfig.workspace_record()` emits `WorkspaceAuthorityRecord(repo_root=self.repo_root, workdir=self.workdir)`
- `_resolve_repo_path(...)` canonicalizes profile paths relative to the repo root
- `Code&DBs/Workflow/runtime/instance.py`
- resolves the canonical repo-local config at `config/runtime_profiles.json`
- enforces that `repo_root` is the repo root owning the config
- enforces that `workdir`, `receipts_dir`, and `topology_dir` stay inside that repo
- `Code&DBs/Workflow/runtime/workflow/_admission.py`
- `_graph_registry_for_request(...)` currently builds `WorkspaceAuthorityRecord(..., repo_root=workdir, workdir=workdir)`
- `Code&DBs/Workflow/runtime/workflow/runtime_setup.py`
- `_build_registry(...)` currently sets `repo_root = workdir`
- `Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py` already proves native-instance resolution accepts a nested `workdir` and rejects contract drift
- `Code&DBs/Workflow/tests/integration/test_bounded_native_primary_proof.py` already proves wrapper and CLI surfaces expose the repo-local native contract
- `Code&DBs/Workflow/tests/integration/test_registry_authority_path.py` already proves intake preserves distinct `repo_root` and `workdir` once a correct `WorkspaceAuthorityRecord` already exists

## 3. Gap or ambiguity still remaining

Missing proof:
- there is no focused integration test proving the workflow runtime builders preserve a native profile where resolved `repo_root != workdir`

Known defect:
- both builder seams currently collapse `repo_root` to `workdir`
- the bug survives because the checked-in default profile uses the same path for both fields

Main ambiguity to prevent:
- the term `workspace` appears across many unrelated runtime, operator, and product surfaces
- Phase 1 can easily drift into broad architecture cleanup unless the sprint stays pinned to the two workflow builder seams above

Not justified by current evidence:
- editing `config/helm_human_layer/seeds/**`
- adding database tables or migrations
- changing registry repository shape
- adding new workspace abstractions
- expanding into hosted/operator request-context semantics

## 4. One bounded first sprint only

Sprint label:
- preserve distinct `repo_root` and `workdir` through workflow runtime registry builders

Sprint outcome:
- one new focused integration proof covers a runtime profile with nested `workdir`
- the two builder seams stop deriving `repo_root` from `workdir`

Sprint tasks:
1. Add `Code&DBs/Workflow/tests/integration/test_workspace_boundary_contract.py`
2. Build a repo-local temporary runtime profile config where:
- `repo_root = "."`
- `workdir = "artifacts"` or another nested repo-local path
3. Exercise `_graph_registry_for_request(...)` from `runtime/workflow/_admission.py`
4. Exercise `_build_registry(...)` from `runtime/workflow/runtime_setup.py`
5. Assert each resulting `WorkspaceAuthorityRecord` preserves:
- `workspace_ref`
- canonical resolved repo root
- canonical resolved nested workdir
6. Fix only the two builder seams needed to make the proof pass

Stop boundary:
- stop once both builder seams preserve the distinct native profile paths and the existing native-boundary proofs still pass

Explicitly not in this sprint:
- changing the checked-in default profile values in `config/runtime_profiles.json`
- wrapper or CLI redesign
- schema or migration work
- registry write-path redesign
- control-plane refactors

## 5. Exact file or subsystem scope

Read scope:
- `planning/phase-program/praxis_0_100_registry.json`
- `config/cascade/specs/W_phase_001_010_foundation_wave_20260414.queue.json`
- `config/runtime_profiles.json`
- `Code&DBs/Workflow/registry/native_runtime_profile_sync.py`
- `Code&DBs/Workflow/runtime/instance.py`
- `Code&DBs/Workflow/runtime/native_authority.py`
- `Code&DBs/Workflow/runtime/workflow/_admission.py`
- `Code&DBs/Workflow/runtime/workflow/runtime_setup.py`
- `Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py`
- `Code&DBs/Workflow/tests/integration/test_bounded_native_primary_proof.py`
- `Code&DBs/Workflow/tests/integration/test_registry_authority_path.py`

Write scope:
- `Code&DBs/Workflow/runtime/workflow/_admission.py`
- `Code&DBs/Workflow/runtime/workflow/runtime_setup.py`
- `Code&DBs/Workflow/tests/integration/test_workspace_boundary_contract.py`

Subsystem boundary:
- native runtime-profile resolution feeding workflow runtime registry builders
- `WorkspaceAuthorityRecord` construction inside workflow admission/setup only

Out of scope:
- `Code&DBs/Workflow/surfaces/api/**`
- `config/helm_human_layer/seeds/**`
- database migrations
- registry repository contracts
- workspace product UX
- fork/worktree policy
- any cleanup outside the two named builder functions

## 6. Done criteria

- a focused integration test exists for a native runtime profile whose resolved `repo_root` and resolved `workdir` are different
- that test fails against the pre-fix collapse behavior
- `_graph_registry_for_request(...)` preserves `config.repo_root` instead of substituting `workdir`
- `_build_registry(...)` preserves the resolved native `repo_root` instead of assigning `repo_root = workdir`
- `test_native_instance_isolation.py` still passes
- `test_bounded_native_primary_proof.py` still passes
- no schema change, config-shape change, or new workspace abstraction is introduced

## 7. Verification commands

Run from the mounted repo root:

```bash
cd /workspace
export PYTHONPATH='/workspace/Code&DBs/Workflow'
python -m pytest '/workspace/Code&DBs/Workflow/tests/integration/test_workspace_boundary_contract.py' -q
python -m pytest '/workspace/Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py' -q
python -m pytest '/workspace/Code&DBs/Workflow/tests/integration/test_bounded_native_primary_proof.py' -q
rg -n 'repo_root=workdir|repo_root = workdir' '/workspace/Code&DBs/Workflow/runtime/workflow/_admission.py' '/workspace/Code&DBs/Workflow/runtime/workflow/runtime_setup.py'
```

Expected verification result:
- the new test proves the runtime builders keep `repo_root` and `workdir` distinct when the profile makes them distinct
- the existing native-boundary proofs continue to pass
- the exact collapse assignment is absent from both known seams

## 8. Review -> healer -> human approval gate

Review:
- confirm the packet stays inside Phase 1 workspace-boundary preservation
- confirm the sprint is one seam-focused sprint, not a workspace-architecture roadmap
- confirm both runtime builder entrypoints are covered
- confirm the new test would have caught the old `repo_root <- workdir` collapse

Healer:
- if review finds drift or undercoverage, repair only:
- `Code&DBs/Workflow/runtime/workflow/_admission.py`
- `Code&DBs/Workflow/runtime/workflow/runtime_setup.py`
- `Code&DBs/Workflow/tests/integration/test_workspace_boundary_contract.py`
- rerun all verification commands

Human approval gate:
- require explicit human approval after review and any healer pass
- do not start Phase 2 `Control Plane Core` before approval is recorded
- do not widen Phase 1 into broader workspace architecture before approval is recorded
