# Phase 1 Workspace Boundary Contract

Status: execution_ready

Registry authority: `planning/phase-program/praxis_0_100_registry.json` phase `1` (`Workspace Boundary Contract`)

## 1. Objective in repo terms

Make the checked-in native runtime profile the single executable authority for repo-local workspace boundary fields in the workflow runtime path:

- `workspace_ref`
- `repo_root`
- `workdir`
- `receipts_dir`
- `topology_dir`

For this sprint, the concrete repo objective is narrower: stop the runtime from collapsing `repo_root` into `workdir` when it materializes in-memory `WorkspaceAuthorityRecord` values from the native runtime profile.

## 2. Current evidence in the repo

- `planning/phase-program/praxis_0_100_registry.json` declares Phase `1` as `Workspace Boundary Contract` and requires the closeout gate `review -> healer -> human_approval`.
- `config/runtime_profiles.json` is the checked-in authority source for the native `praxis` profile and already declares all five boundary inputs:
- `workspace_ref`
- `repo_root`
- `workdir`
- `receipts_dir`
- `topology_dir`
- `Code&DBs/Workflow/registry/native_runtime_profile_sync.py` resolves `repo_root` and `workdir` independently against the repo root and exposes `WorkspaceAuthorityRecord(repo_root=..., workdir=...)`.
- `Code&DBs/Workflow/runtime/instance.py` already enforces the hard repo boundary contract:
- config must be `config/runtime_profiles.json`
- `repo_root` must resolve back to the owning repo
- `workdir`, `receipts_dir`, and `topology_dir` must stay inside that repo
- env vars may assert the contract but may not override it
- `Code&DBs/Workflow/runtime/native_authority.py` fails closed if the checked-in default `workspace_ref` or runtime profile ref is empty.
- `Code&DBs/Workflow/registry/repository.py` and `Code&DBs/Workflow/tests/integration/test_registry_authority_path.py` already prove that once a correct `WorkspaceAuthorityRecord` exists, admission and bundle payloads preserve distinct `repo_root` and `workdir` values.
- The live defect is upstream of that durable registry path:
- `Code&DBs/Workflow/runtime/workflow/_admission.py` in `_graph_registry_for_request(...)` builds `WorkspaceAuthorityRecord(repo_root=workdir, workdir=workdir)`.
- `Code&DBs/Workflow/runtime/workflow/runtime_setup.py` in `_build_registry(...)` does the same `repo_root = workdir` collapse before creating `WorkspaceAuthorityRecord(...)`.
- Existing native proofs are partial, not sufficient:
- `Code&DBs/Workflow/tests/integration/test_native_instance_isolation.py` proves fail-closed config/env behavior and already uses a nested `workdir="artifacts"` fixture shape.
- `Code&DBs/Workflow/tests/integration/test_bounded_native_primary_proof.py` proves wrapper and CLI surfaces expose the checked-in native contract.
- `Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py` proves repo-local native instance values flow into an end-to-end path, but today it uses `native_instance.repo_root` and `native_instance.workdir` from the same checked-in `"."` profile, so it does not detect collapse.

## 3. Gap or ambiguity still remaining

- The checked-in `praxis` profile currently sets both `repo_root` and `workdir` to `"."`, so the default repo state does not prove those are preserved as distinct contract fields.
- Two live runtime builders currently erase the distinction even though the authority model supports it:
- `runtime/workflow/_admission.py::_graph_registry_for_request`
- `runtime/workflow/runtime_setup.py::_build_registry`
- There is no focused contract test that starts from a native runtime profile where `repo_root != workdir` and proves those exact values survive the in-memory registry materialization seam.
- Phase 1 still must not widen into workspace product semantics, fork/worktree ownership policy, operator selectors, UI seeds, or multi-tenant workspace design.

## 4. One bounded first sprint only

Implement one focused workspace-boundary contract sprint around the native profile to in-memory registry seam.

Sprint contents:

- Add one focused integration test that uses a repo-local runtime profile with:
- `repo_root = "."`
- `workdir = "artifacts"` or another nested existing directory under the repo root
- Exercise the native-derived registry builders that currently materialize `WorkspaceAuthorityRecord` values.
- Make the minimal code fix so both builders preserve:
- canonical `repo_root`
- canonical nested `workdir`
- Stop there.

Do not change:

- database schema
- registry table shape
- workspace naming or ownership policy
- operator/UI workspace behavior
- broader runtime path cleanup outside these two builders

## 5. Exact file or subsystem scope

Primary implementation scope:

- `Code&DBs/Workflow/runtime/workflow/_admission.py`
- `Code&DBs/Workflow/runtime/workflow/runtime_setup.py`

Primary test scope:

- add `Code&DBs/Workflow/tests/integration/test_workspace_boundary_contract.py`

Read-only authority references:

- `config/runtime_profiles.json`
- `Code&DBs/Workflow/registry/native_runtime_profile_sync.py`
- `Code&DBs/Workflow/runtime/instance.py`
- `Code&DBs/Workflow/runtime/native_authority.py`
- `Code&DBs/Workflow/registry/repository.py`
- `Code&DBs/Workflow/tests/integration/test_registry_authority_path.py`

Explicitly out of scope:

- `Code&DBs/Workflow/surfaces/api/`
- fork ownership and worktree binding flows
- `config/helm_human_layer/seeds/`
- database migrations
- new workspace tables or new workspace abstraction
- any Phase 2 `Control Plane Core` expansion

## 6. Done criteria

- A focused automated proof exists for a native runtime profile where `repo_root` and `workdir` are different repo-local paths.
- That proof fails against the old `repo_root = workdir` behavior.
- `_graph_registry_for_request(...)` preserves distinct `repo_root` and `workdir` values from the native runtime profile.
- `runtime_setup._build_registry(...)` preserves the same distinction.
- Existing fail-closed native boundary tests still pass.
- No schema changes, no new workspace model, and no scope expansion beyond these registry-builder seams.

## 7. Verification commands

Run from repo root:

```bash
PYTHONPATH=Code\&DBs/Workflow pytest Code\&DBs/Workflow/tests/integration/test_workspace_boundary_contract.py -q
PYTHONPATH=Code\&DBs/Workflow pytest Code\&DBs/Workflow/tests/integration/test_native_instance_isolation.py -q
PYTHONPATH=Code\&DBs/Workflow pytest Code\&DBs/Workflow/tests/integration/test_bounded_native_primary_proof.py -q
rg -n "repo_root=workdir|repo_root = workdir" Code\&DBs/Workflow/runtime/workflow/_admission.py Code\&DBs/Workflow/runtime/workflow/runtime_setup.py
```

Expected post-sprint intent:

- the new contract test passes
- the two runtime builders no longer collapse `repo_root` into `workdir`
- existing native fail-closed proofs still pass

## 8. Review -> healer -> human approval gate

Review:

- confirm the sprint only repairs native profile to in-memory registry boundary preservation
- confirm both builder paths preserve distinct `repo_root` and `workdir`
- confirm no workspace-product or operator-policy scope was added

Healer:

- if review finds ambiguity or regression, repair only:
- the two builder files
- the new contract test
- rerun the verification commands

Human approval gate:

- require explicit human approval after review and any healer pass
- do not open Phase 2 work or any broader workspace refactor until that approval is recorded
