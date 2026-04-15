# Phase 75 Current Packet

Status: execution_ready

Registry authority:
- `planning/phase-program/praxis_0_100_registry.json` declares phase `75` as `Replay and Rehydrate` in arc `70-79 make execution deterministic and replayable`, status `queued`, predecessor phase `74`, and required closeout `review -> healer -> human_approval`.
- This packet prepares one bounded first sprint candidate for Phase 75. It does not change registry focus, does not mark Phase 75 approved, and does not widen into a full snapshot/cache rollout.

## 1. Current state summary
- `Code&DBs/Workflow/runtime/sandbox_runtime.py` still materializes execution input by copying the full workspace into the sandbox root and discovering mutations with before/after manifests. The local lane uses `_hydrate_copy(...)`, `_workspace_manifest(...)`, and `_dehydrate_copy(...)`; the remote lane uploads a base64 tarball of the full workspace and mirrors changed files back after execution.
- `Code&DBs/Workflow/surfaces/cloudflare_sandbox_bridge/README.md` explicitly says the bridge supports only `workspace_materialization="copy"`, warns that the current full-workspace upload may hit Worker request-size limits, and does not mirror deletions back to the host.
- `Code&DBs/Workflow/runtime/repo_snapshot_store.py` and the canonical `repo_snapshots` table already exist, but they currently serve receipt and proof provenance through `record_repo_snapshot(...)`; they are not the authoritative hydration identity used by sandbox execution.
- `Code&DBs/Workflow/runtime/workflow/submission_capture.py` and `Code&DBs/Workflow/runtime/workflow/submission_diff.py` already compute workspace manifests and artifact refs for submission evidence, but those manifests are post-hoc comparison surfaces, not reusable execution inputs.
- The runtime therefore has proof of repo provenance and proof of write outputs, but it still lacks one canonical content-derived identity for the exact workspace bytes that were hydrated into a sandbox session.

## 2. One bounded next task inside Phase 75
- Add one canonical `workspace_snapshot_ref` seam at sandbox hydration time.
- The first sprint computes a stable content-addressed fingerprint from the hydrated workspace input itself:
- normalize sandbox input paths using the same ignored-directory rules already used by `sandbox_runtime.py`
- hash file content, not only file size or mtime
- derive one stable `workspace_snapshot_ref` from the ordered path-to-content-hash mapping
- thread that ref through `WorkspaceSnapshot`, `HydrationReceipt`, `SandboxExecutionResult`, and receipt `workspace_provenance`
- prove identical workspace bytes produce the same `workspace_snapshot_ref`, and one content change produces a different ref
- Stop at authoritative identity. Do not yet implement delta hydration, cache lookup, remote reuse, bridge-side fetch-by-ref, or new persistence tables.

## 3. Exact file scope
- Primary implementation scope:
- `Code&DBs/Workflow/runtime/sandbox_runtime.py`
- `Code&DBs/Workflow/runtime/receipt_provenance.py`
- `Code&DBs/Workflow/runtime/workflow/receipt_writer.py`
- Primary proof scope:
- `Code&DBs/Workflow/tests/unit/test_sandbox_runtime.py`
- `Code&DBs/Workflow/tests/unit/test_receipt_store_payloads.py`
- `Code&DBs/Workflow/tests/unit/test_unified_workflow.py`
- Optional helper-only addition if needed to keep hashing logic single-owned:
- `Code&DBs/Workflow/runtime/workspace_snapshot.py`
- Explicitly out of scope:
- `Code&DBs/Workflow/runtime/repo_snapshot_store.py`
- `Code&DBs/Workflow/storage/**` migrations or schema changes
- `Code&DBs/Workflow/surfaces/cloudflare_sandbox_bridge/**` behavior changes beyond carrying the new ref in request/response payloads
- submission diff redesign
- artifact-store redesign
- content-addressed cache reuse

## 4. Done criteria
- Sandbox hydration computes one stable `workspace_snapshot_ref` from file-content hashes over the hydrated workspace input.
- `workspace_snapshot_ref` is deterministic for identical workspace content across repeated executions.
- A single content change inside the hydrated workspace changes `workspace_snapshot_ref`.
- `workspace_provenance` in receipts includes `workspace_snapshot_ref` alongside existing workspace-root/runtime-profile fields.
- Local and remote sandbox lanes can both carry the ref without claiming support for anything beyond `workspace_materialization="copy"`.
- Existing receipt/write-manifest proofs still pass after the new field is added.
- No migration file is added or modified.
- No cache/reuse semantics are introduced in this sprint.

## 5. Verification commands
- `cd /Users/nate/Praxis`
- `export PYTHONPATH='Code&DBs/Workflow'`
- `python3 -m pytest 'Code&DBs/Workflow/tests/unit/test_sandbox_runtime.py' 'Code&DBs/Workflow/tests/unit/test_receipt_store_payloads.py' 'Code&DBs/Workflow/tests/unit/test_unified_workflow.py' -q`
- `rg -n 'workspace_snapshot_ref|build_workspace_provenance|WorkspaceSnapshot|HydrationReceipt|SandboxExecutionResult' 'Code&DBs/Workflow/runtime' 'Code&DBs/Workflow/tests/unit'`

## 6. Stop boundary
- Stop after workspace hydration has one canonical content-derived identity and that identity is visible in runtime receipts.
- Do not implement snapshot fetch-by-ref, tarball deduplication, delta upload/download, or remote cache lookup in this packet.
- Do not repurpose `repo_snapshot_ref` as a fake substitute for content-derived workspace identity.
- Do not widen into Phase 74 provenance/evidence cleanup or Phase 79 recovery logic.

## 7. Gate
- Review:
- confirm the sprint creates authoritative workspace-input identity rather than another size/mtime manifest
- confirm the new proof covers both determinism and change detection
- confirm the packet stayed out of cache reuse and schema work
- Healer:
- if review finds fake determinism, duplicate hashing logic, or receipt drift, repair only the scoped files above
- rerun the full verification command set
- Human approval:
- require explicit human approval before treating this packet as an active Phase 75 sprint or opening a second Phase 75 packet for cache reuse/delta rehydrate
