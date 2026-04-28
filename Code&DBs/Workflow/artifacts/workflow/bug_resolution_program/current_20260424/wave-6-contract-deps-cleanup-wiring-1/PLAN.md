# Wave 6 Contract/Deps Cleanup Wiring 1 Plan

## Authority Model
- Primary authority is the current workflow shard for `workflow_db478ee39a1d` and the execution bundle embedded in that shard.
- Bug scope authority is `BUG-0FB23DDF [P2/WIRING]: Extract shared client-core library for CLI and API`.
- Repo source of truth is the checked-in workspace state plus the packet-specific artifacts under `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-6-contract-deps-cleanup-wiring-1/`.
- Do not invent missing architecture. If a file, import path, or package boundary is not confirmed by local reading, treat it as unresolved and stop at discovery.
- This job is review/planning only. No source edits, no database writes, and no bug-state mutation belong in this packet.

## Files To Read
- The bug packet for `BUG-0FB23DDF`, including title, description, related notes, and any linked receipts or prior attempts.
- The CLI client implementation files that currently own request construction, auth, config, transport, or retry logic.
- The API client implementation files that currently duplicate the same client responsibilities.
- Shared support code already present in the repo: config loaders, auth helpers, HTTP wrappers, request/response models, and package exports.
- Build and package manifests that determine how a shared client library would be wired into both CLI and API targets.
- Tests covering CLI and API client behavior, plus any package-level tests that would detect import or bundling regressions.

## Files Allowed To Change
- Only this plan file: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-6-contract-deps-cleanup-wiring-1/PLAN.md`
- No source files, manifests, generated artifacts, database records, or workflow metadata may be changed in this job.

## Verification Path
- Planning only in this packet: verify that the scope is narrow, the ownership boundaries are explicit, and the later execution packet has a clear read/change set.
- The later implementation packet should verify the wiring with targeted CLI and API tests for the extracted shared client library.
- The later implementation packet should also run the smallest build/typecheck path that proves both entrypoints consume the shared core without behavior drift.
- If the later packet cannot prove a refactor-safe boundary, it should stop before any broad contract changes and report the missing evidence.

## Stop Boundary
- Stop after writing this plan.
- Do not inspect or change code beyond what is needed to describe the plan.
- Do not attempt the shared-library extraction in this job.
- Do not resolve the bug, attach evidence, or update bug state from this packet.

## Per-Bug Intended Outcome
- `BUG-0FB23DDF`: identify the duplicated CLI/API client code path, define the minimum shared `client-core` extraction boundary, and hand off a plan that lets the execution packet move transport/auth/config/shared-request logic into one library with no user-visible behavior change.
