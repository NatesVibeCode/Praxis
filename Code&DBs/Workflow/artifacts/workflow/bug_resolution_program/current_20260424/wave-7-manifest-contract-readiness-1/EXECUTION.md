# Wave 7 Manifest Contract Readiness Execution

## Result
- No runtime, surface, or test code was changed in this packet.
- This packet documents the current authority coverage and the proof-backed terminal status for the four in-scope bugs.
- The correct outcome for this packet is `DEFERRED` for all four bugs, because the plan explicitly constrained this job to proof gathering and documentation, not behavior edits.

## Changed Files
- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-7-manifest-contract-readiness-1/EXECUTION.md`

## Evidence Collected
- `praxis_orient` confirmed the active authority envelope, including degraded-mode handling, startup wiring status, and the workflow database authority fingerprint.
- `praxis_context_shard` confirmed the execution shard and the packet's limited write scope.
- `praxis_discover` was attempted but refused with shard-enforcement errors, so source inspection was used instead.
- Local source inspection covered the packet plan and the runtime/surface/test files named in the plan.

## Per-Bug Outcome

### BUG-62F78235
- Intended terminal status: `DEFERRED`
- Evidence:
  - `Code&DBs/Workflow/runtime/workflow/pipeline_eval.py:266-282` already computes `quarantine_candidates` and recommends `quarantine_or_recompile_before_retry` when errors are present.
  - `Code&DBs/Workflow/runtime/workflow/_status.py:503-546` still aggregates runtime health from job failures and retryability, so the remaining work is not here in the packet artifact.
- Rationale:
  - The quarantine signal exists, but this packet did not change the retry/quarantine authority path that consumes it.

### BUG-8F6A612A
- Intended terminal status: `DEFERRED`
- Evidence:
  - `Code&DBs/Workflow/runtime/verifier_authority.py:281-292` is DB-backed and returns verifier/healer registry snapshots from Postgres authority.
  - `Code&DBs/Workflow/runtime/verification.py:113-180` resolves `verify_refs` through `verification_registry` rows and expands catalog-backed verification commands.
  - `Code&DBs/Workflow/surfaces/cli/commands/workflow.py:1203-1274` exposes `workflow verify-platform` through the registry authority path.
- Rationale:
  - The execution authority is catalog-backed, but this packet did not implement a new scope-to-verifier chooser or prove the scope-matching gap closed end to end.

### BUG-123C17AC
- Intended terminal status: `DEFERRED`
- Evidence:
  - `Code&DBs/Workflow/surfaces/_subsystems_base.py:172-205` returns `{"booted": True}` even when registry sync steps were skipped, while also exposing `registry_sync` and `heartbeat_started`.
  - `Code&DBs/Workflow/surfaces/_subsystems_base.py:126-170` tracks `startup_wiring_done`, registry sync successes/skips/failures, and the auto-wiring gate.
  - `Code&DBs/Workflow/tests/unit/test_api_rest_startup.py:47-75` currently asserts the fake boot path via `booted: True`, which is consistent with the current masking concern.
- Rationale:
  - The runtime already records skipped wiring, but this packet did not change the startup truth contract or the consumer assertions that still focus on `booted: True`.

### BUG-9D09F47D
- Intended terminal status: `DEFERRED`
- Evidence:
  - `Code&DBs/Workflow/surfaces/api/rest.py:477-483` explicitly treats capability-mount failures as degraded startup.
  - `Code&DBs/Workflow/surfaces/api/rest.py:620-630` records `capability_mount_errors` and `capabilities_mount_degraded` when invalid capability bindings exist.
  - `Code&DBs/Workflow/surfaces/api/handlers/workflow_launcher.py:27-35` advertises workflow operational readiness flags, but does not itself project mount-degradation state.
- Rationale:
  - Partial capability-mount degradation is already tracked in the API surface, but this packet did not change the outward discovery contract so a partial API cannot yet be proven canonical from the launcher/discovery side.

## Notes
- No bug-state mutation was performed.
- No verifier or runtime tests were changed or added in this packet.
- The follow-on packet should implement the actual contract hardening or prove a tighter terminal status if the later evidence supports `WONT_FIX`.
