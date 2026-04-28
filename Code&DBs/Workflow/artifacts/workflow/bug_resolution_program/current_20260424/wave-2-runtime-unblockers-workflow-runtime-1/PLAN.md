# Wave 2 Runtime Unblockers: Workflow Runtime 1 Plan

## Authority Model

- `Praxis.db` bug records and receipt history are the authoritative source for bug scope, failure labels, and repeatability.
- The execution shard for this job is the authoritative source for run identity, write scope, and tool permissions.
- The hydrated workspace under `/workspace` is the authoritative source for code and artifact discovery during downstream implementation.
- This job is plan-only. No source code changes are allowed here.
- If a path or file is missing from the workspace, treat that as a discovery result, not as permission to assume its contents.

## Files To Read

Read only what is needed to ground the downstream fix plan:

- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-2-runtime-unblockers-workflow-runtime-1/**`
- `Code&DBs/Workflow/**/workflow/**`
- `Code&DBs/Workflow/**/sandbox/**`
- `Code&DBs/Workflow/**/cli/**`
- `Code&DBs/Workflow/**/bug/**`
- `Code&DBs/Workflow/requirements.runtime.txt`
- Any packet-local notes, receipts, or manifests under `scratch/workflow_900a51ac7f3f/**`

Priority for reading:

1. Packet-local artifact context and any prior receipts for this workflow.
2. Runtime CLI entrypoints related to discover, bug search, and submission.
3. Docker/sandbox code that governs `docker_packet_only`.
4. Submission and receipt plumbing that can emit `workflow_submission.required_missing`.
5. Shared runtime utilities that may create file descriptor pressure or silent hangs.

## Files Allowed To Change

- None in this job.
- This plan file is the only artifact to create or update.
- All source files remain read-only until the implementation packet.

## Verification Path

Downstream verification should be targeted to the observed failure modes:

- Reproduce the `docker_packet_only` sandbox path for BUG-AA7CA63D and confirm the fix removes the `too many open files` failure.
- Exercise the workflow submission path for BUG-1980557E and confirm all required fields are present before submission.
- Replay the repeated `sandbox_error` receipts for BUG-39D02693 and BUG-7C5D8AE4 until the underlying sandbox failure is eliminated, not masked.
- Run the workflow CLI discover and bug search path for BUG-8D8C5256 with a bounded timeout and visible progress or fail-fast behavior.
- Confirm the affected bugs have updated receipts or bug history that show the failure mode no longer reproduces.

## Stop Boundary

Stop after the plan is written if any of the following are true:

- A required file path cannot be grounded in the workspace or packet artifacts.
- The downstream fix would require changes outside the runtime/workflow surface.
- The evidence is insufficient to distinguish a single root cause from multiple independent failures.
- Any step would require editing source code in this plan-only job.

## Per-Bug Intended Outcome

- `BUG-AA7CA63D`:
  - Remove the file-descriptor leak or descriptor explosion in the `docker_packet_only` sandbox path.
  - Expected end state: the workflow packet starts and runs without `too many open files`.

- `BUG-1980557E`:
  - Fix the submission payload construction so required submission fields are always populated.
  - Expected end state: submission no longer fails with `workflow_submission.required_missing`.

- `BUG-39D02693`:
  - Eliminate the sandbox failure that is being surfaced as repeated `sandbox_error` receipts.
  - Expected end state: repeated receipts resolve to a stable success or a concrete, actionable failure.

- `BUG-7C5D8AE4`:
  - Address the same `sandbox_error` class if it shares the same root cause, but keep the bug tracked separately until evidence proves it is identical to BUG-39D02693.
  - Expected end state: the repeated receipt pattern stops and the runtime path becomes deterministic.

- `BUG-8D8C5256`:
  - Make workflow CLI discover and bug search fail fast or report progress instead of hanging silently.
  - Expected end state: discover/search returns within a bounded time or emits an explicit timeout/error with enough context to debug.

## Notes

- This packet is intentionally narrow: it establishes the authority boundary, the minimal read set, and the downstream verification shape.
- No code change should be attempted until the runtime packet has a concrete implementation target and a bounded verification path.
