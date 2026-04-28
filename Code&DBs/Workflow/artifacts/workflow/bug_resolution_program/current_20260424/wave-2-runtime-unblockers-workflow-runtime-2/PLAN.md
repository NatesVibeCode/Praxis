# Wave 2 Runtime Unblockers Plan

## Scope
- Packet: `wave-2-runtime-unblockers-workflow-runtime-2`
- Bugs in scope:
  - `BUG-9CA0CB78` - repeated receipt failure: `sandbox_error`
  - `BUG-A3B51E8D` - repeated receipt failure: `workflow.timeout`

## Authority Model
- Primary authority is the live Praxis workflow session for this job:
  - execution shard / bundle
  - current workflow context snapshot
  - live bug tracker records for the two scoped bug ids
  - live receipt and status history when accessible through the workflow authority
- Treat the execution bundle as the write-boundary authority.
- Treat stale projections, cached summaries, or prior packet conclusions as advisory only.
- For this planning job, do not modify code, database state, bug state, or receipt state.
- Read-side health already shows the platform is healthy, but the read-side circuit breaker is open for some stale projections; avoid using stale projection content as evidence for decisions.

## Files To Read
- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-2-runtime-unblockers-workflow-runtime-2/PLAN.md`
- `Code&DBs/Workflow/requirements.runtime.txt`
- Any packet-local artifacts in `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-2-runtime-unblockers-workflow-runtime-2/` that are already present when execution starts

## Files Allowed To Change
- For this job: only `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-2-runtime-unblockers-workflow-runtime-2/PLAN.md`
- No code files, no database migrations, no workflow runtime implementation files, and no bug-tracker records are to be changed in this planning pass.

## Verification Path
- Verify the plan file is written at the exact packet path.
- Verify the plan includes:
  - authority model
  - files to read
  - files allowed to change
  - verification path
  - stop boundary
  - per-bug intended outcome
- Verify the plan stays within the current workflow write scope and does not describe or authorize code edits for this job.
- Seal the result through Praxis submission after the document is in place.

## Stop Boundary
- Stop after the plan document is written and sealed.
- Do not edit runtime code, tests, database records, or bug metadata in this job.
- Do not attempt to resolve `BUG-9CA0CB78` or `BUG-A3B51E8D` here; that belongs to the follow-on execution packet.
- Do not broaden scope to other bugs, providers, or workflow packets.

## Intended Outcome Per Bug
- `BUG-9CA0CB78`
  - Intended outcome for the follow-on execution packet: isolate the source of the repeated `sandbox_error` receipt failures, produce an evidence-backed remediation plan, and unblock receipt generation without expanding scope.
- `BUG-A3B51E8D`
  - Intended outcome for the follow-on execution packet: isolate the source of the repeated `workflow.timeout` receipt failures, produce an evidence-backed remediation plan, and reduce or eliminate timeout-triggered receipt failures without expanding scope.
