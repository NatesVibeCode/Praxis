# Wave 0 Packet Plan: DB Authority Orphans

Bug in scope:
- `BUG-F8C9F5B5` `[P2/WIRING]` `[hygiene-2026-04-24/db-authority-orphans]` Data dictionary still exposes DB authority tables with no production runtime owner

## Authority Model

Decision order for this packet:
1. `Praxis.db` standing orders and workflow governance rules.
2. The bug record for `BUG-F8C9F5B5`.
3. The data dictionary / registry surfaces that expose DB authority tables.
4. Read-only verification evidence from the current workflow shard.

Operating rule:
- Treat this as an evidence-only planning packet.
- Do not infer runtime ownership from stale catalog exposure alone; verify against the production runtime owner source before declaring impact.
- Do not change code, schema, seed data, or ownership records in this job.

## Files To Read

Read only the minimum set needed to prove the issue and define the next move:
- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-0-bug-evidence-authority-data-registry-1/PLAN.md`
- The authoritative data dictionary surface that lists DB authority tables and owner metadata.
- The production runtime owner registry or manifest used to determine whether an owner exists.
- The bug packet / bug history for `BUG-F8C9F5B5`.
- Any workflow receipts or prior evidence that already cite the same authority-table exposure.

If a file does not exist in the current workspace snapshot, do not fabricate a path; locate the authoritative source from the workflow registry before proceeding.

## Files Allowed To Change

Allowed:
- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-0-bug-evidence-authority-data-registry-1/PLAN.md`

Not allowed in this job:
- Any code file.
- Any DB migration, seed, fixture, or runtime ownership record.
- Any bug status mutation.

## Verification Path

1. Read the data dictionary entry for the DB authority tables implicated by the bug.
2. Confirm which tables are exposed without a production runtime owner.
3. Cross-check the production runtime owner registry or manifest to verify the absence is real, not a stale catalog projection.
4. Capture evidence citations in the packet notes so the next wave can act without re-deriving the same facts.
5. Stop after the evidence boundary is reached; do not begin remediation in this packet.

## Stop Boundary

Stop when both conditions are met:
- The packet has a verified list of DB authority tables still exposed by the data dictionary.
- Each listed table has been checked against the production runtime owner source and confirmed to have no production runtime owner, or the evidence shows a mismatch that makes the bug non-actionable from this packet.

Do not cross the boundary into:
- fixing the data dictionary,
- assigning ownership,
- changing runtime metadata,
- or closing the bug.

## Intended Outcome

`BUG-F8C9F5B5`
- Produce a clean evidence packet that proves whether the data dictionary is exposing DB authority tables without a production runtime owner.
- If the exposure is confirmed, leave a precise remediation target for the next wave.
- If the exposure is not confirmed, record the mismatch and stop without changing anything.

