# Bugs in scope

- `BUG-7378056B`
- `BUG-415FC105`
- Packet: `bug_resolution_program_20260428_full.wave-0-authority-repair-authority-bug-system-dataset-scan-split`
- Authority owner: `lane:authority_bug_system`
- Lane: `Authority / bug system (authority_bug_system)`
- Wave: `wave_0_authority_repair`
- Packet kind: `authority_repair`
- Cluster: `dataset-scan-split`
- Depends on wave: `none`

# Titles in scope

- Dataset candidate subscriber outer loops are unproven while tests only cover pure helpers
- Dataset candidates scan multiplexes cursor ingestion and direct receipts backfill under one action

# Files to read first

- Coordination source named by the packet contract: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/bug_resolution_program_kickoff_20260428_full.json`
- This packet plan output: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets/wave-0-authority-repair-authority-bug-system-dataset-scan-split/PLAN.md`
- First read the live implementation, tests, and workflow surfaces that back:
  - dataset candidate subscriber outer loops
  - dataset candidate scan action routing
  - cursor ingestion
  - direct receipts backfill
  - bug stats/list/search surfaces
  - replay-ready view
- Limitation: the hydrated workspace at `/workspace` did not contain the repo snapshot or the named coordination JSON during this planning job, so no repo file inventory could be proven from local disk.

# Files allowed to change

- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets/wave-0-authority-repair-authority-bug-system-dataset-scan-split/PLAN.md`

# Verification or closure proof required

- Verification surface must return cleanly for the affected path across all of:
  - workflow orient
  - bug stats
  - bug list
  - bug search
  - replay-ready view
- Closure proof must show the affected dataset-scan-split path is clean on those surfaces after the repair.
- For `BUG-7378056B`, prove subscriber outer-loop coverage at the integration or end-to-end level rather than only via pure helper tests.
- For `BUG-415FC105`, prove the scan path cleanly separates cursor ingestion from direct receipts backfill, or otherwise show the multiplexed action no longer leaks ambiguity into the verification surfaces above.

# Stop boundary

- Do not edit code in this job.
- Do not change any file outside this `PLAN.md`.
- Do not treat guessed file paths as authoritative; re-hydrate or locate the live repo snapshot before execution work.
- If the coordination JSON or repo snapshot remains unavailable in the execution workspace, stop and resolve workspace hydration before attempting the downstream `execute_packet` job.
