# Bugs in scope

- `BUG-2CF335E3`
- `BUG-25224975`
- Packet: `bug_resolution_program_20260428_full.wave-0-authority-repair-authority-bug-system-hygiene-2026-04-22-secret-authority`
- Owner lane: `authority_bug_system`
- Wave: `wave_0_authority_repair`
- Packet kind: `authority_repair`
- Cluster: `hygiene-2026-04-22-secret-authority`
- Depends on wave: `none`

# Titles in scope

- [hygiene-2026-04-22/secret-authority] Sandbox env assembly copies host env and dotenv before secret allowlist
- [hygiene-2026-04-22/secret-authority] OAuth refresh reads client credentials directly from process env

# Files to read first

- `/workspace/Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/bug_resolution_program_kickoff_20260428_full.json`
- The live workspace truth at execution time is that `/workspace` contains only the packet artifact tree under `Code&DBs`; no checked-out implementation repo files or kickoff JSON are present, so no additional repo-backed file list can be verified from this environment.
- Once the workspace is hydrated with the repo, read the current authority-bug-system workflow files that implement:
  - sandbox environment assembly for workflow execution
  - secret allowlist filtering/order of operations
  - OAuth refresh/client credential sourcing
  - workflow orient, bug stats/list/search, and replay-ready surfaces for the affected path

# Files allowed to change

- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-22-secret-authority/PLAN.md`
- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-22-secret-authority/EXECUTION.md`

# Verification or closure proof required

- Verification surface must return cleanly for the affected path across:
  - workflow orient
  - bug stats/list/search
  - replay-ready view
- Closure proof for execution should demonstrate that:
  - sandbox env assembly no longer copies host env or dotenv values before secret allowlist enforcement
  - OAuth refresh no longer reads client credentials directly from process env for the affected path
- In this planning job, no verification was run because the referenced repo/artifact tree is absent from the live workspace.

# Execution-time truth

- The live command workspace remains unhydrated for this packet: `find /workspace -maxdepth 4 -type f` returned no implementation files, and before this execution record was written the packet directory contained only this `PLAN.md`.
- Because the affected authority path is not present locally, this job cannot truthfully claim `FIXED`; the narrowest correct terminal outcome for both in-scope bugs is `DEFERRED` pending a hydrated repo workspace.
- The required verification ref remains `verify.bug_resolution_packet.wave-0-authority-repair-authority-bug-system-hygiene-2026-04-22-secret-authority.execute_packet`.
- `praxis_query` and `praxis_discover` currently fail shard-enforcement preconditions in this workspace, so this job records proof and rationale instead of forcing speculative code or unverifiable workflow steps.

# Stop boundary

- Do not edit application code in this job.
- Do not widen write scope beyond this packet's `PLAN.md` and `EXECUTION.md`.
- Stop if the repo is not hydrated in the live workspace; resolving implementation files, exact read order, and verification commands requires the missing `Code&DBs/Workflow` tree and kickoff JSON to exist locally.
