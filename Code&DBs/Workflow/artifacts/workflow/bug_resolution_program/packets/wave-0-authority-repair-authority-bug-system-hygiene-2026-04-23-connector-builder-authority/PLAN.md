# Bugs in scope

- `BUG-A63D9317`
- `BUG-0AB8A780`

# Titles in scope

- `[hygiene-2026-04-23/connector-builder-authority] Connector registrar auto-imports filesystem clients and writes multiple registry projections`
- `[hygiene-2026-04-23/connector-builder-authority] Connector build authority is split between dead codegen artifacts and manifest registry flow`

# Files to read first

- `/workspace/Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/bug_resolution_program_kickoff_20260428_full.json`
  - Required source for packet extraction, but it is not present in the current hydrated workspace.
- `/workspace/Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-23-connector-builder-authority/PLAN.md`
  - Packet-local planning file for this job.

# Files allowed to change

- `/workspace/Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-23-connector-builder-authority/PLAN.md`
- `/workspace/Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-23-connector-builder-authority/EXECUTION.md`

# Verification or closure proof required

- Verification surface must return cleanly for the affected path across workflow orient, bug stats/list/search, and replay-ready view.
- `depends_on_wave: none`
- Current blocker: the kickoff authority file and surrounding repo tree are absent from the live workspace, so packet extraction and repo-truth validation cannot be completed from local sources in this job.
- Current blocker: the available Praxis authority path does not provide a clean replay surface in this workspace. `praxis query` and `praxis discover` return explicit shard-enforcement errors, while `praxis workflow tools list` and `praxis workflow tools call ...` timed out under an explicit `20s` timeout.
- This execution job should record proof and intended terminal outcomes in `EXECUTION.md`; it should not resolve the bugs.

# Stop boundary

- Do not edit code in this job.
- Do not change any file outside the packet `PLAN.md` and `EXECUTION.md`.
- Stop if the kickoff file remains unavailable in the hydrated workspace; do not infer packet details beyond the explicit contract provided to this job.
