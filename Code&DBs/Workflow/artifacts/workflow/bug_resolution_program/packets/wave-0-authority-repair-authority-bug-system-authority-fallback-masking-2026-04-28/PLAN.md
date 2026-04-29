# Bugs in scope

- `BUG-293B874A`

# Titles in scope

- Fallback-based compatibility paths mask authority failures across runtime evidence, impact analysis, and MCP transport

# Files to read first

- Source coordination file expected by the packet request: `/workspace/Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/bug_resolution_program_kickoff_20260428_full.json`
- Packet output path for this plan: `/workspace/Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets/wave-0-authority-repair-authority-bug-system-authority-fallback-masking-2026-04-28/PLAN.md`
- Current repo truth in this workspace: the workspace is not hydrated, the source coordination file is absent, and no additional repo files could be verified from `/workspace`

# Files allowed to change

- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets/wave-0-authority-repair-authority-bug-system-authority-fallback-masking-2026-04-28/PLAN.md`

# Verification or closure proof required

- Packet contract requires clean results for the affected path across:
- `workflow orient`
- `bug stats`
- `bug list`
- `bug search`
- replay-ready view
- Authority context required by contract:
- authority owner: `lane:authority_bug_system`
- lane: `Authority / bug system (authority_bug_system)`
- wave: `wave_0_authority_repair`
- packet kind: `authority_repair`
- cluster: `authority-fallback-masking-2026-04-28`
- depends on wave: `none`
- Current workspace limitation: closure proof cannot be produced from repo evidence here because the referenced coordination file and surrounding repo implementation files are not present in `/workspace`

# Stop boundary

- Do not edit code in this job
- Do not change any file outside the single allowed plan path above
- Stop if repo-backed packet extraction, affected-path discovery, or verification-surface tracing would require files that are not present in the current workspace
