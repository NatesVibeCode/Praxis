# Bugs in scope

- `BUG-B5F3106D`
- Packet: `bug_resolution_program_20260428_full.wave-0-authority-repair-authority-bug-system-control-commands-optionality-2026-04-28`
- Authority owner: `lane:authority_bug_system`
- Lane: `Authority / bug system (authority_bug_system)`
- Wave: `wave_0_authority_repair`
- Packet kind: `authority_repair`
- Cluster: `control-commands-optionality-2026-04-28`
- Depends on wave: `none`

# Titles in scope

- `workflow_chain bootstrap and run status soften missing control_commands authority instead of failing closed`

# Files to read first

- `/workspace/Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/bug_resolution_program_kickoff_20260428_full.json`
- Current workspace truth: the kickoff file above is not present in this container, and `/workspace` does not contain the expected repo tree. No additional repo files could be verified from disk for this packet.
- Once the repo/artifact tree is hydrated, read the authority bug-system workflow surfaces that implement or expose:
  - workflow orient
  - bug stats/list/search
  - replay-ready view
  - the affected `workflow_chain` bootstrap/run-status authority path for `control_commands`

# Files allowed to change

- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets/wave-0-authority-repair-authority-bug-system-control-commands-optionality-2026-04-28/PLAN.md`
- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets/wave-0-authority-repair-authority-bug-system-control-commands-optionality-2026-04-28/EXECUTION.md`

# Verification or closure proof required

- Verification surface must return cleanly for the affected path across all of:
  - workflow orient
  - bug stats/list/search
  - replay-ready view
- Closure proof for this packet requires repo-backed evidence that the `workflow_chain` bootstrap and run-status path now softens missing `control_commands` authority instead of failing closed.
- In the current container, proof is blocked because `/workspace` contains only the packet artifact tree; no repo-backed Python/application surfaces for `workflow_chain`, `control_commands`, or the authority bug-system commands are present to inspect or change.
- Additional blocking proof from this execution:
  - `praxis_context_shard` reports `workspace_mode: docker_packet_only` and `scope_resolution_error` because the scoped packet path does not match any Python file under `/workspace`.
  - `praxis_query` and `praxis_discover` both fail with `Tool cannot prove workflow shard enforcement yet`, so they cannot supply authoritative repo-surface discovery from this container.
  - `praxis_health` returns a degraded preflight preview with the read-side circuit breaker open, which prevents treating read-model/tool fallback as clean authority proof for this bug lane.

# Intended execution outcome

- `BUG-B5F3106D`: `DEFERRED`
- Reason: this job's truthful narrowest path is to record that the authority repair cannot be executed or verified inside the current packet-only workspace, rather than inventing code or silently softening the proof standard.

# Stop boundary

- Do not edit application code in this job.
- Stop after producing this plan packet.
- If the repo/artifact tree remains absent, do not infer additional file targets or implementation details beyond the packet contract provided in the request.
