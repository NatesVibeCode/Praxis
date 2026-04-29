# Bugs in scope

- `BUG-AF7C1773`
- `authority_owner`: `lane:authority_bug_system`
- `lane`: `Authority / bug system (authority_bug_system)`
- `wave`: `wave_0_authority_repair`
- `packet_kind`: `authority_repair`
- `cluster`: `cluster: authority-bug-system (bug.tag.cluster:authority-bug-system)`
- `depends_on_wave`: none

# Titles in scope

- `praxis_wave start reports a running wave but observe from a fresh process shows no waves`

# Files to read first

- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets/wave-0-authority-repair-authority-bug-system-cluster-authority-bug-system/PLAN.md`
- `/workspace/Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/bug_resolution_program_kickoff_20260428_full.json` is referenced by contract but is not present in the current workspace snapshot.
- No workflow implementation files for `praxis_wave`, workflow orient, bug stats/list/search, or replay-ready surfaces are present anywhere under `/workspace` in the current repo snapshot.

# Files allowed to change

- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets/wave-0-authority-repair-authority-bug-system-cluster-authority-bug-system/PLAN.md`

# Verification or closure proof required

- The execution packet must prove `workflow orient` returns cleanly for the affected path.
- The execution packet must prove bug stats, bug list, and bug search all return cleanly for the affected path.
- The execution packet must prove the replay-ready view returns cleanly for the affected path.
- Because the kickoff JSON and workflow source files are absent from the current workspace snapshot, no source-backed verification can be completed from this job; closure requires a hydrated workspace that contains the missing coordination file and the relevant workflow implementation surfaces.

# Stop boundary

- This job is limited to writing this packet plan from current workspace truth.
- Do not edit workflow code, tests, configs, or other packet artifacts in this job.
- Do not invent missing kickoff contents or missing source paths; treat their absence as a workspace constraint that downstream execution must resolve before implementation or verification.
