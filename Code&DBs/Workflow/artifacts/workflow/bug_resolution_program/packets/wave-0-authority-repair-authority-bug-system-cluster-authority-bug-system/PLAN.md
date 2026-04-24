# Bugs in scope

- `BUG-30798A5F`
- `BUG-88BE517A`
- `BUG-1FC820AC`
- `authority_owner`: `lane:authority_bug_system`
- `lane`: `Authority / bug system (authority_bug_system)`
- `wave`: `wave_0_authority_repair`
- `packet_kind`: `authority_repair`
- `cluster`: `cluster: authority-bug-system (bug.tag.cluster:authority-bug-system)`
- `depends_on_wave`: none

# Titles in scope

- `bug_resolution_program materialize-packets imports DB-backed tool modules and emits authority failures for offline rendering`
- `scripts/praxis workflow front door hangs for orient and bugs commands without output`
- `Repo WORKFLOW_DATABASE_URL points at host.docker.internal, which does not resolve from the host shell`

# Files to read first

- `scripts/praxis`
- `scripts/_workflow_env.sh`
- `README.md`
- `SETUP.md`
- `Code&DBs/Workflow/surfaces/_workflow_database.py`
- `Code&DBs/Workflow/runtime/_workflow_database.py`
- `Code&DBs/Workflow/surfaces/cli/praxis.py`
- `Code&DBs/Workflow/surfaces/cli/main.py`
- `Code&DBs/Workflow/surfaces/cli/commands/operate.py`
- `Code&DBs/Workflow/surfaces/cli/commands/query.py`
- `Code&DBs/Workflow/surfaces/cli/mcp_tools.py`
- `Code&DBs/Workflow/surfaces/mcp/tools/operator.py`
- `Code&DBs/Workflow/surfaces/mcp/tools/bugs.py`
- `Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py`
- `Code&DBs/Workflow/surfaces/api/handlers/_bug_surface_contract.py`
- `Code&DBs/Workflow/runtime/operations/queries/operator_observability.py`
- `Code&DBs/Workflow/surfaces/api/handlers/_query_bugs.py`

# Files allowed to change

- `scripts/praxis`
- `scripts/_workflow_env.sh`
- `README.md`
- `SETUP.md`
- `Code&DBs/Workflow/surfaces/_workflow_database.py`
- `Code&DBs/Workflow/runtime/_workflow_database.py`
- `Code&DBs/Workflow/surfaces/cli/praxis.py`
- `Code&DBs/Workflow/surfaces/cli/main.py`
- `Code&DBs/Workflow/surfaces/cli/commands/operate.py`
- `Code&DBs/Workflow/surfaces/cli/commands/query.py`
- `Code&DBs/Workflow/surfaces/cli/mcp_tools.py`
- `Code&DBs/Workflow/surfaces/mcp/tools/operator.py`
- `Code&DBs/Workflow/surfaces/mcp/tools/bugs.py`
- `Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py`
- `Code&DBs/Workflow/surfaces/api/handlers/_bug_surface_contract.py`
- `Code&DBs/Workflow/runtime/operations/queries/operator_observability.py`
- `Code&DBs/Workflow/surfaces/api/handlers/_query_bugs.py`
- Matching unit/integration tests for the touched authority surfaces under `Code&DBs/Workflow/tests/unit/` and `Code&DBs/Workflow/tests/integration/`

# Verification or closure proof required

- `workflow orient` must return a payload cleanly through the catalog-backed path, and `POST /orient` must return the same authority envelope shape cleanly.
- `workflow bugs stats`, `workflow bugs list`, and `workflow bugs search <query>` must all return cleanly from the same bug authority path, without silent hangs and without masking DB-authority failures.
- The replay-ready read path must return cleanly through the canonical operator surface for `operator.replay_ready_bugs` / `/api/operator/replay-ready-bugs`.
- DB authority resolution must fail closed and explain the authority source; it must not strand host-shell callers on an unusable `host.docker.internal` DSN without a clear authority-specific error.
- Proof must include focused tests for the touched CLI/MCP/API authority paths and a command transcript or receipt showing the clean orient + bug stats/list/search + replay-ready flow.

# Stop boundary

- This packet is limited to authority repair for orient, bug read surfaces, replay-ready bug view, and workflow DB authority resolution.
- Do not change unrelated workflow execution, provider routing, bug write semantics, or broader packet/rendering pipelines outside the files above.
- Do not introduce a parallel authority source, localhost Postgres fallback, or sidecar-only config path.
- Do not resolve by suppressing or flattening authority failures; surface them explicitly.
