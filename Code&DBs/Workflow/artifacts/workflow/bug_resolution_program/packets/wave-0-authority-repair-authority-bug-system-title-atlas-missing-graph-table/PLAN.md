# Bugs in scope

- `BUG-37D18B71`
- Authority owner: `lane:authority_bug_system`
- Lane: `Authority / bug system (authority_bug_system)`
- Wave: `wave_0_authority_repair`
- Packet kind: `authority_repair`
- Cluster: `atlas.missing.graph-table`

# Titles in scope

- Atlas missing graph-table toggle and data-graph hierarchy-aware node metadata

# Files to read first

- `Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py`
  Why: `/orient` is the canonical authority envelope and the verification contract explicitly requires `workflow orient` to return cleanly.
- `Code&DBs/Workflow/runtime/primitive_contracts.py`
  Why: `/orient` projects primitive bug-surface defaults from here; the bug list vs backlog split is authoritative here.
- `Code&DBs/Workflow/surfaces/api/handlers/_query_bugs.py`
  Why: owns the HTTP bug list route and the replay-ready GET view wiring.
- `Code&DBs/Workflow/surfaces/api/handlers/workflow_query_core.py`
  Why: owns bug `list`, `search`, `stats`, `replay`, and operator-view dispatch.
- `Code&DBs/Workflow/surfaces/api/handlers/_bug_surface_contract.py`
  Why: shared contract for bug list/search/stats/replay payload shaping and replay-state annotation.
- `Code&DBs/Workflow/runtime/operations/queries/operator_observability.py`
  Why: runtime authority for the `replay_ready_bugs` read model.
- `Code&DBs/Workflow/surfaces/mcp/tools/bugs.py`
  Why: MCP bug surface must stay aligned with the HTTP bug contract for `list`, `search`, and `stats`.
- `Code&DBs/Workflow/runtime/atlas_graph.py`
  Why: canonical Atlas graph payload assembly, including table-node area inference and payload metadata.
- `Code&DBs/Workflow/surfaces/api/rest.py`
  Why: exposes `GET /api/atlas/graph`, the app-facing Atlas graph authority.
- `Code&DBs/Workflow/surfaces/app/src/atlas/AtlasPage.tsx`
  Why: current Atlas UI surface; this is where any graph/table toggle or hierarchy-aware node metadata rendering would have to land.
- `Code&DBs/Workflow/surfaces/app/src/atlas/AtlasPage.css`
  Why: paired UI surface styling if Atlas gains a table mode or richer node-detail treatment.
- `Code&DBs/Workflow/tests/unit/test_dependency_truth_surfaces.py`
  Why: pins `/orient` packet shape and read order.
- `Code&DBs/Workflow/tests/unit/test_atlas_graph.py`
  Why: pins Atlas payload metadata, freshness behavior, and schema-area inference.
- `Code&DBs/Workflow/tests/unit/test_workflow_query_handlers.py`
  Why: pins operator-view dispatch, including `replay_ready_bugs`.
- `Code&DBs/Workflow/tests/unit/test_operator_observability_queries.py`
  Why: pins replay-ready bug query semantics.
- `Code&DBs/Workflow/tests/unit/test_bug_surface_default_open_only_unified.py`
  Why: pins the machine-facing vs operator-facing bug-surface default split.

# Files allowed to change

- `Code&DBs/Workflow/surfaces/app/src/atlas/AtlasPage.tsx`
- `Code&DBs/Workflow/surfaces/app/src/atlas/AtlasPage.css`
- `Code&DBs/Workflow/runtime/atlas_graph.py`
- `Code&DBs/Workflow/surfaces/api/rest.py`
- `Code&DBs/Workflow/surfaces/api/handlers/_query_bugs.py`
- `Code&DBs/Workflow/surfaces/api/handlers/workflow_query_core.py`
- `Code&DBs/Workflow/surfaces/api/handlers/_bug_surface_contract.py`
- `Code&DBs/Workflow/runtime/operations/queries/operator_observability.py`
- `Code&DBs/Workflow/surfaces/mcp/tools/bugs.py`
- `Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py`
- `Code&DBs/Workflow/runtime/primitive_contracts.py`
- Targeted verification tests for the files above:
  `Code&DBs/Workflow/tests/unit/test_atlas_graph.py`
  `Code&DBs/Workflow/tests/unit/test_dependency_truth_surfaces.py`
  `Code&DBs/Workflow/tests/unit/test_workflow_query_handlers.py`
  `Code&DBs/Workflow/tests/unit/test_operator_observability_queries.py`
  `Code&DBs/Workflow/tests/unit/test_bug_surface_default_open_only_unified.py`

# Verification or closure proof required

- `workflow orient` or the equivalent `/orient` surface returns a clean authority packet for this lane, with `standing_orders` and the canonical read order intact.
- Bug `stats` returns cleanly through the shared bug contract.
- Bug `list` returns cleanly through the shared bug contract.
- Bug `search` returns cleanly through the shared bug contract.
- The replay-ready read model returns cleanly through the operator view path (`replay_ready_bugs`) without write-side backfill behavior.
- `GET /api/atlas/graph` returns a clean Atlas payload from `runtime.atlas_graph.build_atlas_payload`.
- Atlas UI proof must show the affected path is fixed for this title:
  either a graph/table toggle exists and works, or the execution packet proves the title is retired/reframed against current Atlas authority with tests updated accordingly.
- Minimum proof set should be targeted tests covering:
  `test_dependency_truth_surfaces.py`
  `test_atlas_graph.py`
  `test_workflow_query_handlers.py`
  `test_operator_observability_queries.py`
  `test_bug_surface_default_open_only_unified.py`

# Stop boundary

- Do not widen beyond `BUG-37D18B71` or the `atlas.missing.graph-table` title cluster.
- Do not refactor unrelated bug APIs, MCP catalog wiring, backlog semantics, or replay backfill maintenance flows.
- Do not add schema migrations, new tables, or new operator-decision policy in this packet unless the execution packet proves an unavoidable authority gap.
- Do not rewrite Atlas layout/visual design outside the missing graph-table toggle and hierarchy-aware node-metadata path.
- Do not change unrelated native operator surfaces, dashboard presets, or broader search/recall behavior.
- This planning packet is read/write authority only for `PLAN.md`; no code changes belong in this job.
