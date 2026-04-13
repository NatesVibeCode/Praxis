# shared_truth_merge

## repo_inventory_core
Files: `runtime/codebase_index_module.py`, `runtime/module_indexer.py`, `runtime/scope_resolver.py`

Live callers: `runtime/heartbeat_runner.py` wires `CodebaseIndexModule`; `surfaces/_subsystems_base.py`, `surfaces/mcp/tools/discover.py`, and `surfaces/cli/workflow_runner.py` use `ModuleIndexer`; `adapters/context_adapter.py`, `runtime/workflow/unified.py`, `surfaces/api/rest.py`, and `surfaces/cli/commands/query.py` call `resolve_scope()`.

Docs/tests: `DYNAMIC_SCOPE_RESOLUTION.md`, `SCOPE_RESOLUTION_IMPLEMENTATION.md`, `SPEC_COMPILER_README.md`, `RUNTIME_CATALOG.md`, `tests/integration/test_module_indexer.py`, and `tests/unit/test_unified_workflow.py`.

Reasoning: these files are rebuilding the same codebase facts through separate scans. `runtime/codebase_index_module.py` has its own Python/TS walk plus `_TS_IMPORT_RE`, `_TS_COMPONENT_RE`, and `_TS_HOOK_RE`; `runtime/module_indexer.py` defines the same TS regex family and its own full walker/extractor; `runtime/scope_resolver.py` reparses the Python tree again to build `ImportGraph`. That is duplicated repo-inventory truth, not merely adjacent code. Merge file walk, import/dependency extraction, and change-hash primitives behind one shared repo-inventory core. Keep `CodebaseIndexModule.run()`, `ModuleIndexer.search()/index_codebase()`, and `resolve_scope()` as thin projections over that shared truth instead of collapsing the whole cluster into one file.

## parse_core
Files: `runtime/output_parser.py`, `adapters/structured_output.py`, `adapters/output_parser_adapter.py`

Live callers: `runtime/capability_feedback.py` and `runtime/execution/orchestrator.py` call `parse_json_from_completion()` from `runtime/output_parser.py`; `runtime/workflow/runtime_setup.py` registers `OutputParserAdapter`; `adapters/cli_llm.py` imports `StructuredOutput` and `parse_model_output()` from `adapters/structured_output.py`.

Docs/tests: `DYNAMIC_SCOPE_RESOLUTION.md`, `SCOPE_RESOLUTION_IMPLEMENTATION.md`, `RUNTIME_CATALOG.md`, `tests/unit/test_structured_output.py`, and `tests/integration/test_workflow_graph.py`.

Reasoning: `runtime/output_parser.py` and `adapters/structured_output.py` both own raw-completion parsing and both duplicate JSON/fence/bracket extraction logic, while also publishing different `StructuredOutput` contracts. `adapters/output_parser_adapter.py` is the live graph-facing projection over the richer parser. Merge the parsing truth into one core, then keep caller-specific projections on top of it: generic dict/list extraction for orchestrator and feedback scoring, and code-block shaping for the workflow adapter path. Do not pull `runtime/output_writer.py` into this merge.

# package_level_consolidation

## provider_route_surface
Files: `registry/provider_fallback.py`, `registry/provider_routing.py`

Live callers: `runtime/provider_route_runtime.py`, `runtime/default_path_pilot.py`, and `registry/model_routing.py` use `registry.provider_routing`; `surfaces/api/native_operator_surface.py` and `observability/operator_dashboard.py` use `registry.provider_fallback`.

Docs/tests: `docs/25_PROVIDER_ROUTE_CONTROL_TOWER.md`, `docs/29_DEFAULT_PATH_PILOT_WIRING.md`, `docs/31_PROVIDER_ROUTE_RUNTIME_WIRING.md`, `docs/34_DEFAULT_PATH_ROUTE_RUNTIME_ADOPTION.md`, `docs/36_NATIVE_OPERATOR_COCKPIT_ADOPTION.md`, `tests/integration/test_provider_route_authority.py`, and `tests/integration/test_provider_route_control_tower.py`.

Reasoning: `registry/provider_fallback.py` is source-level wrapper duplication, not a second authority. It aliases `ProviderRouteControlTower = ProviderRouteAuthority`, aliases the repository error type, and every repository method delegates straight into `PostgresProviderRouteAuthorityRepository`. The split that remains is naming and packaging for different caller families. Consolidate behind one canonical package surface with compatibility exports or one re-export layer; do not keep two long-term module entrypoints over the same Postgres truth.

## execution_packet_surface
Files: `runtime/execution_packet_runtime.py`, `runtime/execution_packet_authority.py`

Live callers: `adapters/cli_llm.py`, `adapters/llm_task.py`, and `runtime/workflow/unified.py` use `runtime/execution_packet_runtime.py` to load packet bindings and fail closed on packet-required execution; `runtime/shadow_execution_packet.py`, `runtime/compile_artifacts.py`, `runtime/workflow/unified.py`, `surfaces/api/operator_read.py`, `surfaces/api/handlers/workflow_query.py`, and `surfaces/api/frontdoor.py` use `runtime/execution_packet_authority.py` for lineage, packet finalization, inspection, and drift views.

Docs/tests: `docs/65_SHADOW_EXECUTION_PACKETS.md`, `tests/integration/test_workflow_migration_contracts.py`, `tests/unit/test_unified_workflow.py`, and the workflow/API read-surface tests that exercise packet inspection paths.

Reasoning: this is one packet domain with two real seams. `runtime/execution_packet_runtime.py` owns executable runtime binding plus compile-index validation; `runtime/execution_packet_authority.py` owns lineage payloads, finalization, inspection, and materialized run views. They should share one package surface and common schema helpers, but they should not be flattened into one undifferentiated file. The doc still describes the packet path as shadow-only, but current source proves packet rows now participate in live adapter execution, which makes packaging more important, not less.

# wire_or_delete

## dormant_context_memory_surfaces
Files: `runtime/proactive_context.py`, `runtime/surface_compositor.py`, `memory/federated_retrieval.py`, `memory/bridge_queries.py`

Live callers: none found in repo-wide non-test source search. The only direct imports are test files: `tests/unit/test_bridge_and_proactive.py`, `tests/unit/test_federated_and_research.py`, and `tests/unit/test_session_and_compositor.py`.

Docs/tests: unit tests plus catalog/generated inventory mentions such as `RUNTIME_CATALOG.md`; no workflow docs or production surface modules cite these seams.

Reasoning: this is not a "low-ref-count" call. It is a no-live-caller call. These modules have isolated unit coverage, but nothing in the production runtime, API surfaces, operator surfaces, or workflow execution paths imports them. Either wire them to a concrete context, retrieval, or surface entrypoint, or delete/archive/demote them so the runtime inventory stops pretending they are live authority.

## protocol_endpoint_runtime
Files: `adapters/protocol_endpoint_runtime.py`

Live callers: none found outside the module itself and its adoption doc. Repo-wide search found no imports in production code, including the explicitly out-of-scope MCP-adjacent files `runtime/workflow/mcp_bridge.py`, `surfaces/workflow_bridge.py`, and `adapters/protocol_events.py`.

Docs/tests: `docs/51_MCP_ENDPOINT_AUTHORITY_ADOPTION.md` and `tests/integration/test_mcp_endpoint_authority_adoption.py`.

Reasoning: the bounded MCP endpoint seam is source-proven in isolation, but it is not actually wired into a live MCP egress path. This is a textbook wire-or-delete case. Either attach it to the real protocol call path or delete/demote the seam and correct the doc that currently reads as if the adoption already happened.

# leave_alone

## output_writer
Files: `runtime/output_writer.py`

Live callers: `adapters/file_writer_adapter.py` is the production writer seam; proof scripts also call `apply_structured_output()`.

Docs/tests: `tests/unit/test_output_writer.py` and `RUNTIME_CATALOG.md`.

Reasoning: `runtime/output_writer.py` does not duplicate parse truth. It owns a separate authority boundary: path validation, workspace containment, atomic writes, and write manifests. Keep it isolated from parser consolidation.

## native_runtime_profile_sync_and_admission
Files: `registry/runtime_profile_admission.py`, `registry/native_runtime_profile_sync.py`

Live callers: `runtime/task_type_router.py` and `runtime/workflow/unified.py` call `load_admitted_runtime_profile_candidates()`; `registry/repository.py` calls `sync_native_runtime_profile_authority_async()`; `runtime/native_authority.py`, `runtime/workflow/receipt_writer.py`, `runtime/workflow/runtime_setup.py`, `runtime/workflow/unified.py`, and `surfaces/cli/workflow_cli.py` consume the default native refs/config helpers from the sync module.

Docs/tests: `tests/unit/test_native_runtime_profile_sync.py` plus the runtime callers above.

Reasoning: the boundary in source is real. `registry/native_runtime_profile_sync.py` reads checked-in native config, projects it into Postgres, and syncs live candidate, budget, and eligibility rows. `registry/runtime_profile_admission.py` reads those tables to resolve admitted runtime candidates and only triggers sync as a precondition for native profiles. Shared tables and one orchestration call do not make them duplicate authority. Leave this read/write split alone.
