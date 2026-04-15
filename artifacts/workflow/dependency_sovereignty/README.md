# Dependency Sovereignty Dispatch References

This folder is the concrete reference set for `roadmap_item.dependency.sovereignty`
and the child packets that must land before any serious replacement discussion
earns the right to exist.

## Matrix

| Roadmap item | Dispatch ref | Registry paths |
| --- | --- | --- |
| `roadmap_item.dependency.sovereignty` | `artifacts/workflow/dependency_sovereignty/dependency_manifest_and_execution.queue.json`, `artifacts/workflow/dependency_sovereignty/dependency_embedding_and_vector.queue.json`, `artifacts/workflow/dependency_sovereignty/dependency_provider_runtime.queue.json` | `Code&DBs/Workflow/runtime/manifest_generator.py`, `Code&DBs/Workflow/runtime/execution/dependency.py`, `Code&DBs/Workflow/runtime/embedding_service.py`, `Code&DBs/Workflow/runtime/provider_route_runtime.py` |
| `roadmap_item.dependency.manifest.truth` | `artifacts/workflow/dependency_sovereignty/dependency_manifest_and_execution.queue.json` | `Code&DBs/Workflow/pyproject.toml`, `Code&DBs/Workflow/requirements.runtime.txt`, `Code&DBs/Workflow/runtime/dependency_contract.py`, `Code&DBs/Workflow/runtime/embedding_service.py`, `Code&DBs/Workflow/surfaces/_subsystems_base.py` |
| `roadmap_item.dependency.execution.runtime.seam` | `artifacts/workflow/dependency_sovereignty/dependency_manifest_and_execution.queue.json` | `Code&DBs/Workflow/adapters/docker_runner.py`, `Code&DBs/Workflow/adapters/cli_llm.py`, `Code&DBs/Workflow/registry/agent_config.py`, `Code&DBs/Workflow/runtime/execution_transport.py` |
| `roadmap_item.dependency.embedding.runtime` | `artifacts/workflow/dependency_sovereignty/dependency_embedding_and_vector.queue.json` | `Code&DBs/Workflow/runtime/embedding_service.py`, `Code&DBs/Workflow/runtime/database_maintenance.py`, `Code&DBs/Workflow/surfaces/_subsystems_base.py` |
| `roadmap_item.dependency.vector.store.seam` | `artifacts/workflow/dependency_sovereignty/dependency_embedding_and_vector.queue.json` | `Code&DBs/Workflow/runtime/task_assembler.py`, `Code&DBs/Workflow/runtime/compiler.py`, `Code&DBs/Workflow/runtime/intent_matcher.py` |
| `roadmap_item.dependency.provider.adapter.runtime` | `artifacts/workflow/dependency_sovereignty/dependency_provider_runtime.queue.json` | `Code&DBs/Workflow/adapters/llm_client.py`, `Code&DBs/Workflow/adapters/cli_llm.py`, `Code&DBs/Workflow/registry/provider_routing.py`, `Code&DBs/Workflow/registry/provider_fallback.py`, `Code&DBs/Workflow/runtime/provider_route_runtime.py` |

## Decision Table

Canonical authority is `operator_decisions` under the typed `architecture_policy`
decision kind. This table is a compact projection of those durable rows, not a
second source of truth.

| Decision key | Scope | Decision | Current consequence |
| --- | --- | --- | --- |
| `architecture-policy::decision-tables::db-native-authority` | `decision_tables` | Decision tables stay DB-native authority. | Cross-cutting control belongs in durable Postgres-backed authority, not in shell folklore. |
| `architecture-policy::decision-tables::scripts-support-only` | `decision_tables` | Scripts support the system; they do not replace it. | Dependency and orchestration work lands in runtime, registry, and DB seams rather than ad hoc scripts. |
| `architecture-policy::embedding-runtime::service-boundary` | `embedding_runtime` | Semantic capability stays at the product surface while heavy local inference leaves default control-plane images. | `api-server` and `workflow-worker` use the service backend contract instead of carrying ambient `torch`. |
| `architecture-policy::embedding-runtime::replacement-contract` | `embedding_runtime` | Do not remove semantic compile/query/discover behavior without a validated replacement. | `torch` stays isolated as the compatibility lane until a leaner backend proves the same contract and acceptable quality. |
| `architecture-policy::embedding-runtime::no-custom-inference` | `embedding_runtime` | Do not hand-roll embedding inference ourselves. | Backend swaps stay behind the embedding contract instead of creating bespoke runtime debt. |
| `architecture-policy::compile-authority::db-backed-enrichment` | `compile_authority` | Compile truth stays DB-backed; embeddings are enrichment. | Semantic degradation can reduce ranking quality without breaking structural compile authority. |

## Rule

- Replacement rows remain directional only until these packets prove the current
  seams are explicit and observable.
- The goal is not dependency churn. The goal is one declared authority path for
  manifest truth, execution behavior, vector semantics, and provider runtime
  selection.

## Notes

- Validate each packet through `./scripts/test.sh validate <queue>` before
  dispatch.
- If a packet cannot name the exact seam it owns, it is too vague and should be
  split again instead of widening the scope.

## Proof Coverage

- Manifest truth is defended by `Code&DBs/Workflow/tests/unit/test_dependency_contract.py`, `Code&DBs/Workflow/tests/unit/test_dependency_truth_surfaces.py`, and `Code&DBs/Workflow/tests/unit/test_startup_wiring.py`; `Code&DBs/Workflow/tests/unit/test_database_maintenance.py` is the live Postgres-backed member of that proof slice.
- Execution runtime seam is defended by `Code&DBs/Workflow/tests/unit/test_docker_runner.py`, `Code&DBs/Workflow/tests/unit/test_transport_support_and_parity.py`, and `Code&DBs/Workflow/tests/unit/test_agent_config.py`.
- The dispatch gate for the packet is `./scripts/test.sh validate artifacts/workflow/dependency_sovereignty/dependency_manifest_and_execution.queue.json`.
