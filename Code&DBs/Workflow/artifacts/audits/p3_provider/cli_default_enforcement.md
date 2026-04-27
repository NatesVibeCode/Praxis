## CLI-default enforcement audit note

This sandbox did not expose the hydrated repository under `/workspace`, so a source-complete confirmation of every route-decision site was not possible.

Indexed authority did surface one likely API-defaulting or API-forcing path that should be treated as requiring direct source inspection on rerun:

- `Code&DBs/Workflow` compile/submission path: bug authority entry `compile/submission path calls api_llm despite operator decision + registry binding cli_llm — MCP timeouts on multi-packet submissions when api_llm transport unwired`
  - Evidence available in indexed authority says this path calls `api_llm` despite the operator decision `operator_decision.architecture_policy.provider_routing.cli_default_api_exception`.
  - Exact source file/line confirmation was blocked because the live sandbox did not mount the advertised repo snapshot.

Related route-decision files named by indexed authority for follow-up source inspection:

- `Code&DBs/Workflow/runtime/execution_transport.py`
- `Code&DBs/Workflow/surfaces/cli/workflow_runner.py`
- `Code&DBs/Workflow/runtime/model_executor.py`
- `Code&DBs/Workflow/runtime/workflow/_execution_core.py`
- `Code&DBs/Workflow/adapters/llm_client.py`
- `Code&DBs/Workflow/adapters/permission_matrix.py`
