# Search Proof
- Workflow validate passed for `/Users/nate/Praxis/artifacts/workflow_eval/probe_exec_20260416/runtime_regression_probe.queue.json`.
- Workflow preview passed.
- `praxis workflow discover "workflow spec with runtime regression deterministic tooling search db action" --json` returned these top module/class paths: `runtime/workflow/decision_context.py`, `adapters/deterministic.py`, `surfaces/workflow_bridge.py`, `runtime/workflow_spec.py`, `runtime/observability.py`, `adapters/deterministic.py::DeterministicTaskAdapter`, `runtime/build_review_decisions.py`, `surfaces/api/handlers/_query_bugs.py`, `runtime/_workflow_database.py`, `runtime/workflow_worker.py`.
- `praxis workflow tools call praxis_bugs --input-json '{"action":"stats"}' --yes` returned: `total=26`, `open_count=5`, `by_status={DEFERRED:1,FIXED:14,OPEN:5,WONT_FIX:6}`, `by_category={RUNTIME:7,ARCHITECTURE:7,TEST:1,VERIFY:5,OTHER:1,WIRING:5}`, `underlinked_count=16`.

# Authority Gaps
- This handoff is non-authoritative because the workflow run failed before durable submission and no `run_id` exists.
- The launch error in `/Users/nate/Praxis/artifacts/workflow.log` was: `PostgresConfigurationError: WORKFLOW_DATABASE_URL authority unavailable: InvalidAuthorizationSpecificationError: role "postgres" does not exist`.
- Regression bug filed: `BUG-AE2B9669` with title `workflow run still resolves postgres role after validate and preview succeed`.

# Next Verification
- Re-run the workflow only after fixing the Postgres authority so durable submission can produce a `run_id`.
- Verify the same queue against the corrected database authority, then confirm the run is recorded durably before treating the handoff as authoritative.
