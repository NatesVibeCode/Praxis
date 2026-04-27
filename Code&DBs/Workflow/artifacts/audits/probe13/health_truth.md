# Health truth inconsistencies

- `id`: HT-001
  `title`: Global health reports "healthy" while multiple dependent subsystems are degraded
  `runtime_subsystems`: `praxis_health.preflight`, `praxis_health.projection_freshness_sla`, `praxis_health.dependency_truth`, `praxis_health.route_outcomes`
  `inconsistency`:
  `praxis_health.preflight.overall` is `healthy`, but the same health payload also reports `projection_freshness_sla.status=critical`, `projection_freshness_sla.read_side_circuit_breaker=open`, `dependency_truth.ok=false`, and `route_outcomes.status=error`.
  `evidence`:
  `praxis_health` at `2026-04-27T14:39:13Z` showed `preflight.overall=healthy`.
  The same snapshot showed `semantic_current_assertions.staleness_seconds=1001.593993`, `operator_decisions_current.staleness_seconds=1127.862075`, `projection_freshness_sla.alert_count=3`, and `route_outcomes.reason="provider_slugs unavailable ... Cannot run the event loop while another loop is running"`.
  `impact`: The top-level health summary overstates readiness and can cause operators to trust a degraded read side.

- `id`: HT-002
  `title`: Operator-panel truth conflicts with receipt-backed workflow status truth
  `runtime_subsystems`: `praxis_query` routed to `operator_panel`, `praxis_status_snapshot`
  `inconsistency`:
  The operator panel snapshot claims an idle, perfect system while receipt authority reports active work and severe failures.
  `evidence`:
  `praxis_query` at `2026-04-27T14:39:25Z` returned `posture=build`, `recent_pass_rate=1.0`, `running_jobs=0`, `pending_jobs=0`, `last_run_id=null`, `last_activity_at=null`, `circuit_breaker_open=[]`.
  `praxis_status_snapshot` at the same review time returned `pass_rate=0.1437`, `adjusted_pass_rate=0.1497`, `queue_depth_running=1`, and `in_flight_workflows=[{"run_id":"workflow_63161a88f483","workflow_name":"p1_probe13_written_seal","total_jobs":2,"completed_jobs":1,"elapsed_seconds":80.9}]`.
  `impact`: Different runtime surfaces disagree on whether work is running and whether the platform is healthy enough to build.

- `id`: HT-003
  `title`: Lane recommendation says "build" despite failure-dominated recent workflow outcomes
  `runtime_subsystems`: `praxis_health.lane_recommendation`, `praxis_status_snapshot`
  `inconsistency`:
  `praxis_health.lane_recommendation.recommended_posture` is `build` with reason `system healthy, pass rate above 80%`, but the canonical status snapshot reports a pass rate below 15%.
  `evidence`:
  `praxis_health.lane_recommendation`: `recommended_posture=build`, `confidence=1.0`, `reasons=["system healthy, pass rate above 80%"]`.
  `praxis_status_snapshot`: `pass_rate=0.1437`, `failure_breakdown.by_category={"verification_failed":2,"infrastructure":7,"sandbox_error":16}`, `top_failure_codes={"cli_adapter.nonzero_exit":20,"sandbox_error":16,"workflow_submission.required_missing":7,"credential.env_var_missing":4,"adapter.transport_unsupported":2,"verification.required_not_run":2}`.
  `impact`: Admission/posture guidance is inconsistent with recent execution truth and may route more work into a failing lane.

- `id`: HT-004
  `title`: Read-side freshness is critically stale even though queue/observability surfaces present as ready
  `runtime_subsystems`: `praxis_health.projection_freshness_sla`, `praxis_status_snapshot`
  `inconsistency`:
  Observability surfaces present as ready, but the read-side authority used by operator-facing projections is stale enough to open the circuit breaker.
  `evidence`:
  `praxis_status_snapshot` reports `ok=true`, `observability_state=ready`, `zone_authority_ready=true`, `in_flight_authority_ready=true`.
  `praxis_health.projection_freshness_sla` reports `status=critical`, `read_side_circuit_breaker=open`, with stale projections `semantic_current_assertions` and `operator_decisions_current`.
  `impact`: "Ready" status signals can be consumed without noticing that projection-backed reads are degraded and potentially untrustworthy.

- `id`: HT-005
  `title`: Runtime dependency truth is broken while transport/provider readiness is reported as healthy
  `runtime_subsystems`: `praxis_health.dependency_truth`, `praxis_health.preflight.checks`, local sandbox toolability
  `inconsistency`:
  Dependency truth says the runtime environment is broken, but provider transport readiness remains uniformly healthy and the platform still advertises a healthy preflight.
  `evidence`:
  `praxis_health.dependency_truth`: `ok=false`, `manifest_path=/workspace/Code&DBs/Workflow/requirements.runtime.txt`, `error="No module named 'google'"`.
  The same health payload reports multiple provider transport checks as passed, including `provider_transport:openai:cli_llm.status=ok`, `provider_transport:anthropic:cli_llm.status=ok`, `provider_transport:openrouter:llm_task.status=ok`.
  Inside this sandbox, the bundled runtime is also visibly broken: `python3` fails on stdlib imports with `json: ModuleNotFoundError` and `urllib.request` fails because `http` cannot be imported, which also breaks the packaged `praxis` CLI.
  `impact`: Health truth is split between transport-only readiness and actual runtime executability; operators can see "ready" even when the local runtime cannot perform basic control-plane operations.

- `id`: HT-006
  `title`: Status snapshot acknowledges stale code while still marking the result OK
  `runtime_subsystems`: `praxis_status_snapshot`, code drift detector
  `inconsistency`:
  The status snapshot marks itself `ok=true`, but its own drift signal says the process is running old code and warns not to trust subsequent results without restart.
  `evidence`:
  `praxis_status_snapshot.ok=true`.
  `praxis_status_snapshot.code_drift_signal`: `code_out_of_date=true`, `drift_seconds=297`, `hint="This process is running stale code ... The result above was computed by the OLD code; restart the container ... before trusting subsequent results."`
  `impact`: Consumers of the status API can treat the result as authoritative even though the process explicitly says the computation came from stale code.

- `id`: HT-007
  `title`: Tooling contract says review surfaces are available, but submission/tool access is incomplete
  `runtime_subsystems`: execution bundle contract, `praxis_get_submission`, `praxis_bugs`
  `inconsistency`:
  The workflow review contract advertises review-oriented MCP tools, but the concrete session cannot fully use them for corroborating runtime truth.
  `evidence`:
  The execution bundle lists `praxis_get_submission` and `praxis_review_submission` as review tools.
  `praxis_get_submission --job-label step_2` returned `workflow_submission.not_found`.
  `praxis_bugs action=list` returned `not permitted inside a workflow session`, despite bug state being a relevant runtime truth surface for review.
  `impact`: Review jobs cannot consistently retrieve the very evidence surfaces the control bundle implies are available, which weakens cross-subsystem reconciliation.
