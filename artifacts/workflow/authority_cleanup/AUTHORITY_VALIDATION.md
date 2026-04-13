# Failure Semantics & Trigger Truth Proof

Date: 2026-04-09  
Owner-facing artifact for closeout jobs:
`roadmap_item.authority.cleanup.failure_semantics.proof`,
`roadmap_item.workflow.trigger.checkpoint_cutover`,
and children touching schedule trigger truth.

## 1) Live claims currently supported by code

### Trigger truth

1) Cron schedules emit durable, replayable trigger events.
- `Code&DBs/Workflow/runtime/cron_scheduler.py` now inserts `system_events` with `event_type='schedule.fired'`, `source_type='workflow_trigger'`, `source_id` set to the trigger id, and payload containing `trigger_id`, `workflow_id`, and `cron_expression`.
- `Code&DBs/Workflow/runtime/heartbeat_runner.py` runs the cron tick path via `_CronHeartbeatModule`, so the emission path is active in heartbeat execution.
- `Code&DBs/Workflow/tests/unit/test_triggers.py::test_evaluate_triggers_processes_event_subscriptions_without_checkpoint` and `::test_evaluate_triggers_emits_depth_exceeded_event` anchor the scheduler/event emission seam indirectly by asserting trigger event loading and checkpoint depth behavior.

2) Trigger evaluation is checkpoint-driven and idempotent to replay.
- `Code&DBs/Workflow/runtime/triggers.py` initializes evaluator subscriptions via `_ensure_trigger_evaluator_subscription` and advances durable cursors with `_upsert_workflow_trigger_checkpoint`.
- `Code&DBs/Workflow/runtime/triggers.py::evaluate_triggers` and `_load_workflow_events_from_checkpoint` drive replay from `subscription_checkpoints`, which keeps trigger consumption deterministic across restarts.
- `Code&DBs/Workflow/tests/unit/test_triggers.py` includes replay assertions:
  - `test_evaluate_triggers_bootstraps_durable_trigger_evaluator_subscription`
  - `test_evaluate_triggers_advances_checkpoint_and_skips_old_events_on_replay`
  - `test_event_subscriptions_resume_from_checkpoint_without_double_processing`

3) Trigger evaluation is part of heartbeat processing.
- `Code&DBs/Workflow/runtime/heartbeat_runner.py::_CronHeartbeatModule` and `::_TriggerEvaluatorModule` both execute in the unified runtime heartbeat path.
- `Code&DBs/Workflow/runtime/workflow/unified.py::run_worker_loop` also executes fallback and heartbeat handling in one loop and currently evaluates worker wakeups through notify/listen plus checkpoint loading hooks.

### Failure semantics

4) Failure classification is canonical and materialized at terminal completion.
- `Code&DBs/Workflow/runtime/failure_classifier.py` defines canonical categories and transient/retry semantics.
- `Code&DBs/Workflow/runtime/workflow/unified.py` computes terminal `failure_category`, `failure_zone`, and `is_transient`, then persists them into `workflow_jobs` via `complete_job`.
- `Code&DBs/Workflow/runtime/workflow/unified.py::test_complete_job_terminal_failure_update_writes_failure_columns` confirms this projection.

5) Retry decisioning is sourced from pre-classified failure semantics.
- `Code&DBs/Workflow/runtime/retry_orchestrator.py` uses classification state for terminal/retry branching.
- `Code&DBs/Workflow/runtime/workflow/unified.py::test_complete_job_non_retryable_failure_does_not_call_retry_orchestrator` and `::test_complete_job_requeues_rate_limit_failures_to_next_agent` confirm the pre-classified path.
- `Code&DBs/Workflow/tests/unit/test_auto_retry.py::test_non_retryable_immediately_false` and `test_rate_limit_failover_wins_over_same_model_retry` verify contract behavior at the classifier+orchestrator seam.

6) Route health and penalties now consume failure semantics instead of compatibility guessing.
- `Code&DBs/Workflow/runtime/task_type_router.py` loads `failure_category_zones` into explicit route metadata and uses `_failure_penalty` + `_normalized_failure_details` from canonical fields.
- `Code&DBs/Workflow/tests/unit/test_workflow_policy.py::test_workflow_with_retry_retries_then_escalates_and_records` and `::test_workflow_with_retry_uses_adapter_http_error_contract` validate retry pressure/routing behavior aligned with canonical outcomes.

7) Failure fields are read from canonical DB columns in status/read models.
- `Code&DBs/Workflow/runtime/workflow/unified.py::get_run_status` and worker read paths include `failure_category`, `failure_zone`, `is_transient` in run/job payloads.
- `Code&DBs/Workflow/runtime/chat_tools.py` projects those same fields from `workflow_jobs` for status/read compatibility layers.
- `Code&DBs/Workflow/tests/unit/test_unified_workflow.py::test_get_run_status_includes_job_timestamps_and_classification_fields` covers this read-side contract.

## 2) Remaining blockers before closeout

1) Unified polling fallback is still active (`roadmap_item.workflow.trigger.checkpoint_cutover.unified.polling.fallback.retirement`).
- Exact file location: `Code&DBs/Workflow/runtime/workflow/unified.py`
- Current proof point: `trigger_eval_interval = 5.0` and periodic `if time.monotonic() - last_trigger_eval > trigger_eval_interval` path in `run_worker_loop`.
- Impact: trigger truth is still partially dual-authority (LISTEN+checkpoint + periodic polling sweep), so the “one authority path” requirement is not met yet.

2) Quality view path has an unbound `ZONE_MAP` reference (`roadmap_item.authority.cleanup.failure_semantics.proof`).
- Exact file location: `Code&DBs/Workflow/runtime/quality_views.py`, function `QualityViewMaterializer._accum_to_profile`.
- Code line pattern: `ZONE_MAP.get(k)` is referenced but no module/global `ZONE_MAP` is defined in that module.
- Impact: any code path calling `_accum_to_profile` for failure-category rollup can fail at runtime and blocks full failure-semantics observability closure.
- Required fix signal: map must use existing instance mapping (e.g. `self._zone_map`) or equivalent safe lookup.

3) Notification truth has not been proven under concurrent wake/replay stress yet.
- Current repo evidence covers checkpoint semantics and replay logic, but no explicit canary proves race/duplication behavior under concurrent NOTIFY bursts plus retries in the same run.
- Impact: residual risk to full trigger cutover confidence; not a functional blocker to existing tests, but it is a closeout-risk blocker for strict “post-failover production canary complete” acceptance.

## 3) Closeout map for downstream job (no forensic re-run required)

- `roadmap_item.workflow.trigger.checkpoint_cutover.schedule.fired.emission`:
  - Evidence status: **READY TO CLOSE**
  - Required evidence present in files/tests listed in sections 1.1 and 1.2.

- `roadmap_item.workflow.trigger.checkpoint_cutover.unified.polling.fallback.retirement`:
  - Evidence status: **BLOCKED**
  - Must close only after periodic trigger polling path is removed from runtime truth.

- `roadmap_item.authority.cleanup.failure_semantics.proof`:
  - Evidence status: **BLOCKED**
  - Must clear blocker 2 (and any similar runtime NameError path in quality rollups) before marking closed.

- `roadmap_item.authority.cleanup.failure_semantics.route_health`:
  - Evidence status: **READY IF 1) route-health tests and 2) failure-zones table coverage pass together**
  - Current status is directionally clear from `task_type_router.py` and `test_workflow_policy.py`, but the closeout job should treat this as conditional unless rerun against full DB-backed zone coverage.

## 4) Exact follow-up tasks to make blockers closeable

1. Delete the fallback sweep window from `Code&DBs/Workflow/runtime/workflow/unified.py` and prove `run_worker_loop` reacts only to durable trigger sources.
2. Fix `Code&DBs/Workflow/runtime/quality_views.py::_accum_to_profile` by replacing the undefined `ZONE_MAP` access with bound instance mapping and adding at least one regression test for mixed internal/external failure categories.
3. Add a concurrency/replay canary in the trigger smoke path that validates no duplicate processing under repeated NOTIFY + retry races.

## 5) Verification files to run by closeout

Run at least:
- `PYTHONPATH='Code&DBs/Workflow' /opt/homebrew/bin/python3 -m pytest --noconftest -q 'Code&DBs/Workflow/tests/unit/test_triggers.py'`
- `PYTHONPATH='Code&DBs/Workflow' /opt/homebrew/bin/python3 -m pytest --noconftest -q 'Code&DBs/Workflow/tests/unit/test_unified_workflow.py::test_get_run_status_includes_job_timestamps_and_classification_fields' 'Code&DBs/Workflow/tests/unit/test_unified_workflow.py::test_complete_job_terminal_failure_update_writes_failure_columns'`
- `PYTHONPATH='Code&DBs/Workflow' /opt/homebrew/bin/python3 -m pytest --noconftest -q 'Code&DBs/Workflow/tests/unit/test_workflow_policy.py::test_workflow_with_retry_retries_then_escalates_and_records'`
- `PYTHONPATH='Code&DBs/Workflow' /opt/homebrew/bin/python3 -m pytest --noconftest -q 'Code&DBs/Workflow/tests/unit/test_auto_retry.py::test_non_retryable_immediately_false' 'Code&DBs/Workflow/tests/unit/test_auto_retry.py::test_rate_limit_failover_wins_over_same_model_retry'`
- `PYTHONPATH='Code&DBs/Workflow' /opt/homebrew/bin/python3 -m pytest --noconftest -q 'Code&DBs/Workflow/tests/unit/test_triggers.py::test_evaluate_triggers_bootstraps_durable_trigger_evaluator_subscription'`

## 6) Operator write gate addendum

Date: 2026-04-09

- `Code&DBs/Workflow/surfaces/api/operator_read.py` now derives roadmap-tree ordering from parsed phase-order tokens instead of lexical SQL ordering, so the subtree read model follows the same numeric authoring truth the writer uses.
- `Code&DBs/Workflow/tests/integration/test_roadmap_tree_view.py` now carries three proofs for the operator write gate:
  - a sandbox-safe renderer proof that `1.2` sorts before `1.10`
  - a live preview-parity proof that compares the write preview payload to the committed tree read model
  - a transaction-safety proof that simulates a mid-write failure and expects no partial tree rows to persist
- Residual risk outcome: the only remaining risk is environmental access to a reachable local Postgres instance for the DB-backed integration proofs. In this sandbox those tests are intentionally skipped because TCP and socket connections to Postgres are blocked. No additional code-path blocker remains in the write/read model seam.
