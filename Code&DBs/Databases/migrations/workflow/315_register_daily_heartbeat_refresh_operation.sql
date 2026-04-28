-- Migration 315: Register the daily heartbeat refresh command through the
-- operation catalog.
--
-- The probe writer already exists in runtime.daily_heartbeat; what was
-- missing was one canonical command boundary. Without it, the wrapper script,
-- CLI alias, and MCP tool all imported the runtime writer directly and skipped
-- receipts/events at the gateway seam.

BEGIN;

INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    current_projection_ref,
    storage_target_ref,
    enabled,
    decision_ref
) VALUES (
    'authority.heartbeat_runs',
    'praxis.engine',
    'stream.authority.heartbeat_runs',
    NULL,
    'praxis.primary_postgres',
    TRUE,
    'decision.operation_catalog_registry.daily_heartbeat_refresh.20260428'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

SELECT register_operation_atomic(
    p_operation_ref         := 'operator-daily-heartbeat-refresh',
    p_operation_name        := 'operator.daily_heartbeat_refresh',
    p_handler_ref           := 'runtime.operations.commands.daily_heartbeat_refresh.handle_daily_heartbeat_refresh',
    p_input_model_ref       := 'runtime.operations.commands.daily_heartbeat_refresh.DailyHeartbeatRefreshCommand',
    p_authority_domain_ref  := 'authority.heartbeat_runs',
    p_authority_ref         := 'authority.heartbeat_runs',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/operator/daily-heartbeat-refresh',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_type            := 'daily.heartbeat.refreshed',
    p_decision_ref          := 'decision.operation_catalog_registry.daily_heartbeat_refresh.20260428',
    p_binding_revision      := 'binding.operation_catalog_registry.daily_heartbeat_refresh.20260428',
    p_label                 := 'Operation: operator.daily_heartbeat_refresh',
    p_summary               := 'Run the daily external-health heartbeat through one receipt-backed command boundary. Persists heartbeat_runs + heartbeat_probe_snapshots and emits daily.heartbeat.refreshed for the resulting evidence packet.'
);

COMMIT;

-- Verification (run manually):
--   SELECT operation_ref, operation_name, event_type, http_path
--     FROM operation_catalog_registry
--    WHERE operation_ref = 'operator-daily-heartbeat-refresh';
