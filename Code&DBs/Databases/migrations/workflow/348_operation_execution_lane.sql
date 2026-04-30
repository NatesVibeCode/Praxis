-- Migration 348: operation execution lane (interactive | background | system)
--                 + kickoff_required flag.
--
-- Anchor decision:
--   architecture-policy::concurrency::operation-execution-lane-typing
--   (operator_decisions row, registered in this migration)
--
-- Why this exists
--   Phase A of the public-beta concurrency push moved CLI/MCP traffic onto
--   praxis-agentd so the API server stops being the coordination point.
--   Phase B types the operations themselves: every gateway-dispatched
--   operation declares the *concurrency lane* it runs in.
--
--     * `interactive`  — caller waits for the answer in a single round-trip.
--                        The gateway enforces a hard deadline at `timeout_ms`
--                        on this lane. Suitable for query-side observe ops
--                        and small command/operate ops.
--     * `background`   — caller does not wait. Either the handler returns
--                        a kickoff/run handle or the operation is consumed
--                        by a worker. The gateway does not enforce caller-
--                        side timeouts here; the work happens out of band.
--     * `system`       — runtime/control-plane operations invoked by Praxis
--                        itself (catalog reads, heartbeat housekeeping,
--                        bootstrap registrations). No caller-side enforcement.
--
--   `kickoff_required = TRUE` is the defense-in-depth backstop: even if a
--   caller asks the gateway to dispatch a `background` op synchronously
--   from an interactive transport (cli/mcp/http), the gateway refuses.
--   The work has to come in through a worker/runtime lane that knows to
--   accept a run_id handoff.
--
--   The 600s provider call inside the UI compose path (the bug that
--   pinned the API for an entire synchronous window) is exactly the
--   shape this prevents from recurring: that operation should be
--   `background` + `kickoff_required=TRUE`, and any future agent that
--   writes a sibling handler will inherit the same enforcement by being
--   in the same column.
--
-- Defaults preserve existing behavior
--   `execution_lane DEFAULT 'background'` and `kickoff_required DEFAULT FALSE`
--   means existing operations roll forward without any classification work
--   and the gateway's new enforcement only activates once an operation is
--   explicitly re-classified to `interactive` or marked `kickoff_required`.
--   Per-operation re-classification is a follow-up packet — auditing all
--   ~150 registered operations and writing a dry-classification report
--   before flipping the trigger is itself a bounded delivery.
--
-- Idempotency
--   ADD COLUMN IF NOT EXISTS, ADD CONSTRAINT NOT VALID + VALIDATE,
--   ON CONFLICT DO NOTHING for the operator_decisions registration.
--
-- PG: `storage/postgres/schema.py` bootstrap runs each migration in one outer
-- transaction (BEGIN/COMMIT in this file are skipped as wrappers). Deferred
-- constraint triggers on operation_catalog_registry (203) can leave pending
-- trigger events; flush with SET CONSTRAINTS so stacked ALTERs succeed.

SET CONSTRAINTS ALL IMMEDIATE;

ALTER TABLE operation_catalog_registry
    ADD COLUMN IF NOT EXISTS execution_lane TEXT NOT NULL DEFAULT 'background';

SET CONSTRAINTS ALL IMMEDIATE;

ALTER TABLE operation_catalog_registry
    DROP CONSTRAINT IF EXISTS operation_catalog_registry_execution_lane_check;

SET CONSTRAINTS ALL IMMEDIATE;

ALTER TABLE operation_catalog_registry
    ADD CONSTRAINT operation_catalog_registry_execution_lane_check
        CHECK (execution_lane IN ('interactive', 'background', 'system')) NOT VALID;

SET CONSTRAINTS ALL IMMEDIATE;

ALTER TABLE operation_catalog_registry
    VALIDATE CONSTRAINT operation_catalog_registry_execution_lane_check;

SET CONSTRAINTS ALL IMMEDIATE;

ALTER TABLE operation_catalog_registry
    ADD COLUMN IF NOT EXISTS kickoff_required BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS operation_catalog_registry_execution_lane_idx
    ON operation_catalog_registry (execution_lane, kickoff_required);

COMMENT ON COLUMN operation_catalog_registry.execution_lane IS
    'Concurrency lane for the dispatcher. ''interactive'' enforces a hard '
    'deadline at timeout_ms on caller-facing dispatches. ''background'' '
    'expects the handler to return a kickoff handle or be consumed by a '
    'worker; no caller-side timeout. ''system'' is for runtime/control '
    'operations (catalog reads, heartbeat). Default ''background'' '
    'preserves prior behavior; per-operation re-classification is a '
    'follow-up.';

COMMENT ON COLUMN operation_catalog_registry.kickoff_required IS
    'Defense-in-depth: when TRUE, the gateway rejects direct synchronous '
    'dispatch of this operation from interactive transports (cli/mcp/http). '
    'Worker/runtime lanes (workflow/heartbeat/internal/sandbox) still accept '
    'the call. Use this for handlers whose work cannot fit inside any '
    'reasonable interactive timeout no matter how large the cap.';

INSERT INTO operator_decisions (
    operator_decision_id,
    decision_key,
    decision_kind,
    decision_status,
    title,
    rationale,
    decided_by,
    decision_source,
    effective_from,
    effective_to,
    decided_at,
    created_at,
    updated_at,
    decision_scope_kind,
    decision_scope_ref,
    scope_clamp
) VALUES (
    'operator_decision.architecture_policy.concurrency.operation_execution_lane_typing',
    'architecture-policy::concurrency::operation-execution-lane-typing',
    'architecture_policy',
    'decided',
    'Operation execution lane (interactive | background | system) + kickoff_required',
    'Every CQRS-gateway operation declares the concurrency lane it runs in. '
    '''interactive'' caps caller-facing dispatch at timeout_ms; ''background'' '
    'returns kickoff handles or is consumed by workers; ''system'' is runtime '
    'control. kickoff_required=TRUE rejects direct synchronous dispatch from '
    'interactive transports (cli/mcp/http) — work must come in through a '
    'worker/runtime lane. Defaults preserve prior behavior; re-classification '
    'is a follow-up packet. Migration 348 introduces the schema and gateway '
    'enforcement; the praxis-agentd broker (Phase A) provides the host-creds-'
    'aware transport that will surface this typing to agents.',
    'praxis',
    'migration_348_operation_execution_lane',
    NOW(),
    NULL,
    NOW(),
    NOW(),
    NOW(),
    'authority_domain',
    'concurrency::gateway_operations',
    '{"applies_to":["operation_catalog_registry","gateway.dispatch"],"does_not_apply_to":[]}'::jsonb
)
ON CONFLICT (decision_key) DO NOTHING;
