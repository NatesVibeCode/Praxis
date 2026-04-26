-- Migration 258: Register the five shell-navigation commands as
-- operation_catalog_registry rows + authority_event_contracts rows.
--
-- Brings shell navigation under the architecture-policy::platform-architecture::
-- conceptual-events-register-through-operation-catalog-registry standing order
-- (the same policy migration 236 closed for surface.action.performed).
--
-- Today every shell-nav action in App.tsx is a raw setState — no receipt,
-- no authority_events row. This migration registers:
--   - shell.surface.opened          → event surface.opened
--   - shell.tab.closed              → event tab.closed
--   - shell.draft.guard.consulted   → event draft.guard.consulted
--   - shell.history.popped          → event history.popped
--   - shell.session.bootstrapped    → event session.bootstrapped
--
-- Handlers live in runtime/operations/commands/shell_navigation_commands.py.
-- Uses register_operation_atomic (added migration 239, default-fixed in 240)
-- to collapse the 3-insert boilerplate per command.
--
-- Anchored to decision.shell_navigation_cqrs.20260426.

BEGIN;

-- 1. shell.surface.opened ---------------------------------------------------
SELECT register_operation_atomic(
    p_operation_ref           := 'shell-surface-opened',
    p_operation_name          := 'shell.surface.opened',
    p_handler_ref             := 'runtime.operations.commands.shell_navigation_commands.handle_shell_surface_opened',
    p_input_model_ref         := 'runtime.operations.commands.shell_navigation_commands.ShellSurfaceOpenedCommand',
    p_authority_domain_ref    := 'authority.surface_catalog',
    p_operation_kind          := 'command',
    p_http_path               := '/api/shell/surface/opened',
    p_event_type              := 'surface.opened',
    p_decision_ref            := 'decision.shell_navigation_cqrs.20260426',
    p_label                   := 'Command: shell.surface.opened',
    p_summary                 := 'Typed command fired when the React shell enters a surface (static tab activation, build/manifest/run-detail/manifest-editor/compose entry, or dashboard-detail drill-in). Records the route_id, slot bindings, and shell state diff.'
);

-- 2. shell.tab.closed -------------------------------------------------------
SELECT register_operation_atomic(
    p_operation_ref           := 'shell-tab-closed',
    p_operation_name          := 'shell.tab.closed',
    p_handler_ref             := 'runtime.operations.commands.shell_navigation_commands.handle_shell_tab_closed',
    p_input_model_ref         := 'runtime.operations.commands.shell_navigation_commands.ShellTabClosedCommand',
    p_authority_domain_ref    := 'authority.surface_catalog',
    p_operation_kind          := 'command',
    p_http_path               := '/api/shell/tab/closed',
    p_event_type              := 'tab.closed',
    p_decision_ref            := 'decision.shell_navigation_cqrs.20260426',
    p_label                   := 'Command: shell.tab.closed',
    p_summary                 := 'Typed command fired when a dynamic tab is closed in the React shell. Records the dynamic_tab_id and the fallback_route_id the shell will activate next.'
);

-- 3. shell.draft.guard.consulted -------------------------------------------
SELECT register_operation_atomic(
    p_operation_ref           := 'shell-draft-guard-consulted',
    p_operation_name          := 'shell.draft.guard.consulted',
    p_handler_ref             := 'runtime.operations.commands.shell_navigation_commands.handle_shell_draft_guard_consulted',
    p_input_model_ref         := 'runtime.operations.commands.shell_navigation_commands.ShellDraftGuardConsultedCommand',
    p_authority_domain_ref    := 'authority.surface_catalog',
    p_operation_kind          := 'command',
    p_http_path               := '/api/shell/draft-guard/consulted',
    p_event_type              := 'draft.guard.consulted',
    p_decision_ref            := 'decision.shell_navigation_cqrs.20260426',
    p_label                   := 'Command: shell.draft.guard.consulted',
    p_summary                 := 'Typed command recording the user decision (leave or stay) when the build-draft guard prompts on a dirty workflow. Analytic only — does not mutate projection state.'
);

-- 4. shell.history.popped ---------------------------------------------------
SELECT register_operation_atomic(
    p_operation_ref           := 'shell-history-popped',
    p_operation_name          := 'shell.history.popped',
    p_handler_ref             := 'runtime.operations.commands.shell_navigation_commands.handle_shell_history_popped',
    p_input_model_ref         := 'runtime.operations.commands.shell_navigation_commands.ShellHistoryPoppedCommand',
    p_authority_domain_ref    := 'authority.surface_catalog',
    p_operation_kind          := 'command',
    p_http_path               := '/api/shell/history/popped',
    p_event_type              := 'history.popped',
    p_decision_ref            := 'decision.shell_navigation_cqrs.20260426',
    p_label                   := 'Command: shell.history.popped',
    p_summary                 := 'Typed command recording back/forward navigation in the React shell. The follow-up shell.surface.opened fired by the popstate handler is what mutates projection state — this row preserves the cause.'
);

-- 5. shell.session.bootstrapped --------------------------------------------
SELECT register_operation_atomic(
    p_operation_ref           := 'shell-session-bootstrapped',
    p_operation_name          := 'shell.session.bootstrapped',
    p_handler_ref             := 'runtime.operations.commands.shell_navigation_commands.handle_shell_session_bootstrapped',
    p_input_model_ref         := 'runtime.operations.commands.shell_navigation_commands.ShellSessionBootstrappedCommand',
    p_authority_domain_ref    := 'authority.surface_catalog',
    p_operation_kind          := 'command',
    p_http_path               := '/api/shell/session/bootstrapped',
    p_event_type              := 'session.bootstrapped',
    p_decision_ref            := 'decision.shell_navigation_cqrs.20260426',
    p_label                   := 'Command: shell.session.bootstrapped',
    p_summary                 := 'Typed command fired once per browser-tab session. Initializes the per-tab session_aggregate_ref and applies any deep-link route from the initial URL.'
);

-- 6. Event contracts --------------------------------------------------------
-- One row per event_type. aggregate_ref_policy='entity_ref' (the
-- per-browser-tab session UUID is the entity); each event row's aggregate_ref
-- column reads from the session_aggregate_ref payload field so the projection
-- reducer can group by browser-tab session.

INSERT INTO authority_event_contracts (
    event_contract_ref,
    event_type,
    authority_domain_ref,
    payload_schema_ref,
    aggregate_ref_policy,
    reducer_refs,
    projection_refs,
    receipt_required,
    replay_policy,
    enabled,
    decision_ref,
    metadata
) VALUES
    (
        'event_contract.surface.opened',
        'surface.opened',
        'authority.surface_catalog',
        'runtime.operations.commands.shell_navigation_commands.ShellSurfaceOpenedCommand',
        'entity_ref',
        '["runtime.surface_projections.reduce_ui_shell_state"]'::jsonb,
        '["ui_shell_state.live"]'::jsonb,
        TRUE,
        'replayable',
        TRUE,
        'decision.shell_navigation_cqrs.20260426',
        jsonb_build_object(
            'source', 'migration.258_register_shell_navigation_commands',
            'payload_keys', jsonb_build_array(
                'session_aggregate_ref',
                'route_id',
                'slot_values',
                'shell_state_diff',
                'reason',
                'caller_ref'
            )
        )
    ),
    (
        'event_contract.tab.closed',
        'tab.closed',
        'authority.surface_catalog',
        'runtime.operations.commands.shell_navigation_commands.ShellTabClosedCommand',
        'entity_ref',
        '["runtime.surface_projections.reduce_ui_shell_state"]'::jsonb,
        '["ui_shell_state.live"]'::jsonb,
        TRUE,
        'replayable',
        TRUE,
        'decision.shell_navigation_cqrs.20260426',
        jsonb_build_object(
            'source', 'migration.258_register_shell_navigation_commands',
            'payload_keys', jsonb_build_array(
                'session_aggregate_ref',
                'dynamic_tab_id',
                'fallback_route_id',
                'caller_ref'
            )
        )
    ),
    (
        'event_contract.draft.guard.consulted',
        'draft.guard.consulted',
        'authority.surface_catalog',
        'runtime.operations.commands.shell_navigation_commands.ShellDraftGuardConsultedCommand',
        'entity_ref',
        '[]'::jsonb,
        '[]'::jsonb,
        TRUE,
        'replayable',
        TRUE,
        'decision.shell_navigation_cqrs.20260426',
        jsonb_build_object(
            'source', 'migration.258_register_shell_navigation_commands',
            'payload_keys', jsonb_build_array(
                'session_aggregate_ref',
                'decision',
                'source_route_id',
                'target_route_id',
                'draft_message',
                'caller_ref'
            )
        )
    ),
    (
        'event_contract.history.popped',
        'history.popped',
        'authority.surface_catalog',
        'runtime.operations.commands.shell_navigation_commands.ShellHistoryPoppedCommand',
        'entity_ref',
        '[]'::jsonb,
        '[]'::jsonb,
        TRUE,
        'replayable',
        TRUE,
        'decision.shell_navigation_cqrs.20260426',
        jsonb_build_object(
            'source', 'migration.258_register_shell_navigation_commands',
            'payload_keys', jsonb_build_array(
                'session_aggregate_ref',
                'target_route_id',
                'slot_values',
                'caller_ref'
            )
        )
    ),
    (
        'event_contract.session.bootstrapped',
        'session.bootstrapped',
        'authority.surface_catalog',
        'runtime.operations.commands.shell_navigation_commands.ShellSessionBootstrappedCommand',
        'entity_ref',
        '["runtime.surface_projections.reduce_ui_shell_state"]'::jsonb,
        '["ui_shell_state.live"]'::jsonb,
        TRUE,
        'replayable',
        TRUE,
        'decision.shell_navigation_cqrs.20260426',
        jsonb_build_object(
            'source', 'migration.258_register_shell_navigation_commands',
            'payload_keys', jsonb_build_array(
                'session_aggregate_ref',
                'initial_route_id',
                'deep_link_search'
            )
        )
    )
ON CONFLICT (authority_domain_ref, event_type) DO UPDATE SET
    payload_schema_ref   = EXCLUDED.payload_schema_ref,
    aggregate_ref_policy = EXCLUDED.aggregate_ref_policy,
    reducer_refs         = EXCLUDED.reducer_refs,
    projection_refs      = EXCLUDED.projection_refs,
    receipt_required     = EXCLUDED.receipt_required,
    replay_policy        = EXCLUDED.replay_policy,
    enabled              = EXCLUDED.enabled,
    decision_ref         = EXCLUDED.decision_ref,
    metadata             = EXCLUDED.metadata,
    updated_at           = now();

-- 7. Event-side data_dictionary_objects -------------------------------------
-- register_operation_atomic creates the COMMAND-side dictionary entries.
-- The matching EVENT-side entries (so the dictionary surfaces both sides)
-- are added explicitly here.

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'event.surface.opened',
        'Event: shell surface opened',
        'event',
        'Fires when the React shell enters a surface. Carries route_id + slot_values + shell_state_diff. Reduces into ui_shell_state.live.',
        jsonb_build_object('source', 'migration.258_register_shell_navigation_commands', 'event_contract_ref', 'event_contract.surface.opened'),
        jsonb_build_object('authority_domain_ref', 'authority.surface_catalog', 'aggregate_ref_policy', 'session_aggregate_ref')
    ),
    (
        'event.tab.closed',
        'Event: shell dynamic tab closed',
        'event',
        'Fires when a dynamic tab is closed in the React shell. Reduces into ui_shell_state.live.',
        jsonb_build_object('source', 'migration.258_register_shell_navigation_commands', 'event_contract_ref', 'event_contract.tab.closed'),
        jsonb_build_object('authority_domain_ref', 'authority.surface_catalog', 'aggregate_ref_policy', 'session_aggregate_ref')
    ),
    (
        'event.draft.guard.consulted',
        'Event: build draft guard consulted',
        'event',
        'Fires when the build-draft guard prompts the user to confirm leaving a dirty workflow. Records the leave/stay decision. Analytic only — no projection update.',
        jsonb_build_object('source', 'migration.258_register_shell_navigation_commands', 'event_contract_ref', 'event_contract.draft.guard.consulted'),
        jsonb_build_object('authority_domain_ref', 'authority.surface_catalog', 'aggregate_ref_policy', 'session_aggregate_ref')
    ),
    (
        'event.history.popped',
        'Event: shell history navigation popped',
        'event',
        'Fires when the user uses browser back/forward to navigate the shell. Cause-of-change record; the follow-up surface.opened mutates state.',
        jsonb_build_object('source', 'migration.258_register_shell_navigation_commands', 'event_contract_ref', 'event_contract.history.popped'),
        jsonb_build_object('authority_domain_ref', 'authority.surface_catalog', 'aggregate_ref_policy', 'session_aggregate_ref')
    ),
    (
        'event.session.bootstrapped',
        'Event: shell session bootstrapped',
        'event',
        'Fires once per browser-tab session when the React shell first mounts. Initializes the per-tab session_aggregate_ref aggregate in ui_shell_state.live.',
        jsonb_build_object('source', 'migration.258_register_shell_navigation_commands', 'event_contract_ref', 'event_contract.session.bootstrapped'),
        jsonb_build_object('authority_domain_ref', 'authority.surface_catalog', 'aggregate_ref_policy', 'session_aggregate_ref')
    )
ON CONFLICT (object_kind) DO UPDATE SET
    label      = EXCLUDED.label,
    category   = EXCLUDED.category,
    summary    = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata   = EXCLUDED.metadata,
    updated_at = now();

COMMIT;
