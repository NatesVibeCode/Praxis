BEGIN;

CREATE TABLE IF NOT EXISTS operator_ideas (
    idea_id text PRIMARY KEY,
    idea_key text NOT NULL,
    title text NOT NULL,
    status text NOT NULL,
    summary text NOT NULL,
    source_kind text NOT NULL,
    source_ref text,
    owner_ref text,
    decision_ref text NOT NULL,
    resolution_summary text,
    opened_at timestamptz NOT NULL,
    resolved_at timestamptz,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT operator_ideas_idea_key_key UNIQUE (idea_key),
    CONSTRAINT operator_ideas_status_check
        CHECK (status IN ('open', 'promoted', 'rejected', 'superseded', 'archived')),
    CONSTRAINT operator_ideas_resolution_window
        CHECK (resolved_at IS NULL OR resolved_at >= opened_at),
    CONSTRAINT operator_ideas_terminal_resolution_summary
        CHECK (
            status = 'open'
            OR (
                resolved_at IS NOT NULL
                AND resolution_summary IS NOT NULL
                AND length(btrim(resolution_summary)) > 0
            )
        )
);

CREATE INDEX IF NOT EXISTS operator_ideas_status_opened_at_idx
    ON operator_ideas (status, opened_at DESC);

CREATE INDEX IF NOT EXISTS operator_ideas_source_idx
    ON operator_ideas (source_kind, source_ref)
    WHERE source_ref IS NOT NULL;

COMMENT ON TABLE operator_ideas IS 'Canonical pre-commitment idea intake. Ideas may be promoted, rejected, superseded, or archived before roadmap commitment.';
COMMENT ON COLUMN operator_ideas.status IS 'Idea lifecycle: open, promoted, rejected, superseded, archived. Do not encode canceled roadmap work here; roadmap commitments do not get canceled.';
COMMENT ON COLUMN operator_ideas.decision_ref IS 'Decision or policy reference that justifies admitting or resolving this idea record.';

CREATE TABLE IF NOT EXISTS operator_idea_promotions (
    idea_promotion_id text PRIMARY KEY,
    idea_id text NOT NULL,
    roadmap_item_id text NOT NULL,
    decision_ref text NOT NULL,
    promoted_by text NOT NULL,
    promoted_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    CONSTRAINT operator_idea_promotions_idea_fkey
        FOREIGN KEY (idea_id)
        REFERENCES operator_ideas (idea_id)
        ON DELETE RESTRICT,
    CONSTRAINT operator_idea_promotions_roadmap_fkey
        FOREIGN KEY (roadmap_item_id)
        REFERENCES roadmap_items (roadmap_item_id)
        ON DELETE RESTRICT,
    CONSTRAINT operator_idea_promotions_unique_edge
        UNIQUE (idea_id, roadmap_item_id)
);

CREATE INDEX IF NOT EXISTS operator_idea_promotions_idea_idx
    ON operator_idea_promotions (idea_id, promoted_at DESC);

CREATE INDEX IF NOT EXISTS operator_idea_promotions_roadmap_idx
    ON operator_idea_promotions (roadmap_item_id, promoted_at DESC);

COMMENT ON TABLE operator_idea_promotions IS 'Canonical promotion evidence linking pre-commitment ideas to committed roadmap items.';
COMMENT ON COLUMN operator_idea_promotions.decision_ref IS 'Decision authority that justified promoting this idea into roadmap commitment.';

ALTER TABLE roadmap_items
    ADD COLUMN IF NOT EXISTS source_idea_id text;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'roadmap_items_source_idea_fkey'
    ) THEN
        ALTER TABLE roadmap_items
            ADD CONSTRAINT roadmap_items_source_idea_fkey
            FOREIGN KEY (source_idea_id)
            REFERENCES operator_ideas (idea_id)
            ON DELETE SET NULL;
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS roadmap_items_source_idea_idx
    ON roadmap_items (source_idea_id)
    WHERE source_idea_id IS NOT NULL;

COMMENT ON COLUMN roadmap_items.source_idea_id IS 'Explicit pre-commitment idea source that promoted into this roadmap item. Roadmap remains commitment authority; idea rejection/cancellation stays in operator_ideas.';

INSERT INTO operation_catalog_registry (
    operation_ref,
    operation_name,
    source_kind,
    operation_kind,
    http_method,
    http_path,
    input_model_ref,
    handler_ref,
    authority_ref,
    projection_ref,
    posture,
    idempotency_policy,
    enabled,
    binding_revision,
    decision_ref
) VALUES (
    'operator-ideas',
    'operator.ideas',
    'operation_command',
    'command',
    'POST',
    '/api/operator/ideas',
    'runtime.operations.commands.operator_control.OperatorIdeasCommand',
    'runtime.operations.commands.operator_control.handle_operator_ideas',
    'authority.operator_ideas',
    'projection.operator_ideas',
    NULL,
    NULL,
    TRUE,
    'binding.operation_catalog_registry.operator_ideas.20260421',
    'architecture-policy::operator-ideas::ideas-are-pre-commitment-roadmap-is-commitment'
)
ON CONFLICT (operation_ref) DO UPDATE SET
    operation_name = EXCLUDED.operation_name,
    source_kind = EXCLUDED.source_kind,
    operation_kind = EXCLUDED.operation_kind,
    http_method = EXCLUDED.http_method,
    http_path = EXCLUDED.http_path,
    input_model_ref = EXCLUDED.input_model_ref,
    handler_ref = EXCLUDED.handler_ref,
    authority_ref = EXCLUDED.authority_ref,
    projection_ref = EXCLUDED.projection_ref,
    posture = EXCLUDED.posture,
    idempotency_policy = EXCLUDED.idempotency_policy,
    enabled = EXCLUDED.enabled,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

COMMIT;
