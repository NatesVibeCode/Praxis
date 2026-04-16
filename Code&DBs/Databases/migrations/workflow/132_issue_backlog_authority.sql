-- Canonical upstream issue authority and issue-to-bug pipeline linkage.

CREATE TABLE issues (
    issue_id text PRIMARY KEY,
    issue_key text NOT NULL,
    title text NOT NULL,
    status text NOT NULL,
    severity text NOT NULL,
    priority text NOT NULL,
    summary text NOT NULL,
    source_kind text NOT NULL,
    discovered_in_run_id text,
    discovered_in_receipt_id text,
    owner_ref text,
    decision_ref text NOT NULL,
    resolution_summary text,
    opened_at timestamptz NOT NULL,
    resolved_at timestamptz,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT issues_issue_key_key UNIQUE (issue_key),
    CONSTRAINT issues_discovered_in_run_fkey
        FOREIGN KEY (discovered_in_run_id)
        REFERENCES workflow_runs (run_id)
        ON DELETE SET NULL,
    CONSTRAINT issues_discovered_in_receipt_fkey
        FOREIGN KEY (discovered_in_receipt_id)
        REFERENCES receipts (receipt_id)
        ON DELETE SET NULL,
    CONSTRAINT issues_resolution_window
        CHECK (resolved_at IS NULL OR resolved_at >= opened_at)
);

CREATE INDEX issues_status_priority_opened_at_idx
    ON issues (status, priority, opened_at DESC);

CREATE INDEX issues_discovered_in_run_idx
    ON issues (discovered_in_run_id);

CREATE INDEX issues_discovered_in_receipt_idx
    ON issues (discovered_in_receipt_id);

COMMENT ON TABLE issues IS 'Canonical upstream issue backlog records over operator-reported and system-reported work intake. Owned by surfaces/.';
COMMENT ON COLUMN issues.summary IS 'Durable issue description before bug or roadmap promotion. Do not hide intake scope only in chat or markdown notes.';

ALTER TABLE bugs
    ADD COLUMN IF NOT EXISTS source_issue_id text;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'bugs_source_issue_fkey'
    ) THEN
        ALTER TABLE bugs
            ADD CONSTRAINT bugs_source_issue_fkey
            FOREIGN KEY (source_issue_id)
            REFERENCES issues (issue_id)
            ON DELETE SET NULL;
    END IF;
END;
$$;

CREATE UNIQUE INDEX IF NOT EXISTS bugs_source_issue_uidx
    ON bugs (source_issue_id)
    WHERE source_issue_id IS NOT NULL;

ALTER TABLE work_item_workflow_bindings
    ADD COLUMN IF NOT EXISTS issue_id text;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'work_item_workflow_bindings_issue_fkey'
    ) THEN
        ALTER TABLE work_item_workflow_bindings
            ADD CONSTRAINT work_item_workflow_bindings_issue_fkey
            FOREIGN KEY (issue_id)
            REFERENCES issues (issue_id)
            ON DELETE CASCADE;
    END IF;
END;
$$;

ALTER TABLE work_item_workflow_bindings
    DROP CONSTRAINT IF EXISTS work_item_workflow_bindings_source_exactly_one;

ALTER TABLE work_item_workflow_bindings
    ADD CONSTRAINT work_item_workflow_bindings_source_exactly_one
    CHECK (
        ((issue_id IS NOT NULL)::integer +
         (roadmap_item_id IS NOT NULL)::integer +
         (bug_id IS NOT NULL)::integer +
         (cutover_gate_id IS NOT NULL)::integer) = 1
    );

ALTER TABLE work_item_workflow_bindings
    DROP CONSTRAINT IF EXISTS work_item_workflow_bindings_unique_edge;

ALTER TABLE work_item_workflow_bindings
    ADD CONSTRAINT work_item_workflow_bindings_unique_edge
    UNIQUE (
        binding_kind,
        issue_id,
        roadmap_item_id,
        bug_id,
        cutover_gate_id,
        workflow_class_id,
        schedule_definition_id,
        workflow_run_id
    );

CREATE INDEX IF NOT EXISTS work_item_workflow_bindings_issue_idx
    ON work_item_workflow_bindings (issue_id, created_at DESC);

COMMENT ON COLUMN bugs.source_issue_id IS 'Explicit upstream issue authority that promoted into this bug. Keep issue-to-bug lineage explicit instead of inferring it from chat history.';
COMMENT ON COLUMN work_item_workflow_bindings.issue_id IS 'Canonical issue source for one workflow binding when work starts before bug promotion.';
