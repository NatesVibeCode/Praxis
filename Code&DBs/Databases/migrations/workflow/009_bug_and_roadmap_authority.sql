-- Canonical bug and roadmap authority tables.

CREATE TABLE bugs (
    bug_id text PRIMARY KEY,
    bug_key text NOT NULL,
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
    CONSTRAINT bugs_bug_key_key UNIQUE (bug_key),
    CONSTRAINT bugs_discovered_in_run_fkey
        FOREIGN KEY (discovered_in_run_id)
        REFERENCES workflow_runs (run_id)
        ON DELETE SET NULL,
    CONSTRAINT bugs_discovered_in_receipt_fkey
        FOREIGN KEY (discovered_in_receipt_id)
        REFERENCES receipts (receipt_id)
        ON DELETE SET NULL,
    CONSTRAINT bugs_resolution_window
        CHECK (resolved_at IS NULL OR resolved_at >= opened_at)
);

CREATE INDEX bugs_status_severity_opened_at_idx
    ON bugs (status, severity, opened_at DESC);

CREATE INDEX bugs_discovered_in_run_idx
    ON bugs (discovered_in_run_id);

CREATE INDEX bugs_discovered_in_receipt_idx
    ON bugs (discovered_in_receipt_id);

COMMENT ON TABLE bugs IS 'Canonical operator-owned bug backlog records over native runtime and evidence. Owned by surfaces/.';
COMMENT ON COLUMN bugs.summary IS 'Durable bug description. Do not replace this with markdown-only backlog notes or shell-managed issue lists.';

CREATE TABLE bug_evidence_links (
    bug_evidence_link_id text PRIMARY KEY,
    bug_id text NOT NULL,
    evidence_kind text NOT NULL,
    evidence_ref text NOT NULL,
    evidence_role text NOT NULL,
    created_at timestamptz NOT NULL,
    created_by text NOT NULL,
    notes text,
    CONSTRAINT bug_evidence_links_bug_fkey
        FOREIGN KEY (bug_id)
        REFERENCES bugs (bug_id)
        ON DELETE CASCADE,
    CONSTRAINT bug_evidence_links_unique_evidence
        UNIQUE (bug_id, evidence_kind, evidence_ref, evidence_role)
);

CREATE INDEX bug_evidence_links_bug_created_at_idx
    ON bug_evidence_links (bug_id, created_at DESC);

CREATE INDEX bug_evidence_links_kind_ref_idx
    ON bug_evidence_links (evidence_kind, evidence_ref);

COMMENT ON TABLE bug_evidence_links IS 'Canonical links from bugs to receipts, events, runs, or artifacts that justify discovery or resolution. Owned by surfaces/.';
COMMENT ON COLUMN bug_evidence_links.evidence_role IS 'Examples: observed_in, reproduces, validates_fix. Keep evidence linkage explicit instead of hiding it in prose.';

CREATE TABLE roadmap_items (
    roadmap_item_id text PRIMARY KEY,
    roadmap_key text NOT NULL,
    title text NOT NULL,
    item_kind text NOT NULL,
    status text NOT NULL,
    lifecycle text NOT NULL DEFAULT 'planned',
    priority text NOT NULL,
    parent_roadmap_item_id text,
    source_bug_id text,
    summary text NOT NULL,
    acceptance_criteria jsonb NOT NULL,
    decision_ref text NOT NULL,
    target_start_at timestamptz,
    target_end_at timestamptz,
    completed_at timestamptz,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT roadmap_items_roadmap_key_key UNIQUE (roadmap_key),
    CONSTRAINT roadmap_items_parent_fkey
        FOREIGN KEY (parent_roadmap_item_id)
        REFERENCES roadmap_items (roadmap_item_id)
        ON DELETE SET NULL,
    CONSTRAINT roadmap_items_source_bug_fkey
        FOREIGN KEY (source_bug_id)
        REFERENCES bugs (bug_id)
        ON DELETE SET NULL,
    CONSTRAINT roadmap_items_lifecycle_check
        CHECK (lifecycle IN ('idea', 'planned', 'claimed', 'completed')),
    CONSTRAINT roadmap_items_target_window
        CHECK (
            target_start_at IS NULL
            OR target_end_at IS NULL
            OR target_end_at >= target_start_at
        )
);

CREATE INDEX roadmap_items_status_priority_target_end_idx
    ON roadmap_items (status, priority, target_end_at DESC);

CREATE INDEX roadmap_items_parent_idx
    ON roadmap_items (parent_roadmap_item_id);

CREATE INDEX roadmap_items_source_bug_idx
    ON roadmap_items (source_bug_id);

COMMENT ON TABLE roadmap_items IS 'Canonical roadmap backlog, capability, and initiative records for native operator planning. Owned by surfaces/.';
COMMENT ON COLUMN roadmap_items.lifecycle IS 'Explicit roadmap lifecycle from idea intake through claimed execution and completed closeout. Do not infer planning state only from bindings or acceptance JSON.';
COMMENT ON COLUMN roadmap_items.acceptance_criteria IS 'Structured acceptance contract for one roadmap item. Do not hide done criteria only in docs or queue prompts.';

CREATE TABLE roadmap_item_dependencies (
    roadmap_item_dependency_id text PRIMARY KEY,
    roadmap_item_id text NOT NULL,
    depends_on_roadmap_item_id text NOT NULL,
    dependency_kind text NOT NULL,
    decision_ref text NOT NULL,
    created_at timestamptz NOT NULL,
    CONSTRAINT roadmap_item_dependencies_item_fkey
        FOREIGN KEY (roadmap_item_id)
        REFERENCES roadmap_items (roadmap_item_id)
        ON DELETE CASCADE,
    CONSTRAINT roadmap_item_dependencies_depends_on_fkey
        FOREIGN KEY (depends_on_roadmap_item_id)
        REFERENCES roadmap_items (roadmap_item_id)
        ON DELETE RESTRICT,
    CONSTRAINT roadmap_item_dependencies_not_self
        CHECK (roadmap_item_id <> depends_on_roadmap_item_id),
    CONSTRAINT roadmap_item_dependencies_unique_edge
        UNIQUE (roadmap_item_id, depends_on_roadmap_item_id, dependency_kind)
);

CREATE INDEX roadmap_item_dependencies_item_idx
    ON roadmap_item_dependencies (roadmap_item_id, created_at DESC);

CREATE INDEX roadmap_item_dependencies_depends_on_idx
    ON roadmap_item_dependencies (depends_on_roadmap_item_id, created_at DESC);

COMMENT ON TABLE roadmap_item_dependencies IS 'Canonical dependency edges between roadmap items. Owned by surfaces/.';
COMMENT ON COLUMN roadmap_item_dependencies.dependency_kind IS 'Examples: blocks, precedes, requires-evidence-from. Keep roadmap sequencing explicit instead of inferring it from docs.';
