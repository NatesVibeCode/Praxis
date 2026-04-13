-- File storage for step-level, workflow-level, and instance-wide context.

CREATE TABLE IF NOT EXISTS uploaded_files (
    id TEXT PRIMARY KEY,                          -- 'file_' + uuid hex
    filename TEXT NOT NULL,                      -- original filename
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    size_bytes BIGINT NOT NULL DEFAULT 0,
    storage_path TEXT NOT NULL,                  -- repo-relative path, e.g. artifacts/uploads/file_abc123.pdf

    -- Scoping: which tier this file belongs to
    scope TEXT NOT NULL DEFAULT 'instance',      -- 'step' | 'workflow' | 'instance'
    workflow_id TEXT REFERENCES workflows(id) ON DELETE CASCADE,
    step_id TEXT,

    -- Metadata
    description TEXT DEFAULT '',
    tags TEXT[] DEFAULT '{}',
    uploaded_by TEXT DEFAULT 'user',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Search
    search_vector tsvector GENERATED ALWAYS AS (
        to_tsvector('english', filename || ' ' || COALESCE(description, ''))
    ) STORED
);

CREATE INDEX IF NOT EXISTS idx_uploaded_files_scope
    ON uploaded_files (scope);

CREATE INDEX IF NOT EXISTS idx_uploaded_files_workflow
    ON uploaded_files (workflow_id)
    WHERE workflow_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_uploaded_files_step
    ON uploaded_files (workflow_id, step_id)
    WHERE step_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_uploaded_files_search
    ON uploaded_files USING gin(search_vector);

ALTER TABLE workflows
    ADD COLUMN IF NOT EXISTS context_files TEXT[] DEFAULT '{}';
