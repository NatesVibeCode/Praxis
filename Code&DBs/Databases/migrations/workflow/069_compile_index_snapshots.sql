BEGIN;

CREATE TABLE IF NOT EXISTS compile_index_snapshots (
    compile_index_ref TEXT PRIMARY KEY,
    compile_surface_revision TEXT NOT NULL,
    compile_surface_name TEXT NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    repo_root TEXT NOT NULL,
    repo_fingerprint TEXT NOT NULL,
    source_fingerprints JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload JSONB NOT NULL,
    decision_ref TEXT NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL,
    stale_after_at TIMESTAMPTZ NOT NULL,
    refresh_count INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT compile_index_snapshots_source_fingerprints_object_check
        CHECK (jsonb_typeof(source_fingerprints) = 'object'),
    CONSTRAINT compile_index_snapshots_source_counts_object_check
        CHECK (jsonb_typeof(source_counts) = 'object'),
    CONSTRAINT compile_index_snapshots_payload_object_check
        CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT compile_index_snapshots_refresh_count_check
        CHECK (refresh_count >= 1),
    CONSTRAINT compile_index_snapshots_freshness_window_check
        CHECK (stale_after_at > refreshed_at)
);

CREATE INDEX IF NOT EXISTS compile_index_snapshots_surface_name_refreshed_idx
    ON compile_index_snapshots (compile_surface_name, refreshed_at DESC, compile_surface_revision DESC);

CREATE INDEX IF NOT EXISTS compile_index_snapshots_surface_name_revision_idx
    ON compile_index_snapshots (compile_surface_name, compile_surface_revision);

CREATE INDEX IF NOT EXISTS compile_index_snapshots_repo_fingerprint_idx
    ON compile_index_snapshots (repo_fingerprint);

CREATE INDEX IF NOT EXISTS compile_index_snapshots_stale_after_idx
    ON compile_index_snapshots (stale_after_at);

COMMENT ON TABLE compile_index_snapshots IS 'Durable compile-index snapshot authority used by the online compiler hot path.';
COMMENT ON COLUMN compile_index_snapshots.compile_index_ref IS 'Content-addressed snapshot ref for the compiler authority snapshot.';
COMMENT ON COLUMN compile_index_snapshots.compile_surface_revision IS 'Revision-aware compiler surface ref derived from the snapshot payload.';
COMMENT ON COLUMN compile_index_snapshots.repo_fingerprint IS 'Current repo fingerprint captured when the snapshot was refreshed.';
COMMENT ON COLUMN compile_index_snapshots.stale_after_at IS 'Freshness cutoff used by online lookup to fail closed when the snapshot expires.';

COMMIT;
