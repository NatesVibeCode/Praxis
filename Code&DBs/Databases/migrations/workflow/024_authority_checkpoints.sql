CREATE TABLE IF NOT EXISTS authority_checkpoints (
    checkpoint_id TEXT PRIMARY KEY,
    card_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    authority_level TEXT NOT NULL DEFAULT 'autonomous',
    question TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    decided_by TEXT,
    decided_at TIMESTAMPTZ,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_checkpoints_model ON authority_checkpoints(model_id);
CREATE INDEX idx_checkpoints_status ON authority_checkpoints(status);
