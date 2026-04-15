BEGIN;

ALTER TABLE workflow_runs
    ADD COLUMN IF NOT EXISTS adoption_key TEXT
    GENERATED ALWAYS AS (
        COALESCE(
            NULLIF(request_envelope->>'adoption_key', ''),
            NULLIF(request_envelope->'spec_snapshot'->>'queue_id', '')
        )
    ) STORED;

CREATE INDEX IF NOT EXISTS workflow_runs_workflow_id_adoption_key_requested_at_idx
    ON workflow_runs (workflow_id, adoption_key, requested_at DESC)
    WHERE adoption_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS workflow_chain_wave_dependencies (
    chain_id TEXT NOT NULL,
    wave_id TEXT NOT NULL,
    depends_on_wave_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, wave_id, depends_on_wave_id),
    CONSTRAINT workflow_chain_wave_dependencies_wave_fkey
        FOREIGN KEY (chain_id, wave_id)
        REFERENCES workflow_chain_waves (chain_id, wave_id)
        ON DELETE CASCADE,
    CONSTRAINT workflow_chain_wave_dependencies_depends_on_fkey
        FOREIGN KEY (chain_id, depends_on_wave_id)
        REFERENCES workflow_chain_waves (chain_id, wave_id)
        ON DELETE CASCADE,
    CONSTRAINT workflow_chain_wave_dependencies_no_self_edge_check
        CHECK (wave_id <> depends_on_wave_id)
);

CREATE INDEX IF NOT EXISTS workflow_chain_wave_dependencies_depends_on_idx
    ON workflow_chain_wave_dependencies (chain_id, depends_on_wave_id);

INSERT INTO workflow_chain_wave_dependencies (
    chain_id,
    wave_id,
    depends_on_wave_id
)
SELECT
    wave.chain_id,
    wave.wave_id,
    dependency.depends_on_wave_id
FROM workflow_chain_waves AS wave
JOIN workflow_chains AS chain_header
    ON chain_header.chain_id = wave.chain_id
CROSS JOIN LATERAL (
    SELECT dependency_value AS depends_on_wave_id
    FROM jsonb_array_elements_text(
        COALESCE(
            (
                SELECT wave_definition->'depends_on'
                FROM jsonb_array_elements(COALESCE(chain_header.definition->'waves', '[]'::jsonb)) AS wave_definition
                WHERE wave_definition->>'wave_id' = wave.wave_id
                LIMIT 1
            ),
            '[]'::jsonb
        )
    ) AS dependency_value
) AS dependency
ON CONFLICT DO NOTHING;

INSERT INTO workflow_chain_wave_dependencies (
    chain_id,
    wave_id,
    depends_on_wave_id
)
SELECT
    chain_id,
    wave_id,
    depends_on_wave_id
FROM workflow_chain_waves
WHERE depends_on_wave_id IS NOT NULL
ON CONFLICT DO NOTHING;

COMMIT;
