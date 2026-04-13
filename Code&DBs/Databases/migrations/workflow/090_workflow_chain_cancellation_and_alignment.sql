BEGIN;

ALTER TABLE workflow_chain_waves
    DROP CONSTRAINT IF EXISTS workflow_chain_waves_status_check;

ALTER TABLE workflow_chain_waves
    DROP CONSTRAINT IF EXISTS workflow_chain_waves_status_v2_check;

ALTER TABLE workflow_chain_waves
    ADD CONSTRAINT workflow_chain_waves_status_v2_check
    CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'blocked', 'cancelled'));

UPDATE workflow_chain_waves AS wave
SET depends_on_wave_id = dependency.depends_on_wave_id
FROM (
    SELECT
        chain_id,
        wave_id,
        CASE WHEN COUNT(*) = 1 THEN MIN(depends_on_wave_id) ELSE NULL END AS depends_on_wave_id
    FROM workflow_chain_wave_dependencies
    GROUP BY chain_id, wave_id
) AS dependency
WHERE dependency.chain_id = wave.chain_id
  AND dependency.wave_id = wave.wave_id
  AND wave.depends_on_wave_id IS DISTINCT FROM dependency.depends_on_wave_id;

UPDATE workflow_chain_waves AS wave
SET depends_on_wave_id = NULL
WHERE wave.depends_on_wave_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM workflow_chain_wave_dependencies AS dependency
      WHERE dependency.chain_id = wave.chain_id
        AND dependency.wave_id = wave.wave_id
  );

COMMIT;
