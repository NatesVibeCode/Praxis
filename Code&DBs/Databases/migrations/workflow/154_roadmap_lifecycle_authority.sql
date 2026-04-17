-- Canonical roadmap lifecycle authority for idea -> planned -> claimed -> completed.

ALTER TABLE roadmap_items
    ADD COLUMN IF NOT EXISTS lifecycle text;

UPDATE roadmap_items
SET lifecycle = CASE
    WHEN completed_at IS NOT NULL OR status IN ('completed', 'done') THEN 'completed'
    ELSE 'planned'
END
WHERE lifecycle IS NULL;

ALTER TABLE roadmap_items
    ALTER COLUMN lifecycle SET DEFAULT 'planned';

ALTER TABLE roadmap_items
    ALTER COLUMN lifecycle SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'roadmap_items_lifecycle_check'
    ) THEN
        ALTER TABLE roadmap_items
            ADD CONSTRAINT roadmap_items_lifecycle_check
            CHECK (lifecycle IN ('idea', 'planned', 'claimed', 'completed'));
    END IF;
END;
$$;

COMMENT ON COLUMN roadmap_items.lifecycle IS 'Explicit roadmap lifecycle from idea intake through claimed execution and completed closeout. Do not infer planning state only from bindings or acceptance JSON.';
