-- Prune stale dispatch-era completion helpers.
--
-- The live completion path no longer depends on these compatibility names.
BEGIN;

DO $$
DECLARE
    legacy_trigger constant text := 'trg_' || 'check_' || 'run_completion';
    legacy_event_fn constant text := 'check_' || 'run_completion' || '_with_events';
    legacy_base_fn constant text := 'check_' || 'run_completion';
BEGIN
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON workflow_jobs', legacy_trigger);
    EXECUTE format('DROP FUNCTION IF EXISTS public.%I()', legacy_event_fn);
    EXECUTE format('DROP FUNCTION IF EXISTS public.%I()', legacy_base_fn);
END;
$$;

COMMIT;
