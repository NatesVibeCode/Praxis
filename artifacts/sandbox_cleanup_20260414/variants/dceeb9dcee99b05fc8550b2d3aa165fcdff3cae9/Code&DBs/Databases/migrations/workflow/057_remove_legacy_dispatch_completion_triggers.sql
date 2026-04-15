-- Remove legacy dispatch trigger paths from the live workflow tables.
--
-- The unified runtime now owns run-state recompute and terminal event emission.
-- Keeping legacy dispatch_* trigger functions on workflow_jobs/workflow_runs
-- causes current control-plane writes to call dead dispatch_* tables.

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

DROP TRIGGER IF EXISTS project_dispatch_result ON workflow_runs;
DROP FUNCTION IF EXISTS project_dispatch_result_trigger();
