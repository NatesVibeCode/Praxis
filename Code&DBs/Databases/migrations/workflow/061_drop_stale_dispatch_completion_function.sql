-- Remove the last stale dispatch-era completion function.
--
-- `dispatch_runs` was renamed to `workflow_runs`, and live trigger bindings
-- were already removed by 057. Leaving this function around creates confusing
-- drift because introspection still shows a callable object that targets the
-- dead dispatch_* table family.

DO $$
DECLARE
    legacy_completion_fn constant text := 'check_' || 'run_completion';
BEGIN
    EXECUTE format('DROP FUNCTION IF EXISTS public.%I()', legacy_completion_fn);
END;
$$;
