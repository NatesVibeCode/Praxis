-- Migration 107: Rename DAG branding to Praxis
-- Cleans up lingering DAG-era branding in authority and registry tables.

-- ============================================================
-- 1. Integration registry branding cleanup
-- ============================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'integration_registry'
    ) THEN
        UPDATE integration_registry
        SET id = 'praxis-dispatch',
            name = 'Praxis Dispatch',
            description = REPLACE(description, 'DAG', 'Praxis'),
            provider = 'praxis'
        WHERE id = 'dag-dispatch';

        UPDATE integration_registry
        SET provider = 'praxis'
        WHERE id = 'notifications'
          AND provider = 'dag';
    END IF;
EXCEPTION
    WHEN undefined_table OR undefined_column THEN
        NULL;
END
$$;

-- ============================================================
-- 2. Runtime profile authority branding cleanup
-- ============================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'runtime_profile_authority'
    ) THEN
        UPDATE runtime_profile_authority
        SET runtime_profile_ref = 'praxis-project'
        WHERE runtime_profile_ref = 'dag-project';
    ELSIF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'registry_runtime_profile_authority'
    ) THEN
        UPDATE registry_runtime_profile_authority
        SET runtime_profile_ref = 'praxis-project'
        WHERE runtime_profile_ref = 'dag-project';
    END IF;
EXCEPTION
    WHEN undefined_table OR undefined_column THEN
        NULL;
END
$$;

-- ============================================================
-- 3. Model profile candidate binding branding cleanup
-- ============================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'model_profile_candidate_bindings'
          AND column_name = 'profile_ref'
    ) THEN
        UPDATE model_profile_candidate_bindings
        SET profile_ref = 'praxis-project'
        WHERE profile_ref = 'dag-project';
    END IF;
EXCEPTION
    WHEN undefined_table OR undefined_column THEN
        NULL;
END
$$;

-- ============================================================
-- 4. Compilation spine branding cleanup
-- ============================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'compilation_spine'
          AND column_name = 'spine_ref'
    ) THEN
        UPDATE compilation_spine
        SET spine_ref = 'praxis_research'
        WHERE spine_ref = 'dag_research';
    END IF;
EXCEPTION
    WHEN undefined_table OR undefined_column THEN
        NULL;
END
$$;

-- ============================================================
-- 5. Adapter config branding cleanup
-- ============================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'adapter_config'
          AND column_name = 'config_value'
    ) THEN
        UPDATE adapter_config
        SET config_value = '"Praxis-APITaskAdapter/1.0"'::jsonb
        WHERE config_value = '"DAG-APITaskAdapter/1.0"'::jsonb;
    END IF;
EXCEPTION
    WHEN undefined_table OR undefined_column THEN
        NULL;
END
$$;
