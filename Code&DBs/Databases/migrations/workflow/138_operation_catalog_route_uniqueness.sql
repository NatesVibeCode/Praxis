BEGIN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM public.operation_catalog_registry
         GROUP BY http_method, http_path
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION
            'operation_catalog_registry contains duplicate http_method/http_path bindings';
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'operation_catalog_registry_method_path_unique'
    ) THEN
        ALTER TABLE public.operation_catalog_registry
            ADD CONSTRAINT operation_catalog_registry_method_path_unique
            UNIQUE (http_method, http_path);
    END IF;
END
$$;

COMMIT;
