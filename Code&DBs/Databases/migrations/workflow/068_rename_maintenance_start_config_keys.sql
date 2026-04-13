BEGIN;

UPDATE maintenance_policies
SET config = jsonb_strip_nulls(
        (COALESCE(config, '{}'::jsonb)
            - 'dispatch_if_clean'
            - 'dispatch_max_attempts'
            - 'repeat_dispatch_seconds')
        || jsonb_build_object(
            'start_if_clean',
            COALESCE(
                config->'start_if_clean',
                config->'dispatch_if_clean',
                'false'::jsonb
            ),
            'start_max_attempts',
            COALESCE(
                config->'start_max_attempts',
                config->'dispatch_max_attempts',
                '2'::jsonb
            ),
            'repeat_start_seconds',
            COALESCE(
                config->'repeat_start_seconds',
                config->'repeat_dispatch_seconds',
                CASE
                    WHEN policy_key = 'system.maintenance_repair.auto' THEN '43200'::jsonb
                    ELSE '259200'::jsonb
                END
            )
        )
    ),
    updated_at = now()
WHERE policy_key IN ('system.maintenance_review.daily', 'system.maintenance_repair.auto')
  AND (
      config ? 'dispatch_if_clean'
      OR config ? 'dispatch_max_attempts'
      OR config ? 'repeat_dispatch_seconds'
      OR NOT (config ? 'start_if_clean')
      OR NOT (config ? 'start_max_attempts')
      OR NOT (config ? 'repeat_start_seconds')
  );

COMMIT;
