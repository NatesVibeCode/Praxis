-- DB-backed read-model freshness SLA policy for health and orient surfaces.

BEGIN;

INSERT INTO platform_config (
    config_key,
    config_value,
    value_type,
    category,
    description,
    min_value,
    max_value
) VALUES
    (
        'observability.projection_freshness.warning_staleness_seconds',
        '300',
        'float',
        'observability',
        'Projection staleness in seconds before the health surface emits a warning alert.',
        30,
        3600
    ),
    (
        'observability.projection_freshness.critical_staleness_seconds',
        '900',
        'float',
        'observability',
        'Projection staleness in seconds before the health surface opens the read-side circuit-breaker verdict.',
        60,
        86400
    ),
    (
        'observability.projection_freshness.warning_lag_events',
        '0',
        'int',
        'observability',
        'Projection event lag above this count emits a warning alert.',
        0,
        10000
    ),
    (
        'observability.projection_freshness.critical_lag_events',
        '100',
        'int',
        'observability',
        'Projection event lag at or above this count opens the read-side circuit-breaker verdict.',
        1,
        1000000
    )
ON CONFLICT (config_key) DO UPDATE SET
    config_value = EXCLUDED.config_value,
    value_type = EXCLUDED.value_type,
    category = EXCLUDED.category,
    description = EXCLUDED.description,
    min_value = EXCLUDED.min_value,
    max_value = EXCLUDED.max_value,
    updated_at = now();

COMMIT;
