-- Migration 330: register Anthropic Sonnet CLI as a compile-family fallback.
--
-- Why:
-- Task-scoped eligibility windows can only admit real routes. The compile-family
-- task windows for anthropic/claude-sonnet-4-6 were being recorded, but the
-- provider job catalog still had nothing to materialize because task_type_routing
-- contained no compile-family Anthropic CLI rows. That made compile look like a
-- policy problem when it was actually missing route authority.
--
-- Decision:
-- Add Sonnet CLI as a low-priority fallback for compile-family tasks. This keeps
-- the current API-first compile winners intact while making the Anthropic CLI lane
-- real, inspectable, and available when an operator explicitly admits it by task.

BEGIN;

INSERT INTO task_type_routing (
    task_type,
    transport_type,
    provider_slug,
    model_slug,
    permitted,
    rank,
    benchmark_score,
    benchmark_name,
    cost_per_m_tokens,
    rationale,
    route_tier,
    route_tier_rank,
    latency_class,
    latency_rank,
    reasoning_control,
    route_source,
    updated_at
) VALUES
    (
        'compile',
        'CLI',
        'anthropic',
        'claude-sonnet-4-6',
        TRUE,
        9,
        0,
        '',
        9.00,
        'Anthropic Sonnet CLI fallback for materialize compile. Kept below existing API-first compile winners so explicit task admission can enable it without stealing the default path.',
        'medium',
        4,
        'instant',
        3,
        '{"source":"migration.330","role":"fallback"}'::jsonb,
        'explicit',
        now()
    ),
    (
        'compile_synthesize',
        'CLI',
        'anthropic',
        'claude-sonnet-4-6',
        TRUE,
        9,
        0,
        '',
        9.00,
        'Anthropic Sonnet CLI fallback for compile_synthesize when task-scoped admission explicitly opens the lane.',
        'medium',
        4,
        'instant',
        3,
        '{"source":"migration.330","role":"fallback"}'::jsonb,
        'explicit',
        now()
    ),
    (
        'compile_pill_match',
        'CLI',
        'anthropic',
        'claude-sonnet-4-6',
        TRUE,
        9,
        0,
        '',
        9.00,
        'Anthropic Sonnet CLI fallback for compile_pill_match. Lower-ranked than the API specialist so explicit admission does not mutate the default picker winner.',
        'medium',
        4,
        'instant',
        3,
        '{"source":"migration.330","role":"fallback"}'::jsonb,
        'explicit',
        now()
    ),
    (
        'compile_author',
        'CLI',
        'anthropic',
        'claude-sonnet-4-6',
        TRUE,
        9,
        0,
        '',
        9.00,
        'Anthropic Sonnet CLI fallback for compile_author when the operator wants compile prose authored on the CLI lane.',
        'medium',
        4,
        'reasoning',
        4,
        '{"source":"migration.330","role":"fallback"}'::jsonb,
        'explicit',
        now()
    ),
    (
        'compile_finalize',
        'CLI',
        'anthropic',
        'claude-sonnet-4-6',
        TRUE,
        9,
        0,
        '',
        9.00,
        'Anthropic Sonnet CLI fallback for compile_finalize. Added so compile-family operator truth can project the lane when task-scoped eligibility admits it.',
        'medium',
        4,
        'instant',
        3,
        '{"source":"migration.330","role":"fallback"}'::jsonb,
        'explicit',
        now()
    )
ON CONFLICT (task_type, sub_task_type, provider_slug, model_slug, transport_type)
DO UPDATE SET
    permitted = EXCLUDED.permitted,
    rank = EXCLUDED.rank,
    benchmark_score = EXCLUDED.benchmark_score,
    benchmark_name = EXCLUDED.benchmark_name,
    cost_per_m_tokens = EXCLUDED.cost_per_m_tokens,
    rationale = EXCLUDED.rationale,
    route_tier = EXCLUDED.route_tier,
    route_tier_rank = EXCLUDED.route_tier_rank,
    latency_class = EXCLUDED.latency_class,
    latency_rank = EXCLUDED.latency_rank,
    reasoning_control = EXCLUDED.reasoning_control,
    route_source = EXCLUDED.route_source,
    updated_at = EXCLUDED.updated_at;

SELECT refresh_private_provider_job_catalog('praxis');
SELECT refresh_private_provider_job_catalog('scratch_agent');
SELECT refresh_private_provider_control_plane_snapshot('praxis');
SELECT refresh_private_provider_control_plane_snapshot('scratch_agent');

COMMIT;
