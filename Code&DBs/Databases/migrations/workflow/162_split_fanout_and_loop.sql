-- 162_split_fanout_and_loop.sql
-- Fan Out and Loop are two distinct primitives in Praxis:
--   * Fan Out: count-based parallel burst (replicate: N). API provider only;
--     CLI adapters do not support the concurrency bursts used by Haiku-backed
--     research / architecture deep-dives and break down under load.
--   * Loop:    item-based for-each (replicate_with: [...]). Any provider;
--     one spec per item, dispatched in parallel or sequentially.
--
-- Both share the `run_workflow_parallel()` engine under the hood but carry
-- different compile-time constraints and different surface treatment.
--
-- Migration 115 seeded a single `think-fan-out` row (plus a legacy alias);
-- migration 057 seeded `cap-research-fan-out`. This migration splits those
-- into two independent catalog entries.

BEGIN;

-- Fan Out: restore the primary `think-fan-out` surface row to its canonical
-- values (idempotent — handles pre-existing seed state and any intermediate
-- drift from the earlier rename attempt).
INSERT INTO surface_catalog_registry (
    catalog_item_id, surface_name, label, icon, family, status, drop_kind,
    action_value, gate_family, description,
    truth_category, truth_badge, truth_detail,
    surface_tier, surface_badge, surface_detail, hard_choice,
    enabled, display_order, binding_revision, decision_ref
) VALUES (
    'think-fan-out',
    'moon',
    'Fan Out',
    'classify',
    'think',
    'ready',
    'node',
    'workflow.fanout',
    NULL,
    'Burst N parallel workers over an API provider (Haiku-class SLM dispatch)',
    'runtime',
    'Runs on release',
    'Fan-out compiles into parallel SLM dispatches bound to API providers; CLI adapters are rejected because they do not survive the concurrency burst.',
    'primary',
    'API only',
    'Count-based burst. Use when you want N parallel SLM workers against the same prompt template — e.g. 40 Haiku workers for broad research or architecture sweeps.',
    NULL,
    TRUE,
    80,
    'binding.surface_catalog_registry.moon.bootstrap.20260415',
    'decision.surface_catalog_registry.moon.bootstrap.20260415'
)
ON CONFLICT (catalog_item_id) DO UPDATE SET
    label = EXCLUDED.label,
    action_value = EXCLUDED.action_value,
    description = EXCLUDED.description,
    truth_detail = EXCLUDED.truth_detail,
    surface_badge = EXCLUDED.surface_badge,
    surface_detail = EXCLUDED.surface_detail,
    updated_at = now();

-- Loop: a new, separate surface row for item-based for-each.
INSERT INTO surface_catalog_registry (
    catalog_item_id, surface_name, label, icon, family, status, drop_kind,
    action_value, gate_family, description,
    truth_category, truth_badge, truth_detail,
    surface_tier, surface_badge, surface_detail, hard_choice,
    enabled, display_order, binding_revision, decision_ref
) VALUES (
    'think-loop',
    'moon',
    'Loop',
    'classify',
    'think',
    'ready',
    'node',
    'workflow.loop',
    NULL,
    'For each item in a list, run a step (item-based map)',
    'runtime',
    'Runs on release',
    'Loop compiles into one spec per item via replicate_with and dispatches them through the shared parallel runtime; any provider is allowed.',
    'primary',
    'Any provider',
    'Item-based map. Use when you have a list of distinct inputs and want to run the same step over each — e.g. per-lead research, per-URL scrape.',
    NULL,
    TRUE,
    81,
    'binding.surface_catalog_registry.moon.loop_split.20260418',
    'decision.surface_catalog_registry.moon.loop_split.20260418'
)
ON CONFLICT (catalog_item_id) DO UPDATE SET
    label = EXCLUDED.label,
    action_value = EXCLUDED.action_value,
    description = EXCLUDED.description,
    truth_detail = EXCLUDED.truth_detail,
    surface_badge = EXCLUDED.surface_badge,
    surface_detail = EXCLUDED.surface_detail,
    updated_at = now();

-- Retire the legacy `auto/fan-out` compatibility alias — the split makes it
-- ambiguous (fanout vs loop) and new surface rows cover both cases cleanly.
DELETE FROM surface_catalog_registry
WHERE catalog_item_id = 'think-fan-out-legacy';

-- Capability catalog: restore fanout capability to its canonical values.
INSERT INTO capability_catalog (
    capability_ref, capability_slug, capability_kind,
    title, summary, description,
    route, engines, signals, reference_slugs,
    enabled, binding_revision, decision_ref
) VALUES (
    'cap-research-fan-out',
    'research/fan-out',
    'fanout',
    'Parallel research fan-out (API burst)',
    'Burst N parallel Haiku workers over a research prompt.',
    'Count-based SLM burst via runtime fan_out dispatch. API providers only; CLI adapters are rejected because they break under concurrency bursts.',
    'workflow.fanout',
    '["fan_out_dispatch", "claude-haiku-4-5-20251001"]'::jsonb,
    '["parallel", "fan out", "burst", "haiku", "workers", "broad", "sweep"]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'binding.capability_catalog.bootstrap.20260408',
    'decision.capability_catalog.bootstrap.20260408'
)
ON CONFLICT (capability_ref) DO UPDATE SET
    capability_slug = EXCLUDED.capability_slug,
    capability_kind = EXCLUDED.capability_kind,
    title = EXCLUDED.title,
    summary = EXCLUDED.summary,
    description = EXCLUDED.description,
    route = EXCLUDED.route,
    engines = EXCLUDED.engines,
    signals = EXCLUDED.signals,
    updated_at = now();

-- Loop: new capability row for item-based for-each.
INSERT INTO capability_catalog (
    capability_ref, capability_slug, capability_kind,
    title, summary, description,
    route, engines, signals, reference_slugs,
    enabled, binding_revision, decision_ref
) VALUES (
    'cap-research-loop',
    'research/loop',
    'loop',
    'Research loop (item-based)',
    'Run a research step over each item in a list.',
    'Item-based parallel map via runtime loop dispatch. Any provider is allowed; one spec per item with templated prompt substitution.',
    'workflow.loop',
    '["loop_dispatch"]'::jsonb,
    '["for each", "loop", "iterate", "per item", "per lead", "per url", "map"]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'binding.capability_catalog.loop_split.20260418',
    'decision.capability_catalog.loop_split.20260418'
)
ON CONFLICT (capability_ref) DO UPDATE SET
    capability_slug = EXCLUDED.capability_slug,
    capability_kind = EXCLUDED.capability_kind,
    title = EXCLUDED.title,
    summary = EXCLUDED.summary,
    description = EXCLUDED.description,
    route = EXCLUDED.route,
    engines = EXCLUDED.engines,
    signals = EXCLUDED.signals,
    updated_at = now();

COMMIT;
