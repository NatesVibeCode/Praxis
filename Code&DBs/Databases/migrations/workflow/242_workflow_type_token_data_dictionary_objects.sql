-- Migration 242: Register workflow type tokens as data_dictionary_objects.
--
-- BUG-5DD67C2A — Moon-generated nodes leave data dictionary, bindings, and
-- imports empty. Phase 2.1.A/B/C made compose phases, definition graph
-- nodes, and runtime jobs carry typed consumes/produces, but the type
-- tokens themselves (code_change, diff, execution_receipt, research_findings,
-- etc) were never registered as data_dictionary_objects. So when typed
-- nodes tried to bind to their produced types, the bindings came back
-- empty — no row to point at.
--
-- This migration registers every type token referenced by
-- ``runtime.workflow_type_contracts._ROUTE_CONTRACTS`` plus the canonical
-- input types. Once these exist, the lineage projector can emit produces /
-- consumes edges from typed nodes to the registered types, Moon Composer
-- bindings populate from the typed contract, and the type-flow validator
-- has concrete entities to reason over.
--
-- Uses category 'object_type' (the only category permitted by the existing
-- check constraint that fits — workflow type tokens are object types in
-- the data-dictionary sense, just at a different scope than entity tables).

BEGIN;

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    -- Trigger / input types
    ('input_text', 'Input text', 'object_type',
     'Free-text input that seeded the workflow run. Producer of trigger* routes; consumer for nearly every downstream stage.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":[],"producer_of":["trigger","trigger/webhook","trigger/schedule"]}'::jsonb),
    ('trigger_event', 'Trigger event', 'object_type',
     'A trigger fire event with kind + payload. Produced by every trigger route.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":[],"producer_of":["trigger","trigger/webhook","trigger/schedule"]}'::jsonb),
    ('webhook_payload', 'Webhook payload', 'object_type',
     'HTTP request body forwarded by a webhook trigger.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":[],"producer_of":["trigger/webhook"]}'::jsonb),
    ('schedule_tick', 'Schedule tick', 'object_type',
     'Cron / schedule firing event with timestamp.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":[],"producer_of":["trigger/schedule"]}'::jsonb),
    ('validated_input', 'Validated input', 'object_type',
     'Input that has passed an explicit validation step.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":["analyze","draft","build"],"producer_of":["validate"]}'::jsonb),

    -- Research / analysis chain
    ('research_findings', 'Research findings', 'object_type',
     'Structured findings from a research / search / docs-gather route.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":["analyze","draft","build","review","debug","architecture","notify"],"producer_of":["research"]}'::jsonb),
    ('evidence_pack', 'Evidence pack', 'object_type',
     'Citations + supporting context produced alongside research_findings.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":["analyze","draft","build","review","debug","notify"],"producer_of":["research"]}'::jsonb),
    ('analysis_result', 'Analysis result', 'object_type',
     'Output of an analyze / classify / score / triage route.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":["draft","build","review","architecture","notify"],"producer_of":["analyze"]}'::jsonb),

    -- Authoring chain
    ('draft', 'Draft', 'object_type',
     'Authored draft / summary from a write / compose / creative route.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":["review","build","notify"],"producer_of":["draft"]}'::jsonb),
    ('summary', 'Summary', 'object_type',
     'Compressed restatement produced by summarize routes.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":["draft","review","notify"],"producer_of":["draft","summarize"]}'::jsonb),

    -- Build chain
    ('code_change', 'Code change', 'object_type',
     'A code edit emitted by a build / implement / refactor route. Includes write_scope and intended diff.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":["review"],"producer_of":["build","implement","develop"]}'::jsonb),
    ('diff', 'Diff', 'object_type',
     'Text diff produced by a build / refactor route. Pairs with code_change.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":["review"],"producer_of":["build","refactor"]}'::jsonb),
    ('execution_receipt', 'Execution receipt', 'object_type',
     'Receipt of a build / fix / refactor / stage execution: ran-what, exit-code, artifact-refs.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":["notify"],"producer_of":["build","stage","execute"]}'::jsonb),

    -- Review chain
    ('review_result', 'Review result', 'object_type',
     'Verdict + findings from a review / audit / check route.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":["build","notify"],"producer_of":["review"]}'::jsonb),

    -- Diagnose / architecture chains
    ('diagnosis', 'Diagnosis', 'object_type',
     'Failure diagnosis produced by a debug / failure-analysis route.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":["build"],"producer_of":["debug","diagnose"]}'::jsonb),
    ('failure', 'Failure', 'object_type',
     'Observed failure / error event consumed by debug routes.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":["debug"],"producer_of":[]}'::jsonb),
    ('error', 'Error', 'object_type',
     'Observed runtime error. Consumed by debug routes alongside failure.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":["debug"],"producer_of":[]}'::jsonb),
    ('architecture_plan', 'Architecture plan', 'object_type',
     'Output of an architecture / design / plan route.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":["build"],"producer_of":["architecture","design","plan"]}'::jsonb),
    ('requirements', 'Requirements', 'object_type',
     'Captured requirements consumed by architecture / design routes.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":["architecture"],"producer_of":[]}'::jsonb),

    -- Fanout / loop
    ('parallel_results', 'Parallel results', 'object_type',
     'Aggregated output of a fanout / loop / map route.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":[],"producer_of":["fanout","loop","map"]}'::jsonb),
    ('item_list', 'Item list', 'object_type',
     'List of items consumed by a fanout / map route.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":["fanout","loop","map"],"producer_of":[]}'::jsonb),

    -- Notify / outbound action chain
    ('action_receipt', 'Action receipt', 'object_type',
     'Receipt of an outbound action (notify / send / github / webhook).',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":[],"producer_of":["notify","send"]}'::jsonb),
    ('notification_status', 'Notification status', 'object_type',
     'Delivery status (sent / failed / queued) for an outbound notification.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":[],"producer_of":["notify"]}'::jsonb),

    -- Generic / default fallback
    ('result', 'Result', 'object_type',
     'Generic result token used as the fallback produces type when a route does not match a more specific contract row.',
     '{"source":"migration.242","phase":"2.1.D"}'::jsonb,
     '{"consumer_of":[],"producer_of":[]}'::jsonb)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;

-- Verification (run manually):
--   SELECT object_kind FROM data_dictionary_objects WHERE category='object_type' ORDER BY object_kind;
--     -> 23 rows: input_text, trigger_event, webhook_payload, schedule_tick,
--        validated_input, research_findings, evidence_pack, analysis_result,
--        draft, summary, code_change, diff, execution_receipt, review_result,
--        diagnosis, failure, error, architecture_plan, requirements,
--        parallel_results, item_list, action_receipt, notification_status,
--        result
