-- Migration 396: Rename compile.* decision-authority rows to materialize.*.
--
-- Migration 395 (one ahead in this same session) registered the Foundation
-- slice taxonomy under ``compile.decision.*`` + ``compile.review.*`` keys.
-- "Compile" was retired same-session because it conflates with the
-- standard tech term (source → bytecode); the Praxis verb is
-- "materialize" — turning intent into graph rows.
--
-- This migration:
--   1. INSERTs eight ``materialize.*`` rows with the same shape as 395 but
--      pointing at the renamed Pydantic contracts in
--      ``runtime.materialize.review_payload``.
--   2. DELETEs the eight ``compile.*`` rows registered by 395.
--
-- Idempotent ON CONFLICT mirrors migration 205's pattern. Migration 395
-- itself stays in the manifest (append-only history); its rows are
-- superseded here.
--
-- The lane discriminator on MaterializeReviewPayload also collapsed from
-- ``Literal["compile","materialize","build_manifest"]`` to
-- ``Literal["auto","manifest"]``, mapping cleanly to the two UI buttons:
--   - ``auto``     → "Materialize it for me" (LLM does it; one-shot)
--   - ``manifest`` → "Build the Manifest" (user authors the scaffold)

BEGIN;

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'materialize.decision.stage_resolution',
        'Stage resolution decision',
        'decision',
        'The stage the materializer admitted for a packet, recorded with the alternatives in _STAGE_TEMPLATES.',
        '{"source":"migration.396","phase":"materialize_review_foundation","supersedes":"compile.decision.stage_resolution"}'::jsonb,
        '{"decision_kind":"stage_resolution","alternatives_supported":true,"pydantic_model_ref":"runtime.materialize.review_payload.MaterializeDecision"}'::jsonb
    ),
    (
        'materialize.decision.write_scope_resolution',
        'Write scope resolution decision',
        'decision',
        'The file set the materializer locked in for a packet write envelope. Foundation stub: alternatives empty until a scope_resolver lane exposes runners-up.',
        '{"source":"migration.396","phase":"materialize_review_foundation","supersedes":"compile.decision.write_scope_resolution"}'::jsonb,
        '{"decision_kind":"write_scope_resolution","alternatives_supported":true,"pydantic_model_ref":"runtime.materialize.review_payload.MaterializeDecision"}'::jsonb
    ),
    (
        'materialize.decision.source_ref_resolution',
        'Source ref resolution decision',
        'decision',
        'The source-authority resolver picked for a source_ref prefix (BUG-, roadmap_item., idea., friction.).',
        '{"source":"migration.396","phase":"materialize_review_foundation","supersedes":"compile.decision.source_ref_resolution"}'::jsonb,
        '{"decision_kind":"source_ref_resolution","alternatives_supported":true,"pydantic_model_ref":"runtime.materialize.review_payload.MaterializeDecision"}'::jsonb
    ),
    (
        'materialize.decision.agent_selection',
        'Agent selection decision',
        'decision',
        'The agent the materializer bound to a packet via the stage template. Foundation stub: alternatives empty until the agent picker exposes its candidate set.',
        '{"source":"migration.396","phase":"materialize_review_foundation","supersedes":"compile.decision.agent_selection"}'::jsonb,
        '{"decision_kind":"agent_selection","alternatives_supported":true,"pydantic_model_ref":"runtime.materialize.review_payload.MaterializeDecision"}'::jsonb
    ),
    (
        'materialize.decision.data_pill_binding',
        'Data pill binding decision',
        'decision',
        'How the materializer resolved an object.field reference from packet prose against data_dictionary_entries. Ambiguous matches surface as alternatives.',
        '{"source":"migration.396","phase":"materialize_review_foundation","supersedes":"compile.decision.data_pill_binding"}'::jsonb,
        '{"decision_kind":"data_pill_binding","alternatives_supported":true,"pydantic_model_ref":"runtime.materialize.review_payload.MaterializeDecision"}'::jsonb
    ),
    (
        'materialize.decision.capability_binding',
        'Capability binding decision',
        'decision',
        'The capability_slug list the catalog bound to a packet. Foundation stub for the future capability picker.',
        '{"source":"migration.396","phase":"materialize_review_foundation","supersedes":"compile.decision.capability_binding"}'::jsonb,
        '{"decision_kind":"capability_binding","alternatives_supported":true,"pydantic_model_ref":"runtime.materialize.review_payload.MaterializeDecision"}'::jsonb
    ),
    (
        'materialize.decision.verification_admission',
        'Verification admission decision',
        'decision',
        'The verifier ref the materializer admitted for a write_envelope file. Foundation stub for the future verifier picker.',
        '{"source":"migration.396","phase":"materialize_review_foundation","supersedes":"compile.decision.verification_admission"}'::jsonb,
        '{"decision_kind":"verification_admission","alternatives_supported":true,"pydantic_model_ref":"runtime.materialize.review_payload.MaterializeDecision"}'::jsonb
    ),
    (
        'materialize.review.packet_decision_record',
        'Materialize review packet decision record',
        'object_type',
        'Per-packet typed envelope returned by the future Review handler — list of MaterializeDecision rows plus unresolved_options for the failure-mode equivalent.',
        '{"source":"migration.396","phase":"materialize_review_foundation","supersedes":"compile.review.packet_decision_record"}'::jsonb,
        '{"pydantic_model_ref":"runtime.materialize.review_payload.PacketDecisionRecord","sibling_payload_ref":"runtime.materialize.review_payload.MaterializeReviewPayload","lane_literal":"auto|manifest"}'::jsonb
    )
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

DELETE FROM data_dictionary_objects
WHERE object_kind IN (
    'compile.decision.stage_resolution',
    'compile.decision.write_scope_resolution',
    'compile.decision.source_ref_resolution',
    'compile.decision.agent_selection',
    'compile.decision.data_pill_binding',
    'compile.decision.capability_binding',
    'compile.decision.verification_admission',
    'compile.review.packet_decision_record'
);

COMMIT;
