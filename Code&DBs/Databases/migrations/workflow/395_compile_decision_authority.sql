-- Migration 395: Register compile-decision taxonomy in data_dictionary_objects.
--
-- Foundation slice for the unified compile + Review front door (decision
-- filed at /Users/nate/.claude/plans/and-praxis-phase-to-plan-enchanted-
-- hanrahan.md). The future chat-model Review handler explains, per
-- packet, what the compiler picked and what runner-ups it had on hand.
-- Capturing alternatives at decision time is the *honest-Review*
-- invariant — the handler must never invent runner-ups after the fact.
--
-- This migration declares the typed authority that the runtime side
-- already populates via:
--   - runtime.compile.review_payload.CompileReviewPayload (Pydantic
--     contract; shape of the future Review handler's return value)
--   - LaunchReceipt.packet_map[i]['alternatives_considered'] (success
--     path; sourced from the per-packet ``compile_decisions`` list
--     attached in compile_plan)
--   - UnresolvedSourceRefError.unresolved_entries[i]['available_options']
--     and the ``available_options`` keys on UnresolvedStageError /
--     UnresolvedWriteScopeError entries (failure path)
--
-- Pattern: enum-as-rows mirroring migration 225 (one row per
-- decision_kind member); the umbrella row points at the Pydantic model
-- via ``metadata.pydantic_model_ref``. Idempotent ON CONFLICT mirrors
-- migration 205's pattern so re-running on a seeded DB updates label /
-- summary / metadata / updated_at without duplicating rows.

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
        'compile.decision.stage_resolution',
        'Stage resolution decision',
        'decision',
        'The stage the compiler admitted for a packet, recorded with the alternatives in _STAGE_TEMPLATES.',
        '{"source":"migration.395","phase":"compile_review_foundation"}'::jsonb,
        '{"decision_kind":"stage_resolution","alternatives_supported":true,"pydantic_model_ref":"runtime.compile.review_payload.CompileDecision"}'::jsonb
    ),
    (
        'compile.decision.write_scope_resolution',
        'Write scope resolution decision',
        'decision',
        'The file set the compiler locked in for a packet write envelope. Foundation stub: alternatives empty until a scope_resolver lane exposes runners-up.',
        '{"source":"migration.395","phase":"compile_review_foundation"}'::jsonb,
        '{"decision_kind":"write_scope_resolution","alternatives_supported":true,"pydantic_model_ref":"runtime.compile.review_payload.CompileDecision"}'::jsonb
    ),
    (
        'compile.decision.source_ref_resolution',
        'Source ref resolution decision',
        'decision',
        'The source-authority resolver picked for a source_ref prefix (BUG-, roadmap_item., idea., friction.).',
        '{"source":"migration.395","phase":"compile_review_foundation"}'::jsonb,
        '{"decision_kind":"source_ref_resolution","alternatives_supported":true,"pydantic_model_ref":"runtime.compile.review_payload.CompileDecision"}'::jsonb
    ),
    (
        'compile.decision.agent_selection',
        'Agent selection decision',
        'decision',
        'The agent the compiler bound to a packet via the stage template. Foundation stub: alternatives empty until the agent picker exposes its candidate set.',
        '{"source":"migration.395","phase":"compile_review_foundation"}'::jsonb,
        '{"decision_kind":"agent_selection","alternatives_supported":true,"pydantic_model_ref":"runtime.compile.review_payload.CompileDecision"}'::jsonb
    ),
    (
        'compile.decision.data_pill_binding',
        'Data pill binding decision',
        'decision',
        'How the compiler resolved an object.field reference from packet prose against data_dictionary_entries. Ambiguous matches surface as alternatives.',
        '{"source":"migration.395","phase":"compile_review_foundation"}'::jsonb,
        '{"decision_kind":"data_pill_binding","alternatives_supported":true,"pydantic_model_ref":"runtime.compile.review_payload.CompileDecision"}'::jsonb
    ),
    (
        'compile.decision.capability_binding',
        'Capability binding decision',
        'decision',
        'The capability_slug list the catalog bound to a packet. Foundation stub for the future capability picker.',
        '{"source":"migration.395","phase":"compile_review_foundation"}'::jsonb,
        '{"decision_kind":"capability_binding","alternatives_supported":true,"pydantic_model_ref":"runtime.compile.review_payload.CompileDecision"}'::jsonb
    ),
    (
        'compile.decision.verification_admission',
        'Verification admission decision',
        'decision',
        'The verifier ref the compiler admitted for a write_envelope file. Foundation stub for the future verifier picker.',
        '{"source":"migration.395","phase":"compile_review_foundation"}'::jsonb,
        '{"decision_kind":"verification_admission","alternatives_supported":true,"pydantic_model_ref":"runtime.compile.review_payload.CompileDecision"}'::jsonb
    ),
    (
        'compile.review.packet_decision_record',
        'Compile review packet decision record',
        'object_type',
        'Per-packet typed envelope returned by the future Review handler — list of CompileDecision rows plus unresolved_options for the failure-mode equivalent.',
        '{"source":"migration.395","phase":"compile_review_foundation"}'::jsonb,
        '{"pydantic_model_ref":"runtime.compile.review_payload.PacketDecisionRecord","sibling_payload_ref":"runtime.compile.review_payload.CompileReviewPayload"}'::jsonb
    )
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;
