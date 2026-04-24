-- Migration 225: Register bug-lifecycle type slugs in data_dictionary_objects.
--
-- Context: Phase 1.3.a of the public beta ramp (decision
-- decision.2026-04-24.public-beta-ramp-master-plan). The MVP
-- type_contract dicts on praxis_bugs / praxis_replay_ready_bugs /
-- praxis_bug_replay_provenance_backfill reference slugs like
-- ``praxis.bug.record`` as the vocabulary for data flow between tool
-- actions. Per architecture-policy::platform-architecture::
-- data-dictionary-universal-compile-time-clamp, those slugs must be
-- first-class data_dictionary_objects rows so the data dictionary is the
-- one authority for type vocabulary (no parallel registries).
--
-- Category ``object_type`` marks these as conceptual data-flow types
-- (distinct from table-backed, integration-backed, or receipt-backed
-- kinds). metadata.consumer_of and metadata.producer_of point at the
-- tool-action slugs that respectively consume and produce each type —
-- reversing the dict-side mapping that lives on the tool TOOLS literals
-- so queries can walk in either direction.
--
-- Follow-up Phase 1.3.b will add compile-time validation that
-- type_contract slugs resolve to an existing row (typed gap otherwise),
-- per architecture-policy::platform-architecture::fail-closed-at-compile-
-- no-silent-defaults.

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
        'praxis.bug.observation', 'Bug observation', 'object_type',
        'Pre-record bug signal (title, description, severity hint) before a bug_id is assigned.',
        '{"source":"migration.225","phase":"1.3.a"}'::jsonb,
        '{"consumer_of":[],"producer_of":[]}'::jsonb
    ),
    (
        'praxis.bug.record', 'Bug record', 'object_type',
        'A filed bug with bug_id. Accumulator shape after praxis_bugs.file or list.',
        '{"source":"migration.225","phase":"1.3.a"}'::jsonb,
        '{"consumer_of":["praxis_bugs.packet","praxis_bugs.history","praxis_bugs.replay","praxis_bugs.attach_evidence","praxis_bugs.patch_resume","praxis_bugs.resolve"],"producer_of":["praxis_bugs.file","praxis_bugs.attach_evidence","praxis_bugs.patch_resume"]}'::jsonb
    ),
    (
        'praxis.bug.record_list', 'Bug record list', 'object_type',
        'List of bug records returned by list, search, or duplicate_check actions.',
        '{"source":"migration.225","phase":"1.3.a"}'::jsonb,
        '{"producer_of":["praxis_bugs.list","praxis_bugs.search","praxis_bugs.duplicate_check"]}'::jsonb
    ),
    (
        'praxis.bug.search_query', 'Bug search query', 'object_type',
        'Search/filter parameters for list, search, and duplicate_check.',
        '{"source":"migration.225","phase":"1.3.a"}'::jsonb,
        '{"consumer_of":["praxis_bugs.search","praxis_bugs.duplicate_check"]}'::jsonb
    ),
    (
        'praxis.bug.stats', 'Bug stats', 'object_type',
        'Aggregate stats output of praxis_bugs.stats.',
        '{"source":"migration.225","phase":"1.3.a"}'::jsonb,
        '{"producer_of":["praxis_bugs.stats"]}'::jsonb
    ),
    (
        'praxis.bug.packet', 'Bug investigation packet', 'object_type',
        'Investigation packet assembled by praxis_bugs.packet: rich context for a single bug.',
        '{"source":"migration.225","phase":"1.3.a"}'::jsonb,
        '{"producer_of":["praxis_bugs.packet"]}'::jsonb
    ),
    (
        'praxis.bug.history', 'Bug history', 'object_type',
        'Historical trail of a bug (receipts, resolutions, replays) from praxis_bugs.history.',
        '{"source":"migration.225","phase":"1.3.a"}'::jsonb,
        '{"producer_of":["praxis_bugs.history"]}'::jsonb
    ),
    (
        'praxis.bug.replay_run', 'Bug replay run', 'object_type',
        'Result of a replay action: replay receipt, run_id, replay outcome.',
        '{"source":"migration.225","phase":"1.3.a"}'::jsonb,
        '{"producer_of":["praxis_bugs.replay"]}'::jsonb
    ),
    (
        'praxis.bug.replay_backfill_result', 'Bug replay backfill result', 'object_type',
        'Result of a replay-provenance backfill: count of rows touched, affected bugs.',
        '{"source":"migration.225","phase":"1.3.a"}'::jsonb,
        '{"producer_of":["praxis_bugs.backfill_replay","praxis_bug_replay_provenance_backfill"]}'::jsonb
    ),
    (
        'praxis.bug.replay_ready_list', 'Replay-ready bug list', 'object_type',
        'Subset of bugs whose provenance is complete enough to replay.',
        '{"source":"migration.225","phase":"1.3.a"}'::jsonb,
        '{"producer_of":["praxis_replay_ready_bugs"]}'::jsonb
    ),
    (
        'praxis.bug.evidence_attachment', 'Bug evidence attachment', 'object_type',
        'Evidence linked to a bug (receipt_ref, run_ref, evidence_kind).',
        '{"source":"migration.225","phase":"1.3.a"}'::jsonb,
        '{"consumer_of":["praxis_bugs.attach_evidence"]}'::jsonb
    ),
    (
        'praxis.bug.resume_patch', 'Bug resume patch', 'object_type',
        'Patch applied via patch_resume to continue bug work (resume_context diff).',
        '{"source":"migration.225","phase":"1.3.a"}'::jsonb,
        '{"consumer_of":["praxis_bugs.patch_resume"]}'::jsonb
    ),
    (
        'praxis.bug.resolution_request', 'Bug resolution request', 'object_type',
        'Request shape for resolve action (target_status, verifier_ref, closeout_note).',
        '{"source":"migration.225","phase":"1.3.a"}'::jsonb,
        '{"consumer_of":["praxis_bugs.resolve"]}'::jsonb
    ),
    (
        'praxis.bug.resolved_record', 'Resolved bug record', 'object_type',
        'Bug record after resolve: terminal state with resolution metadata.',
        '{"source":"migration.225","phase":"1.3.a"}'::jsonb,
        '{"producer_of":["praxis_bugs.resolve"]}'::jsonb
    )
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;
