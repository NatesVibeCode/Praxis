-- Migration 310: Backfill route_identity on existing receipt rows.
--
-- Problem (surfaced 2026-04-28 during E2E smoke):
--   PostgresEvidenceReader is fail-closed by design — reads of receipt rows
--   missing the route_identity field in inputs JSON raise
--   `postgres.missing_route_identity`. This is correct architecturally
--   (see test_evidence_route_identity_recovery.py), but the writer at
--   runtime/workflow/receipt_writer.py was historically not populating
--   route_identity on workflow_job receipts. 238 of 992 rows in receipts
--   today are missing the field, so any praxis_run status / observability
--   query against those runs crashes.
--
--   The writer is fixed forward by the same-day code change in
--   receipt_writer.py; this migration backfills the existing rows so
--   observability works retroactively.
--
-- Strategy:
--   For each receipt row missing inputs.route_identity, synthesize the
--   field from columns we already have on the row (workflow_id, run_id,
--   request_id, attempt_no) plus joined workflow_runs context
--   (authority_context_digest, context_bundle_id). Synthetic claim_id
--   uses the same `claim:{run_id}:{job_id}:{attempt_no}` shape as the
--   forward-fix in receipt_writer.py. transition_seq stays at 0 inside
--   route_identity — the reader pulls it from top-level inputs anyway.
--
--   Runs in one statement per affected receipt (no PL/pgSQL loop), using
--   jsonb_set for atomic in-place mutation.
--
-- decision_ref: decision.2026-04-28.backfill-receipt-route-identity

BEGIN;

UPDATE receipts r
   SET inputs = jsonb_set(
       r.inputs,
       '{route_identity}',
       jsonb_build_object(
           'workflow_id', r.workflow_id,
           'run_id', r.run_id,
           'request_id', r.request_id,
           'authority_context_ref',
               COALESCE(NULLIF(wr.context_bundle_id, ''), 'context:' || r.run_id),
           'authority_context_digest',
               COALESCE(NULLIF(wr.authority_context_digest, ''), 'missing'),
           'claim_id',
               'claim:' || r.run_id || ':backfill:' || r.attempt_no::text,
           'attempt_no', r.attempt_no,
           'transition_seq', 0
       ),
       true
   )
  FROM workflow_runs wr
 WHERE r.run_id = wr.run_id
   AND NOT (r.inputs ? 'route_identity');

-- Catch any orphan receipts whose run row is missing entirely (shouldn't
-- happen normally, but the reader doesn't care — it just needs the field
-- to exist with valid shapes).
UPDATE receipts r
   SET inputs = jsonb_set(
       r.inputs,
       '{route_identity}',
       jsonb_build_object(
           'workflow_id', r.workflow_id,
           'run_id', r.run_id,
           'request_id', r.request_id,
           'authority_context_ref', 'context:' || r.run_id,
           'authority_context_digest', 'missing',
           'claim_id', 'claim:' || r.run_id || ':orphan:' || r.attempt_no::text,
           'attempt_no', r.attempt_no,
           'transition_seq', 0
       ),
       true
   )
 WHERE NOT (r.inputs ? 'route_identity');

COMMIT;

-- Verification (run manually):
--   SELECT COUNT(*) FROM receipts WHERE NOT (inputs ? 'route_identity');
--   -- Expect 0.
