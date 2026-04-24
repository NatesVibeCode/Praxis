-- Migration 220: archive mobile v1 - drop the multi-user mobile access tables.
--
-- Mobile v1 (WebAuthn device enrollment, per-command approval flow, bootstrap
-- tokens, session budgets) conflated two different use cases (external-user
-- access and operator god-mode). Neither shipped. The archive lives on branch
-- archive/mobile-v1-2026-04-24 (doc: docs/archive/mobile-v1.md).
--
-- capability_grants stays - it is used by the workflow admission
-- audit trail in storage/postgres/admission.py. gate_evaluations.grant_id
-- and plan_envelope_hash also stay - used by submission policy.
--
-- Revival is not a replay of migrations 185-189. A new forward migration must
-- recreate whatever schema the next mobile iteration actually needs.

BEGIN;

DROP TABLE IF EXISTS mobile_session_budget_events CASCADE;
DROP TABLE IF EXISTS mobile_sessions CASCADE;
DROP TABLE IF EXISTS mobile_bootstrap_tokens CASCADE;
DROP TABLE IF EXISTS webauthn_challenges CASCADE;
DROP TABLE IF EXISTS approval_requests CASCADE;
DROP TABLE IF EXISTS device_enrollments CASCADE;

UPDATE authority_legacy_domain_assignment_rules
   SET enabled = FALSE,
       assignment_reason = assignment_reason || ' Archived by 220_archive_mobile_v1.sql.',
       updated_at = now()
 WHERE rule_ref IN (
       'legacy_domain.rule.mobile',
       'legacy_domain.rule.webauthn',
       'legacy_domain.rule.device',
       'legacy_domain.rule.session_blast_radius'
   )
   AND enabled IS DISTINCT FROM FALSE;

UPDATE authority_domains
   SET enabled = FALSE,
       decision_ref = 'decision.2026-04-24.mobile-v1-archived',
       updated_at = now()
 WHERE authority_domain_ref = 'authority.mobile_access'
   AND enabled IS DISTINCT FROM FALSE;

COMMIT;
