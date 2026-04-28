-- Migration 301: Widen policy_definitions.enforcement_kind to include update_clamp.
--
-- Anchor decision:
--   architecture-policy::policy-authority::data-layer-teeth
--
-- Found during P4.2.f end-to-end verification: the schema's CHECK
-- constraint omitted 'update_clamp' even though the activator
-- (policy_authority_attach_table_policy) explicitly handles the value
-- and returns a feature_not_supported error directing callers to file
-- a follow-up migration when concrete clamp policies land. Result:
-- the operator can never file a clamp row to test the activator's
-- "not yet implemented" path.
--
-- This migration aligns the CHECK with the activator's vocabulary so
-- clamp policies can be authored even though enforcement isn't wired
-- yet. The activator still refuses to attach them.

BEGIN;

ALTER TABLE policy_definitions
    DROP CONSTRAINT policy_definitions_enforcement_kind_check;

ALTER TABLE policy_definitions
    ADD CONSTRAINT policy_definitions_enforcement_kind_check
    CHECK (enforcement_kind IN (
        'insert_reject',
        'update_reject',
        'delete_reject',
        'truncate_reject',
        'update_clamp'
    ));

COMMIT;
