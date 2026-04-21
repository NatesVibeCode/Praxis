-- Migration 193: repair native self-hosted smoke workflow_definition drift.
--
-- Three independent drift axes bite the smoke in sequence on databases
-- predating the canonical smoke shape:
--
-- (A) Workspace/runtime rename. Migration 149 seeded the row with
--     `workspace_ref = 'praxis'` and `runtime_profile_ref = 'praxis'`. On
--     databases that already carried a historical `dag-project` smoke row,
--     149's `ON CONFLICT DO UPDATE` never actually ran -- it was backfilled
--     into `schema_migrations` by expected-object-presence inference. The
--     drift sits in `request_envelope`, the envelope's nested `nodes[]`
--     (authority_requirements + execution_boundary), `normalized_definition`,
--     and the separate `workflow_definition_nodes` table.
-- (B) Deterministic-adapter tightening. Commit b10c2254 (2026-04-19) made
--     `adapters.deterministic.DeterministicTaskAdapter` fail closed with
--     `adapter.deterministic_builder_missing` whenever a node lacks both a
--     `deterministic_builder` and an explicit
--     `input_payload.allow_passthrough_echo = true`. The smoke never
--     registered a builder (it is intentionally a passthrough). Migration
--     149 was updated in lock-step to emit the opt-in, but DBs already
--     holding pre-b10c2254 rows still carry input_payloads without it.
--
-- Failure cascade observed end-to-end:
--   1. RegistryResolver -> `registry.workspace_unknown` on the envelope's
--      top-level workspace_ref (A).
--   2. contracts.domain.validate_request_contract -> `request.graph_invalid`
--      because nested node authority_requirements disagree with the envelope
--      workspace_ref (A).
--   3. DeterministicTaskAdapter -> `adapter.deterministic_builder_missing`,
--      run ends `failed`, smoke terminal check raises
--      `operator_flow.smoke_execution_invalid` (B).
--
-- Every step of the cascade terminates before `succeeded`, so
-- `scripts/bootstrap` (which runs `native-smoke.sh` as its final
-- fresh-clone proof, driving
-- roadmap_item.fresh.instance.onboarding.readiness.bootstrap.one.command.fresh.clone)
-- fails.
--
-- This migration re-asserts the canonical smoke shape across all four
-- persistence surfaces: the envelope top-level fields, the envelope's
-- nested nodes array, the `normalized_definition` copy, and the
-- `workflow_definition_nodes` table. It is idempotent: on a truly fresh DB
-- (where migration 149 actually executed) all values are already canonical
-- and these UPDATEs become no-ops.

BEGIN;

WITH praxis_node_array AS (
    SELECT COALESCE(
        jsonb_agg(
            jsonb_set(
                jsonb_set(
                    jsonb_set(
                        node,
                        '{authority_requirements}',
                        jsonb_build_object(
                            'workspace_ref', 'praxis',
                            'runtime_profile_ref', 'praxis'
                        ),
                        true
                    ),
                    '{execution_boundary}',
                    jsonb_build_object('workspace_ref', 'praxis'),
                    true
                ),
                '{inputs,input_payload,allow_passthrough_echo}',
                'true'::jsonb,
                true
            )
        ),
        '[]'::jsonb
    ) AS nodes
    FROM workflow_definitions,
         LATERAL jsonb_array_elements(request_envelope->'nodes') AS node
    WHERE workflow_definition_id = 'workflow_definition.native_self_hosted_smoke.v1'
)
UPDATE workflow_definitions
SET request_envelope = jsonb_set(
        jsonb_set(
            jsonb_set(request_envelope, '{workspace_ref}', '"praxis"'::jsonb, true),
            '{runtime_profile_ref}', '"praxis"'::jsonb, true
        ),
        '{nodes}',
        (SELECT nodes FROM praxis_node_array),
        true
    ),
    normalized_definition = jsonb_set(
        jsonb_set(
            jsonb_set(normalized_definition, '{workspace_ref}', '"praxis"'::jsonb, true),
            '{runtime_profile_ref}', '"praxis"'::jsonb, true
        ),
        '{nodes}',
        (SELECT nodes FROM praxis_node_array),
        true
    )
WHERE workflow_definition_id = 'workflow_definition.native_self_hosted_smoke.v1';

UPDATE workflow_definition_nodes
SET authority_requirements = jsonb_build_object(
        'workspace_ref', 'praxis',
        'runtime_profile_ref', 'praxis'
    ),
    execution_boundary = jsonb_build_object(
        'workspace_ref', 'praxis'
    ),
    inputs = jsonb_set(
        inputs,
        '{input_payload,allow_passthrough_echo}',
        'true'::jsonb,
        true
    )
WHERE workflow_definition_id = 'workflow_definition.native_self_hosted_smoke.v1';

COMMIT;
