-- Migration 275: Re-point compose-plan-via-llm at a gateway-friendly handler.
--
-- Migration 248 registered ``compose-plan-via-llm`` in
-- ``operation_catalog_registry`` but the ``input_model_ref`` pointed at a
-- class that did not exist (``runtime.compose_plan_via_llm.ComposePlanViaLLMCommand``)
-- and the ``handler_ref`` was the bare function ``compose_plan_via_llm``
-- whose signature did not match the gateway's ``(command, subsystems)``
-- calling convention. Result: every dispatch through
-- ``execute_operation_from_subsystems(operation_name='compose_plan_via_llm', ...)``
-- failed at binding resolution.
--
-- This migration repoints the registration at the new wrapper added in
-- ``runtime/operations/commands/compose_plan_via_llm_command.py``. The
-- ``compose_experiment`` runner needs the gateway path to work so each
-- child compose run produces its own ``plan.composed`` receipt + event for
-- CQRS replay parity with single-compose calls.

BEGIN;

UPDATE operation_catalog_registry
   SET input_model_ref = 'runtime.operations.commands.compose_plan_via_llm_command.ComposePlanViaLLMCommand',
       handler_ref     = 'runtime.operations.commands.compose_plan_via_llm_command.handle_compose_plan_via_llm',
       binding_revision = 'binding.operation_catalog_registry.compose_plan_via_llm.gateway.20260426',
       updated_at = now()
 WHERE operation_ref = 'compose-plan-via-llm';

-- Mirror in authority_object_registry metadata so handler discovery stays consistent.
UPDATE authority_object_registry
   SET metadata = jsonb_set(
        COALESCE(metadata, '{}'::jsonb),
        '{handler_ref}',
        '"runtime.operations.commands.compose_plan_via_llm_command.handle_compose_plan_via_llm"'::jsonb,
        TRUE
    ),
       updated_at = now()
 WHERE object_ref = 'operation.compose_plan_via_llm';

COMMIT;

-- Verification (run manually):
--   SELECT operation_ref, input_model_ref, handler_ref FROM operation_catalog_registry
--    WHERE operation_ref = 'compose-plan-via-llm';
