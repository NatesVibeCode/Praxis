-- Migration 235: Re-point launch_plan + compose_plan operations at the
-- gateway-friendly command handlers.
--
-- Migration 234 registered the operations with handler_ref / input_model_ref
-- pointing directly at the underlying business functions
-- (runtime.spec_compiler.launch_plan and
-- runtime.intent_composition.compose_plan_from_intent). Those functions take
-- a ``conn`` keyword arg + plan-shape parameters, which is incompatible with
-- the (command, subsystems) signature the catalog gateway invokes
-- (operation_catalog_gateway.execute_operation_binding line 703).
--
-- This migration re-points the rows at the new
-- runtime.operations.commands.plan_orchestration wrappers
-- (LaunchPlanCommand + handle_launch_plan / ComposePlanCommand +
-- handle_compose_plan). Once handlers route through the catalog gateway
-- via execute_operation_from_subsystems, completion records an
-- authority_operation_receipt with auto-generated event_id and an
-- authority_events row emitting plan.launched / plan.composed — replacing
-- the emit_system_event observability sidecar with receipt-backed CQRS.

BEGIN;

UPDATE operation_catalog_registry
   SET handler_ref = 'runtime.operations.commands.plan_orchestration.handle_launch_plan',
       input_model_ref = 'runtime.operations.commands.plan_orchestration.LaunchPlanCommand',
       input_schema_ref = 'runtime.operations.commands.plan_orchestration.LaunchPlanCommand',
       binding_revision = 'binding.operation_catalog_registry.launch_plan.gateway.20260424',
       updated_at = now()
 WHERE operation_ref = 'launch-plan';

UPDATE operation_catalog_registry
   SET handler_ref = 'runtime.operations.commands.plan_orchestration.handle_compose_plan',
       input_model_ref = 'runtime.operations.commands.plan_orchestration.ComposePlanCommand',
       input_schema_ref = 'runtime.operations.commands.plan_orchestration.ComposePlanCommand',
       binding_revision = 'binding.operation_catalog_registry.compose_plan.gateway.20260424',
       updated_at = now()
 WHERE operation_ref = 'compose-plan';

UPDATE authority_object_registry
   SET metadata = metadata
       || jsonb_build_object(
            'handler_ref', 'runtime.operations.commands.plan_orchestration.handle_launch_plan'
          ),
       updated_at = now()
 WHERE object_ref = 'operation.launch_plan';

UPDATE authority_object_registry
   SET metadata = metadata
       || jsonb_build_object(
            'handler_ref', 'runtime.operations.commands.plan_orchestration.handle_compose_plan'
          ),
       updated_at = now()
 WHERE object_ref = 'operation.compose_plan';

COMMIT;
