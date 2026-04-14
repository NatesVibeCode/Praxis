"""Workflow authority package.

This layer owns validated read models, resolution, and control-plane snapshots.
It does not own raw SQL, connection management, or execution-time scheduling.
"""

from .operator_control import (
    CutoverGateAuthorityRecord,
    CutoverGateResolution,
    OperatorControlAuthority,
    OperatorControlRepositoryError,
    OperatorDecisionAuthorityRecord,
    OperatorDecisionResolution,
    load_operator_control_authority,
)
from .workflow_class_resolution import (
    WorkflowClassResolutionDecision,
    WorkflowClassResolutionError,
    WorkflowClassResolutionRuntime,
    load_workflow_class_resolution_runtime,
)
from .workflow_schedule import (
    NativeWorkflowScheduleCatalog,
    NativeWorkflowScheduleResolution,
    RecurringRunWindowAuthorityRecord,
    ScheduleDefinitionAuthorityRecord,
    ScheduleRepositoryError,
    load_workflow_schedule_catalog,
)
from .transport_eligibility import (
    AdapterTransportSupportAuthorityRecord,
    ModelTransportEligibilityAuthorityRecord,
    ProviderTransportEligibilityAuthorityRecord,
    RoutePreflightJobAuthorityRecord,
    TransportEligibilityAuthority,
    TransportEligibilityAuthorityError,
    load_transport_eligibility_authority,
)

__all__ = [
    "CutoverGateAuthorityRecord",
    "CutoverGateResolution",
    "NativeWorkflowScheduleCatalog",
    "NativeWorkflowScheduleResolution",
    "OperatorControlAuthority",
    "OperatorControlRepositoryError",
    "OperatorDecisionAuthorityRecord",
    "OperatorDecisionResolution",
    "RecurringRunWindowAuthorityRecord",
    "ScheduleDefinitionAuthorityRecord",
    "ScheduleRepositoryError",
    "AdapterTransportSupportAuthorityRecord",
    "ModelTransportEligibilityAuthorityRecord",
    "ProviderTransportEligibilityAuthorityRecord",
    "RoutePreflightJobAuthorityRecord",
    "TransportEligibilityAuthority",
    "TransportEligibilityAuthorityError",
    "WorkflowClassResolutionDecision",
    "WorkflowClassResolutionError",
    "WorkflowClassResolutionRuntime",
    "load_operator_control_authority",
    "load_transport_eligibility_authority",
    "load_workflow_class_resolution_runtime",
    "load_workflow_schedule_catalog",
]
