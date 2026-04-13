"""Runtime authority package."""

from .domain import (
    AtomicEvidenceWriter,
    EvidenceCommitResult,
    LifecycleTransition,
    RouteIdentity,
    RunState,
    RuntimeBoundaryError,
    RuntimeLifecycleError,
)
from .intake import (
    LifecycleProofStep,
    WorkflowIntakeOutcome,
    WorkflowIntakePlanner,
)

__all__ = [
    "AtomicEvidenceWriter",
    "ClaimLeaseProposalRuntime",
    "ClaimLeaseProposalSnapshot",
    "ClaimLeaseProposalTransitionRequest",
    "EvidenceCommitResult",
    "LifecycleTransition",
    "LifecycleProofStep",
    "RouteIdentity",
    "RunState",
    "RuntimeBoundaryError",
    "RuntimeLifecycleError",
    "NativeWorkflowClassRecord",
    "NativeScheduleDefinitionRecord",
    "NativeScheduledWorkflow",
    "NativeSchedulerError",
    "NativeSchedulerRuntime",
    "SandboxSessionRequest",
    "RuntimeOrchestrator",
    "WorkflowIntakeOutcome",
    "WorkflowIntakePlanner",
    "NodeExecutionRecord",
    "RunExecutionResult",
]


def __getattr__(name: str):
    if name in {
        "ClaimLeaseProposalRuntime",
        "ClaimLeaseProposalSnapshot",
        "ClaimLeaseProposalTransitionRequest",
        "NodeExecutionRecord",
        "RunExecutionResult",
        "NativeWorkflowClassRecord",
        "NativeScheduleDefinitionRecord",
        "NativeScheduledWorkflow",
        "NativeSchedulerError",
        "NativeSchedulerRuntime",
        "RuntimeOrchestrator",
        "SandboxSessionRequest",
    }:
        exports = {}
        if name in {
            "NodeExecutionRecord",
            "RunExecutionResult",
            "RuntimeOrchestrator",
        }:
            from .execution import (
                NodeExecutionRecord,
                RunExecutionResult,
                RuntimeOrchestrator,
            )

            exports.update(
                {
                    "NodeExecutionRecord": NodeExecutionRecord,
                    "RunExecutionResult": RunExecutionResult,
                    "RuntimeOrchestrator": RuntimeOrchestrator,
                }
            )
        if name in {
            "NativeWorkflowClassRecord",
            "NativeScheduleDefinitionRecord",
            "NativeScheduledWorkflow",
            "NativeSchedulerError",
            "NativeSchedulerRuntime",
        }:
            from .native_scheduler import (
                NativeWorkflowClassRecord,
                NativeScheduleDefinitionRecord,
                NativeScheduledWorkflow,
                NativeSchedulerError,
                NativeSchedulerRuntime,
            )

            exports.update(
                {
                    "NativeWorkflowClassRecord": NativeWorkflowClassRecord,
                    "NativeScheduleDefinitionRecord": NativeScheduleDefinitionRecord,
                    "NativeScheduledWorkflow": NativeScheduledWorkflow,
                    "NativeSchedulerError": NativeSchedulerError,
                    "NativeSchedulerRuntime": NativeSchedulerRuntime,
                }
            )
        if name in {
            "ClaimLeaseProposalRuntime",
            "ClaimLeaseProposalSnapshot",
            "ClaimLeaseProposalTransitionRequest",
            "SandboxSessionRequest",
        }:
            from .claims import (
                ClaimLeaseProposalRuntime,
                ClaimLeaseProposalSnapshot,
                ClaimLeaseProposalTransitionRequest,
                SandboxSessionRequest,
            )

            exports.update(
                {
                    "ClaimLeaseProposalRuntime": ClaimLeaseProposalRuntime,
                    "ClaimLeaseProposalSnapshot": ClaimLeaseProposalSnapshot,
                    "ClaimLeaseProposalTransitionRequest": ClaimLeaseProposalTransitionRequest,
                    "SandboxSessionRequest": SandboxSessionRequest,
                }
            )
        return exports[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
