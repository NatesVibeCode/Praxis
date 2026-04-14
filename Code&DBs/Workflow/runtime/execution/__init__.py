"""Runtime execution engine — workflow graph orchestration with deterministic paths."""

from .context import extract_file_refs, inject_accumulated_context
from .dependency import _DependencyResolution, _ExecutionCursor
from .orchestrator import (
    CanonicalEvidenceReader,
    NodeExecutionRecord,
    RunExecutionResult,
    RuntimeOrchestrator,
    TransitionProofWriter,
)
from .state_machine import ALLOWED_TRANSITIONS

__all__ = [
    "ALLOWED_TRANSITIONS",
    "CanonicalEvidenceReader",
    "NodeExecutionRecord",
    "RunExecutionResult",
    "RuntimeOrchestrator",
    "TransitionProofWriter",
]
