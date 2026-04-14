"""Explicit Postgres control-plane storage for the workflow runtime.

This module keeps the persistence path boring and closed over explicit
configuration:

- `WORKFLOW_DATABASE_URL` is the only configuration input
- no fallback database path is guessed
- no hidden global connection state is kept
- schema bootstrap is explicit and idempotent
- writes happen through one transactional control-plane path
"""

# Exception classes
from .validators import (
    PostgresConfigurationError,
    PostgresSchemaError,
    PostgresStorageError,
    PostgresWriteError,
    _encode_jsonb,
    _require_text,
    _require_utc,
)

# Schema management
from .schema import (
    ControlPlaneSchemaReadiness,
    WorkflowSchemaReadiness,
    bootstrap_control_plane_schema,
    bootstrap_workflow_schema,
    inspect_control_plane_schema,
    inspect_workflow_schema,
)

# Evidence reading
from .evidence import (
    WORKFLOW_DATABASE_URL_ENV,
    PostgresEvidenceReader,
    fetch_workflow_evidence_timeline,
)
from runtime.compile_artifacts import (
    CompileArtifactError,
    CompileArtifactRecord,
    CompileArtifactStore,
    ExecutionPacketRecord,
)

# Connection management
from .connection import (
    SyncPostgresConnection,
    connect_workflow_database,
    create_workflow_pool,
    ensure_postgres_available,
    get_workflow_pool,
    resolve_workflow_database_url,
    shutdown_workflow_pool,
)

# Vector-store adapter
from .vector_store import (
    PreparedVectorQuery,
    PostgresVectorStore,
    VectorFilter,
    cosine_similarity,
    decode_vector_value,
)

# Admission persistence
from .admission import (
    WorkflowAdmissionDecisionWrite,
    WorkflowAdmissionSubmission,
    WorkflowAdmissionWriteResult,
    WorkflowRunWrite,
    persist_workflow_admission,
)

# Definition persistence
from .definitions import _persist_workflow_definition
from .operator_control_repository import (
    PostgresOperatorControlRepository,
    bootstrap_operator_control_repository_schema,
)
from .operator_frame_repository import PostgresOperatorFrameRepository
from .memory_graph_repository import PostgresMemoryGraphRepository
from .receipt_repository import PostgresReceiptRepository
from .evidence_repository import PostgresEvidenceRepository
from .compile_artifact_repository import PostgresCompileArtifactRepository
from .command_repository import PostgresCommandRepository
from .friction_repository import PostgresFrictionRepository
from .roadmap_authoring_repository import PostgresRoadmapAuthoringRepository
from .observability_maintenance_repository import (
    PostgresObservabilityMaintenanceRepository,
)
from .bug_evidence_repository import PostgresBugEvidenceRepository
from .subscription_repository import PostgresSubscriptionRepository
from .task_route_eligibility_repository import (
    PostgresTaskRouteEligibilityRepository,
)
from .transport_eligibility_repository import (
    PostgresTransportEligibilityRepository,
)
from .verification_repository import PostgresVerificationRepository
from .work_item_closeout_repository import PostgresWorkItemCloseoutRepository
from .workflow_runtime_repository import (
    create_app_manifest,
    create_authority_checkpoint,
    decide_authority_checkpoint,
    load_app_manifest_record,
    record_app_manifest_history,
    reset_observability_metrics,
    upsert_app_manifest,
)
from .workflow_schedule_repository import PostgresWorkflowScheduleRepository

__all__ = [
    "ControlPlaneSchemaReadiness",
    "PostgresEvidenceReader",
    "PostgresEvidenceRepository",
    "PostgresBugEvidenceRepository",
    "PostgresConfigurationError",
    "PostgresCommandRepository",
    "PostgresFrictionRepository",
    "PostgresMemoryGraphRepository",
    "PostgresObservabilityMaintenanceRepository",
    "PostgresOperatorControlRepository",
    "PostgresOperatorFrameRepository",
    "PostgresReceiptRepository",
    "PostgresRoadmapAuthoringRepository",
    "PostgresSchemaError",
    "PostgresStorageError",
    "PostgresSubscriptionRepository",
    "PostgresTaskRouteEligibilityRepository",
    "PostgresTransportEligibilityRepository",
    "PostgresVerificationRepository",
    "PostgresWorkItemCloseoutRepository",
    "PostgresWorkflowScheduleRepository",
    "PreparedVectorQuery",
    "PostgresWriteError",
    "PostgresVectorStore",
    "SyncPostgresConnection",
    "WORKFLOW_DATABASE_URL_ENV",
    "CompileArtifactError",
    "CompileArtifactRecord",
    "CompileArtifactStore",
    "PostgresCompileArtifactRepository",
    "WorkflowAdmissionDecisionWrite",
    "WorkflowAdmissionSubmission",
    "WorkflowAdmissionWriteResult",
    "WorkflowSchemaReadiness",
    "WorkflowRunWrite",
    "bootstrap_operator_control_repository_schema",
    "bootstrap_control_plane_schema",
    "bootstrap_workflow_schema",
    "connect_workflow_database",
    "create_workflow_pool",
    "create_app_manifest",
    "create_authority_checkpoint",
    "cosine_similarity",
    "decide_authority_checkpoint",
    "decode_vector_value",
    "ensure_postgres_available",
    "fetch_workflow_evidence_timeline",
    "get_workflow_pool",
    "inspect_control_plane_schema",
    "inspect_workflow_schema",
    "load_app_manifest_record",
    "persist_workflow_admission",
    "resolve_workflow_database_url",
    "record_app_manifest_history",
    "reset_observability_metrics",
    "shutdown_workflow_pool",
    "VectorFilter",
    "ExecutionPacketRecord",
    "_encode_jsonb",
    "_persist_workflow_definition",
    "_require_text",
    "_require_utc",
    "upsert_app_manifest",
]
