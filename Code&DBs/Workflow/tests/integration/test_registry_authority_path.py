from __future__ import annotations

import asyncio
import uuid

from _pg_test_conn import get_test_env
from contracts.domain import (
    MINIMAL_WORKFLOW_EDGE_TYPE,
    MINIMAL_WORKFLOW_NODE_TYPE,
    SUPPORTED_SCHEMA_VERSION,
    WorkflowEdgeContract,
    WorkflowNodeContract,
    WorkflowRequest,
)
from policy.domain import AdmissionDecisionKind
from registry.domain import (
    RuntimeProfileAuthorityRecord,
    UnresolvedAuthorityContext,
    WorkspaceAuthorityRecord,
)
from registry.repository import (
    PostgresRegistryAuthorityRepository,
    bootstrap_registry_authority_schema,
    load_registry_resolver,
)
from runtime import RunState, WorkflowIntakePlanner
from storage.postgres import connect_workflow_database


_TEST_ENV = get_test_env()


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _request(
    *,
    suffix: str,
    workspace_ref: str,
    runtime_profile_ref: str,
) -> WorkflowRequest:
    return WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id=f"workflow.{suffix}",
        request_id=f"request.{suffix}",
        workflow_definition_id=f"workflow_definition.{suffix}.v1",
        definition_hash=f"sha256:{suffix}",
        workspace_ref=workspace_ref,
        runtime_profile_ref=runtime_profile_ref,
        nodes=(
            WorkflowNodeContract(
                node_id="node_0",
                node_type=MINIMAL_WORKFLOW_NODE_TYPE,
                adapter_type=MINIMAL_WORKFLOW_NODE_TYPE,
                display_name="prepare",
                inputs={"task_name": "prepare"},
                expected_outputs={"result": "prepared"},
                success_condition={"status": "success"},
                failure_behavior={"status": "fail_closed"},
                authority_requirements={
                    "workspace_ref": workspace_ref,
                    "runtime_profile_ref": runtime_profile_ref,
                },
                execution_boundary={"workspace_ref": workspace_ref},
                position_index=0,
            ),
            WorkflowNodeContract(
                node_id="node_1",
                node_type=MINIMAL_WORKFLOW_NODE_TYPE,
                adapter_type=MINIMAL_WORKFLOW_NODE_TYPE,
                display_name="admit",
                inputs={"task_name": "admit"},
                expected_outputs={"result": "admitted"},
                success_condition={"status": "success"},
                failure_behavior={"status": "fail_closed"},
                authority_requirements={
                    "workspace_ref": workspace_ref,
                    "runtime_profile_ref": runtime_profile_ref,
                },
                execution_boundary={"workspace_ref": workspace_ref},
                position_index=1,
            ),
        ),
        edges=(
            WorkflowEdgeContract(
                edge_id="edge_0",
                edge_type=MINIMAL_WORKFLOW_EDGE_TYPE,
                from_node_id="node_0",
                to_node_id="node_1",
                release_condition={"upstream_result": "success"},
                payload_mapping={},
                position_index=0,
            ),
        ),
    )


def test_registry_authority_rows_drive_intake_resolution_and_fail_closed_for_unknown_profile() -> None:
    asyncio.run(_exercise_registry_authority_path())


async def _exercise_registry_authority_path() -> None:
    conn = await connect_workflow_database(env=_TEST_ENV)
    try:
        await bootstrap_registry_authority_schema(conn)
        suffix = _unique_suffix()
        workspace_ref = f"workspace.{suffix}"
        runtime_profile_ref = f"runtime_profile.{suffix}"
        workspace_record = WorkspaceAuthorityRecord(
            workspace_ref=workspace_ref,
            repo_root=f"/tmp/{workspace_ref}",
            workdir=f"/tmp/{workspace_ref}/workdir",
        )
        runtime_profile_record = RuntimeProfileAuthorityRecord(
            runtime_profile_ref=runtime_profile_ref,
            model_profile_id=f"model.{suffix}",
            provider_policy_id=f"provider_policy.{suffix}",
            sandbox_profile_ref=runtime_profile_ref,
        )

        repository = PostgresRegistryAuthorityRepository(conn)
        await repository.upsert_workspace_authority(workspace_record)
        await repository.upsert_runtime_profile_authority(runtime_profile_record)

        assert await repository.fetch_workspace_authority(
            workspace_refs=(workspace_ref,),
        ) == (workspace_record,)
        assert await repository.fetch_runtime_profile_authority(
            runtime_profile_refs=(runtime_profile_ref,),
        ) == (runtime_profile_record,)

        resolver = await load_registry_resolver(
            conn,
            workspace_refs=(workspace_ref,),
            runtime_profile_refs=(runtime_profile_ref,),
        )
        planner = WorkflowIntakePlanner(registry=resolver)

        valid_outcome = planner.plan(
            request=_request(
                suffix=suffix,
                workspace_ref=workspace_ref,
                runtime_profile_ref=runtime_profile_ref,
            ),
        )

        assert valid_outcome.validation_result.is_valid is True
        assert valid_outcome.admission_decision.decision is AdmissionDecisionKind.ADMIT
        assert valid_outcome.admission_decision.reason_code == "policy.admission_allowed"
        assert valid_outcome.admission_state is RunState.CLAIM_ACCEPTED
        assert valid_outcome.authority_context is not None
        assert not isinstance(valid_outcome.authority_context, UnresolvedAuthorityContext)
        assert valid_outcome.authority_context.workspace_ref == workspace_ref
        assert valid_outcome.authority_context.runtime_profile_ref == runtime_profile_ref
        assert (
            valid_outcome.authority_context.bundle_payload["workspace"]["repo_root"]
            == workspace_record.repo_root
        )
        assert (
            valid_outcome.authority_context.bundle_payload["workspace"]["workdir"]
            == workspace_record.workdir
        )
        assert (
            valid_outcome.authority_context.bundle_payload["runtime_profile"]["model_profile_id"]
            == runtime_profile_record.model_profile_id
        )
        assert (
            valid_outcome.authority_context.bundle_payload["runtime_profile"]["provider_policy_id"]
            == runtime_profile_record.provider_policy_id
        )
        assert (
            valid_outcome.authority_context.bundle_payload["runtime_profile"]["sandbox_profile_ref"]
            == runtime_profile_record.sandbox_profile_ref
        )

        missing_profile_outcome = planner.plan(
            request=_request(
                suffix=f"{suffix}.missing",
                workspace_ref=workspace_ref,
                runtime_profile_ref=f"runtime_profile.missing.{suffix}",
            ),
        )

        assert missing_profile_outcome.validation_result.is_valid is True
        assert missing_profile_outcome.admission_decision.decision is AdmissionDecisionKind.REJECT
        assert missing_profile_outcome.admission_decision.reason_code == "registry.profile_unknown"
        assert missing_profile_outcome.admission_state is RunState.CLAIM_REJECTED
        assert isinstance(
            missing_profile_outcome.authority_context,
            UnresolvedAuthorityContext,
        )
        assert (
            missing_profile_outcome.authority_context.unresolved_reason_code
            == "registry.profile_unknown"
        )
        assert (
            missing_profile_outcome.route_identity.authority_context_ref
            == missing_profile_outcome.authority_context.context_bundle_id
        )
        assert (
            missing_profile_outcome.route_identity.authority_context_digest
            == missing_profile_outcome.authority_context.bundle_hash
        )
    finally:
        await conn.close()
