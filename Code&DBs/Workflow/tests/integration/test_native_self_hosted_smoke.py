from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import uuid

import pytest

from registry.domain import RuntimeProfileAuthorityRecord, WorkspaceAuthorityRecord
from registry.repository import (
    PostgresRegistryAuthorityRepository,
    bootstrap_registry_authority_schema,
    load_registry_resolver,
)
from runtime.execution import RuntimeOrchestrator
from runtime.instance import (
    PRAXIS_RUNTIME_PROFILE_ENV,
    PRAXIS_RUNTIME_PROFILES_CONFIG_ENV,
    resolve_native_instance,
)
from runtime.intake import WorkflowIntakePlanner
from runtime.persistent_evidence import PostgresEvidenceWriter
from runtime.outbox import PostgresWorkflowOutboxSubscriber, bootstrap_workflow_outbox_schema
from storage.postgres import (
    PostgresEvidenceReader,
    bootstrap_control_plane_schema,
    connect_workflow_database,
)
from surfaces.api import frontdoor

_QUEUE_FILENAME = "PRAXIS_NATIVE_SELF_HOSTED_SMOKE.queue.json"
_PATH_ENV_NAMES = {
    "PRAXIS_LOCAL_POSTGRES_DATA_DIR",
    PRAXIS_RUNTIME_PROFILES_CONFIG_ENV,
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _queue_path() -> Path:
    return _repo_root() / "artifacts" / "workflow" / _QUEUE_FILENAME


def _load_queue() -> dict[str, object]:
    return json.loads(_queue_path().read_text(encoding="utf-8"))


def _native_smoke_contract(queue_payload: dict[str, object]) -> dict[str, object]:
    native_smoke = queue_payload.get("native_smoke")
    assert isinstance(native_smoke, dict)
    return native_smoke


def _runtime_env(smoke_contract: dict[str, object]) -> dict[str, str]:
    raw_env = smoke_contract.get("runtime_env")
    assert isinstance(raw_env, dict)

    repo_root = _repo_root()
    resolved: dict[str, str] = {}
    for name, value in raw_env.items():
        assert isinstance(name, str)
        assert isinstance(value, str)
        if name in _PATH_ENV_NAMES:
            resolved[name] = str((repo_root / value).resolve())
        else:
            resolved[name] = value
    return resolved


def _request_payload(smoke_contract: dict[str, object]) -> dict[str, object]:
    workflow_request = smoke_contract.get("workflow_request")
    assert isinstance(workflow_request, dict)
    return workflow_request


def _isolated_request_payload(smoke_contract: dict[str, object]) -> dict[str, object]:
    request_payload = deepcopy(_request_payload(smoke_contract))
    suffix = uuid.uuid4().hex[:10]
    for field_name in ("workflow_id", "request_id", "workflow_definition_id", "definition_hash"):
        value = request_payload.get(field_name)
        assert isinstance(value, str)
        request_payload[field_name] = f"{value}.{suffix}"
    return request_payload


def _runtime_profile_record(*, runtime_profile_ref: str) -> RuntimeProfileAuthorityRecord:
    return RuntimeProfileAuthorityRecord(
        runtime_profile_ref=runtime_profile_ref,
        model_profile_id=f"model_profile.{runtime_profile_ref}.default",
        provider_policy_id=f"provider_policy.{runtime_profile_ref}.default",
    )
def test_native_self_hosted_smoke_proves_repo_local_authority_and_durable_graph_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_payload = _load_queue()

    assert queue_payload["phase"] == "DAGW11G"
    assert queue_payload["workflow_id"] == "dag_native_self_hosted_smoke"
    assert queue_payload["anti_requirements"] == [
        "no broad legacy cutover",
        "no second orchestration state machine",
        "no transport-specific complexity",
    ]
    _root = str(_repo_root())
    assert queue_payload["verify"] == [
        {
            "command": (
                f"PYTHONPATH='{_root}/Code&DBs/Workflow' "
                "python3 -m pytest -q "
                f"'{_root}/Code&DBs/Workflow/tests/integration/"
                "test_native_self_hosted_smoke.py'"
            )
        }
    ]
    assert len(queue_payload["jobs"]) == 1
    assert queue_payload["jobs"][0]["label"] == "PRAXW01.1_build_self_hosted_smoke"

    smoke_contract = _native_smoke_contract(queue_payload)
    assert set(smoke_contract["runtime_env"]) == {
        "WORKFLOW_DATABASE_URL",
        "PRAXIS_RUNTIME_PROFILES_CONFIG",
        "PRAXIS_RUNTIME_PROFILE",
        "PRAXIS_LOCAL_POSTGRES_DATA_DIR",
    }
    assert smoke_contract["authority_path"] == {
        "queue_contract": "artifacts/workflow/PRAXIS_NATIVE_SELF_HOSTED_SMOKE.queue.json",
        "instance_resolver": "runtime.instance.resolve_native_instance",
        "registry_authority": "registry.repository.load_registry_resolver",
        "frontdoor": "surfaces.api.frontdoor.NativeDagFrontdoor",
        "durable_run_authority": "workflow_runs",
        "durable_outcome_authority": [
            "workflow_events",
            "receipts",
            "workflow_outbox",
        ],
    }
    assert smoke_contract["outcome_proof"] == {
        "required_terminal_state": "succeeded",
        "required_node_order": ["node_0", "node_1"],
        "inspection_surface": "runtime.execution.RuntimeOrchestrator.inspect_run",
        "outbox_surface": "runtime.outbox.PostgresWorkflowOutboxSubscriber",
    }

    env = _runtime_env(smoke_contract)
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", env["WORKFLOW_DATABASE_URL"])
    native_instance = resolve_native_instance(env=env)
    request_payload = _isolated_request_payload(smoke_contract)
    runtime_profile_record = _runtime_profile_record(
        runtime_profile_ref=request_payload["runtime_profile_ref"],
    )
    assert isinstance(request_payload["workspace_ref"], str)
    workspace_record = WorkspaceAuthorityRecord(
        workspace_ref=request_payload["workspace_ref"],
        repo_root=native_instance.repo_root,
        workdir=native_instance.workdir,
    )

    loop = asyncio.new_event_loop()
    writer: PostgresEvidenceWriter | None = None
    conn = loop.run_until_complete(connect_workflow_database(env=env))
    try:
        loop.run_until_complete(bootstrap_control_plane_schema(conn))
        loop.run_until_complete(bootstrap_registry_authority_schema(conn))
        loop.run_until_complete(bootstrap_workflow_outbox_schema(conn))

        repository = PostgresRegistryAuthorityRepository(conn)
        loop.run_until_complete(repository.upsert_workspace_authority(workspace_record))
        loop.run_until_complete(repository.upsert_runtime_profile_authority(runtime_profile_record))
        resolver = loop.run_until_complete(
            load_registry_resolver(
                conn,
                workspace_refs=(workspace_record.workspace_ref,),
                runtime_profile_refs=(runtime_profile_record.runtime_profile_ref,),
            )
        )

        api = frontdoor.NativeDagFrontdoor(registry=resolver)
        monkeypatch.setattr(
            frontdoor,
            "_now",
            lambda: datetime(2000, 1, 1, tzinfo=timezone.utc),
        )
        submit_payload = api.submit(request_payload=request_payload, env=env)
        status_payload = api.status(run_id=submit_payload["run"]["run_id"], env=env)
        health_payload = api.health(env=env)

        workflow_request = frontdoor._request_from_mapping(request_payload)
        outcome = WorkflowIntakePlanner(registry=resolver).plan(request=workflow_request)
        writer = PostgresEvidenceWriter(database_url=env["WORKFLOW_DATABASE_URL"])
        execution_result = RuntimeOrchestrator().execute_deterministic_path(
            intake_outcome=outcome,
            evidence_writer=writer,
            accumulate_context=False,
        )

        run_row = loop.run_until_complete(
            conn.fetchrow(
                """
                SELECT
                    run_id,
                    workflow_id,
                    request_id,
                    workflow_definition_id,
                    current_state,
                    context_bundle_id,
                    authority_context_digest,
                    admission_decision_id
                FROM workflow_runs
                WHERE run_id = $1
                """,
                outcome.run_id,
            )
        )
        assert run_row is not None
    finally:
        if writer is not None:
            try:
                writer._bridge.run(writer.close())
            finally:
                writer._bridge.close()
        loop.run_until_complete(conn.close())
        loop.close()

    reader = PostgresEvidenceReader(env=env)
    canonical_evidence = reader.evidence_timeline(outcome.run_id)
    inspection = RuntimeOrchestrator(evidence_reader=reader).inspect_run(run_id=outcome.run_id)
    outbox_batch = PostgresWorkflowOutboxSubscriber(env=env).read_batch(
        run_id=outcome.run_id,
        limit=32,
    )

    assert submit_payload["native_instance"] == native_instance.to_contract()
    assert status_payload["native_instance"] == native_instance.to_contract()
    assert health_payload["native_instance"] == native_instance.to_contract()
    assert health_payload["database"]["database_url"] == env["WORKFLOW_DATABASE_URL"]
    assert health_payload["database"]["process_running"] is True
    assert health_payload["database"]["database_reachable"] is True
    assert health_payload["database"]["schema_bootstrapped"] is True

    assert submit_payload["admission_decision"]["decision"] == "admit"
    assert submit_payload["run"]["run_id"] == outcome.run_id
    assert submit_payload["run"]["workflow_id"] == request_payload["workflow_id"]
    assert submit_payload["run"]["request_id"] == request_payload["request_id"]
    assert submit_payload["run"]["workflow_definition_id"] == request_payload["workflow_definition_id"]
    assert submit_payload["run"]["current_state"] == "claim_accepted"

    assert status_payload["run"]["run_id"] == outcome.run_id
    assert status_payload["run"]["current_state"] == "claim_accepted"
    assert status_payload["inspection"] is None

    assert run_row["run_id"] == outcome.run_id
    assert run_row["workflow_id"] == request_payload["workflow_id"]
    assert run_row["request_id"] == request_payload["request_id"]
    assert run_row["workflow_definition_id"] == request_payload["workflow_definition_id"]
    assert run_row["current_state"] == "claim_accepted"
    assert run_row["context_bundle_id"] == outcome.authority_context.context_bundle_id
    assert run_row["authority_context_digest"] == outcome.route_identity.authority_context_digest
    assert run_row["admission_decision_id"] == outcome.admission_decision.admission_decision_id

    assert outcome.authority_context is not None
    assert outcome.authority_context.workspace_ref == workspace_record.workspace_ref
    assert outcome.authority_context.runtime_profile_ref == runtime_profile_record.runtime_profile_ref
    assert outcome.authority_context.bundle_payload["workspace"]["repo_root"] == native_instance.repo_root
    assert outcome.authority_context.bundle_payload["workspace"]["workdir"] == native_instance.workdir
    assert (
        outcome.authority_context.bundle_payload["runtime_profile"]["model_profile_id"]
        == runtime_profile_record.model_profile_id
    )
    assert (
        outcome.authority_context.bundle_payload["runtime_profile"]["provider_policy_id"]
        == runtime_profile_record.provider_policy_id
    )

    assert execution_result.current_state.value == "succeeded"
    assert execution_result.terminal_reason_code == "runtime.workflow_succeeded"
    assert execution_result.node_order == ("node_0", "node_1")
    assert execution_result.node_results[0].outputs == {"result": "prepared"}
    assert execution_result.node_results[1].outputs == {"result": "persisted"}

    assert len(canonical_evidence) == 18
    assert [row.evidence_seq for row in canonical_evidence] == list(range(1, 19))
    assert [
        row.record.event_type
        for row in canonical_evidence
        if row.kind == "workflow_event"
    ] == [
        "claim_received",
        "claim_validated",
        "workflow_queued",
        "workflow_started",
        "node_started",
        "node_succeeded",
        "node_started",
        "node_succeeded",
        "workflow_succeeded",
    ]

    assert inspection.completeness.is_complete is True
    assert inspection.watermark.evidence_seq == 18
    assert inspection.current_state == "succeeded"
    assert inspection.node_timeline == (
        "node_0:running",
        "node_0:succeeded",
        "node_1:running",
        "node_1:succeeded",
    )
    assert inspection.terminal_reason == "runtime.workflow_succeeded"

    assert [row.evidence_seq for row in outbox_batch.rows] == list(range(1, 19))
    assert outbox_batch.cursor.last_evidence_seq == 18
    assert outbox_batch.has_more is False
    assert outbox_batch.rows[0].authority_table == "workflow_events"
    assert outbox_batch.rows[0].envelope["event_type"] == "claim_received"
    assert outbox_batch.rows[-1].authority_table == "receipts"
    assert outbox_batch.rows[-1].envelope["receipt_type"] == "workflow_completion_receipt"
    assert outbox_batch.rows[-1].envelope["status"] == "succeeded"
