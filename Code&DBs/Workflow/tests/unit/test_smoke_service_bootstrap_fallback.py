from __future__ import annotations

from dataclasses import dataclass
import tempfile
from pathlib import Path

import surfaces.api._smoke_service as smoke_service
from surfaces.api import frontdoor, native_ops, operator_read

WORKSPACE_ROOT = str(Path(tempfile.gettempdir()) / "praxis-workspace")


@dataclass
class _FakeInspection:
    current_state: str
    terminal_reason: str
    node_timeline: list[str]


@dataclass
class _FakeFrontdoorService:
    request_calls: list[dict[str, object]]
    status_calls: list[dict[str, object]]
    native_instance_contract: dict[str, str]

    def submit(self, *, request_payload, env=None):
        assert env is not None
        self.request_calls.append(
            {
                "request_payload": dict(request_payload),
                "env": dict(env),
            }
        )
        return {
            "native_instance": dict(self.native_instance_contract),
            "run": {
                "run_id": "run:workflow.native-self-hosted-smoke:fallback",
                "workflow_id": request_payload["workflow_id"],
                "request_id": request_payload["request_id"],
                "current_state": "claim_accepted",
                "admitted_definition_hash": request_payload["definition_hash"],
            },
            "admission_decision": {
                "decision": "admit",
            },
        }

    def status(self, *, run_id, env=None):
        assert env is not None
        self.status_calls.append({"run_id": run_id, "env": dict(env)})
        return {
            "native_instance": dict(self.native_instance_contract),
            "run": {
                "run_id": run_id,
                "workflow_id": "workflow.native-self-hosted-smoke",
                "request_id": "request.native-self-hosted-smoke",
                "workflow_definition_id": "workflow_definition.native_self_hosted_smoke.v1",
                "current_state": "succeeded",
            },
            "inspection": {
                "run_id": run_id,
                "current_state": "succeeded",
                "terminal_reason": "runtime.workflow_succeeded",
                "evidence_refs": [
                    "workflow_event:workflow.native-self-hosted-smoke:1",
                    "receipt:workflow.native-self-hosted-smoke:18",
                ],
            },
        }


def test_run_local_operator_flow_uses_frontdoor_health_database_payloads(
    monkeypatch,
) -> None:
    env = {
        "WORKFLOW_DATABASE_URL": "postgresql://nate@localhost:5432/praxis",
        "PRAXIS_RUNTIME_PROFILE": "praxis",
        "PRAXIS_RUNTIME_PROFILES_CONFIG": f"{WORKSPACE_ROOT}/config/runtime_profiles.json",
    }
    request_payload = {
        "workflow_id": "workflow.native-self-hosted-smoke",
        "request_id": "request.native-self-hosted-smoke",
        "workflow_definition_id": "workflow_definition.native_self_hosted_smoke.v1",
        "definition_hash": "definition.native_self_hosted_smoke.v1",
    }
    instance_contract = {
        "praxis_instance_name": "praxis",
        "praxis_receipts_dir": f"{WORKSPACE_ROOT}/artifacts/runtime_receipts",
        "praxis_runtime_profile": "praxis",
        "praxis_topology_dir": f"{WORKSPACE_ROOT}/artifacts/runtime_topology",
        "repo_root": WORKSPACE_ROOT,
        "runtime_profiles_config": f"{WORKSPACE_ROOT}/config/runtime_profiles.json",
        "workdir": WORKSPACE_ROOT,
    }
    frontdoor_service = _FakeFrontdoorService(
        request_calls=[],
        status_calls=[],
        native_instance_contract=instance_contract,
    )
    seen: dict[str, list[dict[str, str]]] = {
        "show_instance_contract": [],
        "frontdoor_health": [],
    }

    monkeypatch.setattr(
        native_ops,
        "show_instance_contract",
        lambda *, env=None: seen["show_instance_contract"].append(dict(env or {}))
        or instance_contract,
    )

    monkeypatch.setattr(
        frontdoor,
        "health",
        lambda *, env=None, bootstrap=False: seen["frontdoor_health"].append(
            {
                "env": dict(env or {}),
                "bootstrap": bootstrap,
            }
        )
        or {
            "database": {
                "database_reachable": True,
                "schema_bootstrapped": True,
                "database_url": env["WORKFLOW_DATABASE_URL"],
            }
        },
    )
    monkeypatch.setattr(
        smoke_service,
        "_execute_smoke_run",
        lambda *, run_id, env: {
            "current_state": "succeeded",
            "terminal_reason": "runtime.workflow_succeeded",
            "node_order": ["node_0", "node_1"],
        },
    )
    monkeypatch.setattr(
        smoke_service,
        "_load_smoke_proof",
        lambda *, run_id, env: {
            "inspection": {
                "current_state": "succeeded",
                "terminal_reason": "runtime.workflow_succeeded",
                "node_order": ["node_0", "node_1"],
                "node_timeline": [
                    "node_0:running",
                    "node_0:succeeded",
                    "node_1:running",
                    "node_1:succeeded",
                ],
                "evidence_refs": [
                    "workflow_event:workflow.native-self-hosted-smoke:1",
                    "receipt:workflow.native-self-hosted-smoke:18",
                ],
                "completeness": {
                    "is_complete": True,
                    "missing_evidence_refs": [],
                },
                "watermark": {
                    "evidence_seq": 18,
                    "source": "canonical_evidence",
                },
            },
            "evidence": {
                "count": 18,
                "first_evidence_seq": 1,
                "last_evidence_seq": 18,
            },
            "outbox": {
                "row_count": 18,
                "cursor_last_evidence_seq": 18,
                "has_more": False,
                "first_authority_table": "workflow_events",
                "last_authority_table": "receipts",
                "last_envelope_kind": "receipt",
                "last_receipt_type": "workflow_completion_receipt",
                "last_status": "succeeded",
            },
        },
    )

    result = operator_read.run_local_operator_flow(
        request_payload=request_payload,
        env=env,
        registry=None,
        frontdoor_service=frontdoor_service,
    )

    assert result["bootstrap"]["database_url"] == env["WORKFLOW_DATABASE_URL"]
    assert result["health"]["database_reachable"] is True
    assert result["health"]["schema_bootstrapped"] is True
    assert result["run"]["run_id"] == "run:workflow.native-self-hosted-smoke:fallback"
    assert result["execution"]["node_order"] == ["node_0", "node_1"]
    assert result["proof"]["outbox"]["last_receipt_type"] == "workflow_completion_receipt"
    assert seen["show_instance_contract"] == [env]
    assert seen["frontdoor_health"] == [
        {"env": env, "bootstrap": True},
        {"env": env, "bootstrap": False},
    ]
    assert frontdoor_service.request_calls == [
        {
            "request_payload": request_payload,
            "env": env,
        }
    ]
    assert frontdoor_service.status_calls == [
        {
            "run_id": "run:workflow.native-self-hosted-smoke:fallback",
            "env": env,
        }
    ]


def test_execute_smoke_run_waits_for_worker_owned_locked_execution(monkeypatch) -> None:
    env = {
        "WORKFLOW_DATABASE_URL": "postgresql://nate@localhost:5432/praxis",
        "PRAXIS_RUNTIME_PROFILE": "praxis",
    }
    seen: dict[str, object] = {}

    class _FakeConn:
        def close(self) -> None:
            seen["closed"] = True

    class _FakeEvidenceReader:
        def __init__(self, *, env):
            seen["reader_env"] = dict(env)

    class _FakeOrchestrator:
        def __init__(self, *, evidence_reader):
            seen["evidence_reader"] = evidence_reader

        def inspect_run(self, *, run_id):
            seen["inspected_run_id"] = run_id
            return _FakeInspection(
                current_state="succeeded",
                terminal_reason="runtime.workflow_succeeded",
                node_timeline=[
                    "node_0:running",
                    "node_0:succeeded",
                    "node_1:running",
                    "node_1:succeeded",
                ],
            )

    monkeypatch.setattr(smoke_service, "get_workflow_pool", lambda *, env: object())
    monkeypatch.setattr(smoke_service, "SyncPostgresConnection", lambda pool: _FakeConn())
    monkeypatch.setattr(
        smoke_service,
        "_execute_admitted_graph_run",
        lambda conn, *, run_id: {
            "status": "locked",
            "reason_code": "workflow.graph_run_already_locked",
        },
    )
    monkeypatch.setattr(smoke_service, "PostgresEvidenceReader", _FakeEvidenceReader)
    monkeypatch.setattr(smoke_service, "RuntimeOrchestrator", _FakeOrchestrator)

    result = smoke_service._execute_smoke_run(run_id="run:locked", env=env)

    assert result == {
        "current_state": "succeeded",
        "terminal_reason": "runtime.workflow_succeeded",
        "node_order": ["node_0", "node_1"],
    }
    assert seen["closed"] is True
    assert seen["reader_env"] == env
    assert seen["inspected_run_id"] == "run:locked"


def test_load_smoke_registry_uses_logical_workspace_identity(monkeypatch) -> None:
    env = {
        "WORKFLOW_DATABASE_URL": "postgresql://nate@localhost:5432/praxis",
        "PRAXIS_RUNTIME_PROFILE": "praxis",
    }
    request_payload = {
        "workspace_ref": "praxis",
        "runtime_profile_ref": "praxis",
    }
    seen: dict[str, object] = {}

    class _FakeConn:
        async def fetch(self, query, *args):
            if "FROM registry_workspace_authority" in query:
                assert args == ("praxis",)
                return [
                    {
                        "workspace_ref": "praxis",
                        "repo_root": WORKSPACE_ROOT,
                        "workdir": "/workspace",
                    }
                ]
            if "FROM registry_runtime_profile_authority" in query:
                assert args == ("praxis",)
                return [
                    {
                        "runtime_profile_ref": "praxis",
                        "model_profile_id": "model_profile.praxis.default",
                        "provider_policy_id": "provider_policy.praxis.default",
                        "sandbox_profile_ref": "sandbox_profile.praxis.default",
                    }
                ]
            raise AssertionError(f"unexpected query: {query}")

        async def close(self) -> None:
            seen["closed"] = True

    async def _connect_workflow_database(*, env):
        seen["env"] = dict(env)
        return _FakeConn()

    async def _bootstrap_registry_authority_schema(conn):
        seen["bootstrapped"] = conn

    monkeypatch.setattr(smoke_service, "connect_workflow_database", _connect_workflow_database)
    monkeypatch.setattr(
        smoke_service,
        "bootstrap_registry_authority_schema",
        _bootstrap_registry_authority_schema,
    )

    registry = smoke_service._load_smoke_registry(env=env, request_payload=request_payload)

    workspace = registry.resolve_workspace(workspace_ref="praxis")
    assert workspace.repo_root == "praxis"
    assert workspace.workdir == "praxis"
    assert registry.resolve_runtime_profile(
        runtime_profile_ref="praxis",
    ).sandbox_profile_ref == "sandbox_profile.praxis.default"
    assert seen["env"] == env
    assert seen["closed"] is True
