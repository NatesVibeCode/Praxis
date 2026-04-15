from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from registry.domain import RegistryResolver, RuntimeProfileAuthorityRecord, WorkspaceAuthorityRecord
from runtime.instance import (
    PRAXIS_RECEIPTS_DIR_ENV,
    PRAXIS_RUNTIME_PROFILE_ENV,
    PRAXIS_RUNTIME_PROFILES_CONFIG_ENV,
    PRAXIS_TOPOLOGY_DIR_ENV,
    NativeInstanceResolutionError,
    resolve_native_instance,
)
from surfaces.api import frontdoor, native_ops, operator_read

_QUEUE_FILENAME = "PRAXIS_NATIVE_SELF_HOSTED_SMOKE.queue.json"
_PATH_ENV_NAMES = {
    PRAXIS_RECEIPTS_DIR_ENV,
    PRAXIS_RUNTIME_PROFILES_CONFIG_ENV,
    PRAXIS_TOPOLOGY_DIR_ENV,
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


def _registry(request_payload: dict[str, object]) -> RegistryResolver:
    workspace_ref = request_payload["workspace_ref"]
    runtime_profile_ref = request_payload["runtime_profile_ref"]
    assert isinstance(workspace_ref, str)
    assert isinstance(runtime_profile_ref, str)

    repo_root = str(_repo_root())
    return RegistryResolver(
        workspace_records={
            workspace_ref: (
                WorkspaceAuthorityRecord(
                    workspace_ref=workspace_ref,
                    repo_root=repo_root,
                    workdir=repo_root,
                ),
            ),
        },
        runtime_profile_records={
            runtime_profile_ref: (
                RuntimeProfileAuthorityRecord(
                    runtime_profile_ref=runtime_profile_ref,
                    model_profile_id="model.native-self-hosted-smoke",
                    provider_policy_id="provider.native-self-hosted-smoke",
                    sandbox_profile_ref=runtime_profile_ref,
                ),
            ),
        },
    )


def _native_instance_contract(env: dict[str, str]) -> dict[str, str]:
    return resolve_native_instance(env=env).to_contract()


@dataclass
class _FakeFrontdoorService:
    request_calls: list[dict[str, object]]
    status_calls: list[dict[str, object]]
    native_instance_contract: dict[str, str]

    def submit(self, *, request_payload, env=None):
        assert env is not None
        self.request_calls.append(
            {
                "request_payload": request_payload,
                "env": dict(env),
            }
        )
        return {
            "native_instance": dict(self.native_instance_contract),
            "run": {
                "run_id": "run:workflow.native-self-hosted-smoke:operator-flow",
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
        self.status_calls.append(
            {
                "run_id": run_id,
                "env": dict(env),
            }
        )
        return {
            "native_instance": dict(self.native_instance_contract),
            "run": {
                "run_id": run_id,
                "workflow_id": "workflow.native-self-hosted-smoke",
                "request_id": "request.native-self-hosted-smoke",
                "workflow_definition_id": "workflow_definition.native_self_hosted_smoke.v1",
                "current_state": "claim_accepted",
                "terminal_reason_code": None,
                "run_idempotency_key": "request.native-self-hosted-smoke",
                "context_bundle_id": f"context:{run_id}",
                "authority_context_digest": "digest.native-self-hosted-smoke",
                "admission_decision_id": f"admission:{run_id}",
                "requested_at": datetime(2026, 4, 2, 12, 0, tzinfo=timezone.utc).isoformat(),
                "admitted_at": datetime(2026, 4, 2, 12, 0, 1, tzinfo=timezone.utc).isoformat(),
                "started_at": None,
                "finished_at": None,
                "last_event_id": "event:workflow.native-self-hosted-smoke:1",
            },
            "inspection": {
                "run_id": run_id,
                "current_state": "claim_accepted",
                "terminal_reason": "runtime.workflow_succeeded",
                "evidence_refs": [
                    "workflow_event:workflow.native-self-hosted-smoke:1",
                ],
            },
        }


class _FakeSmokeDatabaseConnection:
    def __init__(self, row: dict[str, object] | None) -> None:
        self.row = row
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False

    async def fetchrow(self, query: str, *args: object) -> dict[str, object] | None:
        self.fetchrow_calls.append((query, args))
        return self.row

    async def close(self) -> None:
        self.closed = True


def test_operator_flow_runs_one_repo_local_sequence_and_surfaces_run_inspection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_payload = _load_queue()
    smoke_contract = _native_smoke_contract(queue_payload)
    request_payload = _request_payload(smoke_contract)
    env = _runtime_env(smoke_contract)
    instance_contract = _native_instance_contract(env)
    registry = _registry(request_payload)
    frontdoor_service = _FakeFrontdoorService(
        request_calls=[],
        status_calls=[],
        native_instance_contract=instance_contract,
    )
    seen: dict[str, list[dict[str, str]]] = {
        "show_instance_contract": [],
        "db_bootstrap": [],
        "db_health": [],
    }

    monkeypatch.setattr(
        native_ops,
        "show_instance_contract",
        lambda *, env=None: seen["show_instance_contract"].append(dict(env or {})) or instance_contract,
    )
    monkeypatch.setattr(
        native_ops,
        "db_bootstrap",
        lambda *, env=None: seen["db_bootstrap"].append(dict(env or {}))
        or {
            "database_reachable": True,
            "schema_bootstrapped": True,
            "database_url": env["WORKFLOW_DATABASE_URL"],
        },
    )
    monkeypatch.setattr(
        native_ops,
        "db_health",
        lambda *, env=None: seen["db_health"].append(dict(env or {}))
        or {
            "database_reachable": True,
            "schema_bootstrapped": True,
            "database_url": env["WORKFLOW_DATABASE_URL"],
        },
    )

    result = operator_read.run_local_operator_flow(
        request_payload=request_payload,
        env=env,
        registry=registry,
        frontdoor_service=frontdoor_service,
    )

    assert result["step_order"] == [
        "show_instance_contract",
        "db_bootstrap",
        "db_health",
        "submit",
        "status",
    ]
    assert seen["show_instance_contract"] == [env]
    assert seen["db_bootstrap"] == [env]
    assert seen["db_health"] == [env]
    assert frontdoor_service.request_calls == [
        {
            "request_payload": request_payload,
            "env": env,
        }
    ]
    assert frontdoor_service.status_calls == [
        {
            "run_id": "run:workflow.native-self-hosted-smoke:operator-flow",
            "env": env,
        }
    ]
    assert result["native_instance"] == instance_contract
    assert result["bootstrap"]["schema_bootstrapped"] is True
    assert result["health"]["database_url"] == env["WORKFLOW_DATABASE_URL"]
    assert result["run"]["run_id"] == "run:workflow.native-self-hosted-smoke:operator-flow"
    assert result["run"]["admitted_definition_hash"] == request_payload["definition_hash"]
    assert result["run"]["admission_decision"] == {
        "decision": "admit",
    }
    assert result["run"]["inspection"]["terminal_reason"] == "runtime.workflow_succeeded"
    assert result["run"]["inspection"]["evidence_refs"] == [
        "workflow_event:workflow.native-self-hosted-smoke:1",
    ]
    assert "legacy-control" not in json.dumps(result, sort_keys=True)


def test_native_self_hosted_smoke_packages_the_checked_in_queue_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_payload = _load_queue()
    smoke_contract = _native_smoke_contract(queue_payload)
    request_payload = _request_payload(smoke_contract)
    env = _runtime_env(smoke_contract)
    registry = _registry(request_payload)
    seen: dict[str, object] = {}

    def _fake_load_smoke_registry(
        *,
        env: dict[str, str],
        request_payload: dict[str, object],
    ) -> RegistryResolver:
        seen["load_env"] = dict(env)
        seen["load_request_payload"] = dict(request_payload)
        return registry

    def _fake_run_local_operator_flow(
        *,
        request_payload: dict[str, object],
        env: dict[str, str] | None = None,
        registry: RegistryResolver | None = None,
        frontdoor_service=None,
    ) -> dict[str, object]:
        seen["flow_request_payload"] = dict(request_payload)
        seen["flow_env"] = dict(env or {})
        seen["flow_registry"] = registry
        seen["flow_frontdoor_service"] = frontdoor_service
        return {"status": "smoke_ok"}

    monkeypatch.setattr(
        operator_read,
        "load_native_self_hosted_smoke_contract",
        lambda: operator_read.NativeSelfHostedSmokeContract(
            queue_path="workflow_definitions:workflow_definition.native_self_hosted_smoke.v1",
            request_payload=request_payload,
            runtime_env=env,
        ),
    )
    monkeypatch.setattr(operator_read, "_load_smoke_registry", _fake_load_smoke_registry)
    monkeypatch.setattr(operator_read, "run_local_operator_flow", _fake_run_local_operator_flow)

    result = operator_read.run_native_self_hosted_smoke()

    assert result == {"status": "smoke_ok"}
    assert seen["load_env"] == env
    assert seen["flow_env"] == env
    assert seen["flow_registry"] == registry
    assert seen["flow_frontdoor_service"] is None
    for field_name in ("workspace_ref", "runtime_profile_ref", "nodes", "edges"):
        assert seen["load_request_payload"][field_name] == request_payload[field_name]
        assert seen["flow_request_payload"][field_name] == request_payload[field_name]
    isolated_workflow_id = seen["load_request_payload"]["workflow_id"]
    isolated_request_id = seen["load_request_payload"]["request_id"]
    isolated_definition_id = seen["load_request_payload"]["workflow_definition_id"]
    isolated_definition_hash = seen["load_request_payload"]["definition_hash"]
    assert isinstance(isolated_workflow_id, str)
    assert isinstance(isolated_request_id, str)
    assert isinstance(isolated_definition_id, str)
    assert isinstance(isolated_definition_hash, str)
    assert isolated_workflow_id.startswith(f"{request_payload['workflow_id']}.")
    assert isolated_request_id.startswith(f"{request_payload['request_id']}.")
    assert isolated_definition_id.startswith(f"{request_payload['workflow_definition_id']}.")
    assert isolated_definition_hash.startswith(f"{request_payload['definition_hash']}.")
    assert seen["flow_request_payload"] == seen["load_request_payload"]


def test_load_native_self_hosted_smoke_contract_fails_closed_on_missing_definition_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_conn = _FakeSmokeDatabaseConnection(row=None)
    seen: dict[str, dict[str, str]] = {}

    async def _connect_workflow_database(*, env: dict[str, str]):
        seen["env"] = dict(env)
        return fake_conn

    monkeypatch.setattr(operator_read, "connect_workflow_database", _connect_workflow_database)

    with pytest.raises(frontdoor.NativeFrontdoorError) as exc_info:
        operator_read.load_native_self_hosted_smoke_contract()

    assert exc_info.value.reason_code == "operator_flow.smoke_contract_missing"
    assert exc_info.value.details == {
        "workflow_definition_id": operator_read._SMOKE_WORKFLOW_DEFINITION_ID,
    }
    assert fake_conn.closed is True
    assert len(fake_conn.fetchrow_calls) == 1
    query, args = fake_conn.fetchrow_calls[0]
    assert query.count("workflow_definition_id = $1") == 1
    assert "workflow_id = $2" not in query
    assert args == (operator_read._SMOKE_WORKFLOW_DEFINITION_ID,)
    assert "env" in seen
    assert "WORKFLOW_DATABASE_URL" in seen["env"]


def test_load_native_self_hosted_smoke_contract_accepts_admitted_definition_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_payload = _load_queue()
    smoke_contract = _native_smoke_contract(queue_payload)
    request_payload = _request_payload(smoke_contract)
    runtime_env = operator_read._default_smoke_runtime_env()
    fake_conn = _FakeSmokeDatabaseConnection(
        row={
            "workflow_definition_id": operator_read._SMOKE_WORKFLOW_DEFINITION_ID,
            "workflow_id": request_payload["workflow_id"],
            "request_envelope": json.dumps(request_payload),
        }
    )

    async def _connect_workflow_database(*, env: dict[str, str]):
        assert env == runtime_env
        return fake_conn

    monkeypatch.setattr(operator_read, "connect_workflow_database", _connect_workflow_database)

    contract = operator_read.load_native_self_hosted_smoke_contract()

    assert contract.queue_path == (
        f"workflow_definitions:{operator_read._SMOKE_WORKFLOW_DEFINITION_ID}"
    )
    assert contract.request_payload == request_payload
    assert contract.runtime_env == runtime_env
    assert fake_conn.closed is True
    assert len(fake_conn.fetchrow_calls) == 1
    query, args = fake_conn.fetchrow_calls[0]
    assert "status IN ('active', 'admitted')" in query
    assert args == (operator_read._SMOKE_WORKFLOW_DEFINITION_ID,)


def test_operator_flow_requires_explicit_authority_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_payload = _load_queue()
    smoke_contract = _native_smoke_contract(queue_payload)
    request_payload = _request_payload(smoke_contract)
    registry = _registry(request_payload)
    frontdoor_service = _FakeFrontdoorService(
        request_calls=[],
        status_calls=[],
        native_instance_contract={},
    )

    monkeypatch.setattr(
        native_ops,
        "show_instance_contract",
        lambda *, env=None: (_ for _ in ()).throw(
            AssertionError("explicit env gate must fail before resolving instance authority")
        ),
    )

    with pytest.raises(frontdoor.NativeFrontdoorError) as exc_info:
        operator_read.run_local_operator_flow(
            request_payload=request_payload,
            registry=registry,
            frontdoor_service=frontdoor_service,
        )

    assert exc_info.value.reason_code == "operator_flow.authority_missing"
    assert frontdoor_service.request_calls == []
    assert frontdoor_service.status_calls == []


def test_operator_flow_fails_closed_when_native_instance_authority_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_payload = _load_queue()
    smoke_contract = _native_smoke_contract(queue_payload)
    request_payload = _request_payload(smoke_contract)
    env = _runtime_env(smoke_contract)
    registry = _registry(request_payload)
    frontdoor_service = _FakeFrontdoorService(
        request_calls=[],
        status_calls=[],
        native_instance_contract=_native_instance_contract(env),
    )
    seen = {
        "db_bootstrap": [],
        "db_health": [],
    }

    def _missing_instance(*, env=None):
        raise NativeInstanceResolutionError(
            "native_instance.profile_unknown",
            "runtime profile is not defined in the repo-local config",
        )

    monkeypatch.setattr(native_ops, "show_instance_contract", _missing_instance)
    monkeypatch.setattr(
        native_ops,
        "db_bootstrap",
        lambda *, env=None: seen["db_bootstrap"].append(dict(env or {})),
    )
    monkeypatch.setattr(
        native_ops,
        "db_health",
        lambda *, env=None: seen["db_health"].append(dict(env or {})),
    )

    with pytest.raises(NativeInstanceResolutionError):
        operator_read.run_local_operator_flow(
            request_payload=request_payload,
            env=env,
            registry=registry,
            frontdoor_service=frontdoor_service,
        )

    assert seen["db_bootstrap"] == []
    assert seen["db_health"] == []
    assert frontdoor_service.request_calls == []
    assert frontdoor_service.status_calls == []


def test_operator_flow_blocks_submit_when_bootstrap_does_not_prove_database_reachability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_payload = _load_queue()
    smoke_contract = _native_smoke_contract(queue_payload)
    request_payload = _request_payload(smoke_contract)
    env = _runtime_env(smoke_contract)
    registry = _registry(request_payload)
    instance_contract = _native_instance_contract(env)
    frontdoor_service = _FakeFrontdoorService(
        request_calls=[],
        status_calls=[],
        native_instance_contract=instance_contract,
    )
    seen: dict[str, list[dict[str, str]]] = {
        "show_instance_contract": [],
        "db_bootstrap": [],
        "db_health": [],
    }

    monkeypatch.setattr(
        native_ops,
        "show_instance_contract",
        lambda *, env=None: seen["show_instance_contract"].append(dict(env or {})) or instance_contract,
    )
    monkeypatch.setattr(
        native_ops,
        "db_bootstrap",
        lambda *, env=None: seen["db_bootstrap"].append(dict(env or {}))
        or {
            "database_url": env["WORKFLOW_DATABASE_URL"],
            "database_reachable": False,
            "schema_bootstrapped": False,
        },
    )
    monkeypatch.setattr(
        native_ops,
        "db_health",
        lambda *, env=None: seen["db_health"].append(dict(env or {}))
        or {
            "database_url": env["WORKFLOW_DATABASE_URL"],
            "database_reachable": True,
            "schema_bootstrapped": True,
        },
    )

    with pytest.raises(frontdoor.NativeFrontdoorError) as exc_info:
        operator_read.run_local_operator_flow(
            request_payload=request_payload,
            env=env,
            registry=registry,
            frontdoor_service=frontdoor_service,
        )

    assert exc_info.value.reason_code == "operator_flow.database_unreachable"
    assert exc_info.value.details["field"] == "bootstrap"
    assert seen["show_instance_contract"] == [env]
    assert seen["db_bootstrap"] == [env]
    assert seen["db_health"] == [env]
    assert frontdoor_service.request_calls == []
    assert frontdoor_service.status_calls == []


def test_operator_flow_blocks_submit_when_health_reports_schema_not_bootstrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_payload = _load_queue()
    smoke_contract = _native_smoke_contract(queue_payload)
    request_payload = _request_payload(smoke_contract)
    env = _runtime_env(smoke_contract)
    registry = _registry(request_payload)
    instance_contract = _native_instance_contract(env)
    frontdoor_service = _FakeFrontdoorService(
        request_calls=[],
        status_calls=[],
        native_instance_contract=instance_contract,
    )
    seen: dict[str, list[dict[str, str]]] = {
        "show_instance_contract": [],
        "db_bootstrap": [],
        "db_health": [],
    }

    monkeypatch.setattr(
        native_ops,
        "show_instance_contract",
        lambda *, env=None: seen["show_instance_contract"].append(dict(env or {})) or instance_contract,
    )
    monkeypatch.setattr(
        native_ops,
        "db_bootstrap",
        lambda *, env=None: seen["db_bootstrap"].append(dict(env or {}))
        or {
            "database_url": env["WORKFLOW_DATABASE_URL"],
            "database_reachable": True,
            "schema_bootstrapped": True,
        },
    )
    monkeypatch.setattr(
        native_ops,
        "db_health",
        lambda *, env=None: seen["db_health"].append(dict(env or {}))
        or {
            "database_url": env["WORKFLOW_DATABASE_URL"],
            "database_reachable": True,
            "schema_bootstrapped": False,
        },
    )

    with pytest.raises(frontdoor.NativeFrontdoorError) as exc_info:
        operator_read.run_local_operator_flow(
            request_payload=request_payload,
            env=env,
            registry=registry,
            frontdoor_service=frontdoor_service,
        )

    assert exc_info.value.reason_code == "operator_flow.schema_not_bootstrapped"
    assert exc_info.value.details["field"] == "health"
    assert seen["show_instance_contract"] == [env]
    assert seen["db_bootstrap"] == [env]
    assert seen["db_health"] == [env]
    assert frontdoor_service.request_calls == []
    assert frontdoor_service.status_calls == []
