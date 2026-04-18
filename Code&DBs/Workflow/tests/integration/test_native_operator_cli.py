from __future__ import annotations

import json
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

import pytest

from _pg_test_conn import ensure_test_database_ready
from contracts.domain import (
    MINIMAL_WORKFLOW_EDGE_TYPE,
    MINIMAL_WORKFLOW_NODE_TYPE,
    SUPPORTED_SCHEMA_VERSION,
    WorkflowEdgeContract,
    WorkflowNodeContract,
    WorkflowRequest,
)
from receipts import AppendOnlyWorkflowEvidenceWriter, EvidenceRow
from registry.domain import (
    RegistryResolver,
    RuntimeProfileAuthorityRecord,
    WorkspaceAuthorityRecord,
)
from runtime import RuntimeOrchestrator, WorkflowIntakePlanner
from runtime.instance import (
    PRAXIS_RUNTIME_PROFILE_ENV,
    PRAXIS_RUNTIME_PROFILES_CONFIG_ENV,
    NativeInstanceResolutionError,
    native_instance_contract,
)
from storage.postgres import resolve_workflow_database_url
from surfaces.api import frontdoor as native_frontdoor
from surfaces.cli import native_operator
from surfaces.cli.main import main as workflow_cli_main


REPO_ROOT = Path(__file__).resolve().parents[4]
_TEST_DATABASE_URL = ensure_test_database_ready()


def _repo_local_env() -> dict[str, str]:
    database_url = resolve_workflow_database_url(
        env={"WORKFLOW_DATABASE_URL": _TEST_DATABASE_URL}
    )
    return {
        "WORKFLOW_DATABASE_URL": database_url,
        PRAXIS_RUNTIME_PROFILES_CONFIG_ENV: str(REPO_ROOT / "config" / "runtime_profiles.json"),
        PRAXIS_RUNTIME_PROFILE_ENV: "praxis",
    }


def _request() -> WorkflowRequest:
    workspace_ref = "workspace.alpha"
    runtime_profile_ref = "runtime_profile.alpha"
    return WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.alpha",
        request_id="request.alpha",
        workflow_definition_id="workflow_definition.alpha.v1",
        definition_hash="sha256:1111222233334444",
        workspace_ref=workspace_ref,
        runtime_profile_ref=runtime_profile_ref,
        nodes=(
            WorkflowNodeContract(
                node_id="node_0",
                node_type=MINIMAL_WORKFLOW_NODE_TYPE,
                adapter_type=MINIMAL_WORKFLOW_NODE_TYPE,
                display_name="prepare",
                inputs={
                    "task_name": "prepare",
                    "input_payload": {"step": 0},
                },
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
                inputs={
                    "task_name": "admit",
                    "input_payload": {"step": 1},
                },
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
                payload_mapping={"prepared_result": "result"},
                position_index=0,
            ),
        ),
    )


def _resolver() -> RegistryResolver:
    workspace_ref = "workspace.alpha"
    runtime_profile_ref = "runtime_profile.alpha"
    return RegistryResolver(
        workspace_records={
            workspace_ref: (
                WorkspaceAuthorityRecord(
                    workspace_ref=workspace_ref,
                    repo_root="/tmp/workspace.alpha",
                    workdir="/tmp/workspace.alpha/workdir",
                ),
            ),
        },
        runtime_profile_records={
            runtime_profile_ref: (
                RuntimeProfileAuthorityRecord(
                    runtime_profile_ref=runtime_profile_ref,
                    model_profile_id="model.alpha",
                    provider_policy_id="provider_policy.alpha",
                    sandbox_profile_ref=runtime_profile_ref,
                ),
            ),
        },
    )


def _successful_run() -> tuple[str, tuple[EvidenceRow, ...]]:
    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=_request())
    writer = AppendOnlyWorkflowEvidenceWriter()
    RuntimeOrchestrator().execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=writer,
    )
    return outcome.run_id, tuple(writer.evidence_timeline(outcome.run_id))


@dataclass
class _FakeStatus:
    label: str

    def to_json(self) -> dict[str, str]:
        return {"label": self.label}


def test_native_operator_cli_stays_repo_local_and_composes_existing_surfaces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id, canonical_evidence = _successful_run()
    repo_local_env = _repo_local_env()

    health_calls: list[tuple[dict[str, str], bool]] = []
    status_calls: list[tuple[str, dict[str, str]]] = []
    evidence_reader_envs: list[dict[str, str]] = []
    evidence_reader_runs: list[str] = []

    def _health(*, env: dict[str, str] | None = None, bootstrap: bool = False) -> dict[str, object]:
        source = dict(env or {})
        health_calls.append((source, bootstrap))
        return {
            "native_instance": native_instance_contract(env=env),
            "database": {"label": "bootstrap" if bootstrap else "health"},
        }

    def _status(*, run_id: str, env: dict[str, str] | None = None) -> dict[str, object]:
        source = dict(env or {})
        status_calls.append((run_id, source))
        return {
            "native_instance": native_instance_contract(env=env),
            "run": {
                "run_id": run_id,
                "workflow_id": "workflow.alpha",
                "current_state": "claim_accepted",
            },
        }

    class _FakeEvidenceReader:
        def __init__(self, *, env: dict[str, str] | None = None) -> None:
            evidence_reader_envs.append(dict(env or {}))

        def evidence_timeline(self, run_id: str) -> tuple[EvidenceRow, ...]:
            evidence_reader_runs.append(run_id)
            return canonical_evidence

    monkeypatch.setattr(native_frontdoor, "health", _health)
    monkeypatch.setattr(native_frontdoor, "status", _status)
    monkeypatch.setattr(native_operator, "PostgresEvidenceReader", _FakeEvidenceReader)
    monkeypatch.setattr(
        native_operator,
        "run_native_self_hosted_smoke",
        lambda: {
            "smoke": "ok",
            "native_instance": native_instance_contract(env=repo_local_env),
        },
    )

    instance_stdout = StringIO()
    assert (
        workflow_cli_main(
            ["native-operator", "instance"],
            env=repo_local_env,
            stdout=instance_stdout,
        )
        == 0
    )
    instance_payload = json.loads(instance_stdout.getvalue())
    assert instance_payload["repo_root"] == str(REPO_ROOT)
    assert instance_payload["workdir"] == str(REPO_ROOT)

    health_stdout = StringIO()
    assert (
        workflow_cli_main(
            ["native-operator", "health"],
            env=repo_local_env,
            stdout=health_stdout,
        )
        == 0
    )
    assert json.loads(health_stdout.getvalue()) == {
        "database": {"label": "health"},
        "native_instance": native_instance_contract(env=repo_local_env),
    }

    bootstrap_stdout = StringIO()
    assert (
        workflow_cli_main(
            ["native-operator", "bootstrap"],
            env=repo_local_env,
            stdout=bootstrap_stdout,
        )
        == 0
    )
    assert json.loads(bootstrap_stdout.getvalue()) == {
        "database": {"label": "bootstrap"},
        "native_instance": native_instance_contract(env=repo_local_env),
    }

    smoke_stdout = StringIO()
    assert (
        workflow_cli_main(
            ["native-operator", "smoke"],
            env=repo_local_env,
            stdout=smoke_stdout,
        )
        == 0
    )
    assert json.loads(smoke_stdout.getvalue()) == {
        "native_instance": native_instance_contract(env=repo_local_env),
        "smoke": "ok",
    }

    inspect_stdout = StringIO()
    assert (
        workflow_cli_main(
            ["native-operator", "inspect", run_id],
            env=repo_local_env,
            stdout=inspect_stdout,
        )
        == 0
    )
    inspect_rendered = inspect_stdout.getvalue()
    assert "kind: inspection" in inspect_rendered
    assert f"run_id: {run_id}" in inspect_rendered
    assert "completeness: complete" in inspect_rendered

    status_stdout = StringIO()
    assert (
        workflow_cli_main(
            ["native-operator", "status", run_id],
            env=repo_local_env,
            stdout=status_stdout,
        )
        == 0
    )
    status_payload = json.loads(status_stdout.getvalue())
    assert status_payload["native_instance"]["repo_root"] == str(REPO_ROOT)
    assert status_payload["run"]["run_id"] == run_id
    assert status_payload["run"]["current_state"] == "claim_accepted"

    topology_stdout = StringIO()
    assert (
        workflow_cli_main(
            ["native-operator", "graph-topology", run_id],
            env=repo_local_env,
            stdout=topology_stdout,
        )
        == 0
    )
    topology_rendered = topology_stdout.getvalue()
    assert "kind: graph_topology" in topology_rendered
    assert f"run_id: {run_id}" in topology_rendered
    assert "nodes_count: 2" in topology_rendered
    assert "edges_count: 1" in topology_rendered

    lineage_stdout = StringIO()
    assert (
        workflow_cli_main(
            ["native-operator", "graph-lineage", run_id],
            env=repo_local_env,
            stdout=lineage_stdout,
        )
        == 0
    )
    lineage_rendered = lineage_stdout.getvalue()
    assert "kind: graph_lineage" in lineage_rendered
    assert f"run_id: {run_id}" in lineage_rendered
    assert "claim_received_ref:" in lineage_rendered

    assert health_calls == [
        (repo_local_env, False),
        (repo_local_env, True),
    ]
    assert status_calls == [
        (run_id, repo_local_env),
        (run_id, repo_local_env),
        (run_id, repo_local_env),
    ]
    assert evidence_reader_envs == [repo_local_env, repo_local_env, repo_local_env]
    assert evidence_reader_runs == [run_id, run_id, run_id, run_id]


@pytest.mark.parametrize(
    ("argv", "expected_reason_code"),
    [
        (["native-operator", "health"], "native_instance.boundary_mismatch"),
        (["native-operator", "bootstrap"], "native_instance.boundary_mismatch"),
        (["native-operator", "graph-topology", "run.orphan"], "native_instance.boundary_mismatch"),
        (["native-operator", "graph-lineage", "run.orphan"], "native_instance.boundary_mismatch"),
    ],
)
def test_native_operator_cli_fails_closed_on_native_instance_mismatch(
    argv: list[str],
    expected_reason_code: str,
) -> None:
    bad_env = {
        **_repo_local_env(),
        "PRAXIS_RECEIPTS_DIR": "/tmp/legacy-control/runtime_receipts",
    }

    with pytest.raises(NativeInstanceResolutionError) as exc_info:
        workflow_cli_main(
            argv,
            env=bad_env,
            stdout=StringIO(),
        )

    assert exc_info.value.reason_code == expected_reason_code
    assert exc_info.value.details["environment_variable"] == "PRAXIS_RECEIPTS_DIR"


def test_native_operator_cli_start_is_removed_and_points_to_instance() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["native-operator", "start"], env=_repo_local_env(), stdout=stdout) == 2

    rendered = stdout.getvalue()
    assert "start has been removed" in rendered
    assert "workflow native-operator instance" in rendered


@pytest.mark.parametrize(
    "argv",
    [
        ["native-operator", "graph-topology", "run.orphan"],
        ["native-operator", "graph-lineage", "run.orphan"],
    ],
)
def test_native_operator_cli_graph_reads_fail_closed_when_frontdoor_cannot_prove_run(
    monkeypatch: pytest.MonkeyPatch,
    argv: list[str],
) -> None:
    repo_local_env = _repo_local_env()
    evidence_reader_init_count = 0
    evidence_reader_run_count = 0

    def _missing_status(*, run_id: str, env: dict[str, str] | None = None) -> dict[str, object]:
        raise native_frontdoor.NativeFrontdoorError(
            "frontdoor.run_missing",
            "run_id is not present in the native control plane",
            details={"run_id": run_id, "env": dict(env or {})},
        )

    class _ForbiddenEvidenceReader:
        def __init__(self, *, env: dict[str, str] | None = None) -> None:
            nonlocal evidence_reader_init_count
            evidence_reader_init_count += 1

        def evidence_timeline(self, run_id: str) -> tuple[EvidenceRow, ...]:
            nonlocal evidence_reader_run_count
            evidence_reader_run_count += 1
            raise AssertionError("graph reads must not load evidence before frontdoor run verification")

    monkeypatch.setattr(native_frontdoor, "status", _missing_status)
    monkeypatch.setattr(native_operator, "PostgresEvidenceReader", _ForbiddenEvidenceReader)

    with pytest.raises(native_frontdoor.NativeFrontdoorError) as exc_info:
        workflow_cli_main(
            argv,
            env=repo_local_env,
            stdout=StringIO(),
        )

    assert exc_info.value.reason_code == "frontdoor.run_missing"
    assert evidence_reader_init_count == 0
    assert evidence_reader_run_count == 0
