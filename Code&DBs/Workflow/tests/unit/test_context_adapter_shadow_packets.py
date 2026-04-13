from __future__ import annotations

import json
from types import SimpleNamespace
from datetime import datetime, timezone

from adapters.context_adapter import ContextCompilerAdapter
from adapters.deterministic import (
    AdapterRegistry,
    DeterministicTaskRequest,
    DeterministicTaskResult,
)
from contracts.domain import WorkflowEdgeContract, WorkflowNodeContract, WorkflowRequest
from receipts.evidence import AppendOnlyWorkflowEvidenceWriter
from registry.domain import RegistryResolver, RuntimeProfileAuthorityRecord, WorkspaceAuthorityRecord
from runtime.execution import RuntimeOrchestrator
from runtime.intake import WorkflowIntakePlanner
from runtime.workflow.orchestrator import WorkflowSpec as RuntimeWorkflowSpec
from runtime.workflow.runtime_setup import _build_workflow_graph
from runtime.workflow.runtime_setup import _shadow_packet_config as _runtime_shadow_packet_config


class _PacketConn:
    def __init__(self) -> None:
        self.rows: list[dict[str, object]] = []
        self.compile_artifact_rows: list[dict[str, object]] = []

    def execute(self, query: str, *args):
        if "INSERT INTO compile_artifacts" in query:
            self.compile_artifact_rows.append(
                {
                    "compile_artifact_id": args[0],
                    "artifact_kind": args[1],
                    "artifact_ref": args[2],
                    "revision_ref": args[3],
                    "parent_artifact_ref": args[4],
                    "input_fingerprint": args[5],
                    "content_hash": args[6],
                    "authority_refs": json.loads(args[7]),
                    "payload": json.loads(args[8]),
                    "decision_ref": args[9],
                }
            )
            return []
        if "FROM compile_artifacts" in query:
            artifact_kind = args[0]
            input_fingerprint = args[1]
            return [
                row
                for row in self.compile_artifact_rows
                if row["artifact_kind"] == artifact_kind and row["input_fingerprint"] == input_fingerprint
            ]
        if "INSERT INTO execution_packets" not in query:
            return []
        self.rows = [
            {
                "execution_packet_id": args[0],
                "definition_revision": args[1],
                "plan_revision": args[2],
                "packet_revision": args[3],
                "parent_artifact_ref": args[4],
                "packet_version": args[5],
                "packet_hash": args[6],
                "workflow_id": args[7],
                "run_id": args[8],
                "spec_name": args[9],
                "source_kind": args[10],
                "authority_refs": json.loads(args[11]),
                "model_messages": json.loads(args[12]),
                "reference_bindings": json.loads(args[13]),
                "capability_bindings": json.loads(args[14]),
                "verify_refs": json.loads(args[15]),
                "authority_inputs": json.loads(args[16]),
                "file_inputs": json.loads(args[17]),
                "payload": json.loads(args[18]),
                "decision_ref": args[19],
            }
        ]
        return []


class _PacketAwareLLMAdapter:
    executor_type = "adapter.cli_llm"

    def __init__(self, conn: _PacketConn) -> None:
        self._conn = conn
        self.seen_packet_revisions: list[str] = []
        self.seen_payloads: list[dict[str, object]] = []

    def execute(self, *, request: DeterministicTaskRequest) -> DeterministicTaskResult:
        payload = dict(request.input_payload)
        if request.dependency_inputs:
            for key, value in request.dependency_inputs.items():
                if isinstance(value, dict):
                    payload.update(value)
                else:
                    payload[key] = value
        assert self._conn.rows, "shadow packet must exist before the LLM adapter executes"
        self.seen_payloads.append(payload)
        self.seen_packet_revisions.append(str(self._conn.rows[0]["packet_revision"]))
        now = datetime.now(timezone.utc)
        return DeterministicTaskResult(
            node_id=request.node_id,
            task_name=request.task_name,
            status="succeeded",
            reason_code="adapter.execution_succeeded",
            executor_type=self.executor_type,
            inputs={"prompt": payload.get("prompt", "")},
            outputs={"completion": "done"},
            started_at=now,
            finished_at=now,
        )


def _request(*, workdir: str) -> DeterministicTaskRequest:
    return DeterministicTaskRequest(
        node_id="context",
        task_name="compile context",
        input_payload={
            "prompt": "Implement the change exactly.",
            "provider_slug": "openai",
            "model_slug": "gpt-5.4",
            "scope_write": ["app.py"],
            "workdir": workdir,
            "system_prompt": "Stay inside scope.",
            "shadow_packet_runtime": {
                "workflow_id": "workflow.alpha",
                "run_id": "run.alpha",
                "request_id": "request.alpha",
                "workflow_definition_id": "workflow_definition.alpha",
                "definition_hash": "sha256:alpha",
                "validation_result_ref": "validation.alpha",
                "admission_decision_id": "admission.alpha",
                "authority_context_ref": "context:run.alpha",
                "authority_context_digest": "digest.alpha",
                "context_bundle_id": "context:run.alpha",
                "context_bundle_hash": "digest.alpha",
                "context_bundle_payload": {
                    "workflow_id": "workflow.alpha",
                    "run_id": "run.alpha",
                    "workspace": {
                        "workspace_ref": "workspace.alpha",
                        "workdir": workdir,
                    },
                },
                "workspace_ref": "workspace.alpha",
                "runtime_profile_ref": "praxis",
                "source_decision_refs": ["validation.alpha"],
            },
        },
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace.alpha",
    )


def _shadow_packet_config() -> dict[str, object]:
    return {
        "adapter_type": "cli_llm",
        "allowed_tools": ["rg", "pytest"],
        "capabilities": ["capability.alpha"],
        "definition_revision": "def_alpha",
        "job_label": "job.alpha",
        "packet_provenance": {
            "source_kind": "workflow_runtime",
            "authority_inputs": {"authority_owner": "test"},
            "file_inputs": {"ticket": "T-1"},
            "definition_row": {
                "definition_revision": "def_alpha",
                "references": [
                    {
                        "type": "integration",
                        "slug": "@repo/search",
                        "resolved_to": "integration_registry:repo/search",
                    }
                ],
                "capabilities": [{"slug": "capability.alpha"}],
            },
            "compiled_spec_row": {
                "definition_revision": "def_alpha",
                "plan_revision": "plan_alpha",
            },
        },
        "plan_revision": "plan_alpha",
        "task_type": "build",
        "verify_refs": ["verify_ref.python.py_compile.test"],
    }


def _explicit_job_shadow_packet_config() -> dict[str, object]:
    return {
        "adapter_type": "cli_llm",
        "allowed_tools": [],
        "capabilities": [],
        "definition_revision": "def_alpha",
        "job_label": "job.alpha",
        "packet_provenance": {
            "source_kind": "workflow_runtime",
            "authority_inputs": {"authority_owner": "test"},
            "file_inputs": {"ticket": "T-2"},
            "definition_row": {
                "definition_revision": "def_alpha",
                "references": [
                    {
                        "type": "integration",
                        "slug": "@repo/search",
                        "resolved_to": "integration_registry:repo/search",
                    }
                ],
                "capabilities": [{"slug": "capability.definition"}],
                "jobs": [
                    {
                        "label": "job.alpha",
                        "reference_slugs": ["@repo/search"],
                        "capabilities": ["capability.definition"],
                    }
                ],
            },
            "compiled_spec_row": {
                "definition_revision": "def_alpha",
                "plan_revision": "plan_alpha",
                "jobs": [
                    {
                        "label": "job.alpha",
                        "prompt": "Implement the change exactly.",
                        "depends_on": ["prep.alpha"],
                        "reference_slugs": ["@repo/search", "review-agent"],
                        "capabilities": ["capability.explicit"],
                        "allowed_tools": ["rg", "fd"],
                        "verify_refs": ["verify_ref.python.explicit_job"],
                    }
                ],
            },
        },
        "plan_revision": "plan_alpha",
        "task_type": "build",
        "verify_refs": [],
    }


def _registry() -> RegistryResolver:
    return RegistryResolver(
        workspace_records={
            "workspace.alpha": [
                WorkspaceAuthorityRecord(
                    workspace_ref="workspace.alpha",
                    repo_root="/tmp/workspace.alpha",
                    workdir="/tmp/workspace.alpha",
                )
            ],
        },
        runtime_profile_records={
            "runtime_profile.alpha": [
                RuntimeProfileAuthorityRecord(
                    runtime_profile_ref="runtime_profile.alpha",
                    model_profile_id="model_profile.alpha",
                    provider_policy_id="provider_policy.alpha",
                )
            ],
        },
    )


def _workflow_request(*, workdir: str, packet_only: bool = False) -> WorkflowRequest:
    authority_requirements = {
        "workspace_ref": "workspace.alpha",
        "runtime_profile_ref": "runtime_profile.alpha",
    }
    execution_boundary = {"workspace_ref": "workspace.alpha"}
    return WorkflowRequest(
        schema_version=1,
        workflow_id="workflow.alpha",
        request_id="request.alpha",
        workflow_definition_id="workflow_definition.alpha",
        definition_hash="sha256:workflow-alpha",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=(
            WorkflowNodeContract(
                node_id="context",
                node_type="deterministic_task",
                adapter_type="context_compiler",
                display_name="compile context",
                inputs={
                    "prompt": "Implement the change exactly.",
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4",
                    "scope_write": ["app.py"],
                    "workdir": workdir,
                    "system_prompt": "Stay inside scope.",
                },
                expected_outputs={},
                success_condition={"kind": "always"},
                failure_behavior={"kind": "stop"},
                authority_requirements=authority_requirements,
                execution_boundary=execution_boundary,
                position_index=0,
            ),
            WorkflowNodeContract(
                node_id="llm",
                node_type="deterministic_task",
                adapter_type="cli_llm",
                display_name="run llm",
                inputs={
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4",
                    **(
                        {
                            "packet_required": True,
                            "definition_revision": "def_alpha",
                            "plan_revision": "plan_alpha",
                        }
                        if packet_only
                        else {}
                    ),
                },
                expected_outputs={},
                success_condition={"kind": "always"},
                failure_behavior={"kind": "stop"},
                authority_requirements=authority_requirements,
                execution_boundary=execution_boundary,
                position_index=1,
            ),
            WorkflowNodeContract(
                node_id="terminal",
                node_type="deterministic_task",
                adapter_type="deterministic_task",
                display_name="terminal",
                inputs={},
                expected_outputs={"terminal": True},
                success_condition={"kind": "always"},
                failure_behavior={"kind": "stop"},
                authority_requirements=authority_requirements,
                execution_boundary=execution_boundary,
                position_index=2,
            ),
        ),
        edges=(
            WorkflowEdgeContract(
                edge_id="edge_0",
                edge_type="after_success",
                from_node_id="context",
                to_node_id="llm",
                release_condition={"kind": "always"},
                payload_mapping=(
                    {
                        "execution_packet_ref": "execution_packet_ref",
                        "execution_packet_hash": "execution_packet_hash",
                    }
                    if packet_only
                    else {"prompt": "user_message", "system_prompt": "system_message"}
                ),
                position_index=0,
            ),
            WorkflowEdgeContract(
                edge_id="edge_1",
                edge_type="after_success",
                from_node_id="llm",
                to_node_id="terminal",
                release_condition={"kind": "always"},
                payload_mapping={},
                position_index=1,
            ),
        ),
        requested_at=datetime.now(timezone.utc),
    )


def test_context_compiler_persists_deterministic_shadow_execution_packet(tmp_path) -> None:
    (tmp_path / "app.py").write_text("def greet() -> str:\n    return 'hi'\n", encoding="utf-8")

    conn_one = _PacketConn()
    conn_two = _PacketConn()
    adapter_one = ContextCompilerAdapter(
        shadow_packet_config=_shadow_packet_config(),
        conn_factory=lambda: conn_one,
    )
    adapter_two = ContextCompilerAdapter(
        shadow_packet_config=_shadow_packet_config(),
        conn_factory=lambda: conn_two,
    )

    result_one = adapter_one.execute(request=_request(workdir=str(tmp_path)))
    result_two = adapter_two.execute(request=_request(workdir=str(tmp_path)))

    assert result_one.status == "succeeded"
    assert result_two.status == "succeeded"
    assert "user_message" not in result_one.outputs
    assert "system_message" not in result_one.outputs
    assert result_one.outputs["shadow_execution_packet_ref"] == conn_one.rows[0]["packet_revision"]
    assert result_one.outputs["shadow_execution_packet_hash"] == conn_one.rows[0]["packet_hash"]
    assert conn_one.rows[0]["packet_hash"] == conn_two.rows[0]["packet_hash"]
    assert conn_one.rows[0]["packet_revision"] == conn_two.rows[0]["packet_revision"]

    row = conn_one.rows[0]
    assert row["workflow_id"] == "workflow.alpha"
    assert row["model_messages"][0]["messages"][0]["content"].startswith("Provider: openai")
    assert "Implement the change exactly." in row["model_messages"][0]["messages"][1]["content"]
    assert "--- EXECUTION CONTROL BUNDLE ---" in row["model_messages"][0]["messages"][1]["content"]
    assert "app.py" in row["payload"]["scope_reads"]
    assert row["payload"]["scope_writes"] == ["app.py"]
    assert row["reference_bindings"][0]["reference_slugs"] == ["@repo/search"]
    assert row["capability_bindings"][0]["capabilities"] == ["capability.alpha"]
    assert "Read" in row["capability_bindings"][0]["allowed_tools"]
    assert "pytest" in row["capability_bindings"][0]["allowed_tools"]
    assert row["capability_bindings"][0]["tool_bucket"] == "build"
    assert "praxis_query" in row["capability_bindings"][0]["mcp_tools"]
    assert "workflow" in row["capability_bindings"][0]["skill_refs"]
    assert row["verify_refs"] == ["verify_ref.python.py_compile.test"]
    assert row["authority_inputs"]["shadow_runtime"]["workflow_definition_id"] == "workflow_definition.alpha"
    assert row["file_inputs"]["packet_file_inputs"] == {"ticket": "T-1"}
    assert row["file_inputs"]["execution_bundle"]["tool_bucket"] == "build"
    assert "praxis_query" in row["file_inputs"]["execution_bundle"]["mcp_tool_names"]
    assert row["file_inputs"]["execution_bundle"]["access_policy"]["write_scope"] == ["app.py"]


def test_context_compiler_fails_closed_when_shadow_packet_authority_is_missing(tmp_path) -> None:
    (tmp_path / "app.py").write_text("def greet() -> str:\n    return 'hi'\n", encoding="utf-8")

    conn = _PacketConn()
    adapter = ContextCompilerAdapter(
        shadow_packet_config={
            "adapter_type": "cli_llm",
            "job_label": "job.alpha",
            "plan_revision": "plan_alpha",
        },
        conn_factory=lambda: conn,
    )

    result = adapter.execute(request=_request(workdir=str(tmp_path)))

    assert result.status == "failed"
    assert result.reason_code == "shadow_packet.definition_revision_missing"
    assert result.failure_code == "shadow_packet.definition_revision_missing"
    assert conn.rows == []


def test_context_compiler_rejects_legacy_shadow_packet_verify_fields(tmp_path) -> None:
    (tmp_path / "app.py").write_text("def greet() -> str:\n    return 'hi'\n", encoding="utf-8")

    conn = _PacketConn()
    shadow_packet_config = _shadow_packet_config()
    shadow_packet_config = dict(shadow_packet_config)
    shadow_packet_config.pop("verify_refs", None)
    shadow_packet_config["verify"] = [
        {
            "verification_ref": "verification.python.py_compile",
            "inputs": {"path": "app.py"},
        }
    ]
    adapter = ContextCompilerAdapter(
        shadow_packet_config=shadow_packet_config,
        conn_factory=lambda: conn,
    )

    result = adapter.execute(request=_request(workdir=str(tmp_path)))

    assert result.status == "failed"
    assert result.reason_code == "shadow_packet.verify_authority_missing"
    assert result.failure_code == "shadow_packet.verify_authority_missing"
    assert conn.rows == []


def test_context_compiler_hydrates_shadow_packet_from_explicit_job_authority(tmp_path) -> None:
    (tmp_path / "app.py").write_text("def greet() -> str:\n    return 'hi'\n", encoding="utf-8")

    conn = _PacketConn()
    adapter = ContextCompilerAdapter(
        shadow_packet_config=_explicit_job_shadow_packet_config(),
        conn_factory=lambda: conn,
    )

    result = adapter.execute(request=_request(workdir=str(tmp_path)))

    assert result.status == "succeeded"
    row = conn.rows[0]
    assert row["reference_bindings"][0]["depends_on"] == ["prep.alpha"]
    assert row["reference_bindings"][0]["reference_slugs"] == ["@repo/search", "review-agent"]
    assert row["capability_bindings"][0]["capabilities"] == ["capability.explicit", "capability.definition"]
    assert "Read" in row["capability_bindings"][0]["allowed_tools"]
    assert "rg" in row["capability_bindings"][0]["allowed_tools"]
    assert "fd" in row["capability_bindings"][0]["allowed_tools"]
    assert row["capability_bindings"][0]["tool_bucket"] == "build"
    assert "praxis_query" in row["capability_bindings"][0]["mcp_tools"]
    assert row["verify_refs"] == ["verify_ref.python.explicit_job"]
    assert row["authority_inputs"]["compiled_job_row"]["label"] == "job.alpha"
    assert row["authority_inputs"]["definition_job_row"]["label"] == "job.alpha"


def test_context_compiler_reuses_shadow_packet_lineage_on_retry(tmp_path) -> None:
    (tmp_path / "app.py").write_text("def greet() -> str:\n    return 'hi'\n", encoding="utf-8")

    conn = _PacketConn()
    adapter = ContextCompilerAdapter(
        shadow_packet_config=_shadow_packet_config(),
        conn_factory=lambda: conn,
    )

    first = adapter.execute(request=_request(workdir=str(tmp_path)))
    second = adapter.execute(request=_request(workdir=str(tmp_path)))

    assert first.status == "succeeded"
    assert second.status == "succeeded"
    assert len(conn.compile_artifact_rows) == 1
    assert conn.rows[0]["payload"]["compile_provenance"]["reuse"]["decision"] == "reused"
    assert conn.rows[0]["parent_artifact_ref"] == conn.compile_artifact_rows[0]["revision_ref"]


def test_context_compiler_rejects_stale_shadow_packet_lineage(tmp_path) -> None:
    (tmp_path / "app.py").write_text("def greet() -> str:\n    return 'hi'\n", encoding="utf-8")

    conn = _PacketConn()
    adapter = ContextCompilerAdapter(
        shadow_packet_config=_shadow_packet_config(),
        conn_factory=lambda: conn,
    )

    first = adapter.execute(request=_request(workdir=str(tmp_path)))
    assert first.status == "succeeded"
    conn.compile_artifact_rows[0]["content_hash"] = "corrupt"

    second = adapter.execute(request=_request(workdir=str(tmp_path)))

    assert second.status == "failed"
    assert second.reason_code == "shadow_packet.reuse_failed_closed"
    assert second.failure_code == "shadow_packet.reuse_failed_closed"


def test_context_compiler_fails_closed_when_reference_binding_authority_is_missing(tmp_path) -> None:
    (tmp_path / "app.py").write_text("def greet() -> str:\n    return 'hi'\n", encoding="utf-8")

    conn = _PacketConn()
    adapter = ContextCompilerAdapter(
        shadow_packet_config={
            "adapter_type": "cli_llm",
            "allowed_tools": [],
            "capabilities": [],
            "definition_revision": "def_alpha",
            "job_label": "job.alpha",
            "packet_provenance": {
                "source_kind": "workflow_runtime",
                "compiled_spec_row": {
                    "definition_revision": "def_alpha",
                    "plan_revision": "plan_alpha",
                    "jobs": [],
                },
                "definition_row": {
                    "definition_revision": "def_alpha",
                    "capabilities": [],
                    "jobs": [],
                },
            },
            "plan_revision": "plan_alpha",
            "task_type": "build",
            "verify_refs": [],
        },
        conn_factory=lambda: conn,
    )

    result = adapter.execute(request=_request(workdir=str(tmp_path)))

    assert result.status == "failed"
    assert result.reason_code == "shadow_packet.reference_authority_missing"
    assert result.failure_code == "shadow_packet.reference_authority_missing"
    assert conn.rows == []


def test_runtime_graph_builds_shadow_packet_before_llm_execution(tmp_path) -> None:
    (tmp_path / "app.py").write_text("def greet() -> str:\n    return 'hi'\n", encoding="utf-8")

    request = _workflow_request(workdir=str(tmp_path), packet_only=True)
    intake = WorkflowIntakePlanner(registry=_registry()).plan(request=request)
    packet_conn = _PacketConn()
    llm_adapter = _PacketAwareLLMAdapter(packet_conn)
    adapter_registry = AdapterRegistry(cli_llm_adapter=llm_adapter)
    adapter_registry.register(
        "context_compiler",
        ContextCompilerAdapter(
            shadow_packet_config=_shadow_packet_config(),
            conn_factory=lambda: packet_conn,
        ),
    )
    evidence_writer = AppendOnlyWorkflowEvidenceWriter()

    result = RuntimeOrchestrator(
        adapter_registry=adapter_registry,
        evidence_reader=evidence_writer,
    ).execute_deterministic_path(
        intake_outcome=intake,
        evidence_writer=evidence_writer,
    )

    assert llm_adapter.seen_packet_revisions == [packet_conn.rows[0]["packet_revision"]]
    assert llm_adapter.seen_payloads[0]["packet_required"] is True
    assert llm_adapter.seen_payloads[0]["execution_packet_ref"] == packet_conn.rows[0]["packet_revision"]
    assert llm_adapter.seen_payloads[0]["execution_packet_hash"] == packet_conn.rows[0]["packet_hash"]
    assert "prompt" not in llm_adapter.seen_payloads[0]
    assert "system_prompt" not in llm_adapter.seen_payloads[0]
    assert packet_conn.rows[0]["run_id"] == intake.run_id
    assert "Implement the change exactly." in packet_conn.rows[0]["model_messages"][0]["messages"][1]["content"]
    assert packet_conn.rows[0]["payload"]["scope_writes"] == ["app.py"]
    assert result.current_state.value == "succeeded"


def test_runtime_setup_routes_migrated_specs_through_packet_only_edge() -> None:
    spec = RuntimeWorkflowSpec(
        prompt="Implement the change exactly.",
        provider_slug="openai",
        model_slug="gpt-5.4",
        adapter_type="cli_llm",
        workdir="/tmp/workspace.alpha",
        scope_write=["app.py"],
        definition_revision="def_alpha",
        plan_revision="plan_alpha",
    )

    request = _build_workflow_graph(spec)

    llm_node = next(node for node in request.nodes if node.node_id == "llm")
    context_to_llm = next(edge for edge in request.edges if edge.from_node_id == "context" and edge.to_node_id == "llm")

    assert llm_node.inputs["packet_required"] is True
    assert llm_node.inputs["definition_revision"] == "def_alpha"
    assert llm_node.inputs["plan_revision"] == "plan_alpha"
    assert context_to_llm.payload_mapping == {
        "execution_packet_ref": "execution_packet_ref",
        "execution_packet_hash": "execution_packet_hash",
    }


def test_non_migrated_runtime_graph_skips_shadow_packet_without_plan_authority(tmp_path) -> None:
    (tmp_path / "app.py").write_text("def greet() -> str:\n    return 'hi'\n", encoding="utf-8")

    request = _workflow_request(workdir=str(tmp_path))
    intake = WorkflowIntakePlanner(registry=_registry()).plan(request=request)
    packet_conn = _PacketConn()

    class _NoPacketLLMAdapter:
        executor_type = "adapter.cli_llm"

        def execute(self, *, request: DeterministicTaskRequest) -> DeterministicTaskResult:
            payload = dict(request.input_payload)
            if request.dependency_inputs:
                for key, value in request.dependency_inputs.items():
                    if isinstance(value, dict):
                        payload.update(value)
                    else:
                        payload[key] = value
            now = datetime.now(timezone.utc)
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="succeeded",
                reason_code="adapter.execution_succeeded",
                executor_type=self.executor_type,
                inputs={"prompt": payload.get("prompt", "")},
                outputs={"completion": "done"},
                started_at=now,
                finished_at=now,
            )

    llm_adapter = _NoPacketLLMAdapter()
    spec = SimpleNamespace(
        adapter_type="cli_llm",
        allowed_tools=None,
        capabilities=None,
        definition_revision="def_alpha",
        label="job.alpha",
        packet_provenance=None,
        plan_revision=None,
        task_type="build",
        verify_refs=["verify_ref.python.py_compile.test"],
    )
    adapter_registry = AdapterRegistry(cli_llm_adapter=llm_adapter)
    adapter_registry.register(
        "context_compiler",
        ContextCompilerAdapter(
            shadow_packet_config=_runtime_shadow_packet_config(spec),
            conn_factory=lambda: packet_conn,
        ),
    )
    evidence_writer = AppendOnlyWorkflowEvidenceWriter()

    result = RuntimeOrchestrator(
        adapter_registry=adapter_registry,
        evidence_reader=evidence_writer,
    ).execute_deterministic_path(
        intake_outcome=intake,
        evidence_writer=evidence_writer,
    )

    assert _runtime_shadow_packet_config(spec) is None
    assert packet_conn.rows == []
    assert result.current_state.value == "succeeded"
