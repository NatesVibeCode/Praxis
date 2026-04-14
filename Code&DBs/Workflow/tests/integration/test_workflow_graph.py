"""Integration test: full dispatch graph with mock adapter.

Proves the workflow graph foundation works without spending API credits:
- Graph builds correctly from WorkflowSpec
- All nodes execute in order via RuntimeOrchestrator
- Outputs flow between nodes via edge payload_mapping
- Evidence is recorded per node (evidence_seq monotonic)
- File writer applies code blocks to disk
- WorkflowResult is projected from the evidence timeline
- Content-hash produces stable definition IDs for identical graphs
"""

from __future__ import annotations
import os
import sys
import tempfile
from pathlib import Path

import pytest

WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKFLOW_ROOT))

from adapters.deterministic import (
    AdapterRegistry,
    DeterministicTaskAdapter,
    DeterministicTaskRequest,
    DeterministicTaskResult,
    TaskAdapter,
)
from contracts.domain import WorkflowRequest
from receipts.evidence import AppendOnlyWorkflowEvidenceWriter, EvidenceRow
from runtime.workflow import WorkflowSpec, _build_workflow_graph, _build_registry
from runtime.workflow_projection import project_workflow_result
from runtime.execution import RuntimeOrchestrator
from runtime.intake import WorkflowIntakePlanner


# ---------------------------------------------------------------------------
# Mock LLM adapter — returns structured output without calling a real model
# ---------------------------------------------------------------------------

class MockLLMAdapter:
    """Mock adapter that returns structured output as if a model produced it."""

    executor_type = "adapter.cli_llm"

    def execute(self, *, request: DeterministicTaskRequest) -> DeterministicTaskResult:
        from datetime import datetime, timezone
        payload = dict(request.input_payload)

        # Merge dependency_inputs (same pattern as real adapter)
        if request.dependency_inputs:
            for k, v in request.dependency_inputs.items():
                if isinstance(v, dict):
                    payload.update(v)
                else:
                    payload[k] = v

        prompt = payload.get("prompt", "")
        scope_write = payload.get("scope_write", [])
        target = scope_write[0] if scope_write else "output.py"

        import json
        # Return raw JSON as completion — the parser node extracts code blocks
        completion_json = json.dumps({
            "code_blocks": [
                {
                    "file_path": target,
                    "content": f'"""Mock output."""\n\ndef farewell(name: str) -> str:\n    return f"Goodbye, {{name}}!"\n',
                    "language": "python",
                    "action": "replace",
                }
            ],
            "explanation": "Added farewell function",
        })

        return DeterministicTaskResult(
            node_id=request.node_id,
            task_name=request.task_name,
            status="succeeded",
            reason_code="adapter.execution_succeeded",
            executor_type=self.executor_type,
            inputs={"prompt_length": len(prompt)},
            outputs={
                "completion": completion_json,
                "provider_slug": "mock",
                "model_slug": "mock-model",
                "latency_ms": 42,
                "execution_mode": "mock",
            },
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
        )


def _build_mock_adapter_registry() -> AdapterRegistry:
    from adapters.context_adapter import ContextCompilerAdapter
    from adapters.output_parser_adapter import OutputParserAdapter
    from adapters.file_writer_adapter import FileWriterAdapter
    from adapters.verify_adapter import VerifyAdapter

    registry = AdapterRegistry()
    registry.register("cli_llm", MockLLMAdapter())
    registry.register("context_compiler", ContextCompilerAdapter())
    registry.register("output_parser", OutputParserAdapter())
    registry.register("file_writer", FileWriterAdapter())
    registry.register("verifier", VerifyAdapter())
    return registry


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDispatchGraphBuild:
    """Verify graph construction from WorkflowSpec."""

    def test_basic_graph_has_context_llm_terminal(self):
        spec = WorkflowSpec(prompt="test")
        request = _build_workflow_graph(spec)
        node_ids = [n.node_id for n in request.nodes]
        assert "context" in node_ids
        assert "llm" in node_ids
        assert "terminal" in node_ids

    def test_scope_write_adds_writer_node(self):
        spec = WorkflowSpec(prompt="test", scope_write=["a.py"], workdir="/tmp")
        request = _build_workflow_graph(spec)
        node_ids = [n.node_id for n in request.nodes]
        assert "writer" in node_ids

    def test_no_scope_write_skips_writer(self):
        spec = WorkflowSpec(prompt="test")
        request = _build_workflow_graph(spec)
        node_ids = [n.node_id for n in request.nodes]
        assert "writer" not in node_ids

    def test_verify_bindings_adds_verifier_node(self):
        spec = WorkflowSpec(
            prompt="test",
            verify_refs=["verify_ref.python.py_compile.a.py.test"],
            workdir="/tmp",
        )
        request = _build_workflow_graph(spec)
        node_ids = [n.node_id for n in request.nodes]
        assert "verifier" in node_ids

    def test_content_hash_is_stable(self):
        spec = WorkflowSpec(prompt="test", scope_write=["a.py"], workdir="/tmp")
        r1 = _build_workflow_graph(spec)
        r2 = _build_workflow_graph(spec)
        assert r1.definition_hash == r2.definition_hash
        # workflow_id is unique per dispatch
        assert r1.workflow_id != r2.workflow_id

    def test_different_graphs_have_different_hashes(self):
        s1 = WorkflowSpec(prompt="test")
        s2 = WorkflowSpec(prompt="test", scope_write=["a.py"], workdir="/tmp")
        r1 = _build_workflow_graph(s1)
        r2 = _build_workflow_graph(s2)
        assert r1.definition_hash != r2.definition_hash


class TestDispatchGraphExecution:
    """Execute a full dispatch graph with mock adapters."""

    @pytest.fixture
    def workspace(self, tmp_path):
        target = tmp_path / "greeting.py"
        target.write_text('"""Greeting."""\n\ndef greet(name):\n    return f"Hello, {name}!"\n')
        return str(tmp_path)

    def test_full_graph_executes_all_nodes(self, workspace):
        spec = WorkflowSpec(
            prompt="Add farewell",
            provider_slug="mock",
            adapter_type="cli_llm",
            workdir=workspace,
            scope_write=["greeting.py"],
            context_sections=[
                {"name": "FILE: greeting.py", "content": "def greet(): pass"},
            ],
        )

        request = _build_workflow_graph(spec)
        registry = _build_registry(spec)
        planner = WorkflowIntakePlanner(registry=registry)
        intake = planner.plan(request=request)

        evidence_writer = AppendOnlyWorkflowEvidenceWriter()
        adapter_registry = _build_mock_adapter_registry()
        orchestrator = RuntimeOrchestrator(
            adapter_registry=adapter_registry,
            evidence_reader=evidence_writer,
        )

        result = orchestrator.execute_deterministic_path(
            intake_outcome=intake,
            evidence_writer=evidence_writer,
        )

        # All nodes executed
        executed_ids = {nr.node_id for nr in result.node_results}
        assert "context" in executed_ids
        assert "llm" in executed_ids
        assert "writer" in executed_ids
        assert "terminal" in executed_ids

        # All nodes succeeded
        for nr in result.node_results:
            assert nr.status == "succeeded", f"{nr.node_id} failed: {nr.failure_code}"

    def test_evidence_recorded_per_node(self, workspace):
        spec = WorkflowSpec(
            prompt="Add farewell",
            workdir=workspace,
            scope_write=["greeting.py"],
            context_sections=[{"name": "FILE", "content": "pass"}],
        )

        request = _build_workflow_graph(spec)
        registry = _build_registry(spec)
        planner = WorkflowIntakePlanner(registry=registry)
        intake = planner.plan(request=request)

        evidence_writer = AppendOnlyWorkflowEvidenceWriter()
        adapter_registry = _build_mock_adapter_registry()
        orchestrator = RuntimeOrchestrator(
            adapter_registry=adapter_registry,
            evidence_reader=evidence_writer,
        )
        orchestrator.execute_deterministic_path(
            intake_outcome=intake,
            evidence_writer=evidence_writer,
        )

        timeline = evidence_writer.evidence_timeline(intake.run_id)
        assert len(timeline) > 0

        # Evidence_seq is monotonically increasing
        seqs = [row.evidence_seq for row in timeline]
        assert seqs == sorted(seqs)
        assert len(set(seqs)) == len(seqs)  # no duplicates

    def test_file_written_by_graph(self, workspace):
        spec = WorkflowSpec(
            prompt="Add farewell",
            workdir=workspace,
            scope_write=["greeting.py"],
            context_sections=[{"name": "FILE", "content": "pass"}],
        )

        request = _build_workflow_graph(spec)
        registry = _build_registry(spec)
        planner = WorkflowIntakePlanner(registry=registry)
        intake = planner.plan(request=request)

        evidence_writer = AppendOnlyWorkflowEvidenceWriter()
        adapter_registry = _build_mock_adapter_registry()
        orchestrator = RuntimeOrchestrator(
            adapter_registry=adapter_registry,
            evidence_reader=evidence_writer,
        )
        orchestrator.execute_deterministic_path(
            intake_outcome=intake,
            evidence_writer=evidence_writer,
        )

        # The file was written by the graph (writer node), not the model
        content = Path(workspace, "greeting.py").read_text()
        assert "farewell" in content
        assert "Goodbye" in content


class TestDispatchProjection:
    """Verify WorkflowResult is projected from evidence timeline."""

    def test_project_from_timeline(self, tmp_path):
        workspace = str(tmp_path)
        (tmp_path / "greeting.py").write_text("pass\n")

        spec = WorkflowSpec(
            prompt="Add farewell",
            provider_slug="mock",
            model_slug="mock-model",
            adapter_type="cli_llm",
            workdir=workspace,
            scope_write=["greeting.py"],
            context_sections=[{"name": "FILE", "content": "pass"}],
        )

        request = _build_workflow_graph(spec)
        registry = _build_registry(spec)
        planner = WorkflowIntakePlanner(registry=registry)
        intake = planner.plan(request=request)

        evidence_writer = AppendOnlyWorkflowEvidenceWriter()
        adapter_registry = _build_mock_adapter_registry()
        orchestrator = RuntimeOrchestrator(
            adapter_registry=adapter_registry,
            evidence_reader=evidence_writer,
        )
        orchestrator.execute_deterministic_path(
            intake_outcome=intake,
            evidence_writer=evidence_writer,
        )

        timeline = evidence_writer.evidence_timeline(intake.run_id)
        projected = project_workflow_result(
            run_id=intake.run_id,
            timeline=timeline,
            spec_provider_slug="mock",
            spec_model_slug="mock-model",
            spec_adapter_type="cli_llm",
        )

        assert projected["status"] == "succeeded"
        assert projected["evidence_count"] > 0
        assert projected["author_model"] == "mock/mock-model"

        so = projected["outputs"].get("structured_output", {})
        assert so.get("has_code") is True

        wm = projected["outputs"].get("write_manifest", {})
        assert wm.get("total_files", 0) > 0
