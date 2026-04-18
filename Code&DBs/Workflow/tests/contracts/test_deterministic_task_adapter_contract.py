from __future__ import annotations

import sys
import types

from adapters import DeterministicTaskAdapter, DeterministicTaskRequest


def test_deterministic_task_adapter_returns_declared_outputs() -> None:
    adapter = DeterministicTaskAdapter()

    result = adapter.execute(
        request=DeterministicTaskRequest(
            node_id="node_0",
            task_name="prepare",
            input_payload={"answer": 42},
            expected_outputs={"result": "prepared"},
            dependency_inputs={},
            execution_boundary_ref="workspace.alpha",
        )
    )

    # Passthrough-echo contract: no `deterministic_builder` in input_payload
    # means the adapter echoes `expected_outputs` back. This path MUST be
    # observable — status stays "succeeded" for backward compatibility, but
    # the reason_code is distinct and the outputs carry a `passthrough_echo`
    # annotation so downstream consumers can detect that no real work ran.
    assert result.status == "succeeded"
    assert result.reason_code == "adapter.execution_passthrough_echo"
    assert result.inputs["task_name"] == "prepare"
    assert result.inputs["input_payload"] == {"answer": 42}
    assert result.outputs == {"result": "prepared", "passthrough_echo": True}
    assert result.failure_code is None


def test_deterministic_task_adapter_preserves_dependency_inputs_in_normalized_request() -> None:
    adapter = DeterministicTaskAdapter()

    result = adapter.execute(
        request=DeterministicTaskRequest(
            node_id="node_1",
            task_name="admit",
            input_payload={"step": 1},
            expected_outputs={"result": "admitted"},
            dependency_inputs={"prepared_result": "prepared"},
            execution_boundary_ref="workspace.alpha",
        )
    )

    assert result.status == "succeeded"
    assert result.reason_code == "adapter.execution_passthrough_echo"
    assert result.inputs["dependency_inputs"] == {"prepared_result": "prepared"}
    assert result.inputs["execution_boundary_ref"] == "workspace.alpha"
    assert result.outputs == {"result": "admitted", "passthrough_echo": True}


def test_deterministic_task_adapter_fails_closed_on_invalid_input() -> None:
    adapter = DeterministicTaskAdapter()

    result = adapter.execute(
        request=DeterministicTaskRequest(
            node_id="node_0",
            task_name="",
            input_payload={"answer": 42},
            expected_outputs={"result": "prepared"},
            dependency_inputs={},
            execution_boundary_ref="workspace.alpha",
        )
    )

    assert result.status == "failed"
    assert result.reason_code == "adapter.input_invalid"
    assert result.failure_code == "adapter.input_invalid"


def test_deterministic_task_adapter_emits_explicit_failure_code() -> None:
    adapter = DeterministicTaskAdapter()

    result = adapter.execute(
        request=DeterministicTaskRequest(
            node_id="node_0",
            task_name="prepare",
            input_payload={
                "force_failure": True,
                "failure_code": "adapter.command_failed",
            },
            expected_outputs={"result": "prepared"},
            dependency_inputs={},
            execution_boundary_ref="workspace.alpha",
        )
    )

    assert result.status == "failed"
    assert result.reason_code == "adapter.command_failed"
    assert result.outputs == {}
    assert result.failure_code == "adapter.command_failed"


def test_deterministic_task_adapter_executes_builder_with_dependency_inputs() -> None:
    adapter = DeterministicTaskAdapter()
    calls: list[dict[str, object]] = []
    module = types.ModuleType("tests.fake_deterministic_builder")

    def _builder(payload: dict[str, object]) -> dict[str, object]:
        calls.append(dict(payload))
        upstream = payload["discover_local_code"]
        return {"review": upstream["tool_result"]["match_count"] + int(payload["seed"])}

    module.build = _builder
    sys.modules[module.__name__] = module
    try:
        result = adapter.execute(
            request=DeterministicTaskRequest(
                node_id="node_builder",
                task_name="review",
                input_payload={
                    "seed": 2,
                    "deterministic_builder": "tests.fake_deterministic_builder.build",
                },
                expected_outputs={},
                dependency_inputs={
                    "discover_local_code": {"tool_result": {"match_count": 3}},
                },
                execution_boundary_ref="workspace.alpha",
            )
        )
    finally:
        sys.modules.pop(module.__name__, None)

    # Real-builder path: reason_code distinguishes from passthrough-echo so
    # receipts can be filtered ("show me all nodes that actually ran work
    # vs. the ones that echoed expected_outputs").
    assert result.status == "succeeded"
    assert result.reason_code == "adapter.execution_succeeded"
    assert result.outputs == {"review": 5}
    assert "passthrough_echo" not in result.outputs
    assert calls == [
        {
            "seed": 2,
            "deterministic_builder": "tests.fake_deterministic_builder.build",
            "discover_local_code": {"tool_result": {"match_count": 3}},
        }
    ]


def test_deterministic_task_adapter_surfaces_builder_failures() -> None:
    adapter = DeterministicTaskAdapter()
    module = types.ModuleType("tests.fake_deterministic_builder_failure")

    def _builder(_payload: dict[str, object]) -> dict[str, object]:
        raise RuntimeError("builder exploded")

    module.build = _builder
    sys.modules[module.__name__] = module
    try:
        result = adapter.execute(
            request=DeterministicTaskRequest(
                node_id="node_builder_failure",
                task_name="review",
                input_payload={
                    "deterministic_builder": "tests.fake_deterministic_builder_failure.build",
                },
                expected_outputs={},
                dependency_inputs={},
                execution_boundary_ref="workspace.alpha",
            )
        )
    finally:
        sys.modules.pop(module.__name__, None)

    assert result.status == "failed"
    assert result.reason_code == "adapter.execution_failed"
    assert result.failure_code == "adapter.deterministic_builder_failed"
    assert result.outputs["failure_reason"] == "builder exploded"
