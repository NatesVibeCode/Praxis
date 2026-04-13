from __future__ import annotations

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

    assert result.status == "succeeded"
    assert result.reason_code == "adapter.execution_succeeded"
    assert result.inputs["task_name"] == "prepare"
    assert result.inputs["input_payload"] == {"answer": 42}
    assert result.outputs == {"result": "prepared"}
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
    assert result.inputs["dependency_inputs"] == {"prepared_result": "prepared"}
    assert result.inputs["execution_boundary_ref"] == "workspace.alpha"
    assert result.outputs == {"result": "admitted"}


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
