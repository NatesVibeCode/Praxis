from __future__ import annotations

from pathlib import Path

from adapters.deterministic import DeterministicTaskRequest
from adapters import verify_adapter


class _VerifyResult:
    passed = True

    def to_json(self):
        return {
            "label": "compile",
            "command": "python3 -m py_compile app.py",
            "passed": True,
        }


def test_verify_adapter_rebases_host_workdir_before_running_checks(
    monkeypatch,
    tmp_path: Path,
) -> None:
    host_root = tmp_path / "host"
    container_root = tmp_path / "container"
    host_root.mkdir()
    container_root.mkdir()
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        verify_adapter,
        "_translate_host_path_to_container",
        lambda value: str(container_root) if value == str(host_root) else value,
    )
    monkeypatch.setattr(
        "storage.postgres.connection.ensure_postgres_available",
        lambda: object(),
    )
    monkeypatch.setattr(
        "runtime.verification.resolve_verify_commands",
        lambda _conn, bindings: list(bindings),
    )

    def _run_verify(commands, *, workdir=None):
        captured["commands"] = commands
        captured["workdir"] = workdir
        return (_VerifyResult(),)

    monkeypatch.setattr("runtime.verification.run_verify", _run_verify)

    result = verify_adapter.VerifyAdapter().execute(
        request=DeterministicTaskRequest(
            node_id="verify",
            task_name="verify",
            input_payload={"bindings": ["verify_ref.python"], "workdir": str(host_root)},
            expected_outputs={},
            dependency_inputs={},
            execution_boundary_ref="boundary",
        )
    )

    assert result.status == "succeeded"
    assert captured["workdir"] == str(container_root)
    assert result.inputs["workdir"] == str(container_root)
    assert result.outputs["workdir"] == str(container_root)
