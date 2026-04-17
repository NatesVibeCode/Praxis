from __future__ import annotations

import json
from io import StringIO

from surfaces.cli import native_operator
from surfaces.cli.main import main as workflow_cli_main


class _FakeInstance:
    def to_contract(self) -> dict[str, str]:
        return {"repo_root": "/tmp/repo", "workdir": "/tmp/repo"}


def _env() -> dict[str, str]:
    return {"WORKFLOW_DATABASE_URL": "postgresql://localhost:5432/praxis_test"}


def test_native_operator_native_primary_cutover_gate_uses_shared_gate(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _execute_operation_from_env(*, env, operation_name: str, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {
            "native_primary_cutover": {
                "workflow_class_id": payload["workflow_class_id"],
                "decision_source": payload["decision_source"],
            },
            "operation_receipt": {
                "operation_name": operation_name,
                "operation_kind": "command",
            },
        }

    monkeypatch.setattr(native_operator, "resolve_native_instance", lambda env=None: _FakeInstance())
    monkeypatch.setattr(
        native_operator.operation_catalog_gateway,
        "execute_operation_from_env",
        _execute_operation_from_env,
    )

    stdout = StringIO()
    assert (
        workflow_cli_main(
            [
                "native-operator",
                "native-primary-cutover-gate",
                "--decided-by",
                "nate",
                "--decision-source",
                "operator",
                "--rationale",
                "Runtime probe is green",
                "--workflow-class-id",
                "workflow_class.runtime_probe",
            ],
            env=_env(),
            stdout=stdout,
        )
        == 0
    )

    payload = json.loads(stdout.getvalue())
    assert captured["operation_name"] == "operator.native_primary_cutover_gate"
    assert captured["payload"]["workflow_class_id"] == "workflow_class.runtime_probe"
    assert payload["native_primary_cutover"]["decision_source"] == "operator"
    assert payload["operation_receipt"]["operation_name"] == "operator.native_primary_cutover_gate"
