from __future__ import annotations

import json

from surfaces.api import native_ops


class _FakeStatus:
    def __init__(self, *, label: str):
        self._label = label

    def to_json(self) -> dict[str, str]:
        return {"label": self._label}


def test_native_ops_wrappers_delegate_to_explicit_database_authority(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        native_ops,
        "database_status_service",
        lambda env=None: _FakeStatus(label="health"),
    )
    monkeypatch.setattr(
        native_ops,
        "database_bootstrap_service",
        lambda env=None: _FakeStatus(label="bootstrap"),
    )

    assert native_ops.main(["db-health"]) == 0
    assert json.loads(capsys.readouterr().out) == {"label": "health"}

    assert native_ops.main(["db-bootstrap"]) == 0
    assert json.loads(capsys.readouterr().out) == {"label": "bootstrap"}

    assert native_ops.main(["db-restart"]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "message": (
            "Database process restart is not owned by native_ops; use the runtime "
            "target/service lifecycle authority for process control."
        ),
        "reason_code": "native_ops.db_restart.not_authoritative",
        "status": "unsupported",
    }


def test_show_instance_contract_reports_repo_local_defaults(monkeypatch, capsys) -> None:
    monkeypatch.delenv("PRAXIS_INSTANCE_NAME", raising=False)
    monkeypatch.delenv("PRAXIS_RECEIPTS_DIR", raising=False)
    monkeypatch.delenv("PRAXIS_RUNTIME_PROFILE", raising=False)
    monkeypatch.delenv("PRAXIS_TOPOLOGY_DIR", raising=False)
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setattr(
        native_ops,
        "native_instance_contract",
        lambda env=None: {
            "praxis_instance_name": "praxis",
            "praxis_runtime_profile": "praxis",
            "praxis_receipts_dir": "/repo/artifacts/runtime_receipts",
            "praxis_topology_dir": "/repo/artifacts/runtime_topology",
        },
    )

    assert native_ops.main(["show-instance-contract"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["praxis_instance_name"] == "praxis"
    assert payload["praxis_runtime_profile"] == "praxis"
    assert payload["praxis_receipts_dir"].endswith("artifacts/runtime_receipts")
    assert payload["praxis_topology_dir"].endswith("artifacts/runtime_topology")
