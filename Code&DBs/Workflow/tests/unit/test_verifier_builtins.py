from __future__ import annotations

import runtime.verifier_builtins as verifier_builtins


def test_schema_authority_verifier_uses_workflow_database_authority(monkeypatch) -> None:
    calls: list[bool] = []

    def _status(*, bootstrap: bool):
        calls.append(bootstrap)
        return {
            "schema_bootstrapped": True,
            "missing_schema_objects": [],
        }

    monkeypatch.setattr(verifier_builtins, "_workflow_database_status_payload", _status)

    status, outputs = verifier_builtins.builtin_verify_schema_authority(inputs={})

    assert status == "passed"
    assert calls == [False]
    assert outputs["summary"] == {
        "schema_bootstrapped": True,
        "missing_schema_object_count": 0,
    }


def test_schema_bootstrap_healer_uses_workflow_database_authority(monkeypatch) -> None:
    calls: list[bool] = []

    def _status(*, bootstrap: bool):
        calls.append(bootstrap)
        return {
            "schema_bootstrapped": True,
            "missing_schema_objects": [],
        }

    monkeypatch.setattr(verifier_builtins, "_workflow_database_status_payload", _status)

    status, outputs = verifier_builtins.builtin_heal_schema_bootstrap(
        inputs={"reason": "test"}
    )

    assert status == "succeeded"
    assert calls == [True]
    assert outputs["inputs"] == {"reason": "test"}
