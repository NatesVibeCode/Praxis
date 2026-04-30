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


def test_receipt_structural_proof_passes_when_provenance_complete() -> None:
    receipt = {
        "receipt_id": "receipt:run:abc",
        "outputs": {
            "git_provenance": {
                "available": True,
                "repo_snapshot_ref": "repo_snapshot:deadbeef",
                "repo_fingerprint": "abc",
            },
            "workspace_provenance": {"workspace_root": "/workspace"},
            "route_identity": {"run_id": "run:abc"},
        },
    }
    status, outputs = verifier_builtins.builtin_verify_receipt_structural_proof(
        inputs={"receipt": receipt}
    )
    assert status == "passed"
    assert outputs["missing"] == []
    assert outputs["checks"]["has_git_provenance"] is True
    assert outputs["checks"]["has_repo_snapshot_ref"] is True
    assert outputs["checks"]["duplicated_git_fields"] is False
    assert outputs["verifier_ref"] == "verifier.receipt.structural_proof"


def test_receipt_structural_proof_fails_when_git_provenance_missing() -> None:
    receipt = {"receipt_id": "receipt:run:xyz", "outputs": {"workspace_provenance": {}}}
    status, outputs = verifier_builtins.builtin_verify_receipt_structural_proof(
        inputs={"receipt": receipt}
    )
    assert status == "failed"
    assert "git_provenance" in outputs["missing"]


def test_receipt_structural_proof_fails_when_repo_snapshot_ref_missing() -> None:
    receipt = {
        "receipt_id": "receipt:run:xyz",
        "outputs": {
            "git_provenance": {"available": True},
        },
    }
    status, outputs = verifier_builtins.builtin_verify_receipt_structural_proof(
        inputs={"receipt": receipt}
    )
    assert status == "failed"
    assert "git_provenance.repo_snapshot_ref" in outputs["missing"]


def test_receipt_structural_proof_passes_when_git_unavailable_marker_set() -> None:
    receipt = {
        "receipt_id": "receipt:run:xyz",
        "outputs": {
            "git_provenance": {"reason_code": "git_provenance_unavailable", "available": False},
            "workspace_provenance": {"workspace_root": "/workspace"},
        },
    }
    status, outputs = verifier_builtins.builtin_verify_receipt_structural_proof(
        inputs={"receipt": receipt}
    )
    assert status == "passed"
    assert outputs["checks"]["git_unavailable_marker"] is True


def test_receipt_structural_proof_fails_when_git_provenance_has_duplicated_workspace_fields() -> None:
    receipt = {
        "receipt_id": "receipt:run:xyz",
        "outputs": {
            "git_provenance": {
                "available": True,
                "repo_snapshot_ref": "repo_snapshot:abc",
                "workspace_root": "/workspace",
            },
        },
    }
    status, outputs = verifier_builtins.builtin_verify_receipt_structural_proof(
        inputs={"receipt": receipt}
    )
    assert status == "failed"
    assert "git_provenance.no_duplicated_workspace_fields" in outputs["missing"]


def test_receipt_structural_proof_dispatch_via_run_builtin_verifier() -> None:
    receipt = {
        "receipt_id": "receipt:run:abc",
        "outputs": {
            "git_provenance": {
                "available": True,
                "repo_snapshot_ref": "repo_snapshot:deadbeef",
            },
        },
    }
    status, outputs = verifier_builtins.run_builtin_verifier(
        "receipt_structural_proof",
        inputs={"receipt": receipt},
        connection_fn=lambda conn: None,
    )
    assert status == "passed"
    assert outputs["verifier_ref"] == "verifier.receipt.structural_proof"
