from __future__ import annotations

import json
from datetime import datetime, timezone

import runtime.sandbox_runtime as sandbox_runtime
from runtime.sandbox_runtime import SandboxExecutionResult
from runtime.workflow import receipt_writer


def test_workspace_manifest_audit_records_missing_and_observed_paths() -> None:
    receipt = sandbox_runtime.HydrationReceipt(
        sandbox_session_id="sandbox.alpha",
        workspace_root="/workspace",
        hydrated_files=2,
        workspace_materialization="copy",
        workspace_snapshot_ref="workspace_snapshot:abc",
        hydrated_paths=("runtime/spec_compiler.py", "runtime/context.py"),
    )
    result = SandboxExecutionResult(
        sandbox_session_id="sandbox.alpha",
        sandbox_group_id="group.alpha",
        sandbox_provider="fake",
        execution_transport="cli",
        exit_code=0,
        stdout="sed: runtime/missing.py: No such file or directory",
        stderr="read runtime/context.py",
        timed_out=False,
        artifact_refs=(),
        started_at="2026-04-09T00:00:00+00:00",
        finished_at="2026-04-09T00:00:01+00:00",
        network_policy="provider_only",
        provider_latency_ms=5,
        execution_mode="fake",
        workspace_root="/workspace",
    )

    audit = sandbox_runtime._workspace_manifest_audit(
        metadata={
            "execution_bundle": {
                "access_policy": {
                    "write_scope": ["runtime/spec_compiler.py"],
                    "declared_read_scope": ["runtime/context.py", "runtime/missing.py"],
                }
            }
        },
        hydration_receipt=receipt,
        result=result,
    )

    assert audit["missing_intended_paths"] == ["runtime/missing.py"]
    assert audit["observed_file_read_refs"] == [
        "runtime/context.py",
        "runtime/missing.py",
    ]


class _ReceiptConn:
    def __init__(self) -> None:
        self.queries: list[tuple[str, tuple]] = []

    def execute(self, query: str, *args):
        self.queries.append((query, args))
        normalized = " ".join(query.split())
        if "FROM workflow_jobs j JOIN workflow_runs wr ON wr.run_id = j.run_id" in normalized:
            return [
                {
                    "workflow_id": "workflow.audit",
                    "request_id": "request.audit",
                    "request_envelope": {
                        "workspace_ref": "workspace://praxis",
                        "runtime_profile_ref": "runtime://praxis",
                        "spec_snapshot": {
                            "workdir": "/repo",
                            "write_scope": ["runtime/example.py"],
                        },
                    },
                    "attempt": 1,
                    "started_at": datetime(2026, 4, 8, 18, 0, tzinfo=timezone.utc),
                    "finished_at": datetime(2026, 4, 8, 18, 0, 1, tzinfo=timezone.utc),
                    "touch_keys": [],
                }
            ]
        if "WITH lock_token AS (" in normalized and "INSERT INTO receipts (" in normalized:
            return [{"evidence_seq": 1}]
        return []


def test_write_job_receipt_persists_workspace_manifest_audit() -> None:
    conn = _ReceiptConn()
    receipt_writer.write_job_receipt(
        conn,
        "run.audit",
        7,
        "build_a",
        "openai/gpt-5.4-mini",
        {
            "status": "succeeded",
            "exit_code": 0,
            "workspace_manifest_audit": {
                "intended_manifest_paths": ["runtime/example.py", "runtime/context.py"],
                "hydrated_manifest_paths": ["runtime/example.py"],
                "missing_intended_paths": ["runtime/context.py"],
                "observed_file_read_refs": ["runtime/context.py"],
                "observed_file_read_mode": "provider_output_path_mentions",
            },
        },
        100,
        repo_root="/repo",
    )

    receipt_insert = next(
        args
        for query, args in conn.queries
        if "INSERT INTO receipts" in " ".join(query.split())
    )
    outputs = json.loads(receipt_insert[16])
    assert outputs["workspace_manifest_audit"]["missing_intended_paths"] == [
        "runtime/context.py"
    ]
    assert outputs["workspace_manifest_audit"]["observed_file_read_refs"] == [
        "runtime/context.py"
    ]


def test_write_job_receipt_persists_artifact_scope_drift() -> None:
    conn = _ReceiptConn()
    receipt_writer.write_job_receipt(
        conn,
        "run.audit",
        7,
        "build_a",
        "openai/gpt-5.4-mini",
        {
            "status": "failed",
            "exit_code": 0,
            "error_code": "workflow_scope.out_of_scope_write",
            "artifact_scope_drift": [
                {
                    "artifact_ref": "submit.py",
                    "declared_write_scope": ["runtime/example.py"],
                    "reason": "outside_write_scope",
                    "submission_required": True,
                }
            ],
        },
        100,
        repo_root="/repo",
    )

    receipt_insert = next(
        args
        for query, args in conn.queries
        if "INSERT INTO receipts" in " ".join(query.split())
    )
    outputs = json.loads(receipt_insert[16])
    assert outputs["artifact_scope_drift_count"] == 1
    assert outputs["artifact_scope_drift"][0]["artifact_ref"] == "submit.py"
