from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

from runtime.workflow import execution_backends, receipt_writer, submission_capture
from runtime.workflow.submission_gate import resolve_submission_for_job


def _sandbox_result(**overrides):
    values = {
        "exit_code": 0,
        "stdout": "candidate submitted",
        "stderr": "",
        "timed_out": False,
        "execution_mode": "docker_local",
        "sandbox_provider": "docker_local",
        "execution_transport": "cli",
        "sandbox_session_id": "sandbox_session:run.alpha:job.alpha",
        "sandbox_group_id": "group:run.alpha",
        "artifact_refs": ("submit.py",),
        "started_at": "2026-04-09T00:00:00+00:00",
        "finished_at": "2026-04-09T00:00:01+00:00",
        "workspace_snapshot_ref": "workspace_snapshot:test1234",
        "workspace_snapshot_cache_hit": True,
        "network_policy": "provider_only",
        "provider_latency_ms": 12,
        "workspace_root": "/tmp/workspace",
        "container_cpu_percent": None,
        "container_mem_bytes": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class _ReceiptConn:
    def __init__(self) -> None:
        self.queries: list[tuple[str, tuple]] = []

    def execute(self, query: str, *args):
        self.queries.append((query, args))
        normalized = " ".join(query.split())
        if "FROM workflow_jobs j JOIN workflow_runs wr ON wr.run_id = j.run_id" in normalized:
            return [
                {
                    "workflow_id": "workflow.scope",
                    "request_id": "request.scope",
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


def _scope_drift() -> list[dict[str, object]]:
    return [
        {
            "artifact_ref": "submit.py",
            "declared_write_scope": ["runtime/example.py"],
            "reason": "outside_write_scope",
            "submission_required": True,
        }
    ]


def test_scope_drift_fails_job_and_persists_receipt_evidence(monkeypatch) -> None:
    payload = execution_backends._result_payload(
        _sandbox_result(artifact_scope_drift=_scope_drift()),
        timeout=15,
        parse_json_output=False,
    )

    assert payload["status"] == "failed"
    assert payload["error_code"] == "workflow_scope.out_of_scope_write"
    assert payload["artifact_scope_drift"][0]["artifact_ref"] == "submit.py"

    conn = _ReceiptConn()
    receipt_writer.write_job_receipt(
        conn,
        "run.scope",
        7,
        "build_a",
        "openai/gpt-5.4-mini",
        payload,
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

    sealed_submission = {
        "submission_id": "submission.scope",
        "result_kind": "code_change_candidate",
        "changed_paths": ["runtime/example.py"],
        "acceptance_status": "pending_review",
        "acceptance_report": {},
    }
    monkeypatch.setattr(
        submission_capture,
        "attach_verification_artifact_refs_for_job",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        submission_capture,
        "get_submission_for_job_attempt",
        lambda *_args, **_kwargs: sealed_submission,
    )

    gate_result = resolve_submission_for_job(
        object(),
        run_id="run.scope",
        workflow_id="workflow.scope",
        job_label="build_a",
        attempt_no=1,
        execution_bundle={"completion_contract": {"submission_required": True}},
        result={
            "stdout": "candidate submitted",
            "stderr": "",
            "artifact_scope_drift": _scope_drift(),
        },
        final_status="succeeded",
        final_error_code="",
        verification_artifact_refs=[],
    )

    assert gate_result.final_status == "failed"
    assert gate_result.final_error_code == "workflow_scope.out_of_scope_write"
    assert "submit.py" in gate_result.result["stderr"]
