from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace

from runtime import receipt_store


@dataclass(frozen=True)
class _StubReceiptRecord:
    id: int
    status: str
    timestamp: datetime | None
    raw: dict

    def to_dict(self) -> dict:
        return dict(self.raw)


def test_list_receipt_payloads_reads_from_postgres_and_normalizes(monkeypatch):
    record = _StubReceiptRecord(
        id=42,
        status="succeeded",
        timestamp=datetime(2026, 4, 6, 12, 0, tzinfo=timezone.utc),
        raw={"run_id": "workflow_test_12345678"},
    )
    monkeypatch.setattr(receipt_store, "list_receipts", lambda **kwargs: [record])

    payloads = receipt_store.list_receipt_payloads(limit=5)

    assert payloads == [{"run_id": "workflow_test_12345678"}]


def test_load_receipt_payload_reads_from_postgres_and_normalizes(monkeypatch):
    record = _StubReceiptRecord(
        id=7,
        status="failed",
        timestamp=datetime(2026, 4, 6, 12, 30, tzinfo=timezone.utc),
        raw={
            "agent": "anthropic/claude-sonnet-4",
            "timestamp": "2026-04-06T12:30:00+00:00",
            "duration_seconds": 1.5,
            "cost_usd": 0.25,
        },
    )
    monkeypatch.setattr(receipt_store, "load_receipt", lambda receipt_id: record)

    payload = receipt_store.load_receipt_payload(7)

    assert payload is not None
    assert payload["provider_slug"] == "anthropic"
    assert payload["model_slug"] == "claude-sonnet-4"
    assert payload["agent_slug"] == "anthropic/claude-sonnet-4"
    assert payload["finished_at"] == "2026-04-06T12:30:00+00:00"
    assert payload["latency_ms"] == 1500
    assert payload["total_cost_usd"] == 0.25


def test_post_receipt_hooks_dedupe_auto_bug_by_aggregation_tags(monkeypatch):
    observed: dict[str, object] = {}

    class _NoopFrictionLedger:
        def __init__(self, conn):
            self.conn = conn

        def record(self, **_kwargs):
            return None

    class _FakeTracker:
        def __init__(self, conn):
            self.conn = conn

        def list_bugs(self, *, open_only, tags, limit):
            observed["dedupe_tags"] = tags
            assert open_only is True
            assert limit == 1
            return [SimpleNamespace(bug_id="BUG-existing")]

        def file_bug(self, **_kwargs):
            raise AssertionError("existing aggregation bug should be reused")

        def link_evidence(self, bug_id, **kwargs):
            observed.setdefault("links", []).append((bug_id, kwargs))

    class _Conn:
        def fetchval(self, *_args):
            return 4

    monkeypatch.setattr(receipt_store, "FrictionLedger", _NoopFrictionLedger)
    monkeypatch.setattr(receipt_store, "BugTracker", _FakeTracker)
    monkeypatch.setattr(receipt_store, "emit_system_event", lambda *_args, **_kwargs: None)

    receipt_store._run_post_receipt_hooks(
        {
            "status": "failed",
            "failure_code": "sandbox_error",
            "failure_category": "sandbox_error",
            "job_label": "wave0_integrated_design",
            "node_id": "wave0_integrated_design",
            "receipt_id": "receipt:workflow_1:1:1",
            "run_id": "workflow_1",
        },
        conn=_Conn(),
    )

    assert observed["dedupe_tags"] == (
        "auto-filed",
        "failure_code:sandbox_error",
        "job_label:wave0_integrated_design",
    )
    assert ("BUG-existing",) == tuple({bug_id for bug_id, _ in observed["links"]})


def test_post_receipt_hooks_emit_events_for_evidence_link_failures(monkeypatch):
    events: list[dict[str, object]] = []

    class _NoopFrictionLedger:
        def __init__(self, conn):
            self.conn = conn

        def record(self, **_kwargs):
            return None

    class _FakeTracker:
        def __init__(self, conn):
            self.conn = conn

        def list_bugs(self, **_kwargs):
            return []

        def file_bug(self, **_kwargs):
            return SimpleNamespace(bug_id="BUG-new"), []

        def link_evidence(self, *_args, **_kwargs):
            raise RuntimeError("evidence authority unavailable")

    class _Conn:
        def fetchval(self, *_args):
            return 3

    def _emit_system_event(_conn, **kwargs):
        events.append(kwargs)

    monkeypatch.setattr(receipt_store, "FrictionLedger", _NoopFrictionLedger)
    monkeypatch.setattr(receipt_store, "BugTracker", _FakeTracker)
    monkeypatch.setattr(receipt_store, "emit_system_event", _emit_system_event)

    receipt_store._run_post_receipt_hooks(
        {
            "status": "failed",
            "failure_code": "workflow.timeout",
            "job_label": "wave6",
            "node_id": "wave6",
            "receipt_id": "receipt:workflow_2:1:1",
            "run_id": "workflow_2",
        },
        conn=_Conn(),
    )

    hooks = {event["payload"]["hook"] for event in events}
    assert {"bug_evidence.receipt", "bug_evidence.run"} <= hooks
    assert {event["event_type"] for event in events} == {"post_receipt_hook.failed"}


def test_apply_receipt_provenance_rewrites_legacy_git_payload_when_repo_snapshot_is_available(
    monkeypatch,
):
    compact_git = {
        "available": True,
        "repo_snapshot_ref": "repo_snapshot:abc123",
        "repo_fingerprint": "fp-123",
        "git_dirty": False,
        "captured_at": "2026-04-09T17:00:00+00:00",
    }
    monkeypatch.setattr(receipt_store, "build_git_provenance", lambda **kwargs: dict(compact_git))

    inputs, outputs = receipt_store._apply_receipt_provenance(
        payload={
            "workspace_root": "/repo",
            "workspace_ref": "workspace://praxis",
            "runtime_profile_ref": "runtime://praxis",
            "workspace_snapshot_ref": "workspace_snapshot:abc123",
        },
        inputs={},
        outputs={
            "git_provenance": {
                "available": True,
                "repo_snapshot_ref": "repo_snapshot:legacy",
                "repo_fingerprint": "fp-legacy",
                "git_dirty": True,
                "captured_at": "2026-04-09T16:00:00+00:00",
                "workspace_root": "/repo",
                "workspace_ref": "workspace://praxis",
                "runtime_profile_ref": "runtime://praxis",
            }
        },
        conn=object(),
    )

    assert outputs["git_provenance"] == compact_git
    assert outputs["workspace_provenance"] == {
        "workspace_root": "/repo",
        "workspace_ref": "workspace://praxis",
        "runtime_profile_ref": "runtime://praxis",
        "workspace_snapshot_ref": "workspace_snapshot:abc123",
    }
    assert inputs["workspace_root"] == "/repo"
    assert inputs["workspace_ref"] == "workspace://praxis"
    assert inputs["runtime_profile_ref"] == "runtime://praxis"


class _ProofMetricsConn:
    def execute(self, query: str, *args):
        raise AssertionError(query)

    def fetchrow(self, query: str, *args):
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT COUNT(*) AS receipts_total,"):
            return {
                "receipts_total": 10,
                "receipts_with_verification_status": 5,
                "receipts_with_attempted_verification": 3,
                "receipts_with_configured_verification": 1,
                "receipts_with_skipped_verification": 1,
                "receipts_with_verification": 2,
                "receipts_with_verified_paths": 3,
                "receipts_with_status_only_verification": 2,
                "receipts_with_path_backed_verification": 3,
                "receipts_with_fully_proved_verification": 2,
                "receipts_with_write_manifest": 4,
                "receipts_with_mutation_provenance": 3,
                "receipts_with_git_provenance": 10,
                "receipts_with_repo_snapshot_ref": 8,
            }
        if normalized.startswith("SELECT COUNT(*) FILTER (WHERE entity_type = 'code_unit') AS code_units"):
            return {
                "code_units": 7,
                "tables": 2,
                "verification_results": 4,
                "failure_results": 1,
            }
        if normalized.startswith("SELECT COUNT(*) FILTER (WHERE relation_type = 'verified_by' AND active = true) AS verified_by_edges"):
            return {
                "verified_by_edges": 4,
                "recorded_in_edges": 5,
                "produced_edges": 6,
                "related_edges": 2,
            }
        if normalized.startswith("SELECT to_regclass('public.compile_artifacts') IS NOT NULL AS compile_artifacts_ready"):
            return {
                "compile_artifacts_ready": True,
                "capability_catalog_ready": True,
                "verify_refs_ready": True,
                "verification_registry_ready": True,
                "compile_index_snapshots_ready": True,
                "execution_packets_ready": True,
                "repo_snapshots_ready": True,
                "verifier_registry_ready": True,
                "healer_registry_ready": True,
                "verifier_healer_bindings_ready": True,
                "verification_runs_ready": True,
                "healing_runs_ready": True,
            }
        if normalized.startswith("SELECT COUNT(*) AS repo_snapshots FROM repo_snapshots"):
            return {"repo_snapshots": 3}
        if normalized.startswith("SELECT (SELECT COUNT(*) FROM verifier_registry) AS verifiers"):
            return {
                "verifiers": 5,
                "healers": 3,
                "verifier_healer_bindings": 3,
                "verification_runs": 9,
                "healing_runs": 2,
            }
        raise AssertionError(query)


def test_proof_metrics_reports_verification_tiers() -> None:
    metrics = receipt_store.proof_metrics(conn=_ProofMetricsConn())

    receipts = metrics["receipts"]
    assert receipts["total"] == 10
    assert receipts["with_verification_status"] == 5
    assert receipts["with_attempted_verification"] == 3
    assert receipts["with_configured_verification"] == 1
    assert receipts["with_skipped_verification"] == 1
    assert receipts["with_verification"] == 2
    assert receipts["with_verified_paths"] == 3
    assert receipts["with_status_only_verification"] == 2
    assert receipts["with_path_backed_verification"] == 3
    assert receipts["with_fully_proved_verification"] == 2
    assert receipts["verification_status_coverage"] == 0.5
    assert receipts["attempted_verification_coverage"] == 0.3
    assert receipts["configured_verification_coverage"] == 0.1
    assert receipts["skipped_verification_coverage"] == 0.1
    assert receipts["verification_coverage"] == 0.2
    assert receipts["status_only_verification_coverage"] == 0.2
    assert receipts["path_backed_verification_coverage"] == 0.3
    assert receipts["fully_proved_verification_coverage"] == 0.2
    assert receipts["repo_snapshot_ref_coverage"] == 0.8
    assert metrics["compile_authority"]["verification_registry_ready"] is True
    assert metrics["recovery_authority"]["authority_ready"] is True
    assert metrics["recovery_authority"]["verifiers"] == 5
    assert metrics["recovery_authority"]["healing_runs"] == 2


class _BackfillConn:
    def __init__(self) -> None:
        self.updates: list[tuple[str, str, str]] = []

    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT r.receipt_id, r.inputs, r.outputs, j.touch_keys, wr.request_envelope FROM receipts AS r"):
            return [{
                "receipt_id": "receipt:1",
                "inputs": {
                    "workspace_root": "/repo",
                    "workspace_ref": "workspace://praxis",
                    "runtime_profile_ref": "runtime://praxis",
                },
                "outputs": {
                    "git_provenance": {
                        "available": True,
                        "repo_snapshot_ref": "repo_snapshot:legacy",
                        "repo_fingerprint": "fp-legacy",
                        "git_dirty": True,
                        "captured_at": "2026-04-09T16:00:00+00:00",
                        "workspace_root": "/repo",
                        "workspace_ref": "workspace://praxis",
                        "runtime_profile_ref": "runtime://praxis",
                    },
                    "workspace_provenance": {
                        "workspace_root": "/repo",
                        "workspace_ref": "workspace://praxis",
                        "runtime_profile_ref": "runtime://praxis",
                        "workspace_snapshot_ref": "workspace_snapshot:abc123",
                    },
                },
                "touch_keys": [],
                "request_envelope": {},
            }]
        if normalized.startswith("UPDATE receipts SET inputs = $2::jsonb, outputs = $3::jsonb WHERE receipt_id = $1"):
            self.updates.append((args[0], args[1], args[2]))
            return []
        raise AssertionError(query)


def test_backfill_receipt_provenance_uses_stored_workspace_root_for_git_compaction(
    monkeypatch,
) -> None:
    compact_git = {
        "available": True,
        "repo_snapshot_ref": "repo_snapshot:abc123",
        "repo_fingerprint": "fp-123",
        "git_dirty": False,
        "captured_at": "2026-04-09T17:00:00+00:00",
    }
    monkeypatch.setattr(receipt_store, "build_git_provenance", lambda **kwargs: dict(compact_git))
    conn = _BackfillConn()

    result = receipt_store.backfill_receipt_provenance(conn=conn)

    assert result["updated_receipts"] == 1
    assert len(conn.updates) == 1
    updated_outputs = json.loads(conn.updates[0][2])
    assert updated_outputs["git_provenance"] == compact_git
    assert updated_outputs["workspace_provenance"]["workspace_root"] == "/repo"
    assert updated_outputs["workspace_provenance"]["workspace_snapshot_ref"] == "workspace_snapshot:abc123"
    assert "workspace_root" not in updated_outputs["git_provenance"]


def test_apply_receipt_provenance_backfills_verified_paths_from_verification_bindings(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        receipt_store,
        "build_git_provenance",
        lambda **kwargs: {
            "available": True,
            "repo_snapshot_ref": "repo_snapshot:abc123",
            "repo_fingerprint": "fp-123",
            "git_dirty": False,
            "captured_at": "2026-04-09T17:00:00+00:00",
        },
    )

    _, outputs = receipt_store._apply_receipt_provenance(
        payload={
            "workspace_root": "/repo",
            "write_scope": ["runtime/example.py"],
            "workspace_ref": "workspace://praxis",
            "runtime_profile_ref": "runtime://praxis",
        },
        inputs={"write_scope": ["runtime/example.py"]},
        outputs={
            "verification_status": "failed",
            "verification_bindings": [
                {
                    "verification_ref": "verification.python.pytest_file",
                    "inputs": {"path": "runtime/example.py"},
                }
            ],
        },
        conn=object(),
    )

    assert outputs["verified_paths"] == ["runtime/example.py"]


def test_apply_receipt_provenance_does_not_infer_verified_paths_from_write_scope_alone(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        receipt_store,
        "build_git_provenance",
        lambda **kwargs: {
            "available": True,
            "repo_snapshot_ref": "repo_snapshot:abc123",
            "repo_fingerprint": "fp-123",
            "git_dirty": False,
            "captured_at": "2026-04-09T17:00:00+00:00",
        },
    )

    _, outputs = receipt_store._apply_receipt_provenance(
        payload={
            "workspace_root": "/repo",
            "write_scope": ["runtime/example.py"],
            "workspace_ref": "workspace://praxis",
            "runtime_profile_ref": "runtime://praxis",
        },
        inputs={"write_scope": ["runtime/example.py"]},
        outputs={"verification_status": "failed"},
        conn=object(),
    )

    assert "verified_paths" not in outputs


def test_apply_receipt_provenance_does_not_backfill_verified_paths_for_skipped_verification(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        receipt_store,
        "build_git_provenance",
        lambda **kwargs: {
            "available": True,
            "repo_snapshot_ref": "repo_snapshot:abc123",
            "repo_fingerprint": "fp-123",
            "git_dirty": False,
            "captured_at": "2026-04-09T17:00:00+00:00",
        },
    )

    _, outputs = receipt_store._apply_receipt_provenance(
        payload={
            "workspace_root": "/repo",
            "write_scope": ["runtime/example.py"],
            "workspace_ref": "workspace://praxis",
            "runtime_profile_ref": "runtime://praxis",
        },
        inputs={"write_scope": ["runtime/example.py"]},
        outputs={"verification_status": "skipped"},
        conn=object(),
    )

    assert "verified_paths" not in outputs
