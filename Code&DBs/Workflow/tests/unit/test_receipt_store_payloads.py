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


def test_post_receipt_hooks_dedupe_auto_bug_by_source_issue_id(monkeypatch):
    observed: dict[str, object] = {}

    class _NoopFrictionLedger:
        def __init__(self, conn):
            self.conn = conn

        def record(self, **_kwargs):
            return None

    class _FakeTracker:
        def __init__(self, conn):
            self.conn = conn

        def list_bugs(self, *, open_only, source_issue_id=None, tags=None, limit):
            observed.setdefault("lookups", []).append(
                {"source_issue_id": source_issue_id, "tags": tags}
            )
            assert open_only is True
            assert limit == 1
            if source_issue_id == "receipt.failure:sandbox_error:wave0_integrated_design":
                return [SimpleNamespace(bug_id="BUG-existing")]
            return []

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

    assert observed["lookups"] == [
        {
            "source_issue_id": "receipt.failure:sandbox_error:wave0_integrated_design",
            "tags": None,
        }
    ]
    assert ("BUG-existing",) == tuple({bug_id for bug_id, _ in observed["links"]})


def test_post_receipt_hooks_files_auto_bug_with_stable_receipt_identity(monkeypatch):
    captured: dict[str, object] = {}

    class _NoopFrictionLedger:
        def __init__(self, conn):
            self.conn = conn

        def record(self, **_kwargs):
            return None

    class _FakeTracker:
        def __init__(self, conn):
            self.conn = conn

        def list_bugs(self, **kwargs):
            captured.setdefault("lookups", []).append(kwargs)
            return []

        def file_bug(self, **kwargs):
            captured["file_bug"] = kwargs
            return SimpleNamespace(bug_id="BUG-new"), []

        def link_evidence(self, bug_id, **kwargs):
            captured.setdefault("links", []).append((bug_id, kwargs))

    class _Conn:
        def fetchval(self, query, *args):
            captured["count_args"] = args
            return 3

    monkeypatch.setattr(receipt_store, "FrictionLedger", _NoopFrictionLedger)
    monkeypatch.setattr(receipt_store, "BugTracker", _FakeTracker)
    monkeypatch.setattr(receipt_store, "emit_system_event", lambda *_args, **_kwargs: None)

    receipt_store._run_post_receipt_hooks(
        {
            "status": "failed",
            "failure_code": "Provider Capacity",
            "failure_category": "capacity",
            "job_label": "Plan Runtime Packet",
            "node_id": "Plan Runtime Packet",
            "provider_slug": "openai",
            "model_slug": "gpt-5.4-mini",
            "receipt_id": "receipt:workflow_3:1:1",
            "run_id": "workflow_3",
        },
        conn=_Conn(),
    )

    assert captured["count_args"] == ("Provider Capacity", "Plan Runtime Packet")
    assert captured["lookups"][0] == {
        "open_only": True,
        "source_issue_id": "receipt.failure:provider-capacity:plan-runtime-packet",
        "limit": 1,
    }
    filed = captured["file_bug"]
    assert filed["source_issue_id"] is None
    assert filed["tags"] == (
        "auto-filed",
        "failure_code:provider-capacity",
        "job_label:plan-runtime-packet",
        "node_id:plan-runtime-packet",
        "failure_category:capacity",
        "provider:openai",
        "model:gpt-5.4-mini",
        "receipt_failure_identity:receipt.failure:provider-capacity:plan-runtime-packet",
    )
    assert all(not tag.startswith("signature:") for tag in filed["tags"])
    assert filed["resume_context"]["auto_bug_identity"]["authority"] == (
        "receipts.failure_code+node_id"
    )
    assert filed["resume_context"]["auto_bug_identity"]["aggregation_fields"] == [
        "failure_code",
        "node_id",
    ]


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


def test_post_receipt_hooks_emit_event_for_friction_ledger_failures(monkeypatch):
    events: list[dict[str, object]] = []

    class _FailingFrictionLedger:
        def __init__(self, conn):
            self.conn = conn

        def record(self, **_kwargs):
            raise RuntimeError("ledger unavailable")

    class _FakeTracker:
        def __init__(self, conn):
            self.conn = conn

        def list_bugs(self, **_kwargs):
            return []

        def file_bug(self, **_kwargs):
            raise AssertionError("failure count is below auto-file threshold")

        def link_evidence(self, *_args, **_kwargs):
            raise AssertionError("no bug was filed or found")

    class _Conn:
        def fetchval(self, *_args):
            return 0

    def _emit_system_event(_conn, **kwargs):
        events.append(kwargs)

    monkeypatch.setattr(receipt_store, "FrictionLedger", _FailingFrictionLedger)
    monkeypatch.setattr(receipt_store, "BugTracker", _FakeTracker)
    monkeypatch.setattr(receipt_store, "emit_system_event", _emit_system_event)

    receipt_store._run_post_receipt_hooks(
        {
            "status": "failed",
            "failure_code": "provider.capacity",
            "job_label": "plan",
            "node_id": "plan",
            "receipt_id": "receipt:workflow_4:1:1",
            "run_id": "workflow_4",
        },
        conn=_Conn(),
    )

    assert len(events) == 1
    event = events[0]
    assert event["event_type"] == "post_receipt_hook.failed"
    assert event["payload"]["hook"] == "friction_ledger.record"
    assert event["payload"]["receipt_id"] == "receipt:workflow_4:1:1"
    assert event["payload"]["run_id"] == "workflow_4"
    assert event["payload"]["failure_code"] == "provider.capacity"
    assert event["payload"]["error_type"] == "RuntimeError"


def test_post_receipt_hooks_emit_event_for_auto_bug_failures(monkeypatch):
    events: list[dict[str, object]] = []

    class _NoopFrictionLedger:
        def __init__(self, conn):
            self.conn = conn

        def record(self, **_kwargs):
            return None

    class _FailingTracker:
        def __init__(self, conn):
            self.conn = conn

        def list_bugs(self, **_kwargs):
            raise RuntimeError("bug authority unavailable")

    class _Conn:
        def fetchval(self, *_args):
            raise AssertionError("bug lookup should fail before counting receipts")

    def _emit_system_event(_conn, **kwargs):
        events.append(kwargs)

    monkeypatch.setattr(receipt_store, "FrictionLedger", _NoopFrictionLedger)
    monkeypatch.setattr(receipt_store, "BugTracker", _FailingTracker)
    monkeypatch.setattr(receipt_store, "emit_system_event", _emit_system_event)

    receipt_store._run_post_receipt_hooks(
        {
            "status": "failed",
            "failure_code": "workflow_submission.required_missing",
            "job_label": "execute",
            "node_id": "execute",
            "receipt_id": "receipt:workflow_5:1:1",
            "run_id": "workflow_5",
        },
        conn=_Conn(),
    )

    assert len(events) == 1
    event = events[0]
    assert event["event_type"] == "post_receipt_hook.failed"
    assert event["payload"]["hook"] == "auto_bug_threshold"
    assert event["payload"]["receipt_id"] == "receipt:workflow_5:1:1"
    assert event["payload"]["run_id"] == "workflow_5"
    assert event["payload"]["failure_code"] == "workflow_submission.required_missing"
    assert event["payload"]["error_type"] == "RuntimeError"


def test_attach_receipt_structural_proof_passes_for_compact_git_provenance() -> None:
    outputs = {
        "git_provenance": {
            "available": True,
            "repo_snapshot_ref": "repo_snapshot:abc",
        },
    }
    enriched = receipt_store._attach_receipt_structural_proof(
        receipt_id="receipt:run:abc",
        outputs=outputs,
    )
    assert enriched["verification"]["status"] == "passed"
    assert enriched["verification"]["verifier_ref"] == "verifier.receipt.structural_proof"
    assert enriched["verification"]["kind"] == "structural_proof"
    assert enriched["verification_status"] == "passed"


def test_attach_receipt_structural_proof_marks_failed_when_provenance_missing() -> None:
    outputs = {"workspace_provenance": {}}
    enriched = receipt_store._attach_receipt_structural_proof(
        receipt_id="receipt:run:xyz",
        outputs=outputs,
    )
    assert enriched["verification"]["status"] == "failed"
    assert "git_provenance" in enriched["verification"]["missing"]
    assert enriched["verification_status"] == "failed"


def test_attach_receipt_structural_proof_preserves_existing_verification_status() -> None:
    outputs = {
        "git_provenance": {
            "available": True,
            "repo_snapshot_ref": "repo_snapshot:abc",
        },
        "verification_status": "passed_by_explicit_run",
    }
    enriched = receipt_store._attach_receipt_structural_proof(
        receipt_id="receipt:run:abc",
        outputs=outputs,
    )
    # New verification block was attached, but pre-existing verification_status
    # was not overwritten because setdefault is used for that field.
    assert enriched["verification"]["status"] == "passed"
    assert enriched["verification_status"] == "passed_by_explicit_run"


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
        if normalized.startswith("SELECT to_regclass('public.materialize_artifacts') IS NOT NULL AS materialize_artifacts_ready"):
            return {
                "materialize_artifacts_ready": True,
                "capability_catalog_ready": True,
                "verify_refs_ready": True,
                "verification_registry_ready": True,
                "materialize_index_snapshots_ready": True,
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
    assert metrics["compile_authority"]["materialize_artifacts_ready"] is True
    assert metrics["compile_authority"]["materialize_index_snapshots_ready"] is True
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
