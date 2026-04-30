from __future__ import annotations

from types import SimpleNamespace

import pytest

from runtime.operations.commands import virtual_lab_sandbox_promotion as commands
from runtime.operations.queries import virtual_lab_sandbox_promotion as queries
from runtime.virtual_lab.sandbox_drift import (
    PredictedActualCheck,
    PromotionCandidate,
    PromotionManifest,
    SandboxDriftError,
    SandboxEvidencePackage,
    SandboxExecutionRecord,
    SandboxReadbackEvidence,
)


def _subsystems():
    return SimpleNamespace(get_pg_conn=lambda: object())


def _candidate() -> PromotionCandidate:
    return PromotionCandidate(
        candidate_id="candidate.phase8.account_sync",
        owner="operator:nate",
        build_ref="build.account_sync.20260430",
        sandbox_target="sandbox.crm.dev",
        scope_ref="scope.client_operating_model.phase_08",
        scenario_refs=("scenario.qualify_account",),
        prediction_refs=("prediction.qualify_account.status",),
    )


def _manifest() -> PromotionManifest:
    return PromotionManifest(
        manifest_id="manifest.phase8",
        created_at="2026-04-30T18:00:00Z",
        created_by="agent.phase_08",
        candidates=(_candidate(),),
    )


def _execution() -> SandboxExecutionRecord:
    return SandboxExecutionRecord(
        execution_id="execution.qualify_account.1",
        candidate_id="candidate.phase8.account_sync",
        scenario_ref="scenario.qualify_account",
        sandbox_target="sandbox.crm.dev",
        environment_ref="sandbox.crm.dev/env/phase8",
        config_ref="config.crm.phase8.locked",
        seed_data_ref="seed.crm.phase8.accounts",
        status="completed",
        started_at="2026-04-30T18:05:00Z",
        ended_at="2026-04-30T18:06:00Z",
    )


def _package() -> SandboxEvidencePackage:
    return SandboxEvidencePackage(
        package_id="evidence_package.qualify_account",
        execution_id="execution.qualify_account.1",
        candidate_id="candidate.phase8.account_sync",
        scenario_ref="scenario.qualify_account",
        evidence=(
            SandboxReadbackEvidence(
                evidence_id="evidence.account_status",
                candidate_id="candidate.phase8.account_sync",
                scenario_ref="scenario.qualify_account",
                observable_ref="crm.account.account:001.status",
                evidence_kind="api_response",
                captured_at="2026-04-30T18:06:01Z",
                available=True,
                trusted=True,
                immutable_ref="receipt.sandbox.readback.1",
                payload={"status": "qualified"},
            ),
        ),
    )


def _check() -> PredictedActualCheck:
    return PredictedActualCheck(
        check_id="check.status",
        dimension="output",
        prediction={"status": "qualified"},
        actual={"status": "qualified"},
        evidence_refs=("evidence.account_status",),
    )


def _simulation_run(status: str = "passed", *, verifier_status: str = "passed"):
    return {
        "run_id": "virtual_lab_simulation_run.phase_07_proof",
        "scenario_id": "scenario.qualify_account",
        "status": status,
        "result_digest": "digest.result",
        "trace_digest": "digest.trace",
        "verifier_results": [{"verifier_id": "verifier.no_blockers", "status": verifier_status}],
        "promotion_blockers": [],
    }


def test_sandbox_promotion_record_requires_simulation_proof_and_persists(monkeypatch) -> None:
    persist_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        commands,
        "load_virtual_lab_simulation_run",
        lambda conn, run_id, **kwargs: _simulation_run(),
    )

    def _persist(conn, **kwargs):
        persist_calls.append(kwargs)
        return {
            "promotion_record_id": kwargs["promotion_record_id"],
            "recommendation": kwargs["summary"]["recommendation"],
        }

    monkeypatch.setattr(commands, "persist_virtual_lab_sandbox_promotion_record", _persist)

    result = commands.handle_virtual_lab_sandbox_promotion_record(
        commands.RecordVirtualLabSandboxPromotionCommand(
            manifest=_manifest().to_json(),
            candidate_records=[
                {
                    "candidate_id": "candidate.phase8.account_sync",
                    "simulation_run_id": "virtual_lab_simulation_run.phase_07_proof",
                    "execution": _execution().to_json(),
                    "evidence_package": _package().to_json(),
                    "checks": [_check().to_json()],
                }
            ],
            observed_by_ref="operator:nate",
            source_ref="phase_08_test",
        ),
        _subsystems(),
    )

    assert result["ok"] is True
    assert result["operation"] == "virtual_lab_sandbox_promotion_record"
    assert result["recommendation"] == "continue"
    assert result["event_payload"]["simulation_run_ids"] == [
        "virtual_lab_simulation_run.phase_07_proof"
    ]
    assert persist_calls[0]["candidate_records"][0]["report"]["status"] == "match"
    assert persist_calls[0]["candidate_records"][0]["simulation_proof"]["verifier_count"] == 1


def test_sandbox_promotion_record_rejects_unverified_simulation(monkeypatch) -> None:
    monkeypatch.setattr(
        commands,
        "load_virtual_lab_simulation_run",
        lambda conn, run_id, **kwargs: _simulation_run(status="failed", verifier_status="failed"),
    )

    with pytest.raises(SandboxDriftError) as excinfo:
        commands.handle_virtual_lab_sandbox_promotion_record(
            commands.RecordVirtualLabSandboxPromotionCommand(
                manifest=_manifest().to_json(),
                candidate_records=[
                    {
                        "candidate_id": "candidate.phase8.account_sync",
                        "simulation_run_id": "virtual_lab_simulation_run.phase_07_failed",
                        "execution": _execution().to_json(),
                        "evidence_package": _package().to_json(),
                        "checks": [_check().to_json()],
                    }
                ],
            ),
            _subsystems(),
        )

    assert excinfo.value.reason_code == "sandbox_drift.simulation_verifier_proof_required"


def test_sandbox_promotion_read_lists_records_and_drift(monkeypatch) -> None:
    monkeypatch.setattr(
        queries,
        "list_virtual_lab_sandbox_promotion_records",
        lambda conn, **kwargs: [{"promotion_record_id": "sandbox_promotion_record.demo", **kwargs}],
    )
    monkeypatch.setattr(
        queries,
        "load_virtual_lab_sandbox_promotion_record",
        lambda conn, promotion_record_id, **kwargs: {
            "promotion_record_id": promotion_record_id,
            "comparison_reports": [{}] if kwargs["include_reports"] else [],
        },
    )
    monkeypatch.setattr(
        queries,
        "list_virtual_lab_sandbox_drift_classifications",
        lambda conn, **kwargs: [{"reason_code": kwargs["reason_code"]}],
    )
    monkeypatch.setattr(
        queries,
        "list_virtual_lab_sandbox_handoffs",
        lambda conn, **kwargs: [{"handoff_kind": kwargs["handoff_kind"]}],
    )
    monkeypatch.setattr(
        queries,
        "list_virtual_lab_sandbox_readback_evidence",
        lambda conn, **kwargs: [{"trusted": kwargs["trusted"]}],
    )

    listed = queries.handle_virtual_lab_sandbox_promotion_read(
        queries.QueryVirtualLabSandboxPromotionRead(
            action="list_records",
            recommendation="rerun_phase",
        ),
        _subsystems(),
    )
    described = queries.handle_virtual_lab_sandbox_promotion_read(
        queries.QueryVirtualLabSandboxPromotionRead(
            action="describe_record",
            promotion_record_id="sandbox_promotion_record.demo",
            include_reports=True,
        ),
        _subsystems(),
    )
    drift = queries.handle_virtual_lab_sandbox_promotion_read(
        queries.QueryVirtualLabSandboxPromotionRead(
            action="list_drift",
            reason_code="OBSERVABILITY_GAP",
        ),
        _subsystems(),
    )
    handoffs = queries.handle_virtual_lab_sandbox_promotion_read(
        queries.QueryVirtualLabSandboxPromotionRead(
            action="list_handoffs",
            handoff_kind="gap",
        ),
        _subsystems(),
    )
    readback = queries.handle_virtual_lab_sandbox_promotion_read(
        queries.QueryVirtualLabSandboxPromotionRead(
            action="list_readback_evidence",
            trusted=False,
        ),
        _subsystems(),
    )

    assert listed["items"][0]["recommendation"] == "rerun_phase"
    assert described["record"]["promotion_record_id"] == "sandbox_promotion_record.demo"
    assert drift["items"][0]["reason_code"] == "OBSERVABILITY_GAP"
    assert handoffs["items"][0]["handoff_kind"] == "gap"
    assert readback["items"][0]["trusted"] is False
