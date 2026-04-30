from __future__ import annotations

from storage.postgres import virtual_lab_sandbox_promotion_repository as repo
from runtime.virtual_lab.sandbox_drift import (
    PredictedActualCheck,
    PromotionCandidate,
    PromotionManifest,
    SandboxEvidencePackage,
    SandboxExecutionRecord,
    SandboxReadbackEvidence,
    build_drift_ledger,
    build_stop_continue_summary,
    compare_predicted_actual,
)


class _RecordingConn:
    def __init__(self) -> None:
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.batch_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def fetchrow(self, sql: str, *args):
        self.fetchrow_calls.append((sql, args))
        if "INSERT INTO virtual_lab_sandbox_promotion_records" in sql:
            return {
                "promotion_record_id": args[0],
                "manifest_id": args[1],
                "manifest_digest": args[2],
                "summary_id": args[3],
                "summary_digest": args[4],
                "recommendation": args[5],
                "candidate_count": args[6],
                "report_count": args[7],
                "drift_classification_count": args[8],
                "handoff_count": args[9],
            }
        return {}

    def fetch(self, sql: str, *args):
        self.fetch_calls.append((sql, args))
        return []

    def execute(self, sql: str, *args):
        self.execute_calls.append((sql, args))

    def execute_many(self, sql: str, rows: list[tuple[object, ...]]) -> None:
        self.batch_calls.append((sql, rows))


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


def _candidate_record() -> tuple[dict, dict]:
    candidate = _candidate()
    manifest = _manifest()
    execution = _execution()
    package = _package()
    check = PredictedActualCheck(
        check_id="check.status",
        dimension="output",
        prediction={"status": "qualified"},
        actual={"status": "qualified"},
        evidence_refs=("evidence.account_status",),
    )
    report = compare_predicted_actual(
        candidate=candidate,
        execution=execution,
        evidence_package=package,
        checks=(check,),
        report_id="comparison.qualify_account",
    )
    ledger = build_drift_ledger(report=report, classifications=(), ledger_id="ledger.qualify_account")
    summary = build_stop_continue_summary(
        manifest=manifest,
        reports=(report,),
        ledgers=(ledger,),
        summary_id="summary.phase8.match",
    )
    record = {
        "candidate": candidate.to_json(),
        "simulation_run_id": "virtual_lab_simulation_run.phase_07_proof",
        "simulation_proof": {
            "run_id": "virtual_lab_simulation_run.phase_07_proof",
            "status": "passed",
            "verifier_count": 1,
            "blocker_count": 0,
        },
        "execution": execution.to_json(),
        "evidence_package": package.to_json(),
        "checks": [check.to_json()],
        "report": report.to_json(),
        "ledger": ledger.to_json(),
    }
    return record, summary.to_json()


def test_persist_virtual_lab_sandbox_promotion_record_writes_parent_and_children() -> None:
    conn = _RecordingConn()
    candidate_record, summary = _candidate_record()

    persisted = repo.persist_virtual_lab_sandbox_promotion_record(
        conn,
        promotion_record_id="sandbox_promotion_record.demo",
        manifest=_manifest().to_json(),
        candidate_records=[candidate_record],
        summary=summary,
        observed_by_ref="operator:nate",
        source_ref="phase_08_test",
    )

    assert "INSERT INTO virtual_lab_sandbox_promotion_records" in conn.fetchrow_calls[0][0]
    assert persisted["promotion_record_id"] == "sandbox_promotion_record.demo"
    assert persisted["recommendation"] == "continue"
    assert any("DELETE FROM virtual_lab_sandbox_handoffs" in call[0] for call in conn.execute_calls)
    assert any("INSERT INTO virtual_lab_sandbox_promotion_candidates" in call[0] for call in conn.batch_calls)
    assert any("INSERT INTO virtual_lab_sandbox_executions" in call[0] for call in conn.batch_calls)
    assert any("INSERT INTO virtual_lab_sandbox_readback_evidence" in call[0] for call in conn.batch_calls)
    assert any("INSERT INTO virtual_lab_sandbox_comparison_reports" in call[0] for call in conn.batch_calls)
    assert any("INSERT INTO virtual_lab_sandbox_comparison_rows" in call[0] for call in conn.batch_calls)
    assert any("INSERT INTO virtual_lab_sandbox_drift_ledgers" in call[0] for call in conn.batch_calls)
