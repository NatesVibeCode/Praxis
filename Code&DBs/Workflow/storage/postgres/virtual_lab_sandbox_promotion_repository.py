"""Postgres persistence for Virtual Lab sandbox promotion authority."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any

from .validators import (
    PostgresWriteError,
    _encode_jsonb,
    _optional_text,
    _require_mapping,
    _require_text,
)


def _normalize_row(row: Any, *, operation: str) -> dict[str, Any]:
    if row is None:
        raise PostgresWriteError(
            "virtual_lab_sandbox_promotion.write_failed",
            f"{operation} returned no row",
        )
    payload = dict(row)
    for key, value in list(payload.items()):
        if isinstance(value, str) and (key.endswith("_json") or key.endswith("_refs_json")):
            try:
                payload[key] = json.loads(value)
            except (TypeError, json.JSONDecodeError):
                continue
    return payload


def _normalize_optional_row(row: Any, *, operation: str) -> dict[str, Any] | None:
    if row is None:
        return None
    return _normalize_row(row, operation=operation)


def _normalize_rows(rows: Any, *, operation: str) -> list[dict[str, Any]]:
    return [_normalize_row(row, operation=operation) for row in (rows or [])]


def _timestamp(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise PostgresWriteError(
                "virtual_lab_sandbox_promotion.invalid_timestamp",
                f"{field_name} must be an ISO timestamp",
                details={"field_name": field_name, "value": value},
            ) from exc
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    raise PostgresWriteError(
        "virtual_lab_sandbox_promotion.invalid_timestamp",
        f"{field_name} must be an ISO timestamp",
        details={"field_name": field_name, "value": value},
    )


def _optional_timestamp(value: Any, *, field_name: str) -> datetime | None:
    if value is None or value == "":
        return None
    return _timestamp(value, field_name=field_name)


def _optional_clean_text(value: object, *, field_name: str) -> str | None:
    if value is None or value == "":
        return None
    return _optional_text(value, field_name=field_name)


def _list_payloads(value: object, *, field_name: str) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise PostgresWriteError(
            "virtual_lab_sandbox_promotion.invalid_payload",
            f"{field_name} must be a list of JSON objects",
            details={"field_name": field_name},
        )
    return [dict(item) for item in value]


def persist_virtual_lab_sandbox_promotion_record(
    conn: Any,
    *,
    promotion_record_id: str,
    manifest: dict[str, Any],
    candidate_records: list[dict[str, Any]],
    summary: dict[str, Any],
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    manifest_payload = dict(_require_mapping(manifest, field_name="manifest"))
    summary_payload = dict(_require_mapping(summary, field_name="summary"))
    candidate_payloads = _list_payloads(candidate_records, field_name="candidate_records")
    if not candidate_payloads:
        raise PostgresWriteError(
            "virtual_lab_sandbox_promotion.candidate_records_required",
            "candidate_records requires at least one candidate record",
        )

    simulation_run_ids = [_require_text(record.get("simulation_run_id"), field_name="simulation_run_id") for record in candidate_payloads]
    reports = [dict(_require_mapping(record.get("report"), field_name="candidate_record.report")) for record in candidate_payloads]
    ledgers = [dict(_require_mapping(record.get("ledger"), field_name="candidate_record.ledger")) for record in candidate_payloads]
    status_counts: dict[str, int] = {}
    for report in reports:
        status = _require_text(report.get("status"), field_name="report.status")
        status_counts[status] = status_counts.get(status, 0) + 1
    drift_classification_count = sum(len(ledger.get("classifications") or []) for ledger in ledgers)
    handoff_count = sum(
        len(classification.get("handoff_refs") or [])
        for ledger in ledgers
        for classification in ledger.get("classifications") or []
    )

    record_row = conn.fetchrow(
        """
        INSERT INTO virtual_lab_sandbox_promotion_records (
            promotion_record_id,
            manifest_id,
            manifest_digest,
            summary_id,
            summary_digest,
            recommendation,
            candidate_count,
            report_count,
            drift_classification_count,
            handoff_count,
            simulation_run_ids_json,
            status_counts_json,
            manifest_json,
            summary_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8,
            $9, $10, $11::jsonb, $12::jsonb, $13::jsonb,
            $14::jsonb, $15, $16
        )
        ON CONFLICT (promotion_record_id) DO UPDATE SET
            manifest_id = EXCLUDED.manifest_id,
            manifest_digest = EXCLUDED.manifest_digest,
            summary_id = EXCLUDED.summary_id,
            summary_digest = EXCLUDED.summary_digest,
            recommendation = EXCLUDED.recommendation,
            candidate_count = EXCLUDED.candidate_count,
            report_count = EXCLUDED.report_count,
            drift_classification_count = EXCLUDED.drift_classification_count,
            handoff_count = EXCLUDED.handoff_count,
            simulation_run_ids_json = EXCLUDED.simulation_run_ids_json,
            status_counts_json = EXCLUDED.status_counts_json,
            manifest_json = EXCLUDED.manifest_json,
            summary_json = EXCLUDED.summary_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING *
        """,
        _require_text(promotion_record_id, field_name="promotion_record_id"),
        _require_text(manifest_payload.get("manifest_id"), field_name="manifest.manifest_id"),
        _require_text(manifest_payload.get("manifest_digest"), field_name="manifest.manifest_digest"),
        _require_text(summary_payload.get("summary_id"), field_name="summary.summary_id"),
        _require_text(summary_payload.get("summary_digest"), field_name="summary.summary_digest"),
        _require_text(summary_payload.get("recommendation"), field_name="summary.recommendation"),
        len(candidate_payloads),
        len(reports),
        drift_classification_count,
        handoff_count,
        _encode_jsonb(simulation_run_ids, field_name="simulation_run_ids"),
        _encode_jsonb(status_counts, field_name="status_counts"),
        _encode_jsonb(manifest_payload, field_name="manifest"),
        _encode_jsonb(summary_payload, field_name="summary"),
        _optional_clean_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_clean_text(source_ref, field_name="source_ref"),
    )

    _delete_child_rows(conn, promotion_record_id)
    _insert_candidates(conn, promotion_record_id, candidate_payloads)
    _insert_executions(conn, promotion_record_id, candidate_payloads)
    _insert_readback_evidence(conn, promotion_record_id, candidate_payloads)
    _insert_reports(conn, promotion_record_id, candidate_payloads)
    _insert_report_rows(conn, promotion_record_id, candidate_payloads)
    _insert_ledgers(conn, promotion_record_id, candidate_payloads)
    _insert_classifications(conn, promotion_record_id, candidate_payloads)
    _insert_handoffs(conn, promotion_record_id, candidate_payloads)

    return {
        **_normalize_row(record_row, operation="persist_virtual_lab_sandbox_promotion_record"),
        "candidate_count": len(candidate_payloads),
        "report_count": len(reports),
        "drift_classification_count": drift_classification_count,
        "handoff_count": handoff_count,
    }


def _delete_child_rows(conn: Any, promotion_record_id: str) -> None:
    for table in (
        "virtual_lab_sandbox_handoffs",
        "virtual_lab_sandbox_drift_classifications",
        "virtual_lab_sandbox_drift_ledgers",
        "virtual_lab_sandbox_comparison_rows",
        "virtual_lab_sandbox_comparison_reports",
        "virtual_lab_sandbox_readback_evidence",
        "virtual_lab_sandbox_executions",
        "virtual_lab_sandbox_promotion_candidates",
    ):
        conn.execute(f"DELETE FROM {table} WHERE promotion_record_id = $1", promotion_record_id)


def _insert_candidates(conn: Any, promotion_record_id: str, records: list[dict[str, Any]]) -> None:
    conn.execute_many(
        """
        INSERT INTO virtual_lab_sandbox_promotion_candidates (
            promotion_record_id, candidate_id, simulation_run_id, owner,
            build_ref, sandbox_target, scope_ref, candidate_json,
            simulation_proof_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb)
        """,
        [
            (
                promotion_record_id,
                _require_text(record["candidate"].get("candidate_id"), field_name="candidate.candidate_id"),
                _require_text(record.get("simulation_run_id"), field_name="simulation_run_id"),
                _require_text(record["candidate"].get("owner"), field_name="candidate.owner"),
                _require_text(record["candidate"].get("build_ref"), field_name="candidate.build_ref"),
                _require_text(record["candidate"].get("sandbox_target"), field_name="candidate.sandbox_target"),
                _require_text(record["candidate"].get("scope_ref"), field_name="candidate.scope_ref"),
                _encode_jsonb(record["candidate"], field_name="candidate"),
                _encode_jsonb(record.get("simulation_proof") or {}, field_name="simulation_proof"),
            )
            for record in records
        ],
    )


def _insert_executions(conn: Any, promotion_record_id: str, records: list[dict[str, Any]]) -> None:
    conn.execute_many(
        """
        INSERT INTO virtual_lab_sandbox_executions (
            promotion_record_id, execution_id, candidate_id, scenario_ref,
            sandbox_target, environment_ref, config_ref, seed_data_ref,
            status, started_at, ended_at, execution_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb)
        """,
        [
            (
                promotion_record_id,
                _require_text(record["execution"].get("execution_id"), field_name="execution.execution_id"),
                _require_text(record["execution"].get("candidate_id"), field_name="execution.candidate_id"),
                _require_text(record["execution"].get("scenario_ref"), field_name="execution.scenario_ref"),
                _require_text(record["execution"].get("sandbox_target"), field_name="execution.sandbox_target"),
                _require_text(record["execution"].get("environment_ref"), field_name="execution.environment_ref"),
                _require_text(record["execution"].get("config_ref"), field_name="execution.config_ref"),
                _require_text(record["execution"].get("seed_data_ref"), field_name="execution.seed_data_ref"),
                _require_text(record["execution"].get("status"), field_name="execution.status"),
                _timestamp(record["execution"].get("started_at"), field_name="execution.started_at"),
                _optional_timestamp(record["execution"].get("ended_at"), field_name="execution.ended_at"),
                _encode_jsonb(record["execution"], field_name="execution"),
            )
            for record in records
        ],
    )


def _insert_readback_evidence(conn: Any, promotion_record_id: str, records: list[dict[str, Any]]) -> None:
    rows: list[tuple[object, ...]] = []
    for record in records:
        package = record["evidence_package"]
        for item in package.get("evidence") or []:
            rows.append(
                (
                    promotion_record_id,
                    _require_text(package.get("package_id"), field_name="evidence_package.package_id"),
                    _require_text(package.get("execution_id"), field_name="evidence_package.execution_id"),
                    _require_text(item.get("evidence_id"), field_name="evidence.evidence_id"),
                    _require_text(item.get("candidate_id"), field_name="evidence.candidate_id"),
                    _require_text(item.get("scenario_ref"), field_name="evidence.scenario_ref"),
                    _require_text(item.get("observable_ref"), field_name="evidence.observable_ref"),
                    _require_text(item.get("evidence_kind"), field_name="evidence.evidence_kind"),
                    _timestamp(item.get("captured_at"), field_name="evidence.captured_at"),
                    bool(item.get("available")),
                    bool(item.get("trusted")),
                    _optional_clean_text(item.get("immutable_ref"), field_name="evidence.immutable_ref"),
                    _encode_jsonb(item, field_name="evidence"),
                )
            )
    if not rows:
        return
    conn.execute_many(
        """
        INSERT INTO virtual_lab_sandbox_readback_evidence (
            promotion_record_id, evidence_package_id, execution_id,
            evidence_id, candidate_id, scenario_ref, observable_ref,
            evidence_kind, captured_at, available, trusted, immutable_ref,
            evidence_json
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8,
            $9, $10, $11, $12, $13::jsonb
        )
        """,
        rows,
    )


def _insert_reports(conn: Any, promotion_record_id: str, records: list[dict[str, Any]]) -> None:
    conn.execute_many(
        """
        INSERT INTO virtual_lab_sandbox_comparison_reports (
            promotion_record_id, report_id, candidate_id, scenario_ref,
            execution_id, evidence_package_id, status, report_digest,
            report_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
        """,
        [
            (
                promotion_record_id,
                _require_text(record["report"].get("report_id"), field_name="report.report_id"),
                _require_text(record["report"].get("candidate_id"), field_name="report.candidate_id"),
                _require_text(record["report"].get("scenario_ref"), field_name="report.scenario_ref"),
                _require_text(record["report"].get("execution_id"), field_name="report.execution_id"),
                _require_text(record["report"].get("evidence_package_id"), field_name="report.evidence_package_id"),
                _require_text(record["report"].get("status"), field_name="report.status"),
                _require_text(record["report"].get("report_digest"), field_name="report.report_digest"),
                _encode_jsonb(record["report"], field_name="report"),
            )
            for record in records
        ],
    )


def _insert_report_rows(conn: Any, promotion_record_id: str, records: list[dict[str, Any]]) -> None:
    rows: list[tuple[object, ...]] = []
    for record in records:
        report = record["report"]
        for item in report.get("rows") or []:
            rows.append(
                (
                    promotion_record_id,
                    _require_text(report.get("report_id"), field_name="report.report_id"),
                    _require_text(item.get("row_id"), field_name="comparison_row.row_id"),
                    _require_text(item.get("check_id"), field_name="comparison_row.check_id"),
                    _require_text(item.get("dimension"), field_name="comparison_row.dimension"),
                    _require_text(item.get("status"), field_name="comparison_row.status"),
                    _optional_clean_text(item.get("disposition"), field_name="comparison_row.disposition"),
                    _optional_clean_text(item.get("blocker_reason"), field_name="comparison_row.blocker_reason"),
                    _encode_jsonb(item, field_name="comparison_row"),
                )
            )
    conn.execute_many(
        """
        INSERT INTO virtual_lab_sandbox_comparison_rows (
            promotion_record_id, report_id, row_id, check_id, dimension,
            status, disposition, blocker_reason, row_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
        """,
        rows,
    )


def _insert_ledgers(conn: Any, promotion_record_id: str, records: list[dict[str, Any]]) -> None:
    conn.execute_many(
        """
        INSERT INTO virtual_lab_sandbox_drift_ledgers (
            promotion_record_id, ledger_id, comparison_report_id,
            ledger_digest, classification_count, ledger_json
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        """,
        [
            (
                promotion_record_id,
                _require_text(record["ledger"].get("ledger_id"), field_name="ledger.ledger_id"),
                _require_text(record["ledger"].get("comparison_report_id"), field_name="ledger.comparison_report_id"),
                _require_text(record["ledger"].get("ledger_digest"), field_name="ledger.ledger_digest"),
                len(record["ledger"].get("classifications") or []),
                _encode_jsonb(record["ledger"], field_name="ledger"),
            )
            for record in records
        ],
    )


def _insert_classifications(conn: Any, promotion_record_id: str, records: list[dict[str, Any]]) -> None:
    rows: list[tuple[object, ...]] = []
    reports_by_id = {record["report"]["report_id"]: record["report"] for record in records}
    ledgers_by_report_id = {record["ledger"]["comparison_report_id"]: record["ledger"] for record in records}
    for record in records:
        for item in record["ledger"].get("classifications") or []:
            report = reports_by_id[_require_text(item.get("comparison_report_id"), field_name="classification.comparison_report_id")]
            ledger = ledgers_by_report_id[report["report_id"]]
            rows.append(
                (
                    promotion_record_id,
                    _require_text(ledger.get("ledger_id"), field_name="ledger.ledger_id"),
                    _require_text(item.get("classification_id"), field_name="classification.classification_id"),
                    _require_text(item.get("comparison_report_id"), field_name="classification.comparison_report_id"),
                    _require_text(report.get("candidate_id"), field_name="report.candidate_id"),
                    _require_text(item.get("row_id"), field_name="classification.row_id"),
                    _encode_jsonb(item.get("reason_codes") or [], field_name="classification.reason_codes"),
                    _require_text(item.get("severity"), field_name="classification.severity"),
                    _require_text(item.get("layer"), field_name="classification.layer"),
                    _require_text(item.get("disposition"), field_name="classification.disposition"),
                    _require_text(item.get("owner"), field_name="classification.owner"),
                    _encode_jsonb(item, field_name="classification"),
                )
            )
    if not rows:
        return
    conn.execute_many(
        """
        INSERT INTO virtual_lab_sandbox_drift_classifications (
            promotion_record_id, ledger_id, classification_id,
            comparison_report_id, candidate_id, row_id, reason_codes_json,
            severity, layer, disposition, owner, classification_json
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7::jsonb,
            $8, $9, $10, $11, $12::jsonb
        )
        """,
        rows,
    )


def _insert_handoffs(conn: Any, promotion_record_id: str, records: list[dict[str, Any]]) -> None:
    rows: list[tuple[object, ...]] = []
    reports_by_id = {record["report"]["report_id"]: record["report"] for record in records}
    for record in records:
        for classification in record["ledger"].get("classifications") or []:
            report = reports_by_id[_require_text(classification.get("comparison_report_id"), field_name="classification.comparison_report_id")]
            for item in classification.get("handoff_refs") or []:
                rows.append(
                    (
                        promotion_record_id,
                        _require_text(classification.get("classification_id"), field_name="classification.classification_id"),
                        _require_text(report.get("candidate_id"), field_name="report.candidate_id"),
                        _require_text(item.get("handoff_kind"), field_name="handoff.handoff_kind"),
                        _require_text(item.get("target_ref"), field_name="handoff.target_ref"),
                        _require_text(item.get("status"), field_name="handoff.status"),
                        _encode_jsonb(item, field_name="handoff"),
                    )
                )
    if not rows:
        return
    conn.execute_many(
        """
        INSERT INTO virtual_lab_sandbox_handoffs (
            promotion_record_id, classification_id, candidate_id,
            handoff_kind, target_ref, status, handoff_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
        """,
        rows,
    )


def list_virtual_lab_sandbox_promotion_records(
    conn: Any,
    *,
    manifest_id: str | None = None,
    candidate_id: str | None = None,
    simulation_run_id: str | None = None,
    recommendation: str | None = None,
    comparison_status: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    clauses = ["TRUE"]
    args: list[Any] = []
    if manifest_id:
        args.append(manifest_id)
        clauses.append(f"r.manifest_id = ${len(args)}")
    if recommendation:
        args.append(recommendation)
        clauses.append(f"r.recommendation = ${len(args)}")
    if candidate_id:
        args.append(candidate_id)
        clauses.append(
            "EXISTS (SELECT 1 FROM virtual_lab_sandbox_promotion_candidates c "
            f"WHERE c.promotion_record_id = r.promotion_record_id AND c.candidate_id = ${len(args)})"
        )
    if simulation_run_id:
        args.append(simulation_run_id)
        clauses.append(
            "EXISTS (SELECT 1 FROM virtual_lab_sandbox_promotion_candidates c "
            f"WHERE c.promotion_record_id = r.promotion_record_id AND c.simulation_run_id = ${len(args)})"
        )
    if comparison_status:
        args.append(comparison_status)
        clauses.append(
            "EXISTS (SELECT 1 FROM virtual_lab_sandbox_comparison_reports cr "
            f"WHERE cr.promotion_record_id = r.promotion_record_id AND cr.status = ${len(args)})"
        )
    args.append(max(1, min(int(limit), 500)))
    rows = conn.fetch(
        f"""
        SELECT r.*
          FROM virtual_lab_sandbox_promotion_records r
         WHERE {' AND '.join(clauses)}
         ORDER BY r.updated_at DESC, r.promotion_record_id
         LIMIT ${len(args)}
        """,
        *args,
    )
    return _normalize_rows(rows, operation="list_virtual_lab_sandbox_promotion_records")


def load_virtual_lab_sandbox_promotion_record(
    conn: Any,
    *,
    promotion_record_id: str,
    include_candidates: bool = True,
    include_executions: bool = True,
    include_readback: bool = True,
    include_reports: bool = True,
    include_drift: bool = True,
    include_handoffs: bool = True,
) -> dict[str, Any] | None:
    record = _normalize_optional_row(
        conn.fetchrow(
            "SELECT * FROM virtual_lab_sandbox_promotion_records WHERE promotion_record_id = $1",
            promotion_record_id,
        ),
        operation="load_virtual_lab_sandbox_promotion_record",
    )
    if record is None:
        return None
    if include_candidates:
        record["candidates"] = _fetch_child_json(
            conn,
            table="virtual_lab_sandbox_promotion_candidates",
            json_column="candidate_json",
            promotion_record_id=promotion_record_id,
            order_by="candidate_id",
        )
    if include_executions:
        record["executions"] = _fetch_child_json(
            conn,
            table="virtual_lab_sandbox_executions",
            json_column="execution_json",
            promotion_record_id=promotion_record_id,
            order_by="execution_id",
        )
    if include_readback:
        record["readback_evidence"] = _fetch_child_json(
            conn,
            table="virtual_lab_sandbox_readback_evidence",
            json_column="evidence_json",
            promotion_record_id=promotion_record_id,
            order_by="captured_at",
        )
    if include_reports:
        record["comparison_reports"] = _fetch_child_json(
            conn,
            table="virtual_lab_sandbox_comparison_reports",
            json_column="report_json",
            promotion_record_id=promotion_record_id,
            order_by="report_id",
        )
        record["comparison_rows"] = _fetch_child_json(
            conn,
            table="virtual_lab_sandbox_comparison_rows",
            json_column="row_json",
            promotion_record_id=promotion_record_id,
            order_by="row_id",
        )
    if include_drift:
        record["drift_ledgers"] = _fetch_child_json(
            conn,
            table="virtual_lab_sandbox_drift_ledgers",
            json_column="ledger_json",
            promotion_record_id=promotion_record_id,
            order_by="ledger_id",
        )
        record["drift_classifications"] = _fetch_child_json(
            conn,
            table="virtual_lab_sandbox_drift_classifications",
            json_column="classification_json",
            promotion_record_id=promotion_record_id,
            order_by="classification_id",
        )
    if include_handoffs:
        record["handoffs"] = _fetch_child_json(
            conn,
            table="virtual_lab_sandbox_handoffs",
            json_column="handoff_json",
            promotion_record_id=promotion_record_id,
            order_by="target_ref",
        )
    return record


def list_virtual_lab_sandbox_drift_classifications(
    conn: Any,
    *,
    promotion_record_id: str | None = None,
    candidate_id: str | None = None,
    reason_code: str | None = None,
    severity: str | None = None,
    layer: str | None = None,
    disposition: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    clauses = ["TRUE"]
    args: list[Any] = []
    for column, value in (
        ("promotion_record_id", promotion_record_id),
        ("candidate_id", candidate_id),
        ("severity", severity),
        ("layer", layer),
        ("disposition", disposition),
    ):
        if value:
            args.append(value)
            clauses.append(f"{column} = ${len(args)}")
    if reason_code:
        args.append(reason_code)
        clauses.append(f"reason_codes_json ? ${len(args)}")
    args.append(max(1, min(int(limit), 500)))
    rows = conn.fetch(
        f"""
        SELECT *
          FROM virtual_lab_sandbox_drift_classifications
         WHERE {' AND '.join(clauses)}
         ORDER BY created_at DESC, classification_id
         LIMIT ${len(args)}
        """,
        *args,
    )
    return _normalize_rows(rows, operation="list_virtual_lab_sandbox_drift_classifications")


def list_virtual_lab_sandbox_handoffs(
    conn: Any,
    *,
    promotion_record_id: str | None = None,
    candidate_id: str | None = None,
    handoff_kind: str | None = None,
    status: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    clauses = ["TRUE"]
    args: list[Any] = []
    for column, value in (
        ("promotion_record_id", promotion_record_id),
        ("candidate_id", candidate_id),
        ("handoff_kind", handoff_kind),
        ("status", status),
    ):
        if value:
            args.append(value)
            clauses.append(f"{column} = ${len(args)}")
    args.append(max(1, min(int(limit), 500)))
    rows = conn.fetch(
        f"""
        SELECT *
          FROM virtual_lab_sandbox_handoffs
         WHERE {' AND '.join(clauses)}
         ORDER BY created_at DESC, target_ref
         LIMIT ${len(args)}
        """,
        *args,
    )
    return _normalize_rows(rows, operation="list_virtual_lab_sandbox_handoffs")


def list_virtual_lab_sandbox_readback_evidence(
    conn: Any,
    *,
    promotion_record_id: str | None = None,
    candidate_id: str | None = None,
    available: bool | None = None,
    trusted: bool | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    clauses = ["TRUE"]
    args: list[Any] = []
    if promotion_record_id:
        args.append(promotion_record_id)
        clauses.append(f"promotion_record_id = ${len(args)}")
    if candidate_id:
        args.append(candidate_id)
        clauses.append(f"candidate_id = ${len(args)}")
    if available is not None:
        args.append(bool(available))
        clauses.append(f"available = ${len(args)}")
    if trusted is not None:
        args.append(bool(trusted))
        clauses.append(f"trusted = ${len(args)}")
    args.append(max(1, min(int(limit), 500)))
    rows = conn.fetch(
        f"""
        SELECT *
          FROM virtual_lab_sandbox_readback_evidence
         WHERE {' AND '.join(clauses)}
         ORDER BY captured_at DESC, evidence_id
         LIMIT ${len(args)}
        """,
        *args,
    )
    return _normalize_rows(rows, operation="list_virtual_lab_sandbox_readback_evidence")


def _fetch_child_json(
    conn: Any,
    *,
    table: str,
    json_column: str,
    promotion_record_id: str,
    order_by: str,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        f"SELECT {json_column} FROM {table} WHERE promotion_record_id = $1 ORDER BY {order_by}",
        promotion_record_id,
    )
    return [row[json_column] for row in _normalize_rows(rows, operation=f"fetch_{table}")]


__all__ = [
    "persist_virtual_lab_sandbox_promotion_record",
    "list_virtual_lab_sandbox_promotion_records",
    "load_virtual_lab_sandbox_promotion_record",
    "list_virtual_lab_sandbox_drift_classifications",
    "list_virtual_lab_sandbox_handoffs",
    "list_virtual_lab_sandbox_readback_evidence",
]
