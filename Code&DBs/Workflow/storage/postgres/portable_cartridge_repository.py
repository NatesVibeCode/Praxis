"""Postgres persistence for portable cartridge deployment contract authority."""

from __future__ import annotations

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
            "portable_cartridge.write_failed",
            f"{operation} returned no row",
        )
    payload = dict(row)
    for key, value in list(payload.items()):
        if isinstance(value, str) and (key.endswith("_json") or key.endswith("_dimensions_json")):
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


def _optional_clean_text(value: object, *, field_name: str) -> str | None:
    if value is None or value == "":
        return None
    return _optional_text(value, field_name=field_name)


def _list_payloads(value: object, *, field_name: str) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise PostgresWriteError(
            "portable_cartridge.invalid_payload",
            f"{field_name} must be a list of JSON objects",
            details={"field_name": field_name},
        )
    return [dict(item) for item in value]


def persist_portable_cartridge_record(
    conn: Any,
    *,
    cartridge_record_id: str,
    manifest: dict[str, Any],
    validation_report: dict[str, Any],
    deployment_contract: dict[str, Any],
    readiness_status: str,
    deployment_mode: str,
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    manifest_payload = dict(_require_mapping(manifest, field_name="manifest"))
    validation_payload = dict(_require_mapping(validation_report, field_name="validation_report"))
    deployment_payload = dict(_require_mapping(deployment_contract, field_name="deployment_contract"))

    object_truth_dependencies = _object_truth_dependencies(manifest_payload)
    assets = _list_payloads(manifest_payload.get("assets"), field_name="manifest.assets")
    bindings = _list_payloads(manifest_payload.get("bindings"), field_name="manifest.bindings")
    verification = dict(_require_mapping(manifest_payload.get("verification"), field_name="manifest.verification"))
    verifier_checks = _list_payloads(
        verification.get("required_checks"),
        field_name="manifest.verification.required_checks",
    )
    audit = dict(_require_mapping(manifest_payload.get("audit"), field_name="manifest.audit"))
    drift_hooks = _list_payloads(audit.get("drift_hooks"), field_name="manifest.audit.drift_hooks")

    row = conn.fetchrow(
        """
        INSERT INTO portable_cartridge_records (
            cartridge_record_id,
            cartridge_id,
            cartridge_version,
            build_id,
            manifest_version,
            manifest_digest,
            deployment_mode,
            readiness_status,
            error_count,
            warning_count,
            object_truth_dependency_count,
            asset_count,
            binding_count,
            required_binding_count,
            verifier_check_count,
            drift_hook_count,
            runtime_sizing_class,
            manifest_json,
            validation_report_json,
            deployment_contract_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8,
            $9, $10, $11, $12, $13, $14, $15, $16,
            $17, $18::jsonb, $19::jsonb, $20::jsonb, $21, $22
        )
        ON CONFLICT (cartridge_record_id) DO UPDATE SET
            cartridge_id = EXCLUDED.cartridge_id,
            cartridge_version = EXCLUDED.cartridge_version,
            build_id = EXCLUDED.build_id,
            manifest_version = EXCLUDED.manifest_version,
            manifest_digest = EXCLUDED.manifest_digest,
            deployment_mode = EXCLUDED.deployment_mode,
            readiness_status = EXCLUDED.readiness_status,
            error_count = EXCLUDED.error_count,
            warning_count = EXCLUDED.warning_count,
            object_truth_dependency_count = EXCLUDED.object_truth_dependency_count,
            asset_count = EXCLUDED.asset_count,
            binding_count = EXCLUDED.binding_count,
            required_binding_count = EXCLUDED.required_binding_count,
            verifier_check_count = EXCLUDED.verifier_check_count,
            drift_hook_count = EXCLUDED.drift_hook_count,
            runtime_sizing_class = EXCLUDED.runtime_sizing_class,
            manifest_json = EXCLUDED.manifest_json,
            validation_report_json = EXCLUDED.validation_report_json,
            deployment_contract_json = EXCLUDED.deployment_contract_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING *
        """,
        _require_text(cartridge_record_id, field_name="cartridge_record_id"),
        _require_text(manifest_payload.get("cartridge_id"), field_name="manifest.cartridge_id"),
        _require_text(manifest_payload.get("cartridge_version"), field_name="manifest.cartridge_version"),
        _require_text(manifest_payload.get("build_id"), field_name="manifest.build_id"),
        _require_text(manifest_payload.get("manifest_version"), field_name="manifest.manifest_version"),
        _require_text(validation_payload.get("canonical_digest"), field_name="validation_report.canonical_digest"),
        _require_text(deployment_mode, field_name="deployment_mode"),
        _require_text(readiness_status, field_name="readiness_status"),
        int(validation_payload.get("error_count") or 0),
        int(validation_payload.get("warning_count") or 0),
        len(object_truth_dependencies),
        len(assets),
        len(bindings),
        sum(1 for binding in bindings if bool(binding.get("required"))),
        len(verifier_checks),
        len(drift_hooks),
        _require_text(deployment_payload.get("runtime_sizing_class"), field_name="deployment_contract.runtime_sizing_class"),
        _encode_jsonb(manifest_payload, field_name="manifest"),
        _encode_jsonb(validation_payload, field_name="validation_report"),
        _encode_jsonb(deployment_payload, field_name="deployment_contract"),
        _optional_clean_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_clean_text(source_ref, field_name="source_ref"),
    )

    _delete_child_rows(conn, cartridge_record_id)
    _insert_object_truth_dependencies(conn, cartridge_record_id, object_truth_dependencies)
    _insert_assets(conn, cartridge_record_id, assets)
    _insert_bindings(conn, cartridge_record_id, bindings)
    _insert_verifier_checks(conn, cartridge_record_id, verifier_checks)
    _insert_drift_hooks(conn, cartridge_record_id, drift_hooks)

    return _normalize_row(row, operation="persist_portable_cartridge_record")


def _object_truth_dependencies(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    object_truth = dict(_require_mapping(manifest.get("object_truth"), field_name="manifest.object_truth"))
    dependencies: list[dict[str, Any]] = []
    for dependency_class in ("primary", "optional", "derived"):
        for item in _list_payloads(
            object_truth.get(dependency_class),
            field_name=f"manifest.object_truth.{dependency_class}",
        ):
            dependencies.append({**item, "dependency_class": dependency_class})
    return dependencies


def _delete_child_rows(conn: Any, cartridge_record_id: str) -> None:
    for table in (
        "portable_cartridge_drift_hooks",
        "portable_cartridge_verifier_checks",
        "portable_cartridge_binding_contracts",
        "portable_cartridge_assets",
        "portable_cartridge_object_truth_dependencies",
    ):
        conn.execute(f"DELETE FROM {table} WHERE cartridge_record_id = $1", cartridge_record_id)


def _insert_object_truth_dependencies(
    conn: Any,
    cartridge_record_id: str,
    dependencies: list[dict[str, Any]],
) -> None:
    if not dependencies:
        return
    conn.execute_many(
        """
        INSERT INTO portable_cartridge_object_truth_dependencies (
            cartridge_record_id, dependency_id, dependency_class, object_ref,
            authority_source, version, digest, failure_policy, required,
            dependency_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
        """,
        [
            (
                cartridge_record_id,
                _require_text(item.get("dependency_id"), field_name="dependency.dependency_id"),
                _require_text(item.get("dependency_class"), field_name="dependency.dependency_class"),
                _optional_clean_text(item.get("object_ref"), field_name="dependency.object_ref"),
                _require_text(item.get("authority_source"), field_name="dependency.authority_source"),
                _optional_clean_text(item.get("version"), field_name="dependency.version"),
                _optional_clean_text(item.get("digest"), field_name="dependency.digest"),
                _require_text(item.get("failure_policy"), field_name="dependency.failure_policy"),
                bool(item.get("required")),
                _encode_jsonb(item, field_name="dependency"),
            )
            for item in dependencies
        ],
    )


def _insert_assets(conn: Any, cartridge_record_id: str, assets: list[dict[str, Any]]) -> None:
    if not assets:
        return
    conn.execute_many(
        """
        INSERT INTO portable_cartridge_assets (
            cartridge_record_id, asset_path, role, media_type, size_bytes,
            digest, executable, required, asset_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
        """,
        [
            (
                cartridge_record_id,
                _require_text(item.get("path"), field_name="asset.path"),
                _require_text(item.get("role"), field_name="asset.role"),
                _require_text(item.get("media_type"), field_name="asset.media_type"),
                int(item.get("size_bytes") or 0),
                _require_text(item.get("digest"), field_name="asset.digest"),
                bool(item.get("executable")),
                bool(item.get("required")),
                _encode_jsonb(item, field_name="asset"),
            )
            for item in assets
        ],
    )


def _insert_bindings(conn: Any, cartridge_record_id: str, bindings: list[dict[str, Any]]) -> None:
    if not bindings:
        return
    conn.execute_many(
        """
        INSERT INTO portable_cartridge_binding_contracts (
            cartridge_record_id, binding_id, kind, required, resolution_phase,
            source, target, contract_ref, binding_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
        """,
        [
            (
                cartridge_record_id,
                _require_text(item.get("binding_id"), field_name="binding.binding_id"),
                _require_text(item.get("kind"), field_name="binding.kind"),
                bool(item.get("required")),
                _require_text(item.get("resolution_phase"), field_name="binding.resolution_phase"),
                _require_text(item.get("source"), field_name="binding.source"),
                _require_text(item.get("target"), field_name="binding.target"),
                _require_text(item.get("contract_ref"), field_name="binding.contract_ref"),
                _encode_jsonb(item, field_name="binding"),
            )
            for item in bindings
        ],
    )


def _insert_verifier_checks(
    conn: Any,
    cartridge_record_id: str,
    checks: list[dict[str, Any]],
) -> None:
    if not checks:
        return
    conn.execute_many(
        """
        INSERT INTO portable_cartridge_verifier_checks (
            cartridge_record_id, check_id, category, required,
            contract_ref, entrypoint, reason_code_family, verifier_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
        """,
        [
            (
                cartridge_record_id,
                _require_text(item.get("check_id"), field_name="verifier.check_id"),
                _require_text(item.get("category"), field_name="verifier.category"),
                bool(item.get("required")),
                _optional_clean_text(item.get("contract_ref"), field_name="verifier.contract_ref"),
                _optional_clean_text(item.get("entrypoint"), field_name="verifier.entrypoint"),
                _require_text(item.get("reason_code_family"), field_name="verifier.reason_code_family"),
                _encode_jsonb(item, field_name="verifier"),
            )
            for item in checks
        ],
    )


def _insert_drift_hooks(conn: Any, cartridge_record_id: str, hooks: list[dict[str, Any]]) -> None:
    if not hooks:
        return
    conn.execute_many(
        """
        INSERT INTO portable_cartridge_drift_hooks (
            cartridge_record_id, hook_id, hook_point, required,
            drift_dimensions_json, evidence_contract_ref, hook_json
        ) VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7::jsonb)
        """,
        [
            (
                cartridge_record_id,
                _require_text(item.get("hook_id"), field_name="drift_hook.hook_id"),
                _require_text(item.get("hook_point"), field_name="drift_hook.hook_point"),
                bool(item.get("required")),
                _encode_jsonb(item.get("drift_dimensions") or [], field_name="drift_hook.drift_dimensions"),
                _require_text(item.get("evidence_contract_ref"), field_name="drift_hook.evidence_contract_ref"),
                _encode_jsonb(item, field_name="drift_hook"),
            )
            for item in hooks
        ],
    )


def list_portable_cartridge_records(
    conn: Any,
    *,
    cartridge_id: str | None = None,
    readiness_status: str | None = None,
    deployment_mode: str | None = None,
    manifest_digest: str | None = None,
    source_ref: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    clauses = ["TRUE"]
    args: list[Any] = []
    for column, value in (
        ("cartridge_id", cartridge_id),
        ("readiness_status", readiness_status),
        ("deployment_mode", deployment_mode),
        ("manifest_digest", manifest_digest),
        ("source_ref", source_ref),
    ):
        if value:
            args.append(value)
            clauses.append(f"{column} = ${len(args)}")
    args.append(max(1, min(int(limit), 500)))
    rows = conn.fetch(
        f"""
        SELECT *
          FROM portable_cartridge_records
         WHERE {' AND '.join(clauses)}
         ORDER BY updated_at DESC, cartridge_record_id
         LIMIT ${len(args)}
        """,
        *args,
    )
    return _normalize_rows(rows, operation="list_portable_cartridge_records")


def load_portable_cartridge_record(
    conn: Any,
    *,
    cartridge_record_id: str,
    include_dependencies: bool = True,
    include_assets: bool = True,
    include_bindings: bool = True,
    include_verifiers: bool = True,
    include_drift_hooks: bool = True,
) -> dict[str, Any] | None:
    record = _normalize_optional_row(
        conn.fetchrow(
            "SELECT * FROM portable_cartridge_records WHERE cartridge_record_id = $1",
            cartridge_record_id,
        ),
        operation="load_portable_cartridge_record",
    )
    if record is None:
        return None
    if include_dependencies:
        record["object_truth_dependencies"] = _fetch_child_json(
            conn,
            table="portable_cartridge_object_truth_dependencies",
            json_column="dependency_json",
            cartridge_record_id=cartridge_record_id,
            order_by="dependency_class, dependency_id",
        )
    if include_assets:
        record["assets"] = _fetch_child_json(
            conn,
            table="portable_cartridge_assets",
            json_column="asset_json",
            cartridge_record_id=cartridge_record_id,
            order_by="asset_path",
        )
    if include_bindings:
        record["bindings"] = _fetch_child_json(
            conn,
            table="portable_cartridge_binding_contracts",
            json_column="binding_json",
            cartridge_record_id=cartridge_record_id,
            order_by="binding_id",
        )
    if include_verifiers:
        record["verifier_checks"] = _fetch_child_json(
            conn,
            table="portable_cartridge_verifier_checks",
            json_column="verifier_json",
            cartridge_record_id=cartridge_record_id,
            order_by="category, check_id",
        )
    if include_drift_hooks:
        record["drift_hooks"] = _fetch_child_json(
            conn,
            table="portable_cartridge_drift_hooks",
            json_column="hook_json",
            cartridge_record_id=cartridge_record_id,
            order_by="hook_point, hook_id",
        )
    return record


def list_portable_cartridge_dependencies(
    conn: Any,
    *,
    cartridge_record_id: str | None = None,
    dependency_id: str | None = None,
    dependency_class: str | None = None,
    authority_source: str | None = None,
    required: bool | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    return _list_child_rows(
        conn,
        table="portable_cartridge_object_truth_dependencies",
        filters=(
            ("cartridge_record_id", cartridge_record_id),
            ("dependency_id", dependency_id),
            ("dependency_class", dependency_class),
            ("authority_source", authority_source),
        ),
        bool_filters=(("required", required),),
        order_by="dependency_class, dependency_id",
        limit=limit,
        operation="list_portable_cartridge_dependencies",
    )


def list_portable_cartridge_assets(
    conn: Any,
    *,
    cartridge_record_id: str | None = None,
    role: str | None = None,
    required: bool | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    return _list_child_rows(
        conn,
        table="portable_cartridge_assets",
        filters=(("cartridge_record_id", cartridge_record_id), ("role", role)),
        bool_filters=(("required", required),),
        order_by="asset_path",
        limit=limit,
        operation="list_portable_cartridge_assets",
    )


def list_portable_cartridge_bindings(
    conn: Any,
    *,
    cartridge_record_id: str | None = None,
    binding_kind: str | None = None,
    required: bool | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    return _list_child_rows(
        conn,
        table="portable_cartridge_binding_contracts",
        filters=(("cartridge_record_id", cartridge_record_id), ("kind", binding_kind)),
        bool_filters=(("required", required),),
        order_by="binding_id",
        limit=limit,
        operation="list_portable_cartridge_bindings",
    )


def list_portable_cartridge_verifiers(
    conn: Any,
    *,
    cartridge_record_id: str | None = None,
    verifier_category: str | None = None,
    required: bool | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    return _list_child_rows(
        conn,
        table="portable_cartridge_verifier_checks",
        filters=(("cartridge_record_id", cartridge_record_id), ("category", verifier_category)),
        bool_filters=(("required", required),),
        order_by="category, check_id",
        limit=limit,
        operation="list_portable_cartridge_verifiers",
    )


def list_portable_cartridge_drift_hooks(
    conn: Any,
    *,
    cartridge_record_id: str | None = None,
    hook_point: str | None = None,
    required: bool | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    return _list_child_rows(
        conn,
        table="portable_cartridge_drift_hooks",
        filters=(("cartridge_record_id", cartridge_record_id), ("hook_point", hook_point)),
        bool_filters=(("required", required),),
        order_by="hook_point, hook_id",
        limit=limit,
        operation="list_portable_cartridge_drift_hooks",
    )


def _list_child_rows(
    conn: Any,
    *,
    table: str,
    filters: tuple[tuple[str, str | None], ...],
    bool_filters: tuple[tuple[str, bool | None], ...],
    order_by: str,
    limit: int,
    operation: str,
) -> list[dict[str, Any]]:
    clauses = ["TRUE"]
    args: list[Any] = []
    for column, value in filters:
        if value:
            args.append(value)
            clauses.append(f"{column} = ${len(args)}")
    for column, value in bool_filters:
        if value is not None:
            args.append(bool(value))
            clauses.append(f"{column} = ${len(args)}")
    args.append(max(1, min(int(limit), 500)))
    rows = conn.fetch(
        f"""
        SELECT *
          FROM {table}
         WHERE {' AND '.join(clauses)}
         ORDER BY {order_by}
         LIMIT ${len(args)}
        """,
        *args,
    )
    return _normalize_rows(rows, operation=operation)


def _fetch_child_json(
    conn: Any,
    *,
    table: str,
    json_column: str,
    cartridge_record_id: str,
    order_by: str,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        f"SELECT {json_column} FROM {table} WHERE cartridge_record_id = $1 ORDER BY {order_by}",
        cartridge_record_id,
    )
    return [row[json_column] for row in _normalize_rows(rows, operation=f"fetch_{table}")]


__all__ = [
    "persist_portable_cartridge_record",
    "list_portable_cartridge_records",
    "load_portable_cartridge_record",
    "list_portable_cartridge_dependencies",
    "list_portable_cartridge_assets",
    "list_portable_cartridge_bindings",
    "list_portable_cartridge_verifiers",
    "list_portable_cartridge_drift_hooks",
]
