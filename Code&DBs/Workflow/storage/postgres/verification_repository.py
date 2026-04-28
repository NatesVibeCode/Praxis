"""Explicit sync Postgres repository for verification and authority persistence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from .validators import (
    _encode_jsonb,
    _optional_text,
    _require_mapping,
    _require_nonnegative_int,
    _require_positive_int,
    _require_text,
    _require_utc,
    PostgresWriteError,
)


def _require_bool(value: object, *, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be a boolean",
            details={"field": field_name},
        )
    return value


def _require_text_sequence(
    value: object,
    *,
    field_name: str,
) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be a sequence of non-empty strings",
            details={"field": field_name},
        )

    normalized: list[str] = []
    for index, item in enumerate(value):
        normalized.append(_require_text(item, field_name=f"{field_name}[{index}]"))
    return tuple(normalized)


def _require_utc_or_iso8601(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return _require_utc(value, field_name=field_name)
    text = _require_text(value, field_name=field_name)
    parsed_text = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(parsed_text)
    except ValueError as exc:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be a UTC ISO-8601 datetime",
            details={"field": field_name},
        ) from exc
    return _require_utc(parsed, field_name=field_name)


def _normalize_verify_ref_rows(
    verify_refs: object,
) -> tuple[tuple[str, str, str, str, str, bool, str, str], ...]:
    if not isinstance(verify_refs, Sequence) or isinstance(
        verify_refs,
        (str, bytes, bytearray),
    ):
        raise PostgresWriteError(
            "postgres.invalid_submission",
            "verify_refs must be a sequence of mappings",
            details={"field": "verify_refs"},
        )

    normalized_rows: list[tuple[str, str, str, str, str, bool, str, str]] = []
    for index, row in enumerate(verify_refs):
        payload = _require_mapping(row, field_name=f"verify_refs[{index}]")
        normalized_rows.append(
            (
                _require_text(payload.get("verify_ref"), field_name=f"verify_refs[{index}].verify_ref"),
                _require_text(
                    payload.get("verification_ref"),
                    field_name=f"verify_refs[{index}].verification_ref",
                ),
                _require_text(payload.get("label"), field_name=f"verify_refs[{index}].label"),
                str(payload.get("description", "")),
                _encode_jsonb(
                    dict(
                        _require_mapping(
                            payload.get("inputs") or {},
                            field_name=f"verify_refs[{index}].inputs",
                        ),
                    ),
                    field_name=f"verify_refs[{index}].inputs",
                ),
                _require_bool(payload.get("enabled", True), field_name=f"verify_refs[{index}].enabled"),
                _require_text(
                    payload.get("binding_revision"),
                    field_name=f"verify_refs[{index}].binding_revision",
                ),
                _require_text(
                    payload.get("decision_ref"),
                    field_name=f"verify_refs[{index}].decision_ref",
                ),
            )
        )
    return tuple(normalized_rows)


def _normalize_verification_registry_rows(
    verification_registry: object,
) -> tuple[tuple[str, str, str, str, str, str, int, str, bool, str], ...]:
    if not isinstance(verification_registry, Sequence) or isinstance(
        verification_registry,
        (str, bytes, bytearray),
    ):
        raise PostgresWriteError(
            "postgres.invalid_submission",
            "verification_registry must be a sequence of mappings",
            details={"field": "verification_registry"},
        )

    normalized_rows: list[tuple[str, str, str, str, str, str, int, str, bool, str]] = []
    for index, row in enumerate(verification_registry):
        payload = _require_mapping(row, field_name=f"verification_registry[{index}]")
        executor_kind = _require_text(
            payload.get("executor_kind"),
            field_name=f"verification_registry[{index}].executor_kind",
        )
        if executor_kind != "argv":
            raise PostgresWriteError(
                "postgres.invalid_submission",
                f"verification_registry[{index}].executor_kind must be argv",
                details={"field": f"verification_registry[{index}].executor_kind"},
            )
        normalized_rows.append(
            (
                _require_text(
                    payload.get("verification_ref"),
                    field_name=f"verification_registry[{index}].verification_ref",
                ),
                _require_text(
                    payload.get("display_name"),
                    field_name=f"verification_registry[{index}].display_name",
                ),
                str(payload.get("description", "")),
                executor_kind,
                _encode_jsonb(
                    list(
                        _require_text_sequence(
                            payload.get("argv_template"),
                            field_name=f"verification_registry[{index}].argv_template",
                        )
                    ),
                    field_name=f"verification_registry[{index}].argv_template",
                ),
                _encode_jsonb(
                    list(
                        _require_text_sequence(
                            payload.get("template_inputs") or [],
                            field_name=f"verification_registry[{index}].template_inputs",
                        )
                    ),
                    field_name=f"verification_registry[{index}].template_inputs",
                ),
                _require_positive_int(
                    payload.get("default_timeout_seconds", 60),
                    field_name=f"verification_registry[{index}].default_timeout_seconds",
                ),
                _require_text(
                    payload.get("workdir_policy") or "job",
                    field_name=f"verification_registry[{index}].workdir_policy",
                ),
                _require_bool(
                    payload.get("enabled", True),
                    field_name=f"verification_registry[{index}].enabled",
                ),
                _require_text(
                    payload.get("decision_ref"),
                    field_name=f"verification_registry[{index}].decision_ref",
                ),
            )
        )
    return tuple(normalized_rows)


class PostgresVerificationRepository:
    """Owns canonical verification and capability-tracking persistence."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def record_capability_outcome(
        self,
        *,
        run_id: str,
        provider_slug: str,
        model_slug: str,
        inferred_capabilities: Sequence[str],
        succeeded: bool,
        output_quality_signals: Mapping[str, object],
        recorded_at: datetime | str,
    ) -> str:
        normalized_run_id = _require_text(run_id, field_name="run_id")
        self._conn.execute(
            """
            INSERT INTO capability_outcomes
            (
                run_id,
                provider_slug,
                model_slug,
                inferred_capabilities,
                succeeded,
                output_quality_signals,
                recorded_at
            )
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
            """,
            normalized_run_id,
            _require_text(provider_slug, field_name="provider_slug"),
            _require_text(model_slug, field_name="model_slug"),
            list(_require_text_sequence(inferred_capabilities, field_name="inferred_capabilities")),
            _require_bool(succeeded, field_name="succeeded"),
            _encode_jsonb(
                dict(_require_mapping(output_quality_signals, field_name="output_quality_signals")),
                field_name="output_quality_signals",
            ),
            _require_utc_or_iso8601(recorded_at, field_name="recorded_at"),
        )
        return normalized_run_id

    def list_capability_outcomes(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT run_id,
                   provider_slug,
                   model_slug,
                   inferred_capabilities,
                   succeeded,
                   output_quality_signals,
                   recorded_at
              FROM capability_outcomes
             ORDER BY recorded_at DESC
            """,
        )
        return [dict(row) for row in rows or []]

    def load_verify_ref(self, *, verify_ref: str) -> dict[str, Any] | None:
        rows = self._conn.execute(
            """
            SELECT verify_ref,
                   verification_ref,
                   label,
                   description,
                   inputs,
                   enabled,
                   binding_revision,
                   decision_ref
              FROM verify_refs
             WHERE verify_ref = $1
            """,
            _require_text(verify_ref, field_name="verify_ref"),
        )
        return dict(rows[0]) if rows else None

    def list_verification_registry_rows(
        self,
        *,
        verification_refs: Sequence[str],
    ) -> list[dict[str, Any]]:
        normalized_refs = _require_text_sequence(
            verification_refs,
            field_name="verification_refs",
        )
        if not normalized_refs:
            return []
        rows = self._conn.execute(
            """
            SELECT verification_ref,
                   display_name,
                   executor_kind,
                   argv_template,
                   template_inputs,
                   default_timeout_seconds,
                   enabled
              FROM verification_registry
             WHERE verification_ref = ANY($1::text[])
            """,
            list(normalized_refs),
        )
        return [dict(row) for row in rows or []]

    def upsert_verify_refs(
        self,
        *,
        verify_refs: Sequence[Mapping[str, object]],
    ) -> int:
        normalized_rows = _normalize_verify_ref_rows(verify_refs)
        if not normalized_rows:
            return 0
        self._conn.execute_many(
            """
            INSERT INTO verify_refs (
                verify_ref,
                verification_ref,
                label,
                description,
                inputs,
                enabled,
                binding_revision,
                decision_ref
            ) VALUES (
                $1, $2, $3, $4, $5::jsonb, $6, $7, $8
            )
            ON CONFLICT (verify_ref) DO UPDATE SET
                verification_ref = EXCLUDED.verification_ref,
                label = EXCLUDED.label,
                description = EXCLUDED.description,
                inputs = EXCLUDED.inputs,
                enabled = EXCLUDED.enabled,
                binding_revision = EXCLUDED.binding_revision,
                decision_ref = EXCLUDED.decision_ref,
                updated_at = now()
            """,
            list(normalized_rows),
        )
        return len(normalized_rows)

    def upsert_verification_registry(
        self,
        *,
        verification_registry: Sequence[Mapping[str, object]],
    ) -> int:
        normalized_rows = _normalize_verification_registry_rows(verification_registry)
        if not normalized_rows:
            return 0
        self._conn.execute_many(
            """
            INSERT INTO verification_registry (
                verification_ref,
                display_name,
                description,
                executor_kind,
                argv_template,
                template_inputs,
                default_timeout_seconds,
                workdir_policy,
                enabled,
                decision_ref
            ) VALUES (
                $1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8, $9, $10
            )
            ON CONFLICT (verification_ref) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                description = EXCLUDED.description,
                executor_kind = EXCLUDED.executor_kind,
                argv_template = EXCLUDED.argv_template,
                template_inputs = EXCLUDED.template_inputs,
                default_timeout_seconds = EXCLUDED.default_timeout_seconds,
                workdir_policy = EXCLUDED.workdir_policy,
                enabled = EXCLUDED.enabled,
                decision_ref = EXCLUDED.decision_ref,
                updated_at = now()
            """,
            list(normalized_rows),
        )
        return len(normalized_rows)

    def list_registered_verifiers(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT verifier_ref,
                   display_name,
                   description,
                   verifier_kind,
                   verification_ref,
                   builtin_ref,
                   default_inputs,
                   enabled,
                   decision_ref
              FROM verifier_registry
             ORDER BY verifier_ref ASC
            """,
        )
        return [dict(row) for row in rows or []]

    def list_registered_healers(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT healer_ref,
                   display_name,
                   description,
                   executor_kind,
                   action_ref,
                   auto_mode,
                   safety_mode,
                   enabled,
                   decision_ref
              FROM healer_registry
             ORDER BY healer_ref ASC
            """,
        )
        return [dict(row) for row in rows or []]

    def list_verifier_healer_bindings(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT binding_ref,
                   verifier_ref,
                   healer_ref,
                   enabled,
                   binding_revision,
                   decision_ref
              FROM verifier_healer_bindings
             ORDER BY verifier_ref ASC, healer_ref ASC
            """,
        )
        return [dict(row) for row in rows or []]

    def load_verifier(self, *, verifier_ref: str) -> dict[str, Any] | None:
        rows = self._conn.execute(
            """
            SELECT verifier_ref,
                   display_name,
                   description,
                   verifier_kind,
                   verification_ref,
                   builtin_ref,
                   default_inputs,
                   enabled,
                   decision_ref
              FROM verifier_registry
             WHERE verifier_ref = $1
            """,
            _require_text(verifier_ref, field_name="verifier_ref"),
        )
        return dict(rows[0]) if rows else None

    def load_healer(self, *, healer_ref: str) -> dict[str, Any] | None:
        rows = self._conn.execute(
            """
            SELECT healer_ref,
                   display_name,
                   description,
                   executor_kind,
                   action_ref,
                   auto_mode,
                   safety_mode,
                   enabled,
                   decision_ref
              FROM healer_registry
             WHERE healer_ref = $1
            """,
            _require_text(healer_ref, field_name="healer_ref"),
        )
        return dict(rows[0]) if rows else None

    def list_bound_healer_refs(
        self,
        *,
        verifier_ref: str,
        limit: int | None = None,
    ) -> tuple[str, ...]:
        normalized_verifier_ref = _require_text(
            verifier_ref,
            field_name="verifier_ref",
        )
        if limit is None:
            rows = self._conn.execute(
                """
                SELECT healer_ref
                  FROM verifier_healer_bindings
                 WHERE verifier_ref = $1
                   AND enabled = TRUE
                 ORDER BY healer_ref ASC
                """,
                normalized_verifier_ref,
            )
        else:
            rows = self._conn.execute(
                """
                SELECT healer_ref
                  FROM verifier_healer_bindings
                 WHERE verifier_ref = $1
                   AND enabled = TRUE
                 ORDER BY healer_ref ASC
                 LIMIT $2
                """,
                normalized_verifier_ref,
                _require_nonnegative_int(limit, field_name="limit"),
            )
        return tuple(str(row.get("healer_ref") or "").strip() for row in rows or [])

    def record_verification_run(
        self,
        *,
        verification_run_id: str,
        verifier_ref: str,
        target_kind: str,
        target_ref: str,
        status: str,
        inputs: Mapping[str, object],
        outputs: Mapping[str, object],
        suggested_healer_ref: str | None,
        healing_candidate: bool,
        decision_ref: str,
        duration_ms: int,
    ) -> str:
        normalized_verification_run_id = _require_text(
            verification_run_id,
            field_name="verification_run_id",
        )
        self._conn.execute(
            """
            INSERT INTO verification_runs (
                verification_run_id,
                verifier_ref,
                target_kind,
                target_ref,
                status,
                inputs,
                outputs,
                suggested_healer_ref,
                healing_candidate,
                decision_ref,
                duration_ms
            ) VALUES (
                $1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10, $11
            )
            """,
            normalized_verification_run_id,
            _require_text(verifier_ref, field_name="verifier_ref"),
            _require_text(target_kind, field_name="target_kind"),
            _require_text(target_ref, field_name="target_ref"),
            _require_text(status, field_name="status"),
            _encode_jsonb(dict(_require_mapping(inputs, field_name="inputs")), field_name="inputs"),
            _encode_jsonb(dict(_require_mapping(outputs, field_name="outputs")), field_name="outputs"),
            _optional_text(suggested_healer_ref, field_name="suggested_healer_ref"),
            _require_bool(healing_candidate, field_name="healing_candidate"),
            _require_text(decision_ref, field_name="decision_ref"),
            _require_nonnegative_int(duration_ms, field_name="duration_ms"),
        )
        return normalized_verification_run_id

    def record_healing_run(
        self,
        *,
        healing_run_id: str,
        healer_ref: str,
        verifier_ref: str,
        target_kind: str,
        target_ref: str,
        status: str,
        inputs: Mapping[str, object],
        outputs: Mapping[str, object],
        decision_ref: str,
        duration_ms: int,
    ) -> str:
        normalized_healing_run_id = _require_text(
            healing_run_id,
            field_name="healing_run_id",
        )
        self._conn.execute(
            """
            INSERT INTO healing_runs (
                healing_run_id,
                healer_ref,
                verifier_ref,
                target_kind,
                target_ref,
                status,
                inputs,
                outputs,
                decision_ref,
                duration_ms
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10
            )
            """,
            normalized_healing_run_id,
            _require_text(healer_ref, field_name="healer_ref"),
            _require_text(verifier_ref, field_name="verifier_ref"),
            _require_text(target_kind, field_name="target_kind"),
            _require_text(target_ref, field_name="target_ref"),
            _require_text(status, field_name="status"),
            _encode_jsonb(dict(_require_mapping(inputs, field_name="inputs")), field_name="inputs"),
            _encode_jsonb(dict(_require_mapping(outputs, field_name="outputs")), field_name="outputs"),
            _require_text(decision_ref, field_name="decision_ref"),
            _require_nonnegative_int(duration_ms, field_name="duration_ms"),
        )
        return normalized_healing_run_id


__all__ = ["PostgresVerificationRepository"]
