"""Postgres persistence for Workflow Context authority."""

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


_PACK_JSON_COLUMNS = (
    "scenario_pack_refs_json",
    "materialized_from_json",
    "evidence_refs_json",
    "blockers_json",
    "verifier_expectations_json",
    "confidence_json",
    "guardrail_json",
    "review_packet_json",
    "synthetic_world_json",
    "metadata_json",
)
_ENTITY_JSON_COLUMNS = ("payload_json", "evidence_refs_json")
_BINDING_JSON_COLUMNS = ("evidence_refs_json", "confidence_json", "guardrail_json")
_TRANSITION_JSON_COLUMNS = ("evidence_refs_json", "guardrail_json")


def _normalize_json_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _normalize_row(row: Any, *, json_columns: tuple[str, ...]) -> dict[str, Any]:
    payload = dict(row or {})
    for key in json_columns:
        if key in payload:
            payload[key] = _normalize_json_value(payload.get(key))
    return payload


def _normalize_rows(rows: Any, *, json_columns: tuple[str, ...]) -> list[dict[str, Any]]:
    return [_normalize_row(row, json_columns=json_columns) for row in rows or []]


def _pack_row_to_domain(row: dict[str, Any]) -> dict[str, Any]:
    pack = dict(row)
    pack["scenario_pack_refs"] = pack.pop("scenario_pack_refs_json", [])
    pack["materialized_from"] = pack.pop("materialized_from_json", {})
    pack["evidence_refs"] = pack.pop("evidence_refs_json", [])
    pack["blockers"] = pack.pop("blockers_json", [])
    pack["verifier_expectations"] = pack.pop("verifier_expectations_json", [])
    pack["confidence"] = pack.pop("confidence_json", {})
    pack["guardrail"] = pack.pop("guardrail_json", {})
    pack["review_packet"] = pack.pop("review_packet_json", {})
    pack["synthetic_world"] = pack.pop("synthetic_world_json", None)
    pack["metadata"] = pack.pop("metadata_json", {})
    return pack


def _entity_row_to_domain(row: dict[str, Any]) -> dict[str, Any]:
    entity = dict(row)
    entity["payload"] = entity.pop("payload_json", {})
    entity["evidence_refs"] = entity.pop("evidence_refs_json", [])
    entity["context_pill"] = entity.get("truth_state")
    return entity


def _binding_row_to_domain(row: dict[str, Any]) -> dict[str, Any]:
    binding = dict(row)
    binding["evidence_refs"] = binding.pop("evidence_refs_json", [])
    binding["confidence"] = binding.pop("confidence_json", {})
    binding["guardrail"] = binding.pop("guardrail_json", {})
    return binding


def _transition_row_to_domain(row: dict[str, Any]) -> dict[str, Any]:
    transition = dict(row)
    transition["evidence_refs"] = transition.pop("evidence_refs_json", [])
    transition["guardrail"] = transition.pop("guardrail_json", {})
    return transition


def persist_context_pack(
    conn: Any,
    *,
    context_pack: dict[str, Any],
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    """Persist one Workflow Context pack plus its current entity projection."""

    pack = dict(_require_mapping(context_pack, field_name="context_pack"))
    context_ref = _require_text(pack.get("context_ref"), field_name="context_pack.context_ref")
    confidence = dict(_require_mapping(pack.get("confidence"), field_name="context_pack.confidence"))
    row = conn.fetchrow(
        """
        INSERT INTO workflow_context_packs (
            context_ref,
            workflow_ref,
            context_mode,
            truth_state,
            seed,
            intent,
            graph_ref,
            source_prompt_ref,
            confidence_score,
            confidence_state,
            unknown_mutator_risk,
            scenario_pack_refs_json,
            materialized_from_json,
            evidence_refs_json,
            blockers_json,
            verifier_expectations_json,
            confidence_json,
            guardrail_json,
            review_packet_json,
            synthetic_world_json,
            metadata_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
            $12::jsonb, $13::jsonb, $14::jsonb, $15::jsonb, $16::jsonb,
            $17::jsonb, $18::jsonb, $19::jsonb, $20::jsonb, $21::jsonb,
            $22, $23
        )
        ON CONFLICT (context_ref) DO UPDATE SET
            workflow_ref = EXCLUDED.workflow_ref,
            context_mode = EXCLUDED.context_mode,
            truth_state = EXCLUDED.truth_state,
            seed = EXCLUDED.seed,
            intent = EXCLUDED.intent,
            graph_ref = EXCLUDED.graph_ref,
            source_prompt_ref = EXCLUDED.source_prompt_ref,
            confidence_score = EXCLUDED.confidence_score,
            confidence_state = EXCLUDED.confidence_state,
            unknown_mutator_risk = EXCLUDED.unknown_mutator_risk,
            scenario_pack_refs_json = EXCLUDED.scenario_pack_refs_json,
            materialized_from_json = EXCLUDED.materialized_from_json,
            evidence_refs_json = EXCLUDED.evidence_refs_json,
            blockers_json = EXCLUDED.blockers_json,
            verifier_expectations_json = EXCLUDED.verifier_expectations_json,
            confidence_json = EXCLUDED.confidence_json,
            guardrail_json = EXCLUDED.guardrail_json,
            review_packet_json = EXCLUDED.review_packet_json,
            synthetic_world_json = EXCLUDED.synthetic_world_json,
            metadata_json = EXCLUDED.metadata_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING
            context_ref,
            workflow_ref,
            context_mode,
            truth_state,
            seed,
            intent,
            graph_ref,
            source_prompt_ref,
            confidence_score,
            confidence_state,
            unknown_mutator_risk,
            scenario_pack_refs_json,
            materialized_from_json,
            evidence_refs_json,
            blockers_json,
            verifier_expectations_json,
            confidence_json,
            guardrail_json,
            review_packet_json,
            synthetic_world_json,
            metadata_json,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
        """,
        context_ref,
        _optional_text(pack.get("workflow_ref"), field_name="workflow_ref"),
        _require_text(pack.get("context_mode"), field_name="context_mode"),
        _require_text(pack.get("truth_state"), field_name="truth_state"),
        _require_text(pack.get("seed"), field_name="seed"),
        _require_text(pack.get("intent"), field_name="intent"),
        _optional_text(pack.get("graph_ref"), field_name="graph_ref"),
        _optional_text(pack.get("source_prompt_ref"), field_name="source_prompt_ref"),
        float(pack.get("confidence_score") or confidence.get("score") or 0.0),
        _require_text(pack.get("confidence_state") or confidence.get("state"), field_name="confidence_state"),
        bool(pack.get("unknown_mutator_risk")),
        _encode_jsonb(pack.get("scenario_pack_refs") or [], field_name="scenario_pack_refs"),
        _encode_jsonb(pack.get("materialized_from") or {}, field_name="materialized_from"),
        _encode_jsonb(pack.get("evidence_refs") or [], field_name="evidence_refs"),
        _encode_jsonb(pack.get("blockers") or [], field_name="blockers"),
        _encode_jsonb(pack.get("verifier_expectations") or [], field_name="verifier_expectations"),
        _encode_jsonb(confidence, field_name="confidence"),
        _encode_jsonb(pack.get("guardrail") or {}, field_name="guardrail"),
        _encode_jsonb(pack.get("review_packet") or {}, field_name="review_packet"),
        _encode_jsonb(pack.get("synthetic_world"), field_name="synthetic_world"),
        _encode_jsonb(pack.get("metadata") or {}, field_name="metadata"),
        _optional_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_text(source_ref, field_name="source_ref"),
    )
    if row is None:
        raise PostgresWriteError(
            "workflow_context.pack_write_failed",
            "context pack insert returned no row",
        )

    conn.execute(
        "DELETE FROM workflow_context_entities WHERE context_ref = $1",
        context_ref,
    )
    for entity in pack.get("entities") or []:
        persist_context_entity(conn, entity=dict(entity), context_ref=context_ref)

    persisted = _pack_row_to_domain(_normalize_row(row, json_columns=_PACK_JSON_COLUMNS))
    persisted["entities"] = list_context_entities(conn, context_ref=context_ref)
    persisted["bindings"] = list_context_bindings(conn, context_ref=context_ref)
    return persisted


def persist_context_entity(conn: Any, *, entity: dict[str, Any], context_ref: str) -> dict[str, Any]:
    """Persist one current entity projection inside a context pack."""

    payload = dict(_require_mapping(entity, field_name="entity"))
    row = conn.fetchrow(
        """
        INSERT INTO workflow_context_entities (
            entity_ref,
            context_ref,
            entity_kind,
            label,
            truth_state,
            io_mode,
            payload_json,
            evidence_refs_json,
            confidence_score
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9
        )
        ON CONFLICT (entity_ref) DO UPDATE SET
            context_ref = EXCLUDED.context_ref,
            entity_kind = EXCLUDED.entity_kind,
            label = EXCLUDED.label,
            truth_state = EXCLUDED.truth_state,
            io_mode = EXCLUDED.io_mode,
            payload_json = EXCLUDED.payload_json,
            evidence_refs_json = EXCLUDED.evidence_refs_json,
            confidence_score = EXCLUDED.confidence_score,
            updated_at = now()
        RETURNING
            entity_ref,
            context_ref,
            entity_kind,
            label,
            truth_state,
            io_mode,
            payload_json,
            evidence_refs_json,
            confidence_score,
            created_at,
            updated_at
        """,
        _require_text(payload.get("entity_ref"), field_name="entity.entity_ref"),
        _require_text(context_ref, field_name="context_ref"),
        _require_text(payload.get("entity_kind"), field_name="entity.entity_kind"),
        _require_text(payload.get("label"), field_name="entity.label"),
        _require_text(payload.get("truth_state"), field_name="entity.truth_state"),
        _require_text(payload.get("io_mode"), field_name="entity.io_mode"),
        _encode_jsonb(payload.get("payload") or {}, field_name="entity.payload"),
        _encode_jsonb(payload.get("evidence_refs") or [], field_name="entity.evidence_refs"),
        float(payload.get("confidence_score") or 0.0),
    )
    if row is None:
        raise PostgresWriteError(
            "workflow_context.entity_write_failed",
            "context entity insert returned no row",
        )
    return _entity_row_to_domain(_normalize_row(row, json_columns=_ENTITY_JSON_COLUMNS))


def load_context_pack(
    conn: Any,
    *,
    context_ref: str,
    include_entities: bool = True,
    include_bindings: bool = True,
    include_transitions: bool = True,
) -> dict[str, Any] | None:
    """Load one Workflow Context pack by ref."""

    row = conn.fetchrow(
        """
        SELECT
            context_ref,
            workflow_ref,
            context_mode,
            truth_state,
            seed,
            intent,
            graph_ref,
            source_prompt_ref,
            confidence_score,
            confidence_state,
            unknown_mutator_risk,
            scenario_pack_refs_json,
            materialized_from_json,
            evidence_refs_json,
            blockers_json,
            verifier_expectations_json,
            confidence_json,
            guardrail_json,
            review_packet_json,
            synthetic_world_json,
            metadata_json,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
          FROM workflow_context_packs
         WHERE context_ref = $1
        """,
        _require_text(context_ref, field_name="context_ref"),
    )
    if row is None:
        return None
    pack = _pack_row_to_domain(_normalize_row(row, json_columns=_PACK_JSON_COLUMNS))
    if include_entities:
        pack["entities"] = list_context_entities(conn, context_ref=context_ref)
    if include_bindings:
        pack["bindings"] = list_context_bindings(conn, context_ref=context_ref)
    if include_transitions:
        pack["transitions"] = list_context_transitions(conn, context_ref=context_ref)
    return pack


def list_context_packs(
    conn: Any,
    *,
    workflow_ref: str | None = None,
    truth_state: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """List current context packs by workflow or truth-state filters."""

    rows = conn.fetch(
        """
        SELECT
            context_ref,
            workflow_ref,
            context_mode,
            truth_state,
            seed,
            intent,
            graph_ref,
            source_prompt_ref,
            confidence_score,
            confidence_state,
            unknown_mutator_risk,
            scenario_pack_refs_json,
            materialized_from_json,
            evidence_refs_json,
            blockers_json,
            verifier_expectations_json,
            confidence_json,
            guardrail_json,
            review_packet_json,
            synthetic_world_json,
            metadata_json,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
          FROM workflow_context_packs
         WHERE ($1::text IS NULL OR workflow_ref = $1)
           AND ($2::text IS NULL OR truth_state = $2)
         ORDER BY updated_at DESC, context_ref
         LIMIT $3
        """,
        _optional_text(workflow_ref, field_name="workflow_ref"),
        _optional_text(truth_state, field_name="truth_state"),
        int(limit),
    )
    return [_pack_row_to_domain(row) for row in _normalize_rows(rows, json_columns=_PACK_JSON_COLUMNS)]


def list_context_entities(
    conn: Any,
    *,
    context_ref: str,
    entity_kind: str | None = None,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT
            entity_ref,
            context_ref,
            entity_kind,
            label,
            truth_state,
            io_mode,
            payload_json,
            evidence_refs_json,
            confidence_score,
            created_at,
            updated_at
          FROM workflow_context_entities
         WHERE context_ref = $1
           AND ($2::text IS NULL OR entity_kind = $2)
         ORDER BY entity_kind, label, entity_ref
        """,
        _require_text(context_ref, field_name="context_ref"),
        _optional_text(entity_kind, field_name="entity_kind"),
    )
    return [_entity_row_to_domain(row) for row in _normalize_rows(rows, json_columns=_ENTITY_JSON_COLUMNS)]


def find_context_entity(
    conn: Any,
    *,
    context_ref: str,
    entity_ref: str,
) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        SELECT
            entity_ref,
            context_ref,
            entity_kind,
            label,
            truth_state,
            io_mode,
            payload_json,
            evidence_refs_json,
            confidence_score,
            created_at,
            updated_at
          FROM workflow_context_entities
         WHERE context_ref = $1
           AND entity_ref = $2
        """,
        _require_text(context_ref, field_name="context_ref"),
        _require_text(entity_ref, field_name="entity_ref"),
    )
    if row is None:
        return None
    return _entity_row_to_domain(_normalize_row(row, json_columns=_ENTITY_JSON_COLUMNS))


def persist_context_transition(
    conn: Any,
    *,
    context_pack: dict[str, Any],
    transition: dict[str, Any],
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    """Persist an updated context pack and append its transition receipt row."""

    persisted_pack = persist_context_pack(
        conn,
        context_pack=context_pack,
        observed_by_ref=observed_by_ref,
        source_ref=source_ref,
    )
    record = dict(_require_mapping(transition, field_name="transition"))
    row = conn.fetchrow(
        """
        INSERT INTO workflow_context_transitions (
            transition_ref,
            context_ref,
            from_truth_state,
            to_truth_state,
            transition_reason,
            decision_ref,
            risk_disposition,
            evidence_refs_json,
            guardrail_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10, $11
        )
        ON CONFLICT (transition_ref) DO UPDATE SET
            context_ref = EXCLUDED.context_ref,
            from_truth_state = EXCLUDED.from_truth_state,
            to_truth_state = EXCLUDED.to_truth_state,
            transition_reason = EXCLUDED.transition_reason,
            decision_ref = EXCLUDED.decision_ref,
            risk_disposition = EXCLUDED.risk_disposition,
            evidence_refs_json = EXCLUDED.evidence_refs_json,
            guardrail_json = EXCLUDED.guardrail_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref
        RETURNING
            transition_ref,
            context_ref,
            from_truth_state,
            to_truth_state,
            transition_reason,
            decision_ref,
            risk_disposition,
            evidence_refs_json,
            guardrail_json,
            observed_by_ref,
            source_ref,
            created_at
        """,
        _require_text(record.get("transition_ref"), field_name="transition.transition_ref"),
        _require_text(record.get("context_ref"), field_name="transition.context_ref"),
        _require_text(record.get("from_truth_state"), field_name="transition.from_truth_state"),
        _require_text(record.get("to_truth_state"), field_name="transition.to_truth_state"),
        _require_text(record.get("transition_reason"), field_name="transition.transition_reason"),
        _optional_text(record.get("decision_ref"), field_name="decision_ref"),
        _optional_text(record.get("risk_disposition"), field_name="risk_disposition"),
        _encode_jsonb(record.get("evidence_refs") or [], field_name="transition.evidence_refs"),
        _encode_jsonb(record.get("guardrail") or {}, field_name="transition.guardrail"),
        _optional_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_text(source_ref, field_name="source_ref"),
    )
    if row is None:
        raise PostgresWriteError(
            "workflow_context.transition_write_failed",
            "context transition insert returned no row",
        )
    return {
        "context_pack": persisted_pack,
        "transition": _transition_row_to_domain(_normalize_row(row, json_columns=_TRANSITION_JSON_COLUMNS)),
    }


def persist_context_binding(
    conn: Any,
    *,
    binding: dict[str, Any],
    context_pack: dict[str, Any],
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    """Persist one context binding and refresh the pack projection."""

    record = dict(_require_mapping(binding, field_name="binding"))
    context_ref = _require_text(record.get("context_ref"), field_name="binding.context_ref")
    row = conn.fetchrow(
        """
        INSERT INTO workflow_context_bindings (
            binding_ref,
            context_ref,
            entity_ref,
            target_authority_domain,
            target_ref,
            binding_state,
            risk_level,
            requires_review,
            reversible,
            reviewed_by_ref,
            confidence_score,
            evidence_refs_json,
            confidence_json,
            guardrail_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12::jsonb, $13::jsonb, $14::jsonb, $15, $16
        )
        ON CONFLICT (binding_ref) DO UPDATE SET
            context_ref = EXCLUDED.context_ref,
            entity_ref = EXCLUDED.entity_ref,
            target_authority_domain = EXCLUDED.target_authority_domain,
            target_ref = EXCLUDED.target_ref,
            binding_state = EXCLUDED.binding_state,
            risk_level = EXCLUDED.risk_level,
            requires_review = EXCLUDED.requires_review,
            reversible = EXCLUDED.reversible,
            reviewed_by_ref = EXCLUDED.reviewed_by_ref,
            confidence_score = EXCLUDED.confidence_score,
            evidence_refs_json = EXCLUDED.evidence_refs_json,
            confidence_json = EXCLUDED.confidence_json,
            guardrail_json = EXCLUDED.guardrail_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING
            binding_ref,
            context_ref,
            entity_ref,
            target_authority_domain,
            target_ref,
            binding_state,
            risk_level,
            requires_review,
            reversible,
            reviewed_by_ref,
            confidence_score,
            evidence_refs_json,
            confidence_json,
            guardrail_json,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
        """,
        _require_text(record.get("binding_ref"), field_name="binding.binding_ref"),
        context_ref,
        _require_text(record.get("entity_ref"), field_name="binding.entity_ref"),
        _require_text(record.get("target_authority_domain"), field_name="binding.target_authority_domain"),
        _require_text(record.get("target_ref"), field_name="binding.target_ref"),
        _require_text(record.get("binding_state"), field_name="binding.binding_state"),
        _require_text(record.get("risk_level"), field_name="binding.risk_level"),
        bool(record.get("requires_review")),
        bool(record.get("reversible")),
        _optional_text(record.get("reviewed_by_ref"), field_name="reviewed_by_ref"),
        float(record.get("confidence_score") or 0.0),
        _encode_jsonb(record.get("evidence_refs") or [], field_name="binding.evidence_refs"),
        _encode_jsonb(record.get("confidence") or {}, field_name="binding.confidence"),
        _encode_jsonb(record.get("guardrail") or {}, field_name="binding.guardrail"),
        _optional_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_text(source_ref, field_name="source_ref"),
    )
    if row is None:
        raise PostgresWriteError(
            "workflow_context.binding_write_failed",
            "context binding insert returned no row",
        )
    persisted_pack = persist_context_pack(
        conn,
        context_pack=context_pack,
        observed_by_ref=observed_by_ref,
        source_ref=source_ref,
    )
    persisted_binding = _binding_row_to_domain(_normalize_row(row, json_columns=_BINDING_JSON_COLUMNS))
    return {
        "context_pack": persisted_pack,
        "binding": persisted_binding,
    }


def list_context_bindings(
    conn: Any,
    *,
    context_ref: str,
    entity_ref: str | None = None,
    binding_state: str | None = None,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT
            binding_ref,
            context_ref,
            entity_ref,
            target_authority_domain,
            target_ref,
            binding_state,
            risk_level,
            requires_review,
            reversible,
            reviewed_by_ref,
            confidence_score,
            evidence_refs_json,
            confidence_json,
            guardrail_json,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
          FROM workflow_context_bindings
         WHERE context_ref = $1
           AND ($2::text IS NULL OR entity_ref = $2)
           AND ($3::text IS NULL OR binding_state = $3)
         ORDER BY updated_at DESC, binding_ref
        """,
        _require_text(context_ref, field_name="context_ref"),
        _optional_text(entity_ref, field_name="entity_ref"),
        _optional_text(binding_state, field_name="binding_state"),
    )
    return [_binding_row_to_domain(row) for row in _normalize_rows(rows, json_columns=_BINDING_JSON_COLUMNS)]


def list_context_transitions(
    conn: Any,
    *,
    context_ref: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT
            transition_ref,
            context_ref,
            from_truth_state,
            to_truth_state,
            transition_reason,
            decision_ref,
            risk_disposition,
            evidence_refs_json,
            guardrail_json,
            observed_by_ref,
            source_ref,
            created_at
          FROM workflow_context_transitions
         WHERE context_ref = $1
         ORDER BY created_at DESC, transition_ref
         LIMIT $2
        """,
        _require_text(context_ref, field_name="context_ref"),
        int(limit),
    )
    return [_transition_row_to_domain(row) for row in _normalize_rows(rows, json_columns=_TRANSITION_JSON_COLUMNS)]


__all__ = [
    "find_context_entity",
    "list_context_bindings",
    "list_context_entities",
    "list_context_packs",
    "list_context_transitions",
    "load_context_pack",
    "persist_context_binding",
    "persist_context_entity",
    "persist_context_pack",
    "persist_context_transition",
]
