from __future__ import annotations

from collections.abc import Mapping
from typing import Any
import re

from memory.federated_retrieval import FederatedRetriever
from surfaces.api import operator_write as operator_control

_DECISION_ENTITY_TYPES = {"", "decision", "architecture_policy", "operator_decision"}
_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_:-]*")


class RecallAuthorityError(RuntimeError):
    """Raised when a recall authority path is unavailable or malformed."""


def _readable_name(*, name: object, source: object, content: object) -> str:
    normalized_name = str(name or "").strip()
    if normalized_name.startswith("doc:"):
        normalized_source = str(source or "").strip()
        if normalized_source.startswith("catalog/"):
            parts = normalized_source.replace("catalog/", "").replace("_", " ").title()
            return f"{parts} Catalog"
        normalized_content = str(content or "").strip()
        if normalized_content:
            first_line = normalized_content.splitlines()[0].strip().lstrip("# ")
            if first_line:
                return first_line[:80]
    return normalized_name


def _readable_type(*, entity_type: object, metadata: object) -> str:
    if isinstance(metadata, Mapping):
        kind = metadata.get("kind") or metadata.get("object_kind")
        if isinstance(kind, str) and kind.strip():
            return kind.strip()
    return str(entity_type or "").strip()


def _resolved_env(subsystems: Any) -> Mapping[str, str] | None:
    env = getattr(subsystems, "_postgres_env", None)
    return env() if callable(env) else None


def _normalize_tokens(query: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys(match.group(0) for match in _TOKEN_RE.finditer(query.lower())))


def _score_text(text: str, *, query_lower: str, tokens: tuple[str, ...], weight: float) -> float:
    if not text:
        return 0.0
    normalized = text.lower()
    score = 0.0
    if query_lower and query_lower in normalized:
        score += 2.0 * weight
    if tokens:
        token_hits = sum(1 for token in tokens if token in normalized)
        if token_hits:
            score += weight * (token_hits / len(tokens))
    return score


def _score_operator_decision(
    record: Mapping[str, Any],
    *,
    query_lower: str,
    tokens: tuple[str, ...],
) -> float:
    weighted_fields = (
        (str(record.get("title") or ""), 5.0),
        (str(record.get("decision_key") or ""), 4.0),
        (str(record.get("rationale") or ""), 3.0),
        (str(record.get("decision_scope_ref") or ""), 3.0),
        (str(record.get("decision_scope_kind") or ""), 2.0),
        (str(record.get("decision_kind") or ""), 2.0),
        (str(record.get("decision_source") or ""), 1.0),
    )
    max_score = sum(weight * 3.0 for _text, weight in weighted_fields)
    raw_score = sum(
        _score_text(text, query_lower=query_lower, tokens=tokens, weight=weight)
        for text, weight in weighted_fields
    )
    if raw_score <= 0:
        return 0.0
    return min(raw_score / max_score, 0.99)


def _normalize_knowledge_result(result: Any) -> dict[str, Any]:
    entity = result.entity
    content = str(entity.content or "").strip()
    return {
        "entity_id": entity.id,
        "name": _readable_name(name=entity.name, source=entity.source, content=entity.content),
        "type": _readable_type(entity_type=entity.entity_type.value, metadata=entity.metadata),
        "score": round(float(result.score), 4),
        "content": content,
        "source": entity.source,
        "found_via": result.found_via,
        "provenance": result.provenance,
    }


def _search_operator_decisions(
    subsystems: Any,
    *,
    query: str,
    entity_type: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    normalized_type = str(entity_type or "").strip().lower()
    if normalized_type not in _DECISION_ENTITY_TYPES:
        return []

    try:
        results = operator_control.list_operator_decisions(
            active_only=False,
            limit=min(max(limit * 10, 100), 500),
            env=_resolved_env(subsystems),
        )
    except Exception as exc:
        raise RecallAuthorityError(
            f"operator decision recall failed: {type(exc).__name__}: {exc}"
        ) from exc
    rows = results.get("operator_decisions")
    if rows is None:
        rows = results.get("results")
    if not isinstance(rows, list):
        raise RecallAuthorityError("operator decision recall returned a non-list payload")

    query_lower = query.strip().lower()
    tokens = _normalize_tokens(query)
    scored_rows: list[tuple[float, dict[str, Any]]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        score = _score_operator_decision(row, query_lower=query_lower, tokens=tokens)
        if score <= 0:
            continue
        scored_rows.append(
            (
                score,
                {
                    "entity_id": str(row.get("operator_decision_id") or "").strip(),
                    "name": str(row.get("title") or row.get("decision_key") or "").strip(),
                    "type": "decision",
                    "score": round(score, 4),
                    "content": str(row.get("rationale") or "").strip(),
                    "source": "operator_decisions",
                    "found_via": "authority_scan",
                    "provenance": {
                        "table": "operator_decisions",
                        "decision_kind": row.get("decision_kind"),
                        "decision_scope_kind": row.get("decision_scope_kind"),
                        "decision_scope_ref": row.get("decision_scope_ref"),
                    },
                },
            )
        )

    scored_rows.sort(
        key=lambda item: (
            -item[0],
            str(item[1].get("name") or ""),
            str(item[1].get("entity_id") or ""),
        )
    )
    return [row for _score, row in scored_rows[:limit]]


def _search_federated_memory_results(
    subsystems: Any,
    *,
    query: str,
    entity_type: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    """Search the memory graph through the federated retriever first."""
    getter = getattr(subsystems, "get_memory_engine", None)
    if not callable(getter):
        return []
    try:
        engine = getter()
    except Exception as exc:
        raise RecallAuthorityError(
            f"federated memory engine unavailable: {type(exc).__name__}: {exc}"
        ) from exc

    try:
        retriever = FederatedRetriever(engine)
        results = retriever.search(query, limit=limit)
    except Exception as exc:
        raise RecallAuthorityError(
            f"federated memory search failed: {type(exc).__name__}: {exc}"
        ) from exc

    normalized_type = str(entity_type or "").strip()
    normalized: list[dict[str, Any]] = []
    for result in results:
        try:
            normalized_result = _normalize_knowledge_result(result)
            if normalized_type and normalized_result.get("type") != normalized_type:
                continue
            normalized.append(normalized_result)
        except Exception:
            continue
    return normalized


def search_recall_results(
    subsystems: Any,
    *,
    query: str,
    entity_type: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    normalized_limit = max(1, int(limit or 20))
    merged: dict[str, dict[str, Any]] = {}

    for result in _search_federated_memory_results(
        subsystems,
        query=query,
        entity_type=entity_type,
        limit=normalized_limit,
    ):
        merged[result["entity_id"]] = result

    kg = subsystems.get_knowledge_graph()
    for result in kg.search(query, entity_type=entity_type, limit=normalized_limit):
        normalized = _normalize_knowledge_result(result)
        merged[normalized["entity_id"]] = normalized

    operator_decisions = _search_operator_decisions(
        subsystems,
        query=query,
        entity_type=entity_type,
        limit=normalized_limit,
    )

    for result in operator_decisions:
        existing = merged.get(result["entity_id"])
        if existing is None or float(result["score"]) > float(existing.get("score") or 0):
            merged[result["entity_id"]] = result

    ranked = sorted(
        merged.values(),
        key=lambda item: (
            -float(item.get("score") or 0.0),
            str(item.get("name") or ""),
            str(item.get("entity_id") or ""),
        ),
    )
    return ranked[:normalized_limit]


__all__ = ["RecallAuthorityError", "search_recall_results"]
