"""Layer 1 (Bind): extract and validate data-pill references from intent prose.

This is the first layer of the planning stack. It takes a prose intent plus
the data dictionary authority and returns:

  - bound pills: explicit ``object.field`` references that resolve to real
    rows in the data dictionary (with type + source provenance).
  - ambiguous candidates: references that matched multiple objects or fields.
  - unbound candidates: references that looked like data pills but did not
    match anything; these are the typos and hallucinated fields the caller
    needs to fix before decomposing intent into steps.

HONEST SCOPE:

  - Deterministic only. Looks for ``snake_case.field_path`` patterns in the
    prose. No NLP, no LLM, no "user's first name" → ``users.first_name``
    inference. If the caller wants loose-prose binding, wrap this with an
    LLM extractor that produces explicit refs before calling.
  - Validation, not authorship. This does not write prose, pick steps,
    choose models, or build prompts. Upstream layers 2 (decompose), 3
    (re-order), 4 (author) still live with the caller.
  - Access-agnostic. The data dictionary answers "does this field exist
    and what is it?" — not "may this caller read/write it?" A separate
    capability/authority check is required for access enforcement.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any

from runtime.data_dictionary import describe_object, list_object_kinds


# ``snake_case.snake_case`` (and optional trailing ``.snake_case`` nesting for
# ``object.parent.child`` paths). Must start with a letter to avoid matching
# version strings like ``1.0.2``.
_REF_PATTERN = re.compile(
    r"\b([a-z][a-z0-9_]*)\.([a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*)\b",
    flags=re.IGNORECASE,
)


@dataclass(frozen=True)
class BoundPill:
    """One data-pill reference that resolved to a real dictionary row."""

    matched_span: str  # exact text from the intent that matched
    object_kind: str  # e.g. "workflow_runs", "users"
    field_path: str  # e.g. "status", "first_name"
    field_kind: str | None  # type from dictionary ("text", "integer", "timestamp", ...)
    source: str  # "auto" | "operator_override" | other projection source
    display_order: int | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AmbiguousCandidate:
    """A reference that matched more than one object or field."""

    matched_span: str
    candidates: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class UnboundCandidate:
    """A reference that looked like a data pill but did not resolve.

    ``reason`` tells the caller why: the object_kind wasn't known, or the
    object exists but the field_path isn't in it. Caller fixes typos or
    drops hallucinated references before composing packets.
    """

    matched_span: str
    object_kind: str | None
    field_path: str | None
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BoundIntent:
    """Output of ``bind_data_pills``.

    The caller (user or LLM) uses this to confirm their intent references
    real fields before they decompose the intent into workflow packets.
    """

    intent: str
    bound: list[BoundPill] = field(default_factory=list)
    ambiguous: list[AmbiguousCandidate] = field(default_factory=list)
    unbound: list[UnboundCandidate] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "intent": self.intent,
            "bound": [pill.to_dict() for pill in self.bound],
            "ambiguous": [pill.to_dict() for pill in self.ambiguous],
            "unbound": [pill.to_dict() for pill in self.unbound],
            "warnings": list(self.warnings),
        }


def _extract_candidate_refs(intent: str) -> list[tuple[str, str, str]]:
    """Extract (matched_span, object_kind, field_path) tuples from the intent.

    Deduplicates so the same ``object.field`` written twice only binds once.
    """
    seen: set[tuple[str, str]] = set()
    candidates: list[tuple[str, str, str]] = []
    for match in _REF_PATTERN.finditer(intent):
        object_kind = match.group(1).lower()
        field_path = match.group(2).lower()
        key = (object_kind, field_path)
        if key in seen:
            continue
        seen.add(key)
        candidates.append((match.group(0), object_kind, field_path))
    return candidates


def _lookup_object_fields(conn: Any, object_kind: str) -> list[dict[str, Any]] | None:
    """Return the field list for an object kind, or None if unknown."""
    try:
        description = describe_object(conn, object_kind=object_kind)
    except Exception:
        return None
    fields = description.get("fields") or []
    return [dict(row) for row in fields]


def bind_data_pills(
    intent: str,
    *,
    conn: Any,
    object_kinds: list[str] | None = None,
) -> BoundIntent:
    """Extract and validate ``object.field`` data pills from prose intent.

    Args:
        intent: prose describing what the caller wants done.
        conn: live Postgres connection for the data dictionary authority.
        object_kinds: optional allowlist. If provided, references to object
            kinds outside this list bind as unbound with
            ``reason='object_kind_not_allowlisted'`` — useful when the
            caller wants to scope binding to a specific workspace.

    Returns:
        ``BoundIntent`` with bound / ambiguous / unbound splits.
    """
    intent_text = intent.strip()
    if not intent_text:
        return BoundIntent(intent="", warnings=["intent is empty"])

    candidates = _extract_candidate_refs(intent_text)
    if not candidates:
        return BoundIntent(
            intent=intent_text,
            warnings=[
                "no object.field references found in intent; "
                "write explicit refs (e.g. 'users.first_name') to bind pills"
            ],
        )

    allowlist: set[str] | None = None
    if object_kinds is not None:
        allowlist = {str(kind).strip().lower() for kind in object_kinds if str(kind).strip()}

    bound: list[BoundPill] = []
    ambiguous: list[AmbiguousCandidate] = []
    unbound: list[UnboundCandidate] = []
    object_cache: dict[str, list[dict[str, Any]] | None] = {}

    for matched_span, object_kind, field_path in candidates:
        if allowlist is not None and object_kind not in allowlist:
            unbound.append(
                UnboundCandidate(
                    matched_span=matched_span,
                    object_kind=object_kind,
                    field_path=field_path,
                    reason="object_kind_not_allowlisted",
                )
            )
            continue

        if object_kind not in object_cache:
            object_cache[object_kind] = _lookup_object_fields(conn, object_kind)
        fields = object_cache[object_kind]

        if fields is None:
            unbound.append(
                UnboundCandidate(
                    matched_span=matched_span,
                    object_kind=object_kind,
                    field_path=field_path,
                    reason="object_kind_not_found",
                )
            )
            continue

        matching_rows = [
            row for row in fields if str(row.get("field_path") or "").lower() == field_path
        ]
        if not matching_rows:
            unbound.append(
                UnboundCandidate(
                    matched_span=matched_span,
                    object_kind=object_kind,
                    field_path=field_path,
                    reason="field_path_not_in_object",
                )
            )
            continue

        if len(matching_rows) > 1:
            ambiguous.append(
                AmbiguousCandidate(
                    matched_span=matched_span,
                    candidates=[
                        {
                            "object_kind": object_kind,
                            "field_path": str(row.get("field_path") or ""),
                            "field_kind": row.get("field_kind"),
                            "source": row.get("source"),
                            "display_order": row.get("display_order"),
                        }
                        for row in matching_rows
                    ],
                )
            )
            continue

        row = matching_rows[0]
        bound.append(
            BoundPill(
                matched_span=matched_span,
                object_kind=object_kind,
                field_path=str(row.get("field_path") or field_path),
                field_kind=(str(row["field_kind"]) if row.get("field_kind") else None),
                source=str(row.get("source") or "auto"),
                display_order=(
                    int(row["display_order"]) if row.get("display_order") is not None else None
                ),
            )
        )

    return BoundIntent(
        intent=intent_text,
        bound=bound,
        ambiguous=ambiguous,
        unbound=unbound,
    )


__all__ = [
    "AmbiguousCandidate",
    "BoundIntent",
    "BoundPill",
    "UnboundCandidate",
    "bind_data_pills",
]
