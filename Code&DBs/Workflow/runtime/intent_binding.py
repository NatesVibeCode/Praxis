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

from runtime.data_dictionary import describe_object, list_object_kinds, set_operator_override


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
class ProposedPillScaffold:
    """Scaffold for a field the caller intends to create.

    Populated on :class:`UnboundCandidate` when the object_kind exists in
    authority but the field_path does not — i.e. the reference could be a
    genuine new field, not just a typo. The caller fills in ``field_kind``
    and ``description`` and passes the scaffold to :func:`commit_proposed_pill`
    to actually write the row through the data dictionary authority.

    The scaffold itself is read-only. No authority mutation happens until
    commit is called with explicit intent.
    """

    object_kind: str
    field_path: str
    field_kind_hint: str | None  # heuristic guess based on name ('datetime', 'number', etc.)
    required_to_fill: list[str]  # fields the caller must supply at commit time
    rationale: str  # why this scaffold was proposed

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class UnboundCandidate:
    """A reference that looked like a data pill but did not resolve.

    ``reason`` tells the caller why: the object_kind wasn't known, the
    object exists but the field_path isn't in it, or the object_kind fell
    outside an allowlist.

    When the object_kind exists and the field_path just isn't there yet,
    ``proposed_pill`` holds a scaffold the caller can fill + commit to
    create the field. That distinguishes "typo" (caller fixes intent) from
    "new field I'm about to add" (caller commits scaffold). For unknown
    object_kinds and allowlist rejections, ``proposed_pill`` stays None —
    we don't offer to create fields on objects the authority doesn't know.
    """

    matched_span: str
    object_kind: str | None
    field_path: str | None
    reason: str
    proposed_pill: ProposedPillScaffold | None = None

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


# Deterministic field_kind hints from the field name. Confident patterns only —
# anything not matched falls through to None so the caller fills explicitly.
# The allowed kinds come from runtime.data_dictionary._ALLOWED_FIELD_KINDS:
# text, number, boolean, enum, json, date, datetime, reference, array, object.
def _infer_field_kind_hint(field_path: str) -> str | None:
    path = field_path.lower().strip()
    if not path:
        return None
    leaf = path.rsplit(".", 1)[-1]
    if leaf.startswith(("is_", "has_", "should_", "can_")) or leaf.endswith("_flag"):
        return "boolean"
    if leaf.endswith(("_at", "_time", "_timestamp")):
        return "datetime"
    if leaf.endswith("_on") or leaf.endswith("_date"):
        return "date"
    if leaf.endswith(("_count", "_total", "_size", "_index", "_seq", "_n")):
        return "number"
    if leaf.endswith(("_json", "_payload", "_metadata")):
        return "json"
    if leaf.endswith(("_ids", "_refs", "_list", "_items", "_tags")):
        return "array"
    if leaf.endswith(("_id", "_ref", "_kind")):
        return "reference"
    return None


def _scaffold_for_missing_field(object_kind: str, field_path: str) -> ProposedPillScaffold:
    hint = _infer_field_kind_hint(field_path)
    required = ["description"]
    if hint is None:
        required.append("field_kind")
    return ProposedPillScaffold(
        object_kind=object_kind,
        field_path=field_path,
        field_kind_hint=hint,
        required_to_fill=required,
        rationale=(
            f"object_kind {object_kind!r} is known to authority but field_path "
            f"{field_path!r} is not — caller may commit this scaffold to create "
            "the field instead of treating the reference as a typo"
        ),
    )


def commit_proposed_pill(
    scaffold: ProposedPillScaffold,
    *,
    conn: Any,
    description: str,
    field_kind: str | None = None,
    label: str | None = None,
    display_order: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Write a proposed pill to the data dictionary authority.

    Explicit action — never called from binding. Caller is expected to
    fill every entry in ``scaffold.required_to_fill`` before invoking.
    Wraps :func:`runtime.data_dictionary.set_operator_override` so the
    row is inserted at the operator layer (winning over projected rows).

    Returns the dictionary authority's receipt: the upserted entry row.
    """
    if not isinstance(description, str) or not description.strip():
        raise ValueError("description is required to commit a proposed pill")
    chosen_kind = (field_kind or scaffold.field_kind_hint or "").strip()
    if not chosen_kind:
        raise ValueError(
            "field_kind is required: no hint was inferred from the field name, "
            "caller must supply one of text/number/boolean/enum/json/date/"
            "datetime/reference/array/object"
        )
    return set_operator_override(
        conn,
        object_kind=scaffold.object_kind,
        field_path=scaffold.field_path,
        field_kind=chosen_kind,
        label=label,
        description=description.strip(),
        display_order=display_order,
        metadata=metadata,
    )


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
                    proposed_pill=_scaffold_for_missing_field(object_kind, field_path),
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
    "ProposedPillScaffold",
    "UnboundCandidate",
    "bind_data_pills",
    "commit_proposed_pill",
]
