"""Layer 1 (Bind): extract, suggest, and validate data-pill refs from intent.

This is the first layer of the planning stack. It takes a prose intent plus
the data dictionary authority and returns:

  - suggested pills: likely ``object.field`` candidates from loose prose,
    scored against the data dictionary and shared intent synonym lexicon.
  - bound pills: explicit ``object.field`` references that resolve to real
    rows in the data dictionary (with type + source provenance).
  - ambiguous candidates: references that matched multiple objects or fields.
  - unbound candidates: references that looked like data pills but did not
    match anything; these are the typos and hallucinated fields the caller
    needs to fix before decomposing intent into steps.

HONEST SCOPE:

  - Deterministic only. Exact binding still only accepts explicit
    ``object_kind.field_path`` patterns in the prose. Loose prose produces
    suggestions, not authority mutations. The caller must confirm candidates
    before they become hard bindings.
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
from runtime.intent_lexicon import expand_query_terms, normalize_match_text


# ``object_kind.field_path`` (and optional trailing ``.snake_case`` nesting for
# field paths). Object kinds may include namespace separators such as
# ``tool:praxis_connector``. Must start with a letter to avoid matching version
# strings like ``1.0.2``.
_REF_PATTERN = re.compile(
    r"\b([a-z][a-z0-9_]*(?::[a-z][a-z0-9_]*)*)\.([a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*)\b",
    flags=re.IGNORECASE,
)


# File extensions whose `name.ext` shape collides with the data-pill ref
# pattern. Filtered out at extract time so prose like "edit catalog.py" or
# "see system_events.py" doesn't auto-bind as object_kind.field_path. If the
# caller genuinely means a data field, they can use a different name.
_FILE_EXT_BLOCKLIST: frozenset[str] = frozenset(
    {
        # Python / config
        "py", "pyi", "pyc", "pyx", "pyd",
        "ini", "cfg", "toml", "yaml", "yml", "json", "json5",
        "env", "lock",
        # Web / app
        "js", "jsx", "ts", "tsx", "mjs", "cjs",
        "html", "htm", "css", "scss", "sass", "less",
        "vue", "svelte",
        # Docs / data
        "md", "mdx", "rst", "txt", "csv", "tsv", "log",
        "xml", "proto", "graphql", "gql",
        # Build / shell
        "sh", "bash", "zsh", "fish", "ps1", "bat",
        "make", "mk", "cmake", "dockerfile",
        # Other languages / compiled
        "go", "rs", "java", "kt", "swift",
        "c", "cc", "cpp", "h", "hpp",
        "rb", "php", "pl", "lua", "sql",
        # Binary / artifacts
        "png", "jpg", "jpeg", "gif", "svg", "ico", "webp",
        "pdf", "zip", "tar", "gz", "bz2", "tgz",
        "wasm", "bin", "out",
    }
)

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_STOPWORDS: frozenset[str] = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "by",
        "for",
        "from",
        "in",
        "into",
        "it",
        "its",
        "of",
        "on",
        "or",
        "our",
        "that",
        "the",
        "then",
        "this",
        "to",
        "up",
        "we",
        "where",
        "with",
    }
)

_SUGGESTION_OBJECT_SCAN_LIMIT = 60


def _looks_like_filename(matched_span: str, field_path: str, intent: str, span_start: int) -> bool:
    """Return True if the match is a filename or path segment, not a data ref.

    Two checks: (1) the field_path is a single known file extension, or
    (2) the matched span is adjacent to a path separator in the surrounding
    text. Either signal means this is a path mention, not a column reference.
    """
    if "." not in field_path:
        leaf = field_path
    else:
        # Multi-part path like ``foo.tar.gz``: check the last segment.
        leaf = field_path.rsplit(".", 1)[-1]
    if leaf in _FILE_EXT_BLOCKLIST:
        return True
    # Path-separator context: ``a/b/foo.py`` or ``/foo/bar.json`` — even if
    # the extension isn't blocklisted, a leading or trailing slash means
    # this is filesystem prose.
    span_end = span_start + len(matched_span)
    char_before = intent[span_start - 1] if span_start > 0 else ""
    char_after = intent[span_end] if span_end < len(intent) else ""
    if char_before in {"/", "\\"} or char_after in {"/", "\\"}:
        return True
    return False


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
class SuggestedPillCandidate:
    """A likely data-pill candidate inferred from loose prose.

    Suggestions are advisory. They come from deterministic dictionary search
    and synonym expansion; they do not bind authority. The caller chooses one
    by writing/confirming the explicit ``object.field`` reference.
    """

    object_kind: str
    field_path: str
    field_kind: str | None
    label: str | None
    description: str | None
    source: str
    display_order: int | None
    score: float
    confidence: str
    matched_terms: list[str]
    reason: str

    @property
    def ref(self) -> str:
        return f"{self.object_kind}.{self.field_path}"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["ref"] = self.ref
        return payload


@dataclass(frozen=True)
class BoundIntent:
    """Output of ``bind_data_pills``.

    The caller (user or LLM) uses this to confirm their intent references
    real fields before they decompose the intent into workflow packets.
    """

    intent: str
    suggested: list[SuggestedPillCandidate] = field(default_factory=list)
    bound: list[BoundPill] = field(default_factory=list)
    ambiguous: list[AmbiguousCandidate] = field(default_factory=list)
    unbound: list[UnboundCandidate] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "intent": self.intent,
            "suggested": [pill.to_dict() for pill in self.suggested],
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
        if _looks_like_filename(match.group(0), field_path, intent, match.start()):
            continue
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


def _stringify_for_match(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, dict):
        return " ".join(_stringify_for_match(item) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return " ".join(_stringify_for_match(item) for item in value)
    return ""


def _terms_for_match(text: str) -> set[str]:
    normalized = normalize_match_text(text)
    terms = set(expand_query_terms(normalized))
    terms.update(_TOKEN_RE.findall(normalized))
    return {term for term in terms if len(term) > 2 and term not in _STOPWORDS}


def _row_match_text(row: dict[str, Any], keys: tuple[str, ...]) -> str:
    return " ".join(_stringify_for_match(row.get(key)) for key in keys)


def _phrase_bonus(intent_normalized: str, *phrases: str | None) -> float:
    bonus = 0.0
    padded_intent = f" {intent_normalized} "
    for phrase in phrases:
        normalized = normalize_match_text(phrase or "")
        if normalized and f" {normalized} " in padded_intent:
            bonus += 3.0
    return bonus


def _confidence_for_score(score: float) -> str:
    if score >= 8.0:
        return "high"
    if score >= 4.0:
        return "medium"
    return "low"


def _score_suggestion(
    *,
    intent_normalized: str,
    intent_terms: set[str],
    object_row: dict[str, Any],
    field_row: dict[str, Any],
) -> tuple[float, list[str]]:
    object_text = _row_match_text(
        object_row,
        ("object_kind", "label", "category", "summary", "description", "metadata"),
    )
    field_text = _row_match_text(
        field_row,
        (
            "field_path",
            "field_kind",
            "label",
            "description",
            "examples",
            "valid_values",
            "metadata",
        ),
    )
    object_terms = _terms_for_match(object_text)
    field_terms = _terms_for_match(field_text)
    object_overlap = intent_terms & object_terms
    field_overlap = intent_terms & field_terms

    field_path = str(field_row.get("field_path") or "")
    object_kind = str(object_row.get("object_kind") or "")
    field_path_terms = _terms_for_match(field_path.replace(".", " ").replace("_", " "))
    direct_field_overlap = intent_terms & field_path_terms

    score = 0.0
    score += len(field_overlap) * 2.0
    score += len(direct_field_overlap) * 1.5
    score += len(object_overlap) * 0.75
    score += _phrase_bonus(
        intent_normalized,
        object_kind,
        str(object_row.get("label") or ""),
        field_path.replace("_", " "),
        str(field_row.get("label") or ""),
    )

    matched_terms = sorted(field_overlap | object_overlap | direct_field_overlap)
    return score, matched_terms


def _suggest_data_pills(
    intent_text: str,
    *,
    conn: Any,
    allowlist: set[str] | None,
    excluded_refs: set[str],
    limit: int,
) -> list[SuggestedPillCandidate]:
    """Return likely data-pill candidates from loose prose.

    This is deliberately deterministic: dictionary rows are the authority,
    the shared lexicon expands synonyms, and scores are transparent.
    """
    if limit <= 0:
        return []

    intent_normalized = normalize_match_text(intent_text)
    intent_terms = _terms_for_match(intent_text)
    if not intent_terms:
        return []

    object_rows = [dict(row) for row in list_object_kinds(conn)]
    if allowlist is not None:
        object_rows = [
            row for row in object_rows if str(row.get("object_kind") or "").lower() in allowlist
        ]

    scored_objects: list[tuple[float, dict[str, Any]]] = []
    for object_row in object_rows:
        object_text = _row_match_text(
            object_row,
            ("object_kind", "label", "category", "summary", "description", "metadata"),
        )
        object_terms = _terms_for_match(object_text)
        overlap = intent_terms & object_terms
        object_kind = str(object_row.get("object_kind") or "")
        label = str(object_row.get("label") or "")
        score = len(overlap) + _phrase_bonus(intent_normalized, object_kind, label)
        scored_objects.append((score, object_row))

    if allowlist is None:
        selected_objects = [
            row
            for score, row in sorted(scored_objects, key=lambda item: item[0], reverse=True)
            if score > 0
        ][:_SUGGESTION_OBJECT_SCAN_LIMIT]
        if not selected_objects:
            selected_objects = [
                row
                for _score, row in sorted(scored_objects, key=lambda item: item[0], reverse=True)
            ][:_SUGGESTION_OBJECT_SCAN_LIMIT]
    else:
        selected_objects = [row for _score, row in scored_objects]

    suggestions_by_ref: dict[str, SuggestedPillCandidate] = {}
    for object_row in selected_objects:
        object_kind = str(object_row.get("object_kind") or "").lower()
        if not object_kind:
            continue
        fields = _lookup_object_fields(conn, object_kind)
        if not fields:
            continue
        for field_row in fields:
            field_path = str(field_row.get("field_path") or "").lower()
            if not field_path:
                continue
            ref = f"{object_kind}.{field_path}"
            if ref in excluded_refs:
                continue
            score, matched_terms = _score_suggestion(
                intent_normalized=intent_normalized,
                intent_terms=intent_terms,
                object_row=object_row,
                field_row=field_row,
            )
            if score < 2.0:
                continue
            candidate = SuggestedPillCandidate(
                object_kind=object_kind,
                field_path=field_path,
                field_kind=(str(field_row["field_kind"]) if field_row.get("field_kind") else None),
                label=(str(field_row["label"]) if field_row.get("label") else None),
                description=(
                    str(field_row["description"]) if field_row.get("description") else None
                ),
                source=str(field_row.get("source") or "auto"),
                display_order=(
                    int(field_row["display_order"])
                    if field_row.get("display_order") is not None
                    else None
                ),
                score=round(score, 3),
                confidence=_confidence_for_score(score),
                matched_terms=matched_terms,
                reason=(
                    "matched loose prose against data dictionary object/field text "
                    "using shared synonym expansion"
                ),
            )
            existing = suggestions_by_ref.get(ref)
            if existing is None or candidate.score > existing.score:
                suggestions_by_ref[ref] = candidate

    return sorted(
        suggestions_by_ref.values(),
        key=lambda item: (-item.score, item.object_kind, item.field_path),
    )[:limit]


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
    suggest: bool = True,
    suggestion_limit: int = 20,
) -> BoundIntent:
    """Suggest loose data pills and validate explicit ``object.field`` refs.

    Args:
        intent: prose describing what the caller wants done.
        conn: live Postgres connection for the data dictionary authority.
        object_kinds: optional allowlist. If provided, references to object
            kinds outside this list bind as unbound with
            ``reason='object_kind_not_allowlisted'`` — useful when the
            caller wants to scope binding to a specific workspace.
        suggest: when true, scan dictionary rows and return likely
            ``suggested`` candidates from loose prose. Suggestions are advisory.
        suggestion_limit: maximum number of suggested candidates to return.

    Returns:
        ``BoundIntent`` with bound / ambiguous / unbound splits.
    """
    intent_text = intent.strip()
    if not intent_text:
        return BoundIntent(intent="", warnings=["intent is empty"])

    allowlist: set[str] | None = None
    if object_kinds is not None:
        allowlist = {str(kind).strip().lower() for kind in object_kinds if str(kind).strip()}

    candidates = _extract_candidate_refs(intent_text)
    explicit_refs = {f"{object_kind}.{field_path}" for _, object_kind, field_path in candidates}
    warnings: list[str] = []
    suggestions: list[SuggestedPillCandidate] = []
    if suggest:
        try:
            suggestions = _suggest_data_pills(
                intent_text,
                conn=conn,
                allowlist=allowlist,
                excluded_refs=explicit_refs,
                limit=max(int(suggestion_limit), 0),
            )
        except Exception as exc:
            warnings.append(f"data-pill suggestions unavailable: {type(exc).__name__}: {exc}")

    if not candidates:
        return BoundIntent(
            intent=intent_text,
            suggested=suggestions,
            warnings=warnings
            + [
                "no object.field references found in intent; "
                "write explicit refs (e.g. 'users.first_name') to bind pills"
            ],
        )

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
        suggested=suggestions,
        bound=bound,
        ambiguous=ambiguous,
        unbound=unbound,
        warnings=warnings,
    )


__all__ = [
    "AmbiguousCandidate",
    "BoundIntent",
    "BoundPill",
    "ProposedPillScaffold",
    "SuggestedPillCandidate",
    "UnboundCandidate",
    "bind_data_pills",
    "commit_proposed_pill",
]
