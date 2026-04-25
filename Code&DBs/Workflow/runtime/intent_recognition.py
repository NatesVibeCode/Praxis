"""Recognize user-stated intent spans and match them to Praxis authority.

This layer is deliberately not a planner. It does not reorder user intent,
invent executable steps, or promote suggestions into confirmed workflow
packets. Its job is the useful boring work:

  - extract spans the user actually wrote
  - match those spans against data dictionary / tool authority
  - surface ambiguity and gaps
  - suggest missing prerequisites only when matched authority says they exist

Downstream compile/compose surfaces can turn confirmed matches into workflow
contracts, but this module keeps recognition separate from execution.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any

from runtime.data_dictionary import describe_object, list_object_kinds
from runtime.intent_binding import bind_data_pills
from runtime.intent_lexicon import expand_query_terms, normalize_match_text


_TOKEN_RE = re.compile(r"[a-z0-9]+")
_STOPWORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "as",
        "for",
        "from",
        "in",
        "into",
        "it",
        "of",
        "on",
        "or",
        "that",
        "the",
        "then",
        "to",
        "we",
        "where",
        "with",
    }
)

_SPAN_PATTERNS: tuple[tuple[str, str, str], ...] = (
    ("input", "app_name", r"\bapp\s+name\b|\bapplication\s+name\b"),
    ("input", "app_domain", r"\bapp\s+domain\b|\bapplication\s+domain\b"),
    ("input", "domain", r"\bdomain\b"),
    ("operation", "custom_integration", r"\bcustom\s+integration\b"),
    ("operation", "integration", r"\bintegration\b|\bconnector\b"),
    ("operation", "research", r"\bresearch\b|\binvestigate\b"),
    ("operation", "plan", r"\bplan(?:ning)?\b|\bscope\b"),
    ("operation", "search", r"\bsearch\b|\bdiscover\b|\bfind\b|\blook\s+up\b"),
    ("operation", "retrieve", r"\bretrieve\b|\bfetch\b|\bcollect\b|\bgather\b"),
    ("operation", "evaluate", r"\bevaluate\b|\bassess\b|\breview\b"),
    ("operation", "build", r"\battempt\s+to\s+build\b|\bbuild\b|\bimplement\b|\bcreate\b"),
    ("operation", "verify", r"\bverify\b|\btest\b|\bsmoke\b"),
    ("operation", "register", r"\bregister\b|\bpromote\b"),
    ("operation", "fan_out", r"\bfan\s*out\b|\bparallel\b|\bmulti[-\s]?angle\b"),
)

_OBJECT_SCAN_LIMIT = 160


@dataclass(frozen=True)
class RecognizedSpan:
    text: str
    start: int
    end: int
    source_order: int
    kind: str
    normalized: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AuthorityMatch:
    span_text: str
    source_order: int
    authority_ref: str
    authority_kind: str
    confidence: str
    score: float
    reason: str
    object_kind: str | None = None
    field_path: str | None = None
    matched_terms: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SuggestedStep:
    title: str
    status: str
    source_authority_ref: str
    reason: str
    confidence: str
    implied_by_span: str | None = None
    prerequisite_for: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RecognitionGap:
    span_text: str
    source_order: int
    kind: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class IntentRecognition:
    intent: str
    extracted: list[RecognizedSpan]
    matched: list[AuthorityMatch]
    suggested: list[SuggestedStep]
    gaps: list[RecognitionGap]
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "intent": self.intent,
            "extracted": [span.to_dict() for span in self.extracted],
            "matched": [match.to_dict() for match in self.matched],
            "suggested": [step.to_dict() for step in self.suggested],
            "gaps": [gap.to_dict() for gap in self.gaps],
            "warnings": list(self.warnings),
        }


def _terms(text: str) -> set[str]:
    normalized = normalize_match_text(text)
    terms = set(expand_query_terms(normalized))
    terms.update(_TOKEN_RE.findall(normalized))
    return {term for term in terms if len(term) > 2 and term not in _STOPWORDS}


def _literal_terms(text: str) -> set[str]:
    normalized = normalize_match_text(text)
    terms = set(_TOKEN_RE.findall(normalized))
    return {term for term in terms if len(term) > 2 and term not in _STOPWORDS}


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, dict):
        return " ".join(_stringify(item) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return " ".join(_stringify(item) for item in value)
    return ""


def _extract_spans(intent: str) -> list[RecognizedSpan]:
    candidates: list[tuple[int, int, str, str, str]] = []
    for kind, key, pattern in _SPAN_PATTERNS:
        for match in re.finditer(pattern, intent, flags=re.IGNORECASE):
            text = match.group(0).strip()
            if text:
                candidates.append((match.start(), match.end(), kind, key, text))

    # Prefer longer spans when they overlap, e.g. "custom integration" over
    # the contained "integration".
    candidates.sort(key=lambda item: (item[0], -(item[1] - item[0]), item[3]))
    accepted: list[tuple[int, int, str, str, str]] = []
    occupied: list[tuple[int, int]] = []
    for start, end, kind, key, text in candidates:
        if any(start < used_end and end > used_start for used_start, used_end in occupied):
            continue
        accepted.append((start, end, kind, key, text))
        occupied.append((start, end))

    accepted.sort(key=lambda item: (item[0], item[1]))
    return [
        RecognizedSpan(
            text=text,
            start=start,
            end=end,
            source_order=index + 1,
            kind=kind,
            normalized=key,
        )
        for index, (start, end, kind, key, text) in enumerate(accepted)
    ]


def _score_text(span: RecognizedSpan, authority_text: str) -> tuple[float, list[str]]:
    span_literal = _literal_terms(f"{span.text} {span.normalized}")
    authority_literal = _literal_terms(authority_text)
    literal_overlap = sorted(span_literal & authority_literal)
    span_terms = _terms(f"{span.text} {span.normalized}")
    authority_terms = _terms(authority_text)
    expanded_overlap = sorted((span_terms & authority_terms) - set(literal_overlap))
    overlap = sorted(set(literal_overlap) | set(expanded_overlap))
    score = float(len(literal_overlap) * 3)
    score += float(len(expanded_overlap) * 1.5)
    authority_normalized = normalize_match_text(authority_text)
    span_normalized = normalize_match_text(span.text)
    if span_normalized and f" {span_normalized} " in f" {authority_normalized} ":
        score += 4.0
    if span.normalized.replace("_", " ") in authority_normalized:
        score += 3.0
    return score, overlap


def _confidence(score: float) -> str:
    if score >= 10.0:
        return "high"
    if score >= 5.0:
        return "medium"
    return "low"


def _best_span_for_terms(
    spans: list[RecognizedSpan],
    candidate_terms: list[str],
    *,
    fallback_text: str,
) -> RecognizedSpan | None:
    if not spans:
        return None
    terms = set(candidate_terms)
    best: tuple[int, RecognizedSpan] | None = None
    for span in spans:
        overlap = len(_terms(f"{span.text} {span.normalized}") & terms)
        if overlap <= 0:
            continue
        if best is None or overlap > best[0]:
            best = (overlap, span)
    if best:
        return best[1]
    fallback_normalized = normalize_match_text(fallback_text)
    for span in spans:
        if normalize_match_text(span.text) and normalize_match_text(span.text) in fallback_normalized:
            return span
    return None


def _data_pill_matches(
    intent: str,
    *,
    conn: Any,
    spans: list[RecognizedSpan],
    limit: int,
) -> tuple[list[AuthorityMatch], list[str]]:
    warnings: list[str] = []
    matches: list[AuthorityMatch] = []
    try:
        binding = bind_data_pills(intent, conn=conn, suggestion_limit=limit)
    except Exception as exc:
        return [], [f"data-pill recognition unavailable: {type(exc).__name__}: {exc}"]

    payload = binding.to_dict()
    for entry in payload.get("bound") or []:
        span_text = str(entry.get("matched_span") or "")
        span = next((item for item in spans if item.text.lower() == span_text.lower()), None)
        matches.append(
            AuthorityMatch(
                span_text=span_text,
                source_order=span.source_order if span else 0,
                authority_ref=f"{entry.get('object_kind')}.{entry.get('field_path')}",
                authority_kind="data_pill.bound",
                confidence="high",
                score=100.0,
                reason="explicit object_kind.field_path reference resolved in data dictionary",
                object_kind=str(entry.get("object_kind") or ""),
                field_path=str(entry.get("field_path") or ""),
                matched_terms=[],
                metadata={"source": entry.get("source"), "field_kind": entry.get("field_kind")},
            )
        )

    for entry in payload.get("suggested") or []:
        matched_terms = [str(term) for term in entry.get("matched_terms") or []]
        choice_terms = sorted(
            _literal_terms(
                " ".join(
                    [
                        str(entry.get("field_path") or "").replace("_", " "),
                        str(entry.get("label") or ""),
                    ]
                )
            )
        )
        span = _best_span_for_terms(
            spans,
            choice_terms,
            fallback_text=f"{entry.get('field_path')} {entry.get('description')}",
        )
        matches.append(
            AuthorityMatch(
                span_text=span.text if span else intent,
                source_order=span.source_order if span else 0,
                authority_ref=str(entry.get("ref") or ""),
                authority_kind="data_pill.suggested",
                confidence=str(entry.get("confidence") or "low"),
                score=float(entry.get("score") or 0.0),
                reason=str(entry.get("reason") or "matched loose prose to data dictionary"),
                object_kind=str(entry.get("object_kind") or ""),
                field_path=str(entry.get("field_path") or ""),
                matched_terms=matched_terms,
                metadata={
                    "field_kind": entry.get("field_kind"),
                    "description": entry.get("description"),
                    "source": entry.get("source"),
                },
            )
        )

    warnings.extend(str(warn) for warn in payload.get("warnings") or [])
    return matches, warnings


def _object_matches(
    *,
    conn: Any,
    spans: list[RecognizedSpan],
    limit_per_span: int,
) -> tuple[list[AuthorityMatch], list[str]]:
    try:
        objects = [dict(row) for row in list_object_kinds(conn)]
    except Exception as exc:
        return [], [f"authority object recognition unavailable: {type(exc).__name__}: {exc}"]

    matches: list[AuthorityMatch] = []
    authority_objects = [
        row
        for row in objects
        if str(row.get("category") or "") in {"tool", "workflow", "capability", "operation"}
    ][:_OBJECT_SCAN_LIMIT]

    for span in spans:
        if span.kind == "input":
            continue
        scored: list[AuthorityMatch] = []
        for row in authority_objects:
            category = str(row.get("category") or "")
            object_kind = str(row.get("object_kind") or "")
            if not object_kind:
                continue
            authority_text = " ".join(
                _stringify(row.get(key))
                for key in ("object_kind", "label", "category", "summary", "metadata")
            )
            score, terms = _score_text(span, authority_text)
            if score < 6.0:
                continue
            scored.append(
                AuthorityMatch(
                    span_text=span.text,
                    source_order=span.source_order,
                    authority_ref=object_kind,
                    authority_kind=category or "authority_object",
                    confidence=_confidence(score),
                    score=round(score, 3),
                    reason="matched extracted user span to data dictionary authority object",
                    object_kind=object_kind,
                    matched_terms=terms,
                    metadata={
                        "label": row.get("label"),
                        "summary": row.get("summary"),
                    },
                )
            )
        scored.sort(key=lambda item: (-item.score, item.authority_ref))
        matches.extend(scored[:limit_per_span])
    return matches, []


def _parse_pipeline_suggestions(summary: str) -> list[str]:
    text = summary or ""
    paren_match = re.search(
        r"\bpipeline\s*\(([^()]*(?:→|->)[^()]*)\)",
        text,
        flags=re.IGNORECASE,
    )
    source = paren_match.group(1) if paren_match else text
    if not paren_match or ("→" not in source and "->" not in source):
        return []
    parts = re.split(r"\s*(?:→|->)\s*", source)
    cleaned = [part.strip(" .;:,") for part in parts if part.strip(" .;:,")]
    return cleaned if len(cleaned) >= 2 else []


def _suggest_from_authority(
    *,
    conn: Any,
    intent: str,
    spans: list[RecognizedSpan],
    matches: list[AuthorityMatch],
) -> tuple[list[SuggestedStep], list[str]]:
    warnings: list[str] = []
    suggestions: dict[tuple[str, str], SuggestedStep] = {}
    stated_terms = _literal_terms(" ".join(span.text for span in spans))

    matched_objects = sorted(
        {
            match.object_kind or match.authority_ref
            for match in matches
            if (match.object_kind or match.authority_ref)
            and (match.object_kind or match.authority_ref).startswith("tool:")
        }
    )
    for object_kind in matched_objects[:12]:
        try:
            description = describe_object(conn, object_kind=object_kind)
        except Exception as exc:
            warnings.append(
                f"authority suggestion unavailable for {object_kind}: {type(exc).__name__}: {exc}"
            )
            continue
        object_row = dict(description.get("object") or {})
        summary = str(object_row.get("summary") or "")
        for item in _parse_pipeline_suggestions(summary):
            item_terms = _literal_terms(item)
            status = "extracted" if item_terms & stated_terms else "suggested"
            if status == "extracted":
                continue
            key = (object_kind, item)
            suggestions[key] = SuggestedStep(
                title=item,
                status="suggested",
                source_authority_ref=object_kind,
                reason="matched authority describes this as part of its pipeline",
                confidence="medium",
                implied_by_span=_span_for_object_match(matches, object_kind),
                prerequisite_for=object_kind,
                metadata={"source": "authority_summary_pipeline"},
            )

        object_text = normalize_match_text(summary)
        if (
            "parallel" in object_text or "multi angle" in object_text or "fan" in object_text
        ) and "research" in _terms(intent):
            key = (object_kind, "fan_out_research")
            suggestions[key] = SuggestedStep(
                title="Decide whether to fan out research across multiple workers or angles",
                status="suggested",
                source_authority_ref=object_kind,
                reason="matched research authority advertises parallel or multi-angle execution",
                confidence="high",
                implied_by_span=_span_for_object_match(matches, object_kind),
                prerequisite_for=object_kind,
                metadata={"source": "authority_summary_parallel_research"},
            )

        fields = [dict(row) for row in description.get("fields") or []]
        required_for_actions = _required_field_suggestions(
            object_kind=object_kind,
            fields=fields,
            stated_terms=stated_terms,
        )
        suggestions.update(required_for_actions)

    return list(suggestions.values()), warnings


def _span_for_object_match(matches: list[AuthorityMatch], object_kind: str) -> str | None:
    for match in matches:
        if (
            match.source_order > 0
            and (match.object_kind == object_kind or match.authority_ref == object_kind)
        ):
            return match.span_text
    for match in matches:
        if match.object_kind == object_kind or match.authority_ref == object_kind:
            return match.span_text
    return None


def _required_field_suggestions(
    *,
    object_kind: str,
    fields: list[dict[str, Any]],
    stated_terms: set[str],
) -> dict[tuple[str, str], SuggestedStep]:
    suggestions: dict[tuple[str, str], SuggestedStep] = {}
    for row in fields:
        field_path = str(row.get("field_path") or "")
        description = str(row.get("description") or "")
        if not field_path or "required for" not in description.lower():
            continue
        required_actions = {
            normalize_match_text(match.group(1))
            for match in re.finditer(
                r"required\s+for\s+'?([a-z0-9_ -]+)'?",
                description,
                flags=re.IGNORECASE,
            )
        }
        if required_actions and not (required_actions & stated_terms):
            continue
        if _terms(field_path.replace("_", " ")) & stated_terms:
            continue
        suggestions[(object_kind, field_path)] = SuggestedStep(
            title=f"Confirm value for {object_kind}.{field_path}",
            status="suggested",
            source_authority_ref=f"{object_kind}.{field_path}",
            reason=description,
            confidence="medium",
            implied_by_span=None,
            prerequisite_for=object_kind,
            metadata={"source": "field_description_required_for"},
        )
    return suggestions


def recognize_intent(
    intent: str,
    *,
    conn: Any,
    match_limit: int = 5,
) -> IntentRecognition:
    text = (intent or "").strip()
    if not text:
        return IntentRecognition(
            intent="",
            extracted=[],
            matched=[],
            suggested=[],
            gaps=[],
            warnings=["intent is empty"],
        )

    spans = _extract_spans(text)
    data_matches, data_warnings = _data_pill_matches(
        text,
        conn=conn,
        spans=spans,
        limit=max(int(match_limit), 1),
    )
    object_matches, object_warnings = _object_matches(
        conn=conn,
        spans=spans,
        limit_per_span=max(min(int(match_limit), 5), 1),
    )

    all_matches = _dedupe_matches([*data_matches, *object_matches])
    suggestions, suggestion_warnings = _suggest_from_authority(
        conn=conn,
        intent=text,
        spans=spans,
        matches=all_matches,
    )
    gaps = _gaps_for_unmatched_spans(spans, all_matches)
    return IntentRecognition(
        intent=text,
        extracted=spans,
        matched=all_matches,
        suggested=suggestions,
        gaps=gaps,
        warnings=[*data_warnings, *object_warnings, *suggestion_warnings],
    )


def _dedupe_matches(matches: list[AuthorityMatch]) -> list[AuthorityMatch]:
    by_key: dict[tuple[str, int, str, str], AuthorityMatch] = {}
    for match in matches:
        key = (
            match.span_text,
            match.source_order,
            match.authority_ref,
            match.authority_kind,
        )
        existing = by_key.get(key)
        if existing is None or match.score > existing.score:
            by_key[key] = match
    return sorted(
        by_key.values(),
        key=lambda item: (item.source_order, -item.score, item.authority_ref),
    )


def _gaps_for_unmatched_spans(
    spans: list[RecognizedSpan],
    matches: list[AuthorityMatch],
) -> list[RecognitionGap]:
    matched_orders = {match.source_order for match in matches if match.source_order > 0}
    gaps: list[RecognitionGap] = []
    for span in spans:
        if span.source_order in matched_orders:
            continue
        gaps.append(
            RecognitionGap(
                span_text=span.text,
                source_order=span.source_order,
                kind=span.kind,
                reason="no authority candidate met the deterministic match threshold",
            )
        )
    return gaps


__all__ = [
    "AuthorityMatch",
    "IntentRecognition",
    "RecognitionGap",
    "RecognizedSpan",
    "SuggestedStep",
    "recognize_intent",
]
