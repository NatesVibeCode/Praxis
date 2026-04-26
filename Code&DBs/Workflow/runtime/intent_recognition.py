"""Deterministic recognition for messy compile prose.

This layer is intentionally not a planner. It extracts spans the user actually
wrote, matches those spans to known authority text, and marks missing pieces as
gaps. Suggestions stay advisory until a command materializes workflow state.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any

from runtime.data_dictionary import describe_object, list_object_kinds
from runtime.intent_binding import bind_data_pills
from runtime.intent_lexicon import expand_query_terms, normalize_match_text


@dataclass(frozen=True)
class RecognizedSpan:
    text: str
    kind: str
    normalized: str
    start: int
    end: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AuthorityMatch:
    span_text: str
    object_kind: str
    label: str
    category: str | None
    confidence: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SuggestedStep:
    label: str
    source_ref: str
    reason: str
    status: str = "suggested"
    confidence: str = "medium"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RecognitionGap:
    span_text: str
    kind: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class IntentRecognition:
    intent: str
    spans: list[RecognizedSpan] = field(default_factory=list)
    matches: list[AuthorityMatch] = field(default_factory=list)
    suggested_steps: list[SuggestedStep] = field(default_factory=list)
    gaps: list[RecognitionGap] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        extracted = [span.to_dict() for span in self.spans]
        matched = [
            {
                **match.to_dict(),
                "authority_ref": match.object_kind,
            }
            for match in self.matches
        ]
        suggested = [
            {
                **step.to_dict(),
                "title": (
                    "fan out research: decide whether research should fan out"
                    if step.label == "decide whether research should fan out"
                    else step.label
                ),
                "source_authority_ref": step.source_ref,
            }
            for step in self.suggested_steps
        ]
        return {
            "intent": self.intent,
            "spans": extracted,
            "extracted": extracted,
            "matches": matched,
            "matched": matched,
            "suggested_steps": suggested,
            "suggested": suggested,
            "gaps": [gap.to_dict() for gap in self.gaps],
        }


_SPAN_PATTERNS: tuple[tuple[str, str, re.Pattern[str]], ...] = (
    ("input", "app_name", re.compile(r"\bapp(?:lication)?\s+name\b", re.I)),
    ("input", "app_domain", re.compile(r"\bapp(?:lication)?\s+domain\b|\bdomain\b", re.I)),
    ("operation", "custom_integration", re.compile(r"\bcustom\s+integration\b|\bintegration\b|\bconnector\b", re.I)),
    ("operation", "plan", re.compile(r"\bplan(?:ning)?\b|\bscope\b", re.I)),
    ("operation", "search", re.compile(r"\bresearch\b|\bsearch\b|\bdiscover\b|\bfind\b|\blook\s+up\b", re.I)),
    ("operation", "retrieve", re.compile(r"\bretrieve\b|\bfetch\b|\bcollect\b|\bgather\b", re.I)),
    ("operation", "evaluate", re.compile(r"\bevaluate\b|\bassess\b|\breview\b", re.I)),
    ("operation", "build", re.compile(r"\battempt\s+to\s+build\b|\bbuild\b|\bcreate\b|\bimplement\b", re.I)),
    ("operation", "verify", re.compile(r"\bverify\b|\bvalidate\b|\btest\b|\bsmoke\b", re.I)),
    ("control", "fan_out", re.compile(r"\bfan[- ]out\b|\bparallel\b|\bmultiple\s+sources\b", re.I)),
)

_COMPILE_PATTERN_LABELS = {
    "app_name": "App name input",
    "app_domain": "App domain input",
    "custom_integration": "Custom integration",
    "plan": "Plan scope",
    "search": "Search sources",
    "retrieve": "Retrieve source material",
    "evaluate": "Evaluate fit",
    "build": "Build attempt",
    "verify": "Verify proof",
    "fan_out": "Fan-out decision",
}


def _extract_spans(intent: str) -> list[RecognizedSpan]:
    spans: list[RecognizedSpan] = []
    seen: set[tuple[int, int, str]] = set()
    for kind, normalized, pattern in _SPAN_PATTERNS:
        for match in pattern.finditer(intent):
            key = (match.start(), match.end(), normalized)
            if key in seen:
                continue
            seen.add(key)
            spans.append(
                RecognizedSpan(
                    text=match.group(0),
                    kind=kind,
                    normalized=normalized,
                    start=match.start(),
                    end=match.end(),
                )
            )
    return sorted(spans, key=lambda span: (span.start, span.end))


def _authority_rows(conn: Any) -> list[dict[str, Any]]:
    try:
        return [dict(row) for row in list_object_kinds(conn)]
    except Exception:
        return []


def _row_text(row: dict[str, Any]) -> str:
    return " ".join(
        str(row.get(key) or "")
        for key in ("object_kind", "label", "summary", "category", "description")
    )


def _matches_for_span(
    span: RecognizedSpan,
    *,
    rows: list[dict[str, Any]],
    limit: int,
) -> list[AuthorityMatch]:
    terms = set(expand_query_terms(f"{span.text} {span.normalized}"))
    if span.normalized == "app_name":
        terms.update({"app", "application", "name"})
    elif span.normalized == "app_domain":
        terms.update({"app", "application", "domain"})
    scored: list[tuple[float, dict[str, Any], str]] = []
    for row in rows:
        haystack = normalize_match_text(_row_text(row))
        if not haystack:
            continue
        score = 0.0
        hits: list[str] = []
        for term in sorted(terms):
            if term and f" {term} " in f" {haystack} ":
                score += 1.0
                hits.append(term)
        object_kind = str(row.get("object_kind") or "")
        if span.normalized and span.normalized in object_kind:
            score += 2.0
            hits.append(span.normalized)
        if score <= 0:
            continue
        scored.append((score, row, ", ".join(hits[:4]) or "term overlap"))
    scored.sort(key=lambda item: (-item[0], str(item[1].get("object_kind") or "")))
    matches: list[AuthorityMatch] = []
    for score, row, reason in scored[:limit]:
        confidence = "high" if score >= 3 else "medium"
        matches.append(
            AuthorityMatch(
                span_text=span.text,
                object_kind=str(row.get("object_kind") or ""),
                label=str(row.get("label") or row.get("object_kind") or ""),
                category=str(row.get("category") or "") or None,
                confidence=confidence,
                reason=f"matched authority text: {reason}",
            )
        )
    return matches


def _append_suggestion(
    suggestions: list[SuggestedStep],
    seen: set[tuple[str, str]],
    *,
    label: str,
    source_ref: str,
    reason: str,
    confidence: str = "medium",
) -> None:
    key = (source_ref, label.lower())
    if key in seen:
        return
    seen.add(key)
    suggestions.append(
        SuggestedStep(
            label=label,
            source_ref=source_ref,
            reason=reason,
            confidence=confidence,
        )
    )


def _span_kinds(spans: list[RecognizedSpan]) -> set[str]:
    return {span.normalized for span in spans}


def _suggest_steps(
    spans: list[RecognizedSpan],
    matches: list[AuthorityMatch],
    rows: list[dict[str, Any]],
) -> list[SuggestedStep]:
    by_kind = {str(row.get("object_kind") or ""): row for row in rows}
    suggestions: list[SuggestedStep] = []
    seen: set[tuple[str, str]] = set()
    for match in matches:
        row = by_kind.get(match.object_kind) or {}
        summary = str(row.get("summary") or row.get("description") or "")
        pipeline_match = re.search(r"pipeline\s*\(([^)]{3,240})\)", summary, flags=re.I)
        if not pipeline_match:
            continue
        raw_steps = re.split(r"\s*(?:->|\u2192)\s*", pipeline_match.group(1))
        for raw in raw_steps:
            label = raw.strip(" .;:")
            if not label:
                continue
            _append_suggestion(
                suggestions,
                seen,
                label=label,
                source_ref=match.object_kind,
                reason="matched authority describes this as part of its pipeline",
            )
    kinds = _span_kinds(spans)
    matched_tool_ref = next(
        (match.object_kind for match in matches if match.object_kind.startswith("tool:")),
        "tool:compile_intent_patterns",
    )
    has_app_input = bool(kinds.intersection({"app_name", "app_domain"}))
    has_research = bool(kinds.intersection({"search", "retrieve", "evaluate"}))
    has_integration = "custom_integration" in kinds
    if has_app_input and has_integration:
        _append_suggestion(
            suggestions,
            seen,
            label="decide built-in vs custom integration path",
            source_ref=matched_tool_ref,
            reason="app integration work should first decide whether an existing integration can satisfy the requested inputs and outputs",
            confidence="high",
        )
    if (has_app_input or has_research) and "fan_out" not in kinds:
        _append_suggestion(
            suggestions,
            seen,
            label="decide whether research should fan out",
            source_ref=matched_tool_ref,
            reason="app/domain discovery often needs parallel official docs, search, and existing-catalog checks before evaluation",
            confidence="high",
        )
    if has_app_input and "search" not in kinds:
        _append_suggestion(
            suggestions,
            seen,
            label="search official app and API sources",
            source_ref=matched_tool_ref,
            reason="app name/domain inputs need authoritative source discovery before retrieval or evaluation",
        )
    if has_research and "retrieve" not in kinds:
        _append_suggestion(
            suggestions,
            seen,
            label="retrieve docs, auth details, and API shape",
            source_ref=matched_tool_ref,
            reason="research findings need durable source material before evaluation",
        )
    if has_research and "evaluate" not in kinds:
        _append_suggestion(
            suggestions,
            seen,
            label="evaluate capability fit and constraints",
            source_ref=matched_tool_ref,
            reason="retrieved source material needs an explicit fit decision before build",
        )
    if has_integration and "verify" not in kinds:
        _append_suggestion(
            suggestions,
            seen,
            label="verify integration with a smoke run",
            source_ref=matched_tool_ref,
            reason="custom integrations need proof that auth, retrieval, and execution work after build",
            confidence="high",
        )
    return suggestions


def recognize_intent(intent: str, *, conn: Any, match_limit: int = 5) -> IntentRecognition:
    clean_intent = (intent or "").strip()
    if not clean_intent:
        raise ValueError("intent must be a non-empty string")
    spans = _extract_spans(clean_intent)
    rows = _authority_rows(conn)
    all_matches: list[AuthorityMatch] = []
    gaps: list[RecognitionGap] = []
    for span in spans:
        span_matches = _matches_for_span(span, rows=rows, limit=match_limit)
        if not span_matches:
            gaps.append(
                RecognitionGap(
                    span_text=span.text,
                    kind=span.normalized,
                    reason="no strong authority candidate found",
                )
            )
        all_matches.extend(span_matches)
    try:
        bound = bind_data_pills(
            clean_intent,
            conn=conn,
            object_kinds=[str(row.get("object_kind") or "") for row in rows],
        )
    except Exception:
        bound = None
    for candidate in getattr(bound, "suggested", ()) or ():
        object_kind = str(getattr(candidate, "object_kind", "") or "").strip()
        field_path = str(getattr(candidate, "field_path", "") or "").strip()
        if not object_kind or not field_path:
            continue
        all_matches.append(
            AuthorityMatch(
                span_text=field_path,
                object_kind=f"{object_kind}.{field_path}",
                label=field_path,
                category="data_pill",
                confidence=str(getattr(candidate, "confidence", "") or "medium"),
                reason=str(getattr(candidate, "reason", "") or "matched data-pill authority"),
            )
        )
    matched_refs = {match.object_kind for match in all_matches}
    if any(ref.startswith("tool:praxis_connector") for ref in matched_refs):
        gaps = [
            gap
            for gap in gaps
            if gap.kind not in {"app_name", "app_domain", "custom_integration"}
        ]
    return IntentRecognition(
        intent=clean_intent,
        spans=spans,
        matches=all_matches,
        suggested_steps=_suggest_steps(spans, all_matches, rows),
        gaps=gaps,
    )


__all__ = [
    "AuthorityMatch",
    "IntentRecognition",
    "RecognitionGap",
    "RecognizedSpan",
    "SuggestedStep",
    "bind_data_pills",
    "describe_object",
    "recognize_intent",
]
