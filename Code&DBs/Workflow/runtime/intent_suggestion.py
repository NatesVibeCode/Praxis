"""Layer 0 (Suggest): atoms from free prose, no ordering produced.

Reuses ``runtime.intent_binding.bind_data_pills`` for pill candidates.
Adds verb-keyed step-type suggestions and parameter detection on raw
prose with no marker requirement.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from runtime.intent_binding import BoundIntent, bind_data_pills


_PILL_STOPWORDS: frozenset[str] = frozenset({
    "the", "and", "for", "with", "from", "into", "this", "that", "then",
    "are", "but", "not", "our", "its", "use", "you", "your", "where",
    "when", "what", "who", "how", "all", "any", "can", "will", "may",
    "should", "would", "could", "does", "have", "has", "had", "been",
    "want", "need", "like", "make", "made", "give", "given", "feed",
    "take", "takes", "given", "input", "output", "step", "steps",
})


def _tokenize_for_pills(text: str) -> set[str]:
    return {
        t for t in re.findall(r"[a-z][a-z0-9_]+", (text or "").lower())
        if len(t) > 2 and t not in _PILL_STOPWORDS
    }


_STAGE_VERB_HINTS: dict[str, tuple[str, float, tuple[str, ...]]] = {
    "build": ("build", 0.85, ("code_generation", "architecture")),
    "implement": ("build", 0.85, ("code_generation",)),
    "wire": ("build", 0.8, ("code_generation",)),
    "add": ("build", 0.7, ("code_generation",)),
    "create": ("build", 0.7, ("code_generation",)),
    "write": ("build", 0.65, ("code_generation",)),
    "compose": ("build", 0.65, ("code_generation",)),
    "migrate": ("build", 0.7, ("code_generation",)),
    "refactor": ("build", 0.75, ("code_generation",)),
    "extend": ("build", 0.7, ("code_generation",)),
    "attempt": ("build", 0.4, ("code_generation",)),
    "ship": ("build", 0.7, ("code_generation",)),
    "research": ("research", 0.9, ("research",)),
    "investigate": ("research", 0.85, ("research",)),
    "explore": ("research", 0.8, ("research",)),
    "study": ("research", 0.75, ("research",)),
    "analyze": ("research", 0.75, ("research", "analysis")),
    "examine": ("research", 0.75, ("research", "analysis")),
    "search": ("research", 0.8, ("research",)),
    "find": ("research", 0.6, ("research",)),
    "discover": ("research", 0.7, ("research",)),
    "retrieve": ("research", 0.75, ("research", "retrieval")),
    "fetch": ("research", 0.7, ("research", "retrieval")),
    "gather": ("research", 0.7, ("research",)),
    "collect": ("research", 0.7, ("research",)),
    "plan": ("research", 0.65, ("architecture", "research")),
    "review": ("review", 0.85, ("review",)),
    "audit": ("review", 0.85, ("review",)),
    "evaluate": ("review", 0.8, ("review", "analysis")),
    "assess": ("review", 0.8, ("review", "analysis")),
    "score": ("review", 0.7, ("review", "analysis")),
    "compare": ("review", 0.7, ("review", "analysis")),
    "verify": ("review", 0.8, ("review", "validation")),
    "confirm": ("review", 0.75, ("review",)),
    "check": ("review", 0.6, ("review",)),
    "validate": ("review", 0.75, ("review", "validation")),
    "test": ("test", 0.85, ("testing",)),
    "fix": ("fix", 0.85, ("debug",)),
    "repair": ("fix", 0.8, ("debug",)),
    "resolve": ("fix", 0.75, ("debug",)),
    "patch": ("fix", 0.75, ("debug",)),
    "rollback": ("fix", 0.8, ("debug",)),
    "revert": ("fix", 0.8, ("debug",)),
    "undo": ("fix", 0.8, ("debug",)),
}


_PHRASE_OVERRIDES: list[tuple[re.Pattern[str], str, float, tuple[str, ...], str]] = [
    (re.compile(r"\b(?:write|add|author)\s+(?:integration\s+|unit\s+|smoke\s+)?tests?\b", re.IGNORECASE),
     "test", 0.9, ("testing",), "write tests"),
    (re.compile(r"\bsmoke[-\s]?(?:test|check|endpoint)\b", re.IGNORECASE),
     "test", 0.85, ("testing",), "smoke test"),
    (re.compile(r"\b(?:make\s+sure|ensure)\b", re.IGNORECASE),
     "review", 0.65, ("review", "validation"), "ensure"),
    (re.compile(r"\b(?:roll\s*back|revert)\b", re.IGNORECASE),
     "fix", 0.85, ("debug",), "rollback"),
    (re.compile(r"\blook\s+at\b", re.IGNORECASE),
     "research", 0.7, ("research",), "look at"),
    (re.compile(r"\bscore\s+(?:the\s+)?fit\b|\bevaluate\s+fit\b", re.IGNORECASE),
     "review", 0.85, ("review", "analysis"), "score fit"),
]


_CLAUSE_SPLIT_RE = re.compile(
    r"(?<=[\.\!\?])\s+|"
    r"\s*[,;:]\s+|"
    r"\s+(?:and|then|or|but|finally|next|after\s+that)\s+",
    re.IGNORECASE,
)

_LEADING_MARKER_RE = re.compile(
    r"^(?:and|then|or|but|finally|next|first|after\s+that)[,:\s]+",
    re.IGNORECASE,
)


def _split_clauses(intent: str) -> list[tuple[str, int]]:
    out: list[tuple[str, int]] = []
    cursor = 0
    for match in _CLAUSE_SPLIT_RE.finditer(intent):
        chunk = intent[cursor : match.start()].strip()
        chunk = _LEADING_MARKER_RE.sub("", chunk).strip()
        if chunk:
            out.append((chunk, cursor))
        cursor = match.end()
    tail = intent[cursor:].strip()
    tail = _LEADING_MARKER_RE.sub("", tail).strip()
    if tail:
        out.append((tail, cursor))
    return out


@dataclass(frozen=True)
class StepTypeSuggestion:
    phrase_span: str
    suggested_stage: str
    confidence: float
    capability_hints: tuple[str, ...]
    matched_verb: str | None
    rule: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "phrase_span": self.phrase_span,
            "suggested_stage": self.suggested_stage,
            "confidence": round(self.confidence, 3),
            "capability_hints": list(self.capability_hints),
            "matched_verb": self.matched_verb,
            "rule": self.rule,
        }


_PARAM_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b(?:we\s+)?feed[s]?\s+in\s+(?:an?\s+|the\s+)?([^,;.\n]+?)(?=\s*[,;.\n]|\s+(?:and|to|then|that|which|so)\b|$)", re.IGNORECASE), "feed_in"),
    (re.compile(r"\bgiven\s+(?:an?\s+|the\s+)?([^,;.\n]+?)(?=\s*[,;.\n]|\s+(?:and|to|then|that|which|so)\b|$)", re.IGNORECASE), "given"),
    (re.compile(r"\btakes?\s+(?:an?\s+|the\s+)?([^,;.\n]+?)(?=\s*[,;.\n]|\s+(?:and|to|then|that|which|so)\b|$)", re.IGNORECASE), "takes"),
    (re.compile(r"\baccept[s]?\s+(?:an?\s+|the\s+)?([^,;.\n]+?)(?=\s*[,;.\n]|\s+(?:and|to|then|that|which|so)\b|$)", re.IGNORECASE), "accepts"),
    (re.compile(r"\b(?:input|param(?:eter)?)\s*(?:is|=|:)\s*(?:an?\s+|the\s+)?([^,;.\n]+?)(?=\s*[,;.\n]|\s+(?:and|to|then|that|which|so)\b|$)", re.IGNORECASE), "input_is"),
]

_TYPE_HINT_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b(?:domain|url|hostname|host)\b", re.IGNORECASE), "url|domain"),
    (re.compile(r"\bid\b", re.IGNORECASE), "identifier"),
    (re.compile(r"\bname\b", re.IGNORECASE), "string"),
    (re.compile(r"\bpath\b", re.IGNORECASE), "filepath"),
    (re.compile(r"\bemail\b", re.IGNORECASE), "email"),
    (re.compile(r"\b(?:json|yaml|toml|xml)\b", re.IGNORECASE), "config_payload"),
    (re.compile(r"\b(?:count|number|n)\b", re.IGNORECASE), "integer"),
]


@dataclass(frozen=True)
class ParameterSuggestion:
    phrase: str
    name: str
    type_hint: str | None
    rule: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "phrase": self.phrase,
            "name": self.name,
            "type_hint": self.type_hint,
            "rule": self.rule,
        }


@dataclass(frozen=True)
class SuggestedPill:
    """A loose-prose pill candidate scored against the data dictionary.

    Differs from BoundPill: not an explicit ``object.field`` ref in the prose;
    inferred by token overlap against dictionary text. The spec author may
    accept, reject, or ignore.
    """

    object_kind: str
    field_path: str | None
    score: int
    matched_terms: list[str]
    label: str | None
    summary: str | None
    field_kind: str | None

    @property
    def ref(self) -> str:
        return f"{self.object_kind}.{self.field_path}" if self.field_path else self.object_kind

    def to_dict(self) -> dict[str, Any]:
        return {
            "ref": self.ref,
            "object_kind": self.object_kind,
            "field_path": self.field_path,
            "score": self.score,
            "matched_terms": list(self.matched_terms),
            "label": self.label,
            "summary": self.summary,
            "field_kind": self.field_kind,
        }


@dataclass(frozen=True)
class SuggestedAtoms:
    intent: str
    pills: BoundIntent
    suggested_pills: list[SuggestedPill]
    step_types: list[StepTypeSuggestion]
    parameters: list[ParameterSuggestion]
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "intent": self.intent,
            "pills": self.pills.to_dict(),
            "suggested_pills": [p.to_dict() for p in self.suggested_pills],
            "step_types": [s.to_dict() for s in self.step_types],
            "parameters": [p.to_dict() for p in self.parameters],
            "notes": list(self.notes),
        }


def _suggest_pills_from_data_dictionary(
    intent: str, *, conn: Any, top_n: int = 12, min_score: int = 2,
) -> list[SuggestedPill]:
    """Scan registered objects + their effective fields for token overlap with the intent.

    Returns top-N (object, field) candidates whose token overlap with the intent
    meets ``min_score``. Object-level signal (label/summary/category) is combined
    with field-level signal (label/description) so an object whose fields are
    relevant scores higher than one whose only its name matches.
    """
    from runtime.data_dictionary import (
        DataDictionaryBoundaryError,
        list_object_kinds,
        list_effective_entries,
    )

    intent_tokens = _tokenize_for_pills(intent)
    if not intent_tokens:
        return []

    candidates: list[SuggestedPill] = []
    try:
        objects = list_object_kinds(conn)
    except DataDictionaryBoundaryError:
        return []

    for obj in objects:
        object_kind = str(obj.get("object_kind") or "")
        if not object_kind:
            continue
        obj_text = " ".join(
            str(obj.get(key) or "")
            for key in ("object_kind", "label", "summary", "category")
        )
        obj_tokens = _tokenize_for_pills(obj_text)
        obj_overlap = obj_tokens & intent_tokens

        try:
            entries = list_effective_entries(conn, object_kind=object_kind)
        except Exception:
            entries = []
        for entry in entries:
            field_path = str(entry.get("field_path") or "")
            if not field_path:
                continue
            field_text = " ".join(
                str(entry.get(key) or "")
                for key in ("field_path", "label", "description")
            )
            field_tokens = _tokenize_for_pills(field_text)
            field_overlap = field_tokens & intent_tokens
            score = len(obj_overlap) + 2 * len(field_overlap)
            if score < min_score:
                continue
            candidates.append(
                SuggestedPill(
                    object_kind=object_kind,
                    field_path=field_path,
                    score=score,
                    matched_terms=sorted(obj_overlap | field_overlap),
                    label=str(entry.get("label") or "") or None,
                    summary=str(entry.get("description") or "")[:160] or None,
                    field_kind=str(entry.get("field_kind") or "") or None,
                )
            )

    candidates.sort(key=lambda p: (-p.score, p.object_kind, p.field_path or ""))
    return candidates[:top_n]


def _slug_from_phrase(phrase: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", phrase.strip().lower()).strip("_")
    return slug[:48] if slug else "param"


def _guess_type_hint(phrase: str) -> str | None:
    for pattern, hint in _TYPE_HINT_RULES:
        if pattern.search(phrase):
            return hint
    return None


def _suggest_parameters(intent: str) -> list[ParameterSuggestion]:
    seen: set[str] = set()
    out: list[ParameterSuggestion] = []
    for pattern, rule in _PARAM_PATTERNS:
        for match in pattern.finditer(intent):
            phrase = match.group(0).strip()
            payload = match.group(1).strip().rstrip(",.;:")
            for piece in re.split(r"\s+or\s+", payload):
                piece = piece.strip(" ,.;")
                if not piece or len(piece) > 60:
                    continue
                key = piece.lower()
                if key in seen:
                    continue
                seen.add(key)
                out.append(
                    ParameterSuggestion(
                        phrase=phrase,
                        name=_slug_from_phrase(piece),
                        type_hint=_guess_type_hint(piece),
                        rule=rule,
                    )
                )
    return out


def _suggest_step_types(intent: str) -> list[StepTypeSuggestion]:
    out: list[StepTypeSuggestion] = []
    for clause, _offset in _split_clauses(intent):
        clause_lower = clause.lower()
        for pattern, stage, conf, caps, label in _PHRASE_OVERRIDES:
            if pattern.search(clause):
                out.append(StepTypeSuggestion(
                    phrase_span=clause, suggested_stage=stage, confidence=conf,
                    capability_hints=caps, matched_verb=label, rule="phrase",
                ))
        for verb, (stage, conf, caps) in _STAGE_VERB_HINTS.items():
            if re.search(rf"\b{re.escape(verb)}(?:s|es|ed|ing)?\b", clause_lower):
                out.append(StepTypeSuggestion(
                    phrase_span=clause, suggested_stage=stage, confidence=conf,
                    capability_hints=caps, matched_verb=verb, rule="verb",
                ))
    by_key: dict[tuple[str, str], StepTypeSuggestion] = {}
    for suggestion in out:
        key = (suggestion.suggested_stage, suggestion.phrase_span)
        prev = by_key.get(key)
        if prev is None or suggestion.confidence > prev.confidence:
            by_key[key] = suggestion
    return sorted(by_key.values(), key=lambda s: (-s.confidence, s.phrase_span))


def suggest_plan_atoms(intent: str, *, conn: Any) -> SuggestedAtoms:
    text = (intent or "").strip()
    if not text:
        return SuggestedAtoms(
            intent="", pills=BoundIntent(intent="", warnings=["intent is empty"]),
            step_types=[], parameters=[], notes=["intent is empty"],
        )
    pills = bind_data_pills(text, conn=conn)
    suggested_pills = _suggest_pills_from_data_dictionary(text, conn=conn)
    step_types = _suggest_step_types(text)
    parameters = _suggest_parameters(text)
    notes: list[str] = []
    if not step_types:
        notes.append("no stage-suggestive verbs detected")
    if not pills.bound and not suggested_pills:
        notes.append("no pill candidates surfaced from prose or data dictionary")
    if not parameters:
        notes.append("no input parameters detected")
    return SuggestedAtoms(
        intent=text, pills=pills, suggested_pills=suggested_pills,
        step_types=step_types, parameters=parameters, notes=notes,
    )
