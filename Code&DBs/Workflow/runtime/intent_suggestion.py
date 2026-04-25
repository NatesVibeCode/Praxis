"""Layer 0 (Suggest): atoms from free prose, no ordering produced.

Where ``intent_decomposition`` and ``intent_binding`` are deterministic
splitters/binders, this layer is a *suggester*: given any prose — five
sentences, no markers, no order — return the candidate pills, candidate
step types, and candidate input parameters as separate streams. The
downstream LLM author (or the operator) composes those atoms into a
workflow.

This layer never picks order, count, or final stage. It surfaces what
the prose contains so a downstream author has the right materials.

HONEST SCOPE:

  - Deterministic. Regex + the same data-dictionary / lexicon authority
    used by ``bind_data_pills``. No LLM call here.
  - Suggestion only. Confidence scores are advisory; no field is bound,
    no step is committed, no order is implied.
  - Whole-prose. Unlike ``decompose_intent``, this does not require step
    markers. Five sentences with no list structure are fully usable input.

Standing-order anchors:
  - ``architecture-policy::workflow-intent-binding::loose-prose-data-pill-suggestions-before-binding``
  - ``platform_architecture / llm-first-infrastructure-trust-compiler-engine``
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from runtime.intent_binding import BoundIntent, bind_data_pills


# Verb → (suggested_stage, base_confidence, capability_hints).
# Broader than ``intent_decomposition._STAGE_VERB_MAP`` on purpose —
# decomposition is conservative because it picks one stage per step;
# the suggester returns candidates and lets the downstream author choose.
_STAGE_VERB_HINTS: dict[str, tuple[str, float, tuple[str, ...]]] = {
    # build family
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
    # research family
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
    # review family
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
    # test family
    "test": ("test", 0.85, ("testing",)),
    # fix family
    "fix": ("fix", 0.85, ("debug",)),
    "repair": ("fix", 0.8, ("debug",)),
    "resolve": ("fix", 0.75, ("debug",)),
    "patch": ("fix", 0.75, ("debug",)),
    "rollback": ("fix", 0.8, ("debug",)),
    "revert": ("fix", 0.8, ("debug",)),
    "undo": ("fix", 0.8, ("debug",)),
}


# Phrase-level overrides. Run alongside verb hints; downstream picks by
# (stage, phrase_span) dedup keyed on highest confidence.
_PHRASE_OVERRIDES: list[tuple[re.Pattern[str], str, float, tuple[str, ...], str]] = [
    (
        re.compile(
            r"\b(?:write|add|author)\s+(?:integration\s+|unit\s+|smoke\s+)?tests?\b",
            re.IGNORECASE,
        ),
        "test",
        0.9,
        ("testing",),
        "write tests",
    ),
    (
        re.compile(r"\bsmoke[-\s]?(?:test|check|endpoint)\b", re.IGNORECASE),
        "test",
        0.85,
        ("testing",),
        "smoke test",
    ),
    (
        re.compile(r"\b(?:make\s+sure|ensure)\b", re.IGNORECASE),
        "review",
        0.65,
        ("review", "validation"),
        "ensure",
    ),
    (
        re.compile(r"\b(?:roll\s*back|revert)\b", re.IGNORECASE),
        "fix",
        0.85,
        ("debug",),
        "rollback",
    ),
    (
        re.compile(r"\blook\s+at\b", re.IGNORECASE),
        "research",
        0.7,
        ("research",),
        "look at",
    ),
    (
        re.compile(r"\bscore\s+(?:the\s+)?fit\b|\bevaluate\s+fit\b", re.IGNORECASE),
        "review",
        0.85,
        ("review", "analysis"),
        "score fit",
    ),
]


# Clause boundaries — break prose into candidate spans.
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
    """Break prose into clause spans for verb scanning.

    Returns ``(clause_text, original_offset)`` pairs. Splits on sentence
    punctuation, comma/semicolon/colon, and conjunction words. Leading
    marker words ('then', 'finally', etc.) are stripped from the resulting
    clause.
    """
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
    """A candidate step type lifted from a clause of the prose."""

    phrase_span: str
    suggested_stage: str
    confidence: float
    capability_hints: tuple[str, ...]
    matched_verb: str | None
    rule: str  # 'verb' | 'phrase'

    def to_dict(self) -> dict[str, Any]:
        return {
            "phrase_span": self.phrase_span,
            "suggested_stage": self.suggested_stage,
            "confidence": round(self.confidence, 3),
            "capability_hints": list(self.capability_hints),
            "matched_verb": self.matched_verb,
            "rule": self.rule,
        }


# Patterns for input parameters: "feed in X", "given X", "takes X", "accepts X".
_PARAM_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (
        re.compile(
            r"\b(?:we\s+)?feed[s]?\s+in\s+(?:an?\s+|the\s+)?([^,;.\n]+?)"
            r"(?=\s*[,;.\n]|\s+(?:and|to|then|that|which|so)\b|$)",
            re.IGNORECASE,
        ),
        "feed_in",
    ),
    (
        re.compile(
            r"\bgiven\s+(?:an?\s+|the\s+)?([^,;.\n]+?)"
            r"(?=\s*[,;.\n]|\s+(?:and|to|then|that|which|so)\b|$)",
            re.IGNORECASE,
        ),
        "given",
    ),
    (
        re.compile(
            r"\btakes?\s+(?:an?\s+|the\s+)?([^,;.\n]+?)"
            r"(?=\s*[,;.\n]|\s+(?:and|to|then|that|which|so)\b|$)",
            re.IGNORECASE,
        ),
        "takes",
    ),
    (
        re.compile(
            r"\baccept[s]?\s+(?:an?\s+|the\s+)?([^,;.\n]+?)"
            r"(?=\s*[,;.\n]|\s+(?:and|to|then|that|which|so)\b|$)",
            re.IGNORECASE,
        ),
        "accepts",
    ),
    (
        re.compile(
            r"\b(?:input|param(?:eter)?)\s*(?:is|=|:)\s*(?:an?\s+|the\s+)?([^,;.\n]+?)"
            r"(?=\s*[,;.\n]|\s+(?:and|to|then|that|which|so)\b|$)",
            re.IGNORECASE,
        ),
        "input_is",
    ),
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
    """A candidate workflow input lifted from a parameter-introducing phrase."""

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
class SuggestedAtoms:
    """Layer 0 output: pills, step types, parameters — no ordering, no count."""

    intent: str
    pills: BoundIntent
    step_types: list[StepTypeSuggestion]
    parameters: list[ParameterSuggestion]
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "intent": self.intent,
            "pills": self.pills.to_dict(),
            "step_types": [s.to_dict() for s in self.step_types],
            "parameters": [p.to_dict() for p in self.parameters],
            "notes": list(self.notes),
        }


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
                out.append(
                    StepTypeSuggestion(
                        phrase_span=clause,
                        suggested_stage=stage,
                        confidence=conf,
                        capability_hints=caps,
                        matched_verb=label,
                        rule="phrase",
                    )
                )
        for verb, (stage, conf, caps) in _STAGE_VERB_HINTS.items():
            if re.search(rf"\b{re.escape(verb)}(?:s|es|ed|ing)?\b", clause_lower):
                out.append(
                    StepTypeSuggestion(
                        phrase_span=clause,
                        suggested_stage=stage,
                        confidence=conf,
                        capability_hints=caps,
                        matched_verb=verb,
                        rule="verb",
                    )
                )
    by_key: dict[tuple[str, str], StepTypeSuggestion] = {}
    for suggestion in out:
        key = (suggestion.suggested_stage, suggestion.phrase_span)
        prev = by_key.get(key)
        if prev is None or suggestion.confidence > prev.confidence:
            by_key[key] = suggestion
    return sorted(by_key.values(), key=lambda s: (-s.confidence, s.phrase_span))


def suggest_plan_atoms(intent: str, *, conn: Any) -> SuggestedAtoms:
    """Suggest pills, step types, and parameters from free prose.

    Layer 0 of the planning stack. Runs deterministically on any prose
    with no marker requirement. Returns suggestions only — no ordering,
    no count, no final stages, no spec. The downstream LLM author (or
    the operator) consumes the atoms and authors the actual workflow.

    Args:
        intent: prose describing what the caller wants done. Length and
            structure are unconstrained.
        conn: live Postgres connection for the data dictionary authority
            (used by the underlying ``bind_data_pills`` call).

    Returns:
        ``SuggestedAtoms`` with pills (suggested + bound + ambiguous +
        unbound from the data dictionary), step_types (verb-/phrase-keyed
        stage candidates with confidence), and parameters (input
        candidates from ``feed in X`` / ``given X`` / ``takes X`` /
        ``accepts X`` / ``input is X`` patterns).
    """
    text = (intent or "").strip()
    if not text:
        return SuggestedAtoms(
            intent="",
            pills=BoundIntent(intent="", warnings=["intent is empty"]),
            step_types=[],
            parameters=[],
            notes=["intent is empty"],
        )

    pills = bind_data_pills(text, conn=conn, suggest=True)
    step_types = _suggest_step_types(text)
    parameters = _suggest_parameters(text)

    notes: list[str] = []
    if not step_types:
        notes.append(
            "no stage-suggestive verbs detected; downstream LLM author "
            "should pick step types from prose context"
        )
    if not pills.bound and not pills.suggested:
        notes.append(
            "no data-pill candidates surfaced; refer to specific objects "
            "or fields in the prose to anchor pills"
        )
    if not parameters:
        notes.append(
            "no input parameters detected; if the workflow takes runtime "
            "inputs, phrase them as 'feed in X' / 'given X' / 'takes X'"
        )
    return SuggestedAtoms(
        intent=text,
        pills=pills,
        step_types=step_types,
        parameters=parameters,
        notes=notes,
    )
