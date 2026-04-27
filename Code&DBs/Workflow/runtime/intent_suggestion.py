"""Layer 0 (Suggest): atoms from free prose, no ordering produced.

Reuses ``runtime.intent_binding.bind_data_pills`` for pill candidates.
Adds verb-keyed step-type suggestions and parameter detection on raw
prose with no marker requirement.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

from runtime.intent_binding import BoundIntent, bind_data_pills

logger = logging.getLogger(__name__)


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


# Categories that should NEVER surface as workflow-composition pills.
# These are infrastructure / control-plane records, not data entities or
# fields that a workflow operates on. Surfacing them produces noisy
# candidates that downstream LLM filters can't reliably reject (the
# 50%-acceptance ceiling we hit on the picker matrix is the symptom).
#
# Conservative default — only `tool:*` is excluded. Other categories
# (command/query/projection/table) are debatable and may carry signal
# in some intents; revisit when we have data showing they're noise too.
_WORKFLOW_PILL_EXCLUDED_CATEGORIES: frozenset[str] = frozenset({"tool"})


# =====================================================================
# Semantic pill retrieval via pgvector (Fix #2)
# =====================================================================
#
# Token overlap is too coarse — it surfaces pills whose docstrings happen
# to share words with the intent but aren't conceptually related. The
# picker matrix proved no LLM filter on top can reliably clean that up
# (50% false-positive ceiling).
#
# Semantic retrieval embeds the intent + each (object, field) corpus row
# and ranks by cosine similarity. Concepts ("integration", "auth",
# "endpoint") cluster together in embedding space even when tokens differ
# ("API spec", "credentials", "URL"). The LLM filter then sees a smaller,
# more semantically-aligned candidate set and the 50% ceiling falls.
#
# Caching strategy: corpus + embeddings are cached at module scope.
# First call after process start pays ~5-15s to embed ~100 rows; every
# subsequent call pays ~50-100ms (just embed the intent).

_DICT_CORPUS_CACHE: list[dict[str, Any]] | None = None
_DICT_CORPUS_LOCK: Any = None  # lazy-init to avoid import-time threading


def _ensure_corpus_lock():
    global _DICT_CORPUS_LOCK
    if _DICT_CORPUS_LOCK is None:
        import threading
        _DICT_CORPUS_LOCK = threading.Lock()
    return _DICT_CORPUS_LOCK


def _load_dictionary_corpus(
    conn: Any, *, excluded_categories: frozenset[str],
) -> list[dict[str, Any]]:
    """Build + embed the (object, field) corpus once per process. Each
    row carries object_kind, field_path, label, description, category,
    and the embedding vector. Cached at module scope.

    Returns an empty list if the embedding backend is unavailable —
    callers fall back to token-overlap.
    """
    global _DICT_CORPUS_CACHE
    if _DICT_CORPUS_CACHE is not None:
        return _DICT_CORPUS_CACHE

    lock = _ensure_corpus_lock()
    with lock:
        if _DICT_CORPUS_CACHE is not None:
            return _DICT_CORPUS_CACHE

        from runtime.embedding_service import EmbeddingService, get_shared_embedder
        if not EmbeddingService.backend_available():
            _DICT_CORPUS_CACHE = []
            return _DICT_CORPUS_CACHE

        embedder = get_shared_embedder()
        if embedder is None:
            _DICT_CORPUS_CACHE = []
            return _DICT_CORPUS_CACHE

        from runtime.data_dictionary import (
            DataDictionaryBoundaryError, list_object_kinds, list_effective_entries,
        )

        try:
            objects = list_object_kinds(conn)
        except DataDictionaryBoundaryError:
            _DICT_CORPUS_CACHE = []
            return _DICT_CORPUS_CACHE

        rows: list[dict[str, Any]] = []
        texts: list[str] = []
        for obj in objects:
            object_kind = str(obj.get("object_kind") or "")
            if not object_kind:
                continue
            category = str(obj.get("category") or "").strip().lower()
            if category in excluded_categories:
                continue
            obj_label = str(obj.get("label") or "")
            obj_summary = str(obj.get("summary") or "")
            try:
                entries = list_effective_entries(conn, object_kind=object_kind)
            except Exception:
                entries = []
            for entry in entries:
                field_path = str(entry.get("field_path") or "")
                if not field_path:
                    continue
                label = str(entry.get("label") or "")
                description = str(entry.get("description") or "")
                # Embed text concatenates the most informative semantic
                # signals — object kind/label/summary + field path/label/description.
                text = (
                    f"{object_kind}.{field_path}\n"
                    f"object: {obj_label} — {obj_summary}\n"
                    f"field: {label} — {description}"
                ).strip()
                rows.append({
                    "object_kind": object_kind, "field_path": field_path,
                    "label": label or None, "description": description[:160] or None,
                    "category": category,
                    "field_kind": str(entry.get("field_kind") or "") or None,
                })
                texts.append(text)

        if not texts:
            _DICT_CORPUS_CACHE = []
            return _DICT_CORPUS_CACHE

        # Chunk the embed call. The semantic-backend service has a
        # short per-request timeout; sending all ~100+ rows in one
        # batch can exceed it. 16-row chunks fit comfortably.
        embeddings: list[list[float]] = []
        chunk_size = 16
        for chunk_start in range(0, len(texts), chunk_size):
            chunk = texts[chunk_start:chunk_start + chunk_size]
            try:
                chunk_vecs = embedder.embed(chunk)
            except Exception as exc:
                logger.warning(
                    "dictionary corpus embed failed at chunk %d-%d: %s",
                    chunk_start, chunk_start + len(chunk), exc,
                )
                _DICT_CORPUS_CACHE = []
                return _DICT_CORPUS_CACHE
            embeddings.extend(chunk_vecs)

        for row, vec in zip(rows, embeddings):
            row["embedding"] = vec
        _DICT_CORPUS_CACHE = rows
        logger.info("dictionary corpus embedded: %d rows", len(rows))
        return _DICT_CORPUS_CACHE


def _cosine(a: list[float], b: list[float]) -> float:
    """Cosine similarity for two same-length vectors. Pure Python — no numpy."""
    n = min(len(a), len(b))
    if n == 0:
        return 0.0
    dot = sum(a[i] * b[i] for i in range(n))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(x * x for x in b) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _suggest_pills_via_semantic(
    intent: str, *, conn: Any, top_n: int = 12, min_similarity: float = 0.45,
    excluded_categories: frozenset[str] = _WORKFLOW_PILL_EXCLUDED_CATEGORIES,
) -> list[SuggestedPill] | None:
    """Semantic-search variant of the pill suggester. Returns None when
    the embedding backend is unavailable — callers fall back to
    token-overlap. Returns [] when the corpus is empty / embedded but
    nothing scored above ``min_similarity``.

    ``min_similarity`` floor (0.45 default) drops the long tail of
    weakly-related rows. Empirical tuning per real intent is recommended;
    0.45 ≈ "noticeably related" for typical sentence-transformer models.
    """
    from runtime.embedding_service import EmbeddingService, get_shared_embedder
    if not EmbeddingService.backend_available():
        return None
    embedder = get_shared_embedder()
    if embedder is None:
        return None

    corpus = _load_dictionary_corpus(conn, excluded_categories=excluded_categories)
    if not corpus:
        return []

    try:
        intent_vec = embedder.embed_one(intent)
    except Exception as exc:
        logger.warning("intent embed failed: %s", exc)
        return None

    scored: list[tuple[float, dict[str, Any]]] = []
    for row in corpus:
        sim = _cosine(intent_vec, row["embedding"])
        if sim < min_similarity:
            continue
        scored.append((sim, row))
    scored.sort(key=lambda t: -t[0])

    return [
        SuggestedPill(
            object_kind=r["object_kind"],
            field_path=r["field_path"],
            score=int(round(sim * 100)),  # surface similarity * 100 as integer score
            matched_terms=[],  # n/a for semantic match
            label=r.get("label"),
            summary=r.get("description"),
            field_kind=r.get("field_kind"),
        )
        for sim, r in scored[:top_n]
    ]


def _suggest_pills_from_data_dictionary(
    intent: str, *, conn: Any, top_n: int = 12, min_score: int = 2,
    excluded_categories: frozenset[str] = _WORKFLOW_PILL_EXCLUDED_CATEGORIES,
) -> list[SuggestedPill]:
    """Surface candidate pills for the intent.

    Strategy: semantic search via pgvector when the embedding backend is
    available (Fix #2 — concept-level matching that recovers synonyms +
    rejects spurious token-overlap noise); fall back to token-overlap
    when the backend isn't available (e.g. semantic-backend container
    down). Both paths apply ``excluded_categories`` (Fix #1) so
    ``tool:*`` pills never reach workflow-compose.

    ``excluded_categories`` filters out object-kinds whose category isn't
    appropriate for workflow composition. Default excludes ``tool`` —
    MCP tool parameter pills are surfaced for their token overlap with
    the intent's docstring but they're the wrong KIND of pill for
    workflow-design. Callers in tool-invocation contexts can pass an
    empty set to disable the filter.
    """
    # Try semantic search first. Returns None when backend unavailable —
    # we fall back to token-overlap. Returns empty list when the corpus
    # was loaded but nothing scored above threshold OR the embed call
    # failed mid-flight (e.g. semantic-backend timeout). In the failure
    # case we ALSO fall back to token-overlap so the caller still gets
    # candidates; the failure is logged at warning level above.
    semantic_results = _suggest_pills_via_semantic(
        intent, conn=conn, top_n=top_n,
        excluded_categories=excluded_categories,
    )
    if semantic_results:
        return semantic_results

    # Fallback: token-overlap (the original heuristic)
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
        category = str(obj.get("category") or "").strip().lower()
        if category in excluded_categories:
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
