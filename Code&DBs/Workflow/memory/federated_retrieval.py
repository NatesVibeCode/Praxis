"""Federated retrieval router — classify queries by domain and route searches."""

from __future__ import annotations

import hashlib
import enum
import re
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone

from memory.engine import MemoryEngine
from memory.retrieval_telemetry import RetrievalMetric, TelemetryStore
from memory.types import EntityType


# ---------------------------------------------------------------------------
# Domain enum
# ---------------------------------------------------------------------------

class RetrievalDomain(enum.Enum):
    PLANNING = "planning"
    OPS = "ops"
    RESEARCH = "research"
    GENERAL = "general"


# ---------------------------------------------------------------------------
# Domain node
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DomainNode:
    domain: RetrievalDomain
    entity_types: tuple[str, ...]
    keywords: tuple[str, ...]
    description: str


_DEFAULT_DOMAINS: list[DomainNode] = [
    DomainNode(
        domain=RetrievalDomain.PLANNING,
        entity_types=("decision", "constraint", "task", "workstream"),
        keywords=("plan", "decide", "roadmap", "milestone", "phase"),
        description="Planning, decisions, constraints, and roadmap items",
    ),
    DomainNode(
        domain=RetrievalDomain.OPS,
        entity_types=("module", "tool", "pattern"),
        keywords=("deploy", "build", "test", "run", "dispatch", "fix"),
        description="Operations, modules, tools, and deployment patterns",
    ),
    DomainNode(
        domain=RetrievalDomain.RESEARCH,
        entity_types=("fact", "lesson", "document"),
        keywords=("research", "learn", "analyze", "study", "find"),
        description="Research findings, lessons learned, and documents",
    ),
]

_TOKENIZE_RE = re.compile(r"[a-z0-9]+")


def _tokenize(text: str) -> list[str]:
    return _TOKENIZE_RE.findall(text.lower())


# ---------------------------------------------------------------------------
# Query intent
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class QueryIntent:
    query: str
    matched_domain: RetrievalDomain
    confidence: float
    matched_keywords: tuple[str, ...]


# ---------------------------------------------------------------------------
# Federated retriever
# ---------------------------------------------------------------------------

class FederatedRetriever:
    """Classify queries by domain and route searches to matching entity types."""

    def __init__(
        self,
        engine: MemoryEngine,
        domains: list[DomainNode] | None = None,
    ) -> None:
        self._engine = engine
        self._domains = domains if domains is not None else list(_DEFAULT_DOMAINS)
        self._telemetry_store: TelemetryStore | None = None

    def classify(self, query: str) -> QueryIntent:
        tokens = set(_tokenize(query))
        best_domain = RetrievalDomain.GENERAL
        best_score = 0.0
        best_keywords: tuple[str, ...] = ()

        for node in self._domains:
            matched = tuple(kw for kw in node.keywords if kw in tokens)
            if not matched:
                continue
            score = len(matched) / len(node.keywords)
            if score > best_score:
                best_score = score
                best_domain = node.domain
                best_keywords = matched

        return QueryIntent(
            query=query,
            matched_domain=best_domain,
            confidence=best_score,
            matched_keywords=best_keywords,
        )

    def _domain_node(self, domain: RetrievalDomain) -> DomainNode | None:
        for node in self._domains:
            if node.domain == domain:
                return node
        return None

    def search(
        self,
        query: str,
        limit: int = 20,
        *,
        record_telemetry: bool = True,
    ) -> list:
        started_at = time.monotonic()
        intent = self.classify(query)
        node = self._domain_node(intent.matched_domain)
        if node is None:
            # GENERAL fallback — search without type filter
            results = self._engine.search(query, limit=limit)
            if record_telemetry:
                self._record_telemetry(
                    query=query,
                    pattern_name="federated.search",
                    result_count=len(results),
                    started_at=started_at,
                )
            return results

        results: list = []
        per_type = max(1, limit // len(node.entity_types))
        for et_name in node.entity_types:
            try:
                et = EntityType(et_name)
            except ValueError:
                continue
            hits = self._engine.search(query, entity_type=et, limit=per_type)
            results.extend(hits)
        results = results[:limit]
        if record_telemetry:
            self._record_telemetry(
                query=query,
                pattern_name="federated.search",
                result_count=len(results),
                started_at=started_at,
            )
        return results

    def search_all_domains(
        self, query: str, limit_per_domain: int = 5
    ) -> dict[str, list]:
        started_at = time.monotonic()
        out: dict[str, list] = {}
        for node in self._domains:
            domain_results: list = []
            per_type = max(1, limit_per_domain // len(node.entity_types))
            for et_name in node.entity_types:
                try:
                    et = EntityType(et_name)
                except ValueError:
                    continue
                hits = self._engine.search(query, entity_type=et, limit=per_type)
                domain_results.extend(hits)
            out[node.domain.value] = domain_results[:limit_per_domain]
        self._record_telemetry(
            query=query,
            pattern_name="federated.search_all_domains",
            result_count=sum(len(results) for results in out.values()),
            started_at=started_at,
        )
        return out

    def _record_telemetry(
        self,
        *,
        query: str,
        pattern_name: str,
        result_count: int,
        started_at: float,
    ) -> None:
        try:
            store = self._telemetry_store
            if store is None:
                store = TelemetryStore(self._engine._connect())
                self._telemetry_store = store
            store.record(
                RetrievalMetric(
                    query_fingerprint=hashlib.sha256(query.encode()).hexdigest()[:8],
                    pattern_name=pattern_name,
                    result_count=result_count,
                    score_min=0.0,
                    score_max=0.0,
                    score_mean=0.0,
                    score_stddev=0.0,
                    tie_break_count=0,
                    latency_ms=(time.monotonic() - started_at) * 1000.0,
                    timestamp=datetime.now(timezone.utc),
                )
            )
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Path predictor
# ---------------------------------------------------------------------------

class PathPredictor:
    """Predict which entity_types are most relevant for a query."""

    def __init__(self, domains: list[DomainNode] | None = None) -> None:
        self._domains = domains if domains is not None else list(_DEFAULT_DOMAINS)

    def predict(
        self, query: str, recent_queries: list[str] | None = None
    ) -> list[str]:
        tokens = set(_tokenize(query))

        # Also fold in recent query context
        if recent_queries:
            for rq in recent_queries:
                tokens.update(_tokenize(rq))

        scored: Counter[str] = Counter()
        for node in self._domains:
            overlap = sum(1 for kw in node.keywords if kw in tokens)
            if overlap == 0:
                continue
            weight = overlap / len(node.keywords)
            for et in node.entity_types:
                scored[et] += weight

        if not scored:
            # Return a generic fallback ordering
            return ["fact", "task", "document"]

        return [et for et, _ in scored.most_common()]
