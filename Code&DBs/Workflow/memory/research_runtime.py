"""Research runtime — search, accumulate findings, and compile briefs."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from memory.engine import MemoryEngine
from memory.retrieval_telemetry import RetrievalMetric, TelemetryStore
from memory.types import Entity, EntityType


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SearchHit:
    title: str
    url: str
    snippet: str
    source: str


@dataclass(frozen=True)
class SearchResult:
    query: str
    hits: tuple[SearchHit, ...]
    total_results: int


# ---------------------------------------------------------------------------
# Citation helper
# ---------------------------------------------------------------------------

class CitationHelper:
    """Format citations and bibliographies from search hits."""

    def format_citation(self, hit: SearchHit, index: int) -> str:
        return f"[{index}] {hit.title} — {hit.url}"

    def format_bibliography(self, hits: list[SearchHit]) -> str:
        lines = [self.format_citation(hit, i + 1) for i, hit in enumerate(hits)]
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Research executor
# ---------------------------------------------------------------------------

class ResearchExecutor:
    """Execute searches against the knowledge graph and record findings."""

    def __init__(self, engine: MemoryEngine | None = None) -> None:
        self._engine = engine
        self._telemetry_store: TelemetryStore | None = None

    def search_local(self, query: str) -> SearchResult:
        if self._engine is None:
            return SearchResult(query=query, hits=(), total_results=0)

        started_at = time.monotonic()
        entities = self._engine.search(query, limit=20)
        hits = tuple(
            SearchHit(
                title=e.name,
                url=f"entity://{e.entity_type.value}/{e.id}",
                snippet=e.content[:200] if e.content else "",
                source=e.source,
            )
            for e in entities
        )
        result = SearchResult(query=query, hits=hits, total_results=len(hits))
        self._record_telemetry(
            query=query,
            pattern_name="research.search_local",
            result_count=result.total_results,
            started_at=started_at,
        )
        return result

    def record_finding(self, query: str, finding: str, source: str) -> None:
        if self._engine is None:
            return
        now = datetime.now(timezone.utc)
        entity = Entity(
            id=f"finding-{hash((query, finding)) & 0xFFFFFFFF:08x}",
            entity_type=EntityType.fact,
            name=query[:120],
            content=finding,
            metadata={"query": query, "source": source},
            created_at=now,
            updated_at=now,
            source=source,
            confidence=0.7,
        )
        self._engine.insert(entity)

    def _record_telemetry(
        self,
        *,
        query: str,
        pattern_name: str,
        result_count: int,
        started_at: float,
    ) -> None:
        if self._engine is None:
            return
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

    def compile_brief(
        self,
        query: str,
        findings: list[str],
        max_tokens: int = 2000,
    ) -> str:
        header = f"# Research Brief: {query}\n\n"
        body_parts: list[str] = []
        char_budget = max_tokens * 4  # rough chars-per-token estimate

        for i, f in enumerate(findings, 1):
            entry = f"## Finding {i}\n{f}\n"
            if len(header) + sum(len(p) for p in body_parts) + len(entry) > char_budget:
                break
            body_parts.append(entry)

        return header + "\n".join(body_parts)


# ---------------------------------------------------------------------------
# Research session
# ---------------------------------------------------------------------------

class ResearchSession:
    """Accumulate findings for a topic and compile into a brief."""

    def __init__(self, executor: ResearchExecutor, topic: str) -> None:
        self._executor = executor
        self._topic = topic
        self._findings: list[dict] = []

    def add_finding(self, finding: str, source: str) -> None:
        self._findings.append({"finding": finding, "source": source})
        self._executor.record_finding(self._topic, finding, source)

    @property
    def findings(self) -> list[dict]:
        return list(self._findings)

    def compile(self) -> str:
        texts = [f["finding"] for f in self._findings]
        return self._executor.compile_brief(self._topic, texts)

    def save(self, path: str) -> None:
        payload = {
            "topic": self._topic,
            "findings": self._findings,
            "compiled": self.compile(),
        }
        with open(path, "w") as fh:
            json.dump(payload, fh, indent=2)
