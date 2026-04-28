"""Execution Constraint Ledger + Miner.

Mines recurring failure patterns from job stderr and stores them as
prompt-injectable constraints in a Postgres-backed ledger.
"""
from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, List, Optional, Tuple

from runtime.embedding_service import EmbeddingService
from storage.postgres.vector_store import PostgresVectorStore

if TYPE_CHECKING:
    from storage.postgres import SyncPostgresConnection


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MinedConstraint:
    constraint_id: str
    pattern: str
    constraint_text: str
    confidence: float
    mined_from_jobs: Tuple[str, ...]
    created_at: datetime


@dataclass(frozen=True)
class ConstraintWriteResult(MinedConstraint):
    merged: bool = False
    similarity: Optional[float] = None


# ---------------------------------------------------------------------------
# Ledger (Postgres-backed)
# ---------------------------------------------------------------------------

_TABLE = "workflow_constraints"
_NEAR_DUPLICATE_THRESHOLD = 0.88


class ConstraintLedger:
    """Postgres-backed store of mined execution constraints."""

    def __init__(
        self,
        conn: "SyncPostgresConnection",
        embedder: Optional[EmbeddingService] = None,
    ) -> None:
        if conn is None:
            raise ValueError(
                "ConstraintLedger requires a SyncPostgresConnection — SQLite is no longer supported"
        )
        self._conn = conn
        self._embedder = embedder
        self._vector_store = (
            PostgresVectorStore(conn, embedder) if embedder is not None else None
        )

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _jobs_to_str(jobs: Tuple[str, ...]) -> str:
        return ",".join(jobs)

    @staticmethod
    def _str_to_jobs(s: str) -> Tuple[str, ...]:
        return tuple(j for j in s.split(",") if j)

    @staticmethod
    def _merge_jobs(existing_jobs: str, new_jobs: Tuple[str, ...]) -> Tuple[str, ...]:
        merged: list[str] = []
        seen: set[str] = set()
        for job in ConstraintLedger._str_to_jobs(existing_jobs):
            if job not in seen:
                seen.add(job)
                merged.append(job)
        for job in new_jobs:
            if job and job not in seen:
                seen.add(job)
                merged.append(job)
        return tuple(merged)

    @staticmethod
    def _scope_prefix(source_jobs_paths: Optional[list] = None) -> str:
        if not source_jobs_paths:
            return ""
        parts = [p.split("/") for p in source_jobs_paths]
        prefix_parts: list[str] = []
        for segments in zip(*parts):
            if len(set(segments)) == 1:
                prefix_parts.append(segments[0])
            else:
                break
        return "/".join(prefix_parts)

    @staticmethod
    def _constraint_embed_text(pattern: str, constraint_text: str) -> str:
        return f"pattern: {pattern}\ndescription: {constraint_text}"

    @staticmethod
    def _scope_query_text(write_paths: List[str]) -> str:
        return "\n".join(write_paths)

    def _row_to_constraint(self, row) -> MinedConstraint:
        return MinedConstraint(
            constraint_id=row["constraint_id"],
            pattern=row["pattern"],
            constraint_text=row["constraint_text"],
            confidence=row["confidence"],
            mined_from_jobs=self._str_to_jobs(row["mined_from_jobs"]),
            created_at=row["created_at"] if isinstance(row["created_at"], datetime) else datetime.fromisoformat(row["created_at"]),
        )

    def _row_to_write_result(
        self,
        row,
        *,
        merged: bool = False,
        similarity: Optional[float] = None,
    ) -> ConstraintWriteResult:
        base = self._row_to_constraint(row)
        return ConstraintWriteResult(
            constraint_id=base.constraint_id,
            pattern=base.pattern,
            constraint_text=base.constraint_text,
            confidence=base.confidence,
            mined_from_jobs=base.mined_from_jobs,
            created_at=base.created_at,
            merged=merged,
            similarity=similarity,
        )

    # -- public API ----------------------------------------------------------

    def _merge_and_return(
        self,
        existing_id: str,
        confidence: float,
        source_jobs: Tuple[str, ...],
        similarity: Optional[float] = None,
    ) -> Optional[ConstraintWriteResult]:
        """Update an existing constraint with merged confidence + jobs."""
        existing_row = self._fetch_one(
            f"SELECT * FROM {_TABLE} WHERE constraint_id = $1", existing_id,
        )
        if existing_row is None:
            return None
        merged_jobs = self._merge_jobs(existing_row["mined_from_jobs"], source_jobs)
        self._conn.execute(
            f"UPDATE {_TABLE} SET confidence = GREATEST(confidence, $2), "
            f"mined_from_jobs = $3 WHERE constraint_id = $1",
            existing_id, confidence, self._jobs_to_str(merged_jobs),
        )
        updated = self._fetch_one(
            f"SELECT * FROM {_TABLE} WHERE constraint_id = $1", existing_id,
        )
        if updated is None:
            return None
        return self._row_to_write_result(updated, merged=True, similarity=similarity)

    def add(
        self,
        pattern: str,
        constraint_text: str,
        confidence: float,
        source_jobs: Tuple[str, ...],
        scope_prefix: str = "",
    ) -> ConstraintWriteResult:
        now = datetime.now(timezone.utc)

        # 1. Exact content dedup — always works, no embedder needed
        existing = self._fetch_one(
            f"SELECT constraint_id FROM {_TABLE} "
            "WHERE pattern = $1 AND constraint_text = $2 LIMIT 1",
            pattern, constraint_text,
        )
        if existing is not None:
            result = self._merge_and_return(
                existing["constraint_id"], confidence, source_jobs,
            )
            if result is not None:
                return result

        # 2. Vector similarity dedup — catches near-duplicates
        if self._vector_store is not None:
            duplicate_rows = []
            try:
                embed_text = self._constraint_embed_text(pattern, constraint_text)
                vector_query = self._vector_store.prepare(embed_text)
                duplicate_rows = vector_query.search(
                    _TABLE,
                    select_columns=("constraint_id", "pattern", "confidence"),
                    limit=3,
                    min_similarity=_NEAR_DUPLICATE_THRESHOLD,
                    score_alias="similarity",
                )
            except Exception:
                duplicate_rows = []
            if duplicate_rows:
                dup = duplicate_rows[0]
                similarity = float(dup["similarity"])
                if similarity >= _NEAR_DUPLICATE_THRESHOLD:
                    result = self._merge_and_return(
                        dup["constraint_id"], confidence, source_jobs, similarity,
                    )
                    if result is not None:
                        return result

        # 3. Insert new constraint
        cid = uuid.uuid4().hex[:12]
        self._conn.execute(
            f"INSERT INTO {_TABLE} "
            "(constraint_id, pattern, constraint_text, confidence, "
            "mined_from_jobs, scope_prefix, created_at) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7)",
            cid, pattern, constraint_text, confidence,
            self._jobs_to_str(source_jobs), scope_prefix, now,
        )
        if self._vector_store is not None:
            try:
                embed_text = self._constraint_embed_text(pattern, constraint_text)
                vq = self._vector_store.prepare(embed_text)
                vq.set_embedding(_TABLE, "constraint_id", cid)
            except Exception:
                pass
        return ConstraintWriteResult(
            constraint_id=cid,
            pattern=pattern,
            constraint_text=constraint_text,
            confidence=confidence,
            mined_from_jobs=source_jobs,
            created_at=now,
        )

    def deduplicate(self) -> int:
        """Collapse exact duplicates, keeping the oldest per (pattern, constraint_text)."""
        result = self._conn.execute(f"""
            WITH survivors AS (
                SELECT DISTINCT ON (pattern, constraint_text) constraint_id
                FROM {_TABLE}
                ORDER BY pattern, constraint_text, created_at
            )
            DELETE FROM {_TABLE}
            WHERE constraint_id NOT IN (SELECT constraint_id FROM survivors)
            RETURNING constraint_id
        """)
        return len(result or [])

    def get_for_scope(self, write_paths: List[str]) -> List[MinedConstraint]:
        if not write_paths:
            return []
        results: dict[str, MinedConstraint] = {}
        global_rows = self._fetch_all(f"SELECT * FROM {_TABLE} WHERE scope_prefix = ''")
        for row in global_rows:
            mc = self._row_to_constraint(row)
            results[mc.constraint_id] = mc
        for wp in write_paths:
            scoped_rows = self._fetch_all(
                f"SELECT * FROM {_TABLE} "
                "WHERE scope_prefix != '' AND $1 LIKE scope_prefix || '%'",
                wp,
            )
            for row in scoped_rows:
                mc = self._row_to_constraint(row)
                results[mc.constraint_id] = mc
        if self._vector_store is not None:
            try:
                query_text = self._scope_query_text(write_paths)
                vector_query = self._vector_store.prepare(query_text)
                for row in vector_query.search(
                    _TABLE,
                    select_columns=("*",),
                    limit=3,
                    min_similarity=_NEAR_DUPLICATE_THRESHOLD,
                    score_alias="similarity",
                ):
                    mc = self._row_to_constraint(row)
                    results[mc.constraint_id] = mc
            except Exception:
                pass
        return list(results.values())

    def inject_into_prompt(self, prompt: str, write_paths: List[str]) -> str:
        constraints = self.get_for_scope(write_paths)
        if not constraints:
            return prompt
        lines = ["\n\n## LEARNED CONSTRAINTS"]
        for c in constraints:
            lines.append(f"- [{c.pattern}] {c.constraint_text}")
        return prompt + "\n".join(lines)

    def list_all(self, min_confidence: float = 0.5) -> List[MinedConstraint]:
        rows = self._fetch_all(
            f"SELECT * FROM {_TABLE} WHERE confidence >= $1 ORDER BY created_at DESC",
            min_confidence,
        )
        return [self._row_to_constraint(r) for r in rows]

    # -- internal query helpers -----------------------------------------------

    def _fetch_all(self, query: str, *params):
        return self._conn.execute(query, *params)

    def _fetch_one(self, query: str, *params):
        rows = self._fetch_all(query, *params)
        return next(iter(rows), None)


# ---------------------------------------------------------------------------
# Miner
# ---------------------------------------------------------------------------

_FAILURE_RULES: list[tuple[re.Pattern, str, str, float]] = [
    (
        re.compile(r"ImportError|ModuleNotFoundError"),
        "ImportError",
        "MUST include all required imports at top of file",
        0.9,
    ),
    (
        re.compile(r"SyntaxError"),
        "SyntaxError",
        "MUST produce valid Python syntax. Run a mental syntax check before outputting.",
        0.9,
    ),
    (
        re.compile(r"IndentationError"),
        "IndentationError",
        "MUST use consistent 4-space indentation throughout",
        0.85,
    ),
    (
        re.compile(r"FileNotFoundError"),
        "FileNotFoundError",
        "MUST verify target paths exist before writing",
        0.8,
    ),
    (
        re.compile(r"AssertionError"),
        "AssertionError",
        "MUST ensure all test assertions match actual behavior",
        0.75,
    ),
]


class ConstraintMiner:
    """Pattern-match common failures into injectable constraints."""

    def mine(
        self,
        failure_code: str,
        stderr: str,
        job_label: str,
        write_paths: List[str],
    ) -> Optional[MinedConstraint]:
        combined = f"{failure_code}\n{stderr}"
        for regex, pattern_name, constraint_text, confidence in _FAILURE_RULES:
            if regex.search(combined):
                cid = uuid.uuid4().hex[:12]
                now = datetime.now(timezone.utc)
                return MinedConstraint(
                    constraint_id=cid,
                    pattern=pattern_name,
                    constraint_text=constraint_text,
                    confidence=confidence,
                    mined_from_jobs=(job_label,),
                    created_at=now,
                )
        return None
