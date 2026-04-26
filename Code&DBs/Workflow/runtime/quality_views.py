"""Quality views: materialized rollups of dispatch quality metrics (Postgres)."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from storage.postgres import SyncPostgresConnection


class QualityWindow(Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"

@dataclass(frozen=True)
class AgentQualityProfile:
    agent_slug: str
    window: QualityWindow
    window_start: datetime
    dispatches: int
    successes: int
    failures: int
    pass_rate: float
    avg_cost: float
    avg_latency_seconds: float
    total_token_cost: float
    failure_codes: dict[str, int]
    failure_categories: dict[str, int] = field(default_factory=dict)
    adjusted_pass_rate: float = 0.0


@dataclass(frozen=True)
class FailureCatalogEntry:
    failure_code: str
    count: int
    last_seen: datetime
    example_job_labels: tuple[str, ...]
    owning_agents: tuple[str, ...]


@dataclass(frozen=True)
class QualityRollup:
    window: QualityWindow
    window_start: datetime
    total_workflows: int
    total_successes: int
    total_failures: int
    overall_pass_rate: float
    total_cost: float
    agent_profiles: tuple[AgentQualityProfile, ...]
    top_failures: tuple[FailureCatalogEntry, ...]
    adjusted_pass_rate: float = 0.0
    failure_zones: dict[str, int] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Internal mutable accumulator (not part of public API)
# ---------------------------------------------------------------------------

@dataclass
class _AgentAccum:
    dispatches: int = 0
    successes: int = 0
    failures: int = 0
    total_cost: float = 0.0
    total_latency: float = 0.0
    failure_codes: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    failure_categories: dict[str, int] = field(default_factory=lambda: defaultdict(int))


@dataclass
class _FailureAccum:
    count: int = 0
    last_seen: datetime | None = None
    examples: list[str] = field(default_factory=list)
    agents: set[str] = field(default_factory=set)


def load_failure_category_zones(
    conn: "SyncPostgresConnection",
    *,
    consumer: str,
) -> dict[str, str]:
    """Load the canonical failure-category zone map or fail explicitly."""

    try:
        rows = conn.execute(
            """
            SELECT category, zone
              FROM failure_category_zones
            """,
        )
    except Exception as exc:
        raise RuntimeError(f"failure_category_zones authority is required for {consumer}") from exc
    zone_map = {
        str(row["category"]): str(row["zone"])
        for row in rows or []
        if row.get("category")
    }
    if not zone_map:
        raise RuntimeError("failure_category_zones did not return any rows")
    return zone_map


class QualityViewMaterializer:
    """Accumulates workflow receipts and materializes quality rollups into Postgres."""

    _MAX_FAILURE_EXAMPLES = 5

    def __init__(self, conn: "SyncPostgresConnection") -> None:
        self._conn = conn
        self._zone_map = self._load_zone_map()

        # In-memory accumulators keyed by (window, window_start)
        # Each value maps agent_slug -> _AgentAccum
        self._agent_accums: dict[tuple[str, str], dict[str, _AgentAccum]] = defaultdict(
            lambda: defaultdict(_AgentAccum)
        )
        # failure_code -> _FailureAccum
        self._failure_accums: dict[str, _FailureAccum] = defaultdict(_FailureAccum)

    def _load_zone_map(self) -> dict[str, str]:
        return load_failure_category_zones(self._conn, consumer="quality views")

    # ------------------------------------------------------------------
    # Ingest
    # ------------------------------------------------------------------

    def ingest_receipt(self, receipt: dict) -> None:
        agent_slug: str = receipt["agent_slug"]
        status: str = receipt["status"]
        failure_code: str | None = receipt.get("failure_code")
        failure_category: str | None = receipt.get("failure_category")
        cost: float = receipt.get("cost", 0.0)
        latency: float = receipt.get("latency_seconds", 0.0)
        job_label: str = receipt.get("job_label", "")
        timestamp: str = receipt["timestamp"]

        ts = _parse_ts(timestamp)

        # Accumulate per window bucket
        for win in QualityWindow:
            ws = _window_start(win, ts)
            key = (win.value, ws.isoformat())
            accum = self._agent_accums[key][agent_slug]
            accum.dispatches += 1
            accum.total_cost += cost
            accum.total_latency += latency
            if status == "succeeded":
                accum.successes += 1
            else:
                accum.failures += 1
                if failure_code:
                    accum.failure_codes[failure_code] += 1
                if failure_category:
                    accum.failure_categories[failure_category] += 1

        # Failure catalog
        if status != "succeeded" and failure_code:
            fa = self._failure_accums[failure_code]
            fa.count += 1
            if fa.last_seen is None or ts > fa.last_seen:
                fa.last_seen = ts
            if job_label and len(fa.examples) < self._MAX_FAILURE_EXAMPLES:
                fa.examples.append(job_label)
            fa.agents.add(agent_slug)

    # ------------------------------------------------------------------
    # Materialize
    # ------------------------------------------------------------------

    def materialize(self, window: QualityWindow, window_start: datetime) -> QualityRollup:
        key = (window.value, window_start.isoformat())
        agent_map = self._agent_accums.get(key, {})

        profiles: list[AgentQualityProfile] = []
        total_d = total_s = total_f = 0
        total_cost = 0.0
        total_failure_zones: dict[str, int] = defaultdict(int)

        for slug, acc in sorted(agent_map.items()):
            pr = _accum_to_profile(slug, window, window_start, acc)
            profiles.append(pr)
            total_d += acc.dispatches
            total_s += acc.successes
            total_f += acc.failures
            total_cost += acc.total_cost
            for cat, cnt in acc.failure_categories.items():
                zone = self._zone_map.get(cat, "unknown")
                total_failure_zones[zone] += cnt

        top_failures = self._build_failure_entries()

        # Adjusted pass rate: exclude external failures from denominator
        external = total_failure_zones.get("external", 0)
        adj_denom = total_d - external
        adjusted_pass_rate = total_s / adj_denom if adj_denom > 0 else 0.0

        rollup = QualityRollup(
            window=window,
            window_start=window_start,
            total_workflows=total_d,
            total_successes=total_s,
            total_failures=total_f,
            overall_pass_rate=total_s / total_d if total_d else 0.0,
            total_cost=total_cost,
            agent_profiles=tuple(profiles),
            top_failures=tuple(top_failures),
            adjusted_pass_rate=round(adjusted_pass_rate, 4),
            failure_zones=dict(total_failure_zones),
        )

        # Persist
        self._write_rollup(rollup)
        for p in profiles:
            self._write_agent_profile(p)
        self._write_failure_catalog(top_failures)

        return rollup

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_rollup(self, window: QualityWindow, window_start: datetime) -> QualityRollup | None:
        row = self._conn.fetchrow(
            'SELECT data FROM quality_rollups WHERE "window" = $1 AND window_start = $2',
            window.value, window_start.isoformat(),
        )
        if row is None:
            return None
        return _rollup_from_json(_json_value(row["data"]))

    def get_agent_profile(
        self, agent_slug: str, window: QualityWindow, window_start: datetime
    ) -> AgentQualityProfile | None:
        row = self._conn.fetchrow(
            'SELECT data FROM agent_profiles WHERE agent_slug = $1 AND "window" = $2 AND window_start = $3',
            agent_slug, window.value, window_start.isoformat(),
        )
        if row is None:
            return None
        return _profile_from_json(_json_value(row["data"]))

    def get_failure_catalog(self, limit: int = 20) -> list[FailureCatalogEntry]:
        rows = self._conn.execute(
            "SELECT failure_code, count, last_seen, examples, agents FROM failure_catalog ORDER BY count DESC LIMIT $1",
            limit,
        )
        return [
            FailureCatalogEntry(
                failure_code=r["failure_code"],
                count=r["count"],
                last_seen=_parse_ts(r["last_seen"]),
                example_job_labels=tuple(_json_value(r["examples"])),
                owning_agents=tuple(_json_value(r["agents"])),
            )
            for r in rows
        ]

    def latest_rollup(self, window: QualityWindow) -> QualityRollup | None:
        row = self._conn.fetchrow(
            'SELECT data FROM quality_rollups WHERE "window" = $1 ORDER BY window_start DESC LIMIT 1',
            window.value,
        )
        if row is None:
            return None
        return _rollup_from_json(_json_value(row["data"]))

    # ------------------------------------------------------------------
    # Internal persistence helpers
    # ------------------------------------------------------------------

    def _write_rollup(self, rollup: QualityRollup) -> None:
        data = _rollup_to_json(rollup)
        self._conn.execute(
            """INSERT INTO quality_rollups ("window", window_start, data)
               VALUES ($1, $2, $3)
               ON CONFLICT ("window", window_start) DO UPDATE SET data = $3""",
            rollup.window.value, rollup.window_start.isoformat(), json.dumps(data),
        )

    def _write_agent_profile(self, profile: AgentQualityProfile) -> None:
        data = _profile_to_json(profile)
        self._conn.execute(
            """INSERT INTO agent_profiles (agent_slug, "window", window_start, data)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (agent_slug, "window", window_start) DO UPDATE SET data = $4""",
            profile.agent_slug, profile.window.value, profile.window_start.isoformat(), json.dumps(data),
        )

    def _write_failure_catalog(self, entries: list[FailureCatalogEntry]) -> None:
        for e in entries:
            self._conn.execute(
                """INSERT INTO failure_catalog (failure_code, count, last_seen, examples, agents)
                   VALUES ($1, $2, $3, $4, $5)
                   ON CONFLICT (failure_code) DO UPDATE SET
                       count = $2, last_seen = $3, examples = $4, agents = $5""",
                e.failure_code,
                e.count,
                e.last_seen,
                json.dumps(list(e.example_job_labels)),
                json.dumps(list(e.owning_agents)),
            )

    def _build_failure_entries(self) -> list[FailureCatalogEntry]:
        entries = []
        for code, fa in self._failure_accums.items():
            entries.append(
                FailureCatalogEntry(
                    failure_code=code,
                    count=fa.count,
                    last_seen=fa.last_seen or datetime.min,
                    example_job_labels=tuple(fa.examples),
                    owning_agents=tuple(sorted(fa.agents)),
                )
            )
        entries.sort(key=lambda e: e.count, reverse=True)
        return entries


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_ts(s) -> datetime:
    if isinstance(s, datetime):
        return s
    return datetime.fromisoformat(s)


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _window_start(window: QualityWindow, ts: datetime) -> datetime:
    if window == QualityWindow.HOURLY:
        return ts.replace(minute=0, second=0, microsecond=0)
    elif window == QualityWindow.DAILY:
        return ts.replace(hour=0, minute=0, second=0, microsecond=0)
    else:  # WEEKLY — Monday start
        day = ts.replace(hour=0, minute=0, second=0, microsecond=0)
        return day - timedelta(days=day.weekday())


def _accum_to_profile(
    slug: str, window: QualityWindow, window_start: datetime, acc: _AgentAccum
) -> AgentQualityProfile:
    external = sum(v for k, v in acc.failure_categories.items() if ZONE_MAP.get(k) == "external")
    adj_denom = acc.dispatches - external
    adjusted_pass_rate = acc.successes / adj_denom if adj_denom > 0 else 0.0
    return AgentQualityProfile(
        agent_slug=slug,
        window=window,
        window_start=window_start,
        dispatches=acc.dispatches,
        successes=acc.successes,
        failures=acc.failures,
        pass_rate=acc.successes / acc.dispatches if acc.dispatches else 0.0,
        avg_cost=acc.total_cost / acc.dispatches if acc.dispatches else 0.0,
        avg_latency_seconds=acc.total_latency / acc.dispatches if acc.dispatches else 0.0,
        total_token_cost=acc.total_cost,
        failure_codes=dict(acc.failure_codes),
        failure_categories=dict(acc.failure_categories),
        adjusted_pass_rate=round(adjusted_pass_rate, 4),
    )


# ---------------------------------------------------------------------------
# JSON serialization round-trip
# ---------------------------------------------------------------------------

def _profile_to_json(p: AgentQualityProfile) -> dict[str, Any]:
    return {
        "agent_slug": p.agent_slug,
        "window": p.window.value,
        "window_start": p.window_start.isoformat(),
        "dispatches": p.dispatches,
        "successes": p.successes,
        "failures": p.failures,
        "pass_rate": p.pass_rate,
        "adjusted_pass_rate": p.adjusted_pass_rate,
        "avg_cost": p.avg_cost,
        "avg_latency_seconds": p.avg_latency_seconds,
        "total_token_cost": p.total_token_cost,
        "failure_codes": p.failure_codes,
        "failure_categories": p.failure_categories,
    }


def _profile_from_json(d: dict[str, Any]) -> AgentQualityProfile:
    return AgentQualityProfile(
        agent_slug=d["agent_slug"],
        window=QualityWindow(d["window"]),
        window_start=datetime.fromisoformat(d["window_start"]),
        dispatches=d["dispatches"],
        successes=d["successes"],
        failures=d["failures"],
        pass_rate=d["pass_rate"],
        avg_cost=d["avg_cost"],
        avg_latency_seconds=d["avg_latency_seconds"],
        total_token_cost=d["total_token_cost"],
        failure_codes=d["failure_codes"],
        failure_categories=d.get("failure_categories", {}),
        adjusted_pass_rate=d.get("adjusted_pass_rate", 0.0),
    )


def _rollup_to_json(r: QualityRollup) -> dict[str, Any]:
    return {
        "window": r.window.value,
        "window_start": r.window_start.isoformat(),
        "total_workflows": r.total_workflows,
        "total_successes": r.total_successes,
        "total_failures": r.total_failures,
        "overall_pass_rate": r.overall_pass_rate,
        "adjusted_pass_rate": r.adjusted_pass_rate,
        "failure_zones": r.failure_zones,
        "total_cost": r.total_cost,
        "agent_profiles": [_profile_to_json(p) for p in r.agent_profiles],
        "top_failures": [
            {
                "failure_code": f.failure_code,
                "count": f.count,
                "last_seen": f.last_seen.isoformat(),
                "example_job_labels": list(f.example_job_labels),
                "owning_agents": list(f.owning_agents),
            }
            for f in r.top_failures
        ],
    }


def _rollup_from_json(d: dict[str, Any]) -> QualityRollup:
    return QualityRollup(
        window=QualityWindow(d["window"]),
        window_start=datetime.fromisoformat(d["window_start"]),
        total_workflows=d["total_workflows"],
        total_successes=d["total_successes"],
        total_failures=d["total_failures"],
        overall_pass_rate=d["overall_pass_rate"],
        total_cost=d["total_cost"],
        agent_profiles=tuple(_profile_from_json(p) for p in d["agent_profiles"]),
        adjusted_pass_rate=d.get("adjusted_pass_rate", 0.0),
        failure_zones=d.get("failure_zones", {}),
        top_failures=tuple(
            FailureCatalogEntry(
                failure_code=f["failure_code"],
                count=f["count"],
                last_seen=datetime.fromisoformat(f["last_seen"]),
                example_job_labels=tuple(f["example_job_labels"]),
                owning_agents=tuple(f["owning_agents"]),
            )
            for f in d["top_failures"]
        ),
    )
