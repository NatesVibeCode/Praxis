"""
Web dashboard data assembly layer.

Produces JSON-serializable dashboard payloads that static HTML pages
in surfaces/web/ can consume. This is NOT a running web server.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

__version__ = "0.1.0"


@dataclass(frozen=True)
class DashboardSection:
    name: str
    data: dict
    updated_at: datetime


@dataclass(frozen=True)
class DashboardPayload:
    sections: tuple[DashboardSection, ...]
    generated_at: datetime
    version: str


class DashboardAssembler:
    """Registers section builders and assembles them into a DashboardPayload."""

    def __init__(self) -> None:
        self._builders: dict[str, Callable[[], dict]] = {}

    def register(self, name: str, builder: Callable[[], dict]) -> None:
        self._builders[name] = builder

    def assemble(self) -> DashboardPayload:
        now = datetime.now(timezone.utc)
        sections = tuple(
            DashboardSection(name=name, data=builder(), updated_at=now)
            for name, builder in self._builders.items()
        )
        return DashboardPayload(
            sections=sections,
            generated_at=now,
            version=__version__,
        )

    @staticmethod
    def to_json(payload: DashboardPayload) -> str:
        def _default(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Not serializable: {type(obj)}")

        raw = asdict(payload)
        return json.dumps(raw, default=_default, indent=2)

    @staticmethod
    def write_to_file(payload: DashboardPayload, path: str) -> None:
        data = DashboardAssembler.to_json(payload)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(data)

    # ── Built-in section builders ──────────────────────────────

    @staticmethod
    def build_workflow_summary(receipts: list[dict]) -> dict:
        total = len(receipts)
        succeeded = sum(1 for r in receipts if r.get("status") == "success")
        failed = total - succeeded
        pass_rate = (succeeded / total * 100) if total else 0.0
        durations = [r["duration"] for r in receipts if "duration" in r]
        avg_duration = (sum(durations) / len(durations)) if durations else 0.0
        return {
            "total": total,
            "succeeded": succeeded,
            "failed": failed,
            "pass_rate": round(pass_rate, 2),
            "avg_duration": round(avg_duration, 2),
        }

    @staticmethod
    def build_agent_leaderboard(receipts: list[dict]) -> dict:
        agents: dict[str, dict] = {}
        for r in receipts:
            slug = r.get("agent", "unknown")
            if slug not in agents:
                agents[slug] = {
                    "dispatches": 0,
                    "successes": 0,
                    "total_cost": 0.0,
                    "total_latency": 0.0,
                }
            bucket = agents[slug]
            bucket["dispatches"] += 1
            if r.get("status") == "success":
                bucket["successes"] += 1
            bucket["total_cost"] += r.get("cost", 0.0)
            bucket["total_latency"] += r.get("duration", 0.0)

        leaderboard = {}
        for slug, b in agents.items():
            n = b["dispatches"]
            leaderboard[slug] = {
                "dispatches": n,
                "pass_rate": round(b["successes"] / n * 100, 2) if n else 0.0,
                "avg_cost": round(b["total_cost"] / n, 4) if n else 0.0,
                "avg_latency": round(b["total_latency"] / n, 2) if n else 0.0,
            }
        return leaderboard

    @staticmethod
    def build_circuit_breaker_status(breakers: dict[str, bool]) -> dict:
        return {provider: is_open for provider, is_open in breakers.items()}

    @staticmethod
    def build_recent_failures(receipts: list[dict], limit: int = 10) -> dict:
        failures = [r for r in receipts if r.get("status") != "success"]
        # Sort by timestamp descending if available
        failures.sort(key=lambda r: r.get("timestamp", ""), reverse=True)
        trimmed = failures[:limit]
        return {
            "count": len(failures),
            "failures": [
                {
                    "label": f.get("label", ""),
                    "code": f.get("code", ""),
                    "timestamp": f.get("timestamp", ""),
                }
                for f in trimmed
            ],
        }

    @staticmethod
    def build_cost_summary(receipts: list[dict]) -> dict:
        total_cost = sum(r.get("cost", 0.0) for r in receipts)

        cost_by_agent: dict[str, float] = {}
        for r in receipts:
            slug = r.get("agent", "unknown")
            cost_by_agent[slug] = cost_by_agent.get(slug, 0.0) + r.get("cost", 0.0)

        cost_by_day: dict[str, float] = {}
        for r in receipts:
            ts = r.get("timestamp", "")
            day = ts[:10] if len(ts) >= 10 else "unknown"
            cost_by_day[day] = cost_by_day.get(day, 0.0) + r.get("cost", 0.0)

        return {
            "total_cost": round(total_cost, 4),
            "cost_by_agent": {k: round(v, 4) for k, v in cost_by_agent.items()},
            "cost_by_day": {k: round(v, 4) for k, v in cost_by_day.items()},
        }


class LiveDataFeed:
    """Loads receipt JSON files from a directory and assembles dashboards."""

    def __init__(self, receipts_dir: str) -> None:
        self._receipts_dir = receipts_dir

    def load_receipts(self, since_hours: int = 24) -> list[dict]:
        dirpath = Path(self._receipts_dir)
        if not dirpath.is_dir():
            return []

        cutoff = datetime.now(timezone.utc).timestamp() - (since_hours * 3600)
        receipts = []
        for fp in dirpath.glob("*.json"):
            try:
                with open(fp) as f:
                    data = json.load(f)
                # Use file mtime as fallback for age filtering
                if fp.stat().st_mtime >= cutoff:
                    receipts.append(data)
            except (json.JSONDecodeError, OSError):
                continue
        return receipts

    def full_dashboard(self) -> DashboardPayload:
        receipts = self.load_receipts()
        asm = DashboardAssembler()
        asm.register("workflow_summary", lambda: DashboardAssembler.build_workflow_summary(receipts))
        asm.register("agent_leaderboard", lambda: DashboardAssembler.build_agent_leaderboard(receipts))
        asm.register("recent_failures", lambda: DashboardAssembler.build_recent_failures(receipts))
        asm.register("cost_summary", lambda: DashboardAssembler.build_cost_summary(receipts))
        return asm.assemble()

    def write_snapshot(self, output_path: str) -> None:
        payload = self.full_dashboard()
        DashboardAssembler.write_to_file(payload, output_path)
