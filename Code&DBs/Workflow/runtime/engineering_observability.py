"""Merged engineering observability authority for bug, code, and platform hotspots.

This module is the operator-facing authority for answering:
  - Which files/modules are currently risky?
  - Which bugs are recurring, under-observed, or ready to replay?
  - Which platform probes are degraded right now?

The goal is to expose one obvious source of truth for engineering problem areas
without forcing callers to know about the static health map, receipt risk
scorer, bug packets, or platform health payload shapes individually.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from .health_map import HealthMapper
from .primitive_contracts import bug_open_status_values
from .risk_scoring import RiskScorer
from .trend_detector import TrendDetector, format_trends

_OPEN_BUG_STATUSES: frozenset[str] = frozenset(bug_open_status_values())

DEFAULT_SCAN_ROOTS: tuple[str, ...] = ("runtime", "surfaces/api", "surfaces/cli")
DEFAULT_BUG_PACKET_LIMIT = 100


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _source_payload(*, available: bool, mode: str, detail: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "available": available,
        "mode": mode,
    }
    if detail:
        payload["detail"] = detail
    return payload


def _trend_to_dict(trend: Any) -> dict[str, Any]:
    return {
        "metric_name": str(getattr(trend, "metric_name", "") or ""),
        "provider_slug": str(getattr(trend, "provider_slug", "") or ""),
        "direction": str(getattr(getattr(trend, "direction", None), "value", getattr(trend, "direction", "")) or ""),
        "baseline_value": float(getattr(trend, "baseline_value", 0.0) or 0.0),
        "current_value": float(getattr(trend, "current_value", 0.0) or 0.0),
        "change_pct": float(getattr(trend, "change_pct", 0.0) or 0.0),
        "sample_count": int(getattr(trend, "sample_count", 0) or 0),
        "severity": str(getattr(trend, "severity", "") or ""),
    }


def _normalize_roots(roots: Sequence[str] | None) -> tuple[str, ...]:
    if not roots:
        return DEFAULT_SCAN_ROOTS
    normalized: list[str] = []
    for root in roots:
        text = str(root or "").strip().strip("/")
        if text:
            normalized.append(text)
    return tuple(normalized) or DEFAULT_SCAN_ROOTS


def _normalize_relative_path(path: str | Path, repo_root: Path) -> str:
    candidate = Path(path)
    if candidate.is_absolute():
        try:
            return candidate.resolve().relative_to(repo_root.resolve()).as_posix()
        except ValueError:
            return candidate.as_posix()
    return candidate.as_posix().lstrip("./")


def _is_test_path(path: str | Path) -> bool:
    candidate = Path(path)
    name = candidate.name.lower()
    parts = {part.lower() for part in candidate.parts}
    if "tests" in parts or "test" in parts:
        return True
    return (
        name.startswith("test_")
        or name.endswith("_test.py")
        or name.endswith("_tests.py")
    )


def _component_for_path(path: str) -> str:
    parts = Path(path).parts
    if not parts:
        return "unknown"
    if parts[0] == "surfaces" and len(parts) >= 2:
        return "/".join(parts[:2])
    if parts[0] == "tests" and len(parts) >= 2:
        return "/".join(parts[:2])
    return parts[0]


def _weighted_average(pairs: Iterable[tuple[float | None, float]]) -> float:
    total_weight = 0.0
    weighted_sum = 0.0
    for value, weight in pairs:
        if value is None:
            continue
        total_weight += weight
        weighted_sum += float(value) * weight
    if total_weight <= 0:
        return 0.0
    return round(weighted_sum / total_weight, 2)


def _status_value(value: Any) -> str:
    return str(getattr(value, "value", value) or "").strip()


def _coerce_mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _coerce_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _ordered_unique_paths(paths: Iterable[Any], repo_root: Path) -> tuple[str, ...]:
    ordered: list[str] = []
    seen: set[str] = set()
    for raw in paths:
        text = str(raw or "").strip()
        if not text:
            continue
        normalized = _normalize_relative_path(text, repo_root)
        if not normalized or _is_test_path(normalized) or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return tuple(ordered)


def _extract_bug_paths(packet: dict[str, Any], repo_root: Path) -> tuple[str, ...]:
    paths: list[Any] = []
    receipts = [
        packet.get("latest_receipt"),
        *_coerce_list(packet.get("recent_receipts")),
        *_coerce_list(packet.get("fallback_receipts")),
    ]
    for receipt in receipts:
        receipt_map = _coerce_mapping(receipt)
        paths.extend(_coerce_list(receipt_map.get("write_paths")))
        paths.extend(_coerce_list(receipt_map.get("verified_paths")))
    minimal_repro = _coerce_mapping(packet.get("minimal_repro"))
    paths.extend(_coerce_list(minimal_repro.get("write_paths")))
    write_set_diff = _coerce_mapping(packet.get("write_set_diff"))
    for key in ("added_paths", "unchanged_paths", "removed_paths"):
        paths.extend(_coerce_list(write_set_diff.get(key)))
    return _ordered_unique_paths(paths, repo_root)


def _dominant_label(counter: Counter[str]) -> str | None:
    if not counter:
        return None
    return sorted(counter.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _bug_list_item(
    bug: Any,
    packet: dict[str, Any],
    *,
    repo_root: Path,
) -> dict[str, Any]:
    lifecycle = _coerce_mapping(packet.get("lifecycle"))
    replay_context = _coerce_mapping(packet.get("replay_context"))
    fix_verification = _coerce_mapping(packet.get("fix_verification"))
    observability_gaps = tuple(str(item) for item in _coerce_list(packet.get("observability_gaps")))
    paths = _extract_bug_paths(packet, repo_root)
    recurrence_count = int(lifecycle.get("recurrence_count") or 1)
    impacted_run_count = int(lifecycle.get("impacted_run_count") or 0)
    has_regression_after_fix = bool(lifecycle.get("has_regression_after_fix"))
    fix_verified = bool(fix_verification.get("fix_verified"))
    replay_ready = bool(replay_context.get("ready"))
    return {
        "bug_id": str(getattr(bug, "bug_id", "") or ""),
        "title": str(getattr(bug, "title", "") or ""),
        "severity": _status_value(getattr(bug, "severity", "")) or "unknown",
        "status": _status_value(getattr(bug, "status", "")) or "unknown",
        "category": _status_value(getattr(bug, "category", "")) or "unknown",
        "source_issue_id": str(getattr(bug, "source_issue_id", "") or "").strip() or None,
        "recurrence_count": recurrence_count,
        "impacted_run_count": impacted_run_count,
        "has_regression_after_fix": has_regression_after_fix,
        "replay_ready": replay_ready,
        "fix_verified": fix_verified,
        "observability_state": str(packet.get("observability_state") or "unknown"),
        "observability_gaps": observability_gaps,
        "file_paths": list(paths),
    }


def build_code_hotspots(
    *,
    repo_root: str | Path,
    bug_tracker: Any | None = None,
    limit: int = 20,
    roots: Sequence[str] | None = None,
    path_prefix: str | None = None,
    bug_packet_limit: int = DEFAULT_BUG_PACKET_LIMIT,
) -> dict[str, Any]:
    repo_root_path = Path(repo_root).resolve()
    normalized_roots = _normalize_roots(roots)
    normalized_prefix = str(path_prefix or "").strip().strip("/")
    sources: dict[str, dict[str, Any]] = {}

    health_by_path: dict[str, Any] = {}
    static_errors: list[str] = []
    for root in normalized_roots:
        root_path = repo_root_path / root
        if not root_path.exists():
            continue
        try:
            modules = HealthMapper().analyze_directory(str(root_path))
            for module in modules:
                rel_path = _normalize_relative_path(module.module_path, repo_root_path)
                if _is_test_path(rel_path):
                    continue
                health_by_path[rel_path] = module
        except Exception as exc:
            static_errors.append(f"{root}: {type(exc).__name__}: {exc}")
    sources["static_health"] = _source_payload(
        available=bool(health_by_path),
        mode="live" if health_by_path else "unavailable",
        detail="; ".join(static_errors) if static_errors else None,
    )

    risk_by_path: dict[str, Any] = {}
    risk_error: str | None = None
    try:
        scorer = RiskScorer()
        for score in scorer.compute_from_receipts():
            rel_path = _normalize_relative_path(score.file_path, repo_root_path)
            if rel_path and not _is_test_path(rel_path):
                risk_by_path[rel_path] = score
    except Exception as exc:
        risk_error = f"{type(exc).__name__}: {exc}"
    sources["receipt_risk"] = _source_payload(
        available=bool(risk_by_path),
        mode="live" if risk_by_path else "unavailable",
        detail=risk_error,
    )

    bug_rollups: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "bug_ids": set(),
            "open_bug_count": 0,
            "regression_count": 0,
            "replay_ready_bug_count": 0,
            "under_observed_bug_count": 0,
            "fix_verified_bug_count": 0,
            "recurrence_count": 0,
            "severity_counts": Counter(),
        }
    )
    unscoped_bug_count = 0
    bug_sample_count = 0
    bug_error: str | None = None
    if bug_tracker is not None:
        try:
            bugs = bug_tracker.list_bugs(limit=max(limit, bug_packet_limit))
            bug_sample_count = len(bugs)
            for bug in bugs:
                packet = bug_tracker.failure_packet(
                    bug.bug_id,
                    receipt_limit=1,
                    allow_backfill=False,
                ) or {}
                item = _bug_list_item(bug, packet, repo_root=repo_root_path)
                paths = item["file_paths"]
                if not paths:
                    unscoped_bug_count += 1
                    continue
                for file_path in paths:
                    rollup = bug_rollups[file_path]
                    rollup["bug_ids"].add(item["bug_id"])
                    if item["status"] in _OPEN_BUG_STATUSES:
                        rollup["open_bug_count"] += 1
                    if item["has_regression_after_fix"]:
                        rollup["regression_count"] += 1
                    if item["replay_ready"]:
                        rollup["replay_ready_bug_count"] += 1
                    if item["observability_state"] != "complete" or item["observability_gaps"]:
                        rollup["under_observed_bug_count"] += 1
                    if item["fix_verified"]:
                        rollup["fix_verified_bug_count"] += 1
                    rollup["recurrence_count"] += int(item["recurrence_count"])
                    rollup["severity_counts"][item["severity"]] += 1
        except Exception as exc:
            bug_error = f"{type(exc).__name__}: {exc}"
    sources["bug_packets"] = _source_payload(
        available=bool(bug_rollups),
        mode="live" if bug_rollups else ("unavailable" if bug_tracker is None else "degraded"),
        detail=bug_error,
    )

    candidate_paths = set(health_by_path) | set(risk_by_path) | set(bug_rollups)
    if normalized_prefix:
        candidate_paths = {
            path for path in candidate_paths if path == normalized_prefix or path.startswith(f"{normalized_prefix}/")
        }

    file_rows: list[dict[str, Any]] = []
    for rel_path in sorted(candidate_paths):
        health = health_by_path.get(rel_path)
        risk = risk_by_path.get(rel_path)
        bug_rollup = bug_rollups.get(rel_path, {})
        static_health_raw = float(getattr(health, "health_score", 0)) if health is not None else None
        static_health_score = (
            min(round(static_health_raw * 2.5, 2), 100.0)
            if static_health_raw is not None
            else None
        )
        risk_score = round(float(getattr(risk, "risk_score", 0.0)), 2) if risk is not None else None
        bug_count = len(bug_rollup.get("bug_ids", set()))
        open_bug_count = int(bug_rollup.get("open_bug_count", 0))
        regression_count = int(bug_rollup.get("regression_count", 0))
        recurrence_count = int(bug_rollup.get("recurrence_count", 0))
        replay_ready_bug_count = int(bug_rollup.get("replay_ready_bug_count", 0))
        under_observed_bug_count = int(bug_rollup.get("under_observed_bug_count", 0))
        bug_pressure = None
        if bug_count or open_bug_count or regression_count or recurrence_count:
            bug_pressure = min(
                100.0,
                open_bug_count * 18.0
                + regression_count * 14.0
                + under_observed_bug_count * 6.0
                + max(recurrence_count - bug_count, 0) * 2.0
                + replay_ready_bug_count * 4.0,
            )
        hotspot_score = _weighted_average(
            (
                (static_health_score, 0.35),
                (risk_score, 0.35),
                (bug_pressure, 0.30),
            )
        )
        signals: list[str] = []
        if open_bug_count:
            signals.append(f"{open_bug_count} open bug(s)")
        if regression_count:
            signals.append(f"{regression_count} regression(s)")
        if recurrence_count > bug_count:
            signals.append(f"{recurrence_count} observed recurrences")
        if under_observed_bug_count:
            signals.append(f"{under_observed_bug_count} under-observed bug(s)")
        if risk_score and risk_score >= 50:
            signals.append(f"receipt risk {risk_score:.1f}")
        if static_health_raw and static_health_raw >= 20:
            signals.append(f"static health {int(static_health_raw)}")
        severity_counts = bug_rollup.get("severity_counts") or Counter()
        file_rows.append(
            {
                "file_path": rel_path,
                "component": _component_for_path(rel_path),
                "hotspot_score": hotspot_score,
                "static_health_score": static_health_score,
                "static_health_raw": static_health_raw,
                "risk_score": risk_score,
                "open_bug_count": open_bug_count,
                "bug_count": bug_count,
                "regression_count": regression_count,
                "recurrence_count": recurrence_count,
                "replay_ready_bug_count": replay_ready_bug_count,
                "under_observed_bug_count": under_observed_bug_count,
                "fix_verified_bug_count": int(bug_rollup.get("fix_verified_bug_count", 0)),
                "dominant_severity": _dominant_label(severity_counts),
                "touch_count": int(getattr(risk, "touch_count", 0) or 0) if risk is not None else None,
                "success_rate": round(float(getattr(risk, "success_rate", 0.0)), 4) if risk is not None else None,
                "avg_duration_ms": int(getattr(risk, "avg_duration_ms", 0) or 0) if risk is not None else None,
                "failure_codes": list(getattr(risk, "failure_codes", ()) or ()),
                "last_touched": (
                    getattr(risk, "last_touched", None).isoformat()
                    if getattr(risk, "last_touched", None) is not None
                    else None
                ),
                "line_count": int(getattr(health, "line_count", 0) or 0) if health is not None else None,
                "function_count": int(getattr(health, "function_count", 0) or 0) if health is not None else None,
                "signals": signals,
            }
        )

    file_rows.sort(
        key=lambda item: (
            -float(item.get("hotspot_score") or 0.0),
            -int(item.get("open_bug_count") or 0),
            -int(item.get("recurrence_count") or 0),
            item.get("file_path") or "",
        )
    )

    component_rollups: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "component": "",
            "file_count": 0,
            "hotspot_score_total": 0.0,
            "open_bug_count": 0,
            "bug_count": 0,
            "regression_count": 0,
        }
    )
    for row in file_rows:
        component = str(row.get("component") or "unknown")
        comp = component_rollups[component]
        comp["component"] = component
        comp["file_count"] += 1
        comp["hotspot_score_total"] += float(row.get("hotspot_score") or 0.0)
        comp["open_bug_count"] += int(row.get("open_bug_count") or 0)
        comp["bug_count"] += int(row.get("bug_count") or 0)
        comp["regression_count"] += int(row.get("regression_count") or 0)

    components = [
        {
            "component": component,
            "score": round(values["hotspot_score_total"] / max(values["file_count"], 1), 2),
            "file_count": values["file_count"],
            "open_bug_count": values["open_bug_count"],
            "bug_count": values["bug_count"],
            "regression_count": values["regression_count"],
        }
        for component, values in component_rollups.items()
    ]
    components.sort(
        key=lambda item: (
            -float(item.get("score") or 0.0),
            -int(item.get("open_bug_count") or 0),
            item.get("component") or "",
        )
    )

    linked_bug_ids = {
        bug_id
        for values in bug_rollups.values()
        for bug_id in values.get("bug_ids", set())
    }
    return {
        "authority": "runtime.engineering_observability.build_code_hotspots",
        "generated_at": _utcnow().isoformat(),
        "filters": {
            "roots": list(normalized_roots),
            "path_prefix": normalized_prefix or None,
            "limit": limit,
        },
        "sources": sources,
        "summary": {
            "total_files": len(file_rows),
            "returned_files": min(limit, len(file_rows)),
            "total_components": len(components),
            "linked_bug_count": len(linked_bug_ids),
            "unscoped_bug_count": unscoped_bug_count,
            "sampled_bug_count": bug_sample_count,
        },
        "components": components[: max(limit, 10)],
        "files": file_rows[:limit],
    }


def build_bug_scoreboard(
    *,
    bug_tracker: Any | None = None,
    limit: int = 20,
    packet_limit: int = DEFAULT_BUG_PACKET_LIMIT,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    repo_root_path = Path(repo_root).resolve() if repo_root is not None else Path.cwd().resolve()
    sources: dict[str, dict[str, Any]] = {}
    if bug_tracker is None:
        return {
            "authority": "runtime.engineering_observability.build_bug_scoreboard",
            "generated_at": _utcnow().isoformat(),
            "sources": {"bug_tracker": _source_payload(available=False, mode="unavailable")},
            "summary": {
                "total_bugs": 0,
                "open_bugs": 0,
                "packet_ready_bugs": 0,
                "replay_ready_bugs": 0,
                "replay_blocked_bugs": 0,
                "fix_verified_bugs": 0,
                "underlinked_bugs": 0,
                "regression_bugs": 0,
                "degraded_packets": 0,
                "sampled_bugs": 0,
                "mttr_hours": None,
                "observability_state": "unavailable",
            },
            "by_status": {},
            "by_severity": {},
            "by_category": {},
            "top_recurring": [],
            "regressions": [],
            "under_observed": [],
            "replay_ready": [],
        }

    try:
        stats = bug_tracker.stats()
    except Exception as exc:
        return {
            "authority": "runtime.engineering_observability.build_bug_scoreboard",
            "generated_at": _utcnow().isoformat(),
            "sources": {
                "bug_tracker": _source_payload(
                    available=False,
                    mode="unavailable",
                    detail=f"{type(exc).__name__}: {exc}",
                )
            },
            "summary": {
                "total_bugs": 0,
                "open_bugs": 0,
                "packet_ready_bugs": 0,
                "replay_ready_bugs": 0,
                "replay_blocked_bugs": 0,
                "fix_verified_bugs": 0,
                "underlinked_bugs": 0,
                "regression_bugs": 0,
                "degraded_packets": 0,
                "sampled_bugs": 0,
                "mttr_hours": None,
                "observability_state": "unavailable",
            },
            "by_status": {},
            "by_severity": {},
            "by_category": {},
            "top_recurring": [],
            "regressions": [],
            "under_observed": [],
            "replay_ready": [],
        }
    sources["bug_tracker"] = _source_payload(
        available=True,
        mode="live",
        detail="; ".join(getattr(stats, "errors", ()) or ()) or None,
    )

    sampled_bugs = bug_tracker.list_bugs(open_only=False, limit=max(limit * 3, packet_limit))
    bug_items: list[dict[str, Any]] = []
    packet_errors: list[str] = []
    for bug in sampled_bugs:
        try:
            packet = bug_tracker.failure_packet(
                bug.bug_id,
                receipt_limit=1,
                allow_backfill=False,
            ) or {}
        except Exception as exc:
            packet_errors.append(f"{bug.bug_id}: {type(exc).__name__}: {exc}")
            continue
        bug_items.append(_bug_list_item(bug, packet, repo_root=repo_root_path))

    regressions = [item for item in bug_items if item["has_regression_after_fix"]]
    under_observed = [
        item
        for item in bug_items
        if item["observability_state"] != "complete" or item["observability_gaps"]
    ]
    replay_ready = [item for item in bug_items if item["replay_ready"]]

    top_recurring = sorted(
        bug_items,
        key=lambda item: (
            -int(item.get("recurrence_count") or 0),
            -int(item.get("impacted_run_count") or 0),
            item.get("title") or "",
        ),
    )
    regressions.sort(
        key=lambda item: (
            -int(item.get("recurrence_count") or 0),
            item.get("title") or "",
        )
    )
    under_observed.sort(
        key=lambda item: (
            -len(item.get("observability_gaps") or ()),
            -int(item.get("recurrence_count") or 0),
            item.get("title") or "",
        ),
    )
    replay_ready.sort(
        key=lambda item: (
            -int(item.get("recurrence_count") or 0),
            item.get("title") or "",
        )
    )

    if packet_errors:
        sources["bug_packets"] = _source_payload(
            available=bool(bug_items),
            mode="degraded",
            detail="; ".join(packet_errors[:5]),
        )
    else:
        sources["bug_packets"] = _source_payload(available=bool(bug_items), mode="live")

    return {
        "authority": "runtime.engineering_observability.build_bug_scoreboard",
        "generated_at": _utcnow().isoformat(),
        "sources": sources,
        "summary": {
            "total_bugs": int(getattr(stats, "total", 0) or 0),
            "open_bugs": int(getattr(stats, "open_count", 0) or 0),
            "packet_ready_bugs": int(getattr(stats, "packet_ready_count", 0) or 0),
            "replay_ready_bugs": int(getattr(stats, "replay_ready_count", 0) or 0),
            "replay_blocked_bugs": int(getattr(stats, "replay_blocked_count", 0) or 0),
            "fix_verified_bugs": int(getattr(stats, "fix_verified_count", 0) or 0),
            "underlinked_bugs": int(getattr(stats, "underlinked_count", 0) or 0),
            "regression_bugs": len(regressions),
            "degraded_packets": len(under_observed),
            "sampled_bugs": len(bug_items),
            "mttr_hours": round(float(getattr(stats, "mttr_hours", 0.0)), 2)
            if getattr(stats, "mttr_hours", None) is not None
            else None,
            "observability_state": str(getattr(stats, "observability_state", "unknown") or "unknown"),
        },
        "by_status": dict(getattr(stats, "by_status", {}) or {}),
        "by_severity": dict(getattr(stats, "by_severity", {}) or {}),
        "by_category": dict(getattr(stats, "by_category", {}) or {}),
        "top_recurring": top_recurring[:limit],
        "regressions": regressions[:limit],
        "under_observed": under_observed[:limit],
        "replay_ready": replay_ready[:limit],
    }


def build_trend_observability(*, limit: int = 5) -> dict[str, Any]:
    """Summarize recent provider trend detection from receipt history."""

    try:
        trends = TrendDetector().detect_from_receipts()
        trend_rows = [_trend_to_dict(trend) for trend in trends]
        severity_counts = Counter(row["severity"] for row in trend_rows)
        direction_counts = Counter(row["direction"] for row in trend_rows)
        digest = format_trends(trends[:limit]) if trend_rows else "No trends detected."
        return {
            "authority": "runtime.engineering_observability.build_trend_observability",
            "generated_at": _utcnow().isoformat(),
            "sources": {
                "receipt_trends": _source_payload(
                    available=bool(trend_rows),
                    mode="live" if trend_rows else "unavailable",
                )
            },
            "summary": {
                "total_trends": len(trend_rows),
                "critical_trends": int(severity_counts.get("critical", 0)),
                "warning_trends": int(severity_counts.get("warning", 0)),
                "info_trends": int(severity_counts.get("info", 0)),
                "degrading_trends": int(direction_counts.get("degrading", 0)),
                "accelerating_trends": int(direction_counts.get("accelerating", 0)),
                "improving_trends": int(direction_counts.get("improving", 0)),
            },
            "trends": trend_rows[:limit],
            "trend_digest": digest,
        }
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}"
        return {
            "authority": "runtime.engineering_observability.build_trend_observability",
            "generated_at": _utcnow().isoformat(),
            "sources": {
                "receipt_trends": _source_payload(
                    available=False,
                    mode="unavailable",
                    detail=detail,
                )
            },
            "summary": {
                "total_trends": 0,
                "critical_trends": 0,
                "warning_trends": 0,
                "info_trends": 0,
                "degrading_trends": 0,
                "accelerating_trends": 0,
                "improving_trends": 0,
            },
            "trends": [],
            "trend_digest": f"Trend detection unavailable: {exc}",
        }


def build_platform_observability(
    *,
    platform_payload: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    payload = platform_payload or {}
    preflight = _coerce_mapping(payload.get("preflight"))
    snapshot = _coerce_mapping(payload.get("operator_snapshot"))
    lane = _coerce_mapping(payload.get("lane_recommendation"))
    proof_metrics = _coerce_mapping(payload.get("proof_metrics"))
    schema_authority = _coerce_mapping(payload.get("schema_authority"))
    trend_observability = build_trend_observability()
    raw_checks = _coerce_list(preflight.get("checks"))

    checks: list[dict[str, Any]] = []
    failed_checks = 0
    warning_checks = 0
    queue_depth = 0
    for raw in raw_checks:
        check = _coerce_mapping(raw)
        raw_status = str(check.get("status") or ("ok" if check.get("passed") else "failed")).lower()
        if raw_status in {"ok", "healthy"}:
            status = "healthy"
        elif raw_status in {"warning", "degraded"}:
            status = "warning"
            warning_checks += 1
        else:
            status = "failed"
            failed_checks += 1
        details = _coerce_mapping(check.get("details"))
        if str(check.get("name") or "") == "queue_depth":
            queue_depth = int(details.get("total_queued") or 0)
        checks.append(
            {
                "name": str(check.get("name") or "unknown"),
                "status": status,
                "detail": str(check.get("message") or ""),
                "passed": bool(check.get("passed")),
                "duration_ms": check.get("duration_ms"),
                "details": details,
            }
        )

    degraded_causes: list[str] = []
    for check in checks:
        if check["status"] != "healthy" and check["detail"]:
            degraded_causes.append(f"{check['name']}: {check['detail']}")
    for reason in _coerce_list(lane.get("reasons")):
        text = str(reason or "").strip()
        if text and text not in degraded_causes:
            degraded_causes.append(text)
    if error:
        degraded_causes.append(error)

    return {
        "authority": "runtime.engineering_observability.build_platform_observability",
        "generated_at": _utcnow().isoformat(),
        "sources": {
            "platform_health": _source_payload(
                available=bool(platform_payload),
                mode="live" if platform_payload else "unavailable",
                detail=error,
            ),
            "receipt_trends": trend_observability.get(
                "sources", {}
            ).get(
                "receipt_trends",
                _source_payload(available=False, mode="unavailable"),
            ),
        },
        "summary": {
            "overall": str(preflight.get("overall") or "unknown"),
            "failed_checks": failed_checks,
            "warning_checks": warning_checks,
            "queue_depth": queue_depth,
            "operator_posture": str(snapshot.get("posture") or "unknown"),
            "recommended_posture": str(lane.get("recommended_posture") or "unknown"),
            "pending_jobs": int(snapshot.get("pending_jobs") or 0),
            "running_jobs": int(snapshot.get("running_jobs") or 0),
            "active_leases": int(snapshot.get("active_leases") or 0),
            "open_circuit_breakers": len(_coerce_list(snapshot.get("circuit_breaker_open"))),
            "loop_warnings": int(snapshot.get("loop_warnings") or 0),
            "write_conflicts": int(snapshot.get("write_conflicts") or 0),
            "governance_blocks": int(snapshot.get("governance_blocks") or 0),
        },
        "checks": checks,
        "degraded_causes": degraded_causes,
        "lane_recommendation": lane,
        "operator_snapshot": snapshot,
        "proof_metrics": proof_metrics,
        "schema_authority": schema_authority,
        "trend_observability": trend_observability,
    }


__all__ = [
    "DEFAULT_BUG_PACKET_LIMIT",
    "DEFAULT_SCAN_ROOTS",
    "build_bug_scoreboard",
    "build_code_hotspots",
    "build_platform_observability",
    "build_trend_observability",
]
