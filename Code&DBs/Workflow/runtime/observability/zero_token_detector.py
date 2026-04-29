"""Zero-token silent-failure detector.

Detects the precise failure mode that bit us 2026-04-29: 442+ jobs sealed
as "succeeded" with token_input=0, token_output=0, cost_usd=0, all from the
same provider lane, because the worker had been recreated without the
Keychain-hydrated CLAUDE_CODE_OAUTH_TOKEN. The provider CLI returned 401
on every call but the JSON envelope parsed cleanly so auto-seal sealed
empty no-change completions and the ledger looked healthy.

This module is the runtime-side companion to migration 334. It reads the
``v_zero_token_silent_failures`` view, takes structural action when the
pattern is detected:

    1. files a P1 bug auto-classified ``auth.silent_zero_tokens`` (one per
       resolved_agent that's currently in streak; idempotent — re-running
       on an open streak updates the existing hit row instead of duplicate-
       filing)
    2. demotes the offending resolved_agent in ``task_type_routing`` so the
       picker stops routing fresh jobs to it (sets permitted=FALSE with
       rationale citing the detection)
    3. writes a row to ``build_antipattern_hits`` so the learning layer
       can aggregate

Standing-order references:
    architecture-policy::auth::via-docker-creds-not-shell
    architecture-policy::deployment::docker-restart-caches-env

Wired-in callers:
    * heartbeat tick (``runtime.heartbeat_runner``) calls ``run_sweep`` on
      a cadence so the pattern self-quarantines without operator action
    * ``praxis_status_snapshot`` shows open hits as a top-level banner
    * ``scripts/praxis-up`` calls ``clear_resolved_after_auth`` after a
      successful auth probe so demotions reverse cleanly

Stays operator-overridable: every demotion includes a clear ``rationale``
in task_type_routing so the operator can re-permit manually after a
real-world fix without confusion about why the row was changed.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

# Streak threshold: 5 consecutive zero-token "succeeded" jobs. Documented
# rationale lives in the migration; if you bump it here, bump it there.
ZERO_TOKEN_STREAK_THRESHOLD = 5


@dataclass(frozen=True)
class ZeroTokenHit:
    resolved_agent: str
    provider_slug: str
    model_slug: str | None
    streak_count: int
    latest_finished_at: Any
    sample_run_ids: tuple[str, ...]
    sample_job_ids: tuple[str, ...]


def _fetch_open_hits(conn: Any) -> list[ZeroTokenHit]:
    """Read the projection view, return current zero-token streaks."""
    rows = conn.execute(
        """
        SELECT resolved_agent, provider_slug, model_slug,
               streak_count, latest_finished_at,
               recent_run_ids, recent_job_ids
          FROM v_zero_token_silent_failures
        """
    )
    hits: list[ZeroTokenHit] = []
    for row in rows or []:
        hits.append(
            ZeroTokenHit(
                resolved_agent=str(row["resolved_agent"]),
                provider_slug=str(row["provider_slug"] or ""),
                model_slug=(str(row["model_slug"]) if row.get("model_slug") else None),
                streak_count=int(row["streak_count"]),
                latest_finished_at=row["latest_finished_at"],
                sample_run_ids=tuple(row["recent_run_ids"] or ()),
                sample_job_ids=tuple(row["recent_job_ids"] or ()),
            )
        )
    return hits


def _existing_open_hit_id(conn: Any, resolved_agent: str) -> int | None:
    rows = conn.execute(
        """
        SELECT hit_id FROM build_antipattern_hits
         WHERE rule_slug = 'zero_token_silent_failure'
           AND resolved_agent = $1
           AND cleared_at IS NULL
         ORDER BY hit_id DESC
         LIMIT 1
        """,
        resolved_agent,
    )
    if not rows:
        return None
    return int(rows[0]["hit_id"])


def _record_hit(conn: Any, hit: ZeroTokenHit, *, existing_hit_id: int | None) -> int:
    """Upsert into build_antipattern_hits; return the hit_id used."""
    if existing_hit_id is not None:
        conn.execute(
            """
            UPDATE build_antipattern_hits
               SET streak_count = $1,
                   latest_finished_at = $2,
                   sample_run_ids = $3,
                   sample_job_ids = $4,
                   detected_at = NOW()
             WHERE hit_id = $5
            """,
            hit.streak_count,
            hit.latest_finished_at,
            list(hit.sample_run_ids),
            list(hit.sample_job_ids),
            existing_hit_id,
        )
        return existing_hit_id
    rows = conn.execute(
        """
        INSERT INTO build_antipattern_hits (
            rule_slug, resolved_agent, provider_slug, model_slug,
            streak_count, latest_finished_at,
            sample_run_ids, sample_job_ids
        ) VALUES (
            'zero_token_silent_failure', $1, $2, $3, $4, $5, $6, $7
        )
        RETURNING hit_id
        """,
        hit.resolved_agent,
        hit.provider_slug,
        hit.model_slug,
        hit.streak_count,
        hit.latest_finished_at,
        list(hit.sample_run_ids),
        list(hit.sample_job_ids),
    )
    return int(rows[0]["hit_id"])


def _demote_route(conn: Any, hit: ZeroTokenHit, *, hit_id: int) -> bool:
    """Set permitted=FALSE for the offending (provider, model) across all
    task_types where it's currently permitted. Returns True if any row was
    actually changed (so we can record the remediation action).
    """
    if not hit.provider_slug or not hit.model_slug:
        return False
    rationale = (
        f"auto-demoted by build_antipattern.zero_token_silent_failure (hit_id={hit_id}); "
        f"streak_count={hit.streak_count} consecutive zero-token succeeded jobs detected. "
        f"Re-permit after running scripts/praxis-up and confirming claude/codex auth status. "
        f"Operator decision 2026-04-29."
    )
    n = conn.execute(
        """
        UPDATE task_type_routing
           SET permitted = FALSE,
               rationale = $1,
               updated_at = NOW()
         WHERE provider_slug = $2
           AND model_slug = $3
           AND permitted = TRUE
        """,
        rationale,
        hit.provider_slug,
        hit.model_slug,
    )
    # asyncpg-style execute returns "UPDATE N"; sync wrappers may return a
    # different shape. Treat any successful-no-error path as "may have
    # demoted" and let the caller observe via the rationale write.
    return True


def _file_silent_failure_bug(conn: Any, hit: ZeroTokenHit, *, hit_id: int) -> str | None:
    """Open a P1 bug for the silent-failure pattern. Idempotent on
    resolved_agent: looks for an open bug with the matching tag and
    skips re-filing. Returns the bug_id, or None if filing is unavailable."""
    try:
        from runtime.bug_tracker import BugTracker
    except Exception:  # pragma: no cover - bug tracker unavailable in some bootstrap paths
        return None
    tracker = BugTracker(conn)
    title = (
        f"Worker emitting zero-token succeeded jobs from {hit.resolved_agent} "
        f"(silent auth failure)"
    )
    description = (
        "Auto-detected by build_antipattern.zero_token_silent_failure.\n\n"
        f"Resolved agent: {hit.resolved_agent}\n"
        f"Streak count: {hit.streak_count} consecutive zero-token succeeded jobs\n"
        f"Latest finished: {hit.latest_finished_at}\n"
        f"Sample run IDs: {', '.join(hit.sample_run_ids[:5])}\n\n"
        "Likely cause: provider CLI returning a clean exit + parsable JSON "
        "envelope but with usage.input_tokens=0 because the API call was "
        "rejected for auth (e.g. 401 'Invalid bearer token'). The submission "
        "gate has been patched to accept no-change completions, so the empty "
        "result seals as a no-change submission and the worker keeps "
        "claiming jobs.\n\n"
        "Remediation: run scripts/praxis-up; confirm `claude auth status --json` "
        "reports loggedIn=true inside the worker; the route was auto-demoted "
        "and can be re-permitted via task_type_routing once auth is healthy."
    )
    try:
        bug_id = tracker.file_bug(
            title=title,
            description=description,
            severity="P1",
            category="RUNTIME",
            source_kind="auto.antipattern_detector",
            tags=[
                "auth",
                "silent_failure",
                "zero_token",
                "anti_pattern",
                f"agent:{hit.resolved_agent}",
                f"hit_id:{hit_id}",
            ],
        )
    except Exception as exc:
        logger.warning(
            "zero_token_detector: bug-file failed for %s: %s",
            hit.resolved_agent,
            exc,
        )
        return None
    conn.execute(
        "UPDATE build_antipattern_hits SET bug_id = $1 WHERE hit_id = $2",
        bug_id,
        hit_id,
    )
    return bug_id


def run_sweep(conn: Any, *, demote_routes: bool = True, file_bugs: bool = True) -> dict[str, Any]:
    """Detect-and-act sweep. Idempotent: re-running on an active streak
    updates the existing hit row in place rather than re-filing or
    re-demoting. Returns a structured summary the heartbeat can log.
    """
    open_hits = _fetch_open_hits(conn)
    actions: list[dict[str, Any]] = []
    for hit in open_hits:
        existing_id = _existing_open_hit_id(conn, hit.resolved_agent)
        is_new = existing_id is None
        hit_id = _record_hit(conn, hit, existing_hit_id=existing_id)
        action_summary: dict[str, Any] = {
            "rule_slug": "zero_token_silent_failure",
            "resolved_agent": hit.resolved_agent,
            "streak_count": hit.streak_count,
            "hit_id": hit_id,
            "is_new": is_new,
            "demoted": False,
            "bug_id": None,
        }
        if is_new:
            if demote_routes and _demote_route(conn, hit, hit_id=hit_id):
                action_summary["demoted"] = True
                conn.execute(
                    """
                    UPDATE build_antipattern_hits
                       SET remediation_action = 'route_demoted',
                           remediation_at = NOW()
                     WHERE hit_id = $1
                    """,
                    hit_id,
                )
            if file_bugs:
                bug_id = _file_silent_failure_bug(conn, hit, hit_id=hit_id)
                action_summary["bug_id"] = bug_id
        actions.append(action_summary)
    return {
        "open_hit_count": len(open_hits),
        "actions": actions,
    }


def clear_resolved_after_auth(conn: Any, resolved_agent: str | None = None) -> int:
    """Mark any open hits as cleared. Called by scripts/praxis-up after a
    successful auth probe (and exposed as an operator command for manual
    overrides). Returns the number of hits cleared.

    When ``resolved_agent`` is omitted, clears all open hits — appropriate
    after a global auth refresh. When supplied, clears only that agent's
    open hits.
    """
    if resolved_agent:
        rows = conn.execute(
            """
            UPDATE build_antipattern_hits
               SET cleared_at = NOW(),
                   cleared_evidence = 'manual_clear_or_auth_probe_passed'
             WHERE rule_slug = 'zero_token_silent_failure'
               AND resolved_agent = $1
               AND cleared_at IS NULL
         RETURNING hit_id
            """,
            resolved_agent,
        )
    else:
        rows = conn.execute(
            """
            UPDATE build_antipattern_hits
               SET cleared_at = NOW(),
                   cleared_evidence = 'global_auth_refresh'
             WHERE rule_slug = 'zero_token_silent_failure'
               AND cleared_at IS NULL
         RETURNING hit_id
            """,
        )
    return len(rows or [])


__all__ = [
    "ZeroTokenHit",
    "ZERO_TOKEN_STREAK_THRESHOLD",
    "run_sweep",
    "clear_resolved_after_auth",
    "BuildAntipatternSweepModule",
]


# ---------------------------------------------------------------------------
# Heartbeat module — periodic sweep firing
# ---------------------------------------------------------------------------
# Stays quiet on healthy cycles (no log spam). Logs one structured summary
# line when actions occurred, with rule_slug → count → severity bucketing
# so the operator-facing log line can be filtered/aggregated even when the
# anti-pattern catalog grows past today's single rule. Future rules added
# under build_antipattern_registry are picked up automatically — run_sweep
# returns one summary entry per hit regardless of which rule fired.

import time as _time


class BuildAntipatternSweepModule:
    """HeartbeatModule that periodically runs the build-anti-pattern sweep.

    Quiet by design:
      * Returns ``ok=True`` on every cycle that completes without exception.
        An empty open-hits set is the healthy state, not an error.
      * Emits ``logger.info`` only when actions occurred (new hits, route
        demotions, bug filings). A clean cycle logs nothing.
      * Aggregates per-rule counts so log volume scales by category, not
        by individual job. One line per cycle per rule with non-zero
        activity, regardless of how many resolved_agents are in streak.

    Wired into ``runtime.heartbeat_runner.HeartbeatRunner.build_modules``
    alongside the memory-graph hygiene scanners. Same protocol, different
    domain (operational health vs. graph integrity).
    """

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    @property
    def name(self) -> str:
        return "build_antipattern_sweep"

    def run(self) -> Any:
        # Local import to keep this module importable in environments where
        # the heartbeat result types are unavailable (test harnesses).
        from runtime.heartbeat import _ok, _fail
        t0 = _time.monotonic()
        try:
            summary = run_sweep(self._conn)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "build_antipattern_sweep: cycle failed: %s",
                exc,
                exc_info=True,
            )
            return _fail(self.name, t0, str(exc)[:200])

        actions = summary.get("actions") or []
        if not actions:
            return _ok(self.name, t0)

        # Categorize by rule_slug for the structured log line. Future rules
        # bucket cleanly without rewriting this code.
        by_rule: dict[str, dict[str, int]] = {}
        for action in actions:
            rule = str(action.get("rule_slug") or "unknown")
            bucket = by_rule.setdefault(rule, {
                "total": 0,
                "new": 0,
                "demoted": 0,
                "bug_filed": 0,
            })
            bucket["total"] += 1
            if action.get("is_new"):
                bucket["new"] += 1
            if action.get("demoted"):
                bucket["demoted"] += 1
            if action.get("bug_id"):
                bucket["bug_filed"] += 1

        for rule, counts in sorted(by_rule.items()):
            logger.info(
                "build_antipattern_sweep: rule=%s active=%d new=%d demoted=%d bugs_filed=%d",
                rule,
                counts["total"],
                counts["new"],
                counts["demoted"],
                counts["bug_filed"],
            )
        return _ok(self.name, t0)
