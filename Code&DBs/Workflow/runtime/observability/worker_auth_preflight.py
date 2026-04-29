"""Worker startup auth preflight.

Runs once when the workflow worker boots, before the claim loop opens. For
every provider currently permitted in ``task_type_routing``, fires a 1-token
probe through the existing ``praxis_cli_auth_doctor`` machinery. Any
provider whose probe returns ``unauthenticated`` or ``timeout`` gets its
routes demoted (``permitted=FALSE``) so the worker doesn't claim jobs it
can't authenticate.

Pairs with:
    * scripts/praxis-up — runs auth_doctor at compose-recreate time, clears
      any open zero_token_silent_failure hits on success
    * docker-compose.yml workflow-worker.healthcheck — ongoing claude-only
      gate
    * runtime/observability/zero_token_detector — post-hoc metric detection
      when the preflight + healthcheck both miss

Why this layer is needed even with the other two:
    * praxis-up only runs at recreate time; the worker can boot via
      `docker start` (no compose recreate, no env refresh)
    * the healthcheck is claude-only; codex / gemini / openrouter aren't
      probed there
    * the zero-token detector fires AFTER the worker has emitted ~5
      silently-failing jobs; preflight catches it before the first claim

Standing-order references:
    architecture-policy::auth::via-docker-creds-not-shell
    architecture-policy::deployment::docker-restart-caches-env
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def _permitted_providers(conn: Any) -> list[str]:
    """Distinct provider_slugs currently permitted in task_type_routing."""
    rows = conn.execute(
        """
        SELECT DISTINCT provider_slug
          FROM task_type_routing
         WHERE permitted = TRUE
           AND COALESCE(provider_slug, '') <> ''
         ORDER BY provider_slug
        """
    )
    return [str(r["provider_slug"]) for r in rows or []]


def _demote_provider_routes(conn: Any, provider_slug: str, reason: str) -> int:
    """Set permitted=FALSE on every active route for this provider. Returns
    the number of rows updated by counting via SELECT first (asyncpg's
    sync wrapper doesn't reliably surface UPDATE rowcount)."""
    if not provider_slug:
        return 0
    count_rows = conn.execute(
        "SELECT COUNT(*) AS c FROM task_type_routing WHERE provider_slug=$1 AND permitted=TRUE",
        provider_slug,
    )
    n = int(count_rows[0]["c"]) if count_rows else 0
    if n == 0:
        return 0
    rationale = (
        f"auto-demoted by worker_auth_preflight: {reason}. "
        "Re-permit via `make refresh` once host auth is fixed; "
        "scripts/praxis-up clears auto-demotions on successful probe. "
        "Operator decision 2026-04-29."
    )
    conn.execute(
        """
        UPDATE task_type_routing
           SET permitted = FALSE,
               rationale = $1,
               updated_at = NOW()
         WHERE provider_slug = $2
           AND permitted = TRUE
        """,
        rationale,
        provider_slug,
    )
    return n


def run_startup_auth_preflight(conn: Any) -> dict[str, Any]:
    """Probe every permitted provider's CLI auth state at worker boot.

    Returns a structured summary the caller can log. Demotes any provider
    that fails the probe so the claim loop doesn't try to route to a dead
    lane. Soft-fails closed: if the probe machinery itself raises, we log
    and return without demoting (don't make a panicking probe shut down
    the whole worker).
    """
    try:
        from surfaces.mcp.tools.provider_onboard import tool_praxis_cli_auth_doctor
    except Exception as exc:
        logger.warning(
            "worker_auth_preflight: cli_auth_doctor unavailable, skipping probe: %s",
            exc,
        )
        return {"skipped": True, "reason": "cli_auth_doctor_import_failed"}

    permitted = set(_permitted_providers(conn))
    if not permitted:
        return {"skipped": True, "reason": "no_permitted_providers"}

    try:
        result = tool_praxis_cli_auth_doctor({})
    except Exception as exc:
        logger.warning(
            "worker_auth_preflight: auth_doctor probe failed: %s",
            exc,
        )
        return {"skipped": True, "reason": "probe_raised", "error": str(exc)[:200]}

    reports = result.get("reports") or []
    demotions: list[dict[str, Any]] = []
    healthy: list[str] = []
    unprobed: list[str] = []

    probed_providers = {str(r.get("provider_slug")) for r in reports if r.get("provider_slug")}
    unprobed.extend(sorted(permitted - probed_providers))

    for r in reports:
        provider = str(r.get("provider_slug") or "").strip()
        if not provider or provider not in permitted:
            continue
        is_healthy = bool(r.get("healthy"))
        if is_healthy:
            healthy.append(provider)
            continue
        auth_state = str(r.get("auth_state") or "unknown")
        summary = str(r.get("summary") or "")[:200]
        demoted = _demote_provider_routes(
            conn,
            provider,
            f"auth_state={auth_state}: {summary}",
        )
        demotions.append({
            "provider_slug": provider,
            "auth_state": auth_state,
            "routes_demoted": demoted,
            "summary": summary,
        })

    summary = {
        "healthy": healthy,
        "demoted": demotions,
        "unprobed_permitted": unprobed,
        "doctor_summary": result.get("summary"),
    }
    if demotions:
        logger.warning(
            "worker_auth_preflight: demoted %d provider(s) at startup: %s",
            len(demotions),
            ", ".join(d["provider_slug"] for d in demotions),
        )
    else:
        logger.info(
            "worker_auth_preflight: %d permitted provider(s) healthy",
            len(healthy),
        )
    return summary


__all__ = ["run_startup_auth_preflight"]
