"""Automatic provider model retirement detector.

For each provider with active rows in ``provider_model_candidates``, this
scanner determines whether each registered model is still on offer:

  (a) Providers that expose a live `/models` endpoint (openai, google, deepseek,
      cursor, openrouter) get an authoritative diff against discovery.
  (b) Providers without a probe-able API — anthropic per
      ``decision.2026-04-20.anthropic-cli-only-restored`` — fall through to
      ``provider_model_retirement_ledger`` (curated table, migration 191).

Safety guards keep a network blip or rate-limit from wiping the active pool:

  * Discovery exception → skip provider this cycle (snapshot status=``degraded``).
  * Live set < 50% of registered set → skip provider (suspect partial response).
  * Both live set and ledger empty → skip (nothing trustworthy to compare).

In ``dry_run=True`` mode (the default for explicit calls) the scanner only
reports. The daily heartbeat invokes it with ``dry_run=False`` so freshly-
discovered retirements actually flow through to ``provider_model_candidates``
(``status='retired'``, ``effective_to=now()``).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import asyncpg

from registry.provider_onboarding import (
    ProviderOnboardingSpec,
    _discover_api_models,
    _resolve_spec,
)

logger = logging.getLogger(__name__)

# Providers without a probe-able /models endpoint must rely on the curated
# ledger. Anthropic is the canonical case (CLI-only, no API key allowed).
_LEDGER_ONLY_PROVIDERS: frozenset[str] = frozenset({"anthropic"})

# Safety: never auto-retire if live discovery loses more than this fraction of
# the previously-registered models. A degenerate response (network blip, auth
# regression, vendor 5xx) often returns a tiny subset and would otherwise wipe
# the pool. The provider gets a `degraded` snapshot and a fresh chance next cycle.
_MAX_SAFE_DROP_FRACTION = 0.5


@dataclass(slots=True, frozen=True)
class RetirementFinding:
    """One model the scanner judged retired (or about to be).

    ``source`` is how we learned: ``api_discovery`` (live diff) or ``ledger``
    (curated table). ``effective_date`` only populated for ledger findings.
    ``ledger_kind`` is the ledger's own classification (retired / deprecating /
    sunset_warning) — None for API-discovery findings.
    """

    provider_slug: str
    model_slug: str
    source: str  # "api_discovery" | "ledger"
    reason: str
    ledger_kind: str | None = None
    effective_date: str | None = None


@dataclass(slots=True, frozen=True)
class ProviderScanOutcome:
    """Per-provider result line. Always emitted, even if no findings."""

    provider_slug: str
    discovery_mode: str  # "api_discovery" | "ledger" | "skipped"
    status: str  # "ok" | "degraded" | "warning" | "failed" | "skipped"
    summary: str
    registered_count: int
    live_count: int | None
    findings: tuple[RetirementFinding, ...] = ()
    error: str | None = None


@dataclass(slots=True)
class RetirementScanReport:
    scanned_at: datetime
    dry_run: bool
    providers_total: int
    providers_acted_on: int
    findings_total: int
    retirements_applied: int
    outcomes: list[ProviderScanOutcome] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

async def _load_active_candidates(
    conn: asyncpg.Connection,
) -> dict[str, list[str]]:
    rows = await conn.fetch(
        """
        SELECT provider_slug, model_slug
          FROM provider_model_candidates
         WHERE status = 'active' AND effective_to IS NULL
         ORDER BY provider_slug, model_slug
        """
    )
    grouped: dict[str, list[str]] = {}
    for row in rows:
        grouped.setdefault(row["provider_slug"], []).append(row["model_slug"])
    return grouped


async def _load_provider_profile(
    conn: asyncpg.Connection,
    provider_slug: str,
) -> Mapping[str, Any] | None:
    return await conn.fetchrow(
        """
        SELECT provider_slug, default_model, api_endpoint, api_protocol_family,
               api_key_env_vars, default_timeout
          FROM provider_cli_profiles
         WHERE status = 'active' AND provider_slug = $1
        """,
        provider_slug,
    )


async def _load_ledger_for_provider(
    conn: asyncpg.Connection,
    provider_slug: str,
) -> list[Mapping[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT model_slug, retirement_effective_date, retirement_kind,
               source, source_url, notes
          FROM provider_model_retirement_ledger
         WHERE provider_slug = $1
         ORDER BY retirement_effective_date ASC
        """,
        provider_slug,
    )
    return [dict(row) for row in rows]


_RETIRE_SQL = """
UPDATE provider_model_candidates
   SET status = 'retired',
       effective_to = $3
 WHERE provider_slug = $1
   AND model_slug = $2
   AND status = 'active'
   AND effective_to IS NULL
"""


async def _retire_candidate(
    conn: asyncpg.Connection,
    *,
    provider_slug: str,
    model_slug: str,
    at: datetime,
) -> bool:
    """Mark one candidate retired. Returns True if a row was updated."""
    result = await conn.execute(_RETIRE_SQL, provider_slug, model_slug, at)
    # asyncpg returns "UPDATE n"
    return result.rsplit(" ", 1)[-1] != "0"


# ---------------------------------------------------------------------------
# Spec building
# ---------------------------------------------------------------------------

def _build_spec_from_profile(
    provider_slug: str,
    profile: Mapping[str, Any],
) -> ProviderOnboardingSpec:
    """Construct the minimal spec needed to drive ``_discover_api_models``."""
    api_key_env_vars = profile.get("api_key_env_vars") or []
    if isinstance(api_key_env_vars, str):
        # asyncpg may hand back the jsonb as a string in some configurations;
        # match the same defensive parsing the heartbeat module uses.
        import json as _json
        try:
            api_key_env_vars = _json.loads(api_key_env_vars)
        except _json.JSONDecodeError:
            api_key_env_vars = []
    return ProviderOnboardingSpec(
        provider_slug=provider_slug,
        selected_transport="api",
        default_model=profile.get("default_model"),
        api_endpoint=profile.get("api_endpoint"),
        api_protocol_family=profile.get("api_protocol_family"),
        api_key_env_vars=tuple(str(v) for v in api_key_env_vars if str(v).strip()),
        default_timeout=int(profile.get("default_timeout") or 60),
    )


# ---------------------------------------------------------------------------
# Per-provider judgment
# ---------------------------------------------------------------------------

def _ledger_findings(
    *,
    provider_slug: str,
    registered: Sequence[str],
    ledger_rows: Sequence[Mapping[str, Any]],
    today: datetime,
) -> list[RetirementFinding]:
    """Map ledger rows to findings for the registered models.

    A row counts as a retirement candidate if its ``retirement_effective_date``
    is on or before today AND its ``retirement_kind`` is ``retired`` (sunsets
    flagged as ``deprecating`` or ``sunset_warning`` come back as advisory
    findings — caller decides whether to act).
    """
    today_date = today.date()
    by_slug: dict[str, Mapping[str, Any]] = {}
    for row in ledger_rows:
        slug = row["model_slug"]
        # Keep the latest entry per slug — ledger rows are ordered ASC, so the
        # last one wins.
        by_slug[slug] = row

    findings: list[RetirementFinding] = []
    for slug in registered:
        row = by_slug.get(slug)
        if row is None:
            continue
        effective = row["retirement_effective_date"]
        kind = row["retirement_kind"]
        if effective <= today_date and kind == "retired":
            findings.append(
                RetirementFinding(
                    provider_slug=provider_slug,
                    model_slug=slug,
                    source="ledger",
                    reason=row.get("notes") or f"Ledger marks {slug} as retired on {effective}.",
                    ledger_kind=kind,
                    effective_date=effective.isoformat(),
                )
            )
    return findings


def _api_discovery_findings(
    *,
    provider_slug: str,
    registered: Sequence[str],
    discovered: Sequence[str],
) -> list[RetirementFinding]:
    discovered_set = {slug.strip() for slug in discovered if slug.strip()}
    findings: list[RetirementFinding] = []
    for slug in registered:
        if slug not in discovered_set:
            findings.append(
                RetirementFinding(
                    provider_slug=provider_slug,
                    model_slug=slug,
                    source="api_discovery",
                    reason=(
                        f"{slug} is not in the live /models response from "
                        f"{provider_slug} ({len(discovered_set)} models offered)."
                    ),
                )
            )
    return findings


async def _scan_provider(
    conn: asyncpg.Connection,
    *,
    provider_slug: str,
    registered: Sequence[str],
    env: Mapping[str, str],
    today: datetime,
) -> ProviderScanOutcome:
    if not registered:
        return ProviderScanOutcome(
            provider_slug=provider_slug,
            discovery_mode="skipped",
            status="skipped",
            summary=f"{provider_slug}: no active candidates",
            registered_count=0,
            live_count=None,
        )

    if provider_slug in _LEDGER_ONLY_PROVIDERS:
        ledger_rows = await _load_ledger_for_provider(conn, provider_slug)
        findings = _ledger_findings(
            provider_slug=provider_slug,
            registered=registered,
            ledger_rows=ledger_rows,
            today=today,
        )
        if not ledger_rows:
            return ProviderScanOutcome(
                provider_slug=provider_slug,
                discovery_mode="ledger",
                status="degraded",
                summary=(
                    f"{provider_slug}: no ledger rows found and live probe is "
                    "disallowed; cannot judge retirements"
                ),
                registered_count=len(registered),
                live_count=None,
            )
        status = "failed" if findings else "ok"
        summary_tail = (
            f"{len(findings)} retired" if findings else "all current"
        )
        return ProviderScanOutcome(
            provider_slug=provider_slug,
            discovery_mode="ledger",
            status=status,
            summary=f"{provider_slug}: ledger-judged ({summary_tail})",
            registered_count=len(registered),
            live_count=None,
            findings=tuple(findings),
        )

    profile = await _load_provider_profile(conn, provider_slug)
    if profile is None:
        return ProviderScanOutcome(
            provider_slug=provider_slug,
            discovery_mode="skipped",
            status="skipped",
            summary=f"{provider_slug}: no provider_cli_profile row",
            registered_count=len(registered),
            live_count=None,
        )
    if not profile.get("api_endpoint") or not profile.get("api_protocol_family"):
        return ProviderScanOutcome(
            provider_slug=provider_slug,
            discovery_mode="skipped",
            status="skipped",
            summary=(
                f"{provider_slug}: profile has no API endpoint/protocol; "
                "cannot probe live model list"
            ),
            registered_count=len(registered),
            live_count=None,
        )

    spec = _build_spec_from_profile(provider_slug, profile)
    try:
        resolved_spec, _, transport_template, _ = _resolve_spec(spec)
        transport_details = {
            "discovery_strategy": transport_template.discovery_strategy,
        }
        discovered = await asyncio.to_thread(
            _discover_api_models,
            resolved_spec,
            env=env,
            transport_details=transport_details,
        )
    except Exception as exc:  # noqa: BLE001 — discovery surfaces are wide
        logger.warning("Live model discovery failed for %s: %s", provider_slug, exc)
        return ProviderScanOutcome(
            provider_slug=provider_slug,
            discovery_mode="api_discovery",
            status="degraded",
            summary=f"{provider_slug}: live discovery failed — skipped this cycle",
            registered_count=len(registered),
            live_count=None,
            error=str(exc)[:500],
        )

    discovered_clean = tuple(slug for slug in discovered if slug)
    if not discovered_clean:
        return ProviderScanOutcome(
            provider_slug=provider_slug,
            discovery_mode="api_discovery",
            status="degraded",
            summary=(
                f"{provider_slug}: live discovery returned zero models — "
                "skipped this cycle"
            ),
            registered_count=len(registered),
            live_count=0,
        )

    findings = _api_discovery_findings(
        provider_slug=provider_slug,
        registered=registered,
        discovered=discovered_clean,
    )

    # Safety guard: degenerate live response (e.g. truncated page).
    if registered and (len(findings) / len(registered)) > _MAX_SAFE_DROP_FRACTION:
        return ProviderScanOutcome(
            provider_slug=provider_slug,
            discovery_mode="api_discovery",
            status="degraded",
            summary=(
                f"{provider_slug}: live discovery would retire "
                f"{len(findings)}/{len(registered)} models — exceeds safety "
                f"threshold ({int(_MAX_SAFE_DROP_FRACTION * 100)}%); skipped"
            ),
            registered_count=len(registered),
            live_count=len(discovered_clean),
        )

    status = "failed" if findings else "ok"
    summary_tail = (
        f"{len(findings)} stale of {len(registered)}"
        if findings
        else "all current"
    )
    return ProviderScanOutcome(
        provider_slug=provider_slug,
        discovery_mode="api_discovery",
        status=status,
        summary=f"{provider_slug}: live-judged ({summary_tail})",
        registered_count=len(registered),
        live_count=len(discovered_clean),
        findings=tuple(findings),
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def scan_provider_model_retirements(
    conn: asyncpg.Connection,
    *,
    env: Mapping[str, str],
    dry_run: bool = True,
    now: datetime | None = None,
) -> RetirementScanReport:
    """Scan all active providers and (optionally) retire stale candidates.

    Pass ``dry_run=False`` to actually flip ``provider_model_candidates`` rows.
    Returns a structured report. Per-provider safety guards mean the caller
    never has to check the report before applying — anything actionable is
    already filtered.
    """
    today = now or datetime.now(timezone.utc)
    grouped = await _load_active_candidates(conn)

    outcomes: list[ProviderScanOutcome] = []
    errors: list[str] = []
    for provider_slug, registered in grouped.items():
        try:
            outcome = await _scan_provider(
                conn,
                provider_slug=provider_slug,
                registered=registered,
                env=env,
                today=today,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Scan crashed for %s", provider_slug)
            errors.append(f"{provider_slug}: {exc}")
            outcome = ProviderScanOutcome(
                provider_slug=provider_slug,
                discovery_mode="skipped",
                status="failed",
                summary=f"{provider_slug}: scan crashed",
                registered_count=len(registered),
                live_count=None,
                error=str(exc)[:500],
            )
        outcomes.append(outcome)

    findings_total = sum(len(o.findings) for o in outcomes)
    retirements_applied = 0
    if not dry_run:
        for outcome in outcomes:
            for finding in outcome.findings:
                # Only retire on hard signals: live API discovery saying the
                # model is gone, or ledger marking it ``retired`` (advisory
                # ``deprecating`` / ``sunset_warning`` rows do not auto-retire).
                if finding.source == "api_discovery" or finding.ledger_kind == "retired":
                    updated = await _retire_candidate(
                        conn,
                        provider_slug=finding.provider_slug,
                        model_slug=finding.model_slug,
                        at=today,
                    )
                    if updated:
                        retirements_applied += 1

    providers_acted_on = sum(1 for o in outcomes if o.findings)
    return RetirementScanReport(
        scanned_at=today,
        dry_run=dry_run,
        providers_total=len(outcomes),
        providers_acted_on=providers_acted_on,
        findings_total=findings_total,
        retirements_applied=retirements_applied,
        outcomes=outcomes,
        errors=errors,
    )
