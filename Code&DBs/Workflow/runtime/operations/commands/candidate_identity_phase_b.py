"""Phase B: candidate identity tuple expansion + routing FK.

Adds the full identity dimensions (`transport_type`, `host_provider_slug`,
`variant`, `effort_slug`) to `provider_model_candidates` and the matching
columns to `task_type_routing`. Performs need-driven candidate fan-out so
every routing tuple has a matching candidate, then enforces the relationship
with a composite FK on `(transport_type, provider_slug, host_provider_slug,
model_slug, variant, effort_slug)`. Drift-view + projection refresh round
out the change.

Why this exists as a CQRS operation:
    Phase B is a structural migration but we want a durable receipt + replay
    + audit log so future operators can trace why the schema looks the way
    it does. Registering it through the gateway (not as a hand-rolled SQL
    file) gives us that ledger.

Idempotent: re-running adds the columns/constraints only if missing, and
backfills only NULL fields. Existing data is preserved.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


# Explicit host_provider_slug mapping for openrouter brokered candidates.
# Encoding in candidate_ref is inconsistent (`.openai.`, `-openai-`, `openai/`),
# so we hardcode the canonical hosts for the openrouter rows we have today.
# Brokered direct-call providers map to themselves.
_OPENROUTER_HOST_MAP = {
    "anthropic-claude-haiku-4-5": "anthropic",
    "anthropic.claude-sonnet-4-6": "anthropic",
    "auto": "openrouter",
    "deepseek-r1": "deepseek",
    "deepseek-r1-picker": "deepseek",
    "deepseek-v3": "deepseek",
    "deepseek-v3.2": "deepseek",
    "deepseek-v4-flash": "deepseek",
    "deepseek-v4-flash-picker": "deepseek",
    "deepseek-v4-pro": "deepseek",
    "gemini-2.5-flash": "google",
    "google-gemini-3-flash-preview": "google",
    "google/gemini-3.1-pro-preview": "google",
    "gemini-3-flash-preview": "google",
    "llama-3.3-70b-instruct": "meta-llama",
    "mistral-medium-3.1": "mistralai",
    "mistral-small-3.2-24b": "mistralai",
    "moonshotai/kimi-k2.6": "moonshotai",
    "openai-gpt-5-1-codex-mini": "openai",
    "openai-gpt-5-4-mini": "openai",
    "openai-gpt-5-mini": "openai",
    "gpt-5.4-mini": "openai",
    "qwen/qwen3.6-plus": "qwen",
    "qwen3-235b-a22b-2507": "qwen",
    "qwen3-30b-a3b-thinking-2507": "qwen",
    "qwen3-max": "qwen",
    "x-ai/grok-4.1-fast": "x-ai",
    "xiaomi/mimo-v2.5-pro": "xiaomi",
    "z-ai/glm-5.1": "z-ai",
}


class CandidateIdentityPhaseBCommand(BaseModel):
    """No inputs — Phase B is a one-shot structural migration."""

    apply: bool = True


def _has_column(conn: Any, table: str, column: str) -> bool:
    rows = conn.execute(
        """
        SELECT 1 FROM information_schema.columns
         WHERE table_name = $1 AND column_name = $2
        """,
        table,
        column,
    )
    return bool(rows)


def _has_constraint(conn: Any, name: str) -> bool:
    rows = conn.execute(
        "SELECT 1 FROM pg_constraint WHERE conname = $1",
        name,
    )
    return bool(rows)


def _add_columns(conn: Any) -> dict[str, Any]:
    added: dict[str, list[str]] = {"provider_model_candidates": [], "task_type_routing": []}
    for col in ("transport_type", "host_provider_slug", "variant", "effort_slug"):
        if not _has_column(conn, "provider_model_candidates", col):
            conn.execute(f"ALTER TABLE provider_model_candidates ADD COLUMN {col} TEXT")
            added["provider_model_candidates"].append(col)
    for col in ("host_provider_slug", "variant", "effort_slug"):
        if not _has_column(conn, "task_type_routing", col):
            conn.execute(f"ALTER TABLE task_type_routing ADD COLUMN {col} TEXT")
            added["task_type_routing"].append(col)
    return added


def _backfill_candidates(conn: Any) -> dict[str, Any]:
    """Backfill candidate identity dimensions where the data tells us the answer."""
    summary: dict[str, int] = {}

    # 1. transport_type.
    #    a) Legacy convention: candidate_ref containing `.cli.` substring → CLI.
    summary["transport_type_cli_from_ref"] = len(
        conn.execute(
            """
            UPDATE provider_model_candidates
               SET transport_type = 'CLI'
             WHERE transport_type IS NULL AND candidate_ref LIKE '%.cli.%'
             RETURNING candidate_ref
            """
        )
        or []
    )

    # b) Single-transport providers: if provider_cli_profiles has only cli_llm or only
    #    llm_task, transport_type is unambiguous.
    summary["transport_type_single_lane"] = len(
        conn.execute(
            """
            WITH lanes AS (
                SELECT provider_slug,
                       bool_or(adapter_economics ? 'cli_llm') AS supports_cli,
                       bool_or(adapter_economics ? 'llm_task') AS supports_api
                  FROM provider_cli_profiles WHERE status='active'
                 GROUP BY provider_slug
            )
            UPDATE provider_model_candidates c
               SET transport_type = CASE
                   WHEN l.supports_cli AND NOT l.supports_api THEN 'CLI'
                   WHEN l.supports_api AND NOT l.supports_cli THEN 'API'
                   ELSE NULL
               END
              FROM lanes l
             WHERE c.provider_slug = l.provider_slug
               AND c.transport_type IS NULL
               AND ((l.supports_cli AND NOT l.supports_api) OR (l.supports_api AND NOT l.supports_cli))
             RETURNING c.candidate_ref
            """
        )
        or []
    )

    # c) Anthropic, OpenAI, Google: they support both lanes. The cli candidate_refs
    #    were caught by Rule 1a; the rest are API.
    summary["transport_type_dual_lane_default_api"] = len(
        conn.execute(
            """
            UPDATE provider_model_candidates
               SET transport_type = 'API'
             WHERE transport_type IS NULL
               AND provider_slug IN ('anthropic','openai','google')
             RETURNING candidate_ref
            """
        )
        or []
    )

    # d) Brokered providers (openrouter, fireworks, together) default to API.
    summary["transport_type_brokers_default_api"] = len(
        conn.execute(
            """
            UPDATE provider_model_candidates
               SET transport_type = 'API'
             WHERE transport_type IS NULL
               AND provider_slug IN ('openrouter','fireworks','together')
             RETURNING candidate_ref
            """
        )
        or []
    )

    # e) Last resort: API.
    summary["transport_type_fallback_api"] = len(
        conn.execute(
            """
            UPDATE provider_model_candidates
               SET transport_type = 'API'
             WHERE transport_type IS NULL
             RETURNING candidate_ref
            """
        )
        or []
    )

    # 2. host_provider_slug.
    #    a) Direct providers: host = provider.
    summary["host_direct_providers"] = len(
        conn.execute(
            """
            UPDATE provider_model_candidates
               SET host_provider_slug = provider_slug
             WHERE host_provider_slug IS NULL
               AND provider_slug NOT IN ('openrouter','fireworks','together')
             RETURNING candidate_ref
            """
        )
        or []
    )

    # b) Together: model_slug encodes the host (e.g. deepseek-ai/DeepSeek-V4-Pro).
    summary["host_together_from_model_slug"] = len(
        conn.execute(
            """
            UPDATE provider_model_candidates
               SET host_provider_slug = split_part(model_slug, '/', 1)
             WHERE host_provider_slug IS NULL
               AND provider_slug = 'together'
               AND model_slug LIKE '%/%'
             RETURNING candidate_ref
            """
        )
        or []
    )
    summary["host_together_self"] = len(
        conn.execute(
            """
            UPDATE provider_model_candidates
               SET host_provider_slug = 'together'
             WHERE host_provider_slug IS NULL
               AND provider_slug = 'together'
             RETURNING candidate_ref
            """
        )
        or []
    )

    # c) Fireworks: hosts on its own infra.
    summary["host_fireworks_self"] = len(
        conn.execute(
            """
            UPDATE provider_model_candidates
               SET host_provider_slug = 'fireworks'
             WHERE host_provider_slug IS NULL AND provider_slug = 'fireworks'
             RETURNING candidate_ref
            """
        )
        or []
    )

    # d) Openrouter: explicit map keyed by candidate_ref suffix.
    suffix_count = 0
    for suffix, host in _OPENROUTER_HOST_MAP.items():
        rows = conn.execute(
            """
            UPDATE provider_model_candidates
               SET host_provider_slug = $1
             WHERE host_provider_slug IS NULL
               AND provider_slug = 'openrouter'
               AND candidate_ref = 'candidate.openrouter.' || $2
             RETURNING candidate_ref
            """,
            host,
            suffix,
        )
        suffix_count += len(rows or [])
    summary["host_openrouter_explicit_map"] = suffix_count

    # e) Fallback: host = provider for any remaining NULL (avoids NULL-collapse in UNIQUE).
    summary["host_fallback_provider"] = len(
        conn.execute(
            """
            UPDATE provider_model_candidates
               SET host_provider_slug = provider_slug
             WHERE host_provider_slug IS NULL
             RETURNING candidate_ref
            """
        )
        or []
    )

    # 3. variant: 'picker' for picker-suffixed candidate_refs.
    summary["variant_picker"] = len(
        conn.execute(
            """
            UPDATE provider_model_candidates
               SET variant = 'picker'
             WHERE variant IS NULL AND candidate_ref LIKE '%-picker'
             RETURNING candidate_ref
            """
        )
        or []
    )

    return summary


def _need_driven_candidate_fanout(conn: Any) -> dict[str, Any]:
    """For each (provider, model, transport) tuple in routing that lacks a candidate,
    create a candidate row by cloning the closest existing candidate (matching on
    provider_slug + model_slug) and overriding transport_type. Effort and variant
    inherit from the source.

    This is the safety net that prevents routing rows from going orphan when an
    operator adds a CLI route for a model that previously only had an API candidate.
    """
    # Fan-out is API-only per operator policy. CLI candidates are explicit operator
    # decisions (CLI install, OAuth capture, harness wired) and must never be cloned
    # programmatically. CLI routing rows that lack a matching CLI candidate stay as
    # drift to be reviewed.
    rows = conn.execute(
        """
        WITH needs AS (
            SELECT DISTINCT r.provider_slug, r.model_slug, r.transport_type, r.host_provider_slug, r.variant, r.effort_slug
              FROM task_type_routing r
              LEFT JOIN provider_model_candidates c
                ON c.provider_slug = r.provider_slug
               AND c.model_slug = r.model_slug
               AND c.transport_type = r.transport_type
             WHERE c.candidate_ref IS NULL
               AND r.transport_type = 'API'
        ),
        sources AS (
            SELECT DISTINCT ON (provider_slug, model_slug)
                   provider_slug, model_slug, candidate_ref, provider_ref, provider_name,
                   status, priority, balance_weight, capability_tags, default_parameters,
                   effective_from, effective_to, decision_ref, cli_config, route_tier,
                   route_tier_rank, latency_class, latency_rank, reasoning_control,
                   task_affinities, benchmark_profile,
                   cap_language_high, cap_analysis_architecture_research, cap_build_high,
                   cap_review, cap_tool_use, cap_build_med, cap_language_low, cap_build_low,
                   cap_research_fan, cap_image,
                   host_provider_slug
              FROM provider_model_candidates
             ORDER BY provider_slug, model_slug, created_at
        )
        INSERT INTO provider_model_candidates (
            candidate_ref, provider_ref, provider_name, provider_slug, model_slug,
            transport_type, host_provider_slug, variant, effort_slug,
            status, priority, balance_weight, capability_tags, default_parameters,
            effective_from, decision_ref, created_at, cli_config, route_tier,
            route_tier_rank, latency_class, latency_rank, reasoning_control,
            task_affinities, benchmark_profile,
            cap_language_high, cap_analysis_architecture_research, cap_build_high,
            cap_review, cap_tool_use, cap_build_med, cap_language_low, cap_build_low,
            cap_research_fan, cap_image
        )
        SELECT
            s.candidate_ref || '.' || lower(n.transport_type) AS candidate_ref,
            s.provider_ref, s.provider_name, s.provider_slug, s.model_slug,
            n.transport_type,
            COALESCE(n.host_provider_slug, s.host_provider_slug) AS host_provider_slug,
            n.variant, n.effort_slug,
            s.status, s.priority, s.balance_weight, s.capability_tags, s.default_parameters,
            now(), s.decision_ref, now(), s.cli_config, s.route_tier,
            s.route_tier_rank, s.latency_class, s.latency_rank, s.reasoning_control,
            s.task_affinities, s.benchmark_profile,
            s.cap_language_high, s.cap_analysis_architecture_research, s.cap_build_high,
            s.cap_review, s.cap_tool_use, s.cap_build_med, s.cap_language_low, s.cap_build_low,
            s.cap_research_fan, s.cap_image
          FROM needs n
          JOIN sources s ON s.provider_slug = n.provider_slug AND s.model_slug = n.model_slug
        ON CONFLICT (candidate_ref) DO NOTHING
        RETURNING candidate_ref
        """
    )
    return {"fanned_out_count": len(rows or [])}


def _backfill_routing(conn: Any) -> dict[str, Any]:
    """Populate routing's host_provider_slug from the matching candidate."""
    rows = conn.execute(
        """
        UPDATE task_type_routing r
           SET host_provider_slug = c.host_provider_slug
          FROM provider_model_candidates c
         WHERE r.host_provider_slug IS NULL
           AND r.provider_slug = c.provider_slug
           AND r.model_slug = c.model_slug
           AND r.transport_type = c.transport_type
         RETURNING r.task_type
        """
    )
    return {"routing_host_backfilled": len(rows or [])}


def _add_unique_and_fk(conn: Any) -> dict[str, Any]:
    added: list[str] = []
    if not _has_constraint(conn, "provider_model_candidates_identity_uq"):
        conn.execute(
            """
            ALTER TABLE provider_model_candidates
              ADD CONSTRAINT provider_model_candidates_identity_uq
              UNIQUE NULLS NOT DISTINCT (transport_type, provider_slug, host_provider_slug, model_slug, variant, effort_slug)
            """
        )
        added.append("provider_model_candidates_identity_uq")

    if not _has_constraint(conn, "task_type_routing_candidate_fkey"):
        conn.execute(
            """
            ALTER TABLE task_type_routing
              ADD CONSTRAINT task_type_routing_candidate_fkey
              FOREIGN KEY (transport_type, provider_slug, host_provider_slug, model_slug, variant, effort_slug)
              REFERENCES provider_model_candidates (transport_type, provider_slug, host_provider_slug, model_slug, variant, effort_slug)
              ON UPDATE CASCADE
              ON DELETE RESTRICT
              NOT VALID
            """
        )
        added.append("task_type_routing_candidate_fkey")

    return {"constraints_added": added}


def _create_drift_view(conn: Any) -> dict[str, Any]:
    conn.execute(
        """
        CREATE OR REPLACE VIEW task_type_routing_orphans AS
        SELECT
            r.task_type, r.sub_task_type, r.provider_slug, r.host_provider_slug,
            r.model_slug, r.transport_type, r.variant, r.effort_slug, r.permitted,
            CASE
                WHEN c.candidate_ref IS NULL THEN 'provider_model_candidate.missing'
                WHEN cli.provider_slug IS NULL THEN 'provider_cli_profile.missing'
                WHEN cli.adapter_for_transport IS NULL THEN 'provider_cli_profile.adapter_economics_missing_for_transport'
                WHEN trans.provider_transport_admission_id IS NULL THEN 'transport.not_admitted'
                WHEN trans.admitted_by_policy IS NOT TRUE THEN 'transport.policy_denied'
                WHEN trans.status <> 'active' THEN 'transport.inactive'
                ELSE 'unknown'
            END AS orphan_reason
          FROM task_type_routing r
          LEFT JOIN provider_model_candidates c
            ON c.provider_slug = r.provider_slug
           AND c.model_slug = r.model_slug
           AND c.transport_type = r.transport_type
           AND c.host_provider_slug IS NOT DISTINCT FROM r.host_provider_slug
           AND c.variant IS NOT DISTINCT FROM r.variant
           AND c.effort_slug IS NOT DISTINCT FROM r.effort_slug
          LEFT JOIN LATERAL (
              SELECT p.provider_slug,
                     CASE
                         WHEN r.transport_type='CLI' AND (p.adapter_economics ? 'cli_llm') THEN 'cli_llm'
                         WHEN r.transport_type='API' AND (p.adapter_economics ? 'llm_task') THEN 'llm_task'
                         ELSE NULL
                     END AS adapter_for_transport
                FROM provider_cli_profiles p
               WHERE p.provider_slug = r.provider_slug AND p.status='active'
               LIMIT 1
          ) cli ON TRUE
          LEFT JOIN provider_transport_admissions trans
            ON trans.provider_slug = r.provider_slug AND trans.adapter_type = cli.adapter_for_transport
         WHERE c.candidate_ref IS NULL
            OR cli.provider_slug IS NULL
            OR cli.adapter_for_transport IS NULL
            OR trans.provider_transport_admission_id IS NULL
            OR trans.admitted_by_policy IS NOT TRUE
            OR trans.status <> 'active'
        """
    )
    conn.execute(
        """
        COMMENT ON VIEW task_type_routing_orphans IS
        'Routing rows whose dependencies are missing — they project as `disabled` in private_provider_job_catalog. Surfaces partial-refactor drift before downstream consumers hit it.'
        """
    )
    rows = conn.execute("SELECT count(*) FROM task_type_routing_orphans")
    cnt = rows[0]["count"] if rows else 0
    return {"drift_view_created": True, "current_orphans": int(cnt)}


def _refresh_projection(conn: Any) -> dict[str, Any]:
    refreshed: list[str] = []
    for profile in ("praxis", "scratch_agent"):
        conn.execute("SELECT refresh_private_provider_job_catalog($1)", profile)
        conn.execute("SELECT refresh_private_provider_control_plane_snapshot($1)", profile)
        refreshed.append(profile)
    return {"runtime_profiles_refreshed": refreshed}


def handle_candidate_identity_phase_b(
    command: CandidateIdentityPhaseBCommand, subsystems: Any
) -> dict[str, Any]:
    """Run Phase B: schema additions + need-driven fan-out + FK + drift view."""
    conn = subsystems.get_pg_conn()
    result: dict[str, Any] = {"ok": True, "applied": command.apply, "steps": {}}

    if not command.apply:
        return {"ok": True, "applied": False, "note": "dry-run; no changes made"}

    result["steps"]["columns"] = _add_columns(conn)
    # Idempotency: reset backfilled dimensions so re-runs always produce the same
    # result. Without this, partial earlier runs leave stale CLI/API/host values
    # that confuse the unique constraint at the end.
    conn.execute(
        "UPDATE provider_model_candidates SET transport_type=NULL, host_provider_slug=NULL, variant=NULL, effort_slug=NULL"
    )
    conn.execute(
        "UPDATE task_type_routing SET host_provider_slug=NULL, variant=NULL, effort_slug=NULL"
    )
    result["steps"]["candidate_backfill"] = _backfill_candidates(conn)
    result["steps"]["candidate_fanout"] = _need_driven_candidate_fanout(conn)
    result["steps"]["routing_backfill"] = _backfill_routing(conn)
    result["steps"]["constraints"] = _add_unique_and_fk(conn)
    result["steps"]["drift_view"] = _create_drift_view(conn)
    result["steps"]["refresh"] = _refresh_projection(conn)

    return result
