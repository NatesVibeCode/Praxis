"""Query and evidence handlers for the workflow HTTP API."""

from __future__ import annotations

import base64
import binascii
import json
import logging
from datetime import datetime, timezone
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
from typing import Any
from urllib.parse import quote

from runtime.canonical_workflows import (
    WorkflowRuntimeBoundaryError,
    commit_workflow,
    materialize_definition_from_build_graph,
    delete_workflow,
    mutate_workflow_build,
    save_workflow,
    save_workflow_trigger,
    trigger_workflow_manually,
    update_workflow_trigger,
)
from runtime.helm_manifest import normalize_helm_bundle, normalize_source_option, resolve_tab
from runtime.object_lifecycle import (
    ObjectLifecycleBoundaryError,
    attach_document,
    create_document,
    create_object,
    create_object_type,
    delete_object,
    update_object,
)
from runtime.file_storage import delete_file, get_file_content, list_files, save_file
from runtime.payload_coercion import (
    coerce_int as _safe_int_impl,
    coerce_isoformat as _isoformat,
    coerce_text as _text,
    json_list as _json_list,
    json_object as _parse_properties,
    parse_json_field as _parse_json_field,
)
from runtime.surface_catalog_reviews import (
    list_surface_catalog_reviews,
    record_surface_catalog_review,
)
from runtime.integrations.display_names import (
    base_integration_name,
    display_name_for_integration,
)
from registry.control_plane_manifests import (
    CONTROL_MANIFEST_FAMILY as _CONTROL_MANIFEST_FAMILY,
    CONTROL_MANIFEST_KIND as _CONTROL_MANIFEST_KIND,
    list_control_manifest_heads as _list_control_manifest_heads,
    list_control_manifest_history as _list_control_manifest_history,
)
from surfaces.api.catalog_authority import build_catalog_payload
from storage.postgres.validators import PostgresWriteError
from . import workflow_query_core as _workflow_query_core
from ._surface_usage import record_api_route_usage as _record_api_route_usage
from ._shared import (
    REPO_ROOT,
    RouteEntry,
    RouteMatcher,
    _ClientError,
    _bug_to_dict,
    _exact,
    _matches,
    _prefix,
    _prefix_suffix,
    _query_params,
    _read_json_body,
    _serialize,
)
from .workflow_admin import _handle_health

logger = logging.getLogger(__name__)

_ALLOWED_FILE_SCOPES = {"instance", "step", "workflow"}
_REFRESHABLE_COMPILE_INDEX_REASON_CODES = {
    "compile_index.snapshot_missing",
    "compile_index.snapshot_stale",
}
_MARKET_MODEL_FAMILY_RULES: tuple[tuple[str, str], ...] = (
    ("qwen", "qwen"),
    ("qwq", "qwen"),
    ("deepseek", "deepseek"),
    ("gemma", "gemma"),
    ("gemini", "gemini"),
    ("claude", "claude"),
    ("codex", "codex"),
    ("gpt", "gpt"),
    ("llama", "llama"),
    ("mixtral", "mixtral"),
    ("pixtral", "pixtral"),
    ("devstral", "devstral"),
    ("ministral", "ministral"),
    ("magistral", "magistral"),
    ("mistral", "mistral"),
    ("grok", "grok"),
    ("kimi", "kimi"),
)
_READY_INTEGRATION_STATUSES = {"connected", "authorized", "ready", "active"}
_SOURCE_OPTION_SEEDS: tuple[dict[str, Any], ...] = (
    {
        "id": "web_search",
        "label": "Web Search",
        "family": "external",
        "kind": "web_search",
        "availability": "ready",
        "activation": "open",
        "description": "Look up current public information when local state is not enough.",
    },
    {
        "id": "external_api",
        "label": "External API",
        "family": "external",
        "kind": "api",
        "availability": "setup_required",
        "activation": "configure",
        "setup_intent": "Set up an external API source for this workspace.",
        "description": "Connect a new API before the workspace can query it.",
    },
    {
        "id": "third_party_dataset",
        "label": "Third-Party Dataset",
        "family": "external",
        "kind": "dataset",
        "availability": "setup_required",
        "activation": "configure",
        "setup_intent": "Set up a third-party dataset source for this workspace.",
        "description": "Attach a dataset feed or import before using it in the workspace.",
    },
)
_DASHBOARD_SECTION_ORDER: tuple[str, ...] = ("live", "saved", "draft")
def _prefix_single_segment(
    path_prefix: str,
    *,
    excluded: set[str] | None = None,
) -> RouteMatcher:
    excluded_values = frozenset(excluded or ())

    def _matches(candidate: str, *, prefix=path_prefix, excluded=excluded_values) -> bool:
        if not candidate.startswith(prefix):
            return False
        suffix = candidate[len(prefix) :]
        return bool(suffix) and "/" not in suffix and suffix not in excluded

    return _matches


def _workflow_build_path(candidate: str) -> bool:
    return candidate.startswith("/api/workflows/") and "/build" in candidate[len("/api/workflows/") :]


def _parse_bug_status(bt_mod, raw_status: object):
    if raw_status is None:
        return None
    status = bt_mod.BugTracker._normalize_status(raw_status, default=None)
    if status is None:
        raise _ClientError("status must be one of OPEN, IN_PROGRESS, FIXED, WONT_FIX, DEFERRED")
    return status


def _parse_bug_severity(bt_mod, raw_severity: object):
    if raw_severity is None:
        return None
    severity = bt_mod.BugTracker._normalize_severity(raw_severity, default=None)
    if severity is None:
        raise _ClientError("severity must be one of P0, P1, P2, P3")
    return severity


def _parse_bug_category(bt_mod, raw_category: object):
    if raw_category is None:
        return None
    category = bt_mod.BugTracker._normalize_category(raw_category, default=None)
    if category is None:
        raise _ClientError(
            "category must be one of SCOPE, VERIFY, IMPORT, WIRING, ARCHITECTURE, RUNTIME, TEST, OTHER"
        )
    return category


def _market_review_metrics(row: dict[str, Any]) -> dict[str, Any]:
    evaluations = row.get("evaluations")
    pricing = row.get("pricing")
    speed_metrics = row.get("speed_metrics")
    if not isinstance(evaluations, dict):
        evaluations = {}
    if not isinstance(pricing, dict):
        pricing = {}
    if not isinstance(speed_metrics, dict):
        speed_metrics = {}
    return {
        "intelligence_index": evaluations.get("artificial_analysis_intelligence_index"),
        "coding_index": evaluations.get("artificial_analysis_coding_index"),
        "math_index": evaluations.get("artificial_analysis_math_index"),
        "price_1m_blended_3_to_1": pricing.get("price_1m_blended_3_to_1"),
        "price_1m_input_tokens": pricing.get("price_1m_input_tokens"),
        "price_1m_output_tokens": pricing.get("price_1m_output_tokens"),
        "median_output_tokens_per_second": speed_metrics.get("median_output_tokens_per_second"),
        "median_time_to_first_token_seconds": speed_metrics.get("median_time_to_first_token_seconds"),
        "median_time_to_first_answer_token": speed_metrics.get("median_time_to_first_answer_token"),
    }


def _market_model_family_slug(row: dict[str, Any]) -> str:
    haystack = " ".join(
        str(part or "").strip().lower()
        for part in (
            row.get("source_model_slug"),
            row.get("model_name"),
        )
    )
    for needle, family_slug in _MARKET_MODEL_FAMILY_RULES:
        if needle and needle in haystack:
            return family_slug
    creator_slug = str(row.get("creator_slug") or "").strip().lower()
    return creator_slug or "unknown"


def _market_model_matches_query(row: dict[str, Any], query: str) -> bool:
    if not query:
        return True
    haystack = " ".join(
        str(part or "").strip().lower()
        for part in (
            row.get("source_slug"),
            row.get("creator_slug"),
            row.get("creator_name"),
            row.get("source_model_slug"),
            row.get("model_name"),
            row.get("family_slug"),
        )
    )
    return query in haystack


def _handle_query(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    return _workflow_query_core.handle_query(subs, body)


def _handle_bugs(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    return _workflow_query_core.handle_bugs(
        subs,
        body,
        parse_bug_status=_parse_bug_status,
        parse_bug_severity=_parse_bug_severity,
        parse_bug_category=_parse_bug_category,
    )


def _handle_recall(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    return _workflow_query_core.handle_recall(subs, body)


def _handle_ingest(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    return _workflow_query_core.handle_ingest(subs, body)


def _handle_graph(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    return _workflow_query_core.handle_graph(subs, body)


def _handle_receipts(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    return _workflow_query_core.handle_receipts(subs, body)


def _handle_constraints(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    return _workflow_query_core.handle_constraints(subs, body)


def _handle_friction(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    return _workflow_query_core.handle_friction(subs, body)


def _handle_heal(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    return _workflow_query_core.handle_heal(subs, body)


def _handle_artifacts(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    return _workflow_query_core.handle_artifacts(subs, body)


def _handle_decompose(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    return _workflow_query_core.handle_decompose(subs, body)


def _handle_research(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    return _workflow_query_core.handle_research(subs, body)


def _handle_operator_view(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    return _workflow_query_core.handle_operator_view(subs, body)


def _source_option_catalog(pg: Any) -> dict[str, dict[str, Any]]:
    reference_rows = pg.execute(
        "SELECT slug, ref_type, display_name, description, resolved_table, resolved_id "
        "FROM reference_catalog ORDER BY ref_type, slug"
    ) or []
    integration_rows = pg.execute(
        "SELECT id, name, description, provider, capabilities, auth_status, icon "
        "FROM integration_registry ORDER BY name"
    ) or []
    try:
        capability_rows = pg.execute(
            "SELECT capability_ref, capability_slug, capability_kind, title, summary, route, reference_slugs "
            "FROM capability_catalog WHERE enabled = TRUE ORDER BY capability_kind, title"
        ) or []
    except Exception:
        capability_rows = []

    capability_by_reference: dict[str, list[str]] = {}
    for row in capability_rows:
        summary = _text(row.get("summary")) or _text(row.get("title"))
        if not summary:
            continue
        for slug in _json_list(row.get("reference_slugs")):
            if not isinstance(slug, str) or not slug.strip():
                continue
            capability_by_reference.setdefault(slug.strip(), []).append(summary)

    integration_statuses = {
        _text(row.get("id")): _text(row.get("auth_status")).lower()
        for row in integration_rows
        if _text(row.get("id"))
    }

    catalog: dict[str, dict[str, Any]] = {
        seed["id"]: normalize_source_option(seed["id"], seed)
        for seed in _SOURCE_OPTION_SEEDS
    }

    for row in integration_rows:
        integration_id = _text(row.get("id"))
        if not integration_id:
            continue
        display_name = display_name_for_integration(row)
        auth_status = _text(row.get("auth_status")).lower()
        ready = auth_status in _READY_INTEGRATION_STATUSES
        option_id = f"integration:{integration_id}"
        catalog[option_id] = normalize_source_option(
            option_id,
            {
                "label": display_name,
                "family": "connected",
                "kind": "integration",
                "availability": "ready" if ready else "setup_required",
                "activation": "attach" if ready else "configure",
                "integration_id": integration_id,
                "setup_intent": f"Set up the {display_name} integration for this workspace."
                if not ready
                else None,
                "description": _text(row.get("description")) or f"{_text(row.get('provider'))} integration",
            },
        )

    for row in reference_rows:
        slug = _text(row.get("slug"))
        if not slug:
            continue
        ref_type = _text(row.get("ref_type")).lower()
        resolved_id = _text(row.get("resolved_id"))
        capability_notes = capability_by_reference.get(slug) or []
        description = _text(row.get("description"))
        if capability_notes:
            summary = "; ".join(capability_notes[:2])
            description = f"{description} {summary}".strip() if description else summary

        family = "reference"
        kind = "document"
        activation = "open"
        availability = "preview"
        setup_intent = None
        integration_id = None

        if ref_type == "object" or slug.startswith("#"):
            family = "workspace"
            kind = "object"
            activation = "attach"
            availability = "ready"
        elif ref_type == "integration" or slug.startswith("@"):
            family = "connected"
            kind = "integration"
            integration_id = resolved_id or slug.lstrip("@").split("/", 1)[0]
            ready = integration_statuses.get(integration_id or "", "") in _READY_INTEGRATION_STATUSES
            availability = "ready" if ready else "setup_required"
            activation = "attach" if ready else "configure"
            if not ready:
                setup_intent = f"Set up the source behind {slug} for this workspace."

        catalog[slug] = normalize_source_option(
            slug,
            {
                "label": _text(row.get("display_name")) or slug,
                "family": family,
                "kind": kind,
                "availability": availability,
                "activation": activation,
                "reference_slug": slug,
                "integration_id": integration_id,
                "setup_intent": setup_intent,
                "description": description,
            },
        )

    return catalog


def _workflow_to_dict(row: dict[str, Any], *, include_definition: bool = False) -> dict[str, Any]:
    # Extract definition type from the stored JSONB
    definition = _parse_json_field(row.get("definition")) or {}
    saved_compiled_spec = _parse_json_field(row.get("compiled_spec"))
    from runtime.operating_model_planner import current_compiled_spec

    current_plan = current_compiled_spec(
        definition,
        saved_compiled_spec,
    )
    definition_type = definition.get("type") if isinstance(definition, dict) else None

    workflow = {
        "id": row["id"],
        "name": row["name"],
        "description": row.get("description"),
        "definition_type": definition_type,  # 'operating_model' | 'pipeline' | None
        "has_spec": saved_compiled_spec is not None,
        "invocation_count": int(row.get("invocation_count") or 0),
        "last_invoked_at": _isoformat(row.get("last_invoked_at")),
        "tags": row.get("tags") or [],
        "version": int(row.get("version") or 1),
        "is_template": bool(row.get("is_template")),
        "created_at": _isoformat(row.get("created_at")),
        "updated_at": _isoformat(row.get("updated_at")),
    }
    if include_definition:
        workflow["definition"] = definition
        workflow["compiled_spec"] = saved_compiled_spec
        workflow["current_compiled_spec"] = current_plan
    return workflow


def _trigger_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "workflow_id": row["workflow_id"],
        "workflow_name": row.get("workflow_name"),
        "event_type": row["event_type"],
        "filter": _parse_json_field(row.get("filter")) or {},
        "enabled": bool(row.get("enabled", True)),
        "cron_expression": row.get("cron_expression"),
        "created_at": _isoformat(row.get("created_at")),
        "last_fired_at": _isoformat(row.get("last_fired_at")),
        "fire_count": int(row.get("fire_count") or 0),
    }


def _run_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    lineage = {
        "child_run_id": row["run_id"],
        "child_workflow_id": row.get("workflow_id"),
        "parent_run_id": row.get("parent_run_id"),
        "parent_job_label": row.get("parent_job_label"),
        "dispatch_reason": row.get("dispatch_reason"),
        "lineage_depth": int(row.get("lineage_depth") or 0),
    }
    return {
        "run_id": row["run_id"],
        "spec_name": row.get("spec_name"),
        "status": row.get("status"),
        "total_jobs": int(row.get("total_jobs") or 0),
        "created_at": _isoformat(row.get("created_at")),
        "finished_at": _isoformat(row.get("finished_at")),
        "parent_run_id": row.get("parent_run_id"),
        "parent_job_label": row.get("parent_job_label"),
        "dispatch_reason": row.get("dispatch_reason"),
        "lineage_depth": int(row.get("lineage_depth") or 0),
        "trigger_depth": int(row.get("trigger_depth") or 0),
        "lineage": lineage,
    }


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _workflow_dashboard_bucket(workflow: dict[str, Any]) -> str:
    trigger = workflow.get("trigger")
    if isinstance(trigger, dict) and bool(trigger.get("enabled")):
        return "live"
    if int(workflow.get("invocation_count") or 0) > 0:
        return "saved"
    return "draft"


def _workflow_dashboard_badge(workflow: dict[str, Any]) -> dict[str, str]:
    trigger = workflow.get("trigger")
    if isinstance(trigger, dict) and bool(trigger.get("enabled")) and trigger.get("cron_expression"):
        return {
            "label": "Scheduled",
            "tone": "scheduled",
            "class_name": "wf-card__badge--scheduled",
        }
    if isinstance(trigger, dict) and bool(trigger.get("enabled")):
        return {
            "label": "Live",
            "tone": "live",
            "class_name": "wf-card__badge--live",
        }
    if isinstance(trigger, dict) and not bool(trigger.get("enabled")):
        return {
            "label": "Paused",
            "tone": "paused",
            "class_name": "wf-card__badge--paused",
        }
    if int(workflow.get("invocation_count") or 0) > 0:
        return {
            "label": "Validated",
            "tone": "validated",
            "class_name": "wf-card__badge--validated",
        }
    return {
        "label": "Draft",
        "tone": "draft",
        "class_name": "wf-card__badge--draft",
    }


def _annotate_dashboard_workflow(workflow: dict[str, Any]) -> dict[str, Any]:
    annotated = dict(workflow)
    annotated["dashboard_bucket"] = _workflow_dashboard_bucket(annotated)
    annotated["dashboard_badge"] = _workflow_dashboard_badge(annotated)
    return annotated


def _load_workflow_inventory(pg: Any) -> list[dict[str, Any]]:
    rows = pg.execute(
        """SELECT w.id, w.name, w.description, w.definition, w.compiled_spec, w.tags,
                  w.version, w.is_template, w.invocation_count, w.last_invoked_at,
                  w.created_at, w.updated_at,
                  t.id AS trigger_id, t.event_type AS trigger_event, t.enabled AS trigger_enabled,
                  t.cron_expression, t.last_fired_at AS trigger_last_fired, t.fire_count AS trigger_fire_count,
                  r.run_id AS latest_run_id, r.spec_name AS latest_run_spec_name,
                  r.status AS latest_run_status, r.total_jobs AS latest_run_total_jobs,
                  r.created_at AS latest_run_created_at, r.finished_at AS latest_run_finished_at,
                  r.parent_run_id AS latest_run_parent_run_id,
                  r.trigger_depth AS latest_run_trigger_depth
           FROM public.workflows w
           LEFT JOIN public.workflow_triggers t ON t.workflow_id = w.id AND t.enabled = TRUE
           LEFT JOIN LATERAL (
               SELECT run_id,
                      COALESCE(request_envelope->>'name', workflow_id) AS spec_name,
                      current_state AS status,
                      COALESCE(NULLIF(request_envelope->>'total_jobs', ''), '0')::int AS total_jobs,
                      requested_at AS created_at,
                      finished_at,
                      request_envelope->>'parent_run_id' AS parent_run_id,
                      COALESCE(NULLIF(request_envelope->>'trigger_depth', ''), '0')::int AS trigger_depth
               FROM public.workflow_runs
               WHERE COALESCE(request_envelope->>'name', workflow_id) = w.name
               ORDER BY requested_at DESC
               LIMIT 1
           ) r ON TRUE
           ORDER BY w.updated_at DESC"""
    )
    seen: set[str] = set()
    workflows: list[dict[str, Any]] = []
    for row in (rows or []):
        record = dict(row)
        workflow_id = str(record["id"])
        if workflow_id in seen:
            continue
        seen.add(workflow_id)

        workflow = _workflow_to_dict(record)
        if record.get("trigger_id"):
            workflow["trigger"] = {
                "id": record["trigger_id"],
                "event_type": record["trigger_event"],
                "enabled": bool(record.get("trigger_enabled")),
                "cron_expression": record.get("cron_expression"),
                "last_fired_at": _isoformat(record.get("trigger_last_fired")),
                "fire_count": int(record.get("trigger_fire_count") or 0),
            }
        if record.get("latest_run_id"):
            workflow["latest_run"] = {
                "run_id": record["latest_run_id"],
                "spec_name": record.get("latest_run_spec_name"),
                "status": record.get("latest_run_status"),
                "total_jobs": int(record.get("latest_run_total_jobs") or 0),
                "created_at": _isoformat(record.get("latest_run_created_at")),
                "finished_at": _isoformat(record.get("latest_run_finished_at")),
                "parent_run_id": record.get("latest_run_parent_run_id"),
                "trigger_depth": int(record.get("latest_run_trigger_depth") or 0),
            }
        workflows.append(_annotate_dashboard_workflow(workflow))
    return workflows


def _load_leaderboard_snapshot(subs: Any, *, since_hours: int = 72) -> list[dict[str, Any]]:
    ingester = subs.get_receipt_ingester()
    receipts = ingester.load_recent(since_hours=since_hours)
    agents: dict[str, dict[str, int]] = {}
    for receipt in receipts:
        slug = receipt.get("agent_slug", receipt.get("agent", "unknown"))
        if slug not in agents:
            agents[slug] = {"total": 0, "succeeded": 0}
        agents[slug]["total"] += 1
        if receipt.get("status") == "succeeded":
            agents[slug]["succeeded"] += 1
    leaderboard: list[dict[str, Any]] = []
    for slug, stats in agents.items():
        parts = str(slug).split("/", 1)
        provider_slug = parts[0] if len(parts) == 2 else ""
        model_slug = parts[1] if len(parts) == 2 else str(slug)
        pass_rate = stats["succeeded"] / stats["total"] if stats["total"] else 0.0
        leaderboard.append({
            "provider_slug": provider_slug,
            "model_slug": model_slug,
            "pass_rate": round(pass_rate, 4),
            "total_workflows": stats["total"],
            "total_cost_usd": 0,
            "avg_latency_ms": 0,
        })
    leaderboard.sort(key=lambda item: (-item["pass_rate"], -item["total_workflows"]))
    return leaderboard


def _load_receipt_rollup(subs: Any, *, since_hours: int = 24) -> dict[str, Any]:
    ingester = subs.get_receipt_ingester()
    receipts = ingester.load_recent(since_hours=since_hours)
    pass_rate = ingester.compute_pass_rate(receipts)
    total_cost = round(sum(_safe_float(receipt.get("cost_usd")) for receipt in receipts), 4)
    return {
        "receipts": receipts,
        "pass_rate": round(pass_rate, 4) if pass_rate is not None else None,
        "top_failure_codes": ingester.top_failure_codes(receipts),
        "total_cost_usd": total_cost,
        "total_runs": len(receipts),
        "since_hours": since_hours,
    }


def _load_recent_runs_snapshot(pg: Any, *, limit: int = 20) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit or 20), 100))
    rows = pg.execute(
        """SELECT r.run_id,
                  COALESCE(r.request_envelope->>'name', r.workflow_id) AS spec_name,
                  r.current_state AS status,
                  COALESCE(NULLIF(r.request_envelope->>'total_jobs', ''), '0')::int AS total_jobs,
                  r.requested_at AS created_at,
                  r.finished_at,
                  COUNT(j.id) FILTER (WHERE j.status IN ('succeeded','failed','dead_letter')) as completed_jobs,
                  COALESCE(SUM(j.cost_usd), 0) as total_cost
           FROM public.workflow_runs r
           LEFT JOIN public.workflow_jobs j ON j.run_id = r.run_id
           GROUP BY r.run_id, r.workflow_id, r.request_envelope, r.current_state, r.requested_at, r.finished_at
           ORDER BY r.requested_at DESC
           LIMIT $1""",
        safe_limit,
    )
    result = []
    for row in (rows or []):
        result.append({
            "run_id": row["run_id"],
            "spec_name": row["spec_name"],
            "status": row["status"],
            "total_jobs": int(row["total_jobs"]),
            "completed_jobs": int(row["completed_jobs"]),
            "total_cost": float(row["total_cost"]),
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            "finished_at": row["finished_at"].isoformat() if row["finished_at"] else None,
        })
    return result


def _dashboard_health_descriptor(
    pass_rate: float | None,
    *,
    queue_status: str,
    queue_error: str | None,
) -> dict[str, str]:
    if queue_error or queue_status == "critical":
        return {
            "readiness": "recover",
            "label": "Recover",
            "tone": "danger",
            "copy": "Queue pressure or probe failures are strong enough that recovery should come before scale.",
        }
    if pass_rate is None:
        if queue_status == "warning":
            return {
                "readiness": "watch",
                "label": "Watch",
                "tone": "warning",
                "copy": "The queue is warming up while outcome receipts are still calibrating.",
            }
        return {
            "readiness": "calibrating",
            "label": "Calibrating",
            "tone": "neutral",
            "copy": "Metrics will harden as receipts and leaderboard data accumulate.",
        }
    if pass_rate >= 0.85 and queue_status == "ok":
        return {
            "readiness": "healthy",
            "label": "Healthy",
            "tone": "healthy",
            "copy": "Recent workflow outcomes are strong and the control plane looks settled.",
        }
    if pass_rate >= 0.6 and queue_status != "critical":
        return {
            "readiness": "watch",
            "label": "Watch",
            "tone": "warning",
            "copy": "The platform is moving, but recent results suggest a few lanes need attention.",
        }
    return {
        "readiness": "recover",
        "label": "Recover",
        "tone": "danger",
        "copy": "Recent outcomes are soft enough that recovery and inspection should come before scale.",
    }


def _build_dashboard_payload(subs: Any) -> dict[str, Any]:
    pg = subs.get_pg_conn() if hasattr(subs, "get_pg_conn") else None
    workflows = _load_workflow_inventory(pg) if pg is not None else []
    receipt_rollup = _load_receipt_rollup(subs)
    queue_snapshot = _queue_depth_snapshot(pg)
    leaderboard = _load_leaderboard_snapshot(subs)
    recent_runs = _load_recent_runs_snapshot(pg, limit=20) if pg is not None else []

    workflow_ids_by_bucket: dict[str, list[str]] = {key: [] for key in _DASHBOARD_SECTION_ORDER}
    for workflow in workflows:
        bucket = str(workflow.get("dashboard_bucket") or "draft")
        workflow_ids_by_bucket.setdefault(bucket, []).append(str(workflow["id"]))

    queue_status = str(queue_snapshot.get("queue_depth_status") or "unknown")
    queue_error = _optional_text(queue_snapshot.get("queue_depth_error"))
    health = _dashboard_health_descriptor(
        receipt_rollup["pass_rate"],
        queue_status=queue_status,
        queue_error=queue_error,
    )
    active_runs = sum(1 for run in recent_runs if str(run.get("status") or "") == "running")
    top_agent_row = leaderboard[0] if leaderboard else None
    top_agent = None
    if isinstance(top_agent_row, dict):
        provider = str(top_agent_row.get("provider_slug") or "").strip()
        model = str(top_agent_row.get("model_slug") or "").strip()
        top_agent = f"{provider}/{model}".strip("/") or None

    sections = [
        {
            "key": key,
            "count": len(workflow_ids_by_bucket.get(key, [])),
            "workflow_ids": workflow_ids_by_bucket.get(key, []),
        }
        for key in _DASHBOARD_SECTION_ORDER
    ]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "workflow_counts": {
                "total": len(workflows),
                "live": len(workflow_ids_by_bucket.get("live", [])),
                "saved": len(workflow_ids_by_bucket.get("saved", [])),
                "draft": len(workflow_ids_by_bucket.get("draft", [])),
            },
            "health": health,
            "runs_24h": int(receipt_rollup["total_runs"]),
            "active_runs": active_runs,
            "pass_rate_24h": receipt_rollup["pass_rate"],
            "total_cost_24h": receipt_rollup["total_cost_usd"],
            "top_agent": top_agent,
            "models_online": len(leaderboard),
            "queue": {
                "depth": int(queue_snapshot.get("queue_depth") or 0),
                "status": queue_status,
                "utilization_pct": _safe_float(queue_snapshot.get("queue_depth_utilization_pct")),
                "pending": int(queue_snapshot.get("queue_depth_pending") or 0),
                "ready": int(queue_snapshot.get("queue_depth_ready") or 0),
                "claimed": int(queue_snapshot.get("queue_depth_claimed") or 0),
                "running": int(queue_snapshot.get("queue_depth_running") or 0),
                "error": queue_error,
            },
        },
        "sections": sections,
        "workflows": workflows,
        "recent_runs": recent_runs,
        "leaderboard": leaderboard,
    }


def _fetch_workflow_runs(pg: Any, workflow_name: str, limit: int = 10) -> list[dict[str, Any]]:
    rows = pg.execute(
        """SELECT run_id,
                  COALESCE(request_envelope->>'name', workflow_id) AS spec_name,
                  current_state AS status,
                  COALESCE(NULLIF(request_envelope->>'total_jobs', ''), '0')::int AS total_jobs,
                  requested_at AS created_at,
                  finished_at,
                  request_envelope->>'parent_run_id' AS parent_run_id,
                  request_envelope->>'parent_job_label' AS parent_job_label,
                  request_envelope->>'dispatch_reason' AS dispatch_reason,
                  COALESCE(NULLIF(request_envelope->>'lineage_depth', ''), '0')::int AS lineage_depth,
                  COALESCE(NULLIF(request_envelope->>'trigger_depth', ''), '0')::int AS trigger_depth
           FROM public.workflow_runs
           WHERE COALESCE(request_envelope->>'name', workflow_id) = $1
           ORDER BY requested_at DESC
           LIMIT $2""",
        workflow_name,
        limit,
    )
    return [_run_to_dict(dict(row)) for row in (rows or [])]


def _fetch_run_packet_inspection(pg: Any, run_id: str) -> dict[str, Any] | None:
    if not isinstance(run_id, str) or not run_id.strip():
        return None
    try:
        rows = pg.execute(
            """
            SELECT
                wr.run_id,
                wr.workflow_id,
                wr.request_id,
                wr.workflow_definition_id,
                wr.current_state,
                wr.packet_inspection,
                wr.request_envelope,
                wr.requested_at,
                wr.admitted_at,
                wr.started_at,
                wr.finished_at,
                wr.last_event_id,
                COALESCE(
                    json_agg(ep.payload ORDER BY ep.created_at, ep.execution_packet_id)
                    FILTER (WHERE ep.execution_packet_id IS NOT NULL),
                    '[]'::jsonb
                ) AS packets
            FROM public.workflow_runs wr
            LEFT JOIN public.execution_packets ep
                ON ep.run_id = wr.run_id
            WHERE wr.run_id = $1
            GROUP BY
                wr.run_id,
                wr.workflow_id,
                wr.request_id,
                wr.workflow_definition_id,
                wr.current_state,
                wr.packet_inspection,
                wr.request_envelope,
                wr.requested_at,
                wr.admitted_at,
                wr.started_at,
                wr.finished_at,
                wr.last_event_id
            """,
            run_id,
        )
    except Exception:
        return None
    if not rows:
        return None

    row = dict(rows[0])
    try:
        from runtime.execution_packet_authority import (
            inspect_execution_packets,
            packet_inspection_from_row,
        )
    except Exception:
        return None
    materialized = packet_inspection_from_row(row)
    if materialized is not None:
        return materialized
    packets = _parse_json_field(row.get("packets"))
    if not isinstance(packets, list) or not packets:
        return None
    try:
        return inspect_execution_packets(packets, run_row=row)
    except Exception:
        return None


def _packet_revision_view(
    *,
    packet: dict[str, Any],
    drift: dict[str, Any] | None,
    run_id: str,
    run_status: str | None,
    requested_at: str | None,
    current_definition_revision: str | None,
    current_plan_revision: str | None,
) -> dict[str, Any]:
    packet_revision = str(packet.get("packet_revision") or "").strip() or None
    packet_hash = str(packet.get("packet_hash") or "").strip() or None
    packet_definition_revision = str(packet.get("definition_revision") or "").strip() or None
    packet_plan_revision = str(packet.get("plan_revision") or "").strip() or None
    drift_status = (
        str(drift.get("status") or "").strip() or None
        if isinstance(drift, dict)
        else None
    )
    drifted = bool(drift.get("is_drifted")) if isinstance(drift, dict) else False
    matches_current_definition = bool(
        packet_definition_revision
        and current_definition_revision
        and packet_definition_revision == current_definition_revision
    )
    matches_current_plan = bool(
        packet_plan_revision
        and current_plan_revision
        and packet_plan_revision == current_plan_revision
    )

    if drifted:
        status = "drifted"
    elif matches_current_definition and (current_plan_revision is None or matches_current_plan):
        status = "current"
    elif matches_current_definition:
        status = "stale_plan"
    elif current_plan_revision and matches_current_plan:
        status = "stale_definition"
    elif current_plan_revision:
        status = "stale_definition_and_plan"
    else:
        status = "stale_definition"

    return {
        "run_id": run_id,
        "run_status": run_status,
        "requested_at": requested_at,
        "packet_revision": packet_revision,
        "packet_hash": packet_hash,
        "definition_revision": packet_definition_revision,
        "plan_revision": packet_plan_revision,
        "drift_status": drift_status,
        "drifted": drifted,
        "status": status,
        "matches_current_definition": matches_current_definition,
        "matches_current_plan": matches_current_plan,
    }


def _workflow_revision_state(
    *,
    definition: Any,
    compiled_spec: Any,
    latest_runs: list[dict[str, Any]],
    pg: Any,
    workflow_name: str | None = None,
) -> dict[str, Any]:
    from runtime.operating_model_planner import current_compiled_spec

    definition_dict = _parse_json_field(definition) or {}
    compiled_spec_dict = _parse_json_field(compiled_spec)
    saved_definition_revision = (
        str(definition_dict.get("definition_revision") or "").strip() or None
        if isinstance(definition_dict, dict)
        else None
    )
    saved_plan_definition_revision = (
        str(compiled_spec_dict.get("definition_revision") or "").strip() or None
        if isinstance(compiled_spec_dict, dict)
        else None
    )
    saved_plan_revision = (
        str(compiled_spec_dict.get("plan_revision") or "").strip() or None
        if isinstance(compiled_spec_dict, dict)
        else None
    )
    saved_current_plan = (
        current_compiled_spec(definition_dict, compiled_spec_dict)
        if isinstance(definition_dict, dict)
        else None
    )
    current_plan = saved_current_plan
    current_definition_revision = saved_definition_revision
    current_plan_definition_revision = (
        str(current_plan.get("definition_revision") or "").strip() or None
        if isinstance(current_plan, dict)
        else None
    )
    current_plan_revision = (
        str(current_plan.get("plan_revision") or "").strip() or None
        if isinstance(current_plan, dict)
        else None
    )
    saved_plan_status = (
        "current"
        if saved_current_plan is not None
        else "stale" if (saved_plan_revision or saved_plan_definition_revision) else "missing"
    )
    current_plan_source = "saved" if current_plan is not None else "missing"

    current_packet = None
    for run in latest_runs:
        run_id = str(run.get("run_id") or "").strip()
        if not run_id:
            continue
        inspection = _fetch_run_packet_inspection(pg, run_id)
        packet = inspection.get("current_packet") if isinstance(inspection, dict) else None
        drift = inspection.get("drift") if isinstance(inspection, dict) else None
        if not isinstance(packet, dict):
            continue
        current_packet = _packet_revision_view(
            packet=packet,
            drift=drift if isinstance(drift, dict) else None,
            run_id=run_id,
            run_status=run.get("status"),
            requested_at=run.get("created_at"),
            current_definition_revision=current_definition_revision,
            current_plan_revision=current_plan_revision,
        )
        break

    return {
        "kind": "workflow_revision_state",
        "saved_definition_revision": saved_definition_revision,
        "saved_plan_definition_revision": saved_plan_definition_revision,
        "saved_plan_revision": saved_plan_revision,
        "saved_plan_status": saved_plan_status,
        "saved_plan": {
            "definition_revision": saved_plan_definition_revision,
            "plan_revision": saved_plan_revision,
            "status": saved_plan_status,
        },
        "current_plan_definition_revision": current_plan_definition_revision,
        "current_plan_revision": current_plan_revision,
        "current_plan_source": current_plan_source,
        "current_plan": {
            "definition_revision": current_plan_definition_revision,
            "plan_revision": current_plan_revision,
            "source": current_plan_source,
        },
        "current_packet": current_packet,
    }


def _workflow_build_subpath(path: str) -> tuple[str, str]:
    prefix = "/api/workflows/"
    if not path.startswith(prefix):
        raise _ClientError("workflow build path is invalid")
    suffix = path[len(prefix) :]
    workflow_id, separator, build_suffix = suffix.partition("/build")
    workflow_id = workflow_id.strip()
    if not separator or not workflow_id:
        raise _ClientError("workflow id is required")
    return workflow_id, build_suffix.lstrip("/")


def _load_workflow_build_row(pg: Any, workflow_id: str) -> dict[str, Any]:
    row = pg.fetchrow(
        "SELECT id, name, description, definition, compiled_spec, version, updated_at "
        "FROM public.workflows WHERE id = $1",
        workflow_id,
    )
    if row is None:
        raise _ClientError(f"Workflow not found: {workflow_id}")
    return dict(row)


def _workflow_build_payload(
    row: dict[str, Any],
    *,
    conn: Any | None = None,
    definition: dict[str, Any] | None = None,
    compiled_spec: dict[str, Any] | None = None,
    build_bundle: dict[str, Any] | None = None,
    planning_notes: list[str] | None = None,
    intent_brief: dict[str, Any] | None = None,
    execution_manifest: dict[str, Any] | None = None,
    undo_receipt: dict[str, Any] | None = None,
    mutation_event_id: int | None = None,
) -> dict[str, Any]:
    effective_definition = _parse_json_field(definition if definition is not None else row.get("definition")) or {}
    effective_compiled_spec = _parse_json_field(compiled_spec if compiled_spec is not None else row.get("compiled_spec"))
    if not isinstance(build_bundle, dict):
        from runtime.build_authority import build_authority_bundle
        from runtime.build_review_decisions import materialize_reviewed_build_definition
        from runtime.build_planning_contract import (
            build_candidate_resolution_manifest,
            build_intent_brief,
            build_reviewable_plan,
        )
        from runtime.operating_model_planner import current_compiled_spec
        from storage.postgres.workflow_build_planning_repository import (
            load_latest_workflow_build_execution_manifest,
        )

        current_plan = current_compiled_spec(effective_definition, effective_compiled_spec)
        if conn is not None:
            effective_definition, _ = materialize_reviewed_build_definition(
                conn,
                workflow_id=_text(row.get("id")),
                definition=effective_definition,
                compiled_spec=current_plan,
            )
        else:
            from runtime.build_authority import apply_authority_bundle

            effective_definition = apply_authority_bundle(effective_definition, compiled_spec=current_plan)
        effective_compiled_spec = current_plan
        build_bundle = build_authority_bundle(effective_definition, compiled_spec=current_plan)
        if not isinstance(intent_brief, dict):
            intent_brief = build_intent_brief(
                definition=effective_definition,
                workflow_id=_text(row.get("id")) or None,
                conn=conn,
            )
        if not isinstance(execution_manifest, dict) and conn is not None:
            definition_revision = _text(effective_definition.get("definition_revision"))
            if definition_revision and isinstance(effective_compiled_spec, dict):
                try:
                    execution_manifest = load_latest_workflow_build_execution_manifest(
                        conn,
                        workflow_id=_text(row.get("id")),
                        definition_revision=definition_revision,
                    )
                except Exception:
                    execution_manifest = None
    else:
        from runtime.build_planning_contract import (
            build_candidate_resolution_manifest,
            build_intent_brief,
            build_reviewable_plan,
        )
        if not isinstance(intent_brief, dict):
            intent_brief = build_intent_brief(
                definition=effective_definition,
                workflow_id=_text(row.get("id")) or None,
                conn=conn,
            )
    blocking_issues = [
        issue
        for issue in build_bundle.get("build_issues", [])
        if isinstance(issue, dict) and _text(issue.get("severity")) == "blocking"
    ]
    candidate_resolution_manifest = build_candidate_resolution_manifest(
        definition=effective_definition,
        workflow_id=_text(row.get("id")) or None,
        conn=conn,
        compiled_spec=effective_compiled_spec,
    )
    reviewable_plan = build_reviewable_plan(
        definition=effective_definition,
        workflow_id=_text(row.get("id")) or None,
        conn=conn,
        compiled_spec=effective_compiled_spec,
        candidate_manifest=candidate_resolution_manifest,
    )
    return {
        "workflow": {
            "id": row["id"],
            "name": row.get("name"),
            "description": row.get("description"),
            "version": int(row.get("version") or 1),
            "updated_at": _isoformat(row.get("updated_at")),
        },
        "intent_brief": intent_brief or {},
        "definition": effective_definition,
        "compiled_spec": effective_compiled_spec,
        "planning_notes": planning_notes or [],
        "build_state": _text(build_bundle.get("projection_status", {}).get("state")) or "blocked",
        "build_blockers": blocking_issues,
        "build_graph": build_bundle.get("build_graph"),
        "binding_ledger": build_bundle.get("binding_ledger") or [],
        "import_snapshots": build_bundle.get("import_snapshots") or [],
        "authority_attachments": build_bundle.get("authority_attachments") or [],
        "review_state": (
            _serialize(effective_definition.get("review_state"))
            if isinstance(effective_definition.get("review_state"), dict)
            else {}
        ),
        "build_issues": build_bundle.get("build_issues") or [],
        "projection_status": build_bundle.get("projection_status") or {},
        "compiled_spec_projection": build_bundle.get("compiled_spec_projection"),
        "candidate_resolution_manifest": candidate_resolution_manifest,
        "reviewable_plan": reviewable_plan,
        "execution_manifest": execution_manifest,
        "undo_receipt": undo_receipt,
        "mutation_event_id": mutation_event_id,
    }


def _validate_workflow_body(
    body: dict[str, Any],
    *,
    require_name: bool,
    require_definition: bool,
) -> str | None:
    workflow_id = body.get("id")
    if "id" in body and workflow_id is not None and not isinstance(workflow_id, str):
        return "id must be a string"

    name = body.get("name")
    if require_name and (not isinstance(name, str) or not name.strip()):
        return "name is required"
    if "name" in body and not isinstance(name, str):
        return "name must be a string"
    if "name" in body and isinstance(name, str) and not name.strip():
        return "name must be a non-empty string"

    definition = body.get("definition")
    build_graph = body.get("build_graph")
    if require_definition and not isinstance(definition, dict) and not isinstance(build_graph, dict):
        return "definition or build_graph is required and must be an object"
    if "definition" in body and definition is not None and not isinstance(definition, dict):
        return "definition must be an object"
    if "build_graph" in body and build_graph is not None and not isinstance(build_graph, dict):
        return "build_graph must be an object"

    compiled_spec = body.get("compiled_spec")
    if "compiled_spec" in body and compiled_spec is not None and not isinstance(compiled_spec, dict):
        return "compiled_spec must be an object"

    description = body.get("description")
    if "description" in body and description is not None and not isinstance(description, str):
        return "description must be a string"

    tags = body.get("tags")
    if "tags" in body and (
        not isinstance(tags, list) or not all(isinstance(tag, str) for tag in tags)
    ):
        return "tags must be a list of strings"

    is_template = body.get("is_template")
    if "is_template" in body and not isinstance(is_template, bool):
        return "is_template must be a boolean"

    return None


def _validate_trigger_body(
    body: dict[str, Any],
    *,
    require_workflow_id: bool,
    require_event_type: bool,
) -> str | None:
    trigger_id = body.get("id")
    if "id" in body and trigger_id is not None and not isinstance(trigger_id, str):
        return "id must be a string"

    workflow_id = body.get("workflow_id")
    if require_workflow_id and (not isinstance(workflow_id, str) or not workflow_id.strip()):
        return "workflow_id is required"
    if "workflow_id" in body and workflow_id is not None and not isinstance(workflow_id, str):
        return "workflow_id must be a string"
    if "workflow_id" in body and isinstance(workflow_id, str) and not workflow_id.strip():
        return "workflow_id must be a non-empty string"

    event_type = body.get("event_type")
    if require_event_type and (not isinstance(event_type, str) or not event_type.strip()):
        return "event_type is required"
    if "event_type" in body and event_type is not None and not isinstance(event_type, str):
        return "event_type must be a string"
    if "event_type" in body and isinstance(event_type, str) and not event_type.strip():
        return "event_type must be a non-empty string"

    trigger_filter = body.get("filter")
    if "filter" in body and trigger_filter is not None and not isinstance(trigger_filter, dict):
        return "filter must be an object"

    cron_expression = body.get("cron_expression")
    if "cron_expression" in body and cron_expression is not None and not isinstance(cron_expression, str):
        return "cron_expression must be a string"

    enabled = body.get("enabled")
    if "enabled" in body and not isinstance(enabled, bool):
        return "enabled must be a boolean"

    return None


def _handle_documents_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
    except (json.JSONDecodeError, ValueError) as exc:
        request._send_json(400, {"error": f"Invalid JSON: {exc}"})
        return

    try:
        if path == "/api/documents":
            request._send_json(
                200,
                create_document(
                    request.subsystems.get_pg_conn(),
                    title=body.get("title"),
                    content=body.get("content"),
                    doc_type=body.get("doc_type"),
                    tags=body.get("tags", []),
                    attached_to=body.get("attached_to", []),
                ),
            )
            return

        document_id = path.split("/api/documents/")[-1].rsplit("/attach", 1)[0]

        request._send_json(
            200,
            attach_document(
                request.subsystems.get_pg_conn(),
                document_id=document_id,
                card_id=body.get("card_id"),
            ),
        )
    except ObjectLifecycleBoundaryError as exc:
        request._send_json(exc.status_code, {"error": str(exc)})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_workflows_get(request: Any, path: str) -> None:
    if path == "/api/workflows":
        try:
            pg = request.subsystems.get_pg_conn()
            workflows = _load_workflow_inventory(pg)
            request._send_json(200, {"workflows": workflows, "count": len(workflows)})
        except Exception as exc:
            request._send_json(500, {"error": str(exc)})
        return

    try:
        workflow_id = path.split("/api/workflows/")[-1]
        pg = request.subsystems.get_pg_conn()
        row = pg.fetchrow("SELECT * FROM public.workflows WHERE id = $1", workflow_id)
        if row is None:
            request._send_json(404, {"error": f"Workflow not found: {workflow_id}"})
            return

        workflow = _workflow_to_dict(dict(row), include_definition=True)
        trigger_rows = pg.execute(
            """SELECT t.*, w.name AS workflow_name
               FROM public.workflow_triggers t
               JOIN public.workflows w ON w.id = t.workflow_id
               WHERE t.workflow_id = $1
               ORDER BY t.created_at DESC""",
            workflow_id,
        )
        workflow["triggers"] = [_trigger_to_dict(dict(trigger)) for trigger in (trigger_rows or [])]
        workflow["latest_runs"] = _fetch_workflow_runs(pg, workflow["name"], limit=10)
        workflow["revision_state"] = _workflow_revision_state(
            definition=workflow.get("definition"),
            compiled_spec=workflow.get("compiled_spec"),
            latest_runs=workflow["latest_runs"],
            pg=pg,
            workflow_name=workflow.get("name"),
        )
        request._send_json(200, {"workflow": workflow})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_workflows_runs_get(request: Any, path: str) -> None:
    try:
        workflow_id = path.split("/api/workflows/")[-1].rsplit("/runs", 1)[0]
        pg = request.subsystems.get_pg_conn()
        workflow = pg.fetchrow("SELECT id, name FROM public.workflows WHERE id = $1", workflow_id)
        if workflow is None:
            request._send_json(404, {"error": f"Workflow not found: {workflow_id}"})
            return

        params = _query_params(request.path)
        limit = int((params.get("limit") or ["20"])[0])
        runs = _fetch_workflow_runs(pg, workflow["name"], limit=limit)
        request._send_json(
            200,
            {
                "workflow_id": workflow["id"],
                "workflow_name": workflow["name"],
                "runs": runs,
                "count": len(runs),
            },
        )
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_workflows_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
    except (json.JSONDecodeError, ValueError) as exc:
        request._send_json(400, {"error": f"Invalid JSON: {exc}"})
        return

    try:
        if path == "/api/workflows":
            error = _validate_workflow_body(body, require_name=True, require_definition=True)
            if error:
                request._send_json(400, {"error": error})
                return
            row = save_workflow(
                request.subsystems.get_pg_conn(),
                workflow_id=None,
                body=body,
            )
            request._send_json(200, {"workflow": _workflow_to_dict(dict(row), include_definition=True)})
            return

        workflow_id = path.split("/api/workflows/")[-1]
        if not workflow_id:
            request._send_json(400, {"error": "workflow id is required"})
            return

        error = _validate_workflow_body(body, require_name=False, require_definition=False)
        if error:
            request._send_json(400, {"error": error})
            return
        if not body:
            request._send_json(400, {"error": "No workflow fields provided for update"})
            return

        row = save_workflow(
            request.subsystems.get_pg_conn(),
            workflow_id=workflow_id,
            body=body,
        )
        request._send_json(200, {"workflow": _workflow_to_dict(dict(row), include_definition=True)})
    except WorkflowRuntimeBoundaryError as exc:
        request._send_json(exc.status_code, {"error": str(exc)})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_workflow_triggers_get(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        rows = pg.execute(
            """SELECT t.*, w.name AS workflow_name
               FROM public.workflow_triggers t
               JOIN public.workflows w ON w.id = t.workflow_id
               ORDER BY t.created_at DESC"""
        )
        triggers = [_trigger_to_dict(dict(row)) for row in (rows or [])]
        request._send_json(200, {"triggers": triggers, "count": len(triggers)})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_workflow_triggers_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
    except (json.JSONDecodeError, ValueError) as exc:
        request._send_json(400, {"error": f"Invalid JSON: {exc}"})
        return

    try:
        if path == "/api/workflow-triggers":
            error = _validate_trigger_body(body, require_workflow_id=True, require_event_type=True)
            if error:
                request._send_json(400, {"error": error})
                return

            row = save_workflow_trigger(
                request.subsystems.get_pg_conn(),
                body=body,
            )
            request._send_json(200, {"trigger": _trigger_to_dict(dict(row))})
            return

        trigger_id = path.split("/api/workflow-triggers/")[-1]
        if not trigger_id:
            request._send_json(400, {"error": "trigger id is required"})
            return

        error = _validate_trigger_body(body, require_workflow_id=False, require_event_type=False)
        if error:
            request._send_json(400, {"error": error})
            return
        if not body:
            request._send_json(400, {"error": "No trigger fields provided for update"})
            return

        row = update_workflow_trigger(
            request.subsystems.get_pg_conn(),
            trigger_id=trigger_id,
            body=body,
        )
        request._send_json(200, {"trigger": _trigger_to_dict(dict(row))})
    except WorkflowRuntimeBoundaryError as exc:
        request._send_json(exc.status_code, {"error": str(exc)})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _load_compile_index_snapshot_for_request(request: Any):
    """Hydrate the compiler authority snapshot once at the request boundary.

    The compiler still binds against one durable snapshot, but stale or missing
    authority is refreshed here so normal repo drift does not strand the UI.
    """

    from runtime.compile_index import (
        CompileIndexAuthorityError,
        load_compile_index_snapshot,
        refresh_compile_index,
    )

    conn = request.subsystems.get_pg_conn()
    try:
        snapshot = load_compile_index_snapshot(
            conn,
            surface_name="compiler",
            require_fresh=True,
            repo_root=REPO_ROOT,
        )
    except CompileIndexAuthorityError as exc:
        if exc.reason_code not in _REFRESHABLE_COMPILE_INDEX_REASON_CODES:
            raise
        snapshot = refresh_compile_index(
            conn,
            repo_root=REPO_ROOT,
            surface_name="compiler",
        )
    return conn, snapshot


def _handle_compile_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
    except (json.JSONDecodeError, ValueError) as exc:
        payload = {"error": f"Invalid JSON: {exc}"}
        request._send_json(400, payload)
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=400,
            response_payload=payload,
            headers=request.headers,
        )
        return

    try:
        prose = body.get("prose", "")
        if not isinstance(prose, str) or not prose.strip():
            payload = {"error": "prose is required"}
            request._send_json(400, payload)
            _record_api_route_usage(
                request.subsystems,
                path=path,
                method="POST",
                status_code=400,
                request_body=body,
                response_payload=payload,
                headers=request.headers,
            )
            return
        title = body.get("title")
        if title is not None and not isinstance(title, str):
            payload = {"error": "title must be a string"}
            request._send_json(400, payload)
            _record_api_route_usage(
                request.subsystems,
                path=path,
                method="POST",
                status_code=400,
                request_body=body,
                response_payload=payload,
                headers=request.headers,
            )
            return

        try:
            conn, compile_index_snapshot = _load_compile_index_snapshot_for_request(request)
        except Exception as exc:
            from runtime.compile_index import CompileIndexAuthorityError

            if isinstance(exc, CompileIndexAuthorityError):
                payload = {
                    "error": str(exc),
                    "reason_code": exc.reason_code,
                    "details": dict(getattr(exc, "details", {}) or {}),
                }
                request._send_json(
                    409,
                    payload,
                )
                _record_api_route_usage(
                    request.subsystems,
                    path=path,
                    method="POST",
                    status_code=409,
                    request_body=body,
                    response_payload=payload,
                    headers=request.headers,
                    conn=getattr(request.subsystems, "get_pg_conn", lambda: None)(),
                )
                return
            raise
        from runtime.compiler import compile_prose

        result = compile_prose(
            prose,
            title=title,
            conn=conn,
            compile_index_snapshot=compile_index_snapshot,
        )
        request._send_json(200, result)
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=200,
            request_body=body,
            response_payload=result,
            headers=request.headers,
            conn=conn,
        )
    except Exception as exc:
        payload = {"error": str(exc)}
        request._send_json(500, payload)
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=500,
            request_body=body,
            response_payload=payload,
            headers=request.headers,
        )


def _handle_refine_definition_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
    except (json.JSONDecodeError, ValueError) as exc:
        payload = {"error": f"Invalid JSON: {exc}"}
        request._send_json(400, payload)
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=400,
            response_payload=payload,
            headers=request.headers,
        )
        return

    try:
        prose = body.get("prose", "")
        if not isinstance(prose, str) or not prose.strip():
            payload = {"error": "prose is required"}
            request._send_json(400, payload)
            _record_api_route_usage(
                request.subsystems,
                path=path,
                method="POST",
                status_code=400,
                request_body=body,
                response_payload=payload,
                headers=request.headers,
            )
            return
        title = body.get("title")
        if title is not None and not isinstance(title, str):
            payload = {"error": "title must be a string"}
            request._send_json(400, payload)
            _record_api_route_usage(
                request.subsystems,
                path=path,
                method="POST",
                status_code=400,
                request_body=body,
                response_payload=payload,
                headers=request.headers,
            )
            return

        try:
            conn, compile_index_snapshot = _load_compile_index_snapshot_for_request(request)
        except Exception as exc:
            from runtime.compile_index import CompileIndexAuthorityError

            if isinstance(exc, CompileIndexAuthorityError):
                payload = {
                    "error": str(exc),
                    "reason_code": exc.reason_code,
                    "details": dict(getattr(exc, "details", {}) or {}),
                }
                request._send_json(
                    409,
                    payload,
                )
                _record_api_route_usage(
                    request.subsystems,
                    path=path,
                    method="POST",
                    status_code=409,
                    request_body=body,
                    response_payload=payload,
                    headers=request.headers,
                    conn=getattr(request.subsystems, "get_pg_conn", lambda: None)(),
                )
                return
            raise
        from runtime.compiler import compile_prose

        result = compile_prose(
            prose,
            title=title,
            enable_llm=True,
            conn=conn,
            compile_index_snapshot=compile_index_snapshot,
        )
        request._send_json(200, result)
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=200,
            request_body=body,
            response_payload=result,
            headers=request.headers,
            conn=conn,
        )
    except Exception as exc:
        payload = {"error": str(exc)}
        request._send_json(500, payload)
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=500,
            request_body=body,
            response_payload=payload,
            headers=request.headers,
        )


def _handle_plan_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
    except (json.JSONDecodeError, ValueError) as exc:
        payload = {"error": f"Invalid JSON: {exc}"}
        request._send_json(400, payload)
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=400,
            response_payload=payload,
            headers=request.headers,
        )
        return

    try:
        definition = body.get("definition")
        build_graph = body.get("build_graph")
        if not isinstance(definition, dict) and not isinstance(build_graph, dict):
            payload = {"error": "definition or build_graph is required and must be an object"}
            request._send_json(400, payload)
            _record_api_route_usage(
                request.subsystems,
                path=path,
                method="POST",
                status_code=400,
                request_body=body,
                response_payload=payload,
                headers=request.headers,
            )
            return
        if isinstance(build_graph, dict):
            definition = materialize_definition_from_build_graph(
                definition if isinstance(definition, dict) else {},
                build_graph=build_graph,
            )
        title = body.get("title")
        if title is not None and not isinstance(title, str):
            payload = {"error": "title must be a string"}
            request._send_json(400, payload)
            _record_api_route_usage(
                request.subsystems,
                path=path,
                method="POST",
                status_code=400,
                request_body=body,
                response_payload=payload,
                headers=request.headers,
            )
            return

        from runtime.operating_model_planner import PlanningBlockedError, plan_definition

        try:
            result = plan_definition(definition, title=title, conn=request.subsystems.get_pg_conn())
        except PlanningBlockedError as exc:
            payload = {"error": str(exc), "unresolved": exc.unresolved}
            request._send_json(400, payload)
            _record_api_route_usage(
                request.subsystems,
                path=path,
                method="POST",
                status_code=400,
                request_body=body,
                response_payload=payload,
                headers=request.headers,
            )
            return

        request._send_json(200, result)
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=200,
            request_body=body,
            response_payload=result,
            headers=request.headers,
        )
    except Exception as exc:
        payload = {"error": str(exc)}
        request._send_json(500, payload)
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=500,
            request_body=body,
            response_payload=payload,
            headers=request.headers,
        )


def _handle_commit_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
    except (json.JSONDecodeError, ValueError) as exc:
        payload = {"error": f"Invalid JSON: {exc}"}
        request._send_json(400, payload)
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=400,
            response_payload=payload,
            headers=request.headers,
        )
        return

    try:
        title = body.get("title", "")
        if not isinstance(title, str) or not title.strip():
            payload = {"error": "title is required"}
            request._send_json(400, payload)
            _record_api_route_usage(
                request.subsystems,
                path=path,
                method="POST",
                status_code=400,
                request_body=body,
                response_payload=payload,
                headers=request.headers,
            )
            return
        definition = body.get("definition")
        build_graph = body.get("build_graph")
        if not isinstance(definition, dict) and not isinstance(build_graph, dict):
            payload = {"error": "definition or build_graph is required and must be an object"}
            request._send_json(400, payload)
            _record_api_route_usage(
                request.subsystems,
                path=path,
                method="POST",
                status_code=400,
                request_body=body,
                response_payload=payload,
                headers=request.headers,
            )
            return
        if "build_graph" in body and build_graph is not None and not isinstance(build_graph, dict):
            payload = {"error": "build_graph must be an object"}
            request._send_json(400, payload)
            _record_api_route_usage(
                request.subsystems,
                path=path,
                method="POST",
                status_code=400,
                request_body=body,
                response_payload=payload,
                headers=request.headers,
            )
            return
        compiled_spec = body.get("compiled_spec")
        if "compiled_spec" in body and compiled_spec is not None and not isinstance(compiled_spec, dict):
            payload = {"error": "compiled_spec must be an object"}
            request._send_json(400, payload)
            _record_api_route_usage(
                request.subsystems,
                path=path,
                method="POST",
                status_code=400,
                request_body=body,
                response_payload=payload,
                headers=request.headers,
            )
            return
        workflow_id = body.get("workflow_id")
        if workflow_id is not None and not isinstance(workflow_id, str):
            payload = {"error": "workflow_id must be a string"}
            request._send_json(400, payload)
            _record_api_route_usage(
                request.subsystems,
                path=path,
                method="POST",
                status_code=400,
                request_body=body,
                response_payload=payload,
                headers=request.headers,
            )
            return

        result = commit_workflow(
            request.subsystems.get_pg_conn(),
            title=title.strip(),
            definition=definition if isinstance(definition, dict) else None,
            compiled_spec=compiled_spec,
            workflow_id=workflow_id,
            build_graph=build_graph if isinstance(build_graph, dict) else None,
        )
        request._send_json(200, result)
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=200,
            request_body=body,
            response_payload=result,
            headers=request.headers,
        )
    except WorkflowRuntimeBoundaryError as exc:
        payload = {"error": str(exc)}
        request._send_json(exc.status_code, payload)
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=exc.status_code,
            request_body=body,
            response_payload=payload,
            headers=request.headers,
        )
    except Exception as exc:
        payload = {"error": str(exc)}
        request._send_json(500, payload)
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=500,
            request_body=body,
            response_payload=payload,
            headers=request.headers,
        )


def _handle_workflow_build_get(request: Any, path: str) -> None:
    try:
        workflow_id, subpath = _workflow_build_subpath(path)
        if subpath:
            request._send_json(404, {"error": f"Unknown build endpoint: {path}"})
            return
        pg = request.subsystems.get_pg_conn()
        row = _load_workflow_build_row(pg, workflow_id)
        request._send_json(200, _workflow_build_payload(row, conn=pg))
    except _ClientError as exc:
        message = str(exc)
        status = 404 if message.startswith("Workflow not found:") else 400
        request._send_json(status, {"error": message})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_workflow_build_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
    except (json.JSONDecodeError, ValueError) as exc:
        request._send_json(400, {"error": f"Invalid JSON: {exc}"})
        return

    try:
        workflow_id, subpath = _workflow_build_subpath(path)
        result = mutate_workflow_build(
            request.subsystems.get_pg_conn(),
            workflow_id=workflow_id,
            subpath=subpath,
            body=body,
        )
        request._send_json(
            200,
            _workflow_build_payload(
                result["row"],
                conn=request.subsystems.get_pg_conn(),
                definition=result["definition"],
                compiled_spec=result["compiled_spec"],
                build_bundle=result["build_bundle"],
                planning_notes=result["planning_notes"],
                intent_brief=result.get("intent_brief"),
                execution_manifest=result.get("execution_manifest"),
                undo_receipt=result.get("undo_receipt"),
                mutation_event_id=result.get("mutation_event_id"),
            ),
        )
    except WorkflowRuntimeBoundaryError as exc:
        request._send_json(exc.status_code, {"error": str(exc)})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_object_types_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
    except (json.JSONDecodeError, ValueError) as exc:
        request._send_json(400, {"error": f"Invalid JSON: {exc}"})
        return

    try:
        request._send_json(
            200,
            create_object_type(
                request.subsystems.get_pg_conn(),
                name=body.get("name"),
                description=body.get("description", ""),
                property_definitions=body.get("property_definitions", {}),
                icon=body.get("icon", ""),
            ),
        )
    except ObjectLifecycleBoundaryError as exc:
        request._send_json(exc.status_code, {"error": str(exc)})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_objects_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
    except (json.JSONDecodeError, ValueError) as exc:
        request._send_json(400, {"error": f"Invalid JSON: {exc}"})
        return

    try:
        if path == "/api/objects":
            request._send_json(
                200,
                create_object(
                    request.subsystems.get_pg_conn(),
                    type_id=body.get("type_id"),
                    properties=body.get("properties", {}),
                ),
            )
            return

        if path == "/api/objects/update":
            request._send_json(
                200,
                update_object(
                    request.subsystems.get_pg_conn(),
                    object_id=body.get("object_id"),
                    properties=body.get("properties", {}),
                ),
            )
            return

        request._send_json(
            200,
            delete_object(
                request.subsystems.get_pg_conn(),
                object_id=body.get("object_id"),
            ),
        )
    except ObjectLifecycleBoundaryError as exc:
        request._send_json(exc.status_code, {"error": str(exc)})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_templates_get(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        params = _query_params(request.path)
        query = (params.get("q", [""])[0]).strip()

        if query:
            rows = pg.execute(
                """SELECT id, name, description, status FROM app_manifests
                        WHERE search_vector @@ plainto_tsquery('english', $1)
                           OR name ILIKE '%' || $1 || '%'
                           OR description ILIKE '%' || $1 || '%'
                        ORDER BY name LIMIT 20""",
                query,
            )
        else:
            rows = pg.execute(
                "SELECT id, name, description, status FROM app_manifests WHERE status = 'active' ORDER BY name LIMIT 20"
            )
        request._send_json(
            200,
            {
                "templates": [
                    {
                        "id": row["id"],
                        "name": row["name"],
                        "description": row.get("description") or "",
                        "status": row["status"],
                    }
                    for row in rows
                ],
                "count": len(rows),
            },
        )
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _manifest_family_from_payload(manifest: Any) -> str | None:
    if isinstance(manifest, str):
        try:
            manifest = json.loads(manifest)
        except (TypeError, json.JSONDecodeError):
            return None
    if not isinstance(manifest, dict):
        return None
    return str(manifest.get("manifest_family") or "").strip() or None


def _manifest_type_from_payload(manifest: Any) -> str | None:
    if isinstance(manifest, str):
        try:
            manifest = json.loads(manifest)
        except (TypeError, json.JSONDecodeError):
            return None
    if not isinstance(manifest, dict):
        return None
    return str(manifest.get("manifest_type") or "").strip() or None


def _manifest_text_from_payload(manifest: Any, *path: str) -> str | None:
    if isinstance(manifest, str):
        try:
            manifest = json.loads(manifest)
        except (TypeError, json.JSONDecodeError):
            return None
    current: Any = manifest
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return str(current or "").strip() or None


def _control_manifest_workspace_ref(manifest: Any) -> str | None:
    for path in (
        ("workspace_ref",),
        ("plan", "workspace_ref"),
        ("approval", "workspace_ref"),
        ("job", "workspace_ref"),
    ):
        value = _manifest_text_from_payload(manifest, *path)
        if value:
            return value
    return None


def _control_manifest_scope_ref(manifest: Any) -> str | None:
    for path in (
        ("scope_ref",),
        ("plan", "scope_ref"),
        ("approval", "scope_ref"),
        ("job", "scope_ref"),
    ):
        value = _manifest_text_from_payload(manifest, *path)
        if value:
            return value
    return None


def _control_manifest_row_base(row: dict[str, Any], *, manifest_key: str) -> dict[str, Any]:
    manifest = row.get(manifest_key)
    return {
        "id": row.get("id"),
        "name": row.get("name"),
        "description": row.get("description") or "",
        "status": row.get("status"),
        "version": row.get("version"),
        "parent_manifest_id": row.get("parent_manifest_id"),
        "kind": _manifest_text_from_payload(manifest, "kind"),
        "manifest_family": _manifest_family_from_payload(manifest),
        "manifest_type": _manifest_type_from_payload(manifest),
        "workspace_ref": _control_manifest_workspace_ref(manifest),
        "scope_ref": _control_manifest_scope_ref(manifest),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


def _manifest_listing_row(row: dict[str, Any]) -> dict[str, Any]:
    manifest = row.get("manifest")
    return {
        "id": row.get("id"),
        "name": row.get("name"),
        "description": row.get("description") or "",
        "status": row.get("status"),
        "manifest_family": _manifest_family_from_payload(manifest),
        "manifest_type": _manifest_type_from_payload(manifest),
        "updated_at": row.get("updated_at"),
    }


def _control_manifest_filter_sql(column: str, field: str) -> str:
    return (
        "COALESCE("
        f"{column}->>'{field}', "
        f"{column}->'plan'->>'{field}', "
        f"{column}->'approval'->>'{field}', "
        f"{column}->'job'->>'{field}', "
        "''"
        ")"
    )


def _handle_manifest_heads_get(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        params = _query_params(request.path)
        workspace_ref = str((params.get("workspace_ref") or [""])[0]).strip()
        scope_ref = str((params.get("scope_ref") or [""])[0]).strip()
        manifest_type = str((params.get("manifest_type") or [""])[0]).strip()
        status = str((params.get("status") or [""])[0]).strip()
        limit = _safe_int_impl((params.get("limit") or ["20"])[0], default=20, minimum=1, maximum=100)
        rows = _list_control_manifest_heads(
            pg,
            workspace_ref=workspace_ref or None,
            scope_ref=scope_ref or None,
            manifest_type=manifest_type or None,
            status=status or None,
            limit=limit,
        )
        request._send_json(
            200,
            _serialize(
                {
                    "heads": rows,
                    "count": len(rows),
                    "filters": {
                        "workspace_ref": workspace_ref or None,
                        "scope_ref": scope_ref or None,
                        "manifest_type": manifest_type or None,
                        "status": status or None,
                        "limit": limit,
                    },
                }
            ),
        )
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_manifest_history_get(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        params = _query_params(request.path)
        workspace_ref = str((params.get("workspace_ref") or [""])[0]).strip()
        scope_ref = str((params.get("scope_ref") or [""])[0]).strip()
        manifest_type = str((params.get("manifest_type") or [""])[0]).strip()
        status = str((params.get("status") or [""])[0]).strip()
        limit = _safe_int_impl((params.get("limit") or ["20"])[0], default=20, minimum=1, maximum=100)
        if not workspace_ref or not scope_ref or not manifest_type:
            request._send_json(
                400,
                {
                    "error": "workspace_ref, scope_ref, and manifest_type are required for control manifest history",
                },
            )
            return

        rows = _list_control_manifest_history(
            pg,
            workspace_ref=workspace_ref,
            scope_ref=scope_ref,
            manifest_type=manifest_type,
            status=status or None,
            limit=limit,
        )
        request._send_json(
            200,
            _serialize(
                {
                    "history": rows,
                    "count": len(rows),
                    "filters": {
                        "workspace_ref": workspace_ref or None,
                        "scope_ref": scope_ref or None,
                        "manifest_type": manifest_type or None,
                        "status": status or None,
                        "limit": limit,
                    },
                }
            ),
        )
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_manifests_get(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        params = _query_params(request.path)
        query = str((params.get("q") or [""])[0]).strip()
        manifest_family = str((params.get("manifest_family") or [""])[0]).strip()
        manifest_type = str((params.get("manifest_type") or [""])[0]).strip()
        status = str((params.get("status") or [""])[0]).strip()
        limit = _safe_int_impl((params.get("limit") or ["20"])[0], default=20, minimum=1, maximum=100)

        sql = (
            "SELECT id, name, description, status, manifest, updated_at "
            "FROM app_manifests WHERE 1=1"
        )
        sql_params: list[Any] = []
        if query:
            sql_params.append(query)
            sql += (
                " AND (search_vector @@ plainto_tsquery('english', $1)"
                " OR name ILIKE '%' || $1 || '%'"
                " OR description ILIKE '%' || $1 || '%'"
                " OR manifest::text ILIKE '%' || $1 || '%')"
            )
        if manifest_family:
            sql_params.append(manifest_family)
            sql += f" AND manifest->>'manifest_family' = ${len(sql_params)}"
        if manifest_type:
            sql_params.append(manifest_type)
            sql += f" AND manifest->>'manifest_type' = ${len(sql_params)}"
        if status:
            sql_params.append(status)
            sql += f" AND status = ${len(sql_params)}"
        sql_params.append(limit)
        sql += f" ORDER BY updated_at DESC, name ASC LIMIT ${len(sql_params)}"

        rows = pg.execute(sql, *sql_params)
        request._send_json(
            200,
            _serialize(
                {
                    "manifests": [_manifest_listing_row(dict(row)) for row in rows],
                    "count": len(rows),
                    "filters": {
                        "q": query or None,
                        "manifest_family": manifest_family or None,
                    "manifest_type": manifest_type or None,
                        "status": status or None,
                        "limit": limit,
                    },
                }
            ),
        )
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_models_get(request: Any, path: str) -> None:
    try:
        params = _query_params(request.path)
        requested_task_type = str((params.get("task_type") or [""])[0]).strip().lower()
        pg = request.subsystems.get_pg_conn()
        ranked_routes: dict[tuple[str, str], int] = {}
        if requested_task_type:
            from runtime.task_type_router import TaskTypeRouter

            chain = TaskTypeRouter(pg).resolve_failover_chain(f"auto/{requested_task_type}")
            ranked_routes = {
                (str(decision.provider_slug), str(decision.model_slug)): index
                for index, decision in enumerate(chain, start=1)
            }
        rows = pg.execute(
            """
            SELECT DISTINCT ON (provider_slug, model_slug)
                   provider_slug,
                   model_slug,
                   status,
                   capability_tags,
                   route_tier,
                   route_tier_rank,
                   latency_class,
                   latency_rank,
                   reasoning_control,
                   task_affinities,
                   benchmark_profile
            FROM provider_model_candidates
            WHERE status = 'active'
            ORDER BY provider_slug, model_slug, priority ASC, created_at DESC
            """
        )
        models = []
        for row in rows:
            route_key = (str(row["provider_slug"]), str(row["model_slug"]))
            if ranked_routes and route_key not in ranked_routes:
                continue
            payload = {
                "provider": row["provider_slug"],
                "model": row["model_slug"],
                "slug": f"{row['provider_slug']}/{row['model_slug']}",
                "capabilities": row["capability_tags"],
                "route_tier": row["route_tier"],
                "route_tier_rank": row["route_tier_rank"],
                "latency_class": row["latency_class"],
                "latency_rank": row["latency_rank"],
                "reasoning_control": row["reasoning_control"],
                "task_affinities": row["task_affinities"],
                "benchmark_profile": row["benchmark_profile"],
            }
            if ranked_routes:
                payload["route_rank"] = ranked_routes[route_key]
            models.append(payload)
        if ranked_routes:
            models.sort(
                key=lambda item: (
                    int(item.get("route_rank") or 999),
                    str(item["slug"]),
                )
            )
        request._send_json(200, {"models": models})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_market_models_get(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        params = _query_params(request.path)
        query = str((params.get("q") or [""])[0]).strip().lower()
        source_filter = str((params.get("source") or [""])[0]).strip().lower()
        creator_filter = str((params.get("creator") or [""])[0]).strip().lower()
        family_filter = str((params.get("family") or [""])[0]).strip().lower()
        binding_filter = str((params.get("binding") or ["all"])[0]).strip().lower() or "all"
        sort_key = str((params.get("sort") or ["creator"])[0]).strip().lower() or "creator"
        limit = _safe_int_impl((params.get("limit") or ["100"])[0], default=100, minimum=1, maximum=500)
        offset = _safe_int_impl((params.get("offset") or ["0"])[0], default=0, minimum=0, maximum=10000)
        market_rows = pg.execute(
            """
            SELECT market_model_ref, source_slug, modality, source_model_id,
                   source_model_slug, model_name, creator_slug, creator_name,
                   evaluations, pricing, speed_metrics, prompt_options,
                   last_synced_at
            FROM market_model_registry
            WHERE modality = 'llm'
            ORDER BY creator_slug, source_model_slug
            """
        )
        binding_rows = pg.execute(
            """
            SELECT b.market_model_ref,
                   b.binding_kind,
                   b.binding_confidence,
                   c.candidate_ref,
                   c.provider_slug,
                   c.model_slug
            FROM provider_model_market_bindings AS b
            JOIN provider_model_candidates AS c
              ON c.candidate_ref = b.candidate_ref
            WHERE c.status = 'active'
            ORDER BY b.market_model_ref, c.provider_slug, c.model_slug
            """
        )
        bindings_by_market_ref: dict[str, list[dict[str, Any]]] = {}
        for row in binding_rows:
            bindings_by_market_ref.setdefault(str(row["market_model_ref"]), []).append(
                {
                    "candidate_ref": row["candidate_ref"],
                    "provider": row["provider_slug"],
                    "model": row["model_slug"],
                    "slug": f"{row['provider_slug']}/{row['model_slug']}",
                    "binding_kind": row["binding_kind"],
                    "binding_confidence": row["binding_confidence"],
                }
            )
        enriched_rows: list[dict[str, Any]] = []
        for row in market_rows:
            local_bindings = bindings_by_market_ref.get(str(row["market_model_ref"]), [])
            binding_status = "bound" if local_bindings else "unbound"
            model = {
                "market_model_ref": row["market_model_ref"],
                "source": row["source_slug"],
                "modality": row["modality"],
                "source_model_id": row["source_model_id"],
                "source_model_slug": row["source_model_slug"],
                "name": row["model_name"],
                "creator_slug": row["creator_slug"],
                "creator_name": row["creator_name"],
                "family_slug": _market_model_family_slug(dict(row)),
                "binding_status": binding_status,
                "local_binding_count": len(local_bindings),
                "evaluations": row["evaluations"],
                "pricing": row["pricing"],
                "speed_metrics": row["speed_metrics"],
                "prompt_options": row["prompt_options"],
                "review_metrics": _market_review_metrics(dict(row)),
                "last_synced_at": (
                    str(row["last_synced_at"]) if row["last_synced_at"] else ""
                ),
                "local_bindings": local_bindings,
            }
            if source_filter and str(model["source"]).strip().lower() != source_filter:
                continue
            if creator_filter and str(model["creator_slug"]).strip().lower() != creator_filter:
                continue
            if family_filter and str(model["family_slug"]).strip().lower() != family_filter:
                continue
            if binding_filter == "bound" and binding_status != "bound":
                continue
            if binding_filter == "unbound" and binding_status != "unbound":
                continue
            if not _market_model_matches_query(model, query):
                continue
            enriched_rows.append(model)

        def _sort_value(item: dict[str, Any]) -> tuple[Any, ...]:
            metrics = item.get("review_metrics")
            if not isinstance(metrics, dict):
                metrics = {}
            if sort_key == "intelligence_desc":
                return (
                    -(metrics.get("intelligence_index") or -1e9),
                    str(item["creator_slug"]),
                    str(item["source_model_slug"]),
                )
            if sort_key == "coding_desc":
                return (
                    -(metrics.get("coding_index") or -1e9),
                    str(item["creator_slug"]),
                    str(item["source_model_slug"]),
                )
            if sort_key == "price_asc":
                return (
                    (metrics.get("price_1m_blended_3_to_1") or 1e9),
                    str(item["creator_slug"]),
                    str(item["source_model_slug"]),
                )
            if sort_key == "speed_desc":
                return (
                    -(metrics.get("median_output_tokens_per_second") or -1e9),
                    str(item["creator_slug"]),
                    str(item["source_model_slug"]),
                )
            return (
                str(item["creator_slug"]),
                str(item["family_slug"]),
                str(item["source_model_slug"]),
            )

        enriched_rows.sort(key=_sort_value)
        paged_rows = enriched_rows[offset: offset + limit]

        creator_facets: dict[tuple[str, str], dict[str, Any]] = {}
        family_facets: dict[str, dict[str, Any]] = {}
        bound_count = 0
        for row in enriched_rows:
            is_bound = row["binding_status"] == "bound"
            if is_bound:
                bound_count += 1
            creator_key = (str(row["creator_slug"]), str(row["creator_name"]))
            creator_bucket = creator_facets.setdefault(
                creator_key,
                {
                    "creator_slug": row["creator_slug"],
                    "creator_name": row["creator_name"],
                    "count": 0,
                    "bound_count": 0,
                    "unbound_count": 0,
                },
            )
            creator_bucket["count"] += 1
            creator_bucket["bound_count" if is_bound else "unbound_count"] += 1
            family_key = str(row["family_slug"])
            family_bucket = family_facets.setdefault(
                family_key,
                {
                    "family_slug": family_key,
                    "count": 0,
                    "bound_count": 0,
                    "unbound_count": 0,
                },
            )
            family_bucket["count"] += 1
            family_bucket["bound_count" if is_bound else "unbound_count"] += 1

        creator_facet_rows = sorted(
            creator_facets.values(),
            key=lambda item: (-int(item["count"]), str(item["creator_slug"])),
        )
        family_facet_rows = sorted(
            family_facets.values(),
            key=lambda item: (-int(item["count"]), str(item["family_slug"])),
        )
        request._send_json(
            200,
            {
                "models": paged_rows,
                "count": len(enriched_rows),
                "page_count": len(paged_rows),
                "filtered_count": len(enriched_rows),
                "total_count": len(market_rows),
                "offset": offset,
                "limit": limit,
                "filters": {
                    "q": query,
                    "source": source_filter,
                    "creator": creator_filter,
                    "family": family_filter,
                    "binding": binding_filter,
                    "sort": sort_key,
                },
                "review": {
                    "total_market_models": len(market_rows),
                    "filtered_market_models": len(enriched_rows),
                    "bound_market_models": bound_count,
                    "unbound_market_models": len(enriched_rows) - bound_count,
                    "returned_market_models": len(paged_rows),
                },
                "facets": {
                    "binding_status": [
                        {"value": "bound", "count": bound_count},
                        {"value": "unbound", "count": len(enriched_rows) - bound_count},
                    ],
                    "creators": creator_facet_rows[:25],
                    "families": family_facet_rows[:25],
                },
            },
        )
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_references_get(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        rows = pg.execute(
            "SELECT slug, ref_type, display_name, description "
            "FROM reference_catalog ORDER BY ref_type, slug"
        )
        refs = [dict(row) for row in (rows or [])]
        request._send_json(200, {"references": refs, "count": len(refs)})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_source_options_get(request: Any, path: str) -> None:
    try:
        params = _query_params(request.path)
        manifest_id = (params.get("manifest_id") or [""])[0].strip()
        tab_id = (params.get("tab_id") or [""])[0].strip() or None
        pg = request.subsystems.get_pg_conn()

        catalog = _source_option_catalog(pg)

        if manifest_id:
            row = pg.fetchrow(
                "SELECT id, name, description, manifest FROM app_manifests WHERE id = $1",
                manifest_id,
            )
            if row is None:
                request._send_json(404, {"error": f"Manifest not found: {manifest_id}"})
                return

            bundle = normalize_helm_bundle(
                _parse_json_field(row.get("manifest")),
                manifest_id=row["id"],
                name=row.get("name"),
                description=row.get("description"),
            )
            selected_tab = resolve_tab(bundle, tab_id)
            local_options = {
                option_id: normalize_source_option(option_id, raw_option)
                for option_id, raw_option in (bundle.get("source_options") or {}).items()
                if isinstance(option_id, str)
            }
            merged = {**catalog, **local_options}
            ordered_ids = [
                option_id
                for option_id in (
                    selected_tab.get("source_option_ids")
                    if isinstance(selected_tab, dict) and isinstance(selected_tab.get("source_option_ids"), list)
                    else []
                )
                if isinstance(option_id, str) and option_id in merged
            ]
            request._send_json(
                200,
                {
                    "manifest_id": manifest_id,
                    "tab_id": selected_tab.get("id") if isinstance(selected_tab, dict) else tab_id,
                    "source_options": [merged[option_id] for option_id in ordered_ids],
                    "count": len(ordered_ids),
                    "catalog_count": len(merged),
                },
            )
            return

        options = list(catalog.values())
        request._send_json(200, {"source_options": options, "count": len(options)})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_integrations_get(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        rows = pg.execute(
            "SELECT id, name, description, provider, capabilities, auth_status, icon FROM integration_registry ORDER BY name"
        )
        integrations = []
        for row in rows:
            item = dict(row)
            item["name"] = base_integration_name(item)
            item["display_name"] = display_name_for_integration(item)
            integrations.append(item)
        request._send_json(
            200,
            {
                "integrations": integrations,
                "count": len(integrations),
            },
        )
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_catalog_get(request: Any, path: str) -> None:
    """Return live catalog items from platform registries + static primitives."""
    try:
        pg = request.subsystems.get_pg_conn()
        request._send_json(200, build_catalog_payload(pg))
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_catalog_review_decisions_get(request: Any, path: str) -> None:
    try:
        params = _query_params(request.path)
        surface_name = (params.get("surface") or ["moon"])[0].strip() or "moon"
        target_kind = (params.get("target_kind") or [""])[0].strip() or None
        target_ref = (params.get("target_ref") or [""])[0].strip() or None
        pg = request.subsystems.get_pg_conn()
        decisions = list_surface_catalog_reviews(
            pg,
            surface_name=surface_name,
            target_kind=target_kind,
            target_ref=target_ref,
        )
        request._send_json(
            200,
            {
                "surface_name": surface_name,
                "filters": {
                    key: value
                    for key, value in {
                        "target_kind": target_kind,
                        "target_ref": target_ref,
                    }.items()
                    if value is not None
                },
                "review_decisions": [_serialize(item) for item in decisions],
                "count": len(decisions),
            },
        )
    except PostgresWriteError as exc:
        request._send_json(
            400,
            {
                "error": str(exc),
                "reason_code": exc.reason_code,
                "details": _serialize(exc.details),
            },
        )
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_catalog_review_decisions_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
    except (json.JSONDecodeError, ValueError) as exc:
        request._send_json(400, {"error": f"Invalid JSON: {exc}"})
        return

    try:
        pg = request.subsystems.get_pg_conn()
        surface_name = _text(body.get("surface_name") or body.get("surface") or "moon") or "moon"
        review_decision = record_surface_catalog_review(
            pg,
            surface_name=surface_name,
            target_kind=body.get("target_kind"),
            target_ref=body.get("target_ref"),
            decision=body.get("decision"),
            actor_type=body.get("actor_type"),
            actor_ref=body.get("actor_ref"),
            approval_mode=body.get("approval_mode"),
            rationale=body.get("rationale"),
            candidate_payload=body.get("candidate_payload"),
        )
        request._send_json(
            200,
            {
                "surface_name": surface_name,
                "review_decision": _serialize(review_decision),
                "applies_overlay": (
                    str(review_decision.get("decision") or "").lower() in {"approve", "widen"}
                    and isinstance(review_decision.get("candidate_payload"), dict)
                ),
            },
        )
    except PostgresWriteError as exc:
        request._send_json(
            400,
            {
                "error": str(exc),
                "reason_code": exc.reason_code,
                "details": _serialize(exc.details),
            },
        )
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_intent_analyze_get(request: Any, path: str) -> None:
    try:
        params = _query_params(request.path)
        intent = (params.get("q", [""])[0]).strip()
        if not intent:
            request._send_json(400, {"error": "q parameter required"})
            return
        pg = request.subsystems.get_pg_conn()

        templates = pg.execute(
            "SELECT id, name, description FROM app_manifests WHERE search_vector @@ plainto_tsquery('english', $1) OR name ILIKE '%' || $1 || '%' ORDER BY name LIMIT 5",
            intent,
        )

        import re as _re

        words = [word for word in _re.findall(r"\w+", intent.lower()) if len(word) > 2]
        or_query = " | ".join(words) if words else intent
        integrations = pg.execute(
            "SELECT id, name, description, icon, capabilities FROM integration_registry WHERE search_vector @@ to_tsquery('english', $1) ORDER BY name LIMIT 5",
            or_query,
        )

        request._send_json(
            200,
            {
                "intent": intent,
                "templates": [
                    {
                        "id": row["id"],
                        "name": row["name"],
                        "description": row.get("description", ""),
                    }
                    for row in templates
                ],
                "integrations": [
                    {
                        "id": row["id"],
                        "name": base_integration_name(row),
                        "display_name": display_name_for_integration(row),
                        "description": row.get("description", ""),
                        "icon": row.get("icon", ""),
                        "capabilities": row.get("capabilities", []),
                    }
                    for row in integrations
                ],
                "can_generate": len(templates) == 0,
            },
        )
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_search_get(request: Any, path: str) -> None:
    try:
        params = _query_params(request.path)
        query = (params.get("q", [""])[0]).strip()
        scope = params.get("scope", ["all"])[0]
        if not query:
            request._send_json(400, {"error": "q parameter required"})
            return
        pg = request.subsystems.get_pg_conn()
        results = []
        if scope in ("all", "objects"):
            rows = pg.execute(
                "SELECT object_id, type_id, properties FROM objects "
                "WHERE search_vector @@ plainto_tsquery('english', $1) AND status = 'active' LIMIT 10",
                query,
            )
            for row in rows:
                props = row["properties"] if isinstance(row["properties"], dict) else {}
                title = next(
                    (str(value) for value in props.values() if isinstance(value, str)),
                    row["object_id"],
                )
                results.append(
                    {
                        "type": "object",
                        "id": row["object_id"],
                        "title": title,
                        "snippet": row["type_id"],
                    }
                )
        if scope in ("all", "manifests"):
            rows = pg.execute(
                "SELECT id, name, description FROM app_manifests "
                "WHERE search_vector @@ plainto_tsquery('english', $1) OR name ILIKE '%' || $1 || '%' LIMIT 10",
                query,
            )
            for row in rows:
                results.append(
                    {
                        "type": "manifest",
                        "id": row["id"],
                        "title": row["name"],
                        "snippet": (row.get("description") or "")[:100],
                    }
                )
        if scope in ("all", "workflows"):
            from runtime.receipt_store import search_receipts

            rows = search_receipts(query, limit=10)
            for row in rows:
                results.append(
                    {
                        "type": "workflow",
                        "id": row.id,
                        "title": row.label,
                        "snippet": f"{row.agent} — {row.status}",
                    }
                )
        request._send_json(
            200,
            {"results": results, "count": len(results), "query": query, "scope": scope},
        )
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_bugs_get(request: Any, path: str) -> None:
    try:
        params = _query_params(request.path)
        limit_raw = (params.get("limit") or ["50"])[0]
        replay_ready_only = (params.get("replay_ready_only") or ["0"])[0] in {"1", "true", "yes"}
        open_only = (params.get("open_only") or ["0"])[0] in {"1", "true", "yes"}
        result = _handle_bugs(
            request.subsystems,
            {
                "action": "list",
                "limit": max(1, int(limit_raw or 50)),
                "replay_ready_only": replay_ready_only,
                "open_only": open_only,
            },
        )
        request._send_json(200, result)
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_bugs_replay_ready_get(request: Any, path: str) -> None:
    try:
        params = _query_params(request.path)
        limit_raw = (params.get("limit") or ["50"])[0]
        refresh_backfill = (params.get("refresh_backfill") or ["1"])[0] not in {"0", "false", "no"}
        result = _workflow_query_core.handle_operator_view(
            request.subsystems,
            {
                "view": "replay_ready_bugs",
                "limit": max(1, int(limit_raw or 50)),
                "refresh_backfill": refresh_backfill,
            },
        )
        request._send_json(200, result)
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_registries_search_get(request: Any, path: str) -> None:
    try:
        params = _query_params(request.path)
        query = (params.get("q") or [""])[0].strip()
        if not query:
            request._send_json(400, {"error": "q parameter is required"})
            return
        pg = request.subsystems.get_pg_conn()
        results = []
        for table, kind in [
            ("registry_ui_components", "ui_component"),
            ("registry_calculations", "calculation"),
            ("registry_workflows", "workflow"),
        ]:
            rows = pg.execute(
                f"SELECT id, name, description, category FROM {table} "
                f"WHERE to_tsvector('english', coalesce(name,'') || ' ' || coalesce(description,'')) "
                f"@@ plainto_tsquery('english', $1) LIMIT 20",
                query,
            )
            for row in rows:
                results.append(
                    {
                        "id": row["id"],
                        "name": row["name"],
                        "description": row.get("description", ""),
                        "kind": kind,
                        "category": row.get("category", ""),
                    }
                )
        request._send_json(200, {"results": results, "count": len(results)})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_documents_get(request: Any, path: str) -> None:
    try:
        params = _query_params(request.path)
        query = (params.get("q") or [""])[0].strip()
        model_id = (params.get("model_id") or [""])[0].strip()
        pg = request.subsystems.get_pg_conn()

        sql = (
            "SELECT object_id, properties FROM objects "
            "WHERE type_id = 'doc_type_document' AND status = 'active'"
        )
        sql_params: list[Any] = []
        if query:
            sql_params.append(query)
            sql += (
                f" AND to_tsvector('english', properties::text) "
                f"@@ plainto_tsquery('english', ${len(sql_params)})"
            )
        if model_id:
            sql_params.append(model_id)
            sql += (
                f" AND COALESCE(properties->'attached_to', '[]'::jsonb) "
                f"? ${len(sql_params)}"
            )
        sql += " ORDER BY updated_at DESC, created_at DESC LIMIT 100"

        rows = pg.execute(sql, *sql_params)
        documents = []
        for row in rows:
            props = _parse_properties(row["properties"])
            title = props.get("title") or row["object_id"]
            content = props.get("content") or ""
            tags = props.get("tags") if isinstance(props.get("tags"), list) else []
            if not isinstance(content, str):
                content = str(content)

            documents.append(
                {
                    "id": row["object_id"],
                    "title": title,
                    "doc_type": props.get("doc_type", ""),
                    "tags": tags,
                    "content_preview": content[:200],
                }
            )

        request._send_json(
            200,
            {
                "documents": documents,
                "count": len(documents),
                "query": query,
                "model_id": model_id,
            },
        )
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_object_types_get(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        rows = pg.execute(
            "SELECT type_id, name, description, icon, property_definitions, created_at "
            "FROM object_types ORDER BY name"
        )
        request._send_json(200, {"types": [dict(row) for row in rows], "count": len(rows)})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_objects_get(request: Any, path: str) -> None:
    if path == "/api/objects":
        try:
            params = _query_params(request.path)
            type_id = (params.get("type") or [""])[0].strip()
            if not type_id:
                request._send_json(400, {"error": "type query parameter is required"})
                return
            status_filter = (params.get("status") or ["active"])[0].strip()
            query = (params.get("q") or [""])[0].strip()
            limit = int((params.get("limit") or ["100"])[0])
            pg = request.subsystems.get_pg_conn()
            if query:
                rows = pg.execute(
                    "SELECT object_id, type_id, properties, status, created_at, updated_at "
                    "FROM objects WHERE type_id = $1 AND status = $2 "
                    "AND search_vector @@ plainto_tsquery('english', $3) LIMIT $4",
                    type_id,
                    status_filter,
                    query,
                    limit,
                )
            else:
                rows = pg.execute(
                    "SELECT object_id, type_id, properties, status, created_at, updated_at "
                    "FROM objects WHERE type_id = $1 AND status = $2 LIMIT $3",
                    type_id,
                    status_filter,
                    limit,
                )
            objects = []
            for row in rows:
                obj = dict(row)
                if isinstance(obj.get("properties"), str):
                    try:
                        obj["properties"] = json.loads(obj["properties"])
                    except (json.JSONDecodeError, TypeError):
                        pass
                objects.append(obj)
            request._send_json(
                200,
                {"objects": objects, "count": len(objects), "type_id": type_id},
            )
        except Exception as exc:
            request._send_json(500, {"error": str(exc)})
        return

    object_id = path.split("/api/objects/")[-1]
    if object_id:
        try:
            pg = request.subsystems.get_pg_conn()
            row = pg.fetchrow(
                "SELECT * FROM objects WHERE object_id = $1",
                object_id,
            )
            if row is None:
                request._send_json(404, {"error": f"Object not found: {object_id}"})
                return
            obj = dict(row)
            if isinstance(obj.get("properties"), str):
                try:
                    obj["properties"] = json.loads(obj["properties"])
                except (json.JSONDecodeError, TypeError):
                    pass
            request._send_json(200, obj)
        except Exception as exc:
            request._send_json(500, {"error": str(exc)})


def _handle_leaderboard_get(request: Any, path: str) -> None:
    """GET /api/leaderboard — agent performance from receipts."""
    try:
        leaderboard = _load_leaderboard_snapshot(request.subsystems, since_hours=72)
        request._send_json(200, {"agents": leaderboard})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _queue_utilization_pct(total_queued: int, critical_threshold: int) -> float:
    if critical_threshold <= 0:
        return 999.9 if total_queued > 0 else 0.0
    return min(round(total_queued / critical_threshold * 100, 1), 999.9)


def _queue_depth_snapshot(pg: Any) -> dict[str, Any]:
    warning_threshold = 500
    critical_threshold = 1000
    if pg is None or not hasattr(pg, "execute"):
        return {
            "queue_depth": 0,
            "queue_depth_status": "unknown",
            "queue_depth_pending": 0,
            "queue_depth_ready": 0,
            "queue_depth_claimed": 0,
            "queue_depth_running": 0,
            "queue_depth_total": 0,
            "queue_depth_warning_threshold": warning_threshold,
            "queue_depth_critical_threshold": critical_threshold,
            "queue_depth_utilization_pct": 0.0,
            "queue_depth_error": "pg connection unavailable",
        }
    try:
        rows = pg.execute(
            """SELECT
                      COUNT(*) FILTER (WHERE status = 'pending') AS pending,
                      COUNT(*) FILTER (WHERE status = 'ready') AS ready,
                      COUNT(*) FILTER (WHERE status = 'claimed') AS claimed,
                      COUNT(*) FILTER (WHERE status = 'running') AS running
               FROM workflow_jobs
               WHERE status IN ('pending', 'ready', 'claimed', 'running')"""
        )
        row = rows[0] if rows else {}
        pending = int(row.get("pending") or 0)
        ready = int(row.get("ready") or 0)
        claimed = int(row.get("claimed") or 0)
        running = int(row.get("running") or 0)
        total_queued = pending + ready
        utilization_pct = _queue_utilization_pct(total_queued, critical_threshold)
        if total_queued >= critical_threshold:
            queue_status = "critical"
        elif total_queued >= warning_threshold:
            queue_status = "warning"
        else:
            queue_status = "ok"
        return {
            "queue_depth": total_queued,
            "queue_depth_status": queue_status,
            "queue_depth_pending": pending,
            "queue_depth_ready": ready,
            "queue_depth_claimed": claimed,
            "queue_depth_running": running,
            "queue_depth_total": total_queued,
            "queue_depth_warning_threshold": warning_threshold,
            "queue_depth_critical_threshold": critical_threshold,
            "queue_depth_utilization_pct": utilization_pct,
            "queue_depth_error": None,
        }
    except Exception as exc:
        return {
            "queue_depth": 0,
            "queue_depth_status": "unknown",
            "queue_depth_pending": 0,
            "queue_depth_ready": 0,
            "queue_depth_claimed": 0,
            "queue_depth_running": 0,
            "queue_depth_total": 0,
            "queue_depth_warning_threshold": warning_threshold,
            "queue_depth_critical_threshold": critical_threshold,
            "queue_depth_utilization_pct": 0.0,
            "queue_depth_error": str(exc),
        }


def _handle_status_get(request: Any, path: str) -> None:
    """GET /api/status — workflow summary stats."""
    try:
        subs = request.subsystems
        receipt_rollup = _load_receipt_rollup(subs, since_hours=24)
        queue_conn = subs.get_pg_conn() if hasattr(subs, "get_pg_conn") else None
        queue_snapshot = _queue_depth_snapshot(queue_conn)
        request._send_json(200, {
            "total_workflows": receipt_rollup["total_runs"],
            "pass_rate": receipt_rollup["pass_rate"],
            "top_failure_codes": receipt_rollup["top_failure_codes"],
            "since_hours": receipt_rollup["since_hours"],
            **queue_snapshot,
        })
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_dashboard_get(request: Any, path: str) -> None:
    """GET /api/dashboard — backend-authored dashboard snapshot."""
    try:
        request._send_json(200, _build_dashboard_payload(request.subsystems))
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_runs_recent_get(request: Any, path: str) -> None:
    """GET /api/runs/recent — recent workflow runs from Postgres."""
    try:
        pg = request.subsystems.get_pg_conn()
        limit = _safe_int_impl(_query_params(request.path).get("limit"), default=20, min_value=1, max_value=100)
        request._send_json(200, _load_recent_runs_snapshot(pg, limit=limit))
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_trigger_post(request: Any, path: str) -> None:
    """POST /api/trigger/{workflow_id} — manually trigger a workflow."""
    body: dict[str, Any] = {}
    try:
        parts = path.rstrip("/").split("/")
        workflow_id = parts[-1] if len(parts) >= 3 else ""

        if not workflow_id:
            payload = {"error": "workflow_id required"}
            request._send_json(400, payload)
            _record_api_route_usage(
                request.subsystems,
                path=path,
                method="POST",
                status_code=400,
                request_body=body,
                response_payload=payload,
                headers=request.headers,
            )
            return

        result = trigger_workflow_manually(
            request.subsystems,
            workflow_id=workflow_id,
            repo_root=REPO_ROOT,
        )
        request._send_json(200, result)
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=200,
            request_body=body,
            response_payload=result,
            headers=request.headers,
            conn=getattr(request.subsystems, "get_pg_conn", lambda: None)(),
        )
    except WorkflowRuntimeBoundaryError as exc:
        payload = {"error": str(exc)}
        request._send_json(exc.status_code, payload)
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=exc.status_code,
            request_body=body,
            response_payload=payload,
            headers=request.headers,
            conn=getattr(request.subsystems, "get_pg_conn", lambda: None)(),
        )
    except Exception as exc:
        payload = {"error": str(exc)}
        request._send_json(500, payload)
        _record_api_route_usage(
            request.subsystems,
            path=path,
            method="POST",
            status_code=500,
            request_body=body,
            response_payload=payload,
            headers=request.headers,
            conn=getattr(request.subsystems, "get_pg_conn", lambda: None)(),
        )


def _file_id_from_path(path: str, *, suffix: str = "") -> str:
    prefix = "/api/files/"
    if not path.startswith(prefix):
        return ""

    value = path[len(prefix):]
    if suffix:
        if not value.endswith(suffix):
            return ""
        value = value[: -len(suffix)]
    return value.strip("/")


def _read_upload_body(request: Any) -> dict[str, Any]:
    content_type = request.headers.get("Content-Type", "")
    if content_type.startswith("multipart/form-data"):
        content_length = int(request.headers.get("Content-Length", 0))
        raw = request.rfile.read(content_length) if content_length else b""
        if not raw:
            raise ValueError("request body is required")

        message = BytesParser(policy=default).parsebytes(
            (
                f"Content-Type: {content_type}\r\n"
                "MIME-Version: 1.0\r\n\r\n"
            ).encode("utf-8")
            + raw
        )
        if not message.is_multipart():
            raise ValueError("invalid multipart form data")

        body: dict[str, Any] = {}
        for part in message.iter_parts():
            if part.get_content_disposition() != "form-data":
                continue
            field_name = part.get_param("name", header="content-disposition")
            payload = part.get_payload(decode=True) or b""
            filename = part.get_filename()
            if filename:
                body["filename"] = filename
                body["content"] = payload
                if part.get_content_type():
                    body["content_type"] = part.get_content_type()
                continue
            if not field_name:
                continue
            charset = part.get_content_charset() or "utf-8"
            body[field_name] = payload.decode(charset)
        return body

    body = _read_json_body(request)
    if not isinstance(body, dict):
        raise ValueError("request body must be a JSON object")
    return body


def _normalize_upload_payload(body: dict[str, Any]) -> dict[str, Any]:
    filename = body.get("filename", "")
    if not isinstance(filename, str) or not filename.strip():
        raise ValueError("filename is required")

    content = body.get("content")
    if isinstance(content, str):
        try:
            content_bytes = base64.b64decode(content, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("content must be valid base64") from exc
    elif isinstance(content, bytes):
        content_bytes = content
    else:
        raise ValueError("content is required")

    scope = body.get("scope", "instance")
    if not isinstance(scope, str) or scope not in _ALLOWED_FILE_SCOPES:
        raise ValueError("scope must be one of: instance, step, workflow")

    workflow_id = body.get("workflow_id")
    if workflow_id is not None and not isinstance(workflow_id, str):
        raise ValueError("workflow_id must be a string")

    step_id = body.get("step_id")
    if step_id is not None and not isinstance(step_id, str):
        raise ValueError("step_id must be a string")

    description = body.get("description", "")
    if description is None:
        description = ""
    if not isinstance(description, str):
        raise ValueError("description must be a string")

    content_type = body.get("content_type", "application/octet-stream")
    if content_type is None:
        content_type = "application/octet-stream"
    if not isinstance(content_type, str):
        raise ValueError("content_type must be a string")

    return {
        "filename": filename.strip(),
        "content": content_bytes,
        "content_type": content_type,
        "scope": scope,
        "workflow_id": workflow_id.strip() if isinstance(workflow_id, str) else None,
        "step_id": step_id.strip() if isinstance(step_id, str) else None,
        "description": description,
    }


def _content_disposition(filename: str) -> str:
    safe_name = Path(filename).name or "download"
    ascii_name = safe_name.encode("ascii", "ignore").decode("ascii") or "download"
    ascii_name = ascii_name.replace('"', "")
    return f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(safe_name)}"


def _handle_files_get(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()

        if path == "/api/files":
            params = _query_params(request.path)
            scope = (params.get("scope") or [None])[0]
            workflow_id = (params.get("workflow_id") or [None])[0]
            step_id = (params.get("step_id") or [None])[0]
            files = list_files(
                pg,
                scope=scope or None,
                workflow_id=workflow_id or None,
                step_id=step_id or None,
            )
            request._send_json(200, {"files": files, "count": len(files)})
            return

        file_id = _file_id_from_path(path, suffix="/content")
        if not file_id:
            request._send_json(400, {"error": "file id is required"})
            return

        payload = get_file_content(pg, str(REPO_ROOT), file_id)
        if payload is None:
            request._send_json(404, {"error": f"File not found: {file_id}"})
            return

        content, content_type, filename = payload
        request._send_bytes(
            200,
            content,
            content_type=content_type or "application/octet-stream",
            content_disposition=_content_disposition(filename),
        )
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_files_post(request: Any, path: str) -> None:
    if request.command != "POST":
        request._send_json(405, {"error": "Method not allowed"})
        return

    try:
        body = _read_upload_body(request)
        payload = _normalize_upload_payload(body)
    except (json.JSONDecodeError, ValueError) as exc:
        request._send_json(400, {"error": f"Invalid upload payload: {exc}"})
        return

    try:
        pg = request.subsystems.get_pg_conn()
        record = save_file(pg, str(REPO_ROOT), **payload)
        request._send_json(200, {"file": record})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_files_delete(request: Any, path: str) -> None:
    if request.command != "DELETE":
        request._send_json(405, {"error": "Method not allowed"})
        return

    file_id = _file_id_from_path(path)
    if not file_id:
        request._send_json(400, {"error": "file id is required"})
        return

    try:
        pg = request.subsystems.get_pg_conn()
        deleted = delete_file(pg, str(REPO_ROOT), file_id)
        if not deleted:
            request._send_json(404, {"error": f"File not found: {file_id}"})
            return
        request._send_json(200, {"deleted": True, "id": file_id})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_workflow_delete(request: Any, path: str) -> None:
    """DELETE /api/workflows/delete/{id} — delete a workflow."""
    try:
        request._send_json(
            200,
            delete_workflow(
                request.subsystems.get_pg_conn(),
                workflow_id=path.split("/api/workflows/delete/")[-1].strip("/"),
            ),
        )
    except WorkflowRuntimeBoundaryError as exc:
        request._send_json(exc.status_code, {"error": str(exc)})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_build_stream(request: Any, path: str) -> None:
    """SSE endpoint: GET /api/workflows/{workflow_id}/build/stream

    Streams build state events (mutations, compilations, commits) as they
    arrive for a workflow. Backed by the durable event log — no polling
    hacks, cursor-based consumption.

    Events:
        data: {"id": 1, "event_type": "mutation", "entity_id": "wf_123", ...}
    """
    import json as _json
    import sys as _sys

    parts = path.split("/")
    # ['', 'api', 'workflows', '{workflow_id}', 'build', 'stream']
    if len(parts) < 6:
        request._send_json(400, {"error": "Expected /api/workflows/{workflow_id}/build/stream"})
        return

    workflow_id = parts[3]

    try:
        if str(REPO_ROOT) not in _sys.path:
            _sys.path.insert(0, str(REPO_ROOT))

        from runtime.event_log import iter_channel, CHANNEL_BUILD_STATE

        pg = request.subsystems.get_pg_conn()

        request.send_response(200)
        request.send_header("Content-Type", "text/event-stream")
        request.send_header("Cache-Control", "no-cache")
        request.send_header("Connection", "keep-alive")
        request.send_header("Access-Control-Allow-Origin", "*")
        request.end_headers()

        for event in iter_channel(
            pg,
            channel=CHANNEL_BUILD_STATE,
            entity_id=workflow_id,
            cursor=0,
            timeout_seconds=300,
            poll_interval=1.0,
        ):
            line = f"data: {_json.dumps(event.to_dict())}\n\n"
            request.wfile.write(line.encode("utf-8"))
            request.wfile.flush()

        request.wfile.write(b"event: done\ndata: {}\n\n")
        request.wfile.flush()

    except (BrokenPipeError, ConnectionResetError):
        pass
    except Exception as exc:
        try:
            request._send_json(500, {"error": str(exc)})
        except Exception:
            pass


QUERY_POST_ROUTES: list[RouteEntry] = [
    (_exact("/api/compile"), _handle_compile_post),
    (_exact("/api/refine-definition"), _handle_refine_definition_post),
    (_exact("/api/plan"), _handle_plan_post),
    (_exact("/api/commit"), _handle_commit_post),
    (_exact("/api/catalog/review-decisions"), _handle_catalog_review_decisions_post),
    (_workflow_build_path, _handle_workflow_build_post),
    (
        lambda candidate: candidate == "/api/documents"
        or (
            candidate.startswith("/api/documents/")
            and candidate.endswith("/attach")
        ),
        _handle_documents_post,
    ),
    (_exact("/api/files"), _handle_files_post),
    (_exact("/api/object-types"), _handle_object_types_post),
    (
        lambda candidate: candidate in {
            "/api/objects",
        },
        _handle_objects_post,
    ),
    (_exact("/api/workflows"), _handle_workflows_post),
    (_exact("/api/workflow-triggers"), _handle_workflow_triggers_post),
    (_prefix("/api/trigger/"), _handle_trigger_post),
]

QUERY_PUT_ROUTES: list[RouteEntry] = [
    (_exact("/api/objects/update"), _handle_objects_post),
    (
        _prefix_single_segment("/api/workflows/", excluded={"run", "delete"}),
        _handle_workflows_post,
    ),
    (_prefix_single_segment("/api/workflow-triggers/"), _handle_workflow_triggers_post),
]

QUERY_GET_ROUTES: list[RouteEntry] = [
    (_prefix_suffix("/api/workflows/", "/build/stream"), _handle_build_stream),
    (_exact("/api/dashboard"), _handle_dashboard_get),
    (_exact("/api/leaderboard"), _handle_leaderboard_get),
    (_exact("/api/status"), _handle_status_get),
    (_exact("/api/runs/recent"), _handle_runs_recent_get),
    (_exact("/api/references"), _handle_references_get),
    (_exact("/api/source-options"), _handle_source_options_get),
    (_exact("/api/manifest-heads"), _handle_manifest_heads_get),
    (_exact("/api/manifests"), _handle_manifests_get),
    (_exact("/api/manifests/history"), _handle_manifest_history_get),
    (_exact("/api/templates"), _handle_templates_get),
    (_exact("/api/models"), _handle_models_get),
    (_exact("/api/models/market"), _handle_market_models_get),
    (_exact("/api/integrations"), _handle_integrations_get),
    (_exact("/api/catalog"), _handle_catalog_get),
    (_exact("/api/catalog/review-decisions"), _handle_catalog_review_decisions_get),
    (_exact("/api/intent/analyze"), _handle_intent_analyze_get),
    (_exact("/api/search"), _handle_search_get),
    (_exact("/api/bugs/replay-ready"), _handle_bugs_replay_ready_get),
    (_exact("/api/bugs"), _handle_bugs_get),
    (_exact("/api/registries/search"), _handle_registries_search_get),
    (_prefix_suffix("/api/files/", "/content"), _handle_files_get),
    (_exact("/api/files"), _handle_files_get),
    (_exact("/api/documents"), _handle_documents_get),
    (_exact("/api/object-types"), _handle_object_types_get),
    (_exact("/api/objects"), _handle_objects_get),
    (_prefix("/api/objects/"), _handle_objects_get),
    (_exact("/api/workflows"), _handle_workflows_get),
    (_workflow_build_path, _handle_workflow_build_get),
    (_prefix_suffix("/api/workflows/", "/runs"), _handle_workflows_runs_get),
    (_prefix("/api/workflows/"), _handle_workflows_get),
    (_exact("/api/workflow-triggers"), _handle_workflow_triggers_get),
]

QUERY_DELETE_ROUTES: list[RouteEntry] = [
    (_exact("/api/objects/delete"), _handle_objects_post),
    (_prefix_single_segment("/api/workflows/delete/"), _handle_workflow_delete),
    (
        lambda candidate: candidate.startswith("/api/files/")
        and not candidate.endswith("/content"),
        _handle_files_delete,
    ),
]

QUERY_ROUTES: dict[str, object] = {
    "/query": _handle_query,
    "/bugs": _handle_bugs,
    "/recall": _handle_recall,
    "/ingest": _handle_ingest,
    "/graph": _handle_graph,
    "/receipts": _handle_receipts,
    "/constraints": _handle_constraints,
    "/friction": _handle_friction,
    "/heal": _handle_heal,
    "/artifacts": _handle_artifacts,
    "/decompose": _handle_decompose,
    "/research": _handle_research,
    "/operator_view": _handle_operator_view,
}


__all__ = [
    "QUERY_DELETE_ROUTES",
    "QUERY_GET_ROUTES",
    "QUERY_PUT_ROUTES",
    "QUERY_POST_ROUTES",
    "QUERY_ROUTES",
]
