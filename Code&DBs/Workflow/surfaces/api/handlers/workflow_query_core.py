"""Core read handlers for the workflow query API surface."""

from __future__ import annotations

import asyncio
import os
import re
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from runtime.workspace_paths import repo_root as workspace_repo_root
from runtime.primitive_contracts import bug_query_default_open_only_backlog
from storage.postgres.workflow_runtime_repository import list_workflow_records
from storage.postgres.validators import PostgresConfigurationError

from surfaces._workflow_database import workflow_database_url_for_repo
from .._payload_contract import coerce_optional_text
from . import _bug_surface_contract as _bug_contract
from ._shared import (
    _ClientError,
    _bug_to_dict,
    _matches,
    _serialize,
    is_demo_placeholder,
    placeholder_error_message,
)
from .workflow_admin import _handle_health


_optional_text = coerce_optional_text
_ISSUE_BACKLOG_KEYWORDS = frozenset(
    {
        "issue",
        "issues",
        "open issue",
        "open issues",
        "issue backlog",
        "upstream issue",
        "upstream issues",
        "intake issue",
        "intake issues",
    }
)
_OPERATOR_STATUS_QUERIES = frozenset({"operator status"})
_OPERATOR_GRAPH_QUERIES = frozenset({"operator graph"})
_SEMANTIC_ASSERTION_QUERIES = frozenset({"semantic assertions", "semantic assertion"})
_SCOREBOARD_QUERIES = frozenset({"scoreboard", "cutover scoreboard"})
_CARRY_FORWARD_QUERIES = frozenset({"carry forward", "carry-forward"})
_STALENESS_QUERIES = frozenset({"staleness"})
_DATA_DICTIONARY_QUERIES = frozenset({"data dictionary", "list tables"})
_CALIBRATION_QUERIES = frozenset({"calibration"})
_ROUTE_STATUS_QUERIES = frozenset({"route status"})
_DYNAMIC_TIMEOUT_QUERIES = frozenset({"dynamic timeout"})
_WORKFLOW_NEVER_RUN_MARKERS = (
    "never run",
    "not run",
    "unrun",
    "un-run",
    "unexecuted",
    "unlaunched",
)
_IMPORT_PATH_PREFIXES = ("import path for ",)
_TEST_COMMAND_PREFIXES = ("test command for ",)
_DATA_DICTIONARY_PREFIXES = (
    "schema for ",
    "schema of ",
    "columns for ",
    "columns in ",
    "fields for ",
    "fields in ",
)
_SHOW_QUERY_PREFIXES = ("show me ", "show ")
_OPERATOR_GRAPH_ALIAS_QUERIES = frozenset({"semantic graph", "cross-domain graph"})
_SEMANTIC_ASSERTION_ALIAS_QUERIES = frozenset({"semantic links", "operator semantics"})
_STALENESS_ALIAS_QUERIES = frozenset({"what is stale", "what is stale right now"})
_IMPORT_PATH_ALIAS_PREFIXES = (
    "how to import ",
    "where is ",
    "defined in ",
    "from import ",
    "import for ",
    "import ",
)
_TEST_COMMAND_ALIAS_PREFIXES = (
    "how to test ",
    "pytest for ",
    "verify command for ",
)


def _build_workflow_bridge(subs: Any):
    """Build the real workflow bridge over live Postgres-backed authorities."""

    from surfaces.workflow_bridge import build_live_workflow_bridge

    repo_root = getattr(subs, "_repo_root", None)
    postgres_env = getattr(subs, "_postgres_env", None)
    env: dict[str, str] = {}
    if callable(postgres_env):
        try:
            env = dict(postgres_env() or {})
        except Exception:
            env = {}
    try:
        if repo_root is None:
            raise PostgresConfigurationError(
                "postgres.config_missing",
                "WORKFLOW_DATABASE_URL is required to inspect workflow bridge state",
            )
        database_url = workflow_database_url_for_repo(repo_root, env=env)
    except PostgresConfigurationError as exc:
        raise RuntimeError("WORKFLOW_DATABASE_URL is required to inspect workflow bridge state") from exc
    return build_live_workflow_bridge(database_url)


def _empty_result(
    *,
    status: str,
    reason_code: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = {
        "status": status,
        "reason_code": reason_code,
    }
    if payload:
        response.update(payload)
    return response


def _operator_view_payload(view: object) -> dict[str, Any]:
    if is_dataclass(view):
        payload = asdict(view)
    elif isinstance(view, dict):
        payload = dict(view)
    else:
        payload = {"value": view}
    return _serialize(payload)


def _annotate_bug_dicts_with_replay_state(
    bt: Any,
    bugs: list[Any],
    *,
    replay_ready_only: bool = False,
    receipt_limit: int = 1,
    limit: int = 50,
) -> list[dict[str, Any]]:
    return _bug_contract.annotate_bug_dicts_with_replay_state(
        bt,
        bugs,
        serialize_bug=_bug_to_dict,
        replay_ready_only=replay_ready_only,
        include_replay_details=True,
        receipt_limit=receipt_limit,
        limit=limit,
    )


def _has_issue_backlog_intent(question: str) -> bool:
    return _normalized_query(question) in _ISSUE_BACKLOG_KEYWORDS


def _normalized_query(question: str) -> str:
    normalized = " ".join(str(question or "").split()).lower()
    return normalized.rstrip("?.!")


def _query_is(question: str, phrases: frozenset[str]) -> bool:
    return _normalized_query(question) in phrases


def _query_starts_with(question: str, prefixes: tuple[str, ...]) -> bool:
    normalized = _normalized_query(question)
    return any(normalized.startswith(prefix) for prefix in prefixes)


def _query_tail_after_prefix(
    question: str,
    prefixes: tuple[str, ...],
    *,
    strip_leading_article: bool = False,
) -> str | None:
    normalized = _normalized_query(question)
    for prefix in prefixes:
        if not normalized.startswith(prefix):
            continue
        tail = normalized[len(prefix):].strip()
        if strip_leading_article and tail.startswith("the "):
            tail = tail[4:].strip()
        return tail
    return None


def _has_never_run_workflow_intent(question: str) -> bool:
    normalized = _normalized_query(question)
    return (
        "workflow" in normalized
        and (
            any(marker in normalized for marker in _WORKFLOW_NEVER_RUN_MARKERS)
            or ("never" in normalized and "run" in normalized)
        )
    )


def _jsonish(value: Any) -> Any:
    if isinstance(value, str):
        try:
            import json as _json

            return _json.loads(value)
        except Exception:
            return value
    return value


def _workflow_record_summary(row: dict[str, Any]) -> dict[str, Any]:
    compiled_spec = _jsonish(row.get("compiled_spec"))
    return {
        "id": row.get("id"),
        "name": row.get("name"),
        "description": row.get("description"),
        "has_spec": compiled_spec is not None,
        "invocation_count": int(row.get("invocation_count") or 0),
        "last_invoked_at": row.get("last_invoked_at"),
        "is_template": bool(row.get("is_template")),
        "updated_at": row.get("updated_at"),
        "created_at": row.get("created_at"),
    }


def _never_run_workflows(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    conn = subs.get_pg_conn()
    limit = body.get("limit", 100)
    try:
        bounded_limit = max(0, min(int(limit), 500))
    except (TypeError, ValueError):
        bounded_limit = 100
    rows = list_workflow_records(conn, never_run=True, limit=bounded_limit)
    workflows = [_serialize(_workflow_record_summary(dict(row))) for row in rows]
    return {
        "routed_to": "workflow_records",
        "view": "never_run",
        "source_authority": "public.workflows(invocation_count,last_invoked_at)",
        "historical_filesystem_specs_considered": False,
        "historical_filesystem_specs_reason": (
            "Filesystem queue specs are derived or historical evidence unless admitted "
            "into DB-backed workflow authority."
        ),
        "count": len(workflows),
        "with_spec_count": sum(1 for workflow in workflows if workflow.get("has_spec")),
        "draft_without_spec_count": sum(
            1 for workflow in workflows if not workflow.get("has_spec")
        ),
        "workflows": workflows,
    }


def _rewrite_canonical_prefixed_query(
    question: str,
    *,
    alias_prefixes: tuple[str, ...],
    canonical_prefix: str,
) -> str | None:
    tail = _query_tail_after_prefix(question, alias_prefixes)
    if not tail:
        return None
    return f"{canonical_prefix}{tail}"


def _unsupported_query_alias(
    *,
    routed_to: str,
    reason_code: str,
    message: str,
    canonical_query: str | None = None,
    canonical_prefix: str | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = {
        "routed_to": routed_to,
        "status": "unsupported_query_alias",
        "reason_code": reason_code,
        "message": message,
    }
    if canonical_query is not None:
        response["canonical_query"] = canonical_query
    if canonical_prefix is not None:
        response["canonical_prefix"] = canonical_prefix
    if payload:
        response.update(payload)
    return response


def _deprecated_specialized_query_alias(question: str) -> dict[str, Any] | None:
    shown_query = _query_tail_after_prefix(
        question,
        _SHOW_QUERY_PREFIXES,
        strip_leading_article=True,
    )
    if shown_query in _OPERATOR_GRAPH_QUERIES:
        return _unsupported_query_alias(
            routed_to="operator_graph",
            reason_code="workflow_query.operator_graph_alias_removed",
            canonical_query="operator graph",
            message="Use `operator graph` exactly.",
        )
    if shown_query in _SEMANTIC_ASSERTION_QUERIES:
        return _unsupported_query_alias(
            routed_to="semantic_assertions",
            reason_code="workflow_query.semantic_assertions_alias_removed",
            canonical_query="semantic assertions",
            message="Use `semantic assertions` exactly.",
        )

    if _query_is(question, _OPERATOR_GRAPH_ALIAS_QUERIES):
        return _unsupported_query_alias(
            routed_to="operator_graph",
            reason_code="workflow_query.operator_graph_alias_removed",
            canonical_query="operator graph",
            message="Use `operator graph` exactly.",
        )
    if _query_is(question, _SEMANTIC_ASSERTION_ALIAS_QUERIES):
        return _unsupported_query_alias(
            routed_to="semantic_assertions",
            reason_code="workflow_query.semantic_assertions_alias_removed",
            canonical_query="semantic assertions",
            message="Use `semantic assertions` exactly.",
        )
    if _query_is(question, _STALENESS_ALIAS_QUERIES):
        return _unsupported_query_alias(
            routed_to="staleness_detector",
            reason_code="workflow_query.staleness_alias_removed",
            canonical_query="staleness",
            message="Use `staleness` exactly.",
        )

    canonical_import_query = _rewrite_canonical_prefixed_query(
        question,
        alias_prefixes=_IMPORT_PATH_ALIAS_PREFIXES,
        canonical_prefix=_IMPORT_PATH_PREFIXES[0],
    )
    if canonical_import_query is not None:
        return _unsupported_query_alias(
            routed_to="import_resolver",
            reason_code="workflow_query.import_path_alias_removed",
            canonical_query=canonical_import_query,
            canonical_prefix=_IMPORT_PATH_PREFIXES[0],
            message=f"Use `{canonical_import_query}`.",
        )

    canonical_test_query = _rewrite_canonical_prefixed_query(
        question,
        alias_prefixes=_TEST_COMMAND_ALIAS_PREFIXES,
        canonical_prefix=_TEST_COMMAND_PREFIXES[0],
    )
    if canonical_test_query is not None:
        return _unsupported_query_alias(
            routed_to="test_commands",
            reason_code="workflow_query.test_command_alias_removed",
            canonical_query=canonical_test_query,
            canonical_prefix=_TEST_COMMAND_PREFIXES[0],
            message=f"Use `{canonical_test_query}`.",
        )

    return None


def handle_query(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    question = (body.get("question") or "").strip().lower()
    if not question:
        raise _ClientError("question is required")

    if _matches(question, ["lane catalog", "lane runtime", "workflow bridge", "worker lane"]):
        try:
            bridge = _build_workflow_bridge(subs)
            catalog = asyncio.run(
                bridge.inspect_lane_catalog(as_of=datetime.now(timezone.utc))
            )
            return {
                "routed_to": "workflow_bridge",
                "view": "lane_catalog",
                "as_of": catalog.as_of.isoformat(),
                "lane_count": len(catalog.lane_records),
                "policy_count": len(catalog.lane_policy_records),
                "lane_names": list(catalog.lane_names),
                "policy_keys": [list(key) for key in catalog.policy_keys],
                "catalog": _serialize(catalog),
            }
        except Exception as exc:
            return {
                "routed_to": "workflow_bridge",
                "view": "lane_catalog",
                "status": "unavailable",
                "reason_code": "workflow_bridge.unavailable",
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            }

    if _matches(question, ["status", "panel", "snapshot", "overview", "dashboard"]):
        panel = subs.get_operator_panel()
        snap = panel.snapshot()
        return {"routed_to": "operator_panel", "snapshot": _serialize(snap)}

    if _has_issue_backlog_intent(question):
        from runtime.operation_catalog_gateway import execute_operation_from_subsystems

        backlog = execute_operation_from_subsystems(
            subs,
            operation_name="operator.issue_backlog",
            payload={
                "limit": 25,
                "open_only": bug_query_default_open_only_backlog(),
            },
        )
        backlog["routed_to"] = "issue_backlog"
        return backlog

    if _has_data_dictionary_intent(question):
        return _data_dictionary(subs, question)

    if _matches(question, ["bug", "defect"]):
        bt = subs.get_bug_tracker()
        bugs = bt.list_bugs(limit=20)
        bug_dicts = _annotate_bug_dicts_with_replay_state(bt, bugs, limit=20)
        return {
            "routed_to": "bug_tracker",
            "bugs": bug_dicts,
            "count": len(bug_dicts),
        }

    if _matches(question, ["quality", "metric", "rollup", "pass rate"]):
        qmod = subs.get_quality_views_mod()
        qm = subs.get_quality_materializer()
        rollup = qm.latest_rollup(qmod.QualityWindow.DAILY)
        if rollup:
            return {"routed_to": "quality_views", "rollup": _serialize(rollup)}
        return {
            "routed_to": "quality_views",
            "rollup": None,
            **_empty_result(
                status="empty",
                reason_code="quality_views.no_rollup_data",
            ),
        }

    if _matches(question, ["fail", "error", "crash", "broken"]):
        ingester = subs.get_receipt_ingester()
        receipts = ingester.load_recent(since_hours=24)
        top_failures = ingester.top_failure_codes(receipts)
        return {
            "routed_to": "failures",
            "top_failure_codes": top_failures,
            "total_receipts_checked": len(receipts),
        }

    if _matches(question, ["agent", "leaderboard", "performance", "who", "how are"]):
        ingester = subs.get_receipt_ingester()
        receipts = ingester.load_recent(since_hours=72)
        agents: dict[str, dict[str, int]] = {}
        for receipt in receipts:
            slug = receipt.get("agent_slug", receipt.get("agent", "unknown"))
            if slug not in agents:
                agents[slug] = {"total": 0, "succeeded": 0}
            agents[slug]["total"] += 1
            if receipt.get("status") == "succeeded":
                agents[slug]["succeeded"] += 1
        leaderboard = []
        for slug, stats in agents.items():
            pass_rate = stats["succeeded"] / stats["total"] if stats["total"] else 0.0
            leaderboard.append(
                {
                    "agent": slug,
                    "workflows": stats["total"],
                    "pass_rate": round(pass_rate, 4),
                }
            )
        leaderboard.sort(key=lambda item: (-item["pass_rate"], -item["workflows"]))
        return {"routed_to": "leaderboard", "agents": leaderboard}

    if _matches(question, ["health", "preflight", "probe"]):
        return _handle_health(subs, {})

    specialized_result = handle_specialized_query(subs, body)
    if specialized_result is not None:
        return specialized_result

    try:
        kg = subs.get_knowledge_graph()
        results = kg.search(question, limit=10)
        return {
            "routed_to": "knowledge_graph",
            "results": [
                {
                    "name": result.entity.name,
                    "type": result.entity.entity_type.value,
                    "score": round(result.score, 4),
                    "content_preview": result.entity.content[:200],
                    "source": result.entity.source,
                }
                for result in results
            ],
        }
    except Exception as exc:
        return {
            "routed_to": "knowledge_graph",
            "results": [],
            **_empty_result(
                status="unavailable",
                reason_code="knowledge_graph.unavailable",
                payload={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                },
            ),
        }


_QUERY_ROOT = workspace_repo_root()
_DIAGNOSE_RUN_ID_RE = re.compile(
    r"(?:diagnose(?:\s+run)?(?:\s+id)?|run(?:\s+id)?)[:=#\s]+([A-Za-z0-9:_-]+)",
    re.IGNORECASE,
)


def _query_repo_root(subs: Any) -> Path:
    repo_root = getattr(subs, "_repo_root", None)
    if isinstance(repo_root, Path):
        return repo_root
    if isinstance(repo_root, str) and repo_root:
        return Path(repo_root)
    return _QUERY_ROOT


def _extract_run_id(question: str) -> str:
    match = _DIAGNOSE_RUN_ID_RE.search(question)
    if match:
        return match.group(1).strip()

    tokens = [token.strip(".,;:()[]{}") for token in question.split() if token.strip()]
    for token in reversed(tokens):
        if len(token) >= 8 and any(ch.isdigit() for ch in token):
            return token
    return ""


def _parse_int(value: Any, default: int) -> int:
    """Parse a user-provided int with a safe fallback."""
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _parse_datetime(value: Any) -> datetime | None:
    """Parse timestamps used by stale candidate rows."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(normalized)
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _collect_staleness_candidates_from_rows(
    source: str,
    rows: list[dict[str, Any]],
    candidates: list[dict],
    seen: set[tuple[str, str]],
    limit: int,
    key_column: str,
    type_value: str,
    last_activity_columns: list[str],
) -> int:
    collected = 0
    for row in rows or []:
        if len(candidates) >= limit:
            break
        item_id = str(row.get(key_column, "") or "").strip()
        if not item_id:
            continue
        last_activity = None
        for column in last_activity_columns:
            last_activity = _parse_datetime(row.get(column))
            if last_activity is not None:
                break
        if last_activity is None:
            continue
        key = (type_value, item_id)
        if key in seen:
            continue
        candidates.append(
            {
                "item_id": item_id,
                "item_type": type_value,
                "last_activity": last_activity,
                "source": source,
            }
        )
        seen.add(key)
        collected += 1
    return collected


def _collect_staleness_candidates_from_database(
    conn,
    *,
    per_source_limit: int,
) -> tuple[list[dict], list[dict], list[str], list[dict]]:
    candidates: list[dict] = []
    sources: list[dict] = []
    warnings: list[str] = []
    errors: list[dict] = []
    seen: set[tuple[str, str]] = set()
    limit = max(per_source_limit, 1)

    if conn is None:
        errors.append({
            "source": "database",
            "reason_code": "workflow_query.staleness_database_unavailable",
            "message": "No Postgres connection is available for staleness candidate discovery.",
        })
        return candidates, sources, warnings, errors

    def _run_query(
        name: str,
        sql: str,
        args: tuple[Any, ...],
        key_column: str,
        type_value: str,
        last_activity_columns: list[str],
    ) -> None:
        try:
            rows = conn.execute(sql, *args)
            count = _collect_staleness_candidates_from_rows(
                source=name,
                rows=rows,
                candidates=candidates,
                seen=seen,
                limit=limit,
                key_column=key_column,
                type_value=type_value,
                last_activity_columns=last_activity_columns,
            )
            sources.append({"source": name, "count": count, "requested": limit})
        except Exception as exc:
            if name == "workflow_runs":
                details = "workflow_runs query unsupported"
            elif name == "workflow_jobs":
                details = "workflow_jobs query unsupported"
            elif name == "memory_entities":
                details = "memory_entities query unsupported"
            elif name == "dispatch_runs":
                details = "dispatch_runs query unsupported"
            else:
                details = f"{name} query unsupported"
            warnings.append(f"{details}: {exc}")
            errors.append({
                "source": name,
                "reason_code": "workflow_query.staleness_candidate_query_failed",
                "message": str(exc),
            })

    _run_query(
        name="memory_entities",
        sql=(
            "SELECT id, entity_type, updated_at "
            "FROM memory_entities "
            "WHERE archived = false "
            "ORDER BY updated_at DESC "
            "LIMIT $1"
        ),
        args=(limit,),
        key_column="id",
        type_value="memory_entities",
        last_activity_columns=["updated_at"],
    )
    _run_query(
        name="workflow_runs",
        sql=(
            "SELECT run_id, requested_at, started_at, finished_at "
            "FROM workflow_runs "
            "ORDER BY requested_at DESC "
            "LIMIT $1"
        ),
        args=(limit,),
        key_column="run_id",
        type_value="work_items",
        last_activity_columns=["finished_at", "started_at", "requested_at"],
    )
    _run_query(
        name="workflow_jobs",
        sql=(
            "SELECT id, created_at, ready_at, claimed_at, started_at, finished_at "
            "FROM workflow_jobs "
            "ORDER BY created_at DESC "
            "LIMIT $1"
        ),
        args=(limit,),
        key_column="id",
        type_value="work_items",
        last_activity_columns=["finished_at", "started_at", "claimed_at", "ready_at", "created_at"],
    )
    _run_query(
        name="dispatch_runs",
        sql=(
            "SELECT run_id, created_at, started_at, finished_at, terminal_reason "
            "FROM dispatch_runs "
            "ORDER BY created_at DESC "
            "LIMIT $1"
        ),
        args=(limit,),
        key_column="run_id",
        type_value="phases",
        last_activity_columns=["finished_at", "started_at", "created_at"],
    )

    return candidates, sources, warnings, errors


def _run_staleness_query(subs: Any, params: dict) -> dict:
    detector = subs.get_staleness_detector()
    per_source_limit = _parse_int(params.get("per_source_limit"), 200)
    max_items = _parse_int(params.get("max_items"), 20)

    direct_items: list[dict] = []
    if isinstance(params.get("items"), list):
        for item in params["items"]:
            if not isinstance(item, dict):
                continue
            item_id = str(item.get("item_id") or item.get("id") or "").strip()
            item_type = str(item.get("item_type") or "work_items").strip() or "work_items"
            last_activity = _parse_datetime(item.get("last_activity"))
            if item_id and last_activity is not None:
                direct_items.append(
                    {
                        "item_id": item_id,
                        "item_type": item_type,
                        "last_activity": last_activity,
                        "source": "direct",
                    }
                )

    candidates = direct_items
    sources = [{"source": "direct", "count": len(direct_items), "requested": len(direct_items)}] if direct_items else []
    warnings: list[str] = []
    errors: list[dict] = []
    candidate_authority_ready = True

    if not candidates:
        conn = getattr(subs, "_pg_conn", None)
        db_candidates, db_sources, db_warnings, db_errors = _collect_staleness_candidates_from_database(
            conn,
            per_source_limit=per_source_limit,
        )
        candidates.extend(db_candidates)
        sources.extend(db_sources)
        warnings.extend(db_warnings)
        errors.extend(db_errors)
        candidate_authority_ready = not (db_errors and not db_sources)

    if not candidates:
        if not candidate_authority_ready:
            return {
                "routed_to": "staleness_detector",
                "status": "degraded",
                "reason_code": "workflow_query.staleness_candidate_authority_failed",
                "message": "Could not inspect DB-backed staleness candidates.",
                "sources": sources,
                "warnings": warnings,
                "errors": errors,
                "candidate_authority_ready": False,
            }
        return {
            "routed_to": "staleness_detector",
            "status": "ok",
            "message": "No staleness candidates available. Provide 'items' or run this tool with active DB connectivity.",
            "sources": sources,
            "warnings": warnings,
            "candidate_authority_ready": candidate_authority_ready,
        }

    try:
        stale = detector.scan(candidates)
    except Exception as exc:
        return {
            "routed_to": "staleness_detector",
            "error": str(exc),
            "sources": sources,
            "warnings": warnings,
            "errors": errors,
            "candidate_authority_ready": candidate_authority_ready,
        }

    stale_items = [_serialize(item) for item in stale[:max_items]]
    if stale_items:
        return {
            "routed_to": "staleness_detector",
            "candidate_count": len(candidates),
            "stale_count": len(stale),
            "returned_count": len(stale_items),
            "sources": sources,
            "warnings": warnings,
            "errors": errors,
            "candidate_authority_ready": candidate_authority_ready,
            "summary": detector.alert_summary(stale),
            "items": stale_items,
        }

    return {
        "routed_to": "staleness_detector",
        "candidate_count": len(candidates),
        "stale_count": 0,
        "returned_count": 0,
        "sources": sources,
        "warnings": warnings,
        "errors": errors,
        "candidate_authority_ready": candidate_authority_ready,
        "message": "Scanned items are all fresh according to configured staleness rules.",
        "summary": detector.alert_summary(stale),
    }


def _extract_data_dictionary_table(question: str) -> str | None:
    lowered = question.lower()
    # Handle optional quotes around schema/table parts
    part = r"(?:['\"]?[a-z_][a-z0-9_]*['\"]?)"
    table_ref = rf"({part}(?:\.{part})?)"
    patterns = [
        rf"schema for {table_ref}",
        rf"schema of {table_ref}",
        rf"table {table_ref}",
        rf"columns? for {table_ref}",
        rf"columns? in {table_ref}",
        rf"fields? for {table_ref}",
        rf"fields? in {table_ref}",
        rf"what columns? does {table_ref} have",
        rf"what fields? does {table_ref} have",
        rf"what is the schema for {table_ref}",
        rf"what is the schema of {table_ref}",
    ]
    for pattern in patterns:
        match = re.search(pattern, lowered)
        if match:
            # Return the full captured ref (e.g. "public.workflow_runs"),
            # stripped of any wrapping quotes or trailing dots/question marks.
            return match.group(1).replace("'", "").replace("\"", "").rstrip(".?")
    return None


def _has_data_dictionary_intent(question: str) -> bool:
    normalized = _normalized_query(question)
    if _query_is(normalized, _DATA_DICTIONARY_QUERIES):
        return True
    if _query_starts_with(normalized, _DATA_DICTIONARY_PREFIXES):
        # Guard: "which table names" should not route to table lookup for "names"
        if "table names" in normalized or "which tables" in normalized:
            return True
        return True
    
    extracted = _extract_data_dictionary_table(normalized)
    if extracted is None:
        return False
        
    # Guard: if we extracted "names" from "which table names", it's probably misrouted generic intent
    if extracted == "names" and "table names" in normalized:
        return True

    return any(marker in normalized for marker in ("schema", "table", "column", "field"))


def _data_dictionary(subs: Any, question: str) -> dict:
    """Return browsable data dictionary from CQRS-backed table projections."""
    from runtime.operation_catalog_gateway import execute_operation_from_subsystems

    normalized = _normalized_query(question)
    # If it's a generic question about table names or schemas, return the list instead of a specific table
    if "table names" in normalized or "which tables" in normalized or _query_is(normalized, _DATA_DICTIONARY_QUERIES):
        return execute_operation_from_subsystems(
            subs,
            operation_name="operator.data_dictionary",
            payload={
                "include_relationships": False,
            },
        )

    table_name = _extract_data_dictionary_table(question)
    return execute_operation_from_subsystems(
        subs,
        operation_name="operator.data_dictionary",
        payload={
            "table_name": str(table_name) if table_name else None,
            "include_relationships": True,
        },
    )


def _import_resolver(subs: Any, question: str) -> dict:
    """Resolve Python import paths from module_embeddings."""
    conn = subs.get_pg_conn()

    cleaned = question
    for phrase in ["import path for", "how to import", "where is", "defined in", "from import", "import for", "import"]:
        cleaned = cleaned.replace(phrase, "")
    symbol = cleaned.strip().strip("'\"?")
    if not symbol:
        return {"routed_to": "import_resolver", "results": []}

    rows = conn.execute(
        "SELECT name, kind, module_path FROM module_embeddings "
        "WHERE name = $1 ORDER BY kind",
        symbol,
    )
    if not rows:
        rows = conn.execute(
            "SELECT name, kind, module_path FROM module_embeddings "
            "WHERE name ILIKE $1 ORDER BY kind LIMIT 10",
            f"%{symbol}%",
        )
    if not rows:
        return {"routed_to": "import_resolver", "results": [], "message": f"No symbol '{symbol}' found in codebase index"}

    seen: set[str] = set()
    results = []
    for row in rows:
        mod_path = row["module_path"]
        import_module = mod_path
        for prefix in ["Code&DBs/Workflow/", "Code and DBs/Workflow/", "Code&DBs/Databases/", "Code and DBs/Databases/"]:
            if import_module.startswith(prefix):
                import_module = import_module[len(prefix):]
        import_module = import_module.replace("/", ".").replace(".py", "")

        if row["kind"] == "module":
            import_stmt = f"import {import_module}"
        else:
            import_stmt = f"from {import_module} import {row['name']}"

        if import_stmt in seen:
            continue
        seen.add(import_stmt)
        results.append(
            {
                "name": row["name"],
                "kind": row["kind"],
                "import": import_stmt,
                "file": mod_path,
            }
        )

    return {"routed_to": "import_resolver", "results": results}


def _test_command_resolver(subs: Any, question: str) -> dict:
    """Resolve test commands for a given file path."""
    import glob as _glob

    cleaned = question
    for phrase in ["test command for", "how to test", "pytest for", "verify command for"]:
        cleaned = cleaned.replace(phrase, "")
    file_path = cleaned.strip().strip("'\"")
    if not file_path:
        return {"routed_to": "test_commands", "error": "No file path found"}

    stem = Path(file_path).stem
    workflow_root = str(_query_repo_root(subs))

    test_files = sorted(
        os.path.relpath(p, workflow_root)
        for p in _glob.glob(f"{workflow_root}/**/test_{stem}*.py", recursive=True)
    )

    commands = [
        f"PYTHONPATH='Code&DBs/Workflow' python3 -m pytest --noconftest -q {tf}"
        for tf in test_files
    ]

    result: dict[str, Any] = {
        "routed_to": "test_commands",
        "file": file_path,
        "test_files": test_files,
        "commands": commands,
    }
    if not test_files:
        result["hint"] = (
            f"No test_{stem}*.py found. "
            f"Syntax check: PYTHONPATH='Code&DBs/Workflow' python3 -m py_compile '{file_path}'"
        )
    return result


def handle_specialized_query(subs: Any, body: dict[str, Any]) -> dict | None:
    """Handle the supported specialized query intents that sit outside the base views."""
    question = (body.get("question") or "").strip().lower()
    if not question:
        return None

    if _matches(question, ["diagnose", "diagnosis", "troubleshoot", "why did", "run id"]):
        return _unsupported_query_alias(
            routed_to="workflow_diagnose",
            reason_code="workflow_query.diagnose_alias_removed",
            canonical_query="praxis workflow diagnose <run_id>",
            message="Use praxis_diagnose or `praxis workflow diagnose <run_id>` directly.",
            payload={"run_id": _extract_run_id(question)},
        )

    if _query_is(question, _OPERATOR_STATUS_QUERIES):
        return handle_operator_view(
            subs,
            {"view": "status", "run_id": _extract_run_id(question) or body.get("run_id")},
        )

    if _query_is(question, _OPERATOR_GRAPH_QUERIES):
        return handle_operator_view(
            subs,
            {
                "view": "operator_graph",
                "as_of": body.get("as_of"),
            },
        )

    if _query_is(question, _SEMANTIC_ASSERTION_QUERIES):
        return handle_operator_view(
            subs,
            {
                "view": "semantics",
                "predicate_slug": body.get("predicate_slug"),
                "subject_kind": body.get("subject_kind"),
                "subject_ref": body.get("subject_ref"),
                "object_kind": body.get("object_kind"),
                "object_ref": body.get("object_ref"),
                "source_kind": body.get("source_kind"),
                "source_ref": body.get("source_ref"),
                "active_only": body.get("active_only", True),
                "as_of": body.get("as_of"),
                "limit": body.get("limit", 50),
            },
        )

    if _query_is(question, _SCOREBOARD_QUERIES):
        return handle_operator_view(
            subs,
            {"view": "scoreboard", "run_id": _extract_run_id(question) or body.get("run_id")},
        )

    if _query_is(question, _CARRY_FORWARD_QUERIES):
        from runtime.session_carry import (
            filter_pack_for_effective_provider_catalog,
            load_effective_provider_job_catalog_for_carry,
            pack_to_summary_dict,
        )

        mgr = subs.get_session_carry_mgr()
        try:
            effective_catalog = load_effective_provider_job_catalog_for_carry(
                subs.get_pg_conn()
            )
        except Exception as exc:
            return {
                "error_code": "session_provider_catalog_unavailable",
                "error": f"provider catalog unavailable for session carry-forward: {exc}",
            }
        action = body.get("action", "latest")
        if action == "latest":
            pack = mgr.latest()
            if pack is None:
                return {"message": "No carry-forward packs saved yet."}
            pack = filter_pack_for_effective_provider_catalog(
                pack,
                effective_provider_job_catalog=effective_catalog,
            )
            return pack_to_summary_dict(pack)
        if action == "validate":
            pack_id = str(body.get("pack_id") or "").strip()
            pack = mgr.latest() if not pack_id else mgr.load(pack_id)
            if pack is None:
                return {"message": "Pack not found."}
            pack = filter_pack_for_effective_provider_catalog(
                pack,
                effective_provider_job_catalog=effective_catalog,
            )
            issues = mgr.validate(pack)
            if not issues:
                return {"valid": True, "pack": pack_to_summary_dict(pack)}
            return {"valid": False, "pack": pack_to_summary_dict(pack), "issues": issues}
        return {"error": f"Unknown session action: {action}"}

    if _query_is(question, _STALENESS_QUERIES):
        return _run_staleness_query(subs, dict(body))

    if _query_starts_with(question, _IMPORT_PATH_PREFIXES):
        return _import_resolver(subs, question)

    if _query_starts_with(question, _TEST_COMMAND_PREFIXES):
        return _test_command_resolver(subs, question)

    if _has_data_dictionary_intent(question):
        return _data_dictionary(subs, question)

    if _query_is(question, _CALIBRATION_QUERIES):
        try:
            from runtime.calibration import CalibrationEngine

            engine = CalibrationEngine({})
            repo_root = _query_repo_root(subs)
            cal_path = os.path.join(str(repo_root), "config", "calibration.json")
            if os.path.isfile(cal_path):
                engine.load(cal_path)
            params = engine.all_params()
            if not params:
                return {"routed_to": "calibration", "message": "No calibrated parameters yet."}
            return {
                "routed_to": "calibration",
                "params": {
                    name: {"value": round(p.value, 4), "min": p.lower, "max": p.upper}
                    for name, p in params.items()
                },
            }
        except Exception as exc:
            return {"routed_to": "calibration", "error": str(exc)}

    if _query_is(question, _ROUTE_STATUS_QUERIES):
        try:
            import runtime.auto_router as auto_router_mod

            tiers = auto_router_mod.all_tiers()
            decisions = {}
            for tier in tiers:
                candidates = auto_router_mod.candidates_for_tier(tier)
                decisions[tier] = [
                    {"provider": c.provider_slug, "model": c.model_slug, "healthy": c.healthy}
                    for c in candidates
                ] if candidates else []
            return {"routed_to": "auto_router", "tiers": decisions}
        except Exception as exc:
            return {"routed_to": "auto_router", "error": str(exc)}

    if _query_is(question, _DYNAMIC_TIMEOUT_QUERIES):
        return {
            "routed_to": "dynamic_timeout",
            "message": "Timeouts are auto-computed per complexity tier. Use praxis_query 'calibration' to see tuned parameters.",
        }

    if _has_never_run_workflow_intent(question):
        return _never_run_workflows(subs, body)

    deprecated_alias = _deprecated_specialized_query_alias(question)
    if deprecated_alias is not None:
        return deprecated_alias

    return None


def handle_bugs(
    subs: Any,
    body: dict[str, Any],
    *,
    parse_bug_status,
    parse_bug_severity,
    parse_bug_category,
) -> dict[str, Any]:
    action = body.get("action", "list")
    bt = subs.get_bug_tracker()
    bt_mod = subs.get_bug_tracker_mod()
    resolved_statuses = {
        bt_mod.BugStatus.FIXED,
        bt_mod.BugStatus.WONT_FIX,
        bt_mod.BugStatus.DEFERRED,
    }
    try:
        if action == "list":
            return _bug_contract.list_bugs_payload(
                bt=bt,
                bt_mod=bt_mod,
                body=body,
                serialize_bug=_bug_to_dict,
                default_limit=50,
                include_replay_details=True,
                parse_status=parse_bug_status,
                parse_severity=parse_bug_severity,
                parse_category=parse_bug_category,
            )

        if action == "file":
            return _bug_contract.file_bug_payload(
                bt=bt,
                bt_mod=bt_mod,
                body=body,
                serialize_bug=_bug_to_dict,
                filed_by_default="workflow_api",
                source_kind_default="workflow_api",
                parse_severity=parse_bug_severity,
                parse_category=parse_bug_category,
            )

        if action == "search":
            return _bug_contract.search_bugs_payload(
                bt=bt,
                bt_mod=bt_mod,
                body=body,
                serialize_bug=_bug_to_dict,
                default_limit=20,
                parse_status=parse_bug_status,
                parse_severity=parse_bug_severity,
                parse_category=parse_bug_category,
            )

        if action == "duplicate_check":
            return _bug_contract.duplicate_check_payload(
                bt=bt,
                bt_mod=bt_mod,
                body=body,
                serialize_bug=_bug_to_dict,
                default_limit=10,
                parse_status=parse_bug_status,
                parse_severity=parse_bug_severity,
                parse_category=parse_bug_category,
            )

        if action == "stats":
            return _bug_contract.stats_payload(bt=bt, serialize=_serialize)

        if action == "packet":
            return _bug_contract.packet_payload(bt=bt, body=body, serialize=_serialize)

        if action == "history":
            return _bug_contract.history_payload(bt=bt, body=body, serialize=_serialize)

        if action == "replay":
            return _bug_contract.replay_payload(bt=bt, body=body, serialize=_serialize)

        if action == "backfill_replay":
            return _bug_contract.backfill_replay_payload(bt=bt, body=body, serialize=_serialize)

        if action == "attach_evidence":
            return _bug_contract.attach_evidence_payload(
                bt=bt,
                body=body,
                serialize=_serialize,
                created_by_default="workflow_api",
            )

        if action == "resolve":
            return _bug_contract.resolve_bug_payload(
                bt=bt,
                bt_mod=bt_mod,
                body=body,
                serialize_bug=_bug_to_dict,
                serialize=_serialize,
                resolved_statuses=resolved_statuses,
                parse_status=parse_bug_status,
                created_by_default="workflow_api",
            )

        if action == "patch_resume":
            return _bug_contract.patch_resume_payload(
                bt=bt,
                body=body,
                serialize_bug=_bug_to_dict,
            )
    except ValueError as exc:
        raise _ClientError(str(exc)) from exc

    raise _ClientError(f"Unknown bug action: {action}")


def handle_recall(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    query = body.get("query", "")
    if not query:
        raise _ClientError("query is required")
    entity_type = body.get("entity_type") or None

    try:
        from surfaces._recall import search_recall_results

        results = search_recall_results(
            subs,
            query=query,
            entity_type=entity_type,
            limit=20,
        )
        return {
            "results": [
                {
                    "entity_id": result["entity_id"],
                    "name": result["name"],
                    "type": result["type"],
                    "score": round(float(result["score"]), 4),
                    "content_preview": str(result.get("content") or "")[:300],
                    "source": result.get("source"),
                    "found_via": result.get("found_via"),
                    "provenance": result.get("provenance"),
                }
                for result in results
            ],
            "count": len(results),
        }
    except Exception as exc:
        return {
            "results": [],
            "count": 0,
            **_empty_result(
                status="unavailable",
                reason_code="knowledge_graph.error",
                payload={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                },
            ),
        }


def handle_ingest(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    kind = body.get("kind", "")
    content = body.get("content", "")
    source = body.get("source", "")
    if not kind or not content or not source:
        raise _ClientError("kind, content, and source are all required")

    try:
        kg = subs.get_knowledge_graph()
        source_type = str(body.get("source_type") or kind or "").strip().lower()
        from memory.multimodal_ingest import (
            SUPPORTED_MULTIMODAL_SOURCE_TYPES,
            ingest_multimodal_to_knowledge_graph,
        )
        if source_type in SUPPORTED_MULTIMODAL_SOURCE_TYPES:

            multimodal = ingest_multimodal_to_knowledge_graph(
                kg,
                content=content,
                source=source,
                source_type=source_type,
            )
            graph_result = multimodal["graph_result"]
            return {
                "accepted": graph_result.accepted,
                "entities_created": graph_result.entities_created,
                "edges_created": graph_result.edges_created,
                "duplicates_skipped": graph_result.duplicates_skipped,
                "errors": list(graph_result.errors),
                "multimodal": {
                    "source_type": multimodal["source_type"],
                    "staging_receipt": _serialize(multimodal["staging_receipt"]),
                },
            }
        result = kg.ingest(kind=kind, content=content, source=source)
        return {
            "accepted": result.accepted,
            "entities_created": result.entities_created,
            "edges_created": result.edges_created,
            "duplicates_skipped": result.duplicates_skipped,
            "errors": list(result.errors),
        }
    except Exception as exc:
        return {"accepted": False, "error": str(exc)}


def handle_graph(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    entity_id = body.get("entity_id", "")
    if not entity_id:
        raise _ClientError("entity_id is required")
    depth = body.get("depth", 1)

    try:
        kg = subs.get_knowledge_graph()
        blast = kg.blast_radius(entity_id)
        return {
            "entity_id": entity_id,
            "depth": depth,
            "blast_radius": _serialize(blast),
        }
    except Exception as exc:
        return {"entity_id": entity_id, "error": str(exc)}


def handle_receipts(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    from runtime.receipt_store import receipt_stats, search_receipts

    action = body.get("action", "search")

    if action == "search":
        query = body.get("query", "")
        if not query:
            raise _ClientError("query is required for search")
        status = body.get("status") or None
        agent = body.get("agent") or None
        limit = body.get("limit", 20)
        results = search_receipts(
            query,
            status=status,
            agent=agent,
            limit=limit,
            conn=subs.get_pg_conn(),
        )
        return {"results": [record.to_search_result() for record in results], "count": len(results)}

    if action == "token_burn":
        since_hours = body.get("since_hours", 24)
        return {"token_burn": receipt_stats(since_hours=since_hours, conn=subs.get_pg_conn())}

    raise _ClientError(f"Unknown receipts action: {action}")


def handle_constraints(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    action = body.get("action", "list")
    ledger = subs.get_constraint_ledger()

    if action == "list":
        items = ledger.list_all(min_confidence=body.get("min_confidence", 0.5))
        if not items:
            return _empty_result(
                status="empty",
                reason_code="constraints.none_found",
                payload={"count": 0, "constraints": []},
            )
        return {
            "count": len(items),
            "constraints": [
                {
                    "constraint_id": item.constraint_id,
                    "pattern": item.pattern,
                    "text": item.constraint_text,
                    "confidence": round(item.confidence, 3),
                    "mined_from": list(item.mined_from_jobs)[:5],
                }
                for item in items
            ],
        }

    if action == "for_scope":
        paths = body.get("write_paths", [])
        if not paths:
            raise _ClientError("write_paths list is required for for_scope")
        items = ledger.get_for_scope(paths)
        if not items:
            return _empty_result(
                status="empty",
                reason_code="constraints.scope_miss",
                payload={"count": 0, "constraints": []},
            )
        return {
            "count": len(items),
            "constraints": [
                {
                    "pattern": item.pattern,
                    "text": item.constraint_text,
                    "confidence": round(item.confidence, 3),
                }
                for item in items
            ],
        }

    raise _ClientError(f"Unknown constraints action: {action}")


def handle_friction(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    action = body.get("action", "stats")
    include_test = body.get("include_test", False)
    ledger = subs.get_friction_ledger()

    if action == "stats":
        stats = ledger.stats(include_test=include_test)
        if stats.total == 0:
            return _empty_result(
                status="empty",
                reason_code="friction.none_recorded",
                payload={
                    "total": 0,
                    "by_type": {},
                    "by_source": {},
                },
            )
        return {
            "total": stats.total,
            "by_type": stats.by_type,
            "by_source": stats.by_source,
            "bounce_rate_24h": round(
                ledger.bounce_rate(since_hours=24, include_test=include_test),
                4,
            ),
        }

    if action == "list":
        events = ledger.list_events(
            source=body.get("source") or None,
            limit=body.get("limit", 20),
            include_test=include_test,
        )
        if not events:
            return _empty_result(
                status="empty",
                reason_code="friction.none_found",
                payload={"count": 0, "events": []},
            )
        return {
            "count": len(events),
            "events": [
                {
                    "event_id": event.event_id,
                    "type": event.friction_type.value,
                    "source": event.source,
                    "job_label": event.job_label,
                    "message": event.message[:200],
                    "timestamp": event.timestamp.isoformat(),
                }
                for event in events
            ],
        }

    if action == "patterns":
        since_hours = body.get("since_hours")
        since = None
        if since_hours is not None:
            since = datetime.now(timezone.utc) - timedelta(hours=float(since_hours))
        patterns = ledger.patterns(
            source=body.get("source") or None,
            since=since,
            limit=body.get("limit", 20),
            scan_limit=body.get("scan_limit", 500),
            include_test=include_test,
            promotion_threshold=body.get("promotion_threshold", 3),
        )
        if not patterns:
            return _empty_result(
                status="empty",
                reason_code="friction.no_patterns",
                payload={"count": 0, "patterns": []},
            )
        return {
            "count": len(patterns),
            "patterns": [pattern.to_json() for pattern in patterns],
        }

    raise _ClientError(f"Unknown friction action: {action}")


def handle_heal(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    job_label = body.get("job_label", "")
    failure_code = body.get("failure_code", "")
    stderr = body.get("stderr", "")
    if not job_label:
        raise _ClientError("job_label is required")
    if not failure_code and not stderr:
        raise _ClientError("failure_code or stderr is required")
    healer = subs.get_self_healer()
    resolved_failure_code = healer.resolve_failure_code(failure_code, stderr)
    rec = healer.diagnose(job_label, failure_code, stderr)
    return {
        "action": rec.action.value,
        "reason": rec.reason,
        "confidence": round(rec.confidence, 3),
        "context_patches": list(rec.context_patches),
        "diagnostics_run": rec.diagnostics_run,
        "resolved_failure_code": resolved_failure_code,
    }


def handle_artifacts(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    action = body.get("action", "stats")
    store = subs.get_artifact_store()

    if action == "stats":
        stats = store.stats()
        if stats["total_artifacts"] == 0:
            return _empty_result(
                status="empty",
                reason_code="artifacts.none_recorded",
                payload=stats,
            )
        return stats

    if action == "list":
        sandbox_id = str(body.get("sandbox_id", "") or "").strip()
        if is_demo_placeholder("sandbox_id", sandbox_id):
            raise _ClientError(placeholder_error_message("sandbox_id", sandbox_id))
        if not sandbox_id:
            raise _ClientError("sandbox_id is required for list")
        items = store.list_by_sandbox(sandbox_id)
        if not items:
            return _empty_result(
                status="empty",
                reason_code="artifacts.scope_miss",
                payload={"sandbox_id": sandbox_id, "count": 0, "artifacts": []},
            )
        return {
            "sandbox_id": sandbox_id,
            "count": len(items),
            "artifacts": [
                {
                    "artifact_id": item.artifact_id,
                    "file_path": item.file_path,
                    "byte_count": item.byte_count,
                    "line_count": item.line_count,
                    "captured_at": item.captured_at.isoformat(),
                }
                for item in items
            ],
        }

    if action == "search":
        query = body.get("query", "")
        if not query:
            raise _ClientError("query is required for search")
        items = store.search(query, limit=body.get("limit", 20))
        if not items:
            return _empty_result(
                status="empty",
                reason_code="artifacts.no_matches",
                payload={"count": 0, "artifacts": []},
            )
        return {
            "count": len(items),
            "artifacts": [
                {
                    "artifact_id": item.artifact_id,
                    "file_path": item.file_path,
                    "sandbox_id": item.sandbox_id,
                    "byte_count": item.byte_count,
                }
                for item in items
            ],
        }

    if action == "diff":
        artifact_id_a = body.get("artifact_id_a", "")
        artifact_id_b = body.get("artifact_id_b", "")
        if not artifact_id_a or not artifact_id_b:
            raise _ClientError("artifact_id_a and artifact_id_b are required")
        return store.diff(artifact_id_a, artifact_id_b)

    raise _ClientError(f"Unknown artifacts action: {action}")


def handle_decompose(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    objective = body.get("objective", "")
    if not objective:
        raise _ClientError("objective is required")
    from runtime.sprint_decomposer import SprintDecomposer

    decomposer = SprintDecomposer()
    sprints = decomposer.decompose(objective, body.get("scope_files", []))
    if not sprints:
        return _empty_result(
            status="empty",
            reason_code="decompose.no_sprints",
            payload={"sprints": []},
        )
    critical = decomposer.critical_path(sprints)
    return {
        "total_sprints": len(sprints),
        "total_estimate_minutes": decomposer.total_estimate(sprints),
        "critical_path": [sprint.label for sprint in critical],
        "sprints": [
            {
                "label": sprint.label,
                "complexity": sprint.complexity.value,
                "depends_on": list(sprint.depends_on),
                "estimate_minutes": sprint.estimated_minutes,
                "files": list(sprint.file_targets)[:10],
            }
            for sprint in sprints
        ],
    }


def handle_research(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    action = body.get("action", "search")
    if action == "search":
        query = body.get("query", "")
        if not query:
            raise _ClientError("query is required for search")
        from memory.research_runtime import ResearchExecutor

        engine = subs.get_memory_engine()
        executor = ResearchExecutor(engine)
        result = executor.search_local(query)
        if not result.hits:
            return _empty_result(
                status="empty",
                reason_code="research.no_hits",
                payload={"count": 0, "hits": []},
            )
        return {
            "count": len(result.hits),
            "hits": [
                {
                    "name": hit.name,
                    "score": round(hit.score, 4),
                    "preview": hit.content_preview[:200],
                }
                for hit in result.hits[:20]
            ],
        }
    raise _ClientError(f"Unknown research action: {action}")


def handle_operator_view(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    from runtime.operation_catalog_gateway import execute_operation_from_subsystems

    view = str(body.get("view") or "status").strip().lower()
    raw_as_of = body.get("as_of")
    as_of = _parse_datetime(raw_as_of)
    if raw_as_of is not None and as_of is None:
        raise _ClientError("as_of must be a valid ISO-8601 datetime when provided")

    operation_name = {
        "status": "operator.run_status",
        "scoreboard": "operator.run_scoreboard",
        "graph": "operator.run_graph",
        "operator_graph": "operator.graph_projection",
        "semantics": "semantic_assertions.list",
        "lineage": "operator.run_lineage",
        "issue_backlog": "operator.issue_backlog",
        "replay_ready_bugs": "operator.replay_ready_bugs",
    }.get(view)
    view_options = (
        "status",
        "scoreboard",
        "graph",
        "operator_graph",
        "semantics",
        "lineage",
        "replay_ready_bugs",
        "issue_backlog",
    )
    if operation_name is None:
        raise _ClientError(f"Unknown view: {view}. Options: {', '.join(view_options)}")

    payload: dict[str, Any]
    if view in {"status", "scoreboard", "graph", "lineage"}:
        run_id = _optional_text(body.get("run_id"))
        if run_id is None:
            raise _ClientError(f"run_id is required for operator view '{view}'")
        payload = {"run_id": run_id}
    elif view == "operator_graph":
        payload = {"as_of": as_of}
    elif view == "semantics":
        payload = {
            "predicate_slug": _optional_text(body.get("predicate_slug")),
            "subject_kind": _optional_text(body.get("subject_kind")),
            "subject_ref": _optional_text(body.get("subject_ref")),
            "object_kind": _optional_text(body.get("object_kind")),
            "object_ref": _optional_text(body.get("object_ref")),
            "source_kind": _optional_text(body.get("source_kind")),
            "source_ref": _optional_text(body.get("source_ref")),
            "active_only": bool(body.get("active_only", True)),
            "as_of": as_of,
            "limit": max(1, int(body.get("limit", 50) or 50)),
        }
    elif view == "issue_backlog":
        payload = {
            "limit": max(1, int(body.get("limit", 50) or 50)),
            "open_only": bool(
                body.get("open_only", bug_query_default_open_only_backlog())
            ),
            "status": _optional_text(body.get("status")),
        }
    else:
        if bool(body.get("refresh_backfill", False)):
            raise _ClientError(
                "replay_ready_bugs is read-only; use praxis workflow bugs backfill_replay for provenance maintenance"
            )
        payload = {
            "limit": max(1, int(body.get("limit", 50) or 50)),
        }
    return execute_operation_from_subsystems(
        subs,
        operation_name=operation_name,
        payload=payload,
    )
