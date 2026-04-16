"""Core read handlers for the workflow query API surface."""

from __future__ import annotations

import asyncio
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any

from storage.postgres.connection import resolve_workflow_database_url
from storage.postgres.validators import PostgresConfigurationError

from ._shared import _ClientError, _bug_to_dict, _matches, _serialize
from .workflow_admin import _handle_health


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _build_workflow_bridge(subs: Any):
    """Build the real workflow bridge over live Postgres-backed authorities."""

    from surfaces.workflow_bridge import build_live_workflow_bridge

    postgres_env = getattr(subs, "_postgres_env", None)
    env: dict[str, str] = {}
    if callable(postgres_env):
        try:
            env = dict(postgres_env() or {})
        except Exception:
            env = {}
    try:
        if env.get("WORKFLOW_DATABASE_URL"):
            database_url = resolve_workflow_database_url(env=env)
        else:
            database_url = resolve_workflow_database_url()
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
    annotated: list[dict[str, Any]] = []
    for bug in bugs:
        bug_dict = _bug_to_dict(bug)
        hint = bt.replay_hint(bug.bug_id, receipt_limit=receipt_limit)
        bug_dict["replay_ready"] = bool((hint or {}).get("available"))
        bug_dict["replay_reason_code"] = str((hint or {}).get("reason_code") or "bug.replay_not_ready")
        bug_dict["replay_run_id"] = (hint or {}).get("run_id")
        bug_dict["replay_receipt_id"] = (hint or {}).get("receipt_id")
        if replay_ready_only and not bug_dict["replay_ready"]:
            continue
        annotated.append(bug_dict)
        if len(annotated) >= limit:
            break
    return annotated


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

    if _matches(question, ["bug", "defect", "issue"]):
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

    if action == "list":
        status = body.get("status")
        severity = body.get("severity")
        limit = max(1, int(body.get("limit", 50) or 50))
        category = parse_bug_category(bt_mod, body.get("category"))
        title_like = body.get("title_like")
        include_replay_state = bool(body.get("include_replay_state", True))
        replay_ready_only = bool(body.get("replay_ready_only", False))
        raw_tags = body.get("tags")
        raw_exclude_tags = body.get("exclude_tags")
        open_only = bool(body.get("open_only", False))
        tags: tuple[str, ...] | None = None
        exclude_tags: tuple[str, ...] | None = None

        if isinstance(raw_tags, str):
            tags = tuple(tag.strip() for tag in raw_tags.split(",") if tag.strip())
        elif isinstance(raw_tags, (list, tuple)):
            tags = tuple(str(tag).strip() for tag in raw_tags if str(tag).strip())

        if isinstance(raw_exclude_tags, str):
            exclude_tags = tuple(tag.strip() for tag in raw_exclude_tags.split(",") if tag.strip())
        elif isinstance(raw_exclude_tags, (list, tuple)):
            exclude_tags = tuple(str(tag).strip() for tag in raw_exclude_tags if str(tag).strip())

        parsed_status = parse_bug_status(bt_mod, status)
        parsed_severity = parse_bug_severity(bt_mod, severity)
        total_count = bt.count_bugs(
            status=parsed_status,
            severity=parsed_severity,
            category=category,
            title_like=title_like if isinstance(title_like, str) else None,
            tags=tags,
            exclude_tags=exclude_tags,
            open_only=open_only,
        )
        bugs = bt.list_bugs(
            status=parsed_status,
            severity=parsed_severity,
            category=category,
            title_like=title_like if isinstance(title_like, str) else None,
            tags=tags,
            exclude_tags=exclude_tags,
            open_only=open_only,
            limit=max(total_count, limit) if replay_ready_only else limit,
        )
        bug_dicts = [_bug_to_dict(bug) for bug in bugs[:limit]]
        if include_replay_state or replay_ready_only:
            bug_dicts = _annotate_bug_dicts_with_replay_state(
                bt,
                bugs,
                replay_ready_only=replay_ready_only,
                limit=limit,
            )
        return {
            "bugs": bug_dicts[:limit],
            "count": len(bug_dicts) if replay_ready_only else total_count,
            "returned_count": len(bug_dicts[:limit]),
        }

    if action == "file":
        title = body.get("title", "")
        if not title:
            raise _ClientError("title is required to file a bug")
        severity = body.get("severity", "P2")
        category = parse_bug_category(bt_mod, body.get("category")) or bt_mod.BugCategory.OTHER
        description = body.get("description", "")
        tags_raw = body.get("tags")
        tags: tuple[str, ...] = ()
        if isinstance(tags_raw, str):
            tags = tuple(tag.strip() for tag in tags_raw.split(",") if tag.strip())
        elif isinstance(tags_raw, (list, tuple)):
            tags = tuple(str(tag).strip() for tag in tags_raw if str(tag).strip())

        source_kind = str(body.get("source_kind") or "workflow_api").strip() or "workflow_api"
        filed_by = str(body.get("filed_by") or "workflow_api").strip() or "workflow_api"
        decision_ref = str(body.get("decision_ref") or "").strip()
        discovered_in_run_id = _optional_text(body.get("discovered_in_run_id"))
        discovered_in_receipt_id = _optional_text(body.get("discovered_in_receipt_id"))
        owner_ref = _optional_text(body.get("owner_ref"))
        resume_ctx = body.get("resume_context")
        if resume_ctx is not None and not isinstance(resume_ctx, dict):
            raise _ClientError("resume_context must be a JSON object when provided")
        try:
            filed = bt.file_bug(
                title=title,
                severity=parse_bug_severity(bt_mod, severity) or bt_mod.BugSeverity.P2,
                category=category,
                description=description,
                filed_by=filed_by,
                source_kind=source_kind,
                decision_ref=decision_ref,
                discovered_in_run_id=discovered_in_run_id,
                discovered_in_receipt_id=discovered_in_receipt_id,
                owner_ref=owner_ref,
                tags=tags,
                resume_context=resume_ctx if isinstance(resume_ctx, dict) else None,
            )
        except ValueError as exc:
            raise _ClientError(str(exc)) from exc
        bug = filed[0] if isinstance(filed, tuple) else filed
        return {"filed": True, "bug": _bug_to_dict(bug)}

    if action == "search":
        title = body.get("title", "")
        if not title:
            raise _ClientError("title is required for search")
        bugs = bt.search(title, limit=20)
        return {"bugs": [_bug_to_dict(bug) for bug in bugs], "count": len(bugs)}

    if action == "stats":
        return {"stats": _serialize(bt.stats())}

    if action == "packet":
        bug_id = str(body.get("bug_id", "")).strip()
        if not bug_id:
            raise _ClientError("bug_id is required to build a failure packet")
        packet = bt.failure_packet(
            bug_id,
            receipt_limit=max(1, int(body.get("receipt_limit", 5) or 5)),
        )
        if packet is None:
            raise _ClientError(f"bug not found: {bug_id}")
        return {"packet": _serialize(packet)}

    if action == "history":
        bug_id = str(body.get("bug_id", "")).strip()
        if not bug_id:
            raise _ClientError("bug_id is required to read bug history")
        packet = bt.failure_packet(
            bug_id,
            receipt_limit=max(1, int(body.get("receipt_limit", 5) or 5)),
        )
        if packet is None:
            raise _ClientError(f"bug not found: {bug_id}")
        agent_actions = _serialize(packet.get("agent_actions"))
        return {
            "history": _serialize(
                {
                    "bug_id": bug_id,
                    "signature": packet.get("signature"),
                    "blast_radius": packet.get("blast_radius"),
                    "historical_fixes": packet.get("historical_fixes"),
                    "fix_verification": packet.get("fix_verification"),
                    "replay_context": packet.get("replay_context"),
                    "resume_context": packet.get("resume_context"),
                    "semantic_neighbors": packet.get("semantic_neighbors"),
                    "agent_actions": {
                        "replay": agent_actions.get("replay") if isinstance(agent_actions, dict) else None,
                    },
                }
            )
        }

    if action == "replay":
        bug_id = str(body.get("bug_id", "")).strip()
        if not bug_id:
            raise _ClientError("bug_id is required to replay a bug")
        replay = bt.replay_bug(
            bug_id,
            receipt_limit=max(1, int(body.get("receipt_limit", 5) or 5)),
        )
        if replay is None:
            raise _ClientError(f"bug not found: {bug_id}")
        return {"replay": _serialize(replay)}

    if action == "backfill_replay":
        limit_raw = body.get("limit")
        limit = None if limit_raw in (None, "") else max(0, int(limit_raw))
        result = bt.bulk_backfill_replay_provenance(
            limit=limit,
            open_only=bool(body.get("open_only", True)),
            receipt_limit=max(1, int(body.get("receipt_limit", 1) or 1)),
        )
        return {"backfill": _serialize(result)}

    if action == "attach_evidence":
        bug_id = str(body.get("bug_id", "")).strip()
        evidence_kind = str(body.get("evidence_kind", "")).strip()
        evidence_ref = str(body.get("evidence_ref", "")).strip()
        evidence_role = str(body.get("evidence_role", "observed_in")).strip() or "observed_in"
        if not bug_id:
            raise _ClientError("bug_id is required to attach bug evidence")
        if not evidence_kind:
            raise _ClientError("evidence_kind is required to attach bug evidence")
        if not evidence_ref:
            raise _ClientError("evidence_ref is required to attach bug evidence")
        try:
            attached = bt.link_evidence(
                bug_id,
                evidence_kind=evidence_kind,
                evidence_ref=evidence_ref,
                evidence_role=evidence_role,
                created_by=str(body.get("created_by") or "workflow_api").strip() or "workflow_api",
                notes=_optional_text(body.get("notes")),
            )
        except ValueError as exc:
            raise _ClientError(str(exc)) from exc
        if attached is None:
            raise _ClientError("failed to attach bug evidence")
        return {"attached": True, "evidence_link": _serialize(attached)}

    if action == "resolve":
        bug_id = str(body.get("bug_id", "")).strip()
        if not bug_id:
            raise _ClientError("bug_id is required to resolve a bug")
        status = parse_bug_status(bt_mod, body.get("status"))
        if status is None:
            raise _ClientError("status is required to resolve a bug")
        if status not in resolved_statuses:
            allowed = ", ".join(sorted(item.value for item in resolved_statuses))
            raise _ClientError(f"resolve status must be one of {allowed}")
        try:
            bug = bt.resolve(bug_id, status)
        except ValueError as exc:
            raise _ClientError(str(exc)) from exc
        if bug is None:
            raise _ClientError(f"bug not found: {bug_id}")
        return {"resolved": True, "bug": _bug_to_dict(bug)}

    if action == "patch_resume":
        bug_id = str(body.get("bug_id", "")).strip()
        if not bug_id:
            raise _ClientError("bug_id is required to patch resume_context")
        raw_patch = body.get("resume_patch")
        if raw_patch is None:
            raw_patch = body.get("patch")
        if not isinstance(raw_patch, dict):
            raise _ClientError("resume_patch must be a JSON object")
        try:
            bug = bt.merge_resume_context(bug_id, raw_patch)
        except ValueError as exc:
            raise _ClientError(str(exc)) from exc
        if bug is None:
            raise _ClientError(f"bug not found: {bug_id}")
        return {"updated": True, "bug": _bug_to_dict(bug)}

    raise _ClientError(f"Unknown bug action: {action}")


def handle_recall(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    query = body.get("query", "")
    if not query:
        raise _ClientError("query is required")
    entity_type = body.get("entity_type") or None

    try:
        kg = subs.get_knowledge_graph()
        results = kg.search(query, entity_type=entity_type, limit=20)
        return {
            "results": [
                {
                    "entity_id": result.entity.id,
                    "name": result.entity.name,
                    "type": result.entity.entity_type.value,
                    "score": round(result.score, 4),
                    "content_preview": result.entity.content[:300],
                    "source": result.entity.source,
                    "found_via": result.found_via,
                    "provenance": result.provenance,
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
        results = search_receipts(query, status=status, agent=agent, limit=limit)
        return {"results": [record.to_search_result() for record in results], "count": len(results)}

    if action == "token_burn":
        since_hours = body.get("since_hours", 24)
        return {"token_burn": receipt_stats(since_hours=since_hours)}

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
        if sandbox_id == "sandbox_abc123":
            sandbox_id = ""
        if not sandbox_id:
            sandbox_id = store.latest_sandbox_id() or ""
        if not sandbox_id:
            raise _ClientError("sandbox_id is required for list and no sandbox artifacts were found")
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
    view = body.get("view", "status")
    if view == "replay_ready_bugs":
        bt = subs.get_bug_tracker()
        limit = max(1, int(body.get("limit", 50) or 50))
        refresh_backfill = bool(body.get("refresh_backfill", True))
        def _parse_status(_mod: Any, raw: object):
            if raw is None:
                return None
            status = _mod.BugTracker._normalize_status(raw, default=None)
            if status is None:
                raise _ClientError(
                    "status must be one of OPEN, IN_PROGRESS, FIXED, WONT_FIX, DEFERRED"
                )
            return status

        def _parse_severity(_mod: Any, raw: object):
            if raw is None:
                return None
            severity = _mod.BugTracker._normalize_severity(raw, default=None)
            if severity is None:
                raise _ClientError("severity must be one of P0, P1, P2, P3")
            return severity

        def _parse_category(_mod: Any, raw: object):
            if raw is None:
                return None
            category = _mod.BugTracker._normalize_category(raw, default=None)
            if category is None:
                raise _ClientError(
                    "category must be one of SCOPE, VERIFY, IMPORT, WIRING, ARCHITECTURE, RUNTIME, TEST, OTHER"
                )
            return category
        maintenance = None
        if refresh_backfill:
            maintenance = _serialize(
                bt.bulk_backfill_replay_provenance(
                    open_only=True,
                    receipt_limit=1,
                )
            )
        bugs_result = handle_bugs(
            subs,
            {
                "action": "list",
                "open_only": True,
                "replay_ready_only": True,
                "include_replay_state": True,
                "limit": limit,
            },
            parse_bug_status=_parse_status,
            parse_bug_severity=_parse_severity,
            parse_bug_category=_parse_category,
        )
        return {
            "view": view,
            "requires": {
                "runtime": "sync_postgres",
                "driver": "postgres",
            },
            "maintenance": maintenance,
            "bugs": bugs_result.get("bugs", []),
            "count": bugs_result.get("count", 0),
            "returned_count": bugs_result.get("returned_count", 0),
            "limit": limit,
            "refresh_backfill": refresh_backfill,
        }
    run_id = _optional_text(body.get("run_id"))
    view_options = ("status", "scoreboard", "graph", "lineage", "replay_ready_bugs")
    if view not in {"status", "scoreboard", "graph", "lineage"}:
        raise _ClientError(f"Unknown view: {view}. Options: {', '.join(view_options)}")
    if run_id is None:
        raise _ClientError(f"run_id is required for operator view '{view}'")

    from observability import (
        cutover_scoreboard_run,
        graph_lineage_run,
        graph_topology_run,
        load_native_operator_support,
        operator_status_run,
        render_cutover_scoreboard,
        render_operator_status,
    )
    from runtime.execution import RuntimeOrchestrator
    from surfaces.cli.render import render_graph_lineage, render_graph_topology
    from surfaces.api._operator_helpers import _run_async
    from storage.postgres import PostgresEvidenceReader

    evidence_reader = PostgresEvidenceReader()
    canonical_evidence = evidence_reader.evidence_timeline(run_id)
    inspection = RuntimeOrchestrator(evidence_reader=evidence_reader).inspect_run(run_id=run_id)
    support = _run_async(load_native_operator_support(run_id=run_id))

    if view == "status":
        read_model = operator_status_run(
            run_id=run_id,
            canonical_evidence=canonical_evidence,
            support=support,
        )
        return {
            "view": view,
            "run_id": run_id,
            "requires": {
                "runtime": "sync_postgres",
                "driver": "postgres",
            },
            "payload": _operator_view_payload(read_model),
            "rendered": render_operator_status(read_model),
        }

    if view == "graph":
        read_model = graph_topology_run(
            run_id=run_id,
            canonical_evidence=canonical_evidence,
        )
        return {
            "view": view,
            "run_id": run_id,
            "requires": {
                "runtime": "sync_postgres",
                "driver": "postgres",
            },
            "payload": _operator_view_payload(read_model),
            "rendered": render_graph_topology(read_model),
        }

    if view == "lineage":
        read_model = graph_lineage_run(
            run_id=run_id,
            canonical_evidence=canonical_evidence,
            operator_frame_source=inspection.operator_frame_source,
            operator_frames=inspection.operator_frames,
        )
        return {
            "view": view,
            "run_id": run_id,
            "requires": {
                "runtime": "sync_postgres",
                "driver": "postgres",
            },
            "payload": _operator_view_payload(read_model),
            "rendered": render_graph_lineage(read_model),
        }

    from surfaces.api import frontdoor

    status_payload = frontdoor.status(run_id=run_id)
    read_model = cutover_scoreboard_run(
        run_id=run_id,
        canonical_evidence=canonical_evidence,
        status_snapshot=status_payload.get("run"),
        support=support,
    )
    return {
        "view": view,
        "run_id": run_id,
        "requires": {
            "runtime": "sync_postgres",
            "driver": "postgres",
        },
        "payload": _operator_view_payload(read_model),
        "rendered": render_cutover_scoreboard(read_model),
    }
