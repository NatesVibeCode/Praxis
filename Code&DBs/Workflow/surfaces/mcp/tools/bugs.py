"""Tools: praxis_bugs."""
from __future__ import annotations

from typing import Any

from ..runtime_context import get_current_workflow_mcp_context
from ..subsystems import _subs
from ..helpers import _bug_to_dict, _serialize

_DESC_TRUNCATE = 200


def _compact_bug(bug) -> dict:
    """Compact bug dict for list output — truncated description, fewer fields."""
    d = _bug_to_dict(bug)
    desc = d.get("description", "")
    if len(desc) > _DESC_TRUNCATE:
        d["description"] = desc[:_DESC_TRUNCATE] + "..."
    for key in ("filed_by", "source_kind", "decision_ref", "owner_ref",
                "discovered_in_run_id", "discovered_in_receipt_id",
                "resolution_summary", "assigned_to"):
        d.pop(key, None)
    return d


def _parse_bug_status(bt_mod, raw_status: object):
    if raw_status is None:
        return None
    status = bt_mod.BugTracker._normalize_status(raw_status, default=None)
    if status is None:
        raise ValueError("status must be one of OPEN, IN_PROGRESS, FIXED, WONT_FIX, DEFERRED")
    return status


def _parse_bug_severity(bt_mod, raw_severity: object):
    if raw_severity is None:
        return None
    severity = bt_mod.BugTracker._normalize_severity(raw_severity, default=None)
    if severity is None:
        raise ValueError("severity must be one of P0, P1, P2, P3")
    return severity


def _parse_bug_category(bt_mod, raw_category: object):
    if raw_category is None:
        return None
    category = bt_mod.BugTracker._normalize_category(raw_category, default=None)
    if category is None:
        raise ValueError(
            "category must be one of SCOPE, VERIFY, IMPORT, WIRING, ARCHITECTURE, RUNTIME, TEST, OTHER"
        )
    return category


def _normalize_filed_severity(bt_mod, raw_severity: object):
    if raw_severity is None:
        return bt_mod.BugSeverity.P2
    severity = bt_mod.BugTracker._normalize_severity(raw_severity, default=None)
    if severity is None:
        raise ValueError("severity must be one of P0, P1, P2, P3")
    return severity


_SESSION_BLOCKED_ACTIONS = frozenset({"list", "search", "stats", "backfill_replay"})


def tool_praxis_bugs(params: dict) -> dict:
    """Bug tracker operations: list, file, search, stats, packet, history, replay, backfill_replay, attach_evidence, patch_resume, resolve."""
    action = params.get("action", "list")

    # Sandboxed workflow sessions may only write bugs or look up a specific bug
    # by ID. Read-enumeration actions (list, search, stats) could expose bugs
    # from unrelated workflows, which may contain sensitive data.
    if action in _SESSION_BLOCKED_ACTIONS and get_current_workflow_mcp_context() is not None:
        return {
            "error": f"praxis_bugs action='{action}' is not permitted inside a workflow session. "
                     "Allowed actions: file, resolve, attach_evidence, packet, history, replay, patch_resume."
        }

    bt = _subs.get_bug_tracker()
    bt_mod = _subs.get_bug_tracker_mod()
    resolved_statuses = {
        bt_mod.BugStatus.FIXED,
        bt_mod.BugStatus.WONT_FIX,
        bt_mod.BugStatus.DEFERRED,
    }

    if action == "list":
        status = params.get("status")
        severity = params.get("severity")
        limit = max(1, int(params.get("limit", 25) or 25))
        try:
            parsed_status = _parse_bug_status(bt_mod, status)
            parsed_severity = _parse_bug_severity(bt_mod, severity)
            category = _parse_bug_category(bt_mod, params.get("category"))
        except ValueError as exc:
            return {"error": str(exc)}
        title_like = params.get("title_like")
        include_replay_state = bool(params.get("include_replay_state", True))
        replay_ready_only = bool(params.get("replay_ready_only", False))
        open_only = bool(params.get("open_only", False))
        raw_tags = params.get("tags")
        raw_exclude_tags = params.get("exclude_tags")
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
        bug_dicts = [_compact_bug(b) for b in bugs[:limit]]
        if include_replay_state or replay_ready_only:
            annotated: list[dict[str, Any]] = []
            for bug in bugs:
                bug_dict = _compact_bug(bug)
                hint = bt.replay_hint(bug.bug_id, receipt_limit=1)
                bug_dict["replay_ready"] = bool((hint or {}).get("available"))
                if replay_ready_only and not bug_dict["replay_ready"]:
                    continue
                annotated.append(bug_dict)
                if len(annotated) >= limit:
                    break
            bug_dicts = annotated
        return {
            "bugs": bug_dicts[:limit],
            "count": len(bug_dicts) if replay_ready_only else total_count,
            "returned_count": len(bug_dicts[:limit]),
        }

    if action == "file":
        title = params.get("title", "")
        if not title:
            return {"error": "title is required to file a bug"}
        try:
            category = _parse_bug_category(bt_mod, params.get("category")) or bt_mod.BugCategory.OTHER
        except ValueError as exc:
            return {"error": str(exc)}
        tags = tuple(
            str(tag).strip()
            for tag in (
                params.get("tags")
                if isinstance(params.get("tags"), (list, tuple))
                else str(params.get("tags") or "").split(",")
            )
            if str(tag).strip()
        )
        filed_by = str(params.get("filed_by") or "mcp_workflow_server").strip() or "mcp_workflow_server"
        resume_ctx = params.get("resume_context")
        if resume_ctx is not None and not isinstance(resume_ctx, dict):
            return {"error": "resume_context must be a JSON object when provided"}
        try:
            bug, similar_bugs = bt.file_bug(
                title=title,
                severity=_normalize_filed_severity(bt_mod, params.get("severity")),
                category=category,
                description=params.get("description", ""),
                filed_by=filed_by,
                source_kind=str(params.get("source_kind") or "mcp_workflow_server").strip() or "mcp_workflow_server",
                decision_ref=str(params.get("decision_ref") or "").strip(),
                discovered_in_run_id=str(params.get("discovered_in_run_id") or "").strip() or None,
                discovered_in_receipt_id=str(params.get("discovered_in_receipt_id") or "").strip() or None,
                owner_ref=str(params.get("owner_ref") or "").strip() or None,
                tags=tags,
                resume_context=resume_ctx if isinstance(resume_ctx, dict) else None,
            )
        except ValueError as exc:
            return {"error": str(exc)}
        result: dict = {"filed": True, "bug": _bug_to_dict(bug)}
        if similar_bugs:
            result["similar_bugs"] = similar_bugs
        return result

    if action == "search":
        title = params.get("title", "")
        if not title:
            return {"error": "title is required for search"}
        bugs = bt.search(title, limit=max(1, int(params.get("limit", 20) or 20)))
        return {"bugs": [_compact_bug(b) for b in bugs], "count": len(bugs)}

    if action == "stats":
        return {"stats": _serialize(bt.stats())}

    if action == "packet":
        bug_id = str(params.get("bug_id", "")).strip()
        if not bug_id:
            return {"error": "bug_id is required to build a failure packet"}
        packet = bt.failure_packet(
            bug_id,
            receipt_limit=max(1, int(params.get("receipt_limit", 5) or 5)),
        )
        if packet is None:
            return {"error": f"bug not found: {bug_id}"}
        return {"packet": _serialize(packet, strip_empty=True)}

    if action == "history":
        bug_id = str(params.get("bug_id", "")).strip()
        if not bug_id:
            return {"error": "bug_id is required to read bug history"}
        packet = bt.failure_packet(
            bug_id,
            receipt_limit=max(1, int(params.get("receipt_limit", 5) or 5)),
        )
        if packet is None:
            return {"error": f"bug not found: {bug_id}"}
        agent_actions = _serialize(packet.get("agent_actions"), strip_empty=True)
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
                },
                strip_empty=True,
            )
        }

    if action == "replay":
        bug_id = str(params.get("bug_id", "")).strip()
        if not bug_id:
            return {"error": "bug_id is required to replay a bug"}
        replay = bt.replay_bug(
            bug_id,
            receipt_limit=max(1, int(params.get("receipt_limit", 5) or 5)),
        )
        if replay is None:
            return {"error": f"bug not found: {bug_id}"}
        return {"replay": _serialize(replay)}

    if action == "backfill_replay":
        limit_raw = params.get("limit")
        limit = None if limit_raw in (None, "") else max(0, int(limit_raw))
        return {
            "backfill": _serialize(
                bt.bulk_backfill_replay_provenance(
                    limit=limit,
                    open_only=bool(params.get("open_only", True)),
                    receipt_limit=max(1, int(params.get("receipt_limit", 1) or 1)),
                )
            )
        }

    if action == "attach_evidence":
        bug_id = str(params.get("bug_id", "")).strip()
        evidence_kind = str(params.get("evidence_kind", "")).strip()
        evidence_ref = str(params.get("evidence_ref", "")).strip()
        evidence_role = str(params.get("evidence_role", "observed_in")).strip() or "observed_in"
        if not bug_id:
            return {"error": "bug_id is required to attach bug evidence"}
        if not evidence_kind:
            return {"error": "evidence_kind is required to attach bug evidence"}
        if not evidence_ref:
            return {"error": "evidence_ref is required to attach bug evidence"}
        try:
            link = bt.link_evidence(
                bug_id,
                evidence_kind=evidence_kind,
                evidence_ref=evidence_ref,
                evidence_role=evidence_role,
                created_by=str(params.get("created_by") or "mcp_workflow_server").strip() or "mcp_workflow_server",
                notes=str(params.get("notes") or "").strip() or None,
            )
        except ValueError as exc:
            return {"error": str(exc)}
        if link is None:
            return {"error": "failed to attach bug evidence"}
        return {"attached": True, "evidence_link": _serialize(link)}

    if action == "resolve":
        bug_id = str(params.get("bug_id", "")).strip()
        if not bug_id:
            return {"error": "bug_id is required to resolve a bug"}
        try:
            status = _parse_bug_status(bt_mod, params.get("status"))
        except ValueError as exc:
            return {"error": str(exc)}
        if status is None:
            return {"error": "status is required to resolve a bug"}
        if status not in resolved_statuses:
            allowed = ", ".join(sorted(item.value for item in resolved_statuses))
            return {"error": f"resolve status must be one of {allowed}"}
        try:
            bug = bt.resolve(bug_id, status)
        except ValueError as exc:
            return {"error": str(exc)}
        if bug is None:
            return {"error": f"bug not found: {bug_id}"}
        return {"resolved": True, "bug": _bug_to_dict(bug)}

    if action == "patch_resume":
        bug_id = str(params.get("bug_id", "")).strip()
        if not bug_id:
            return {"error": "bug_id is required to patch resume_context"}
        raw_patch = params.get("resume_patch")
        if raw_patch is None:
            raw_patch = params.get("patch")
        if not isinstance(raw_patch, dict):
            return {"error": "resume_patch must be a JSON object"}
        try:
            bug = bt.merge_resume_context(bug_id, raw_patch)
        except ValueError as exc:
            return {"error": str(exc)}
        if bug is None:
            return {"error": f"bug not found: {bug_id}"}
        return {"updated": True, "bug": _bug_to_dict(bug)}

    return {"error": f"Unknown bug action: {action}"}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_bugs": (
        tool_praxis_bugs,
        {
            "description": (
                "Track bugs in the platform's Postgres-backed bug tracker. List open bugs, file new "
                "ones, search by keyword, inspect similar historical fixes, replay a bug from canonical evidence, "
                "bulk backfill replay provenance, or resolve existing bugs.\n\n"
                "Search uses Postgres full-text ranking and may blend in vector similarity when the "
                "embedding lane is available.\n\n"
                "USE WHEN: something is broken and needs tracking, or you want to see known issues.\n\n"
                "EXAMPLES:\n"
                "  List open bugs:    praxis_bugs(action='list', status='OPEN')\n"
                "  File a new bug:    praxis_bugs(action='file', title='TaskAssembler fails on empty manifests', "
                "severity='P1', description='...')\n"
                "  Search for a bug:  praxis_bugs(action='search', title='routing')\n"
                "  Search open bugs:  praxis_bugs(action='search', title='timeout', status='OPEN')\n"
                "  Packet for a bug:  praxis_bugs(action='packet', bug_id='BUG-1234')\n"
                "  History for bug:   praxis_bugs(action='history', bug_id='BUG-1234')\n"
                "  Replay a bug:      praxis_bugs(action='replay', bug_id='BUG-1234')\n"
                "  Backfill replay:   praxis_bugs(action='backfill_replay')\n"
                "  Attach evidence:   praxis_bugs(action='attach_evidence', bug_id='BUG-1234', evidence_kind='receipt', evidence_ref='receipt:abc')\n"
                "  Patch handoff:     praxis_bugs(action='patch_resume', bug_id='BUG-1234', resume_patch={'hypothesis': '...', 'next_steps': ['...']})\n"
                "  Bug stats:         praxis_bugs(action='stats')\n"
                "  Resolve a bug:     praxis_bugs(action='resolve', bug_id='BUG-1234', status='WONT_FIX')\n\n"
                "STATUSES: OPEN, IN_PROGRESS, FIXED, WONT_FIX, DEFERRED\n"
                "SEVERITIES: P0 (critical), P1 (high), P2 (medium), P3 (low)"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Operation: 'list', 'file', 'search', 'stats', 'packet', 'history', 'replay', 'backfill_replay', 'attach_evidence', 'patch_resume', or 'resolve'.",
                        "enum": ["list", "file", "search", "stats", "packet", "history", "replay", "backfill_replay", "attach_evidence", "patch_resume", "resolve"],
                    },
                    "bug_id": {"type": "string", "description": "Bug id (for resolve)."},
                    "title": {"type": "string", "description": "Bug title (for file/search)."},
                    "severity": {"type": "string", "description": "Bug severity: P0, P1, P2, P3.", "default": "P2"},
                    "status": {"type": "string", "description": "Status filter (for list) or terminal resolution status (for resolve): OPEN, IN_PROGRESS, FIXED, WONT_FIX, DEFERRED."},
                    "category": {"type": "string", "description": "Bug category for list/file actions: SCOPE, VERIFY, IMPORT, WIRING, ARCHITECTURE, RUNTIME, TEST, OTHER."},
                    "title_like": {
                        "type": "string",
                        "description": "Substring match across title/description/summary (case-insensitive, for list).",
                    },
                    "open_only": {
                        "type": "boolean",
                        "description": "When true, excludes FIXED/WONT_FIX/DEFERRED statuses.",
                        "default": False,
                    },
                    "include_replay_state": {
                        "type": "boolean",
                        "description": "When true, annotate listed bugs with replay readiness and reason codes.",
                        "default": True,
                    },
                    "replay_ready_only": {
                        "type": "boolean",
                        "description": "When true, return only bugs that have authoritative replay provenance.",
                        "default": False,
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum bugs to return for list, or scan during backfill_replay when provided.",
                        "minimum": 0,
                        "default": 25,
                    },
                    "tags": {
                        "type": "array",
                        "description": "Tag filters for list (all tags must match).",
                        "items": {"type": "string"},
                    },
                    "exclude_tags": {
                        "type": "array",
                        "description": "Exclude bugs containing any of these tags.",
                        "items": {"type": "string"},
                    },
                    "description": {"type": "string", "description": "Bug description (for file)."},
                    "source_kind": {"type": "string", "description": "Provenance source for file actions."},
                    "filed_by": {"type": "string", "description": "Caller identity for file actions."},
                    "decision_ref": {"type": "string", "description": "Decision reference for file actions."},
                    "discovered_in_run_id": {"type": "string", "description": "Run provenance for file actions."},
                    "discovered_in_receipt_id": {"type": "string", "description": "Receipt provenance for file actions."},
                    "owner_ref": {"type": "string", "description": "Bug owner reference for file actions."},
                    "receipt_limit": {"type": "integer", "description": "How many recent receipts to include in packet output.", "minimum": 1, "default": 5},
                    "evidence_kind": {"type": "string", "description": "Evidence kind for attach_evidence, such as receipt, run, verification_run, or healing_run."},
                    "evidence_ref": {"type": "string", "description": "Evidence reference id for attach_evidence."},
                    "evidence_role": {"type": "string", "description": "Evidence role for attach_evidence, such as observed_in, attempted_fix, or validates_fix."},
                    "created_by": {"type": "string", "description": "Actor attaching evidence."},
                    "notes": {"type": "string", "description": "Optional notes for attach_evidence."},
                    "resume_context": {
                        "type": "object",
                        "description": "Optional initial investigator handoff when filing (hypothesis, next_steps, etc.).",
                    },
                    "resume_patch": {
                        "type": "object",
                        "description": "Shallow merge into bugs.resume_context for patch_resume (replaces whole array values).",
                    },
                    "patch": {
                        "type": "object",
                        "description": "Alias for resume_patch on patch_resume.",
                    },
                },
                "required": ["action"],
            },
        },
    ),
}
