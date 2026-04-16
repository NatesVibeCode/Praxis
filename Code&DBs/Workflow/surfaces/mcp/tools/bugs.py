"""Tools: praxis_bugs."""
from __future__ import annotations

from typing import Any

from surfaces.api.handlers import _bug_surface_contract as _bug_contract

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
    return _bug_contract.parse_bug_status(bt_mod, raw_status)


def _parse_bug_severity(bt_mod, raw_severity: object):
    return _bug_contract.parse_bug_severity(bt_mod, raw_severity)


def _parse_bug_category(bt_mod, raw_category: object):
    return _bug_contract.parse_bug_category(bt_mod, raw_category)


def _normalize_filed_severity(bt_mod, raw_severity: object):
    return _bug_contract.parse_bug_severity(bt_mod, raw_severity) or bt_mod.BugSeverity.P2


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

    try:
        if action == "list":
            return _bug_contract.list_bugs_payload(
                bt=bt,
                bt_mod=bt_mod,
                body=params,
                serialize_bug=_compact_bug,
                default_limit=25,
                include_replay_details=False,
                parse_status=_parse_bug_status,
                parse_severity=_parse_bug_severity,
                parse_category=_parse_bug_category,
            )

        if action == "file":
            return _bug_contract.file_bug_payload(
                bt=bt,
                bt_mod=bt_mod,
                body=params,
                serialize_bug=_bug_to_dict,
                filed_by_default="mcp_workflow_server",
                source_kind_default="mcp_workflow_server",
                include_similar_bugs=True,
                parse_severity=_parse_bug_severity,
                parse_category=_parse_bug_category,
            )

        if action == "search":
            return _bug_contract.search_bugs_payload(
                bt=bt,
                bt_mod=bt_mod,
                body=params,
                serialize_bug=_compact_bug,
                default_limit=20,
                parse_status=_parse_bug_status,
                parse_severity=_parse_bug_severity,
                parse_category=_parse_bug_category,
            )

        if action == "stats":
            return _bug_contract.stats_payload(bt=bt, serialize=_serialize)

        if action == "packet":
            return _bug_contract.packet_payload(bt=bt, body=params, serialize=_serialize)

        if action == "history":
            return _bug_contract.history_payload(bt=bt, body=params, serialize=_serialize)

        if action == "replay":
            return _bug_contract.replay_payload(bt=bt, body=params, serialize=_serialize)

        if action == "backfill_replay":
            return _bug_contract.backfill_replay_payload(bt=bt, body=params, serialize=_serialize)

        if action == "attach_evidence":
            return _bug_contract.attach_evidence_payload(
                bt=bt,
                body=params,
                serialize=_serialize,
                created_by_default="mcp_workflow_server",
            )

        if action == "resolve":
            return _bug_contract.resolve_bug_payload(
                bt=bt,
                bt_mod=bt_mod,
                body=params,
                serialize_bug=_bug_to_dict,
                resolved_statuses=resolved_statuses,
                parse_status=_parse_bug_status,
            )

        if action == "patch_resume":
            return _bug_contract.patch_resume_payload(
                bt=bt,
                body=params,
                serialize_bug=_bug_to_dict,
            )
    except ValueError as exc:
        return {"error": str(exc)}

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
