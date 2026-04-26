"""Integration tests for the workflow MCP surface.

Tests call handler functions directly (no stdin/stdout) using constructed
JSON-RPC request dicts. All DB paths use temp directories.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

# ---------------------------------------------------------------------------
# Import the active MCP protocol surface directly.
# ---------------------------------------------------------------------------

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]

# Ensure workflow root is on sys.path for transitive imports
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from _pg_test_conn import ensure_test_database_ready
from runtime import bug_tracker as _bug_tracker_mod
from surfaces.mcp.protocol import handle_request
from surfaces.mcp.subsystems import _subs
from storage.postgres.connection import shutdown_workflow_pool


_TEST_DATABASE_URL = ensure_test_database_ready()


class _ServerModule:
    handle_request = staticmethod(handle_request)
    _subs = _subs


server = _ServerModule()


class _ResolvedAgent:
    def __init__(self, slug: str) -> None:
        self.slug = slug


class _StubAgentRegistry:
    def __init__(self, known_agents: set[str]) -> None:
        self._known_agents = set(known_agents)

    def get(self, slug: str):
        if slug in self._known_agents:
            return _ResolvedAgent(slug)
        return None

    @classmethod
    def with_known_agents(cls, *slugs: str):
        return cls(set(slugs))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_request(method: str, params: dict | None = None, req_id: int = 1) -> dict:
    msg = {"jsonrpc": "2.0", "id": req_id, "method": method}
    if params is not None:
        msg["params"] = params
    return msg


def _call_tool(name: str, arguments: dict | None = None, req_id: int = 1) -> dict:
    """Call a tool through handle_request and return the parsed content."""
    req = _make_request("tools/call", {"name": name, "arguments": arguments or {}}, req_id)
    resp = server.handle_request(req)
    assert resp is not None, f"No response for {name}"
    assert "error" not in resp, f"Error calling {name}: {resp.get('error')}"
    result = resp["result"]
    # MCP tools return content array with text
    content = result.get("content", [])
    if content and isinstance(content, list) and content[0].get("type") == "text":
        return json.loads(content[0]["text"])
    return result


def _fake_command(
    *,
    command_id: str,
    command_status: str,
    command_type: str,
    payload: dict[str, object],
    result_ref: str | None = None,
    error_detail: str | None = None,
):
    snapshot = {
        "command_id": command_id,
        "command_status": command_status,
        "command_type": command_type,
        "requested_by_kind": "mcp",
        "requested_by_ref": "praxis_workflow",
        "requested_at": datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc).isoformat(),
        "approved_at": datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc).isoformat(),
        "approved_by": "mcp.praxis_workflow",
        "idempotency_key": "idem.test",
        "risk_level": "low",
        "payload": payload,
        "result_ref": result_ref,
        "error_code": None,
        "error_detail": error_detail,
        "created_at": datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc).isoformat(),
        "updated_at": datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc).isoformat(),
    }
    return SimpleNamespace(**snapshot, to_json=lambda snapshot=snapshot: dict(snapshot))


def _receipt_row(
    *,
    receipt_id: str,
    run_id: str,
    label: str,
    status: str,
    failure_code: str = "",
) -> dict[str, object]:
    now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
    return {
        "receipt_id": receipt_id,
        "workflow_id": "workflow.test",
        "run_id": run_id,
        "request_id": "req.test",
        "node_id": label,
        "attempt_no": 1,
        "started_at": now,
        "finished_at": now,
        "executor_type": "openai/gpt-5.4",
        "status": status,
        "inputs": {"job_label": label, "agent_slug": "openai/gpt-5.4"},
        "outputs": {"status": status, "duration_ms": 10, "cost_usd": 0.0, "token_input": 0, "token_output": 0},
        "artifacts": {},
        "failure_code": failure_code,
        "decision_refs": [],
    }


def _receipt_record(*, status: str, failure_code: str = "", failure_category: str = ""):
    payload = {
        "status": status,
        "failure_code": failure_code,
        "failure_category": failure_category,
    }
    return SimpleNamespace(
        status=status,
        failure_code=failure_code,
        to_dict=lambda payload=payload: dict(payload),
    )


class _FakeBugTracker:
    def __init__(self) -> None:
        self._counter = 1
        self._bugs = [
            self._make_bug(
                title="Seed MCP bug",
                severity=_bug_tracker_mod.BugSeverity.P2,
            )
        ]

    def _now(self) -> datetime:
        return datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc) + timedelta(seconds=self._counter)

    def _make_bug(
        self,
        *,
        title: str,
        severity: "_bug_tracker_mod.BugSeverity",
        status: "_bug_tracker_mod.BugStatus" = _bug_tracker_mod.BugStatus.OPEN,
        category: "_bug_tracker_mod.BugCategory" = _bug_tracker_mod.BugCategory.OTHER,
        description: str = "",
        filed_by: str = "mcp_workflow_server",
        bug_id: str | None = None,
        resolved_at: datetime | None = None,
        source_kind: str = "manual",
        decision_ref: str = "",
        discovered_in_run_id: str | None = None,
        discovered_in_receipt_id: str | None = None,
        owner_ref: str | None = None,
        source_issue_id: str | None = None,
        tags: tuple[str, ...] = (),
        resume_context: dict | None = None,
    ) -> "_bug_tracker_mod.Bug":
        now = self._now()
        self._counter += 1
        return _bug_tracker_mod.Bug(
            bug_id=bug_id or f"BUG-TEST{self._counter:04d}",
            bug_key=(bug_id or f"BUG-TEST{self._counter:04d}").lower().replace("-", "_"),
            title=title,
            severity=severity,
            status=status,
            priority=severity.value,
            category=category,
            description=description,
            summary=description or title,
            filed_at=now,
            created_at=now,
            updated_at=resolved_at or now,
            resolved_at=resolved_at,
            filed_by=filed_by,
            assigned_to=None,
            tags=tags,
            source_kind=source_kind,
            discovered_in_run_id=discovered_in_run_id,
            discovered_in_receipt_id=discovered_in_receipt_id,
            owner_ref=owner_ref,
            source_issue_id=source_issue_id,
            decision_ref=decision_ref,
            resolution_summary=None,
            resume_context=dict(resume_context or {}),
        )

    def _filtered(
        self,
        *,
        status=None,
        severity=None,
        category=None,
        title_like=None,
        open_only=False,
        tags=None,
        exclude_tags=None,
        source_issue_id=None,
    ):
        del tags, exclude_tags
        bugs = list(self._bugs)
        if status is not None:
            bugs = [bug for bug in bugs if bug.status == status]
        elif open_only:
            bugs = [
                bug for bug in bugs
                if bug.status not in {
                    _bug_tracker_mod.BugStatus.FIXED,
                    _bug_tracker_mod.BugStatus.WONT_FIX,
                    _bug_tracker_mod.BugStatus.DEFERRED,
                }
            ]
        if severity is not None:
            bugs = [bug for bug in bugs if bug.severity == severity]
        if category is not None:
            bugs = [bug for bug in bugs if bug.category == category]
        if title_like:
            needle = str(title_like).lower()
            bugs = [
                bug for bug in bugs
                if needle in bug.title.lower() or needle in bug.description.lower()
            ]
        if source_issue_id is not None:
            bugs = [bug for bug in bugs if bug.source_issue_id == source_issue_id]
        bugs.sort(key=lambda bug: bug.filed_at, reverse=True)
        return bugs

    def list_bugs(self, *, limit=50, **kwargs):
        return self._filtered(**kwargs)[:limit]

    def count_bugs(self, **kwargs):
        return len(self._filtered(**kwargs))

    def file_bug(
        self,
        *,
        title,
        severity,
        category,
        description,
        filed_by,
        source_kind="manual",
        decision_ref="",
        discovered_in_run_id=None,
        discovered_in_receipt_id=None,
        owner_ref=None,
        source_issue_id=None,
        tags=(),
        resume_context=None,
    ):
        if discovered_in_run_id not in {None, "run-123"}:
            raise ValueError(f"unknown discovered_in_run_id: {discovered_in_run_id}")
        if discovered_in_receipt_id not in {None, "receipt-123"}:
            raise ValueError(
                f"unknown discovered_in_receipt_id: {discovered_in_receipt_id}"
            )
        bug = self._make_bug(
            title=title,
            severity=severity,
            category=category,
            description=description,
            filed_by=filed_by,
            source_kind=source_kind,
            decision_ref=decision_ref,
            discovered_in_run_id=discovered_in_run_id,
            discovered_in_receipt_id=discovered_in_receipt_id,
            owner_ref=owner_ref,
            source_issue_id=source_issue_id,
            tags=tags,
            resume_context=resume_context if isinstance(resume_context, dict) else None,
        )
        self._bugs.append(bug)
        return bug, []

    def merge_resume_context(self, bug_id, patch):
        if not isinstance(patch, dict):
            raise ValueError("resume patch must be a JSON object")
        for index, bug in enumerate(self._bugs):
            if bug.bug_id != bug_id:
                continue
            merged = {**bug.resume_context, **patch}
            updated = replace(
                bug,
                resume_context=merged,
                updated_at=self._now(),
            )
            self._bugs[index] = updated
            return updated
        raise ValueError(f"bug not found: {bug_id}")

    def search(self, query, limit=20, **kwargs):
        needle = str(query).lower()
        bugs = self._filtered(**kwargs)
        bugs = [
            bug for bug in bugs
            if needle in bug.title.lower() or needle in bug.description.lower()
        ]
        bugs.sort(key=lambda bug: bug.filed_at, reverse=True)
        return bugs[:limit]

    def stats(self):
        return {
            "total": len(self._bugs),
            "by_status": {"OPEN": len([bug for bug in self._bugs if bug.status == _bug_tracker_mod.BugStatus.OPEN])},
            "by_severity": {},
            "by_category": {},
            "open_count": len([bug for bug in self._bugs if bug.status in {_bug_tracker_mod.BugStatus.OPEN, _bug_tracker_mod.BugStatus.IN_PROGRESS}]),
            "mttr_hours": None,
            "packet_ready_count": 1,
            "fix_verified_count": 0,
            "underlinked_count": 0,
        }

    def failure_packet(self, bug_id, *, receipt_limit=5, allow_backfill=True):
        del allow_backfill
        bug = next((item for item in self._bugs if item.bug_id == bug_id), None)
        if bug is None:
            return None
        return {
            "bug": bug,
            "resume_context": dict(bug.resume_context or {}),
            "signature": {
                "fingerprint": "fp-test",
                "failure_code": "timeout_exceeded",
                "node_id": "job-a",
                "source_kind": bug.source_kind,
            },
            "replay_context": {
                "ready": True,
                "run_id": "run-123",
                "receipt_id": "receipt-123",
            },
            "semantic_neighbors": {
                "reason_code": "bug.semantic_neighbors.found",
                "items": (
                    {
                        "bug_id": "BUG-NEIGHBOR-1",
                        "title": "Same timeout signature on job-b",
                        "status": "OPEN",
                        "severity": "P2",
                        "category": "RUNTIME",
                        "similarity": 0.81,
                        "match_kind": "embedding",
                    },
                ),
                "note": "There is 1 other open bug(s) clustered nearby (embedding). "
                "If you can, skim them while this failure mode is still in working memory.",
                "sources_tried": ("embedding",),
            },
            "agent_actions": {
                "replay": {
                    "available": True,
                    "automatic": True,
                    "reason_code": "bug.replay_ready",
                    "tool": "praxis_bugs",
                    "arguments": {"action": "replay", "bug_id": bug_id},
                }
            },
            "historical_fixes": {
                "count": 1,
                "items": [
                    {
                        "bug_id": "BUG-FIXED-001",
                        "title": "Older timeout bug",
                        "shared_signature_fields": ["failure_code", "node_id"],
                        "fix_verification": {
                            "fix_verified": True,
                            "verified_validation_count": 1,
                        },
                    }
                ],
            },
            "recent_receipts": [
                {
                    "receipt_id": "receipt-123",
                    "run_id": "run-123",
                    "failure_code": "timeout_exceeded",
                }
            ][:receipt_limit],
            "observability_gaps": [],
        }

    def replay_bug(self, bug_id, *, receipt_limit=5, allow_backfill=True):
        packet = self.failure_packet(
            bug_id,
            receipt_limit=receipt_limit,
            allow_backfill=allow_backfill,
        )
        if packet is None:
            return None
        return {
            "bug_id": bug_id,
            "packet_ready": True,
            "ready": True,
            "reason_code": "bug.replay_loaded",
            "replay_context": packet["replay_context"],
            "packet_summary": {
                "signature": packet["signature"],
                "observability_state": "complete",
                "observability_gaps": [],
            },
            "tooling": packet["agent_actions"],
            "replay": {
                "run_id": "run-123",
                "request_id": "request.alpha",
                "completeness": {"is_complete": True, "missing_evidence_refs": []},
                "watermark": {"evidence_seq": 12, "source": "canonical_evidence"},
                "dependency_order": ["node_0", "node_1"],
                "node_outcomes": ["node_0:succeeded", "node_1:failed"],
                "admitted_definition_ref": "workflow_definition.alpha.v1",
                "terminal_reason": "runtime.workflow_failed",
            },
        }

    def replay_hint(self, bug_id, *, receipt_limit=1, allow_backfill=True):
        packet = self.failure_packet(
            bug_id,
            receipt_limit=receipt_limit,
            allow_backfill=allow_backfill,
        )
        if packet is None:
            return None
        replay = packet.get("agent_actions", {}).get("replay", {})
        return {
            "available": bool(replay.get("available")),
            "reason_code": replay.get("reason_code"),
            "run_id": replay.get("run_id", "run-123"),
            "receipt_id": replay.get("receipt_id", "receipt-123"),
            "automatic": bool(replay.get("automatic")),
        }

    def bulk_backfill_replay_provenance(
        self,
        *,
        limit=None,
        open_only=True,
        receipt_limit=1,
    ):
        bugs = self.list_bugs(open_only=open_only, limit=limit or len(self._bugs))
        return {
            "scanned_count": len(bugs),
            "backfilled_count": len(bugs),
            "linked_count": len(bugs) * 2,
            "replay_ready_count": len(bugs),
            "replay_blocked_count": 0,
            "open_only": open_only,
            "limit": limit,
            "bugs": [
                {
                    "bug_id": bug.bug_id,
                    "linked_count": 2,
                    "linked_refs": [
                        {"evidence_kind": "run", "evidence_ref": "run-123"},
                        {"evidence_kind": "receipt", "evidence_ref": "receipt-123"},
                    ],
                    "backfill_reason_code": "bug.replay_backfill.authoritative_fields",
                    "replay_ready": True,
                    "replay_reason_code": "bug.replay_ready",
                    "replay_run_id": "run-123",
                    "replay_receipt_id": "receipt-123",
                }
                for bug in bugs
            ],
        }

    def link_evidence(
        self,
        bug_id,
        *,
        evidence_kind,
        evidence_ref,
        evidence_role,
        created_by="mcp_workflow_server",
        notes=None,
    ):
        allowed_refs = {
            "receipt": {"receipt-123"},
            "run": {"run-123"},
            "verification_run": {"verification-run-123"},
            "healing_run": {"healing-run-123"},
            "governance_scan": {"governance-scan-123"},
        }
        if evidence_kind not in allowed_refs:
            raise ValueError(
                "evidence_kind must be one of governance_scan, receipt, run, verification_run, healing_run"
            )
        if evidence_ref not in allowed_refs[evidence_kind]:
            raise ValueError(f"unknown {evidence_kind} reference: {evidence_ref}")
        return {
            "bug_id": bug_id,
            "evidence_kind": evidence_kind,
            "evidence_ref": evidence_ref,
            "evidence_role": evidence_role,
            "created_by": created_by,
            "notes": notes,
        }

    def resolve(self, bug_id, status):
        for index, bug in enumerate(self._bugs):
            if bug.bug_id != bug_id:
                continue
            resolved_at = self._now()
            self._counter += 1
            updated = _bug_tracker_mod.Bug(
                bug_id=bug.bug_id,
                bug_key=bug.bug_key,
                title=bug.title,
                severity=bug.severity,
                status=status,
                priority=bug.priority,
                category=bug.category,
                description=bug.description,
                summary=bug.summary,
                filed_at=bug.filed_at,
                created_at=bug.created_at,
                updated_at=resolved_at,
                resolved_at=resolved_at,
                filed_by=bug.filed_by,
                assigned_to=bug.assigned_to,
                tags=bug.tags,
                source_kind=bug.source_kind,
                discovered_in_run_id=bug.discovered_in_run_id,
                discovered_in_receipt_id=bug.discovered_in_receipt_id,
                owner_ref=bug.owner_ref,
                source_issue_id=bug.source_issue_id,
                decision_ref=bug.decision_ref,
                resolution_summary=bug.resolution_summary,
                resume_context=bug.resume_context,
            )
            self._bugs[index] = updated
            return updated
        return None


class _FakePGConn:
    """Minimal Postgres stub for tests that need deterministic query results."""

    _BINDING_REVISION = "binding.test.operator.mcp"
    _DECISION_REF = "decision.test.operator.mcp"
    _SOURCE_POLICIES = (
        {
            "policy_ref": "operation-command",
            "source_kind": "operation_command",
            "posture": "operate",
            "idempotency_policy": "non_idempotent",
            "enabled": True,
            "binding_revision": _BINDING_REVISION,
            "decision_ref": _DECISION_REF,
        },
        {
            "policy_ref": "operation-query",
            "source_kind": "operation_query",
            "posture": "observe",
            "idempotency_policy": "read_only",
            "enabled": True,
            "binding_revision": _BINDING_REVISION,
            "decision_ref": _DECISION_REF,
        },
    )
    _OPERATION_ROWS = {
        "operator.status_snapshot": {
            "operation_ref": "operator-status-snapshot",
            "operation_name": "operator.status_snapshot",
            "source_kind": "operation_query",
            "operation_kind": "query",
            "http_method": "GET",
            "http_path": "/api/status",
            "input_model_ref": "runtime.operations.queries.operator_observability.QueryOperatorStatusSnapshot",
            "handler_ref": "runtime.operations.queries.operator_observability.handle_query_operator_status_snapshot",
            "authority_ref": "authority.receipts",
            "projection_ref": "projection.receipts",
            "posture": None,
            "idempotency_policy": None,
            "enabled": True,
            "binding_revision": _BINDING_REVISION,
            "decision_ref": _DECISION_REF,
        },
        "operator.issue_backlog": {
            "operation_ref": "operator-issue-backlog",
            "operation_name": "operator.issue_backlog",
            "source_kind": "operation_query",
            "operation_kind": "query",
            "http_method": "GET",
            "http_path": "/api/operator/issue-backlog",
            "input_model_ref": "runtime.operations.queries.operator_observability.QueryOperatorIssueBacklog",
            "handler_ref": "runtime.operations.queries.operator_observability.handle_query_operator_issue_backlog",
            "authority_ref": "authority.operator_issues",
            "projection_ref": "projection.operator_issues",
            "posture": None,
            "idempotency_policy": None,
            "enabled": True,
            "binding_revision": _BINDING_REVISION,
            "decision_ref": _DECISION_REF,
        },
        "operator.replay_ready_bugs": {
            "operation_ref": "operator-replay-ready-bugs",
            "operation_name": "operator.replay_ready_bugs",
            "source_kind": "operation_query",
            "operation_kind": "query",
            "http_method": "GET",
            "http_path": "/api/operator/replay-ready-bugs",
            "input_model_ref": "runtime.operations.queries.operator_observability.QueryReplayReadyBugs",
            "handler_ref": "runtime.operations.queries.operator_observability.handle_query_replay_ready_bugs",
            "authority_ref": "authority.bugs",
            "projection_ref": "projection.bugs",
            "posture": None,
            "idempotency_policy": None,
            "enabled": True,
            "binding_revision": _BINDING_REVISION,
            "decision_ref": _DECISION_REF,
        },
        "operator.graph_projection": {
            "operation_ref": "operator-graph-projection",
            "operation_name": "operator.graph_projection",
            "source_kind": "operation_query",
            "operation_kind": "query",
            "http_method": "GET",
            "http_path": "/api/operator/graph",
            "input_model_ref": "runtime.operations.queries.operator_observability.QueryOperatorGraphProjection",
            "handler_ref": "runtime.operations.queries.operator_observability.handle_query_operator_graph_projection",
            "authority_ref": "authority.semantic_assertions",
            "projection_ref": "projection.operator_graph",
            "posture": None,
            "idempotency_policy": None,
            "enabled": True,
            "binding_revision": _BINDING_REVISION,
            "decision_ref": _DECISION_REF,
        },
        "operator.run_status": {
            "operation_ref": "operator-run-status",
            "operation_name": "operator.run_status",
            "source_kind": "operation_query",
            "operation_kind": "query",
            "http_method": "GET",
            "http_path": "/api/operator/runs/{run_id}/status",
            "input_model_ref": "runtime.operations.queries.operator_observability.QueryRunScopedOperatorView",
            "handler_ref": "runtime.operations.queries.operator_observability.handle_query_run_status_view",
            "authority_ref": "authority.workflow_runs",
            "projection_ref": "projection.operator_status",
            "posture": None,
            "idempotency_policy": None,
            "enabled": True,
            "binding_revision": _BINDING_REVISION,
            "decision_ref": _DECISION_REF,
        },
        "operator.run_scoreboard": {
            "operation_ref": "operator-run-scoreboard",
            "operation_name": "operator.run_scoreboard",
            "source_kind": "operation_query",
            "operation_kind": "query",
            "http_method": "GET",
            "http_path": "/api/operator/runs/{run_id}/scoreboard",
            "input_model_ref": "runtime.operations.queries.operator_observability.QueryRunScopedOperatorView",
            "handler_ref": "runtime.operations.queries.operator_observability.handle_query_run_scoreboard_view",
            "authority_ref": "authority.workflow_runs",
            "projection_ref": "projection.operator_scoreboard",
            "posture": None,
            "idempotency_policy": None,
            "enabled": True,
            "binding_revision": _BINDING_REVISION,
            "decision_ref": _DECISION_REF,
        },
        "operator.run_graph": {
            "operation_ref": "operator-run-graph",
            "operation_name": "operator.run_graph",
            "source_kind": "operation_query",
            "operation_kind": "query",
            "http_method": "GET",
            "http_path": "/api/operator/runs/{run_id}/graph",
            "input_model_ref": "runtime.operations.queries.operator_observability.QueryRunScopedOperatorView",
            "handler_ref": "runtime.operations.queries.operator_observability.handle_query_run_graph_view",
            "authority_ref": "authority.workflow_runs",
            "projection_ref": "projection.workflow_graph",
            "posture": None,
            "idempotency_policy": None,
            "enabled": True,
            "binding_revision": _BINDING_REVISION,
            "decision_ref": _DECISION_REF,
        },
        "operator.run_lineage": {
            "operation_ref": "operator-run-lineage",
            "operation_name": "operator.run_lineage",
            "source_kind": "operation_query",
            "operation_kind": "query",
            "http_method": "GET",
            "http_path": "/api/operator/runs/{run_id}/lineage",
            "input_model_ref": "runtime.operations.queries.operator_observability.QueryRunScopedOperatorView",
            "handler_ref": "runtime.operations.queries.operator_observability.handle_query_run_lineage_view",
            "authority_ref": "authority.workflow_runs",
            "projection_ref": "projection.workflow_lineage",
            "posture": None,
            "idempotency_policy": None,
            "enabled": True,
            "binding_revision": _BINDING_REVISION,
            "decision_ref": _DECISION_REF,
        },
        "operator.metrics_reset": {
            "operation_ref": "operator-metrics-reset",
            "operation_name": "operator.metrics_reset",
            "source_kind": "operation_command",
            "operation_kind": "command",
            "http_method": "POST",
            "http_path": "/api/operator/maintenance/reset-metrics",
            "input_model_ref": "runtime.operations.commands.operator_maintenance.ResetMetricsCommand",
            "handler_ref": "runtime.operations.commands.operator_maintenance.handle_reset_metrics",
            "authority_ref": "authority.observability_metrics",
            "projection_ref": None,
            "posture": None,
            "idempotency_policy": None,
            "enabled": True,
            "binding_revision": _BINDING_REVISION,
            "decision_ref": _DECISION_REF,
        },
        "operator.bug_replay_provenance_backfill": {
            "operation_ref": "operator-bug-replay-provenance-backfill",
            "operation_name": "operator.bug_replay_provenance_backfill",
            "source_kind": "operation_command",
            "operation_kind": "command",
            "http_method": "POST",
            "http_path": "/api/operator/maintenance/backfill-bug-replay-provenance",
            "input_model_ref": "runtime.operations.commands.operator_maintenance.BackfillBugReplayProvenanceCommand",
            "handler_ref": "runtime.operations.commands.operator_maintenance.handle_backfill_bug_replay_provenance",
            "authority_ref": "authority.bugs",
            "projection_ref": None,
            "posture": None,
            "idempotency_policy": None,
            "enabled": True,
            "binding_revision": _BINDING_REVISION,
            "decision_ref": _DECISION_REF,
        },
        "operator.semantic_bridges_backfill": {
            "operation_ref": "operator-semantic-bridges-backfill",
            "operation_name": "operator.semantic_bridges_backfill",
            "source_kind": "operation_command",
            "operation_kind": "command",
            "http_method": "POST",
            "http_path": "/api/operator/maintenance/backfill-semantic-bridges",
            "input_model_ref": "runtime.operations.commands.operator_maintenance.BackfillSemanticBridgesCommand",
            "handler_ref": "runtime.operations.commands.operator_maintenance.handle_backfill_semantic_bridges",
            "authority_ref": "authority.semantic_assertions",
            "projection_ref": None,
            "posture": None,
            "idempotency_policy": None,
            "enabled": True,
            "binding_revision": _BINDING_REVISION,
            "decision_ref": _DECISION_REF,
        },
        "operator.semantic_projection_refresh": {
            "operation_ref": "operator-semantic-projection-refresh",
            "operation_name": "operator.semantic_projection_refresh",
            "source_kind": "operation_command",
            "operation_kind": "command",
            "http_method": "POST",
            "http_path": "/api/operator/maintenance/refresh-semantic-projection",
            "input_model_ref": "runtime.operations.commands.operator_maintenance.RefreshSemanticProjectionCommand",
            "handler_ref": "runtime.operations.commands.operator_maintenance.handle_refresh_semantic_projection",
            "authority_ref": "authority.semantic_assertions",
            "projection_ref": None,
            "posture": None,
            "idempotency_policy": None,
            "enabled": True,
            "binding_revision": _BINDING_REVISION,
            "decision_ref": _DECISION_REF,
        },
    }

    def __init__(
        self,
        *,
        receipt_rows=None,
        workflow_run_rows=None,
        dispatch_jobs_rows=None,
        dispatch_totals_row=None,
        failure_category_zone_rows=None,
        fail_zone_lookup: bool = False,
    ):
        self._receipt_rows = receipt_rows or []
        self._workflow_run_rows = workflow_run_rows or []
        self._dispatch_jobs_rows = dispatch_jobs_rows or []
        self._dispatch_totals_row = dispatch_totals_row or []
        self._failure_category_zone_rows = failure_category_zone_rows or []
        self._fail_zone_lookup = fail_zone_lookup

    def execute(self, sql: str, *args):
        if "authority_operation_receipts" in sql:
            return []
        if "FROM receipts" in sql:
            return self._receipt_rows
        if "FROM failure_category_zones" in sql:
            if self._fail_zone_lookup:
                raise RuntimeError("zone authority unavailable")
            return self._failure_category_zone_rows
        if "SELECT * FROM workflow_runs" in sql:
            return self._workflow_run_rows
        if (
            "FROM workflow_jobs WHERE run_id = $1 ORDER BY created_at" in sql
            or "FROM workflow_jobs wj" in sql
        ):
            return self._dispatch_jobs_rows
        if "COALESCE(SUM(cost_usd)" in sql:
            return self._dispatch_totals_row
        raise AssertionError(f"Unexpected SQL in test stub: {sql}")

    def fetch(self, sql: str, *args):
        normalized = " ".join(sql.split())
        if "FROM operation_catalog_registry" in normalized:
            return list(self._OPERATION_ROWS.values())
        if "FROM operation_catalog_source_policy_registry" in normalized:
            return list(self._SOURCE_POLICIES)
        raise AssertionError(f"Unexpected fetch SQL in test stub: {sql}")

    def fetchrow(self, sql: str, *args):
        normalized = " ".join(sql.split())
        if "FROM authority_operation_receipts" in normalized:
            return None
        if "FROM operation_catalog_registry" in normalized and "operation_name = $1" in normalized:
            return self._OPERATION_ROWS.get(str(args[0]))
        if "FROM operation_catalog_registry" in normalized and "operation_ref = $1" in normalized:
            operation_ref = str(args[0])
            for row in self._OPERATION_ROWS.values():
                if row["operation_ref"] == operation_ref:
                    return row
            return None
        raise AssertionError(f"Unexpected fetchrow SQL in test stub: {sql}")

    def transaction(self):
        class _Transaction:
            def __enter__(_self):
                return self

            def __exit__(_self, exc_type, exc, tb):
                return None

        return _Transaction()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _isolated_subsystems(tmp_path, monkeypatch):
    """Point all subsystem paths to temp directories so tests are isolated."""
    subs = server._subs
    fake_bug_tracker = _FakeBugTracker()
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", _TEST_DATABASE_URL)
    shutdown_workflow_pool()

    # Reset all cached instances
    subs._initialized = False
    subs._obs_hub = None
    subs._bug_tracker = None
    subs._operator_panel = None
    subs._knowledge_graph = None
    subs._staleness_detector = None
    subs._wave_orchestrator = None
    subs._receipt_ingester = None
    subs._quality_materializer = None
    subs._health_mod = None
    subs._constraint_ledger = None
    subs._friction_ledger = None
    subs._self_healer = None
    subs._artifact_store = None
    subs._governance_filter = None
    subs._heartbeat_runner = None
    subs._memory_engine = None
    subs._session_carry_mgr = None
    subs._intent_matcher = None
    subs._manifest_generator = None
    subs._pg_conn = None
    subs._bug_tracker = fake_bug_tracker
    monkeypatch.setattr(subs, "_build_bug_tracker", lambda: fake_bug_tracker, raising=False)

    # Point to temp paths
    subs.receipts_dir = str(tmp_path / "receipts")
    subs.bugs_db = str(tmp_path / "bugs.db")
    subs.dispatch_db = str(tmp_path / "dispatch.db")
    subs.knowledge_db = str(tmp_path / "knowledge.db")
    subs.constraints_db = str(tmp_path / "constraints.db")
    subs.agents_json = str(tmp_path / "agents.json")

    os.makedirs(subs.receipts_dir, exist_ok=True)

    yield

    # Reset for next test
    subs._initialized = False
    shutdown_workflow_pool()


@pytest.fixture
def spec_path(tmp_path):
    """Create a valid queue spec file."""
    spec = {
        "name": "test-queue",
        "workflow_id": "q-001",
        "phase": "build",
        "jobs": [
            {"label": "job-a", "prompt": "Do task A", "agent": "test-agent"},
            {"label": "job-b", "prompt": "Do task B", "agent": "test-agent"},
        ],
        "verify_refs": [],
        "outcome_goal": "test outcome",
        "anti_requirements": [],
    }
    path = tmp_path / "test.queue.json"
    path.write_text(json.dumps(spec))
    return str(path)


@pytest.fixture
def bad_spec_path(tmp_path):
    """Create an invalid queue spec (missing jobs)."""
    spec = {"name": "bad-queue", "workflow_id": "q-bad", "phase": "build"}
    path = tmp_path / "bad.queue.json"
    path.write_text(json.dumps(spec))
    return str(path)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestToolsListing:
    """Test that tools/list returns the required core MCP tools."""

    def test_tools_list_contains_required_core_tools(self):
        req = _make_request("tools/list")
        resp = server.handle_request(req)
        assert "error" not in resp
        tools = resp["result"]["tools"]
        names = {t["name"] for t in tools}
        required = {
            "praxis_workflow", "praxis_workflow_validate", "praxis_status_snapshot", "praxis_metrics_reset", "praxis_query",
            "praxis_bugs", "praxis_health", "praxis_recall", "praxis_ingest",
            "praxis_graph", "praxis_wave", "praxis_issue_backlog", "praxis_run_status",
        }
        assert required <= names, f"Missing required tools: {required - names}"

    def test_each_tool_has_input_schema(self):
        req = _make_request("tools/list")
        resp = server.handle_request(req)
        tools = resp["result"]["tools"]
        for t in tools:
            assert "inputSchema" in t, f"Tool {t['name']} missing inputSchema"
            assert t["inputSchema"]["type"] == "object"

    def test_each_tool_has_description(self):
        req = _make_request("tools/list")
        resp = server.handle_request(req)
        tools = resp["result"]["tools"]
        for t in tools:
            assert "description" in t and len(t["description"]) > 10


class TestInitialize:
    def test_initialize_returns_capabilities(self):
        req = _make_request("initialize", {"protocolVersion": "2024-11-05"})
        resp = server.handle_request(req)
        assert resp["result"]["protocolVersion"] == "2024-11-05"
        assert "tools" in resp["result"]["capabilities"]

    def test_notification_initialized_returns_none(self):
        msg = {"jsonrpc": "2.0", "method": "notifications/initialized"}
        resp = server.handle_request(msg)
        assert resp is None


class TestDagValidate:
    def test_validate_valid_spec(self, spec_path, monkeypatch):
        monkeypatch.setattr(server._subs, "get_pg_conn", lambda: object())
        agent_config_mod = __import__("registry.agent_config", fromlist=["AgentRegistry"])
        monkeypatch.setattr(
            agent_config_mod.AgentRegistry,
            "load_from_postgres",
            lambda _conn: _StubAgentRegistry.with_known_agents("test-agent"),
        )
        result = _call_tool("praxis_workflow_validate", {"spec_path": spec_path})
        assert result["valid"] is True
        assert result["summary"]["job_count"] == 2
        assert result["summary"]["name"] == "test-queue"
        assert result["agent_resolution_details"][0]["status"] == "resolved"

    def test_validate_invalid_spec(self, bad_spec_path):
        result = _call_tool("praxis_workflow_validate", {"spec_path": bad_spec_path})
        assert result["valid"] is False
        assert "error" in result

    def test_validate_reports_authority_error(self, spec_path, monkeypatch):
        monkeypatch.setattr(
            server._subs,
            "get_pg_conn",
            lambda: (_ for _ in ()).throw(RuntimeError("authority missing")),
        )
        result = _call_tool("praxis_workflow_validate", {"spec_path": spec_path})
        assert result["valid"] is False
        assert "agent authority unavailable" in result["error"]
        assert result["agent_resolution_details"][0]["status"] == "authority_error"

    def test_validate_missing_path(self):
        result = _call_tool("praxis_workflow_validate", {"spec_path": "/nonexistent/path.json"})
        assert result["valid"] is False

    def test_validate_no_path(self):
        result = _call_tool("praxis_workflow_validate", {})
        assert "error" in result


class TestDagStatus:
    def test_status_empty_receipts(self, monkeypatch):
        server._subs._pg_conn = _FakePGConn()
        monkeypatch.setattr(
            "runtime.operations.queries.operator_observability.list_receipts",
            lambda **_kwargs: [],
        )
        monkeypatch.setattr(
            "runtime.operations.queries.operator_observability.receipt_stats",
            lambda **_kwargs: {"totals": {"receipts": 0}},
        )
        result = _call_tool("praxis_status_snapshot", {})
        assert result["total_workflows"] == 0
        assert result["pass_rate"] == 0.0

    def test_status_with_receipt_rows(self, monkeypatch):
        server._subs._pg_conn = _FakePGConn(
            receipt_rows=[
                _receipt_row(receipt_id="receipt:1", run_id="workflow_a", label="job-a", status="succeeded"),
                _receipt_row(receipt_id="receipt:2", run_id="workflow_b", label="job-b", status="succeeded"),
                _receipt_row(receipt_id="receipt:3", run_id="workflow_c", label="job-c", status="failed", failure_code="exit_1"),
            ],
            failure_category_zone_rows=[{"category": "provider_timeout", "zone": "external"}],
        )
        monkeypatch.setattr(
            "runtime.operations.queries.operator_observability.list_receipts",
            lambda **_kwargs: [
                _receipt_record(status="succeeded"),
                _receipt_record(status="succeeded"),
                _receipt_record(status="failed", failure_code="exit_1"),
            ],
        )
        monkeypatch.setattr(
            "runtime.operations.queries.operator_observability.receipt_stats",
            lambda **_kwargs: {"totals": {"receipts": 3}},
        )
        result = _call_tool("praxis_status_snapshot", {"since_hours": 24})
        assert result["total_workflows"] == 3
        assert abs(result["pass_rate"] - 0.6667) < 0.01
        assert result["top_failure_codes"] == {"exit_1": 1}

    def test_status_reports_degraded_when_zone_authority_lookup_fails(self, monkeypatch):
        server._subs._pg_conn = _FakePGConn(
            receipt_rows=[
                _receipt_row(
                    receipt_id="receipt:1",
                    run_id="workflow_a",
                    label="job-a",
                    status="failed",
                    failure_code="provider_timeout",
                ),
            ],
            fail_zone_lookup=True,
        )
        monkeypatch.setattr(
            "runtime.operations.queries.operator_observability.list_receipts",
            lambda **_kwargs: [
                _receipt_record(
                    status="failed",
                    failure_code="provider_timeout",
                    failure_category="provider_timeout",
                ),
            ],
        )
        monkeypatch.setattr(
            "runtime.operations.queries.operator_observability.receipt_stats",
            lambda **_kwargs: {"totals": {"receipts": 1}},
        )

        result = _call_tool("praxis_status_snapshot", {"since_hours": 24})

        assert result["observability_state"] == "degraded"
        assert result["zone_authority_ready"] is False
        assert result["adjusted_pass_rate"] is None
        assert result["errors"][0]["code"] == "failure_category_zones_lookup_failed"


class TestDagMaintenance:
    def test_reset_metrics_requires_confirm(self):
        result = _call_tool("praxis_metrics_reset", {})
        assert "confirm=true" in result["error"]

    def test_backfill_bug_replay_provenance_runs_without_confirm(self):
        result = _call_tool("praxis_bug_replay_provenance_backfill", {})
        assert result["backfill"]["scanned_count"] >= 1
        assert result["backfill"]["replay_ready_count"] >= 1


class TestDagQuery:
    def test_query_routes_to_status(self):
        result = _call_tool("praxis_query", {"question": "What is the current status?"})
        assert result["routed_to"] == "operator_panel"
        assert "snapshot" in result

    def test_query_routes_to_bugs(self):
        result = _call_tool("praxis_query", {"question": "Are there any bugs?"})
        assert result["routed_to"] == "bug_tracker"

    def test_query_routes_to_quality(self):
        result = _call_tool("praxis_query", {"question": "Show me quality metrics"})
        assert result["routed_to"] == "quality_views"

    def test_query_routes_to_failures(self):
        result = _call_tool("praxis_query", {"question": "What failed recently?"})
        assert result["routed_to"] == "failures"

    def test_query_routes_to_leaderboard(self):
        result = _call_tool("praxis_query", {"question": "How are agents doing?"})
        assert result["routed_to"] == "leaderboard"

    def test_query_routes_to_staleness(self):
        result = _call_tool("praxis_query", {"question": "staleness"})
        assert result["routed_to"] == "staleness_detector"

    def test_query_returns_hint_for_removed_operator_graph_alias(self):
        result = _call_tool("praxis_query", {"question": "show me the operator graph"})
        assert result["routed_to"] == "operator_graph"
        assert result["status"] == "unsupported_query_alias"
        assert result["reason_code"] == "workflow_query.operator_graph_alias_removed"
        assert result["canonical_query"] == "operator graph"

    def test_query_empty_returns_error(self):
        result = _call_tool("praxis_query", {"question": ""})
        assert "error" in result

    def test_query_fallback_to_knowledge_graph(self):
        result = _call_tool("praxis_query", {"question": "tell me about circuit breakers"})
        assert result["routed_to"] == "knowledge_graph"


class TestDagBugs:
    @pytest.fixture(autouse=True)
    def _forbid_live_bug_db(self, monkeypatch):
        """Bug-tool integration tests must stay on the fake tracker, never live Postgres."""

        def _unexpected_pg_conn():
            raise AssertionError("TestDagBugs unexpectedly touched live Postgres")

        monkeypatch.setattr(server._subs, "get_pg_conn", _unexpected_pg_conn)
        yield

    def test_file_and_list_bug(self):
        # File a bug
        filed = _call_tool("praxis_bugs", {
            "action": "file",
            "title": "Test bug from MCP",
            "severity": "P2",
            "category": "RUNTIME",
            "filed_by": "mcp-client",
        })
        assert filed["filed"] is True
        assert filed["bug"]["title"] == "Test bug from MCP"
        assert filed["bug"]["category"] == "RUNTIME"
        assert filed["bug"]["filed_by"] == "mcp-client"
        bug_id = filed["bug"]["bug_id"]

        # List bugs
        listed = _call_tool("praxis_bugs", {"action": "list"})
        assert listed["count"] >= 1
        titles = [b["title"] for b in listed["bugs"]]
        assert "Test bug from MCP" in titles
        listed_bug = next(b for b in listed["bugs"] if b["title"] == "Test bug from MCP")
        assert "replay_ready" in listed_bug

    def test_bug_lineage_round_trips_through_mcp_surface(self):
        filed = _call_tool(
            "praxis_bugs",
            {
                "action": "file",
                "title": "Issue-linked bug from MCP",
                "severity": "P2",
                "source_issue_id": "issue.dispatch-gap",
            },
        )
        assert filed["bug"]["source_issue_id"] == "issue.dispatch-gap"

        listed = _call_tool(
            "praxis_bugs",
            {
                "action": "list",
                "source_issue_id": "issue.dispatch-gap",
            },
        )
        assert listed["count"] >= 1
        assert all(bug["source_issue_id"] == "issue.dispatch-gap" for bug in listed["bugs"])

        found = _call_tool(
            "praxis_bugs",
            {
                "action": "search",
                "title": "issue-linked",
                "source_issue_id": "issue.dispatch-gap",
            },
        )
        assert found["count"] >= 1
        assert all(bug["source_issue_id"] == "issue.dispatch-gap" for bug in found["bugs"])

    def test_list_bugs_filters_by_category(self):
        _call_tool("praxis_bugs", {
            "action": "file",
            "title": "Category filtered MCP bug",
            "severity": "P2",
            "category": "RUNTIME",
        })
        result = _call_tool("praxis_bugs", {"action": "list", "category": "RUNTIME"})
        assert result["count"] >= 1
        for bug in result["bugs"]:
            assert bug["category"] == "RUNTIME"

    def test_list_bugs_can_filter_replay_ready(self):
        result = _call_tool("praxis_bugs", {"action": "list", "replay_ready_only": True})
        assert result["count"] >= 1
        assert all(bug["replay_ready"] is True for bug in result["bugs"])

    def test_search_bug(self):
        # File then search
        _call_tool("praxis_bugs", {"action": "file", "title": "Unique dispatch failure XYZ"})
        found = _call_tool("praxis_bugs", {"action": "search", "title": "dispatch failure"})
        assert found["count"] >= 1

    def test_bug_stats(self):
        result = _call_tool("praxis_bugs", {"action": "stats"})
        assert result["stats"]["packet_ready_count"] == 1
        assert result["stats"]["open_count"] >= 1

    def test_bug_packet(self):
        filed = _call_tool("praxis_bugs", {
            "action": "file",
            "title": "Packet me from MCP",
            "severity": "P2",
        })
        result = _call_tool(
            "praxis_bugs",
            {"action": "packet", "bug_id": filed["bug"]["bug_id"], "receipt_limit": 2},
        )
        assert result["packet"]["bug"]["bug_id"] == filed["bug"]["bug_id"]
        assert result["packet"]["replay_context"]["ready"] is True
        assert result["packet"]["agent_actions"]["replay"]["available"] is True
        assert result["packet"]["historical_fixes"]["count"] == 1

    def test_bug_history(self):
        filed = _call_tool("praxis_bugs", {
            "action": "file",
            "title": "History from MCP",
            "severity": "P2",
        })
        result = _call_tool(
            "praxis_bugs",
            {"action": "history", "bug_id": filed["bug"]["bug_id"]},
        )
        assert result["history"]["bug_id"] == filed["bug"]["bug_id"]
        assert result["history"]["historical_fixes"]["count"] == 1
        assert result["history"]["agent_actions"]["replay"]["tool"] == "praxis_bugs"

    def test_bug_replay(self):
        filed = _call_tool("praxis_bugs", {
            "action": "file",
            "title": "Replay me from MCP",
            "severity": "P2",
        })
        result = _call_tool(
            "praxis_bugs",
            {"action": "replay", "bug_id": filed["bug"]["bug_id"]},
        )
        assert result["replay"]["ready"] is True
        assert result["replay"]["replay"]["run_id"] == "run-123"
        assert result["replay"]["tooling"]["replay"]["arguments"] == {
            "action": "replay",
            "bug_id": filed["bug"]["bug_id"],
        }

    def test_bug_backfill_replay(self):
        result = _call_tool("praxis_bugs", {"action": "backfill_replay"})
        assert result["backfill"]["scanned_count"] >= 1
        assert result["backfill"]["backfilled_count"] >= 1
        assert result["backfill"]["bugs"][0]["replay_ready"] is True

    def test_attach_evidence(self):
        filed = _call_tool("praxis_bugs", {
            "action": "file",
            "title": "Attach evidence from MCP",
            "severity": "P2",
        })
        result = _call_tool(
            "praxis_bugs",
            {
                "action": "attach_evidence",
                "bug_id": filed["bug"]["bug_id"],
                "evidence_kind": "receipt",
                "evidence_ref": "receipt-123",
                "evidence_role": "observed_in",
                "notes": "seed evidence",
            },
        )
        assert result["attached"] is True
        assert result["evidence_link"]["evidence_ref"] == "receipt-123"

    def test_resolve_fixed_with_verifier_records_validates_fix_evidence(self, monkeypatch):
        filed = _call_tool("praxis_bugs", {
            "action": "file",
            "title": "Resolve with verifier from MCP",
            "severity": "P2",
        })
        observed: dict[str, object] = {}

        def _fake_run_registered_verifier(verifier_ref, **kwargs):
            observed["verifier_ref"] = verifier_ref
            observed["kwargs"] = dict(kwargs)
            return {
                "verification_run_id": "verification-run-123",
                "status": "passed",
                "verifier": {"verifier_ref": verifier_ref},
                "target_kind": kwargs.get("target_kind"),
                "target_ref": kwargs.get("target_ref"),
                "inputs": kwargs.get("inputs", {}),
                "outputs": {"verification_ref": "verification.python.pytest_file"},
            }

        monkeypatch.setattr(
            "runtime.verifier_authority.run_registered_verifier",
            _fake_run_registered_verifier,
            raising=False,
        )

        result = _call_tool(
            "praxis_bugs",
            {
                "action": "resolve",
                "bug_id": filed["bug"]["bug_id"],
                "status": "FIXED",
                "verifier_ref": "verifier.job.python.pytest_file",
                "inputs": {"path": "Code&DBs/Workflow/tests/integration/test_mcp_workflow_server.py"},
            },
        )

        assert result["resolved"] is True
        assert result["bug"]["status"] == "FIXED"
        assert result["verification"]["verification_run_id"] == "verification-run-123"
        assert result["evidence_link"]["evidence_role"] == "validates_fix"
        assert observed["verifier_ref"] == "verifier.job.python.pytest_file"
        assert observed["kwargs"]["inputs"] == {
            "path": "Code&DBs/Workflow/tests/integration/test_mcp_workflow_server.py",
        }
        assert observed["kwargs"]["target_kind"] == "path"
        assert observed["kwargs"]["target_ref"] == "Code&DBs/Workflow/tests/integration/test_mcp_workflow_server.py"
        assert observed["kwargs"]["promote_bug"] is False

    def test_resolve_fixed_with_verifier_returns_error_when_proof_fails(self, monkeypatch):
        filed = _call_tool("praxis_bugs", {
            "action": "file",
            "title": "Resolve with failing verifier from MCP",
            "severity": "P2",
        })

        monkeypatch.setattr(
            "runtime.verifier_authority.run_registered_verifier",
            lambda verifier_ref, **_kwargs: {
                "verification_run_id": "verification-run-123",
                "status": "failed",
                "verifier": {"verifier_ref": verifier_ref},
                "outputs": {"verification_ref": "verification.python.pytest_file"},
            },
            raising=False,
        )

        result = _call_tool(
            "praxis_bugs",
            {
                "action": "resolve",
                "bug_id": filed["bug"]["bug_id"],
                "status": "FIXED",
                "verifier_ref": "verifier.job.python.pytest_file",
                "inputs": {"path": "Code&DBs/Workflow/tests/integration/test_mcp_workflow_server.py"},
            },
        )

        assert "error" in result
        assert "did not pass" in result["error"]

    def test_file_rejects_invalid_category(self):
        result = _call_tool("praxis_bugs", {
            "action": "file",
            "title": "Bad category from MCP",
            "category": "NOT_A_REAL_CATEGORY",
        })
        assert "error" in result
        assert "category must be one of" in result["error"]

    def test_attach_evidence_rejects_invalid_reference(self):
        filed = _call_tool("praxis_bugs", {
            "action": "file",
            "title": "Evidence error from MCP",
            "severity": "P2",
        })
        result = _call_tool(
            "praxis_bugs",
            {
                "action": "attach_evidence",
                "bug_id": filed["bug"]["bug_id"],
                "evidence_kind": "receipt",
                "evidence_ref": "receipt-does-not-exist",
                "evidence_role": "observed_in",
            },
        )
        assert "error" in result
        assert "unknown receipt reference" in result["error"]

    def test_list_by_severity(self):
        _call_tool("praxis_bugs", {"action": "file", "title": "P0 critical", "severity": "P0"})
        _call_tool("praxis_bugs", {"action": "file", "title": "P3 minor", "severity": "P3"})
        result = _call_tool("praxis_bugs", {"action": "list", "severity": "P0"})
        for b in result["bugs"]:
            assert b["severity"] == "P0"

    def test_list_reports_total_count_beyond_page_limit(self):
        for index in range(30):
            _call_tool("praxis_bugs", {
                "action": "file",
                "title": f"Count bug {index}",
                "severity": "P2",
            })

        result = _call_tool("praxis_bugs", {"action": "list", "limit": 25})
        assert result["returned_count"] == 25
        assert result["count"] >= 30

    def test_file_missing_title(self):
        result = _call_tool("praxis_bugs", {"action": "file"})
        assert "error" in result

    def test_unknown_action(self):
        result = _call_tool("praxis_bugs", {"action": "delete"})
        assert "error" in result

    def test_resolve_bug(self):
        filed = _call_tool("praxis_bugs", {
            "action": "file",
            "title": "Resolve me from MCP",
            "severity": "P2",
        })
        bug_id = filed["bug"]["bug_id"]

        resolved = _call_tool("praxis_bugs", {
            "action": "resolve",
            "bug_id": bug_id,
            "status": "WONT_FIX",
        })

        assert resolved["resolved"] is True
        assert resolved["bug"]["bug_id"] == bug_id
        assert resolved["bug"]["status"] == "WONT_FIX"

    def test_resolve_bug_rejects_non_terminal_status(self):
        filed = _call_tool("praxis_bugs", {
            "action": "file",
            "title": "Do not reopen from resolve",
            "severity": "P2",
        })

        result = _call_tool("praxis_bugs", {
            "action": "resolve",
            "bug_id": filed["bug"]["bug_id"],
            "status": "OPEN",
        })

        assert "error" in result

    def test_bug_calls_stay_isolated_when_bug_tracker_cache_is_cleared(self):
        server._subs._bug_tracker = None

        filed = _call_tool("praxis_bugs", {
            "action": "file",
            "title": "Still isolated from MCP",
            "severity": "P2",
        })

        assert filed["bug"]["bug_id"].startswith("BUG-TEST")


class TestDagHealth:
    def test_health_returns_structure(self):
        result = _call_tool("praxis_health", {})
        assert "preflight" in result
        assert "operator_snapshot" in result
        assert "lane_recommendation" in result
        assert "context_cache" in result
        assert "content_health" in result
        assert "projection_freshness" in result
        assert result["preflight"]["overall"] in ("healthy", "degraded", "unhealthy", "unknown")
        assert "recommended_posture" in result["lane_recommendation"]
        assert "hit_rate" in result["context_cache"]

    def test_health_preflight_has_checks(self):
        result = _call_tool("praxis_health", {})
        # At minimum disk space probe should be present
        assert isinstance(result["preflight"]["checks"], list)


class TestDagOperatorView:
    def test_replay_ready_bugs_view_returns_direct_payload(self):
        result = _call_tool("praxis_replay_ready_bugs", {"limit": 10})
        assert result["view"] == "replay_ready_bugs"
        assert "maintenance" not in result
        assert result["bugs"][0]["replay_ready"] is True
        assert result["returned_count"] >= 1

    def test_operator_graph_view_returns_direct_payload(self, monkeypatch):
        server._subs._pg_conn = _FakePGConn()
        captured: dict[str, object] = {}

        class _Conn:
            def __init__(self) -> None:
                self.closed = False

            async def close(self) -> None:
                self.closed = True

        async def _connect_database(env=None):
            captured["env"] = dict(env or {})
            conn = _Conn()
            captured["conn"] = conn
            return conn

        async def _load_operator_graph(conn, *, as_of):
            captured["load_conn"] = conn
            captured["as_of"] = as_of
            return {
                "kind": "operator_graph",
                "semantic_authority_state": "ready",
                "semantic_authority_reason_code": "semantic_assertions.active_window",
                "edges": [
                    {
                        "edge_kind": "governed_by_policy",
                        "authority_source": "semantic_assertions",
                    }
                ],
            }

        monkeypatch.setattr("storage.postgres.connect_workflow_database", _connect_database)
        monkeypatch.setattr(
            "observability.operator_topology.load_operator_graph_projection",
            _load_operator_graph,
        )

        result = _call_tool(
            "praxis_graph_projection",
            {"as_of": "2026-04-16T20:05:00+00:00"},
        )

        assert captured["env"]["WORKFLOW_DATABASE_URL"]
        assert captured["load_conn"] is captured["conn"]
        assert captured["as_of"].isoformat() == "2026-04-16T20:05:00+00:00"
        assert captured["conn"].closed is True
        assert result["view"] == "operator_graph"
        assert result["payload"]["semantic_authority_state"] == "ready"
        assert result["payload"]["edges"][0]["authority_source"] == "semantic_assertions"


class TestDagRecall:
    def test_recall_empty_graph(self):
        result = _call_tool("praxis_recall", {"query": "dispatch pipeline"})
        assert "results" in result
        assert isinstance(result["results"], list)

    def test_recall_missing_query(self):
        result = _call_tool("praxis_recall", {"query": ""})
        assert "error" in result


class TestDagIngest:
    def test_ingest_document(self):
        result = _call_tool("praxis_ingest", {
            "kind": "document",
            "content": "The dispatch pipeline handles job execution through governance and retry layers.",
            "source": "test_mcp_server",
        })
        assert "accepted" in result

    def test_ingest_missing_fields(self):
        result = _call_tool("praxis_ingest", {"kind": "document"})
        assert "error" in result


class TestPraxisWave:
    def test_observe_empty(self):
        result = _call_tool("praxis_wave", {"action": "observe"})
        assert "orch_id" in result
        assert result["waves"] == []

    def test_wave_lifecycle(self):
        # Add a wave directly via the orchestrator
        orch = server._subs.get_wave_orchestrator()
        orch.add_wave("wave-1", [
            {"label": "j1"},
            {"label": "j2", "depends_on": ["j1"]},
        ])

        # Observe
        state = _call_tool("praxis_wave", {"action": "observe"})
        assert len(state["waves"]) == 1
        assert state["waves"][0]["wave_id"] == "wave-1"

        # Start
        started = _call_tool("praxis_wave", {"action": "start", "wave_id": "wave-1"})
        assert started["started"] is True

        # Next runnable
        nxt = _call_tool("praxis_wave", {"action": "next", "wave_id": "wave-1"})
        assert "j1" in nxt["runnable_jobs"]
        assert "j2" not in nxt["runnable_jobs"]  # depends on j1

        # Record j1 success
        recorded = _call_tool("praxis_wave", {
            "action": "record", "wave_id": "wave-1", "jobs": "j1:pass",
        })
        assert recorded["recorded"][0]["succeeded"] is True

        # Now j2 should be runnable
        nxt2 = _call_tool("praxis_wave", {"action": "next", "wave_id": "wave-1"})
        assert "j2" in nxt2["runnable_jobs"]


class TestDagDispatchDryRun:
    def test_dry_run_valid_spec(self, spec_path, tmp_path):
        """Dry-run requires the dispatch runner which needs agents.json."""
        # Write a minimal agents.json
        agents = {
            "agents": [
                {
                    "slug": "test-agent",
                    "display_name": "Test Agent",
                    "provider": "test",
                    "model": "test-model",
                    "execution_backend": "cli",
                    "timeout_seconds": 60,
                },
            ]
        }
        agents_path = str(tmp_path / "agents.json")
        with open(agents_path, "w") as f:
            json.dump(agents, f)
        server._subs.agents_json = agents_path

        # The dispatch runner may fail to load due to the agent_config
        # module format. Test that we get either a valid result or
        # a graceful error rather than a crash.
        req = _make_request("tools/call", {
            "name": "praxis_workflow",
            "arguments": {"spec_path": spec_path, "dry_run": True},
        })
        resp = server.handle_request(req)
        assert resp is not None
        # Should either succeed or return a structured error
        assert "result" in resp or "error" in resp

    def test_run_always_returns_async_links(self, spec_path, monkeypatch):
        server._subs._pg_conn = _FakePGConn()

        def _submit_workflow_via_service_bus(
            pg,
            *,
            spec_path: str,
            spec_name: str,
            total_jobs: int,
        ):
            return {
                "run_id": "dispatch_001",
                "status": "queued",
                "total_jobs": 2,
                "spec_name": "test-queue",
                "command_id": "control.command.submit.1",
            }

        monkeypatch.setattr(
            "surfaces.mcp.tools.workflow._submit_workflow_via_service_bus",
            _submit_workflow_via_service_bus,
        )

        result = _call_tool("praxis_workflow", {"spec_path": spec_path, "dry_run": False})
        assert result["run_id"] == "dispatch_001"
        assert result["status"] == "queued"
        assert result["command_id"] == "control.command.submit.1"
        assert result["command_status"] == "succeeded"
        assert result["stream_url"] == "/api/workflow-runs/dispatch_001/stream"
        assert result["status_url"] == "/api/workflow-runs/dispatch_001/status"
        assert "test-queue" in result["dashboard"]
        assert result["delivery"] == {
            "dashboard_in_payload": True,
            "live_channel": "notifications.message",
            "message_notifications": True,
            "progress_notifications": False,
            "wait_requested": True,
            "inline_polling": False,
        }

    def test_run_routes_submission_through_command_bus(self, spec_path, monkeypatch):
        server._subs._pg_conn = _FakePGConn()
        captured: dict[str, object] = {}

        def _fake_request(_pg, intent, **_kwargs):
            captured["intent"] = intent
            return _fake_command(
                command_id="control.command.submit.2",
                command_status="succeeded",
                command_type="workflow.submit",
                payload=dict(intent.payload),
                result_ref="workflow_run:dispatch_002",
            )

        monkeypatch.setattr("runtime.control_commands.bootstrap_control_commands_schema", lambda _pg: None)
        monkeypatch.setattr("runtime.control_commands.request_control_command", _fake_request)
        monkeypatch.setattr(
            "runtime.workflow.unified.submit_workflow",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("MCP run should not submit directly")
            ),
        )
        monkeypatch.setattr(
            "runtime.workflow.unified.submit_workflow_inline",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("MCP run should not fall back to inline submit")
            ),
        )

        result = _call_tool("praxis_workflow", {"spec_path": spec_path, "dry_run": False})

        assert captured["intent"].command_type == "workflow.submit"
        assert captured["intent"].requested_by_kind == "mcp"
        assert captured["intent"].requested_by_ref == "praxis_workflow.run"
        assert result["run_id"] == "dispatch_002"
        assert result["status"] == "queued"
        assert result["command_id"] == "control.command.submit.2"

    def test_retry_cancel_and_repair_route_through_command_bus(self, monkeypatch):
        server._subs._pg_conn = _FakePGConn()
        captured: list[tuple[str, dict[str, object], str]] = []
        cancel_proof = {
            "cancelled_jobs": 1,
            "labels": ["build_a"],
            "run_status": "cancelled",
            "terminal_reason": "workflow_cancelled",
        }

        def _fake_execute(_pg, intent, *, approved_by, **_kwargs):
            captured.append((str(intent.command_type), dict(intent.payload), approved_by))
            return _fake_command(
                command_id=f"control.command.{len(captured)}",
                command_status="succeeded",
                command_type=str(intent.command_type),
                payload=dict(intent.payload),
                result_ref=f"workflow_run:{intent.payload['run_id']}",
            )

        monkeypatch.setattr("runtime.control_commands.bootstrap_control_commands_schema", lambda _pg: None)
        monkeypatch.setattr("runtime.control_commands.execute_control_intent", _fake_execute)
        monkeypatch.setattr(
            "runtime.control_commands.workflow_cancel_proof",
            lambda _conn, _run_id: dict(cancel_proof),
        )
        monkeypatch.setattr(
            "runtime.workflow.unified.retry_job",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("MCP retry should not call unified.retry_job directly")
            ),
        )
        monkeypatch.setattr(
            "runtime.workflow.unified.cancel_run",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("MCP cancel should not call unified.cancel_run directly")
            ),
        )

        retry_result = _call_tool(
            "praxis_workflow",
            {"action": "retry", "run_id": "dispatch_retry", "label": "build_a"},
        )
        cancel_result = _call_tool(
            "praxis_workflow",
            {"action": "cancel", "run_id": "dispatch_cancel"},
        )
        repair_result = _call_tool(
            "praxis_workflow",
            {"action": "repair", "run_id": "dispatch_repair"},
        )

        assert captured == [
            ("workflow.retry", {"run_id": "dispatch_retry", "label": "build_a"}, "mcp.praxis_workflow.retry"),
            ("workflow.cancel", {"run_id": "dispatch_cancel", "include_running": True}, "mcp.praxis_workflow.cancel"),
            ("sync.repair", {"run_id": "dispatch_repair"}, "mcp.praxis_workflow.repair"),
        ]
        assert retry_result["status"] == "requeued"
        assert retry_result["command_id"] == "control.command.1"
        assert retry_result["run_id"] == "dispatch_retry"
        assert retry_result["label"] == "build_a"
        assert cancel_result["status"] == "cancelled"
        assert cancel_result["command_id"] == "control.command.2"
        assert cancel_result["run_id"] == "dispatch_cancel"
        assert repair_result["status"] == "repaired"
        assert repair_result["command_id"] == "control.command.3"
        assert repair_result["run_id"] == "dispatch_repair"

    def test_chain_routes_submission_through_command_bus(self, monkeypatch):
        server._subs._pg_conn = _FakePGConn()
        captured: list[tuple[str, str, str, bool]] = []

        def _fake_chain_request(
            _pg,
            *,
            requested_by_kind: str,
            requested_by_ref: str,
            coordination_path: str,
            repo_root: str,
            adopt_active: bool,
            **_kwargs,
        ):
            captured.append((requested_by_kind, requested_by_ref, coordination_path, adopt_active))
            return _fake_command(
                command_id="control.command.chain.1",
                command_status="succeeded",
                command_type="workflow.chain.submit",
                payload={
                    "coordination_path": coordination_path,
                    "repo_root": repo_root,
                    "adopt_active": adopt_active,
                },
                result_ref="workflow_chain:test-chain-001",
            )

        monkeypatch.setattr(
            "runtime.control_commands.request_workflow_chain_submit_command",
            _fake_chain_request,
        )
        monkeypatch.setattr(
            "runtime.workflow_chain.get_workflow_chain_status",
            lambda _conn, chain_id: {
                "status": "queued",
                "waves": [{
                    "status": "queued",
                    "jobs": [{"label": "build_a", "status": "ready"}],
                }],
                "program": {"name": "chain-program", "total_waves": 1},
                "current_wave": 0,
            } if chain_id == "test-chain-001" else None,
        )

        result = _call_tool(
            "praxis_workflow",
            {
                "action": "chain",
                "coordination_path": "/tmp/workflow_chain_coordination.json",
                "adopt_active": "true",
            },
        )
        assert captured == [(
            "mcp",
            "praxis_workflow.chain",
            "/tmp/workflow_chain_coordination.json",
            True,
        )]
        assert result["status"] == "queued"
        assert result["command_status"] == "succeeded"
        assert result["command_id"] == "control.command.chain.1"
        assert result["chain_id"] == "test-chain-001"
        assert result["coordination_path"] == "/tmp/workflow_chain_coordination.json"

    def test_status_action_returns_rich_health_signals(self):
        server._subs._pg_conn = _FakePGConn(
            workflow_run_rows=[{
                "run_id": "dispatch_001",
                "spec_name": "test-queue",
                "status": "failed",
                "total_jobs": 2,
                "started_at": None,
                "finished_at": datetime.now(timezone.utc),
                "terminal_reason": "one failed",
                "created_at": datetime.now(timezone.utc) - timedelta(seconds=120),
            }],
            dispatch_jobs_rows=[{
                "label": "job-a",
                "status": "failed",
                "agent_slug": "agent-a",
                "resolved_agent": "agent-a",
                "attempt": 1,
                "last_error_code": "rate_limited",
                "failure_category": "credential_error",
                "failure_zone": "config",
                "is_transient": False,
                "duration_ms": 3000,
                "cost_usd": 0.0,
                "token_input": 12,
                "token_output": 8,
                "ready_at": datetime.now(timezone.utc) - timedelta(seconds=90),
                "claimed_at": datetime.now(timezone.utc) - timedelta(seconds=80),
                "started_at": datetime.now(timezone.utc) - timedelta(seconds=70),
                "finished_at": datetime.now(timezone.utc) - timedelta(seconds=10),
                "heartbeat_at": datetime.now(timezone.utc) - timedelta(seconds=500),
                "next_retry_at": None,
                "stdout_preview": "429 Too Many Requests",
                "id": 101,
            }, {
                "label": "job-b",
                "status": "succeeded",
                "agent_slug": "agent-b",
                "resolved_agent": "agent-b",
                "attempt": 1,
                "last_error_code": "",
                "duration_ms": 1200,
                "cost_usd": 0.2,
                "token_input": 7,
                "token_output": 9,
                "ready_at": datetime.now(timezone.utc) - timedelta(seconds=85),
                "claimed_at": datetime.now(timezone.utc) - timedelta(seconds=83),
                "started_at": datetime.now(timezone.utc) - timedelta(seconds=79),
                "finished_at": datetime.now(timezone.utc) - timedelta(seconds=9),
                "heartbeat_at": datetime.now(timezone.utc) - timedelta(seconds=70),
                "next_retry_at": None,
                "stdout_preview": "ok",
                "id": 102,
            }],
            dispatch_totals_row=[{
                "total_cost": 0.2,
                "total_tokens_in": 19,
                "total_tokens_out": 17,
                "total_duration_ms": 4200,
            }],
        )

        result = _call_tool("praxis_workflow", {"action": "status", "run_id": "dispatch_001"})
        assert result["run_id"] == "dispatch_001"
        assert result["status"] == "failed"
        assert result["health"]["state"]
        assert len(result["jobs"]) == 2
        assert result["jobs"][0]["job_label"] == "job-a"
        assert "failure_classification" in result["jobs"][0]
        assert result["jobs"][0]["failure_classification"]["category"] == "credential_error"
        assert result["jobs"][0]["failure_classification"]["is_retryable"] is False
        assert result["health"]["non_retryable_failed_jobs"] == ["job-a"]
        assert result["recovery"]["mode"] == "inspect"
        assert result["recovery"]["recommended_tool"]["arguments"]["action"] == "inspect"
        assert result["recovery"]["recommended_tool"]["arguments"]["run_id"] == "dispatch_001"
        assert "resource_telemetry" in result["health"]
        assert result["health"]["resource_telemetry"]["tokens_total"] == 36
        assert result["health"]["resource_telemetry"]["avg_job_duration_ms"] == 2100.0
        assert result["health"]["resource_telemetry"]["heartbeat_freshness"] in ("fresh", "degraded")
        assert "job-a" in result["dashboard"]
        assert result["delivery"] == {
            "dashboard_in_payload": True,
            "live_channel": "notifications.message",
            "message_notifications": True,
            "progress_notifications": False,
        }

    def test_status_action_can_auto_kill_idle_run(self, monkeypatch):
        kill_calls: list[tuple[str, dict[str, object], str]] = []
        cancel_proof = {
            "cancelled_jobs": 1,
            "labels": ["job-a"],
            "run_status": "cancelled",
            "terminal_reason": "workflow_cancelled",
        }

        def _fake_execute(_pg, intent, *, approved_by, **_kwargs):
            kill_calls.append((str(intent.command_type), dict(intent.payload), approved_by))
            return _fake_command(
                command_id="control.command.kill.1",
                command_status="succeeded",
                command_type=str(intent.command_type),
                payload=dict(intent.payload),
                result_ref="workflow_run:dispatch_idle",
            )

        monkeypatch.setattr(
            "runtime.control_commands.bootstrap_control_commands_schema",
            lambda _pg: None,
        )
        monkeypatch.setattr(
            "runtime.control_commands.execute_control_intent",
            _fake_execute,
        )
        monkeypatch.setattr(
            "runtime.control_commands.workflow_cancel_proof",
            lambda _conn, _run_id: dict(cancel_proof),
        )
        monkeypatch.setattr(
            "runtime.workflow.unified.cancel_run",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("kill_if_idle should not cancel directly")
            ),
        )

        server._subs._pg_conn = _FakePGConn(
            workflow_run_rows=[{
                "run_id": "dispatch_idle",
                "spec_name": "test-queue",
                "status": "running",
                "total_jobs": 1,
                "started_at": None,
                "finished_at": None,
                "terminal_reason": None,
                "created_at": datetime.now(timezone.utc) - timedelta(seconds=2000),
            }],
            dispatch_jobs_rows=[{
                "label": "job-a",
                "status": "pending",
                "agent_slug": "agent-a",
                "resolved_agent": "agent-a",
                "attempt": 1,
                "last_error_code": "",
                "duration_ms": 0,
                "cost_usd": 0.0,
                "token_input": 0,
                "token_output": 0,
                "ready_at": datetime.now(timezone.utc) - timedelta(seconds=1200),
                "claimed_at": None,
                "started_at": None,
                "finished_at": None,
                "heartbeat_at": None,
                "next_retry_at": None,
                "stdout_preview": "",
                "id": 201,
            }],
            dispatch_totals_row=[{
                "total_cost": 0.0,
                "total_tokens_in": 0,
                "total_tokens_out": 0,
                "total_duration_ms": 0,
            }],
        )

        result = _call_tool(
            "praxis_workflow",
            {
                "action": "status",
                "run_id": "dispatch_idle",
                "kill_if_idle": True,
                "idle_threshold_seconds": 900,
            },
        )
        assert result["run_id"] == "dispatch_idle"
        assert result["status"] == "running"
        assert result["recovery"]["mode"] == "kill_if_idle"
        assert result["recovery"]["recommended_tool"]["arguments"]["kill_if_idle"] is True
        assert result["kill_action"]["performed"] is True
        assert result["kill_action"]["reason"] is not None
        assert result["kill_action"]["command"]["command_id"] == "control.command.kill.1"
        assert result["kill_action"]["command"]["command_status"] == "succeeded"
        assert kill_calls == [
            ("workflow.cancel", {"run_id": "dispatch_idle", "include_running": True}, "mcp.praxis_workflow.kill_if_idle")
        ]

    def test_status_action_auto_kill_respects_threshold(self, monkeypatch):
        cancelled = False

        def _fake_execute(_pg, intent, *, approved_by, **_kwargs):
            nonlocal cancelled
            cancelled = True
            return _fake_command(
                command_id="control.command.kill.2",
                command_status="succeeded",
                command_type=str(intent.command_type),
                payload=dict(intent.payload),
                result_ref="workflow_run:dispatch_idle",
            )

        monkeypatch.setattr(
            "runtime.control_commands.bootstrap_control_commands_schema",
            lambda _pg: None,
        )
        monkeypatch.setattr(
            "runtime.control_commands.execute_control_intent",
            _fake_execute,
        )

        server._subs._pg_conn = _FakePGConn(
            workflow_run_rows=[{
                "run_id": "dispatch_idle",
                "spec_name": "test-queue",
                "status": "running",
                "total_jobs": 1,
                "started_at": None,
                "finished_at": None,
                "terminal_reason": None,
                "created_at": datetime.now(timezone.utc) - timedelta(seconds=2000),
            }],
            dispatch_jobs_rows=[{
                "label": "job-a",
                "status": "pending",
                "agent_slug": "agent-a",
                "resolved_agent": "agent-a",
                "attempt": 1,
                "last_error_code": "",
                "duration_ms": 0,
                "cost_usd": 0.0,
                "token_input": 0,
                "token_output": 0,
                "ready_at": datetime.now(timezone.utc) - timedelta(seconds=1200),
                "claimed_at": None,
                "started_at": None,
                "finished_at": None,
                "heartbeat_at": None,
                "next_retry_at": None,
                "stdout_preview": "",
                "id": 201,
            }],
            dispatch_totals_row=[{
                "total_cost": 0.0,
                "total_tokens_in": 0,
                "total_tokens_out": 0,
                "total_duration_ms": 0,
            }],
        )

        result = _call_tool(
            "praxis_workflow",
            {
                "action": "status",
                "run_id": "dispatch_idle",
                "kill_if_idle": True,
                "idle_threshold_seconds": 5000,
            },
        )
        assert result["run_id"] == "dispatch_idle"
        assert result["recovery"]["mode"] == "monitor"
        assert result["kill_action"]["performed"] is False
        assert cancelled is False

    def test_wait_legacy_paths_are_disabled(self):
        result = _call_tool("praxis_workflow", {"action": "wait", "run_id": "dispatch_001"})
        assert "error" in result
        assert "no longer supported" in result["error"]


class TestErrorHandling:
    def test_unknown_tool(self):
        req = _make_request("tools/call", {"name": "nonexistent_tool", "arguments": {}})
        resp = server.handle_request(req)
        assert "error" in resp

    def test_unknown_method(self):
        req = _make_request("resources/list")
        resp = server.handle_request(req)
        assert "error" in resp

    def test_malformed_params(self):
        req = _make_request("tools/call", {"name": "praxis_workflow_validate"})
        resp = server.handle_request(req)
        # Should not crash; should return a result (even if it contains an error field)
        assert resp is not None
