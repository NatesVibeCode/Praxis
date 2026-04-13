"""Integration tests for the dispatch REST API surface.

Starts the server on a random available port in a background thread,
runs tests against it, then shuts down.  All subsystems are stubbed
so tests run without real databases.
"""

from __future__ import annotations

import base64
import json
import sys
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Ensure the Workflow root is on sys.path so direct-file imports resolve
# ---------------------------------------------------------------------------

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
_API_DIR = str(_WORKFLOW_ROOT / "surfaces" / "api")
if _API_DIR not in sys.path:
    sys.path.insert(0, _API_DIR)

import workflow_api  # noqa: E402
from handlers import workflow_admin as admin_handlers  # noqa: E402
from handlers import workflow_query as query_handlers  # noqa: E402
from handlers import workflow_run as workflow_run_handlers  # noqa: E402
from runtime.workflow.mcp_session import mint_workflow_mcp_session_token  # noqa: E402


# ---------------------------------------------------------------------------
# Stub types that mirror the real subsystem contracts
# ---------------------------------------------------------------------------


class _StubPreflightOverall(Enum):
    PASS = "pass"


@dataclass
class _StubCheck:
    name: str = "stub_probe"
    passed: bool = True
    message: str = "ok"
    duration_ms: float = 1.0
    status: str = "ok"
    details: dict[str, object] = field(default_factory=dict)


@dataclass
class _StubPreflight:
    overall: _StubPreflightOverall = _StubPreflightOverall.PASS
    checks: list = field(default_factory=lambda: [_StubCheck()])
    timestamp: datetime = field(
        default_factory=lambda: datetime(2026, 4, 4, tzinfo=timezone.utc)
    )


@dataclass
class _StubLane:
    recommended_posture: str = "operate"
    confidence: float = 0.95
    reasons: list = field(default_factory=lambda: ["all probes green"])
    degraded_cause: str | None = None


@dataclass
class _StubSnapshot:
    state: str = "nominal"


class _StubBugSeverity(Enum):
    P0 = "P0"
    P1 = "P1"
    P2 = "P2"
    P3 = "P3"


class _StubBugStatus(Enum):
    OPEN = "OPEN"
    IN_PROGRESS = "IN_PROGRESS"
    FIXED = "FIXED"
    WONT_FIX = "WONT_FIX"
    DEFERRED = "DEFERRED"


class _StubBugCategory(Enum):
    OTHER = "OTHER"
    RUNTIME = "RUNTIME"


@dataclass
class _StubBug:
    bug_id: str = "BUG-001"
    bug_key: str = "bug_001"
    title: str = "test bug"
    severity: _StubBugSeverity = _StubBugSeverity.P2
    status: _StubBugStatus = _StubBugStatus.OPEN
    priority: str = "P2"
    category: _StubBugCategory = _StubBugCategory.OTHER
    description: str = "desc"
    summary: str = "desc"
    filed_at: datetime = field(
        default_factory=lambda: datetime(2026, 4, 4, tzinfo=timezone.utc)
    )
    created_at: datetime = field(
        default_factory=lambda: datetime(2026, 4, 4, tzinfo=timezone.utc)
    )
    updated_at: datetime = field(
        default_factory=lambda: datetime(2026, 4, 4, tzinfo=timezone.utc)
    )
    resolved_at: datetime | None = None
    filed_by: str = "test"
    assigned_to: str | None = None
    tags: tuple[str, ...] = field(default_factory=tuple)
    source_kind: str = "manual"
    discovered_in_run_id: str | None = None
    discovered_in_receipt_id: str | None = None
    owner_ref: str | None = None
    decision_ref: str = ""
    resolution_summary: str | None = None


class _StubEntityType(Enum):
    TOPIC = "topic"


@dataclass
class _StubEntity:
    id: str = "ent-1"
    name: str = "test entity"
    entity_type: _StubEntityType = _StubEntityType.TOPIC
    content: str = "some content about testing"
    source: str = "test"


@dataclass
class _StubSearchResult:
    entity: _StubEntity = field(default_factory=_StubEntity)
    score: float = 0.85
    found_via: str = "text_search"
    provenance: str = "test"


@dataclass
class _StubIngestResult:
    accepted: bool = True
    entities_created: int = 1
    edges_created: int = 0
    duplicates_skipped: int = 0
    errors: list = field(default_factory=list)


class _StubWaveStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"


@dataclass
class _StubWaveJob:
    job_label: str = "job-1"
    status: str = "pending"
    depends_on: list = field(default_factory=list)


@dataclass
class _StubWave:
    wave_id: str = "wave-1"
    status: _StubWaveStatus = _StubWaveStatus.PENDING
    started_at: datetime | None = None
    completed_at: datetime | None = None
    jobs: list = field(default_factory=lambda: [_StubWaveJob()])
    gate_verdict: Any = None


@dataclass
class _StubDagState:
    orch_id: str = "orch-default"
    current_wave: str = "wave-1"
    created_at: datetime = field(
        default_factory=lambda: datetime(2026, 4, 4, tzinfo=timezone.utc)
    )
    waves: list = field(default_factory=lambda: [_StubWave()])


@dataclass
class _StubJobResult:
    job_label: str = "build-1"
    agent_slug: str = "agent-a"
    status: str = "succeeded"
    exit_code: int = 0
    duration_seconds: float = 12.5
    verify_passed: bool = True
    retry_count: int = 0


@dataclass
class _StubRunResult:
    spec_name: str = "test-spec"
    total_jobs: int = 1
    succeeded: int = 1
    failed: int = 0
    skipped: int = 0
    blocked: int = 0
    duration_seconds: float = 12.5
    receipts_written: list = field(default_factory=lambda: ["receipt-1.json"])
    job_results: list = field(default_factory=lambda: [_StubJobResult()])


class _StubWorkflowSpecError(Exception):
    pass


@dataclass
class _StubWorkflowSpec:
    name: str = "test-spec"
    jobs: list = field(
        default_factory=lambda: [{"label": "job-a", "agent": "agent-a", "prompt": "stub prompt"}]
    )

    def summary(self):
        return {"total_jobs": len(self.jobs), "name": self.name}

    @classmethod
    def load(cls, path: str):
        if "invalid" in path:
            raise _StubWorkflowSpecError(f"Invalid spec: {path}")
        return cls()


class _StubResolvedAgent:
    def __init__(self, slug: str) -> None:
        self.slug = slug


class _StubAgentRegistry:
    def __init__(self, known_agents: set[str]) -> None:
        self._known_agents = set(known_agents)

    def get(self, slug: str):
        if slug in self._known_agents:
            return _StubResolvedAgent(slug)
        return None

    @classmethod
    def with_known_agents(cls, *slugs: str):
        return cls(set(slugs))


class _StubPgConn:
    def __init__(self) -> None:
        self.uploaded_files: dict[str, dict[str, Any]] = {}
        self.workflow_job_runtime_context: dict[tuple[str, str], dict[str, Any]] = {}
        now = datetime(2026, 4, 4, tzinfo=timezone.utc)
        self.object_types: dict[str, dict[str, Any]] = {}
        self.objects: dict[str, dict[str, Any]] = {}
        self.workflows: dict[str, dict[str, Any]] = {
            "wf-123": {
                "id": "wf-123",
                "name": "Seed Workflow",
                "description": "Original workflow",
                "definition": {
                    "type": "operating_model",
                    "definition_revision": "def-seed",
                    "source_prose": "Seed workflow",
                    "compiled_prose": "Seed workflow",
                    "narrative_blocks": [],
                    "references": [],
                    "capabilities": [],
                    "authority": "",
                    "sla": {},
                    "trigger_intent": [],
                    "draft_flow": [],
                },
                "compiled_spec": {
                    "name": "Seed Workflow",
                    "workflow_id": "seed-workflow",
                    "definition_revision": "def-seed",
                    "plan_revision": "plan-seed",
                    "jobs": [],
                    "triggers": [],
                },
                "tags": ["seed"],
                "version": 1,
                "is_template": False,
                "created_at": now,
                "updated_at": now,
                "invocation_count": 0,
                "last_invoked_at": None,
            }
        }
        self.workflow_triggers: dict[str, dict[str, Any]] = {
            "trg-123": {
                "id": "trg-123",
                "workflow_id": "wf-123",
                "event_type": "manual",
                "filter": {},
                "enabled": True,
                "cron_expression": None,
                "created_at": now,
                "last_fired_at": None,
                "fire_count": 0,
            }
        }

    def _parse_json(self, value: Any) -> Any:
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        return value

    def _workflow_row(self, workflow_id: str) -> dict[str, Any] | None:
        row = self.workflows.get(workflow_id)
        return dict(row) if row is not None else None

    def _trigger_row(self, trigger_id: str) -> dict[str, Any] | None:
        row = self.workflow_triggers.get(trigger_id)
        return dict(row) if row is not None else None

    def execute(self, query: str, *params: Any):
        normalized = " ".join(query.split())

        if normalized.startswith("INSERT INTO workflow_job_runtime_context"):
            now = datetime(2026, 4, 4, tzinfo=timezone.utc)
            run_id = str(params[0])
            job_label = str(params[1])
            record = {
                "run_id": run_id,
                "job_label": job_label,
                "workflow_id": params[2],
                "execution_context_shard": self._parse_json(params[3]) or {},
                "execution_bundle": self._parse_json(params[4]) or {},
                "created_at": now,
                "updated_at": now,
            }
            self.workflow_job_runtime_context[(run_id, job_label)] = record
            return []

        if normalized == (
            "SELECT run_id, job_label, workflow_id, execution_context_shard, execution_bundle, "
            "created_at, updated_at FROM workflow_job_runtime_context WHERE run_id = $1 AND job_label = $2"
        ):
            record = self.workflow_job_runtime_context.get((str(params[0]), str(params[1])))
            return [dict(record)] if record is not None else []

        if normalized.startswith("INSERT INTO uploaded_files"):
            now = datetime(2026, 4, 4, tzinfo=timezone.utc)
            self.uploaded_files[params[0]] = {
                "id": params[0],
                "filename": params[1],
                "content_type": params[2],
                "size_bytes": params[3],
                "storage_path": params[4],
                "scope": params[5],
                "workflow_id": params[6],
                "step_id": params[7],
                "description": params[8],
                "created_at": now,
            }
            return []

        if normalized == "SELECT storage_path FROM uploaded_files WHERE id = $1":
            record = self.uploaded_files.get(params[0])
            return [{"storage_path": record["storage_path"]}] if record else []

        if normalized == "DELETE FROM uploaded_files WHERE id = $1":
            self.uploaded_files.pop(params[0], None)
            return []

        if normalized == "UPDATE objects SET status = 'deleted', updated_at = now() WHERE object_id = $1":
            object_id = str(params[0])
            row = self.objects.get(object_id)
            if row is not None:
                updated = dict(row)
                updated["status"] = "deleted"
                updated["updated_at"] = datetime(2026, 4, 4, tzinfo=timezone.utc)
                self.objects[object_id] = updated
            return []

        if normalized in {
            "DELETE FROM public.workflow_triggers WHERE workflow_id = $1",
            "DELETE FROM workflow_triggers WHERE workflow_id = $1",
        }:
            workflow_id = str(params[0])
            self.workflow_triggers = {
                trigger_id: row
                for trigger_id, row in self.workflow_triggers.items()
                if str(row.get("workflow_id")) != workflow_id
            }
            return []

        if normalized in {
            "DELETE FROM public.workflows WHERE id = $1",
            "DELETE FROM workflows WHERE id = $1",
        }:
            self.workflows.pop(str(params[0]), None)
            return []

        if normalized.startswith("INSERT INTO workflows "):
            workflow_id = str(params[0])
            now = datetime(2026, 4, 4, tzinfo=timezone.utc)
            row = dict(self.workflows.get(workflow_id, {}))
            row.update(
                {
                    "id": workflow_id,
                    "name": params[1],
                    "description": params[2],
                    "definition": self._parse_json(params[3]),
                    "compiled_spec": self._parse_json(params[4]) if len(params) > 4 else None,
                    "version": int(row.get("version") or 0) + 1,
                    "updated_at": now,
                    "created_at": row.get("created_at", now),
                }
            )
            self.workflows[workflow_id] = row
            return []

        if normalized == "UPDATE public.workflows SET invocation_count = invocation_count + 1, last_invoked_at = now() WHERE id = $1":
            workflow_id = str(params[0])
            row = self.workflows.get(workflow_id)
            if row is not None:
                updated = dict(row)
                updated["invocation_count"] = int(updated.get("invocation_count") or 0) + 1
                updated["last_invoked_at"] = datetime(2026, 4, 4, tzinfo=timezone.utc)
                self.workflows[workflow_id] = updated
            return []

        if normalized.startswith("INSERT INTO workflow_triggers "):
            trigger_id = str(params[0])
            now = datetime(2026, 4, 4, tzinfo=timezone.utc)
            row = {
                "id": trigger_id,
                "workflow_id": str(params[1]),
                "event_type": params[2],
                "filter": self._parse_json(params[3]),
                "cron_expression": params[4],
                "enabled": bool(params[5]) if len(params) > 5 else True,
                "created_at": now,
                "last_fired_at": None,
                "fire_count": 0,
            }
            self.workflow_triggers[trigger_id] = row
            return []

        if normalized == "SELECT storage_path, content_type, filename FROM uploaded_files WHERE id = $1":
            record = self.uploaded_files.get(params[0])
            if not record:
                return []
            return [
                {
                    "storage_path": record["storage_path"],
                    "content_type": record["content_type"],
                    "filename": record["filename"],
                }
            ]

        if "FROM uploaded_files" in normalized and "ORDER BY created_at DESC LIMIT 100" in normalized:
            expected: dict[str, Any] = {}
            index = 0
            if "scope = $" in normalized:
                expected["scope"] = params[index]
                index += 1
            if "workflow_id = $" in normalized:
                expected["workflow_id"] = params[index]
                index += 1
            if "step_id = $" in normalized:
                expected["step_id"] = params[index]
                index += 1

            rows = []
            for record in self.uploaded_files.values():
                if any(record.get(key) != value for key, value in expected.items()):
                    continue
                rows.append(dict(record))
            rows.sort(key=lambda item: item["created_at"], reverse=True)
            return rows[:100]

        raise NotImplementedError(query)

    def fetchval(self, query: str, *params: Any):
        normalized = " ".join(query.split())
        if normalized == "SELECT 1 FROM object_types WHERE type_id = $1":
            return 1 if str(params[0]) in self.object_types else None
        if normalized in {
            "SELECT 1 FROM workflows WHERE id = $1",
            "SELECT 1 FROM public.workflows WHERE id = $1",
        }:
            return 1 if str(params[0]) in self.workflows else None
        if normalized == "SELECT 1 FROM workflow_triggers WHERE id = $1":
            return 1 if str(params[0]) in self.workflow_triggers else None
        if normalized in {
            "SELECT name FROM workflows WHERE id = $1",
            "SELECT name FROM public.workflows WHERE id = $1",
        }:
            workflow = self.workflows.get(str(params[0]))
            return workflow.get("name") if workflow else None
        return None

    def fetchrow(self, query: str, *params: Any):
        normalized = " ".join(query.split())
        now = datetime(2026, 4, 4, tzinfo=timezone.utc)

        if normalized.startswith("INSERT INTO public.workflows"):
            workflow_id = str(params[0])
            row = dict(self.workflows.get(workflow_id, {}))
            row.update(
                {
                    "id": workflow_id,
                    "name": params[1],
                    "description": params[2],
                    "definition": self._parse_json(params[3]),
                    "compiled_spec": self._parse_json(params[4]),
                    "tags": list(params[5]),
                    "version": int(row.get("version") or 0) + 1,
                    "is_template": params[6],
                    "created_at": row.get("created_at", now),
                    "updated_at": now,
                }
            )
            self.workflows[workflow_id] = row
            return dict(row)

        if normalized.startswith("INSERT INTO object_types"):
            row = {
                "type_id": params[0],
                "name": params[1],
                "description": params[2],
                "icon": params[3],
                "property_definitions": self._parse_json(params[4]),
                "created_at": now,
            }
            self.object_types[str(params[0])] = row
            return dict(row)

        if normalized.startswith("INSERT INTO objects"):
            row = {
                "object_id": params[0],
                "type_id": params[1],
                "properties": self._parse_json(params[2]),
                "status": "active",
                "created_at": now,
                "updated_at": now,
            }
            self.objects[str(params[0])] = row
            return dict(row)

        if normalized.startswith(
            "UPDATE objects SET properties = properties || $2::jsonb, updated_at = now() WHERE object_id = $1 RETURNING *"
        ):
            object_id = str(params[0])
            row = self.objects.get(object_id)
            if row is None:
                return None
            updated = dict(row)
            properties = dict(updated.get("properties") or {})
            properties.update(self._parse_json(params[1]) or {})
            updated["properties"] = properties
            updated["updated_at"] = now
            self.objects[object_id] = updated
            return dict(updated)

        if normalized.startswith("UPDATE public.workflows SET") and normalized.endswith("RETURNING *"):
            workflow_id = str(params[0])
            row = self.workflows.get(workflow_id)
            if row is None:
                return None
            updated = dict(row)
            param_index = 1
            if "name =" in normalized:
                updated["name"] = params[param_index]
                param_index += 1
            if "description =" in normalized:
                updated["description"] = params[param_index]
                param_index += 1
            if "definition =" in normalized:
                updated["definition"] = self._parse_json(params[param_index])
                param_index += 1
            if "compiled_spec =" in normalized:
                updated["compiled_spec"] = self._parse_json(params[param_index])
                param_index += 1
            if "tags =" in normalized:
                updated["tags"] = params[param_index]
                param_index += 1
            if "is_template =" in normalized:
                updated["is_template"] = params[param_index]
                param_index += 1
            updated["version"] = int(updated.get("version") or 0) + 1
            updated["updated_at"] = now
            self.workflows[workflow_id] = updated
            return dict(updated)

        if normalized in {
            "SELECT id, name FROM public.workflows WHERE id = $1",
            "SELECT id, name FROM workflows WHERE id = $1",
            "SELECT name, definition, compiled_spec FROM public.workflows WHERE id = $1",
            "SELECT name, definition, compiled_spec FROM workflows WHERE id = $1",
            "SELECT * FROM public.workflows WHERE id = $1",
        }:
            return self._workflow_row(str(params[0]))

        if normalized.startswith("INSERT INTO workflow_triggers") and normalized.endswith("RETURNING *"):
            trigger_id = str(params[0])
            row = {
                "id": trigger_id,
                "workflow_id": str(params[1]),
                "event_type": params[2],
                "filter": self._parse_json(params[3]),
                "enabled": bool(params[4]),
                "cron_expression": params[5],
                "created_at": now,
                "last_fired_at": None,
                "fire_count": 0,
            }
            self.workflow_triggers[trigger_id] = row
            return dict(row)

        if normalized.startswith("UPDATE workflow_triggers SET") and normalized.endswith("RETURNING *"):
            trigger_id = str(params[0])
            row = self.workflow_triggers.get(trigger_id)
            if row is None:
                return None
            updated = dict(row)
            param_index = 1
            if "workflow_id =" in normalized:
                updated["workflow_id"] = params[param_index]
                param_index += 1
            if "event_type =" in normalized:
                updated["event_type"] = params[param_index]
                param_index += 1
            if "filter =" in normalized:
                updated["filter"] = self._parse_json(params[param_index])
                param_index += 1
            if "cron_expression =" in normalized:
                updated["cron_expression"] = params[param_index]
                param_index += 1
            if "enabled =" in normalized:
                updated["enabled"] = bool(params[param_index])
                param_index += 1
            self.workflow_triggers[trigger_id] = updated
            return dict(updated)

        if normalized in {
            "SELECT id, workflow_id, event_type, filter, enabled, cron_expression, created_at, last_fired_at, fire_count FROM workflow_triggers WHERE id = $1",
            "SELECT id, workflow_id, event_type, filter, enabled, cron_expression, created_at, last_fired_at, fire_count FROM public.workflow_triggers WHERE id = $1",
        }:
            return self._trigger_row(str(params[0]))

        return None


# ---------------------------------------------------------------------------
# Build a fully-stubbed _Subsystems
# ---------------------------------------------------------------------------


def _make_stubbed_subsystems() -> workflow_api._Subsystems:
    subs = workflow_api._Subsystems()
    pg = _StubPgConn()
    subs._pg_conn = pg

    # Operator panel
    panel = MagicMock()
    panel.snapshot.return_value = _StubSnapshot()
    panel.recommend_lane.return_value = _StubLane()
    subs._operator_panel = panel

    # Bug tracker
    bt = MagicMock()
    bug_open = _StubBug(
        bug_id="BUG-001",
        bug_key="bug_001",
        title="test bug",
        summary="desc",
        priority="P2",
        category=_StubBugCategory.OTHER,
    )
    bug_runtime = _StubBug(
        bug_id="BUG-RUNTIME",
        bug_key="bug_runtime",
        title="runtime bug",
        summary="runtime desc",
        priority="P1",
        category=_StubBugCategory.RUNTIME,
        status=_StubBugStatus.FIXED,
        description="runtime desc",
    )
    seeded_bugs: list[_StubBug] = [bug_open, bug_runtime]

    def _filter_bugs(
        *,
        status=None,
        severity=None,
        category=None,
        title_like=None,
        open_only=False,
        tags=None,
        exclude_tags=None,
    ):
        del tags, exclude_tags
        bugs = list(seeded_bugs)
        if status is not None:
            bugs = [bug for bug in bugs if bug.status == status]
        elif open_only:
            bugs = [
                bug for bug in bugs
                if bug.status not in {
                    _StubBugStatus.FIXED,
                    _StubBugStatus.WONT_FIX,
                    _StubBugStatus.DEFERRED,
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
        bugs.sort(key=lambda bug: bug.filed_at, reverse=True)
        if category is None:
            return [bug_open]
        return bugs

    bt.list_bugs.side_effect = lambda limit=50, **kwargs: _filter_bugs(**kwargs)[:limit]
    bt.count_bugs.side_effect = lambda **kwargs: len(_filter_bugs(**kwargs))
    bt.search.side_effect = lambda query, limit=20: [
        bug
        for bug in seeded_bugs
        if str(query).lower() in bug.title.lower() or str(query).lower() in bug.description.lower()
    ][:limit]
    bt.stats.return_value = {
        "total": len(seeded_bugs),
        "by_status": {"OPEN": len([bug for bug in seeded_bugs if bug.status == _StubBugStatus.OPEN])},
        "by_severity": {"P2": 1, "P1": 1},
        "by_category": {"OTHER": 1, "RUNTIME": 1},
        "open_count": len([bug for bug in seeded_bugs if bug.status in {_StubBugStatus.OPEN, _StubBugStatus.IN_PROGRESS}]),
        "mttr_hours": None,
        "packet_ready_count": 1,
        "fix_verified_count": 0,
        "underlinked_count": 0,
    }

    def _file_bug(
        *,
        title: str,
        severity: _StubBugSeverity,
        category: _StubBugCategory,
        description: str,
        filed_by: str,
        source_kind: str = "workflow_api",
        decision_ref: str = "",
        discovered_in_run_id: str | None = None,
        discovered_in_receipt_id: str | None = None,
        owner_ref: str | None = None,
        tags=(),
        ):
        if discovered_in_run_id not in {None, "run-123"}:
            raise ValueError(f"unknown discovered_in_run_id: {discovered_in_run_id}")
        if discovered_in_receipt_id not in {None, "receipt-123"}:
            raise ValueError(
                f"unknown discovered_in_receipt_id: {discovered_in_receipt_id}"
            )
        return _StubBug(
            bug_id="BUG-FILED",
            bug_key="bug_filed",
            title=title,
            severity=severity,
            category=category,
            description=description,
            summary=description,
            filed_by=filed_by,
            source_kind=source_kind,
            discovered_in_run_id=discovered_in_run_id,
            discovered_in_receipt_id=discovered_in_receipt_id,
            owner_ref=owner_ref,
            decision_ref=decision_ref,
            tags=tuple(tags),
        )

    def _failure_packet(bug_id: str, *, receipt_limit: int = 5):
        return {
            "bug": _StubBug(bug_id=bug_id, bug_key=bug_id.lower().replace("-", "_")),
            "signature": {
                "fingerprint": "fp-test",
                "failure_code": "timeout_exceeded",
                "node_id": "job-a",
                "source_kind": "manual",
            },
            "lifecycle": {
                "recurrence_count": 3,
                "fix_validation_count": 1,
            },
            "replay_context": {
                "ready": True,
                "run_id": "run-123",
                "receipt_id": "receipt-123",
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

    def _replay_bug(bug_id: str, *, receipt_limit: int = 5):
        packet = _failure_packet(bug_id, receipt_limit=receipt_limit)
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

    def _replay_hint(bug_id: str, *, receipt_limit: int = 1):
        if bug_id == "BUG-001":
            return {
                "available": True,
                "reason_code": "bug.replay_ready",
                "run_id": "run-123",
                "receipt_id": "receipt-123",
                "automatic": True,
            }
        return {
            "available": False,
            "reason_code": "bug.replay_missing_run_context",
            "run_id": None,
            "receipt_id": None,
            "automatic": False,
        }

    def _bulk_backfill_replay_provenance(
        *,
        limit: int | None = None,
        open_only: bool = True,
        receipt_limit: int = 1,
    ):
        return {
            "scanned_count": 1 if open_only else 2,
            "backfilled_count": 1,
            "linked_count": 2,
            "replay_ready_count": 1,
            "replay_blocked_count": 0,
            "open_only": open_only,
            "limit": limit,
            "bugs": [
                {
                    "bug_id": "BUG-001",
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
            ],
        }

    def _link_evidence(
        bug_id: str,
        *,
        evidence_kind: str,
        evidence_ref: str,
        evidence_role: str,
        created_by: str = "workflow_api",
        notes: str | None = None,
    ):
        allowed_refs = {
            "receipt": {"receipt-123"},
            "run": {"run-123"},
            "verification_run": {"verification-run-123"},
            "healing_run": {"healing-run-123"},
        }
        if evidence_kind not in allowed_refs:
            raise ValueError(
                "evidence_kind must be one of receipt, run, verification_run, healing_run"
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

    def _resolve(bug_id: str, status: _StubBugStatus):
        return _StubBug(
            bug_id=bug_id,
            title="resolved stub bug",
            status=status,
        )

    bt.file_bug.side_effect = _file_bug
    bt.failure_packet.side_effect = _failure_packet
    bt.replay_bug.side_effect = _replay_bug
    bt.replay_hint.side_effect = _replay_hint
    bt.bulk_backfill_replay_provenance.side_effect = _bulk_backfill_replay_provenance
    bt.link_evidence.side_effect = _link_evidence
    bt.resolve.side_effect = _resolve
    subs._bug_tracker = bt

    # Receipt ingester
    ingester = MagicMock()
    ingester.load_recent.return_value = [
        {"agent_slug": "agent-a", "status": "succeeded"},
        {"agent_slug": "agent-a", "status": "failed"},
    ]
    ingester.compute_pass_rate.return_value = 0.5
    ingester.top_failure_codes.return_value = [{"code": "TIMEOUT", "count": 1}]
    subs._receipt_ingester = ingester

    # Knowledge graph
    kg = MagicMock()
    kg.search.return_value = [_StubSearchResult()]
    kg.ingest.return_value = _StubIngestResult()
    kg.blast_radius.return_value = {"center": "ent-1", "nodes": 3}
    subs._knowledge_graph = kg

    # Wave orchestrator
    orch = MagicMock()
    orch.observe.return_value = _StubDagState()
    subs._wave_orchestrator = orch

    subs.get_pg_conn = lambda: pg

    # Mark as initialized so _ensure_init skips directory creation
    subs._initialized = True

    return subs


# ---------------------------------------------------------------------------
# Patched import helpers: prevent loading real runtime modules
# ---------------------------------------------------------------------------


def _make_stub_health_module():
    """Return a module-like object for health."""
    mod = MagicMock()

    class _DBProbe:
        def __init__(self, path):
            self.path = path

    class _DiskProbe:
        def __init__(self, path):
            self.path = path

    class _FileProbe:
        def __init__(self, *args):
            self.args = args

    class _PreflightRunner:
        def __init__(self, probes):
            self.probes = probes

        def run(self):
            return _StubPreflight()

    mod.DatabaseProbe = _DBProbe
    mod.PostgresProbe = _DBProbe
    mod.PostgresConnectivityProbe = _DBProbe
    mod.DiskSpaceProbe = _DiskProbe
    mod.FileExistsProbe = _FileProbe
    mod.ProviderTransportProbe = _FileProbe
    mod.PreflightRunner = _PreflightRunner
    return mod


def _make_stub_bug_tracker_mod():
    mod = MagicMock()
    mod.BugStatus = _StubBugStatus
    mod.BugSeverity = _StubBugSeverity
    mod.BugCategory = _StubBugCategory
    mod.BugTracker._normalize_status.side_effect = (
        lambda raw, default=None: _StubBugStatus(str(raw).strip().upper())
        if raw is not None and str(raw).strip().upper() in _StubBugStatus.__members__
        else default
    )
    mod.BugTracker._normalize_severity.side_effect = (
        lambda raw, default=None: _StubBugSeverity(str(raw).strip().upper())
        if raw is not None and str(raw).strip().upper() in _StubBugSeverity.__members__
        else default
    )
    mod.BugTracker._normalize_category.side_effect = (
        lambda raw, default=None: _StubBugCategory(str(raw).strip().upper())
        if raw is not None and str(raw).strip().upper() in _StubBugCategory.__members__
        else default
    )
    return mod


def _make_stub_workflow_spec_mod():
    mod = MagicMock()
    mod.WorkflowSpec = _StubWorkflowSpec
    mod.WorkflowSpecError = _StubWorkflowSpecError
    return mod


def _make_stub_quality_views_mod():
    mod = MagicMock()

    class _QW(Enum):
        DAILY = "daily"

    mod.QualityWindow = _QW
    return mod


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def api_server():
    """Start a WorkflowAPIServer on a random port, yield base_url, then shutdown."""
    import os

    previous_database_url = os.environ.get("WORKFLOW_DATABASE_URL")
    os.environ["WORKFLOW_DATABASE_URL"] = "postgresql://test@localhost:5432/praxis_test"
    subs = _make_stubbed_subsystems()
    original_workflow_spec_mod = workflow_run_handlers._workflow_spec_mod

    # Patch the module-returning methods on the subs instance
    subs.get_health_mod = _make_stub_health_module
    subs.get_bug_tracker_mod = _make_stub_bug_tracker_mod
    subs.get_quality_views_mod = _make_stub_quality_views_mod
    workflow_run_handlers._workflow_spec_mod = _make_stub_workflow_spec_mod

    # Point DB paths at non-existent files so probes use stub paths
    subs.bugs_db = "/tmp/_dag_test_nonexistent_bugs.db"
    subs.dispatch_db = "/tmp/_dag_test_nonexistent_dispatch.db"
    subs.knowledge_db = "/tmp/_dag_test_nonexistent_knowledge.db"
    subs.agents_json = "/tmp/_dag_test_nonexistent_agents.json"

    server = workflow_api.WorkflowAPIServer(
        host="127.0.0.1", port=0, subsystems=subs
    )
    server.serve_background()
    addr = server.server_address
    base_url = f"http://{addr[0]}:{addr[1]}"
    yield base_url
    server.shutdown()
    workflow_run_handlers._workflow_spec_mod = original_workflow_spec_mod
    if previous_database_url is None:
        os.environ.pop("WORKFLOW_DATABASE_URL", None)
    else:
        os.environ["WORKFLOW_DATABASE_URL"] = previous_database_url


def _post(
    base_url: str,
    path: str,
    body: dict | None = None,
    *,
    include_ui_header: bool = True,
) -> tuple[int, dict]:
    """POST JSON to the server, return (status_code, parsed_json)."""
    data = json.dumps(body or {}).encode("utf-8")
    request_headers = {"Content-Type": "application/json"}
    if include_ui_header:
        request_headers["X-Praxis-UI"] = "1"
    req = urllib.request.Request(
        f"{base_url}{path}",
        data=data,
        headers=request_headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def _json_request(
    base_url: str,
    path: str,
    *,
    method: str,
    body: dict | None = None,
    include_ui_header: bool = True,
) -> tuple[int, dict]:
    status, raw, _ = _request(
        base_url,
        path,
        method=method,
        body=json.dumps(body or {}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        include_ui_header=include_ui_header,
    )
    return status, json.loads(raw)


def _request(
    base_url: str,
    path: str,
    *,
    method: str = "GET",
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    include_ui_header: bool = True,
) -> tuple[int, bytes, Any]:
    request_headers = dict(headers or {})
    if include_ui_header:
        request_headers.setdefault("X-Praxis-UI", "1")
    req = urllib.request.Request(
        f"{base_url}{path}",
        data=body,
        headers=request_headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.read(), resp.headers
    except urllib.error.HTTPError as e:
        return e.code, e.read(), e.headers


@pytest.fixture
def file_api_server(tmp_path):
    import os

    previous_database_url = os.environ.get("WORKFLOW_DATABASE_URL")
    os.environ["WORKFLOW_DATABASE_URL"] = "postgresql://test@localhost:5432/praxis_test"
    subs = _make_stubbed_subsystems()
    original_workflow_spec_mod = workflow_run_handlers._workflow_spec_mod

    subs.get_health_mod = _make_stub_health_module
    subs.get_bug_tracker_mod = _make_stub_bug_tracker_mod
    subs.get_quality_views_mod = _make_stub_quality_views_mod
    workflow_run_handlers._workflow_spec_mod = _make_stub_workflow_spec_mod
    subs.bugs_db = "/tmp/_dag_test_nonexistent_bugs.db"
    subs.dispatch_db = "/tmp/_dag_test_nonexistent_dispatch.db"
    subs.knowledge_db = "/tmp/_dag_test_nonexistent_knowledge.db"
    subs.agents_json = "/tmp/_dag_test_nonexistent_agents.json"

    with patch.object(query_handlers, "REPO_ROOT", tmp_path):
        server = workflow_api.WorkflowAPIServer(
            host="127.0.0.1",
            port=0,
            subsystems=subs,
        )
        server.serve_background()
        addr = server.server_address
        base_url = f"http://{addr[0]}:{addr[1]}"
        yield base_url, subs._pg_conn, tmp_path
        server.shutdown()
        workflow_run_handlers._workflow_spec_mod = original_workflow_spec_mod
        if previous_database_url is None:
            os.environ.pop("WORKFLOW_DATABASE_URL", None)
        else:
            os.environ["WORKFLOW_DATABASE_URL"] = previous_database_url


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestOrient:
    def test_returns_capabilities_and_status(self, api_server):
        status, data = _post(api_server, "/orient")
        assert status == 200
        assert data["platform"] == "dag-workflow"
        assert data["version"] == "1.0.0"
        assert data["instruction_authority"]["kind"] == "orient_instruction_authority"
        assert data["instruction_authority"]["packet_read_order"][:3] == [
            "roadmap_truth",
            "queue_refs",
            "current_state_notes",
        ]
        assert "workflow_runs" in data["capabilities"]
        assert "/orient" in data["endpoints"]
        assert "instructions" in data
        assert data["lane_recommendation"]["recommended_posture"] == "operate"

    def test_orient_has_health(self, api_server):
        status, data = _post(api_server, "/orient")
        assert status == 200
        assert data["health"]["overall"] == "pass"


class TestMcpBridge:
    def test_initialize_roundtrip(self, api_server):
        status, data = _json_request(
            api_server,
            "/mcp",
            method="POST",
            body={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": "2024-11-05"},
            },
        )

        assert status == 200
        assert data["result"]["serverInfo"]["name"] == "praxis-mcp"

    def test_tools_list_requires_signed_workflow_token(self, api_server):
        status, data = _json_request(
            api_server,
            "/mcp?allowed_tools=praxis_query,praxis_status",
            method="POST",
            body={"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
        )

        assert status == 401
        assert data["reason_code"] == "workflow_mcp.token_missing"

    def test_tools_list_honors_allowed_tools_query_filter(self, api_server, monkeypatch):
        monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_SIGNING_SECRET", "test-secret")
        workflow_token = mint_workflow_mcp_session_token(
            run_id="run.alpha",
            workflow_id="workflow.alpha",
            job_label="job-alpha",
            allowed_tools=["praxis_query", "praxis_status", "praxis_context_shard"],
        )
        status, data = _json_request(
            api_server,
            f"/mcp?allowed_tools=praxis_query,praxis_status&workflow_token={workflow_token}",
            method="POST",
            body={"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
        )

        assert status == 200
        names = {tool["name"] for tool in data["result"]["tools"]}
        assert names == {"praxis_query", "praxis_status"}

    def test_dag_context_shard_reads_persisted_runtime_context(self, api_server, monkeypatch):
        from surfaces.mcp.tools import runtime_context as mcp_runtime_context

        monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_SIGNING_SECRET", "test-secret")
        monkeypatch.setattr(mcp_runtime_context._subs, "get_pg_conn", lambda: object())
        monkeypatch.setattr(
            mcp_runtime_context,
            "load_workflow_job_runtime_context",
            lambda _conn, *, run_id, job_label: {
                "run_id": run_id,
                "job_label": job_label,
                "workflow_id": "workflow.alpha",
                "execution_context_shard": {
                    "write_scope": ["runtime/example.py"],
                    "resolved_read_scope": ["runtime/support.py"],
                    "blast_radius": ["runtime/downstream.py"],
                    "test_scope": ["tests/test_example.py"],
                    "verify_refs": ["verify.spec.global"],
                    "context_sections": [{"name": "FILE: runtime/support.py", "content": "def helper():\n    return 1\n"}],
                },
                "execution_bundle": {
                    "run_id": "run.alpha",
                    "workflow_id": "workflow.alpha",
                    "job_label": "job-alpha",
                    "tool_bucket": "build",
                    "mcp_tool_names": ["praxis_context_shard", "praxis_query"],
                },
                "created_at": datetime(2026, 4, 4, tzinfo=timezone.utc),
                "updated_at": datetime(2026, 4, 4, tzinfo=timezone.utc),
            },
        )
        workflow_token = mint_workflow_mcp_session_token(
            run_id="run.alpha",
            workflow_id="workflow.alpha",
            job_label="job-alpha",
            allowed_tools=["praxis_context_shard"],
        )

        status, data = _json_request(
            api_server,
            f"/mcp?workflow_token={workflow_token}",
            method="POST",
            body={
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "praxis_context_shard",
                    "arguments": {"view": "summary"},
                },
            },
        )

        assert status == 200
        payload = json.loads(data["result"]["content"][0]["text"])
        assert payload["run_id"] == "run.alpha"
        assert payload["job_label"] == "job-alpha"
        assert payload["write_scope"] == ["runtime/example.py"]
        assert payload["execution_bundle"]["tool_bucket"] == "build"


class TestDispatch:
    def test_dry_run(self, api_server):
        status, data = _post(
            api_server,
            "/workflow-runs",
            {"spec_path": "/tmp/test.queue.json", "dry_run": True},
        )
        assert status == 200
        assert data["spec_name"] == "test-spec"
        assert data["total_jobs"] == 1
        assert len(data["job_results"]) == 1

    def test_missing_spec_path(self, api_server):
        status, data = _post(api_server, "/workflow-runs", {})
        assert status == 400
        assert "spec_path" in data["error"]


class TestValidate:
    def test_valid_spec(self, api_server, monkeypatch):
        agent_config_mod = __import__("registry.agent_config", fromlist=["AgentRegistry"])
        monkeypatch.setattr(
            agent_config_mod.AgentRegistry,
            "load_from_postgres",
            lambda _conn: _StubAgentRegistry.with_known_agents("agent-a"),
        )
        status, data = _post(
            api_server, "/workflow-validate", {"spec_path": "/tmp/test.queue.json"}
        )
        assert status == 200
        assert data["valid"] is True
        assert "summary" in data
        assert data["agent_resolution_details"][0]["status"] == "resolved"

    def test_invalid_spec(self, api_server):
        status, data = _post(
            api_server,
            "/workflow-validate",
            {"spec_path": "/tmp/invalid.queue.json"},
        )
        assert status == 200
        assert data["valid"] is False
        assert "error" in data

    def test_unresolved_agent_is_invalid(self, api_server, monkeypatch):
        agent_config_mod = __import__("registry.agent_config", fromlist=["AgentRegistry"])
        monkeypatch.setattr(
            agent_config_mod.AgentRegistry,
            "load_from_postgres",
            lambda _conn: _StubAgentRegistry.with_known_agents("other-agent"),
        )
        status, data = _post(
            api_server, "/workflow-validate", {"spec_path": "/tmp/test.queue.json"}
        )
        assert status == 200
        assert data["valid"] is False
        assert data["agent_resolution_details"][0]["status"] == "unresolved"

    def test_missing_spec_path(self, api_server):
        status, data = _post(api_server, "/workflow-validate", {})
        assert status == 400


class TestStatus:
    def test_default_lookback(self, api_server):
        status, data = _post(api_server, "/status")
        assert status == 200
        assert "total_workflows" in data
        assert "pass_rate" in data
        assert data["since_hours"] == 24


class TestQuery:
    def test_routes_status_query(self, api_server):
        status, data = _post(
            api_server, "/query", {"question": "show me the status"}
        )
        assert status == 200
        assert data["routed_to"] == "operator_panel"

    def test_routes_bug_query(self, api_server):
        status, data = _post(
            api_server, "/query", {"question": "any open bugs?"}
        )
        assert status == 200
        assert data["routed_to"] == "bug_tracker"

    def test_missing_question(self, api_server):
        status, data = _post(api_server, "/query", {})
        assert status == 400

    def test_fallback_to_knowledge_graph(self, api_server):
        status, data = _post(
            api_server,
            "/query",
            {"question": "explain the frombulator design"},
        )
        assert status == 200
        assert data["routed_to"] == "knowledge_graph"


class TestBugs:
    def test_list_bugs(self, api_server):
        status, data = _post(api_server, "/bugs", {"action": "list"})
        assert status == 200
        assert data["count"] >= 1
        assert data["returned_count"] == 1
        assert data["bugs"][0]["bug_id"] == "BUG-001"
        assert data["bugs"][0]["replay_ready"] is True
        assert data["bugs"][0]["replay_reason_code"] == "bug.replay_ready"
        assert data["bugs"][0]["replay_run_id"] == "run-123"

    def test_list_bugs_filters_by_category(self, api_server):
        status, data = _post(api_server, "/bugs", {"action": "list", "category": "RUNTIME"})
        assert status == 200
        assert data["count"] == 1
        assert data["returned_count"] == 1
        assert data["bugs"][0]["category"] == "RUNTIME"

    def test_list_bugs_can_filter_replay_ready(self, api_server):
        status, data = _post(api_server, "/bugs", {"action": "list", "replay_ready_only": True})
        assert status == 200
        assert data["count"] == 1
        assert all(bug["replay_ready"] is True for bug in data["bugs"])

    def test_file_bug(self, api_server):
        status, data = _post(
            api_server,
            "/bugs",
            {
                "action": "file",
                "title": "new test bug",
                "severity": "P2",
                "category": "RUNTIME",
                "tags": ["api", "metadata"],
                "source_kind": "manual",
                "filed_by": "workflow_api",
                "decision_ref": "decision:bugs:test",
                "discovered_in_run_id": "run-123",
                "discovered_in_receipt_id": "receipt-123",
                "owner_ref": "owner-123",
            },
        )
        assert status == 200
        assert data["filed"] is True
        assert data["bug"]["category"] == "RUNTIME"
        assert data["bug"]["tags"] == ["api", "metadata"]
        assert data["bug"]["source_kind"] == "manual"
        assert data["bug"]["decision_ref"] == "decision:bugs:test"
        assert data["bug"]["discovered_in_run_id"] == "run-123"
        assert data["bug"]["discovered_in_receipt_id"] == "receipt-123"
        assert data["bug"]["owner_ref"] == "owner-123"
        assert data["bug"]["filed_by"] == "workflow_api"

    def test_search_bugs(self, api_server):
        status, data = _post(
            api_server, "/bugs", {"action": "search", "title": "test"}
        )
        assert status == 200
        assert data["count"] >= 1

    def test_bug_stats(self, api_server):
        status, data = _post(api_server, "/bugs", {"action": "stats"})
        assert status == 200
        assert data["stats"]["packet_ready_count"] == 1
        assert data["stats"]["open_count"] == 1

    def test_bug_packet(self, api_server):
        status, data = _post(
            api_server,
            "/bugs",
            {"action": "packet", "bug_id": "BUG-001", "receipt_limit": 2},
        )
        assert status == 200
        assert data["packet"]["bug"]["bug_id"] == "BUG-001"
        assert data["packet"]["replay_context"]["ready"] is True
        assert data["packet"]["signature"]["failure_code"] == "timeout_exceeded"
        assert data["packet"]["agent_actions"]["replay"]["available"] is True
        assert data["packet"]["historical_fixes"]["count"] == 1

    def test_bug_history(self, api_server):
        status, data = _post(
            api_server,
            "/bugs",
            {"action": "history", "bug_id": "BUG-001"},
        )
        assert status == 200
        assert data["history"]["bug_id"] == "BUG-001"
        assert data["history"]["historical_fixes"]["count"] == 1
        assert data["history"]["agent_actions"]["replay"]["tool"] == "praxis_bugs"

    def test_bug_replay(self, api_server):
        status, data = _post(
            api_server,
            "/bugs",
            {"action": "replay", "bug_id": "BUG-001"},
        )
        assert status == 200
        assert data["replay"]["ready"] is True
        assert data["replay"]["replay"]["run_id"] == "run-123"
        assert data["replay"]["tooling"]["replay"]["arguments"] == {
            "action": "replay",
            "bug_id": "BUG-001",
        }
        assert data["replay"]["historical_fixes"]["count"] == 1

    def test_bug_backfill_replay(self, api_server):
        status, data = _post(
            api_server,
            "/bugs",
            {"action": "backfill_replay", "open_only": True},
        )
        assert status == 200
        assert data["backfill"]["scanned_count"] == 1
        assert data["backfill"]["backfilled_count"] == 1
        assert data["backfill"]["bugs"][0]["replay_ready"] is True

    def test_get_api_bugs_uses_canonical_bug_shape(self, api_server):
        status, raw, _ = _request(api_server, "/api/bugs")
        assert status == 200
        data = json.loads(raw)
        bug = data["bugs"][0]
        assert bug["bug_id"] == "BUG-001"
        assert bug["bug_key"] == "bug_001"
        assert bug["priority"] == "P2"
        assert bug["summary"] == "desc"
        assert bug["opened_at"] == bug["filed_at"]
        assert bug["created_at"]
        assert "resolution_summary" in bug
        assert bug["replay_ready"] is True
        assert bug["replay_reason_code"] == "bug.replay_ready"

    def test_attach_bug_evidence(self, api_server):
        status, data = _post(
            api_server,
            "/bugs",
            {
                "action": "attach_evidence",
                "bug_id": "BUG-001",
                "evidence_kind": "receipt",
                "evidence_ref": "receipt-123",
                "evidence_role": "observed_in",
                "notes": "seed evidence",
            },
        )
        assert status == 200
        assert data["attached"] is True
        assert data["evidence_link"]["evidence_kind"] == "receipt"
        assert data["evidence_link"]["evidence_ref"] == "receipt-123"

    def test_file_bug_rejects_invalid_provenance(self, api_server):
        status, data = _post(
            api_server,
            "/bugs",
            {
                "action": "file",
                "title": "bad provenance",
                "discovered_in_run_id": "run-does-not-exist",
            },
        )
        assert status == 400
        assert "unknown discovered_in_run_id" in data["error"]

    def test_file_bug_rejects_invalid_category(self, api_server):
        status, data = _post(
            api_server,
            "/bugs",
            {
                "action": "file",
                "title": "bad category",
                "category": "NOT_A_REAL_CATEGORY",
            },
        )
        assert status == 400
        assert "category must be one of" in data["error"]

    def test_attach_bug_evidence_rejects_invalid_evidence(self, api_server):
        status, data = _post(
            api_server,
            "/bugs",
            {
                "action": "attach_evidence",
                "bug_id": "BUG-001",
                "evidence_kind": "receipt",
                "evidence_ref": "receipt-does-not-exist",
                "evidence_role": "observed_in",
            },
        )
        assert status == 400
        assert "unknown receipt reference" in data["error"]

    def test_get_api_replay_ready_bugs_view(self, api_server):
        status, raw, _ = _request(api_server, "/api/bugs/replay-ready")
        assert status == 200
        data = json.loads(raw)
        assert data["view"] == "replay_ready_bugs"
        assert data["maintenance"]["backfilled_count"] == 1
        assert data["bugs"][0]["replay_ready"] is True
        assert data["returned_count"] == 1

    def test_file_bug_missing_title(self, api_server):
        status, data = _post(api_server, "/bugs", {"action": "file"})
        assert status == 400

    def test_resolve_bug(self, api_server):
        status, data = _post(
            api_server,
            "/bugs",
            {"action": "file", "title": "api resolve target", "severity": "P2"},
        )
        assert status == 200
        bug_id = data["bug"]["bug_id"]

        status, data = _post(
            api_server,
            "/bugs",
            {"action": "resolve", "bug_id": bug_id, "status": "WONT_FIX"},
        )
        assert status == 200
        assert data["resolved"] is True
        assert data["bug"]["bug_id"] == bug_id
        assert data["bug"]["status"] == "WONT_FIX"

    def test_resolve_bug_rejects_non_terminal_status(self, api_server):
        status, data = _post(
            api_server,
            "/bugs",
            {"action": "file", "title": "api invalid resolve target", "severity": "P2"},
        )
        assert status == 200
        bug_id = data["bug"]["bug_id"]

        status, data = _post(
            api_server,
            "/bugs",
            {"action": "resolve", "bug_id": bug_id, "status": "OPEN"},
        )
        assert status == 400


class TestHealth:
    def test_returns_probes(self, api_server):
        status, data = _post(api_server, "/health")
        assert status == 200
        assert "preflight" in data
        assert data["preflight"]["overall"] == "pass"
        assert "operator_snapshot" in data
        assert "lane_recommendation" in data


class TestOperatorControl:
    def test_operator_view_replay_ready_bugs_returns_direct_payload(self, api_server):
        status, data = _post(
            api_server,
            "/operator_view",
            {"view": "replay_ready_bugs", "limit": 10},
        )

        assert status == 200
        assert data["view"] == "replay_ready_bugs"
        assert data["maintenance"]["linked_count"] == 2
        assert data["bugs"][0]["replay_reason_code"] == "bug.replay_ready"

    def test_task_route_eligibility_post_writes_timed_window(self, api_server):
        captured: dict[str, Any] = {}

        def _set_task_route_eligibility_window(**kwargs):
            captured.update(kwargs)
            return {
                "task_route_eligibility": {
                    "task_route_eligibility_id": (
                        "task-route-eligibility."
                        "anthropic.any-task.any-model.rejected.20260408T160000Z"
                    ),
                    "provider_slug": kwargs["provider_slug"],
                    "task_type": kwargs["task_type"],
                    "model_slug": kwargs["model_slug"],
                    "eligibility_status": kwargs["eligibility_status"],
                    "reason_code": kwargs["reason_code"],
                    "rationale": kwargs["rationale"],
                    "effective_from": kwargs["effective_from"].isoformat(),
                    "effective_to": kwargs["effective_to"].isoformat(),
                    "decision_ref": "decision:test",
                    "created_at": "2026-04-08T16:00:00+00:00",
                },
                "superseded_task_route_eligibility_ids": [],
            }

        with patch.object(
            admin_handlers.operator_write,
            "set_task_route_eligibility_window",
            _set_task_route_eligibility_window,
        ):
            status, data = _post(
                api_server,
                "/api/operator/task-route-eligibility",
                {
                    "provider_slug": "anthropic",
                    "eligibility_status": "rejected",
                    "effective_from": "2026-04-08T09:00:00-07:00",
                    "effective_to": "2026-04-10T09:00:00-07:00",
                    "reason_code": "provider_disabled",
                    "rationale": "Anthropic off until Friday morning",
                },
            )

        assert status == 200
        assert captured["provider_slug"] == "anthropic"
        assert captured["eligibility_status"] == "rejected"
        assert captured["reason_code"] == "provider_disabled"
        assert captured["effective_from"].isoformat() == "2026-04-08T09:00:00-07:00"
        assert captured["effective_to"].isoformat() == "2026-04-10T09:00:00-07:00"
        assert data["task_route_eligibility"]["provider_slug"] == "anthropic"

    def test_task_route_eligibility_post_rejects_naive_datetime(self, api_server):
        status, data = _post(
            api_server,
            "/api/operator/task-route-eligibility",
            {
                "provider_slug": "anthropic",
                "effective_to": "2026-04-10T09:00:00",
            },
        )

        assert status == 400
        assert "timezone" in data["error"]

    def test_roadmap_write_post_uses_shared_gate(self, api_server):
        captured: dict[str, Any] = {}

        def _roadmap_write(**kwargs):
            captured.update(kwargs)
            return {
                "action": kwargs["action"],
                "normalized_payload": {
                    "title": kwargs["title"],
                    "template": kwargs["template"],
                    "parent_roadmap_item_id": kwargs["parent_roadmap_item_id"],
                },
                "auto_fixes": [],
                "warnings": [],
                "blocking_errors": [],
                "preview": {
                    "roadmap_items": [
                        {
                            "roadmap_item_id": "roadmap_item.authority.cleanup.operator_write_gate",
                        },
                    ],
                    "roadmap_item_dependencies": [],
                },
                "committed": False,
            }

        with patch.object(
            admin_handlers.operator_write,
            "roadmap_write",
            _roadmap_write,
        ):
            status, data = _post(
                api_server,
                "/api/operator/roadmap-write",
                {
                    "action": "validate",
                    "title": "Unified operator write gate",
                    "intent_brief": "Single validation gate for roadmap writes",
                    "template": "hard_cutover_program",
                    "priority": "p1",
                    "parent_roadmap_item_id": "roadmap_item.authority.cleanup",
                    "depends_on": [
                        "roadmap_item.authority.cleanup.validation_review",
                    ],
                    "phase_ready": True,
                },
            )

        assert status == 200
        assert captured["action"] == "validate"
        assert captured["title"] == "Unified operator write gate"
        assert captured["intent_brief"] == "Single validation gate for roadmap writes"
        assert captured["template"] == "hard_cutover_program"
        assert captured["priority"] == "p1"
        assert captured["parent_roadmap_item_id"] == "roadmap_item.authority.cleanup"
        assert captured["depends_on"] == [
            "roadmap_item.authority.cleanup.validation_review",
        ]
        assert captured["phase_ready"] is True
        assert data["normalized_payload"]["template"] == "hard_cutover_program"

    def test_work_item_closeout_post_uses_shared_gate(self, api_server):
        captured: dict[str, Any] = {}

        def _reconcile_work_item_closeout(**kwargs):
            captured.update(kwargs)
            return {
                "action": kwargs["action"],
                "proof_threshold": {
                    "bug_requires_evidence_role": "validates_fix",
                    "roadmap_requires_source_bug_fix_proof": True,
                },
                "evaluated": {
                    "bug_ids": kwargs.get("bug_ids", []),
                    "roadmap_item_ids": kwargs.get("roadmap_item_ids", []),
                },
                "candidates": {"bugs": [], "roadmap_items": []},
                "skipped": {"bugs": [], "roadmap_items": []},
                "committed": False,
                "applied": {"bugs": [], "roadmap_items": []},
            }

        with patch.object(
            admin_handlers.operator_write,
            "reconcile_work_item_closeout",
            _reconcile_work_item_closeout,
        ):
            status, data = _post(
                api_server,
                "/api/operator/work-item-closeout",
                {
                    "action": "preview",
                    "bug_ids": ["bug.closeout.1"],
                    "roadmap_item_ids": ["roadmap_item.closeout.1"],
                },
            )

        assert status == 200
        assert captured["action"] == "preview"
        assert captured["bug_ids"] == ["bug.closeout.1"]
        assert captured["roadmap_item_ids"] == ["roadmap_item.closeout.1"]
        assert data["proof_threshold"]["bug_requires_evidence_role"] == "validates_fix"

    def test_roadmap_view_post_reads_tree(self, api_server):
        captured: dict[str, Any] = {}

        def _query_roadmap_tree(**kwargs):
            captured.update(kwargs)
            return {
                "kind": "roadmap_tree",
                "root_roadmap_item_id": kwargs["root_roadmap_item_id"],
                "counts": {"roadmap_items": 2, "roadmap_item_dependencies": 1},
                "rendered_markdown": "# Unified operator write validation gate",
            }

        with patch.object(
            admin_handlers.operator_read,
            "query_roadmap_tree",
            _query_roadmap_tree,
        ):
            status, data = _post(
                api_server,
                "/api/operator/roadmap-view",
                {
                    "root_roadmap_item_id": (
                        "roadmap_item.authority.cleanup.unified.operator.write.validation.gate"
                    ),
                },
            )

        assert status == 200
        assert captured["root_roadmap_item_id"] == (
            "roadmap_item.authority.cleanup.unified.operator.write.validation.gate"
        )
        assert data["kind"] == "roadmap_tree"
        assert data["counts"]["roadmap_items"] == 2


class TestRecall:
    def test_search(self, api_server):
        status, data = _post(
            api_server, "/recall", {"query": "testing patterns"}
        )
        assert status == 200
        assert data["count"] >= 1
        assert data["results"][0]["name"] == "test entity"

    def test_missing_query(self, api_server):
        status, data = _post(api_server, "/recall", {})
        assert status == 400


class TestIngest:
    def test_creates_entity(self, api_server):
        status, data = _post(
            api_server,
            "/ingest",
            {
                "kind": "document",
                "content": "A new design doc",
                "source": "test-suite",
            },
        )
        assert status == 200
        assert data["accepted"] is True
        assert data["entities_created"] == 1

    def test_missing_fields(self, api_server):
        status, data = _post(
            api_server, "/ingest", {"kind": "document"}
        )
        assert status == 400


class TestGraph:
    def test_blast_radius(self, api_server):
        status, data = _post(
            api_server, "/graph", {"entity_id": "ent-1"}
        )
        assert status == 200
        assert data["entity_id"] == "ent-1"

    def test_missing_entity_id(self, api_server):
        status, data = _post(api_server, "/graph", {})
        assert status == 400


class TestWave:
    def test_observe(self, api_server):
        status, data = _post(
            api_server, "/wave", {"action": "observe"}
        )
        assert status == 200
        assert data["orch_id"] == "orch-default"
        assert len(data["waves"]) >= 1


class TestFiles:
    def test_upload_list_download_delete_flow(self, file_api_server):
        base_url, pg, tmp_path = file_api_server
        payload = {
            "filename": "invoice.pdf",
            "content": base64.b64encode(b"pdf-bytes").decode("ascii"),
            "content_type": "application/pdf",
            "scope": "step",
            "workflow_id": "wf_123",
            "step_id": "ps_abc",
            "description": "Quarterly invoice",
        }

        status, data = _post(base_url, "/api/files", payload)
        assert status == 200
        file_id = data["file"]["id"]
        stored_path = tmp_path / data["file"]["storage_path"]
        assert stored_path.read_bytes() == b"pdf-bytes"
        assert file_id in pg.uploaded_files

        status, body, _ = _request(
            base_url,
            "/api/files?scope=step&workflow_id=wf_123&step_id=ps_abc",
        )
        listing = json.loads(body)
        assert status == 200
        assert listing["count"] == 1
        assert listing["files"][0]["id"] == file_id

        status, body, headers = _request(
            base_url,
            f"/api/files/{file_id}/content",
        )
        assert status == 200
        assert body == b"pdf-bytes"
        assert headers["Content-Type"] == "application/pdf"
        assert "attachment" in headers["Content-Disposition"]

        status, body, _ = _request(
            base_url,
            f"/api/files/{file_id}",
            method="DELETE",
        )
        deleted = json.loads(body)
        assert status == 200
        assert deleted["deleted"] is True
        assert not stored_path.exists()
        assert file_id not in pg.uploaded_files

    def test_upload_accepts_multipart_form_data(self, file_api_server):
        base_url, _, tmp_path = file_api_server
        boundary = "----dag-upload-boundary"
        body = (
            f"--{boundary}\r\n"
            'Content-Disposition: form-data; name="file"; filename="notes.txt"\r\n'
            "Content-Type: text/plain\r\n\r\n"
            "hello world\r\n"
            f"--{boundary}\r\n"
            'Content-Disposition: form-data; name="scope"\r\n\r\n'
            "workflow\r\n"
            f"--{boundary}\r\n"
            'Content-Disposition: form-data; name="workflow_id"\r\n\r\n'
            "wf_999\r\n"
            f"--{boundary}--\r\n"
        ).encode("utf-8")

        status, raw, _ = _request(
            base_url,
            "/api/files",
            method="POST",
            body=body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        )
        data = json.loads(raw)
        assert status == 200
        assert data["file"]["filename"] == "notes.txt"
        assert data["file"]["scope"] == "workflow"
        assert (tmp_path / data["file"]["storage_path"]).read_bytes() == b"hello world"

    def test_put_to_delete_only_file_route_returns_405(self, file_api_server):
        base_url, _, _ = file_api_server
        status, raw, _ = _request(
            base_url,
            "/api/files/file_missing",
            method="PUT",
            body=b"{}",
            headers={"Content-Type": "application/json"},
        )
        data = json.loads(raw)
        assert status == 405
        assert "Method not allowed" in data["error"]


class TestMutationRoutes:
    def test_put_and_delete_routes_use_canonical_verbs(self, file_api_server):
        base_url, pg, _ = file_api_server

        status, type_data = _post(
            base_url,
            "/api/object-types",
            {
                "name": "Widget",
                "description": "A canonical object type",
                "property_definitions": {"title": {"type": "string"}},
            },
        )
        assert status == 200
        type_id = type_data["type_id"]
        assert type_id.startswith("widget-")
        assert type_id in pg.object_types

        status, object_data = _post(
            base_url,
            "/api/objects",
            {
                "type_id": type_id,
                "properties": {"title": "Initial", "state": "draft"},
            },
        )
        assert status == 200
        object_id = object_data["object_id"]
        assert pg.objects[object_id]["properties"]["title"] == "Initial"

        status, raw, _ = _request(
            base_url,
            "/api/objects/update",
            method="PUT",
            body=json.dumps(
                {
                    "object_id": object_id,
                    "properties": {"state": "ready", "owner": "nate"},
                }
            ).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        updated = json.loads(raw)
        assert status == 200
        assert updated["properties"]["title"] == "Initial"
        assert updated["properties"]["state"] == "ready"
        assert updated["properties"]["owner"] == "nate"
        assert pg.objects[object_id]["status"] == "active"

        status, raw = _json_request(
            base_url,
            "/api/objects/update",
            method="POST",
            body={"object_id": object_id, "properties": {"state": "ignored"}},
        )
        assert status == 405
        assert "Method not allowed" in raw["error"]

        status, raw, _ = _request(
            base_url,
            "/api/objects/delete",
            method="DELETE",
            body=json.dumps({"object_id": object_id}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        deleted = json.loads(raw)
        assert status == 200
        assert deleted["deleted"] is True
        assert pg.objects[object_id]["status"] == "deleted"

        status, raw = _json_request(
            base_url,
            "/api/objects/delete",
            method="POST",
            body={"object_id": object_id},
        )
        assert status == 405
        assert "Method not allowed" in raw["error"]

        status, raw = _json_request(
            base_url,
            "/api/workflows/wf-123",
            method="PUT",
            body={"description": "Updated workflow description"},
        )
        workflow_update = raw
        assert status == 200
        assert workflow_update["workflow"]["description"] == "Updated workflow description"
        assert pg.workflows["wf-123"]["description"] == "Updated workflow description"

        status, raw = _json_request(
            base_url,
            "/api/workflows/wf-123",
            method="POST",
            body={"description": "Updated workflow description"},
        )
        assert status == 405
        assert "Method not allowed" in raw["error"]

        status, raw = _json_request(
            base_url,
            "/api/workflow-triggers/trg-123",
            method="PUT",
            body={"enabled": False},
        )
        trigger_update = raw
        assert status == 200
        assert trigger_update["trigger"]["enabled"] is False
        assert pg.workflow_triggers["trg-123"]["enabled"] is False

        status, raw = _json_request(
            base_url,
            "/api/workflow-triggers/trg-123",
            method="POST",
            body={"enabled": False},
        )
        assert status == 405
        assert "Method not allowed" in raw["error"]

        status, raw, _ = _request(
            base_url,
            "/api/workflows/delete/wf-123",
            method="DELETE",
            body=json.dumps({}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        workflow_deleted = json.loads(raw)
        assert status == 200
        assert workflow_deleted["deleted"] is True
        assert "wf-123" not in pg.workflows
        assert "trg-123" not in pg.workflow_triggers

        status, raw = _json_request(
            base_url,
            "/api/workflows/delete/wf-123",
            method="POST",
            body={},
        )
        assert status == 405
        assert "Method not allowed" in raw["error"]


class TestVerbRouting:
    def test_put_uses_put_handler(self, api_server, monkeypatch):
        calls: list[str] = []

        def _put_handler(request, path):
            calls.append(path)
            request._send_json(200, {"handled_by": "put", "path": path})
            return True

        def _unexpected_handler(*args, **kwargs):
            raise AssertionError("workflow_api routed PUT through the wrong handler")

        monkeypatch.setattr(workflow_api, "handle_put_request", _put_handler)
        monkeypatch.setattr(workflow_api, "handle_post_request", _unexpected_handler)
        monkeypatch.setattr(workflow_api, "handle_delete_request", _unexpected_handler)

        status, raw, _ = _request(
            api_server,
            "/verb-check",
            method="PUT",
            body=b"{}",
            headers={"Content-Type": "application/json"},
        )

        data = json.loads(raw)
        assert status == 200
        assert data == {"handled_by": "put", "path": "/verb-check"}
        assert calls == ["/verb-check"]

    def test_delete_uses_delete_handler(self, api_server, monkeypatch):
        calls: list[str] = []

        def _delete_handler(request, path):
            calls.append(path)
            request._send_json(200, {"handled_by": "delete", "path": path})
            return True

        def _unexpected_handler(*args, **kwargs):
            raise AssertionError("workflow_api routed DELETE through the wrong handler")

        monkeypatch.setattr(workflow_api, "handle_delete_request", _delete_handler)
        monkeypatch.setattr(workflow_api, "handle_post_request", _unexpected_handler)
        monkeypatch.setattr(workflow_api, "handle_put_request", _unexpected_handler)

        status, raw, _ = _request(
            api_server,
            "/verb-check",
            method="DELETE",
        )

        data = json.loads(raw)
        assert status == 200
        assert data == {"handled_by": "delete", "path": "/verb-check"}
        assert calls == ["/verb-check"]


class TestLauncherRoutes:
    def test_launcher_status_delegates(self, api_server, monkeypatch):
        expected = {
            "ok": True,
            "ready": True,
            "platform_state": "ready",
            "launch_url": "http://127.0.0.1:8420/app",
        }
        monkeypatch.setattr(admin_handlers.workflow_launcher, "launcher_status_payload", lambda: expected)

        status, raw, _ = _request(api_server, "/api/launcher/status", include_ui_header=True)

        assert status == 200
        assert json.loads(raw) == expected

    def test_launcher_recover_delegates(self, api_server, monkeypatch):
        expected = (200, {"ok": True, "action": "launch"})
        monkeypatch.setattr(
            admin_handlers.workflow_launcher,
            "launcher_recover_payload",
            lambda **kwargs: expected,
        )

        status, data = _json_request(
            api_server,
            "/api/launcher/recover",
            method="POST",
            body={"action": "launch"},
        )

        assert status == 200
        assert data == expected[1]


class TestUiHeaderGate:
    def test_accepts_new_ui_header(self, api_server):
        status, data = _post(api_server, "/orient")
        assert status == 200
        assert data

    def test_rejects_missing_ui_header(self, api_server):
        status, raw, _ = _request(
            api_server,
            "/orient",
            method="POST",
            headers={"Content-Type": "application/json"},
            include_ui_header=False,
        )
        assert status == 400
        payload = json.loads(raw)
        assert payload["error"] == "Missing or invalid X-Praxis-UI header"


class TestEdgeCases:
    def test_404_on_unknown_path(self, api_server):
        status, data = _post(api_server, "/nonexistent")
        assert status == 404
        assert "error" in data

    def test_400_on_malformed_json(self, api_server):
        """Send raw bytes that aren't valid JSON."""
        req = urllib.request.Request(
            f"{api_server}/health",
            data=b"not json at all{{{",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req) as resp:
                code, body = resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as e:
            code, body = e.code, json.loads(e.read())
        assert code == 400
        assert "Invalid JSON" in body["error"]

    def test_get_root(self, api_server):
        """GET / returns service info."""
        req = urllib.request.Request(f"{api_server}/", method="GET")
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
        assert data["service"] == "dag-workflow-api"

    def test_empty_body_on_health(self, api_server):
        """POST /health with empty body still works."""
        req = urllib.request.Request(
            f"{api_server}/health",
            data=b"",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
        assert data["preflight"]["overall"] == "pass"
