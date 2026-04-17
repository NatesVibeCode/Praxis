from __future__ import annotations

import io
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from runtime import canonical_workflows
from runtime.compile_index import CompileIndexAuthorityError
from runtime.operations.queries import handoff as handoff_queries
from runtime.self_healing import SelfHealingOrchestrator
from policy.workflow_lanes import (
    WorkflowLaneAuthorityRecord,
    WorkflowLaneCatalog,
    WorkflowLanePolicyAuthorityRecord,
)
from surfaces.api.handlers import workflow_query
from surfaces.api.handlers import workflow_query_core
from surfaces.api import handlers as api_handlers


class _RequestStub:
    def __init__(
        self,
        body: dict[str, Any] | None = None,
        *,
        subsystems: Any | None = None,
        path: str = "/api/test",
    ) -> None:
        raw = json.dumps(body or {}).encode("utf-8")
        self.headers = {"Content-Length": str(len(raw))}
        self.rfile = io.BytesIO(raw)
        self.subsystems = subsystems or SimpleNamespace(get_pg_conn=lambda: None)
        self.path = path
        self.sent: tuple[int, dict[str, Any]] | None = None

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        self.sent = (status, payload)


class _RecordingPg:
    def __init__(
        self,
        reference_rows: list[dict[str, Any]] | None = None,
        integration_rows: list[dict[str, Any]] | None = None,
        capability_rows: list[dict[str, Any]] | None = None,
        manifest_rows: dict[str, dict[str, Any]] | None = None,
        head_rows: list[dict[str, Any]] | None = None,
        history_rows: list[dict[str, Any]] | None = None,
        workflow_rows: dict[str, dict[str, Any]] | None = None,
        market_rows: list[dict[str, Any]] | None = None,
        binding_rows: list[dict[str, Any]] | None = None,
        workflow_run_rows: list[dict[str, Any]] | None = None,
        trigger_rows: list[dict[str, Any]] | None = None,
        packet_query_rows: dict[str, dict[str, Any]] | None = None,
        build_review_decision_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.reference_rows = reference_rows or []
        self.integration_rows = integration_rows or []
        self.capability_rows = capability_rows or []
        self.manifest_rows = manifest_rows or {}
        self.head_rows = head_rows or []
        self.history_rows = history_rows or []
        self.workflow_rows = workflow_rows or {}
        self.market_rows = market_rows or [
            {
                "market_model_ref": "market_model.artificial_analysis.llm.model-123",
                "source_slug": "artificial_analysis",
                "modality": "llm",
                "source_model_id": "model-123",
                "source_model_slug": "gpt-5.4",
                "model_name": "GPT-5.4",
                "creator_slug": "openai",
                "creator_name": "OpenAI",
                "evaluations": {"artificial_analysis_intelligence_index": 70.1},
                "pricing": {"price_1m_blended_3_to_1": 12.3},
                "speed_metrics": {"median_output_tokens_per_second": 98.2},
                "prompt_options": {"prompt_length": "medium"},
                "last_synced_at": "2026-04-08T12:00:00Z",
            }
        ]
        self.binding_rows = binding_rows or [
            {
                "market_model_ref": "market_model.artificial_analysis.llm.model-123",
                "binding_kind": "normalized_slug_alias",
                "binding_confidence": 0.99,
                "candidate_ref": "candidate.openai.gpt-5.4",
                "provider_slug": "openai",
                "model_slug": "gpt-5.4",
            }
        ]
        self.workflow_run_rows = workflow_run_rows or []
        self.trigger_rows = trigger_rows or []
        self.packet_query_rows = packet_query_rows or {}
        self.build_review_decision_rows = build_review_decision_rows or []
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.event_log_rows: list[dict[str, Any]] = []

    def execute(self, query: str, *params: Any) -> list[dict[str, Any]]:
        self.executed.append((query, params))
        stripped = query.strip()
        if stripped.startswith("INSERT INTO event_log"):
            row = {
                "id": len(self.event_log_rows) + 1,
                "channel": params[0],
                "event_type": params[1],
                "entity_id": params[2],
                "entity_kind": params[3],
                "payload": json.loads(params[4]) if isinstance(params[4], str) else params[4],
                "emitted_by": params[5],
            }
            self.event_log_rows.append(row)
            return [{"id": row["id"]}]
        if stripped.startswith("SELECT pg_notify"):
            return []
        if "FROM reference_catalog" in query:
            return self.reference_rows
        if "FROM integration_registry" in query:
            return self.integration_rows
        if "FROM capability_catalog" in query:
            return self.capability_rows
        if "FROM market_model_registry" in query:
            return self.market_rows
        if "FROM provider_model_market_bindings" in query:
            return self.binding_rows
        if "FROM control_manifest_heads h" in query:
            rows = list(self.head_rows)
            param_index = 0
            if "h.workspace_ref = $" in query:
                workspace_ref = str(params[param_index]).strip()
                param_index += 1
                rows = [row for row in rows if str(row.get("workspace_ref") or "").strip() == workspace_ref]
            if "h.scope_ref = $" in query:
                scope_ref = str(params[param_index]).strip()
                param_index += 1
                rows = [row for row in rows if str(row.get("scope_ref") or "").strip() == scope_ref]
            if "h.manifest_type = $" in query:
                manifest_type = str(params[param_index]).strip()
                param_index += 1
                rows = [row for row in rows if str(row.get("manifest_type") or "").strip() == manifest_type]
            if "h.head_status = $" in query:
                head_status = str(params[param_index]).strip()
                param_index += 1
                rows = [row for row in rows if str(row.get("head_status") or "").strip() == head_status]
            limit = int(params[param_index]) if param_index < len(params) else len(rows)
            return rows[:limit]
        if "FROM app_manifest_history" in query and "manifest_snapshot" in query and "change_description" in query:
            rows = list(self.history_rows)
            param_index = 0

            def _payload_text(payload: Any, *path: str) -> str:
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except (TypeError, json.JSONDecodeError):
                        return ""
                current: Any = payload
                for key in path:
                    if not isinstance(current, dict):
                        return ""
                    current = current.get(key)
                return str(current or "").strip()

            def _control_ref(payload: Any, field: str) -> str:
                for path in (
                    (field,),
                    ("plan", field),
                    ("approval", field),
                    ("job", field),
                ):
                    value = _payload_text(payload, *path)
                    if value:
                        return value
                return ""

            if "COALESCE(manifest_snapshot->>'kind', '') = $" in query:
                kind = str(params[param_index]).strip()
                param_index += 1
                rows = [row for row in rows if _payload_text(row.get("manifest_snapshot"), "kind") == kind]
            if "COALESCE(manifest_snapshot->>'manifest_family', '') = $" in query:
                manifest_family = str(params[param_index]).strip()
                param_index += 1
                rows = [
                    row
                    for row in rows
                    if _payload_text(row.get("manifest_snapshot"), "manifest_family") == manifest_family
                ]
            if "manifest_snapshot->>'workspace_ref' = $" in query or "COALESCE(manifest_snapshot->>'workspace_ref'" in query:
                workspace_ref = str(params[param_index]).strip()
                param_index += 1
                rows = [row for row in rows if _control_ref(row.get("manifest_snapshot"), "workspace_ref") == workspace_ref]
            if "manifest_snapshot->>'scope_ref' = $" in query or "COALESCE(manifest_snapshot->>'scope_ref'" in query:
                scope_ref = str(params[param_index]).strip()
                param_index += 1
                rows = [row for row in rows if _control_ref(row.get("manifest_snapshot"), "scope_ref") == scope_ref]
            if "manifest_snapshot->>'manifest_type' = $" in query:
                manifest_type = str(params[param_index]).strip()
                param_index += 1
                rows = [
                    row
                    for row in rows
                    if _payload_text(row.get("manifest_snapshot"), "manifest_type") == manifest_type
                ]
            if "manifest_snapshot->>'status' = $" in query:
                status = str(params[param_index]).strip()
                param_index += 1
                rows = [
                    row
                    for row in rows
                    if _payload_text(row.get("manifest_snapshot"), "status") == status
                ]
            rows = sorted(
                rows,
                key=lambda row: (
                    str(row.get("created_at") or ""),
                    str(row.get("manifest_id") or ""),
                    int(row.get("version") or 0),
                ),
                reverse=True,
            )
            limit = int(params[param_index]) if param_index < len(params) else len(rows)
            return rows[:limit]
        if "FROM app_manifests" in query and "SELECT id, name, description, status, manifest, updated_at" in query:
            rows = list(self.manifest_rows.values())
            param_index = 0
            if "plainto_tsquery('english', $1)" in query or "ILIKE '%' || $1 || '%'" in query:
                search = str(params[param_index]).strip().lower()
                param_index += 1
                if search:
                    filtered_rows = []
                    for row in rows:
                        haystack = " ".join(
                            str(part or "").strip().lower()
                            for part in (
                                row.get("id"),
                                row.get("name"),
                                row.get("description"),
                                row.get("status"),
                                json.dumps(row.get("manifest") or {}, sort_keys=True),
                            )
                        )
                        if search in haystack:
                            filtered_rows.append(row)
                    rows = filtered_rows
            if "manifest->>'manifest_family' = $" in query:
                family = str(params[param_index]).strip()
                param_index += 1
                rows = [
                    row
                    for row in rows
                    if str((row.get("manifest") or {}).get("manifest_family") or "") == family
                ]
            if "manifest->>'manifest_type' = $" in query:
                manifest_type = str(params[param_index]).strip()
                param_index += 1
                rows = [
                    row
                    for row in rows
                    if str((row.get("manifest") or {}).get("manifest_type") or "") == manifest_type
                ]
            if "status = $" in query:
                status = str(params[param_index]).strip()
                param_index += 1
                rows = [row for row in rows if str(row.get("status") or "") == status]
            rows = sorted(
                rows,
                key=lambda row: (
                    str(row.get("updated_at") or ""),
                    str(row.get("name") or ""),
                ),
                reverse=True,
            )
            limit = int(params[param_index]) if param_index < len(params) else len(rows)
            return rows[:limit]
        if "FROM app_manifests" in query and "SELECT id, name, description, status, version, parent_manifest_id, manifest, created_at, updated_at" in query:
            rows = list(self.manifest_rows.values())
            param_index = 0

            def _payload_text(payload: Any, *path: str) -> str:
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except (TypeError, json.JSONDecodeError):
                        return ""
                current: Any = payload
                for key in path:
                    if not isinstance(current, dict):
                        return ""
                    current = current.get(key)
                return str(current or "").strip()

            def _control_ref(payload: Any, field: str) -> str:
                for path in (
                    (field,),
                    ("plan", field),
                    ("approval", field),
                    ("job", field),
                ):
                    value = _payload_text(payload, *path)
                    if value:
                        return value
                return ""

            if "COALESCE(manifest->>'kind', '') = $" in query:
                kind = str(params[param_index]).strip()
                param_index += 1
                rows = [row for row in rows if _payload_text(row.get("manifest"), "kind") == kind]
            if "COALESCE(manifest->>'manifest_family', '') = $" in query:
                manifest_family = str(params[param_index]).strip()
                param_index += 1
                rows = [
                    row
                    for row in rows
                    if _payload_text(row.get("manifest"), "manifest_family") == manifest_family
                ]
            if "manifest->>'workspace_ref' = $" in query or "COALESCE(manifest->>'workspace_ref'" in query:
                workspace_ref = str(params[param_index]).strip()
                param_index += 1
                rows = [row for row in rows if _control_ref(row.get("manifest"), "workspace_ref") == workspace_ref]
            if "manifest->>'scope_ref' = $" in query or "COALESCE(manifest->>'scope_ref'" in query:
                scope_ref = str(params[param_index]).strip()
                param_index += 1
                rows = [row for row in rows if _control_ref(row.get("manifest"), "scope_ref") == scope_ref]
            if "manifest->>'manifest_type' = $" in query:
                manifest_type = str(params[param_index]).strip()
                param_index += 1
                rows = [
                    row
                    for row in rows
                    if _payload_text(row.get("manifest"), "manifest_type") == manifest_type
                ]
            if "status = $" in query:
                status = str(params[param_index]).strip()
                param_index += 1
                rows = [row for row in rows if str(row.get("status") or "") == status]
            rows = sorted(
                rows,
                key=lambda row: (
                    str(row.get("updated_at") or ""),
                    int(row.get("version") or 0),
                    str(row.get("name") or ""),
                ),
                reverse=True,
            )
            limit = int(params[param_index]) if param_index < len(params) else len(rows)
            return rows[:limit]
        if "FROM public.workflow_triggers" in query and "WHERE t.workflow_id = $1" in query:
            workflow_id = str(params[0])
            return [row for row in self.trigger_rows if str(row.get("workflow_id")) == workflow_id]
        if "FROM public.workflow_runs wr" in query and "WHERE wr.run_id = $1" in query:
            row = self.packet_query_rows.get(str(params[0]))
            return [row] if row else []
        if "FROM public.workflow_runs" in query and "WHERE COALESCE(request_envelope->>'name', workflow_id) = $1" in query:
            workflow_name = str(params[0])
            rows = []
            for row in self.workflow_run_rows:
                if str(row.get("workflow_name") or row.get("spec_name") or "") != workflow_name:
                    continue
                rows.append({key: value for key, value in row.items() if key != "workflow_name"})
            return rows
        if "FROM workflows WHERE id = $1" in query or "FROM public.workflows WHERE id = $1" in query:
            workflow = self.workflow_rows.get(str(params[0]))
            return [workflow] if workflow else []
        if "FROM workflows WHERE name = $1" in query or "FROM public.workflows WHERE name = $1" in query:
            workflow_name = str(params[0])
            for workflow in self.workflow_rows.values():
                if workflow.get("name") == workflow_name:
                    return [workflow]
            return []
        if "FROM workflow_build_review_decisions" in query and "SELECT DISTINCT ON (target_kind, target_ref" in query:
            workflow_id = str(params[0])
            definition_revision = str(params[1])
            filtered = [
                row
                for row in self.build_review_decision_rows
                if str(row.get("workflow_id")) == workflow_id
                and str(row.get("definition_revision")) == definition_revision
            ]
            latest: dict[tuple[str, str], dict[str, Any]] = {}
            for row in filtered:
                key = (
                    str(row.get("target_kind")),
                    str(row.get("target_ref")),
                    str(row.get("slot_ref") or ""),
                )
                current = latest.get(key)
                row_key = (
                    str(row.get("decided_at") or ""),
                    str(row.get("created_at") or ""),
                    str(row.get("review_decision_id") or ""),
                )
                current_key = (
                    str(current.get("decided_at") or ""),
                    str(current.get("created_at") or ""),
                    str(current.get("review_decision_id") or ""),
                ) if current else None
                if current is None or row_key > current_key:
                    latest[key] = row
            return list(latest.values())
        return []

    def fetchrow(self, query: str, *params: Any) -> dict[str, Any] | None:
        if "FROM app_manifests WHERE id = $1" in query:
            return self.manifest_rows.get(str(params[0]))
        stripped = query.strip()
        if stripped.startswith("INSERT INTO public.workflows"):
            workflow_id = str(params[0])
            row = dict(self.workflow_rows.get(workflow_id, {}))
            row.update(
                {
                    "id": workflow_id,
                    "name": params[1],
                    "description": params[2],
                    "definition": json.loads(params[3]),
                    "compiled_spec": json.loads(params[4]) if params[4] is not None else None,
                    "tags": list(params[5]),
                    "version": int(row.get("version") or 0) + 1,
                    "is_template": params[6],
                }
            )
            self.workflow_rows[workflow_id] = row
            self.executed.append((query, params))
            return row
        if stripped.startswith("UPDATE public.workflows"):
            workflow_id = str(params[0])
            workflow = self.workflow_rows.get(workflow_id)
            if workflow is None:
                self.executed.append((query, params))
                return None
            updated = dict(workflow)
            param_index = 1
            if "name =" in query:
                updated["name"] = params[param_index]
                param_index += 1
            if "description =" in query:
                updated["description"] = params[param_index]
                param_index += 1
            if "definition =" in query:
                updated["definition"] = json.loads(params[param_index])
                param_index += 1
            if "compiled_spec =" in query:
                compiled_param = params[param_index]
                updated["compiled_spec"] = json.loads(compiled_param) if compiled_param is not None else None
                param_index += 1
            if "tags =" in query:
                updated["tags"] = list(params[param_index])
                param_index += 1
            if "is_template =" in query:
                updated["is_template"] = params[param_index]
                param_index += 1
            updated["version"] = int(updated.get("version") or 0) + 1
            self.workflow_rows[workflow_id] = updated
            self.executed.append((query, params))
            return updated
        if stripped.startswith("INSERT INTO workflow_triggers"):
            source_trigger_id = None
            if len(params) == 7:
                source_trigger_id = params[2]
            row = {
                "id": params[0],
                "workflow_id": params[1],
                "source_trigger_id": source_trigger_id,
                "event_type": params[3] if len(params) == 7 else params[2],
                "filter": json.loads(params[4] if len(params) == 7 else params[3]),
                "enabled": params[5] if len(params) == 7 else params[4],
                "cron_expression": params[6] if len(params) == 7 else params[5],
            }
            self.trigger_rows.append(row)
            self.executed.append((query, params))
            return row
        if stripped.startswith("INSERT INTO workflow_build_review_decisions"):
            row = {
                "review_decision_id": params[0],
                "workflow_id": params[1],
                "definition_revision": params[2],
                "review_group_ref": params[3],
                "target_kind": params[4],
                "target_ref": params[5],
                "slot_ref": params[6],
                "decision": params[7],
                "actor_type": params[8],
                "actor_ref": params[9],
                "authority_scope": params[10],
                "approval_mode": params[11],
                "rationale": params[12],
                "source_subpath": params[13],
                "supersedes_decision_ref": params[14],
                "candidate_ref": params[15],
                "candidate_payload": json.loads(params[16]) if params[16] is not None else None,
                "decided_at": params[17],
                "created_at": params[17],
            }
            self.build_review_decision_rows.append(row)
            self.executed.append((query, params))
            return row
        if stripped.startswith("UPDATE workflow_triggers"):
            trigger_id = str(params[0])
            row = next((item for item in self.trigger_rows if str(item.get("id")) == trigger_id), None)
            self.executed.append((query, params))
            if row is None:
                return None
            updated = dict(row)
            param_index = 1
            if "workflow_id =" in query:
                updated["workflow_id"] = params[param_index]
                param_index += 1
            if "source_trigger_id =" in query:
                updated["source_trigger_id"] = params[param_index]
                param_index += 1
            if "event_type =" in query:
                updated["event_type"] = params[param_index]
                param_index += 1
            if "filter =" in query:
                updated["filter"] = json.loads(params[param_index])
                param_index += 1
            if "cron_expression =" in query:
                updated["cron_expression"] = params[param_index]
                param_index += 1
            if "enabled =" in query:
                updated["enabled"] = params[param_index]
                param_index += 1
            self.trigger_rows = [
                updated if str(item.get("id")) == trigger_id else item
                for item in self.trigger_rows
            ]
            return updated
        if "FROM workflow_build_review_decisions" in stripped and "LIMIT 1" in stripped:
            workflow_id = str(params[0])
            definition_revision = str(params[1])
            target_kind = str(params[2])
            target_ref = str(params[3])
            slot_ref = str(params[4]) if len(params) > 4 else None
            rows = [
                row
                for row in self.build_review_decision_rows
                if str(row.get("workflow_id")) == workflow_id
                and str(row.get("definition_revision")) == definition_revision
                and str(row.get("target_kind")) == target_kind
                and str(row.get("target_ref")) == target_ref
                and (slot_ref is None or str(row.get("slot_ref") or "") == slot_ref)
            ]
            rows.sort(
                key=lambda row: (
                    str(row.get("decided_at") or ""),
                    str(row.get("created_at") or ""),
                    str(row.get("review_decision_id") or ""),
                ),
                reverse=True,
            )
            self.executed.append((query, params))
            return rows[0] if rows else None
        rows = self.execute(query, *params)
        return rows[0] if rows else None

    def fetchval(self, query: str, *params: Any) -> Any:
        if "SELECT 1 FROM workflow_triggers WHERE id = $1" in query:
            return 1 if any(str(row.get("id")) == str(params[0]) for row in self.trigger_rows) else None
        row = self.fetchrow(query, *params)
        if row is None:
            return None
        if not isinstance(row, dict):
            return row
        return next(iter(row.values()), None)


class _MutableWorkflowPg(_RecordingPg):
    def fetchrow(self, query: str, *params: Any) -> dict[str, Any] | None:
        if query.strip().startswith("SELECT id, name, description, definition, compiled_spec, version, updated_at"):
            workflow = self.workflow_rows.get(str(params[0]))
            return workflow
        if query.strip().startswith("UPDATE public.workflows"):
            workflow_id = str(params[0])
            workflow = self.workflow_rows.get(workflow_id)
            if workflow is None:
                return None
            
            # Map parameters by parsing the UPDATE statement
            # Format: SET name = $2, description = $3, definition = $4::jsonb, compiled_spec = $5::jsonb ...
            assignments = re.findall(r"(\w+)\s*=\s*\$(\d+)", query)
            for field, param_idx_str in assignments:
                param_idx = int(param_idx_str) - 1
                value = params[param_idx]
                if field == "name":
                    workflow["name"] = value
                elif field == "description":
                    workflow["description"] = value
                elif field == "definition":
                    workflow["definition"] = json.loads(value) if isinstance(value, str) else value
                elif field == "compiled_spec":
                    workflow["compiled_spec"] = json.loads(value) if isinstance(value, str) else value
            
            workflow["version"] = int(workflow.get("version") or 1) + 1
            workflow["updated_at"] = "2026-04-09T20:00:00Z"
            return workflow
        return super().fetchrow(query, *params)

    def execute(self, query: str, *params: Any) -> list[dict[str, Any]]:
        if query.strip().startswith("DELETE FROM workflow_triggers WHERE workflow_id = $1"):
            self.trigger_rows = [row for row in self.trigger_rows if str(row.get("workflow_id")) != str(params[0])]
            return []
        if query.strip().startswith("INSERT INTO workflow_triggers"):
            source_trigger_id = None
            if len(params) == 7:
                source_trigger_id = params[2]
            self.trigger_rows.append(
                {
                    "id": params[0],
                    "workflow_id": params[1],
                    "source_trigger_id": source_trigger_id,
                    "event_type": params[3] if len(params) == 7 else params[2],
                    "filter": json.loads(params[4] if len(params) == 7 else params[3]),
                    "cron_expression": params[6] if len(params) == 7 else params[5],
                    "enabled": params[5] if len(params) == 7 else params[4],
                }
            )
            return []
        return super().execute(query, *params)


class _QueueStatusPg:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute(self, query: str, *params: Any) -> list[dict[str, Any]]:  # noqa: ARG002
        self.queries.append(query)
        if "FROM workflow_jobs" in query:
            return [
                {
                    "pending": 300,
                    "ready": 250,
                    "claimed": 12,
                    "running": 8,
                }
            ]
        raise AssertionError(f"unexpected query: {query}")


class _QueueStatusIngester:
    def load_recent(self, since_hours: int = 24):  # noqa: ARG002
        return [{"id": "r1"}, {"id": "r2"}]

    def compute_pass_rate(self, receipts):  # noqa: ARG002
        return 0.75

    def top_failure_codes(self, receipts):  # noqa: ARG002
        return {"TIMEOUT": 2}


class _QueueStatusSubsystems:
    def __init__(self) -> None:
        self._pg = _QueueStatusPg()
        self._ingester = _QueueStatusIngester()

    def get_pg_conn(self):
        return self._pg

    def get_receipt_ingester(self):
        return self._ingester


def test_handle_status_get_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    request = _RequestStub(subsystems=_QueueStatusSubsystems())

    def _execute(subsystems, *, operation_name: str, payload: dict[str, Any]):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"total_workflows": 2, "queue_depth": 550}

    monkeypatch.setattr(
        "runtime.operation_catalog_gateway.execute_operation_from_subsystems",
        _execute,
    )

    workflow_query._handle_status_get(request, "/api/status")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["total_workflows"] == 2
    assert payload["queue_depth"] == 550
    assert captured["operation_name"] == "operator.status_snapshot"
    assert captured["payload"] == {"since_hours": 24}


class _CriticalQueueStatusPg:
    def execute(self, query: str, *params: Any) -> list[dict[str, Any]]:
        normalized = " ".join(query.split())
        if "FROM workflow_jobs" in normalized:
            return [{"pending": 600, "ready": 400, "claimed": 1, "running": 2}]
        if "COUNT(*) AS total" in normalized:
            return [{"total": 0, "passed": 0, "failed": 0}]
        raise AssertionError(f"unexpected query: {query}")


class _CriticalQueueStatusSubsystems:
    def __init__(self) -> None:
        self._pg = _CriticalQueueStatusPg()
        self._ingester = _QueueStatusIngester()

    def get_pg_conn(self):
        return self._pg

    def get_receipt_ingester(self):
        return self._ingester


def test_handle_status_get_surfaces_gateway_errors(monkeypatch) -> None:
    request = _RequestStub(subsystems=_CriticalQueueStatusSubsystems())

    def _execute(*_args, **_kwargs):
        raise RuntimeError("status unavailable")

    monkeypatch.setattr(
        "runtime.operation_catalog_gateway.execute_operation_from_subsystems",
        _execute,
    )

    workflow_query._handle_status_get(request, "/api/status")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 500
    assert payload == {"error": "status unavailable"}


class _DashboardPg:
    def execute(self, query: str, *params: Any) -> list[dict[str, Any]]:
        if "FROM public.workflows w" in query:
            return [
                {
                    "id": "wf_live",
                    "name": "Support Intake",
                    "description": "Handle inbound support requests.",
                    "definition": {
                        "type": "operating_model",
                        "trigger_intent": [{"event_type": "schedule"}],
                    },
                    "compiled_spec": {"jobs": [{"label": "triage"}]},
                    "tags": [],
                    "version": 3,
                    "is_template": False,
                    "invocation_count": 12,
                    "last_invoked_at": datetime(2026, 4, 14, 11, 45, tzinfo=timezone.utc),
                    "created_at": datetime(2026, 4, 10, 9, 0, tzinfo=timezone.utc),
                    "updated_at": datetime(2026, 4, 14, 11, 50, tzinfo=timezone.utc),
                    "trigger_id": "trigger_live",
                    "trigger_event": "schedule",
                    "trigger_enabled": True,
                    "cron_expression": "@hourly",
                    "trigger_last_fired": datetime(2026, 4, 14, 11, 0, tzinfo=timezone.utc),
                    "trigger_fire_count": 8,
                    "latest_run_id": "run_live",
                    "latest_run_spec_name": "Support Intake",
                    "latest_run_status": "running",
                    "latest_run_total_jobs": 4,
                    "latest_run_created_at": datetime(2026, 4, 14, 11, 50, tzinfo=timezone.utc),
                    "latest_run_finished_at": None,
                    "latest_run_parent_run_id": None,
                    "latest_run_trigger_depth": 0,
                },
                {
                    "id": "wf_saved",
                    "name": "Daily Report",
                    "description": "Generate the daily report.",
                    "definition": {"type": "pipeline"},
                    "compiled_spec": {"jobs": [{"label": "report"}]},
                    "tags": [],
                    "version": 2,
                    "is_template": False,
                    "invocation_count": 3,
                    "last_invoked_at": datetime(2026, 4, 13, 18, 0, tzinfo=timezone.utc),
                    "created_at": datetime(2026, 4, 11, 9, 0, tzinfo=timezone.utc),
                    "updated_at": datetime(2026, 4, 13, 18, 0, tzinfo=timezone.utc),
                    "trigger_id": None,
                    "trigger_event": None,
                    "trigger_enabled": None,
                    "cron_expression": None,
                    "trigger_last_fired": None,
                    "trigger_fire_count": 0,
                    "latest_run_id": "run_saved",
                    "latest_run_spec_name": "Daily Report",
                    "latest_run_status": "succeeded",
                    "latest_run_total_jobs": 1,
                    "latest_run_created_at": datetime(2026, 4, 13, 18, 0, tzinfo=timezone.utc),
                    "latest_run_finished_at": datetime(2026, 4, 13, 18, 1, tzinfo=timezone.utc),
                    "latest_run_parent_run_id": None,
                    "latest_run_trigger_depth": 0,
                },
                {
                    "id": "wf_draft",
                    "name": "Unlaunched Draft",
                    "description": "A workflow draft.",
                    "definition": {"type": "pipeline"},
                    "compiled_spec": None,
                    "tags": [],
                    "version": 1,
                    "is_template": False,
                    "invocation_count": 0,
                    "last_invoked_at": None,
                    "created_at": datetime(2026, 4, 14, 9, 0, tzinfo=timezone.utc),
                    "updated_at": datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc),
                    "trigger_id": None,
                    "trigger_event": None,
                    "trigger_enabled": None,
                    "cron_expression": None,
                    "trigger_last_fired": None,
                    "trigger_fire_count": 0,
                    "latest_run_id": None,
                    "latest_run_spec_name": None,
                    "latest_run_status": None,
                    "latest_run_total_jobs": None,
                    "latest_run_created_at": None,
                    "latest_run_finished_at": None,
                    "latest_run_parent_run_id": None,
                    "latest_run_trigger_depth": None,
                },
            ]
        if "FROM workflow_jobs" in query:
            return [{"pending": 2, "ready": 1, "claimed": 0, "running": 0}]
        if "FROM public.workflow_runs r" in query and "GROUP BY r.run_id" in query:
            return [
                {
                    "run_id": "run_live",
                    "spec_name": "Support Intake",
                    "status": "running",
                    "total_jobs": 4,
                    "created_at": datetime(2026, 4, 14, 11, 50, tzinfo=timezone.utc),
                    "finished_at": None,
                    "completed_jobs": 2,
                    "total_cost": 3.5,
                },
                {
                    "run_id": "run_saved",
                    "spec_name": "Daily Report",
                    "status": "succeeded",
                    "total_jobs": 1,
                    "created_at": datetime(2026, 4, 13, 18, 0, tzinfo=timezone.utc),
                    "finished_at": datetime(2026, 4, 13, 18, 1, tzinfo=timezone.utc),
                    "completed_jobs": 1,
                    "total_cost": 1.25,
                },
            ]
        raise AssertionError(f"unexpected query: {query}")


class _DashboardIngester:
    def load_recent(self, since_hours: int = 24):  # noqa: ARG002
        return [
            {"agent_slug": "openai/gpt-5.4", "status": "succeeded", "cost_usd": 4.25},
            {"agent_slug": "openai/gpt-5.4", "status": "succeeded", "cost_usd": 2.0},
            {"agent_slug": "anthropic/claude-3.7", "status": "failed", "cost_usd": 1.5},
        ]

    def compute_pass_rate(self, receipts):  # noqa: ARG002
        return 0.9

    def top_failure_codes(self, receipts):  # noqa: ARG002
        return {"TIMEOUT": 1}


class _DashboardSubsystems:
    def __init__(self) -> None:
        self._pg = _DashboardPg()
        self._ingester = _DashboardIngester()

    def get_pg_conn(self):
        return self._pg

    def get_receipt_ingester(self):
        return self._ingester


def test_handle_dashboard_get_returns_backend_authored_counts_and_health() -> None:
    request = _RequestStub(subsystems=_DashboardSubsystems(), path="/api/dashboard")

    workflow_query._handle_dashboard_get(request, "/api/dashboard")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["summary"]["workflow_counts"] == {
        "total": 3,
        "live": 1,
        "saved": 1,
        "draft": 1,
    }
    assert payload["summary"]["health"] == {
        "readiness": "healthy",
        "label": "Healthy",
        "tone": "healthy",
        "copy": "Recent workflow outcomes are strong and the control plane looks settled.",
    }
    assert payload["summary"]["runs_24h"] == 3
    assert payload["summary"]["active_runs"] == 1
    assert payload["summary"]["pass_rate_24h"] == 0.9
    assert payload["summary"]["total_cost_24h"] == 7.75
    assert payload["summary"]["top_agent"] == "openai/gpt-5.4"
    assert payload["summary"]["models_online"] == 2
    assert payload["summary"]["queue"] == {
        "depth": 3,
        "status": "ok",
        "utilization_pct": 0.3,
        "pending": 2,
        "ready": 1,
        "claimed": 0,
        "running": 0,
        "error": None,
    }
    assert payload["sections"] == [
        {"key": "live", "count": 1, "workflow_ids": ["wf_live"]},
        {"key": "saved", "count": 1, "workflow_ids": ["wf_saved"]},
        {"key": "draft", "count": 1, "workflow_ids": ["wf_draft"]},
    ]
    workflows = {workflow["id"]: workflow for workflow in payload["workflows"]}
    assert workflows["wf_live"]["dashboard_bucket"] == "live"
    assert workflows["wf_live"]["dashboard_badge"]["label"] == "Scheduled"
    assert workflows["wf_saved"]["dashboard_bucket"] == "saved"
    assert workflows["wf_saved"]["dashboard_badge"]["label"] == "Validated"
    assert workflows["wf_draft"]["dashboard_bucket"] == "draft"
    assert workflows["wf_draft"]["dashboard_badge"]["label"] == "Draft"
    assert payload["recent_runs"][0]["run_id"] == "run_live"


def test_handle_heal_infers_failure_code_from_stderr() -> None:
    subs = SimpleNamespace(get_self_healer=lambda: SelfHealingOrchestrator())

    payload = workflow_query_core.handle_heal(
        subs,
        {
            "job_label": "phase_010_operator_control_authority",
            "stderr": "TypeError: failure_code must be a non-empty string",
        },
    )

    assert payload["action"] == "fix_and_retry"
    assert payload["resolved_failure_code"] == "orchestration.failure_code_missing"


def test_deprecated_routes_return_404() -> None:
    for path in ["/api/compile", "/api/refine-definition", "/api/plan", "/api/commit"]:
        request = _RequestStub(
            {"prose": "test"},
            subsystems=SimpleNamespace(get_pg_conn=lambda: object()),
            path=path,
        )
        handled = api_handlers.handle_post_request(request, path)
        # Should be False because the route is no longer in QUERY_POST_ROUTES
        assert handled is False, f"Route {path} should not be handled"


def test_handle_post_request_records_query_surface_usage(monkeypatch) -> None:
    request = _RequestStub({"question": "status"}, subsystems=SimpleNamespace(), path="/query")
    recorded: list[dict[str, Any]] = []

    monkeypatch.setattr(
        workflow_query,
        "_record_api_route_usage",
        lambda _subs, **kwargs: recorded.append(kwargs),
    )

    handled = api_handlers.handle_post_request(request, "/query")

    assert handled is True
    assert request.sent == (
        410,
        {
            "error": "/query is gone. Use praxis workflow query/discover/recall/tools or workflow-scoped build surfaces instead.",
            "replacement": "praxis workflow query|discover|recall|tools",
        },
    )
    assert len(recorded) == 1
    assert recorded[0]["path"] == "/query"
    assert recorded[0]["method"] == "POST"
    assert recorded[0]["status_code"] == 410
    assert recorded[0]["request_body"] == {"question": "status"}
    assert recorded[0]["response_payload"]["replacement"] == "praxis workflow query|discover|recall|tools"
    assert recorded[0]["headers"] == request.headers


def test_handle_trigger_post_records_surface_usage_on_success(monkeypatch) -> None:
    pg = _RecordingPg(
        workflow_rows={
            "wf_123": {
                "id": "wf_123",
                "definition": {"definition_revision": "def_trigger_usage"},
                "compiled_spec": {"jobs": [{"label": "review"}], "triggers": []},
            }
        }
    )
    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/trigger/wf_123",
    )
    recorded: list[dict[str, Any]] = []

    monkeypatch.setattr(
        workflow_query,
        "_record_api_route_usage",
        lambda _subs, **kwargs: recorded.append(kwargs),
    )
    monkeypatch.setattr(
        workflow_query,
        "trigger_workflow_manually",
        lambda *_args, **_kwargs: {
            "triggered": True,
            "workflow_id": "wf_123",
            "workflow_name": "Inbox Triage",
            "run_id": "run_123",
        },
    )

    workflow_query._handle_trigger_post(request, "/api/trigger/wf_123")

    assert request.sent == (
        200,
        {
            "triggered": True,
            "workflow_id": "wf_123",
            "workflow_name": "Inbox Triage",
            "run_id": "run_123",
        },
    )
    assert len(recorded) == 1
    assert recorded[0]["path"] == "/api/trigger/wf_123"
    assert recorded[0]["status_code"] == 200
    assert recorded[0]["conn"] is pg
    assert recorded[0]["response_payload"]["run_id"] == "run_123"


def test_handle_workflow_build_post_bootstrap_compiles_into_workflow_build_payload() -> None:
    workflow_row = {
        "id": "wf_build_bootstrap",
        "name": "Bootstrap Draft",
        "description": "Bootstrap Draft",
        "definition": {},
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-15T10:00:00Z",
    }
    pg = _MutableWorkflowPg(workflow_rows={"wf_build_bootstrap": workflow_row})
    request = _RequestStub(
        {"prose": "Review support inbox", "title": "Bootstrap Draft"},
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_bootstrap/build/bootstrap",
    )

    with patch(
        "runtime.compiler.compile_prose",
        return_value={
            "definition": {
                "type": "operating_model",
                "source_prose": "Review support inbox",
                "compiled_prose": "Review support inbox",
                "references": [],
                "capabilities": [],
                "trigger_intent": [],
                "draft_flow": [],
                "definition_revision": "def_bootstrap_001",
            }
        },
    ) as compile_mock:
        workflow_query._handle_workflow_build_post(
            request,
            "/api/workflows/wf_build_bootstrap/build/bootstrap",
        )

    compile_mock.assert_called_once()
    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["workflow"]["id"] == "wf_build_bootstrap"
    assert payload["definition"]["workflow_id"] == "wf_build_bootstrap"


def test_handle_workflow_build_post_harden_uses_workflow_scoped_build_state() -> None:
    workflow_row = {
        "id": "wf_build_harden",
        "name": "Graph Draft",
        "description": "Graph Draft",
        "definition": {},
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-15T10:00:00Z",
    }
    pg = _MutableWorkflowPg(workflow_rows={"wf_build_harden": workflow_row})
    request = _RequestStub(
        {
            "title": "Graph Draft",
            "build_graph": {
                "nodes": [
                    {
                        "node_id": "trigger-001",
                        "kind": "step",
                        "title": "Manual",
                        "route": "trigger",
                        "trigger": {"event_type": "manual", "filter": {}},
                    },
                    {
                        "node_id": "step-001",
                        "kind": "step",
                        "title": "Fetch status",
                        "route": "@webhook/post",
                        "integration_args": {
                            "request_preset": "fetch_json",
                            "url": "https://api.example.com/status",
                            "method": "GET",
                        },
                    },
                ],
                "edges": [
                    {
                        "edge_id": "edge-trigger-step",
                        "kind": "sequence",
                        "from_node_id": "trigger-001",
                        "to_node_id": "step-001",
                    }
                ],
            },
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_harden/build/harden",
    )

    workflow_query._handle_workflow_build_post(
        request,
        "/api/workflows/wf_build_harden/build/harden",
    )

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["workflow"]["id"] == "wf_build_harden"
    assert payload["definition"]["workflow_id"] == "wf_build_harden"
    assert {node["node_id"] for node in payload["build_graph"]["nodes"]} == {"trigger-001", "step-001"}


def test_handle_workflow_build_get_returns_authority_bundle() -> None:
    workflow_row = {
        "id": "wf_build_123",
        "name": "Support Intake",
        "description": "Compile support intake",
        "definition": {
            "type": "operating_model",
            "source_prose": "Research #ticket/status and triage support issues",
            "compiled_prose": "Research #ticket/status and triage support issues",
            "narrative_blocks": [
                {
                    "id": "block-001",
                    "title": "Research ticket state",
                    "summary": "Research ticket state before acting.",
                    "text": "Research ticket state before acting.",
                    "order": 1,
                    "reference_slugs": ["#ticket/status"],
                    "capability_slugs": [],
                }
            ],
            "references": [
                {
                    "id": "ref-001",
                    "type": "object",
                    "slug": "#ticket/status",
                    "raw": "#ticket/status",
                    "resolved": False,
                    "resolved_to": None,
                }
            ],
            "capabilities": [],
            "authority": "",
            "sla": {},
            "trigger_intent": [],
            "draft_flow": [
                {
                    "id": "step-001",
                    "title": "Research ticket state",
                    "summary": "Research ticket state before acting.",
                    "source_block_ids": ["block-001"],
                    "reference_slugs": ["#ticket/status"],
                    "capability_slugs": [],
                    "depends_on": [],
                    "order": 1,
                }
            ],
            "definition_revision": "def_build_get",
        },
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-09T19:00:00Z",
    }
    pg = _RecordingPg(workflow_rows={"wf_build_123": workflow_row})
    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_123/build",
    )

    workflow_query._handle_workflow_build_get(request, "/api/workflows/wf_build_123/build")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["workflow"]["id"] == "wf_build_123"
    assert payload["build_state"] == "blocked"
    assert any(node["node_id"] == "step-001" for node in payload["build_graph"]["nodes"])
    assert payload["binding_ledger"][0]["binding_id"] == "binding:ref-001"
    assert payload["compiled_spec_projection"] is None


def test_handle_workflow_build_get_surfaces_trigger_projection() -> None:
    workflow_row = {
        "id": "wf_build_trigger",
        "name": "Inbox Triage",
        "description": "Compile inbox triage",
        "definition": {
            "type": "operating_model",
            "source_prose": "triage-agent reviews the support inbox.",
            "compiled_prose": "triage-agent reviews the support inbox.",
            "narrative_blocks": [
                {
                    "id": "block-001",
                    "title": "Review support inbox",
                    "summary": "triage-agent reviews the support inbox.",
                    "text": "triage-agent reviews the support inbox.",
                    "order": 1,
                    "reference_slugs": ["triage-agent"],
                    "capability_slugs": [],
                }
            ],
            "references": [
                {
                    "id": "ref-001",
                    "type": "agent",
                    "slug": "triage-agent",
                    "raw": "triage-agent",
                    "resolved": True,
                    "resolved_to": "task_type_routing:auto/review",
                    "config": {"route": "auto/review"},
                }
            ],
            "capabilities": [],
            "authority": "",
            "sla": {},
            "trigger_intent": [
                {
                    "id": "trigger-001",
                    "title": "Email received",
                    "summary": "Start when a new support email arrives.",
                    "event_type": "email.received",
                    "filter": {"mailbox": "support"},
                    "source_ref": "@gmail/search",
                }
            ],
            "draft_flow": [
                {
                    "id": "step-001",
                    "title": "Review support inbox",
                    "summary": "triage-agent reviews the support inbox.",
                    "source_block_ids": ["block-001"],
                    "reference_slugs": ["triage-agent"],
                    "capability_slugs": [],
                    "depends_on": [],
                    "order": 1,
                }
            ],
            "definition_revision": "def_build_trigger",
        },
        "compiled_spec": {
            "name": "Inbox Triage",
            "definition_revision": "def_build_trigger",
            "plan_revision": "plan_build_trigger",
            "jobs": [
                {
                    "label": "Review support inbox",
                    "agent": "auto/review",
                    "prompt": "Review support inbox",
                    "source_step_id": "step-001",
                    "source_node_id": "step-001",
                }
            ],
            "triggers": [
                {
                    "event_type": "email.received",
                    "filter": {"mailbox": "support"},
                    "source_trigger_id": "trigger-001",
                    "source_ref": "@gmail/search",
                }
            ],
        },
        "version": 1,
        "updated_at": "2026-04-09T19:00:00Z",
    }
    pg = _RecordingPg(workflow_rows={"wf_build_trigger": workflow_row})
    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_trigger/build",
    )

    workflow_query._handle_workflow_build_get(request, "/api/workflows/wf_build_trigger/build")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["build_state"] == "blocked"
    trigger_nodes = [node for node in payload["build_graph"]["nodes"] if node.get("route") == "trigger"]
    assert trigger_nodes
    assert trigger_nodes[0]["summary"] == "Start when a new support email arrives."
    assert trigger_nodes[0]["trigger"] == {
        "event_type": "email.received",
        "cron_expression": "",
        "source_ref": "@gmail/search",
        "filter": {"mailbox": "support"},
    }
    assert payload["binding_ledger"][0]["state"] == "suggested"
    assert payload["binding_ledger"][0]["accepted_target"] is None
    assert payload["compiled_spec_projection"]["graph_id"] == payload["build_graph"]["graph_id"]
    assert payload["compiled_spec_projection"]["compiled_spec"]["triggers"] == [
        {
            "event_type": "email.received",
            "filter": {"mailbox": "support"},
            "source_trigger_id": "trigger-001",
            "source_ref": "@gmail/search",
        }
    ]


def test_handle_workflow_build_get_overlays_db_review_decisions_over_definition_shadow() -> None:
    workflow_row = {
        "id": "wf_build_review_overlay",
        "name": "Support Intake",
        "description": "Compile support intake",
        "definition": {
            "type": "operating_model",
            "source_prose": "triage-agent reviews the support inbox.",
            "compiled_prose": "triage-agent reviews the support inbox.",
            "narrative_blocks": [],
            "references": [
                {
                    "id": "ref-001",
                    "type": "integration",
                    "slug": "triage-agent",
                    "raw": "triage-agent",
                    "resolved": True,
                    "resolved_to": "task_type_routing:auto/review",
                }
            ],
            "capabilities": [],
            "authority": "",
            "sla": {},
            "trigger_intent": [],
            "draft_flow": [
                {
                    "id": "step-001",
                    "title": "Review support inbox",
                    "summary": "triage-agent reviews the support inbox.",
                    "source_block_ids": [],
                    "reference_slugs": ["triage-agent"],
                    "depends_on": [],
                    "order": 1,
                }
            ],
            "binding_ledger": [
                {
                    "binding_id": "binding:ref-001",
                    "source_kind": "reference",
                    "source_label": "triage-agent",
                    "source_span": None,
                    "source_node_ids": ["step-001"],
                    "state": "captured",
                    "candidate_targets": [
                        {
                            "target_ref": "task_type_routing:auto/review",
                            "label": "Auto Review",
                            "kind": "agent",
                        }
                    ],
                    "accepted_target": None,
                    "rationale": "Needs an accepted authority target before planning can run cleanly.",
                    "created_at": "2026-04-09T19:00:00+00:00",
                    "updated_at": "2026-04-09T19:00:00+00:00",
                    "freshness": None,
                }
            ],
            "definition_revision": "def_build_review_overlay",
        },
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-09T19:00:00Z",
    }
    pg = _RecordingPg(
        workflow_rows={"wf_build_review_overlay": workflow_row},
        build_review_decision_rows=[
            {
                "review_decision_id": "wbrd_001",
                "workflow_id": "wf_build_review_overlay",
                "definition_revision": "def_build_review_overlay",
                "target_kind": "binding",
                "target_ref": "binding:ref-001",
                "decision": "approve",
                "actor_type": "human",
                "actor_ref": "build_workspace",
                "approval_mode": "manual",
                "rationale": "Approved explicitly.",
                "source_subpath": "bindings/binding:ref-001/accept",
                "candidate_ref": "task_type_routing:auto/review",
                "candidate_payload": {
                    "target_ref": "task_type_routing:auto/review",
                    "label": "Auto Review",
                    "kind": "agent",
                },
                "decided_at": "2026-04-09T19:05:00Z",
                "created_at": "2026-04-09T19:05:00Z",
            }
        ],
    )
    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_review_overlay/build",
    )

    workflow_query._handle_workflow_build_get(request, "/api/workflows/wf_build_review_overlay/build")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    binding = next(entry for entry in payload["binding_ledger"] if entry["binding_id"] == "binding:ref-001")
    assert binding["state"] == "accepted"
    assert binding["accepted_target"]["target_ref"] == "task_type_routing:auto/review"
    assert payload["build_state"] == "ready"
    assert payload["candidate_resolution_manifest"]["execution_readiness"] == "review_required"
    manifest_slot = payload["candidate_resolution_manifest"]["binding_slots"][0]
    assert manifest_slot["approval_state"] == "approved"
    assert manifest_slot["approved_ref"] == "task_type_routing:auto/review"
    assert any(
        item["slot_ref"] == "workflow_shape"
        for item in payload["candidate_resolution_manifest"]["required_confirmations"]
    )
    assert payload["reviewable_plan"]["approved_binding_refs"] == [
        {
            "slot_ref": "binding:ref-001",
            "candidate_ref": "task_type_routing:auto/review",
        }
    ]
    assert payload["reviewable_plan"]["approval_records"][0]["approved_by"] == "build_workspace"


def test_handle_workflow_build_post_persists_attachment_and_replans() -> None:
    workflow_row = {
        "id": "wf_build_mutation",
        "name": "Support Intake",
        "description": "Compile support intake",
        "definition": {
            "type": "operating_model",
            "source_prose": "triage-agent reviews the support inbox.",
            "compiled_prose": "triage-agent reviews the support inbox.",
            "narrative_blocks": [
                {
                    "id": "block-001",
                    "title": "Review support inbox",
                    "summary": "triage-agent reviews the support inbox.",
                    "text": "triage-agent reviews the support inbox.",
                    "order": 1,
                    "reference_slugs": ["triage-agent"],
                    "capability_slugs": [],
                }
            ],
            "references": [
                {
                    "id": "ref-001",
                    "type": "agent",
                    "slug": "triage-agent",
                    "raw": "triage-agent",
                    "resolved": True,
                    "resolved_to": "task_type_routing:auto/review",
                    "config": {"route": "auto/review"},
                }
            ],
            "capabilities": [],
            "authority": "",
            "sla": {},
            "trigger_intent": [],
            "draft_flow": [
                {
                    "id": "step-001",
                    "title": "Review support inbox",
                    "summary": "triage-agent reviews the support inbox.",
                    "source_block_ids": ["block-001"],
                    "reference_slugs": ["triage-agent"],
                    "capability_slugs": [],
                    "depends_on": [],
                    "order": 1,
                }
            ],
            "definition_revision": "def_build_mutation",
        },
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-09T19:00:00Z",
    }
    pg = _MutableWorkflowPg(workflow_rows={"wf_build_mutation": workflow_row})
    request = _RequestStub(
        {
            "node_id": "step-001",
            "authority_kind": "reference",
            "authority_ref": "@gmail/search",
            "role": "input",
            "label": "Gmail Search",
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_mutation/build/attachments",
    )

    workflow_query._handle_workflow_build_post(request, "/api/workflows/wf_build_mutation/build/attachments")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["workflow"]["id"] == "wf_build_mutation"
    assert payload["authority_attachments"][0]["authority_ref"] == "@gmail/search"
    assert payload["mutation_event_id"] == 1
    assert payload["undo_receipt"] == {
        "workflow_id": "wf_build_mutation",
        "steps": [
            {
                "subpath": f"attachments/{payload['authority_attachments'][0]['attachment_id']}/restore",
                "body": {"attachment": None},
            }
        ],
    }
    mutation_event = next(row for row in pg.event_log_rows if row["emitted_by"] == "mutate_workflow_build")
    assert mutation_event == {
        "id": 1,
        "channel": "build_state",
        "event_type": "mutation",
        "entity_id": "wf_build_mutation",
        "entity_kind": "workflow",
        "payload": {
            "subpath": "attachments",
            "undo_receipt": payload["undo_receipt"],
        },
        "emitted_by": "mutate_workflow_build",
    }
    assert payload["build_state"] == "blocked"
    assert payload["compiled_spec"] is None
    assert payload["compiled_spec_projection"] is None
    assert payload["binding_ledger"][0]["state"] == "suggested"
    persisted_definition = pg.workflow_rows["wf_build_mutation"]["definition"]
    assert persisted_definition["authority_attachments"][0]["authority_ref"] == "@gmail/search"


def test_handle_workflow_build_post_rejects_legacy_binding_accept_alias() -> None:
    workflow_row = {
        "id": "wf_build_binding_alias",
        "name": "Support Intake",
        "description": "Compile support intake",
        "definition": {
            "type": "operating_model",
            "definition_revision": "def_build_binding_alias",
            "binding_ledger": [],
        },
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-09T19:00:00Z",
    }
    pg = _MutableWorkflowPg(workflow_rows={"wf_build_binding_alias": workflow_row})
    request = _RequestStub(
        {"accepted_target": {"target_ref": "task_type_routing:auto/review"}},
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_binding_alias/build/bindings/binding:ref-001/accept",
    )

    workflow_query._handle_workflow_build_post(
        request,
        "/api/workflows/wf_build_binding_alias/build/bindings/binding:ref-001/accept",
    )

    assert request.sent == (
        410,
        {
            "error": "Legacy binding approval aliases are gone. Use /api/workflows/{workflow_id}/build/review_decisions.",
        },
    )


def test_handle_workflow_build_post_rejects_legacy_import_admit_alias() -> None:
    workflow_row = {
        "id": "wf_build_import_alias",
        "name": "Support Intake",
        "description": "Compile support intake",
        "definition": {
            "type": "operating_model",
            "definition_revision": "def_build_import_alias",
            "import_snapshots": [],
        },
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-09T19:00:00Z",
    }
    pg = _MutableWorkflowPg(workflow_rows={"wf_build_import_alias": workflow_row})
    request = _RequestStub(
        {"admitted_target": {"target_ref": "#escalation-policy"}},
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_import_alias/build/imports/import_001/admit",
    )

    workflow_query._handle_workflow_build_post(
        request,
        "/api/workflows/wf_build_import_alias/build/imports/import_001/admit",
    )

    assert request.sent == (
        410,
        {
            "error": "Legacy import admission aliases are gone. Use /api/workflows/{workflow_id}/build/review_decisions.",
        },
    )


def test_handle_workflow_build_post_accept_binding_emits_restore_receipt() -> None:
    binding_id = "binding:ref-001"
    workflow_row = {
        "id": "wf_build_binding_accept",
        "name": "Support Intake",
        "description": "Compile support intake",
        "definition": {
            "type": "operating_model",
            "source_prose": "triage-agent reviews the support inbox.",
            "compiled_prose": "triage-agent reviews the support inbox.",
            "narrative_blocks": [],
            "references": [],
            "capabilities": [],
            "authority": "",
            "sla": {},
            "trigger_intent": [],
            "draft_flow": [
                {
                    "id": "step-001",
                    "title": "Review support inbox",
                    "summary": "triage-agent reviews the support inbox.",
                    "source_block_ids": [],
                    "depends_on": [],
                    "order": 1,
                }
            ],
            "binding_ledger": [
                {
                    "binding_id": binding_id,
                    "source_kind": "reference",
                    "source_label": "triage-agent",
                    "source_span": None,
                    "source_node_ids": ["step-001"],
                    "state": "captured",
                    "candidate_targets": [
                        {
                            "target_ref": "task_type_routing:auto/review",
                            "label": "Auto Review",
                            "kind": "agent",
                        }
                    ],
                    "accepted_target": None,
                    "rationale": "Needs an accepted authority target before planning can run cleanly.",
                    "created_at": "2026-04-09T19:00:00+00:00",
                    "updated_at": "2026-04-09T19:00:00+00:00",
                    "freshness": None,
                }
            ],
            "definition_revision": "def_build_binding_accept",
        },
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-09T19:00:00Z",
    }
    pg = _MutableWorkflowPg(workflow_rows={"wf_build_binding_accept": workflow_row})
    request = _RequestStub(
        {
            "target_kind": "binding",
            "target_ref": binding_id,
            "decision": "approve",
            "candidate_payload": {
                "target_ref": "task_type_routing:auto/review",
                "label": "Auto Review",
                "kind": "agent",
            },
            "rationale": "Accepted in build workspace.",
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_binding_accept/build/review_decisions",
    )

    workflow_query._handle_workflow_build_post(
        request,
        "/api/workflows/wf_build_binding_accept/build/review_decisions",
    )

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    accepted = next(entry for entry in payload["binding_ledger"] if entry["binding_id"] == binding_id)
    assert accepted["state"] == "accepted"
    review_event = next(
        params
        for query, params in pg.executed
        if "INSERT INTO event_log" in query and params[1] == "review_decision"
    )
    review_row = next(
        params
        for query, params in pg.executed
        if "INSERT INTO workflow_build_review_decisions" in query
    )
    review_payload = json.loads(review_event[4])
    assert review_payload["target_kind"] == "binding"
    assert review_payload["target_ref"] == binding_id
    assert review_payload["decision"] == "approve"
    assert review_payload["candidate_ref"] == "task_type_routing:auto/review"
    assert review_payload["actor_type"] == "human"
    assert review_row[4] == "binding"
    assert review_row[5] == binding_id
    assert review_row[7] == "approve"
    assert payload["undo_receipt"] == {
        "workflow_id": "wf_build_binding_accept",
        "steps": [
            {
                "subpath": "review_decisions",
                "body": {
                    "target_kind": "binding",
                    "target_ref": binding_id,
                    "slot_ref": binding_id,
                    "decision": "revoke",
                    "approval_mode": "undo_restore",
                    "rationale": "Undo restore to the prior unapproved build-review state.",
                },
            }
        ],
    }
    persisted_definition = pg.workflow_rows["wf_build_binding_accept"]["definition"]
    persisted_binding = next(entry for entry in persisted_definition["binding_ledger"] if entry["binding_id"] == binding_id)
    assert persisted_binding["accepted_target"] is None
    assert persisted_binding["state"] == "suggested"


def test_handle_workflow_build_post_records_proposal_request_without_binding_acceptance() -> None:
    binding_id = "binding:ref-001"
    workflow_row = {
        "id": "wf_build_binding_proposal",
        "name": "Support Intake",
        "description": "Compile support intake",
        "definition": {
            "type": "operating_model",
            "source_prose": "triage-agent reviews the support inbox.",
            "compiled_prose": "triage-agent reviews the support inbox.",
            "narrative_blocks": [],
            "references": [],
            "capabilities": [],
            "authority": "",
            "sla": {},
            "trigger_intent": [],
            "draft_flow": [
                {
                    "id": "step-001",
                    "title": "Review support inbox",
                    "summary": "triage-agent reviews the support inbox.",
                    "source_block_ids": [],
                    "depends_on": [],
                    "order": 1,
                }
            ],
            "binding_ledger": [
                {
                    "binding_id": binding_id,
                    "source_kind": "reference",
                    "source_label": "triage-agent",
                    "source_span": None,
                    "source_node_ids": ["step-001"],
                    "state": "captured",
                    "candidate_targets": [
                        {
                            "target_ref": "task_type_routing:auto/review",
                            "label": "Auto Review",
                            "kind": "agent",
                        }
                    ],
                    "accepted_target": None,
                    "rationale": "Needs an accepted authority target before planning can run cleanly.",
                    "created_at": "2026-04-09T19:00:00+00:00",
                    "updated_at": "2026-04-09T19:00:00+00:00",
                    "freshness": None,
                }
            ],
            "definition_revision": "def_build_binding_proposal",
        },
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-09T19:00:00Z",
    }
    pg = _MutableWorkflowPg(workflow_rows={"wf_build_binding_proposal": workflow_row})
    request = _RequestStub(
        {
            "target_kind": "binding",
            "target_ref": binding_id,
            "decision": "proposal_request",
            "candidate_payload": {
                "target_ref": "task_type_routing:auto/escalate",
                "label": "Auto Escalate",
                "kind": "agent",
            },
            "rationale": "Please surface the escalation route as an alternate candidate.",
            "review_actor_type": "model",
            "review_actor_ref": "planner-agent",
            "approval_mode": "review",
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_binding_proposal/build/review_decisions",
    )

    workflow_query._handle_workflow_build_post(
        request,
        "/api/workflows/wf_build_binding_proposal/build/review_decisions",
    )

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    binding = next(entry for entry in payload["binding_ledger"] if entry["binding_id"] == binding_id)
    assert binding["accepted_target"] is None
    manifest_slot = next(
        entry
        for entry in payload["candidate_resolution_manifest"]["binding_slots"]
        if entry["slot_ref"] == binding_id
    )
    assert manifest_slot["approval_state"] == "unapproved"
    review_row = next(
        params
        for query, params in pg.executed
        if "INSERT INTO workflow_build_review_decisions" in query
    )
    assert review_row[7] == "proposal_request"
    proposal_request = payload["reviewable_plan"]["proposal_requests"][0]
    assert proposal_request["target_kind"] == "binding"
    assert proposal_request["target_ref"] == binding_id
    assert proposal_request["candidate_ref"] == "task_type_routing:auto/escalate"
    assert proposal_request["requested_by"] == {
        "actor_type": "model",
        "actor_ref": "planner-agent",
    }
    assert proposal_request["rationale"] == "Please surface the escalation route as an alternate candidate."


def test_handle_workflow_build_post_restores_attachment_record() -> None:
    workflow_row = {
        "id": "wf_build_attachment_restore",
        "name": "Support Intake",
        "description": "Compile support intake",
        "definition": {
            "type": "operating_model",
            "source_prose": "triage-agent reviews the support inbox.",
            "compiled_prose": "triage-agent reviews the support inbox.",
            "narrative_blocks": [],
            "references": [],
            "capabilities": [],
            "authority": "",
            "sla": {},
            "trigger_intent": [],
            "draft_flow": [
                {
                    "id": "step-001",
                    "title": "Review support inbox",
                    "summary": "triage-agent reviews the support inbox.",
                    "source_block_ids": [],
                    "depends_on": [],
                    "order": 1,
                }
            ],
            "authority_attachments": [
                {
                    "attachment_id": "attachment_001",
                    "node_id": "step-001",
                    "authority_kind": "reference",
                    "authority_ref": "@gmail/search",
                    "role": "input",
                    "label": "Gmail Search",
                    "promote_to_state": False,
                    "state_node_id": None,
                }
            ],
            "definition_revision": "def_build_attachment_restore",
        },
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-09T19:00:00Z",
    }
    pg = _MutableWorkflowPg(workflow_rows={"wf_build_attachment_restore": workflow_row})
    request = _RequestStub(
        {"attachment": None},
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_attachment_restore/build/attachments/attachment_001/restore",
    )

    workflow_query._handle_workflow_build_post(
        request,
        "/api/workflows/wf_build_attachment_restore/build/attachments/attachment_001/restore",
    )

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["authority_attachments"] == []
    persisted_definition = pg.workflow_rows["wf_build_attachment_restore"]["definition"]
    assert persisted_definition["authority_attachments"] == []


def test_handle_workflow_build_post_review_restore_revokes_binding_approval() -> None:
    binding_id = "binding:ref-001"
    workflow_row = {
        "id": "wf_build_review_restore",
        "name": "Support Intake",
        "description": "Compile support intake",
        "definition": {
            "type": "operating_model",
            "source_prose": "triage-agent reviews the support inbox.",
            "compiled_prose": "triage-agent reviews the support inbox.",
            "narrative_blocks": [],
            "references": [
                {
                    "id": "ref-001",
                    "type": "integration",
                    "slug": "triage-agent",
                    "raw": "triage-agent",
                    "resolved": True,
                    "resolved_to": "task_type_routing:auto/review",
                }
            ],
            "capabilities": [],
            "authority": "",
            "sla": {},
            "trigger_intent": [],
            "draft_flow": [
                {
                    "id": "step-001",
                    "title": "Review support inbox",
                    "summary": "triage-agent reviews the support inbox.",
                    "source_block_ids": [],
                    "reference_slugs": ["triage-agent"],
                    "depends_on": [],
                    "order": 1,
                }
            ],
            "definition_revision": "def_build_review_restore",
        },
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-09T19:00:00Z",
    }
    pg = _MutableWorkflowPg(
        workflow_rows={"wf_build_review_restore": workflow_row},
        build_review_decision_rows=[
            {
                "review_decision_id": "wbrd_approve",
                "workflow_id": "wf_build_review_restore",
                "definition_revision": "def_build_review_restore",
                "target_kind": "binding",
                "target_ref": binding_id,
                "decision": "approve",
                "actor_type": "human",
                "actor_ref": "build_workspace",
                "approval_mode": "manual",
                "rationale": "Approved explicitly.",
                "source_subpath": f"bindings/{binding_id}/accept",
                "candidate_ref": "task_type_routing:auto/review",
                "candidate_payload": {
                    "target_ref": "task_type_routing:auto/review",
                    "label": "Auto Review",
                    "kind": "agent",
                },
                "decided_at": "2026-04-09T19:05:00Z",
                "created_at": "2026-04-09T19:05:00Z",
            }
        ],
    )
    request = _RequestStub(
        {
            "target_kind": "binding",
            "target_ref": binding_id,
            "decision": "revoke",
            "approval_mode": "undo_restore",
            "rationale": "Undo restore to the prior unapproved build-review state.",
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_review_restore/build/review_decisions",
    )

    workflow_query._handle_workflow_build_post(
        request,
        "/api/workflows/wf_build_review_restore/build/review_decisions",
    )

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    binding = next(entry for entry in payload["binding_ledger"] if entry["binding_id"] == binding_id)
    assert binding["state"] == "suggested"
    assert binding["accepted_target"] is None
    assert payload["build_state"] == "blocked"
    review_row = pg.build_review_decision_rows[-1]
    assert review_row["decision"] == "revoke"
    assert review_row["target_ref"] == binding_id


def test_handle_workflow_build_post_restores_binding_record() -> None:
    previous_binding = {
        "binding_id": "binding:ref-001",
        "source_kind": "reference",
        "source_label": "triage-agent",
        "source_span": None,
        "source_node_ids": ["step-001"],
        "state": "captured",
        "candidate_targets": [
            {
                "target_ref": "task_type_routing:auto/review",
                "label": "Auto Review",
                "kind": "agent",
            }
        ],
        "accepted_target": None,
        "rationale": "Needs an accepted authority target before planning can run cleanly.",
        "created_at": "2026-04-09T19:00:00+00:00",
        "updated_at": "2026-04-09T19:00:00+00:00",
        "freshness": None,
    }
    workflow_row = {
        "id": "wf_build_binding_restore",
        "name": "Support Intake",
        "description": "Compile support intake",
        "definition": {
            "type": "operating_model",
            "source_prose": "triage-agent reviews the support inbox.",
            "compiled_prose": "triage-agent reviews the support inbox.",
            "narrative_blocks": [],
            "references": [],
            "capabilities": [],
            "authority": "",
            "sla": {},
            "trigger_intent": [],
            "draft_flow": [
                {
                    "id": "step-001",
                    "title": "Review support inbox",
                    "summary": "triage-agent reviews the support inbox.",
                    "source_block_ids": [],
                    "depends_on": [],
                    "order": 1,
                }
            ],
            "binding_ledger": [
                {
                    **previous_binding,
                    "state": "accepted",
                    "accepted_target": {
                        "target_ref": "task_type_routing:auto/review",
                        "label": "Auto Review",
                        "kind": "agent",
                    },
                    "rationale": "Accepted in build workspace.",
                }
            ],
            "definition_revision": "def_build_binding_restore",
        },
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-09T19:00:00Z",
    }
    pg = _MutableWorkflowPg(workflow_rows={"wf_build_binding_restore": workflow_row})
    request = _RequestStub(
        {"binding": previous_binding},
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_binding_restore/build/bindings/binding:ref-001/restore",
    )

    workflow_query._handle_workflow_build_post(
        request,
        "/api/workflows/wf_build_binding_restore/build/bindings/binding:ref-001/restore",
    )

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    restored = next(entry for entry in payload["binding_ledger"] if entry["binding_id"] == "binding:ref-001")
    assert restored["state"] == "captured"
    assert restored["accepted_target"] is None
    persisted_definition = pg.workflow_rows["wf_build_binding_restore"]["definition"]
    persisted_restored = next(entry for entry in persisted_definition["binding_ledger"] if entry["binding_id"] == "binding:ref-001")
    assert persisted_restored["state"] == "captured"
    assert persisted_restored["accepted_target"] is None


def test_handle_workflow_build_post_removes_staged_import_and_binding_records() -> None:
    snapshot_id = "import_001"
    binding_id = f"binding:import:{snapshot_id}"
    workflow_row = {
        "id": "wf_build_import_restore",
        "name": "Support Intake",
        "description": "Compile support intake",
        "definition": {
            "type": "operating_model",
            "source_prose": "Use imported escalation policy evidence.",
            "compiled_prose": "Use imported escalation policy evidence.",
            "narrative_blocks": [],
            "references": [],
            "capabilities": [],
            "authority": "",
            "sla": {},
            "trigger_intent": [],
            "draft_flow": [
                {
                    "id": "step-001",
                    "title": "Review support inbox",
                    "summary": "Use imported escalation policy evidence.",
                    "source_block_ids": [],
                    "depends_on": [],
                    "order": 1,
                }
            ],
            "import_snapshots": [
                {
                    "snapshot_id": snapshot_id,
                    "source_kind": "net",
                    "source_locator": "find escalation policy",
                    "requested_shape": {
                        "label": "Escalation Policy",
                        "target_ref": "#escalation-policy",
                        "kind": "type",
                    },
                    "payload": {"note": "Requested from search"},
                    "freshness_ttl": 3600,
                    "captured_at": "2026-04-09T19:00:00+00:00",
                    "stale_after_at": "2026-04-09T20:00:00+00:00",
                    "approval_state": "staged",
                    "admitted_targets": [],
                    "binding_id": binding_id,
                    "node_id": "step-001",
                }
            ],
            "binding_ledger": [
                {
                    "binding_id": binding_id,
                    "source_kind": "import_request",
                    "source_label": "Escalation Policy",
                    "source_span": None,
                    "source_node_ids": ["step-001"],
                    "state": "suggested",
                    "candidate_targets": [
                        {
                            "target_ref": "#escalation-policy",
                            "label": "Escalation Policy",
                            "kind": "type",
                        }
                    ],
                    "accepted_target": None,
                    "rationale": "Suggested from staged external evidence.",
                    "created_at": "2026-04-09T19:00:00+00:00",
                    "updated_at": "2026-04-09T19:00:00+00:00",
                    "freshness": None,
                }
            ],
            "definition_revision": "def_build_import_restore",
        },
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-09T19:00:00Z",
    }
    pg = _MutableWorkflowPg(workflow_rows={"wf_build_import_restore": workflow_row})
    request_import = _RequestStub(
        {"snapshot": None},
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path=f"/api/workflows/wf_build_import_restore/build/imports/{snapshot_id}/restore",
    )

    workflow_query._handle_workflow_build_post(
        request_import,
        f"/api/workflows/wf_build_import_restore/build/imports/{snapshot_id}/restore",
    )

    request_binding = _RequestStub(
        {"binding": None},
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path=f"/api/workflows/wf_build_import_restore/build/bindings/{binding_id}/restore",
    )

    workflow_query._handle_workflow_build_post(
        request_binding,
        f"/api/workflows/wf_build_import_restore/build/bindings/{binding_id}/restore",
    )

    assert request_binding.sent is not None
    status, payload = request_binding.sent
    assert status == 200
    assert payload["import_snapshots"] == []
    assert payload["binding_ledger"] == []
    persisted_definition = pg.workflow_rows["wf_build_import_restore"]["definition"]
    assert persisted_definition["import_snapshots"] == []
    assert persisted_definition["binding_ledger"] == []


def test_handle_workflow_build_post_admit_import_emits_restore_receipt() -> None:
    snapshot_id = "import_001"
    binding_id = f"binding:import:{snapshot_id}"
    staged_snapshot = {
        "snapshot_id": snapshot_id,
        "source_kind": "net",
        "source_locator": "find escalation policy",
        "requested_shape": {
            "label": "Escalation Policy",
            "target_ref": "#escalation-policy",
            "kind": "type",
        },
        "payload": {"note": "Requested from search"},
        "freshness_ttl": 3600,
        "captured_at": "2026-04-09T19:00:00+00:00",
        "stale_after_at": "2026-04-09T20:00:00+00:00",
        "approval_state": "staged",
        "admitted_targets": [],
        "binding_id": binding_id,
        "node_id": "step-001",
    }
    workflow_row = {
        "id": "wf_build_import_admit",
        "name": "Support Intake",
        "description": "Compile support intake",
        "definition": {
            "type": "operating_model",
            "source_prose": "Use imported escalation policy evidence.",
            "compiled_prose": "Use imported escalation policy evidence.",
            "narrative_blocks": [],
            "references": [],
            "capabilities": [],
            "authority": "",
            "sla": {},
            "trigger_intent": [],
            "draft_flow": [
                {
                    "id": "step-001",
                    "title": "Review support inbox",
                    "summary": "Use imported escalation policy evidence.",
                    "source_block_ids": [],
                    "depends_on": [],
                    "order": 1,
                }
            ],
            "import_snapshots": [staged_snapshot],
            "binding_ledger": [
                {
                    "binding_id": binding_id,
                    "source_kind": "import_request",
                    "source_label": "Escalation Policy",
                    "source_span": None,
                    "source_node_ids": ["step-001"],
                    "state": "suggested",
                    "candidate_targets": [
                        {
                            "target_ref": "#escalation-policy",
                            "label": "Escalation Policy",
                            "kind": "type",
                        }
                    ],
                    "accepted_target": None,
                    "rationale": "Suggested from staged external evidence.",
                    "created_at": "2026-04-09T19:00:00+00:00",
                    "updated_at": "2026-04-09T19:00:00+00:00",
                    "freshness": None,
                }
            ],
            "definition_revision": "def_build_import_admit",
        },
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-09T19:00:00Z",
    }
    pg = _MutableWorkflowPg(workflow_rows={"wf_build_import_admit": workflow_row})
    request = _RequestStub(
        {
            "target_kind": "import_snapshot",
            "target_ref": snapshot_id,
            "decision": "approve",
            "candidate_payload": {
                "target_ref": "#escalation-policy",
                "label": "Escalation Policy",
                "kind": "type",
            },
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_import_admit/build/review_decisions",
    )

    workflow_query._handle_workflow_build_post(
        request,
        "/api/workflows/wf_build_import_admit/build/review_decisions",
    )

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["import_snapshots"][0]["approval_state"] == "admitted"
    review_event = next(
        params
        for query, params in pg.executed
        if "INSERT INTO event_log" in query and params[1] == "review_decision"
    )
    review_row = next(
        params
        for query, params in pg.executed
        if "INSERT INTO workflow_build_review_decisions" in query
    )
    review_payload = json.loads(review_event[4])
    assert review_payload["target_kind"] == "import_snapshot"
    assert review_payload["target_ref"] == snapshot_id
    assert review_payload["decision"] == "approve"
    assert review_payload["candidate_ref"] == "#escalation-policy"
    assert review_row[4] == "import_snapshot"
    assert review_row[5] == snapshot_id
    assert payload["undo_receipt"] == {
        "workflow_id": "wf_build_import_admit",
        "steps": [
            {
                "subpath": "review_decisions",
                "body": {
                    "target_kind": "import_snapshot",
                    "target_ref": snapshot_id,
                    "slot_ref": snapshot_id,
                    "decision": "revoke",
                    "approval_mode": "undo_restore",
                    "rationale": "Undo restore to the prior unapproved build-review state.",
                },
            }
        ],
    }
    persisted_definition = pg.workflow_rows["wf_build_import_admit"]["definition"]
    assert persisted_definition["import_snapshots"][0]["approval_state"] == "staged"


def test_handle_workflow_build_post_materializes_import_and_attachment() -> None:
    workflow_row = {
        "id": "wf_build_materialize",
        "name": "Support Intake",
        "description": "Compile support intake",
        "definition": {
            "type": "operating_model",
            "source_prose": "triage-agent reviews the support inbox.",
            "compiled_prose": "triage-agent reviews the support inbox.",
            "narrative_blocks": [
                {
                    "id": "block-001",
                    "title": "Review support inbox",
                    "summary": "triage-agent reviews the support inbox.",
                    "text": "triage-agent reviews the support inbox.",
                    "order": 1,
                    "reference_slugs": ["triage-agent"],
                    "capability_slugs": [],
                }
            ],
            "references": [
                {
                    "id": "ref-001",
                    "type": "agent",
                    "slug": "triage-agent",
                    "raw": "triage-agent",
                    "resolved": True,
                    "resolved_to": "task_type_routing:auto/review",
                    "config": {"route": "auto/review"},
                }
            ],
            "capabilities": [],
            "authority": "",
            "sla": {},
            "trigger_intent": [],
            "draft_flow": [
                {
                    "id": "step-001",
                    "title": "Review support inbox",
                    "summary": "triage-agent reviews the support inbox.",
                    "source_block_ids": ["block-001"],
                    "reference_slugs": ["triage-agent"],
                    "capability_slugs": [],
                    "depends_on": [],
                    "order": 1,
                }
            ],
            "definition_revision": "def_build_materialize",
        },
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-09T19:00:00Z",
    }
    pg = _MutableWorkflowPg(workflow_rows={"wf_build_materialize": workflow_row})
    request = _RequestStub(
        {
            "node_id": "step-001",
            "source_kind": "net",
            "source_locator": "find current escalation policy",
            "requested_shape": {
                "label": "Escalation Policy",
                "target_ref": "#escalation-policy",
                "kind": "type",
            },
            "authority_kind": "reference",
            "authority_ref": "#escalation-policy",
            "role": "evidence",
            "label": "Escalation Policy",
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_materialize/build/materialize-here",
    )

    workflow_query._handle_workflow_build_post(request, "/api/workflows/wf_build_materialize/build/materialize-here")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["authority_attachments"][0]["authority_ref"] == "#escalation-policy"
    assert payload["import_snapshots"][0]["approval_state"] == "staged"
    assert payload["import_snapshots"][0]["admitted_targets"] == []
    assert payload["mutation_event_id"] == 1
    assert payload["undo_receipt"] == {
        "workflow_id": "wf_build_materialize",
        "steps": [
            {
                "subpath": f"attachments/{payload['authority_attachments'][0]['attachment_id']}/restore",
                "body": {"attachment": None},
            },
            {
                "subpath": f"imports/{payload['import_snapshots'][0]['snapshot_id']}/restore",
                "body": {"snapshot": None},
            },
        ],
    }
    mutation_event = next(row for row in pg.event_log_rows if row["emitted_by"] == "mutate_workflow_build")
    assert mutation_event == {
        "id": 1,
        "channel": "build_state",
        "event_type": "mutation",
        "entity_id": "wf_build_materialize",
        "entity_kind": "workflow",
        "payload": {
            "subpath": "materialize-here",
            "undo_receipt": payload["undo_receipt"],
        },
        "emitted_by": "mutate_workflow_build",
    }
    assert payload["build_state"] == "blocked"
    assert payload["compiled_spec"] is None
    assert payload["compiled_spec_projection"] is None
    binding = next(entry for entry in payload["binding_ledger"] if entry["binding_id"].startswith("binding:import:"))
    assert binding["state"] == "suggested"
    assert binding["accepted_target"] is None
    persisted_definition = pg.workflow_rows["wf_build_materialize"]["definition"]
    assert persisted_definition["authority_attachments"][0]["authority_ref"] == "#escalation-policy"
    assert persisted_definition["import_snapshots"][0]["approval_state"] == "staged"


def test_handle_workflow_build_post_materialize_here_stays_structural_without_review_approval() -> None:
    workflow_row = {
        "id": "wf_build_materialize_idempotent",
        "name": "Support Intake",
        "description": "Compile support intake",
        "definition": {
            "type": "operating_model",
            "source_prose": "triage-agent reviews the support inbox.",
            "compiled_prose": "triage-agent reviews the support inbox.",
            "narrative_blocks": [],
            "references": [],
            "capabilities": [],
            "authority": "",
            "sla": {},
            "trigger_intent": [],
            "draft_flow": [
                {
                    "id": "step-001",
                    "title": "Review support inbox",
                    "summary": "triage-agent reviews the support inbox.",
                    "source_block_ids": [],
                    "depends_on": [],
                    "order": 1,
                }
            ],
            "definition_revision": "def_build_materialize_idempotent",
        },
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-09T19:00:00Z",
    }
    pg = _MutableWorkflowPg(workflow_rows={"wf_build_materialize_idempotent": workflow_row})
    request_body = {
        "node_id": "step-001",
        "source_kind": "net",
        "source_locator": "find current escalation policy",
        "requested_shape": {
            "label": "Escalation Policy",
            "target_ref": "#escalation-policy",
            "kind": "type",
        },
        "authority_kind": "reference",
        "authority_ref": "#escalation-policy",
        "role": "evidence",
        "label": "Escalation Policy",
    }

    first_request = _RequestStub(
        request_body,
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_materialize_idempotent/build/materialize-here",
    )
    workflow_query._handle_workflow_build_post(
        first_request,
        "/api/workflows/wf_build_materialize_idempotent/build/materialize-here",
    )

    second_request = _RequestStub(
        request_body,
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_materialize_idempotent/build/materialize-here",
    )
    workflow_query._handle_workflow_build_post(
        second_request,
        "/api/workflows/wf_build_materialize_idempotent/build/materialize-here",
    )

    assert second_request.sent is not None
    status, payload = second_request.sent
    assert status == 200
    assert payload["import_snapshots"][0]["approval_state"] == "staged"
    assert payload["import_snapshots"][0]["admitted_targets"] == []
    assert len(payload["authority_attachments"]) == 1
    assert payload["authority_attachments"][0]["authority_ref"] == "#escalation-policy"


def test_handle_workflow_build_post_build_graph_emits_db_backed_restore_receipt() -> None:
    workflow_row = {
        "id": "wf_build_graph_receipt",
        "name": "Support Intake",
        "description": "Compile support intake",
        "definition": {
            "type": "operating_model",
            "source_prose": "triage-agent reviews the support inbox.",
            "compiled_prose": "triage-agent reviews the support inbox.",
            "narrative_blocks": [],
            "references": [],
            "capabilities": [],
            "authority": "",
            "sla": {},
            "trigger_intent": [],
            "draft_flow": [
                {
                    "id": "step-001",
                    "title": "Review support inbox",
                    "summary": "triage-agent reviews the support inbox.",
                    "source_block_ids": [],
                    "depends_on": [],
                    "order": 1,
                }
            ],
            "definition_revision": "def_build_graph_receipt",
        },
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-09T19:00:00Z",
    }
    pg = _MutableWorkflowPg(workflow_rows={"wf_build_graph_receipt": workflow_row})
    request = _RequestStub(
        {
            "nodes": [
                {
                    "node_id": "step-001",
                    "kind": "step",
                    "title": "Review support inbox",
                    "summary": "triage-agent reviews the support inbox.",
                    "route": "task_type_routing:auto/review",
                    "status": "ready",
                    "integration_args": {},
                },
                {
                    "node_id": "step-002",
                    "kind": "step",
                    "title": "Escalate",
                    "summary": "Escalate the inbox to a human.",
                    "route": "",
                    "status": "",
                    "integration_args": {},
                },
            ],
            "edges": [
                {
                    "edge_id": "edge-step-001-step-002",
                    "kind": "sequence",
                    "from_node_id": "step-001",
                    "to_node_id": "step-002",
                }
            ],
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_graph_receipt/build/build_graph",
    )

    workflow_query._handle_workflow_build_post(
        request,
        "/api/workflows/wf_build_graph_receipt/build/build_graph",
    )

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["mutation_event_id"] == 1
    assert payload["undo_receipt"] is not None
    assert payload["undo_receipt"]["workflow_id"] == "wf_build_graph_receipt"
    assert payload["undo_receipt"]["steps"][0]["subpath"] == "build_graph"
    assert isinstance(payload["undo_receipt"]["steps"][0]["body"]["nodes"], list)
    assert isinstance(payload["undo_receipt"]["steps"][0]["body"]["edges"], list)
    assert payload["undo_receipt"]["steps"][0]["body"]["nodes"][0]["node_id"] == "step-001"
    mutation_event = next(row for row in pg.event_log_rows if row["emitted_by"] == "mutate_workflow_build")
    assert mutation_event == {
        "id": 1,
        "channel": "build_state",
        "event_type": "mutation",
        "entity_id": "wf_build_graph_receipt",
        "entity_kind": "workflow",
        "payload": {
            "subpath": "build_graph",
            "undo_receipt": payload["undo_receipt"],
        },
        "emitted_by": "mutate_workflow_build",
    }


def test_handle_workflow_build_post_delegates_to_runtime_owner() -> None:
    pg = _RecordingPg()
    request = _RequestStub(
        {"node_id": "step-001", "authority_kind": "reference", "authority_ref": "@gmail/search"},
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build/build/attachments",
    )
    runtime_result = {
        "row": {
            "id": "wf_build",
            "name": "Build Workflow",
            "description": "desc",
            "definition": {},
            "compiled_spec": {},
            "version": 1,
            "updated_at": "2026-04-09T20:00:00Z",
        },
        "definition": {},
        "compiled_spec": {},
        "build_bundle": {"projection_status": {"state": "ready"}},
        "planning_notes": [],
    }
    built_payload = {"workflow": {"id": "wf_build"}}

    with (
        patch.object(workflow_query, "mutate_workflow_build", return_value=runtime_result) as mutate_mock,
        patch.object(workflow_query, "build_workflow_build_moment", return_value=built_payload) as build_mock,
    ):
        workflow_query._handle_workflow_build_post(request, "/api/workflows/wf_build/build/attachments")

    mutate_mock.assert_called_once()
    build_mock.assert_called_once_with(
        runtime_result["row"],
        conn=pg,
        definition=runtime_result["definition"],
        compiled_spec=runtime_result["compiled_spec"],
        build_bundle=runtime_result["build_bundle"],
        planning_notes=runtime_result["planning_notes"],
        intent_brief=runtime_result.get("intent_brief"),
        execution_manifest=runtime_result.get("execution_manifest"),
        undo_receipt=runtime_result.get("undo_receipt"),
        mutation_event_id=runtime_result.get("mutation_event_id"),
    )
    assert request.sent is not None
    assert request.sent[0] == 200
    assert request.sent[1] == built_payload


def test_handle_workflow_triggers_post_delegates_to_runtime_owner() -> None:
    request = _RequestStub(
        {
            "workflow_id": "wf_123",
            "event_type": "manual",
            "enabled": True,
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: object()),
    )

    with patch.object(
        workflow_query,
        "save_workflow_trigger",
        return_value={
            "id": "trg_123",
            "workflow_id": "wf_123",
            "workflow_name": "Inbox Triage",
            "source_trigger_id": None,
            "event_type": "manual",
            "filter": {},
            "enabled": True,
            "cron_expression": None,
            "created_at": None,
            "last_fired_at": None,
            "fire_count": 0,
        },
    ) as save_mock:
        workflow_query._handle_workflow_triggers_post(request, "/api/workflow-triggers")

    save_mock.assert_called_once()
    assert request.sent is not None
    assert request.sent[0] == 200
    assert request.sent[1]["trigger"]["id"] == "trg_123"
    assert request.sent[1]["trigger"]["workflow_name"] == "Inbox Triage"


def test_handle_workflow_trigger_update_delegates_to_runtime_owner() -> None:
    request = _RequestStub(
        {
            "event_type": "email.received",
            "enabled": False,
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: object()),
        path="/api/workflow-triggers/trg_123",
    )

    with patch.object(
        workflow_query,
        "update_workflow_trigger",
        return_value={
            "id": "trg_123",
            "workflow_id": "wf_123",
            "workflow_name": "Inbox Triage",
            "source_trigger_id": None,
            "event_type": "email.received",
            "filter": {},
            "enabled": False,
            "cron_expression": None,
            "created_at": None,
            "last_fired_at": None,
            "fire_count": 0,
        },
    ) as update_mock:
        workflow_query._handle_workflow_triggers_post(request, "/api/workflow-triggers/trg_123")

    update_mock.assert_called_once()
    assert request.sent == (
        200,
        {
            "trigger": {
                "id": "trg_123",
                "workflow_id": "wf_123",
                "workflow_name": "Inbox Triage",
                "source_trigger_id": None,
                "event_type": "email.received",
                "filter": {},
                "enabled": False,
                "cron_expression": None,
                "created_at": None,
                "last_fired_at": None,
                "fire_count": 0,
            }
        },
    )


def test_handle_workflow_trigger_update_rejects_blank_event_type() -> None:
    request = _RequestStub(
        {"event_type": "   "},
        subsystems=SimpleNamespace(get_pg_conn=lambda: object()),
        path="/api/workflow-triggers/trg_123",
    )

    workflow_query._handle_workflow_triggers_post(request, "/api/workflow-triggers/trg_123")

    assert request.sent == (400, {"error": "event_type must be a non-empty string"})


def test_handle_workflows_get_leaves_definition_only_workflows_without_current_plan() -> None:
    pg = _RecordingPg(
        workflow_rows={
            "wf_explicit": {
                "id": "wf_explicit",
                "name": "Explicit Inbox Triage",
                "description": "Run the explicit inbox flow",
                "definition": {
                    "type": "operating_model",
                    "definition_revision": "def_explicit_123",
                    "source_prose": "Run the explicit inbox flow",
                    "compiled_prose": "Run the explicit inbox flow",
                    "narrative_blocks": [],
                    "references": [],
                    "capabilities": [],
                    "authority": "",
                    "sla": {},
                    "trigger_intent": [{"event_type": "email.received"}],
                    "jobs": [{"label": "search-inbox", "prompt": "Search the inbox"}],
                },
                "compiled_spec": None,
                "tags": [],
                "version": 1,
                "is_template": False,
                "invocation_count": 0,
                "last_invoked_at": None,
                "created_at": "2026-04-08T13:00:00Z",
                "updated_at": "2026-04-08T13:00:00Z",
            }
        },
    )
    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_explicit",
    )

    workflow_query._handle_workflows_get(request, "/api/workflows/wf_explicit")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["workflow"]["has_spec"] is False
    current_compiled_spec = payload["workflow"]["current_compiled_spec"]
    assert current_compiled_spec is None
    revision_state = payload["workflow"]["revision_state"]
    assert revision_state["saved_plan_status"] == "missing"
    assert revision_state["current_plan_source"] == "missing"
    assert revision_state["current_plan_definition_revision"] is None
    assert revision_state["current_plan_revision"] is None


def test_handle_workflows_get_marks_definition_only_packets_current_without_plan_authority() -> None:
    pg = _RecordingPg(
        workflow_rows={
            "wf_explicit": {
                "id": "wf_explicit",
                "name": "Explicit Inbox Triage",
                "description": "Run the explicit inbox flow",
                "definition": {
                    "type": "operating_model",
                    "definition_revision": "def_explicit_123",
                    "source_prose": "Run the explicit inbox flow",
                    "compiled_prose": "Run the explicit inbox flow",
                    "narrative_blocks": [],
                    "references": [],
                    "capabilities": [],
                    "authority": "",
                    "sla": {},
                    "trigger_intent": [{"event_type": "email.received"}],
                    "jobs": [{"label": "search-inbox", "prompt": "Search the inbox"}],
                },
                "compiled_spec": None,
                "tags": [],
                "version": 1,
                "is_template": False,
                "invocation_count": 1,
                "last_invoked_at": "2026-04-08T13:00:00Z",
                "created_at": "2026-04-08T13:00:00Z",
                "updated_at": "2026-04-08T13:05:00Z",
            }
        },
        workflow_run_rows=[
            {
                "workflow_name": "Explicit Inbox Triage",
                "run_id": "run_explicit_123",
                "spec_name": "Explicit Inbox Triage",
                "status": "succeeded",
                "total_jobs": 1,
                "created_at": "2026-04-08T13:00:00Z",
                "finished_at": "2026-04-08T13:05:00Z",
                "parent_run_id": None,
                "trigger_depth": 0,
            }
        ],
        packet_query_rows={
            "run_explicit_123": {
                "run_id": "run_explicit_123",
                "workflow_id": "wf_explicit",
                "request_id": "req_explicit_123",
                "workflow_definition_id": "workflow_definition.explicit",
                "current_state": "succeeded",
                "request_envelope": {
                    "name": "Explicit Inbox Triage",
                    "spec_snapshot": {
                        "definition_revision": "def_explicit_123",
                        "plan_revision": "plan_old_123",
                        "packet_provenance": {"source_kind": "workflow_submit"},
                    },
                },
                "requested_at": "2026-04-08T13:00:00Z",
                "admitted_at": "2026-04-08T13:00:01Z",
                "started_at": "2026-04-08T13:00:02Z",
                "finished_at": "2026-04-08T13:05:00Z",
                "last_event_id": "event_explicit_123",
                "packets": [
                    {
                        "definition_revision": "def_explicit_123",
                        "plan_revision": "plan_old_123",
                        "packet_version": 1,
                        "workflow_id": "wf_explicit",
                        "run_id": "run_explicit_123",
                        "spec_name": "Explicit Inbox Triage",
                        "source_kind": "workflow_submit",
                        "authority_refs": ["def_explicit_123", "plan_old_123"],
                        "model_messages": [],
                        "reference_bindings": [],
                        "capability_bindings": [],
                        "verify_refs": [],
                        "authority_inputs": {
                            "packet_provenance": {"source_kind": "workflow_submit"},
                        },
                        "file_inputs": {},
                        "packet_hash": "packet_hash_explicit_123",
                        "packet_revision": "packet_explicit_123:1",
                    }
                ],
            }
        },
    )
    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_explicit",
    )

    workflow_query._handle_workflows_get(request, "/api/workflows/wf_explicit")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    revision_state = payload["workflow"]["revision_state"]
    assert revision_state["saved_plan_status"] == "missing"
    assert revision_state["current_plan_source"] == "missing"
    assert revision_state["current_plan_definition_revision"] is None
    assert revision_state["current_plan_revision"] is None
    assert revision_state["current_packet"] == {
        "run_id": "run_explicit_123",
        "run_status": "succeeded",
        "requested_at": "2026-04-08T13:00:00Z",
        "packet_revision": "packet_explicit_123:1",
        "packet_hash": "packet_hash_explicit_123",
        "definition_revision": "def_explicit_123",
        "plan_revision": "plan_old_123",
        "drift_status": "aligned",
        "drifted": False,
        "status": "current",
        "matches_current_definition": True,
        "matches_current_plan": False,
    }


def test_handle_workflows_get_surfaces_saved_plan_and_latest_packet_revision_state() -> None:
    pg = _RecordingPg(
        workflow_rows={
            "wf_123": {
                "id": "wf_123",
                "name": "Inbox Triage",
                "description": "Handle support email",
                "definition": {
                    "type": "operating_model",
                    "definition_revision": "def_current_123",
                    "source_prose": "Handle support email",
                    "compiled_prose": "Use @gmail/search before triage-agent responds.",
                    "narrative_blocks": [],
                    "references": [],
                    "capabilities": [],
                    "authority": "",
                    "sla": {},
                    "trigger_intent": [],
                    "draft_flow": [],
                },
                "compiled_spec": {
                    "name": "Inbox Triage",
                    "definition_revision": "def_stale_123",
                    "plan_revision": "plan_old_123",
                    "jobs": [{"label": "search-inbox", "agent": "integration/gmail"}],
                    "triggers": [],
                },
                "tags": [],
                "version": 3,
                "is_template": False,
                "invocation_count": 4,
                "last_invoked_at": "2026-04-08T13:00:00Z",
                "created_at": "2026-04-07T10:00:00Z",
                "updated_at": "2026-04-08T13:00:00Z",
            }
        },
        workflow_run_rows=[
            {
                "workflow_name": "Inbox Triage",
                "run_id": "run_123",
                "spec_name": "Inbox Triage",
                "status": "succeeded",
                "total_jobs": 1,
                "created_at": "2026-04-08T13:00:00Z",
                "finished_at": "2026-04-08T13:05:00Z",
                "parent_run_id": None,
                "trigger_depth": 0,
            }
        ],
        packet_query_rows={
            "run_123": {
                "run_id": "run_123",
                "workflow_id": "wf_123",
                "request_id": "req_123",
                "workflow_definition_id": "workflow_definition.123",
                "current_state": "succeeded",
                    "request_envelope": {
                        "name": "Inbox Triage",
                        "spec_snapshot": {
                            "definition_revision": "def_stale_123",
                            "plan_revision": "plan_old_123",
                            "packet_provenance": {"source_kind": "workflow_submit"},
                        },
                    },
                "requested_at": "2026-04-08T13:00:00Z",
                "admitted_at": "2026-04-08T13:00:01Z",
                "started_at": "2026-04-08T13:00:02Z",
                "finished_at": "2026-04-08T13:05:00Z",
                "last_event_id": "event_123",
                "packets": [
                    {
                        "definition_revision": "def_stale_123",
                        "plan_revision": "plan_old_123",
                        "packet_version": 1,
                        "workflow_id": "wf_123",
                        "run_id": "run_123",
                        "spec_name": "Inbox Triage",
                        "source_kind": "workflow_submit",
                        "authority_refs": ["def_stale_123", "plan_old_123"],
                        "model_messages": [],
                        "reference_bindings": [],
                        "capability_bindings": [],
                        "verify_refs": [],
                        "authority_inputs": {
                            "packet_provenance": {"source_kind": "workflow_submit"},
                        },
                        "file_inputs": {},
                        "packet_hash": "packet_hash_123",
                        "packet_revision": "packet_123:1",
                    }
                ],
            }
        },
    )
    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_123",
    )

    workflow_query._handle_workflows_get(request, "/api/workflows/wf_123")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    revision_state = payload["workflow"]["revision_state"]
    assert revision_state["saved_definition_revision"] == "def_current_123"
    assert revision_state["saved_plan_definition_revision"] == "def_stale_123"
    assert revision_state["saved_plan_revision"] == "plan_old_123"
    assert revision_state["saved_plan_status"] == "stale"
    assert revision_state["current_plan_definition_revision"] is None
    assert revision_state["current_plan_revision"] is None
    assert revision_state["current_plan_source"] == "missing"
    assert revision_state["current_packet"] == {
        "run_id": "run_123",
        "run_status": "succeeded",
        "requested_at": "2026-04-08T13:00:00Z",
        "packet_revision": "packet_123:1",
        "packet_hash": "packet_hash_123",
        "definition_revision": "def_stale_123",
        "plan_revision": "plan_old_123",
        "drift_status": "aligned",
        "drifted": False,
        "status": "stale_definition",
        "matches_current_definition": False,
        "matches_current_plan": False,
    }
    assert payload["workflow"]["current_compiled_spec"] is None


def test_handle_market_models_get_returns_bound_market_rows() -> None:
    pg = _RecordingPg()
    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/models/market",
    )

    workflow_query._handle_market_models_get(request, "/api/models/market")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["count"] == 1
    assert payload["filtered_count"] == 1
    assert payload["total_count"] == 1
    assert payload["models"][0]["creator_slug"] == "openai"
    assert payload["models"][0]["local_bindings"][0]["slug"] == "openai/gpt-5.4"
    assert payload["models"][0]["local_bindings"][0]["binding_kind"] == "normalized_slug_alias"
    assert payload["models"][0]["local_bindings"][0]["binding_confidence"] == 0.99
    assert payload["models"][0]["binding_status"] == "bound"
    assert payload["models"][0]["family_slug"] == "gpt"
    assert payload["models"][0]["review_metrics"]["intelligence_index"] == 70.1
    assert payload["review"]["bound_market_models"] == 1
    assert payload["facets"]["creators"][0]["creator_slug"] == "openai"
    assert payload["facets"]["families"][0]["family_slug"] == "gpt"


def test_handle_models_get_filters_to_requested_task_type_chain() -> None:
    class _ModelsPg:
        def execute(self, query: str, *params: Any) -> list[dict[str, Any]]:
            del params
            if "FROM provider_model_candidates" not in query:
                raise AssertionError(query)
            return [
                {
                    "provider_slug": "deepseek",
                    "model_slug": "deepseek-r3",
                    "status": "active",
                    "capability_tags": ["research"],
                    "route_tier": "low",
                    "route_tier_rank": 3,
                    "latency_class": "instant",
                    "latency_rank": 1,
                    "reasoning_control": {},
                    "task_affinities": {"primary": ["research"], "secondary": [], "specialized": [], "avoid": []},
                    "benchmark_profile": {},
                },
                {
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4",
                    "status": "active",
                    "capability_tags": ["build"],
                    "route_tier": "high",
                    "route_tier_rank": 1,
                    "latency_class": "reasoning",
                    "latency_rank": 2,
                    "reasoning_control": {},
                    "task_affinities": {"primary": ["build"], "secondary": ["review"], "specialized": [], "avoid": []},
                    "benchmark_profile": {},
                },
            ]

    class _FakeDecision:
        def __init__(self, provider_slug: str, model_slug: str) -> None:
            self.provider_slug = provider_slug
            self.model_slug = model_slug

    class _FakeRouter:
        def __init__(self, _pg: Any) -> None:
            self._pg = _pg

        def resolve_failover_chain(self, agent_slug: str):
            assert agent_slug == "auto/build"
            return [_FakeDecision("openai", "gpt-5.4")]

    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: _ModelsPg()),
        path="/api/models?task_type=build",
    )

    with patch("runtime.task_type_router.TaskTypeRouter", _FakeRouter):
        workflow_query._handle_models_get(request, request.path)

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert [model["slug"] for model in payload["models"]] == ["openai/gpt-5.4"]
    assert payload["models"][0]["route_rank"] == 1


def test_handle_market_models_get_supports_review_filters() -> None:
    pg = _RecordingPg(
        market_rows=[
            {
                "market_model_ref": "market_model.artificial_analysis.llm.gemma-4-31b",
                "source_slug": "artificial_analysis",
                "modality": "llm",
                "source_model_id": "gemma-4-31b",
                "source_model_slug": "gemma-4-31b",
                "model_name": "Gemma 4 31B (Reasoning)",
                "creator_slug": "google",
                "creator_name": "Google",
                "evaluations": {"artificial_analysis_intelligence_index": 32.4},
                "pricing": {"price_1m_blended_3_to_1": 0.9},
                "speed_metrics": {"median_output_tokens_per_second": 120.0},
                "prompt_options": {"prompt_length": "medium"},
                "last_synced_at": "2026-04-08T12:00:00Z",
            },
            {
                "market_model_ref": "market_model.artificial_analysis.llm.deepseek-r1",
                "source_slug": "artificial_analysis",
                "modality": "llm",
                "source_model_id": "deepseek-r1",
                "source_model_slug": "deepseek-r1",
                "model_name": "DeepSeek R1",
                "creator_slug": "deepseek",
                "creator_name": "DeepSeek",
                "evaluations": {"artificial_analysis_intelligence_index": 41.0},
                "pricing": {"price_1m_blended_3_to_1": 2.4},
                "speed_metrics": {"median_output_tokens_per_second": 42.0},
                "prompt_options": {"prompt_length": "medium"},
                "last_synced_at": "2026-04-08T12:00:00Z",
            },
        ],
        binding_rows=[],
    )
    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/models/market?family=gemma&binding=unbound&sort=intelligence_desc",
    )

    workflow_query._handle_market_models_get(request, "/api/models/market")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["count"] == 1
    assert payload["filtered_count"] == 1
    assert payload["models"][0]["family_slug"] == "gemma"
    assert payload["models"][0]["creator_slug"] == "google"
    assert payload["models"][0]["binding_status"] == "unbound"
    assert payload["review"]["unbound_market_models"] == 1
    assert payload["facets"]["families"][0]["family_slug"] == "gemma"


def test_handle_trigger_post_rejects_workflow_without_current_plan() -> None:
    pg = _RecordingPg(
        workflow_rows={
            "wf_123": {
                "id": "wf_123",
                "name": "Inbox Triage",
                "definition": {"definition_revision": "def_123"},
                "compiled_spec": None,
            }
        }
    )
    request = _RequestStub(subsystems=SimpleNamespace(get_pg_conn=lambda: pg))

    workflow_query._handle_trigger_post(request, "/api/trigger/wf_123")

    assert request.sent == (
        400,
        {"error": "Workflow 'Inbox Triage' has no approved execution manifest. Review and harden the workflow first."},
    )
    assert not any("INSERT INTO system_events" in query for query, _ in pg.executed)


def test_handle_trigger_post_uses_command_bus_helper(tmp_path, monkeypatch) -> None:
    temp_dir = tmp_path / "artifacts" / "workflow"
    temp_dir.mkdir(parents=True, exist_ok=True)

    pg = _RecordingPg(
        workflow_rows={
            "wf_123": {
                "id": "wf_123",
                "name": "Inbox Triage",
                "definition": {
                    "type": "operating_model",
                    "definition_revision": "def_123",
                    "compile_provenance": {
                        "compile_index_ref": "compile_index.alpha",
                        "compile_surface_revision": "compile_surface.alpha",
                    },
                },
                "compiled_spec": {
                    "definition_revision": "def_123",
                    "name": "Inbox Triage",
                    "workflow_id": "wf_123",
                    "phase": "build",
                    "plan_revision": "plan_123",
                    "jobs": [
                        {
                            "label": "search-inbox",
                            "prompt": "Search the inbox",
                        }
                    ],
                    "outcome_goal": "triage the inbox",
                    "anti_requirements": [],
                },
            }
        }
    )
    request = _RequestStub(subsystems=SimpleNamespace(get_pg_conn=lambda: pg))

    monkeypatch.setattr(workflow_query, "REPO_ROOT", tmp_path)

    fake_result = {
        "run_id": "dispatch_123",
        "status": "queued",
        "spec_name": "Inbox Triage",
        "total_jobs": 1,
    }
    captured_spec: dict[str, Any] = {}

    def _fake_submit(*_args, **kwargs):
        captured_spec.update(kwargs["spec"])
        return fake_result

    with patch.object(canonical_workflows, "_submit_spec_via_service_bus", side_effect=_fake_submit) as bus_mock:
        with patch.object(
            canonical_workflows,
            "_latest_execution_manifest",
            return_value={
                "execution_manifest_ref": "execution_manifest:wf_123:def_123:1",
                "compiled_spec": {
                    "definition_revision": "def_123",
                    "name": "Inbox Triage",
                    "workflow_id": "wf_123",
                    "phase": "build",
                    "plan_revision": "plan_123",
                    "jobs": [
                        {
                            "label": "search-inbox",
                            "prompt": "Search the inbox",
                        }
                    ],
                    "outcome_goal": "triage the inbox",
                    "anti_requirements": [],
                },
                "tool_allowlist": {"mcp_tools": ["praxis_status"], "adapter_tools": ["repo_fs"]},
                "verify_refs": ["verify.alpha"],
                "approved_bundle_refs": ["capability_bundle:triage"],
            },
        ):
            workflow_query._handle_trigger_post(request, "/api/trigger/wf_123")

    assert bus_mock.call_count == 1
    assert request.sent == (
        200,
        {
            "triggered": True,
            "workflow_id": "wf_123",
            "workflow_name": "Inbox Triage",
            "run_id": "dispatch_123",
        },
    )
    assert any("INSERT INTO system_events" in query for query, _ in pg.executed)
    assert captured_spec["packet_provenance"] == {
        "source_kind": "workflow_trigger",
        "workflow_row": {
            "id": "wf_123",
            "name": "Inbox Triage",
            "definition": {
                "type": "operating_model",
                "definition_revision": "def_123",
                "compile_provenance": {
                    "compile_index_ref": "compile_index.alpha",
                    "compile_surface_revision": "compile_surface.alpha",
                },
            },
            "compiled_spec": {
                "definition_revision": "def_123",
                "name": "Inbox Triage",
                "workflow_id": "wf_123",
                "phase": "build",
                "plan_revision": "plan_123",
                "jobs": [{"label": "search-inbox", "prompt": "Search the inbox"}],
                "outcome_goal": "triage the inbox",
                "anti_requirements": [],
            },
        },
        "definition_row": {
            "type": "operating_model",
            "definition_revision": "def_123",
            "compile_provenance": {
                "compile_index_ref": "compile_index.alpha",
                "compile_surface_revision": "compile_surface.alpha",
            },
        },
        "compiled_spec_row": {
            "definition_revision": "def_123",
            "name": "Inbox Triage",
            "workflow_id": "wf_123",
            "phase": "build",
            "plan_revision": "plan_123",
            "jobs": [{"label": "search-inbox", "prompt": "Search the inbox"}],
            "outcome_goal": "triage the inbox",
            "anti_requirements": [],
        },
        "execution_manifest": {
            "execution_manifest_ref": "execution_manifest:wf_123:def_123:1",
            "compiled_spec": {
                "definition_revision": "def_123",
                "name": "Inbox Triage",
                "workflow_id": "wf_123",
                "phase": "build",
                "plan_revision": "plan_123",
                "jobs": [{"label": "search-inbox", "prompt": "Search the inbox"}],
                "outcome_goal": "triage the inbox",
                "anti_requirements": [],
            },
            "tool_allowlist": {"mcp_tools": ["praxis_status"], "adapter_tools": ["repo_fs"]},
            "verify_refs": ["verify.alpha"],
            "approved_bundle_refs": ["capability_bundle:triage"],
        },
    }
    assert captured_spec["execution_manifest_ref"] == "execution_manifest:wf_123:def_123:1"
    assert any(
        "UPDATE public.workflows SET invocation_count = invocation_count + 1" in query
        for query, _ in pg.executed
    )


def test_handle_trigger_post_rejects_definition_only_workflows_even_with_legacy_jobs() -> None:
    pg = _RecordingPg(
        workflow_rows={
            "wf_explicit": {
                "id": "wf_explicit",
                "name": "Explicit Inbox Triage",
                "definition": {
                    "type": "operating_model",
                    "definition_revision": "def_explicit_123",
                    "source_prose": "Run the explicit inbox flow",
                    "compiled_prose": "Run the explicit inbox flow",
                    "narrative_blocks": [],
                    "references": [],
                    "capabilities": [],
                    "authority": "",
                    "sla": {},
                    "trigger_intent": [{"event_type": "email.received"}],
                    "jobs": [{"label": "search-inbox", "prompt": "Search the inbox"}],
                },
                "compiled_spec": None,
                "invocation_count": 0,
                "last_invoked_at": None,
            }
        }
    )
    request = _RequestStub(subsystems=SimpleNamespace(get_pg_conn=lambda: pg))

    with patch.object(
        canonical_workflows,
        "_submit_spec_via_service_bus",
        side_effect=AssertionError("definition-only trigger path should not submit"),
    ):
        workflow_query._handle_trigger_post(request, "/api/trigger/wf_explicit")

    assert request.sent == (
        400,
        {"error": "Workflow 'Explicit Inbox Triage' has no approved execution manifest. Review and harden the workflow first."},
    )
    assert not any("INSERT INTO system_events" in query for query, _ in pg.executed)


def test_handle_trigger_post_delegates_to_runtime_owner() -> None:
    request = _RequestStub(subsystems=SimpleNamespace(get_pg_conn=lambda: object()))

    with patch.object(
        workflow_query,
        "trigger_workflow_manually",
        return_value={"triggered": True, "workflow_id": "wf_123", "run_id": "run_123"},
    ) as trigger_mock:
        workflow_query._handle_trigger_post(request, "/api/trigger/wf_123")

    trigger_mock.assert_called_once()
    assert request.sent == (200, {"triggered": True, "workflow_id": "wf_123", "run_id": "run_123"})


def test_handle_workflows_post_keeps_current_plan_stable_on_rename() -> None:
    class _UpdatePg(_RecordingPg):
        def fetchrow(self, query: str, *params: Any) -> dict[str, Any] | None:
            if query.startswith("UPDATE public.workflows SET"):
                self.executed.append((query, params))
                workflow_id = str(params[0])
                workflow = dict(self.workflow_rows[workflow_id])
                if "name =" in query:
                    workflow["name"] = params[1]
                compiled_match = re.search(r"compiled_spec = \$(\d+)::jsonb", query)
                if compiled_match:
                    compiled_param = params[int(compiled_match.group(1)) - 1]
                    workflow["compiled_spec"] = (
                        json.loads(compiled_param) if compiled_param is not None else None
                    )
                self.workflow_rows[workflow_id] = workflow
                return workflow
            return super().fetchrow(query, *params)

    pg = _UpdatePg(
        workflow_rows={
            "wf_explicit": {
                "id": "wf_explicit",
                "name": "Explicit Inbox Triage",
                "description": "Run the explicit inbox flow",
                "definition": {
                    "type": "operating_model",
                    "definition_revision": "def_explicit_123",
                    "source_prose": "Run the explicit inbox flow",
                    "compiled_prose": "Run the explicit inbox flow",
                    "narrative_blocks": [],
                    "references": [],
                    "capabilities": [],
                    "authority": "",
                    "sla": {},
                    "trigger_intent": [{"event_type": "email.received"}],
                    "jobs": [{"label": "search-inbox", "prompt": "Search the inbox"}],
                },
                "compiled_spec": {
                    "name": "Explicit Inbox Triage",
                    "workflow_id": "explicit_inbox_triage",
                    "phase": "build",
                    "outcome_goal": "Run the explicit inbox flow",
                    "jobs": [{"label": "search-inbox", "prompt": "Search the inbox"}],
                    "triggers": [{"event_type": "email.received", "filter": {}}],
                    "definition_revision": "def_explicit_123",
                    "plan_revision": "plan_explicit_123",
                    "compile_provenance": {
                        "surface_revision": "planner.surface.test",
                        "input_fingerprint": "planner.input.test",
                    },
                },
                "tags": [],
                "version": 1,
                "is_template": False,
                "created_at": "2026-04-08T13:00:00Z",
                "updated_at": "2026-04-08T13:00:00Z",
            }
        },
    )
    request = _RequestStub(
        {"name": "Renamed Explicit Inbox Triage"},
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_explicit",
    )

    workflow_query._handle_workflows_post(request, "/api/workflows/wf_explicit")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    compiled_spec = payload["workflow"]["compiled_spec"]
    assert compiled_spec["name"] == "Explicit Inbox Triage"
    assert compiled_spec["workflow_id"] == "explicit_inbox_triage"
    assert payload["workflow"]["name"] == "Renamed Explicit Inbox Triage"
    assert not any("compiled_spec =" in query for query, _ in pg.executed)


def test_handle_workflows_post_rejects_blank_name_update() -> None:
    request = _RequestStub(
        {"name": "   "},
        subsystems=SimpleNamespace(get_pg_conn=lambda: object()),
        path="/api/workflows/wf_123",
    )

    workflow_query._handle_workflows_post(request, "/api/workflows/wf_123")

    assert request.sent == (400, {"error": "name must be a non-empty string"})


def test_handle_workflows_post_creates_graph_backed_workflow_without_browser_definition() -> None:
    pg = _RecordingPg()
    request = _RequestStub(
        {
            "name": "Graph Save",
            "build_graph": {
                "nodes": [
                    {
                        "node_id": "trigger-001",
                        "kind": "step",
                        "title": "Manual",
                        "route": "trigger",
                        "trigger": {
                            "event_type": "manual",
                            "filter": {},
                        },
                    },
                    {
                        "node_id": "step-001",
                        "kind": "step",
                        "title": "Notify ops",
                        "summary": "Notify ops when the run completes.",
                        "route": "@notifications/send",
                        "integration_args": {
                            "title": "Notify ops",
                            "message": "Run complete",
                            "status": "info",
                            "metadata": {
                                "channel": "ops",
                            },
                        },
                    },
                ],
                "edges": [
                    {
                        "edge_id": "edge-trigger-step",
                        "kind": "sequence",
                        "from_node_id": "trigger-001",
                        "to_node_id": "step-001",
                    }
                ],
            },
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows",
    )

    workflow_query._handle_workflows_post(request, "/api/workflows")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    workflow = payload["workflow"]
    assert workflow["name"] == "Graph Save"
    assert workflow["has_spec"] is False
    assert workflow["definition"]["definition_revision"].startswith("def_")
    assert workflow["definition"]["trigger_intent"] == [
        {
            "id": "trigger-001",
            "title": "Manual",
            "summary": "Manual",
            "event_type": "manual",
            "filter": {},
            "source_node_id": "trigger-001",
            "source_block_ids": [],
            "reference_slugs": [],
        }
    ]
    assert workflow["definition"]["execution_setup"]["phases"] == [
        {
            "step_id": "step-001",
            "agent_route": "@notifications/send",
            "system_prompt": "",
            "required_inputs": [],
            "outputs": [],
            "persistence_targets": [],
            "handoff_target": None,
            "integration_args": {
                "title": "Notify ops",
                "message": "Run complete",
                "status": "info",
                "metadata": {
                    "channel": "ops",
                },
            },
        }
    ]


def test_handle_trigger_post_does_not_fall_back_to_definition_only_workflows(monkeypatch) -> None:
    pg = _RecordingPg(
        workflow_rows={
            "wf-canonical": {
                "id": "wf-canonical",
                "name": "wf_name_only",
                "definition": {"definition_revision": "def_123"},
                "compiled_spec": None,
                "invocation_count": 0,
                "last_invoked_at": None,
            }
        }
    )
    request = _RequestStub(subsystems=SimpleNamespace(get_pg_conn=lambda: pg))

    with patch.object(
        canonical_workflows,
        "_submit_spec_via_service_bus",
        side_effect=AssertionError("definition-only trigger path should not submit"),
    ):
        workflow_query._handle_trigger_post(request, "/api/trigger/wf-canonical")

    assert request.sent == (
        400,
        {"error": "Workflow 'wf_name_only' has no approved execution manifest. Review and harden the workflow first."},
    )
    assert any("FROM public.workflows WHERE id = $1" in query for query, _ in pg.executed)
    assert not any("FROM public.workflows WHERE name = $1" in query for query, _ in pg.executed)


def test_handle_workflow_delete_delegates_to_runtime_owner() -> None:
    request = _RequestStub(subsystems=SimpleNamespace(get_pg_conn=lambda: object()))

    with patch.object(
        workflow_query,
        "delete_workflow",
        return_value={"deleted": True, "workflow_id": "wf_123"},
    ) as delete_mock:
        workflow_query._handle_workflow_delete(request, "/api/workflows/delete/wf_123")

    delete_mock.assert_called_once()
    assert request.sent == (200, {"deleted": True, "workflow_id": "wf_123"})


def test_save_workflow_trigger_persists_and_surfaces_workflow_name() -> None:
    pg = _RecordingPg(
        workflow_rows={
            "wf_123": {
                "id": "wf_123",
                "name": "Inbox Triage",
            }
        }
    )

    row = canonical_workflows.save_workflow_trigger(
        pg,
        body={
            "workflow_id": "wf_123",
            "event_type": "manual",
            "enabled": True,
        },
    )

    assert row["workflow_id"] == "wf_123"
    assert row["workflow_name"] == "Inbox Triage"
    assert row["event_type"] == "manual"
    assert len(pg.trigger_rows) == 1
    assert pg.trigger_rows[0]["workflow_id"] == "wf_123"


def test_update_workflow_trigger_rejects_blank_event_type() -> None:
    pg = _RecordingPg(
        workflow_rows={
            "wf_123": {
                "id": "wf_123",
                "name": "Inbox Triage",
            }
        },
        trigger_rows=[
            {
                "id": "trg_123",
                "workflow_id": "wf_123",
                "event_type": "manual",
                "filter": {},
                "enabled": True,
                "cron_expression": None,
            }
        ],
    )

    try:
        canonical_workflows.update_workflow_trigger(
            pg,
            trigger_id="trg_123",
            body={"event_type": "   "},
        )
    except canonical_workflows.WorkflowRuntimeBoundaryError as exc:
        assert exc.status_code == 400
        assert str(exc) == "event_type must be a non-empty string"
    else:
        raise AssertionError("expected blank event_type to be rejected")


def test_workflow_query_handler_does_not_import_trigger_storage_writes_directly() -> None:
    source = Path(workflow_query.__file__).read_text(encoding="utf-8")

    assert "upsert_workflow_trigger_record" not in source
    assert "update_workflow_trigger_record" not in source
    assert "workflow_exists" not in source


def test_workflow_query_handler_does_not_own_canonical_write_sql() -> None:
    source = Path(workflow_query.__file__).read_text(encoding="utf-8")

    forbidden_sql_snippets = (
        "INSERT INTO public.workflows",
        "UPDATE public.workflows",
        "DELETE FROM public.workflows",
        "INSERT INTO workflow_triggers",
        "UPDATE workflow_triggers",
        "DELETE FROM workflow_triggers",
        "INSERT INTO public.objects",
        "UPDATE objects SET",
        "DELETE FROM objects",
        "INSERT INTO object_types",
        "UPDATE object_types",
        "DELETE FROM object_types",
    )
    leaked = [snippet for snippet in forbidden_sql_snippets if snippet in source]
    assert leaked == [], f"workflow_query.py still owns canonical write SQL: {leaked}"


def test_handle_references_get_reads_reference_catalog() -> None:
    pg = _RecordingPg(
        [
            {
                "slug": "@gmail/search",
                "ref_type": "integration",
                "display_name": "Gmail Search",
                "description": "Search connected Gmail accounts",
            }
        ]
    )
    request = _RequestStub(subsystems=SimpleNamespace(get_pg_conn=lambda: pg))

    workflow_query._handle_references_get(request, "/api/references")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["count"] == 1
    assert payload["references"][0]["slug"] == "@gmail/search"
    assert any("FROM reference_catalog" in query for query, _ in pg.executed)


def test_handle_source_options_get_returns_global_catalog() -> None:
    pg = _RecordingPg(
        reference_rows=[
            {
                "slug": "#account",
                "ref_type": "object",
                "display_name": "Account",
                "description": "Workspace accounts",
                "resolved_table": "object_types",
                "resolved_id": "account",
            },
            {
                "slug": "@gmail/search",
                "ref_type": "integration",
                "display_name": "Gmail Search",
                "description": "Search connected Gmail mailboxes",
                "resolved_table": "integration_registry",
                "resolved_id": "gmail",
            },
        ],
        integration_rows=[
            {
                "id": "gmail",
                "name": "Gmail",
                "description": "Connected Gmail account",
                "provider": "google",
                "capabilities": [{"action": "search"}],
                "auth_status": "connected",
                "icon": "mail",
            }
        ],
        capability_rows=[
            {
                "capability_ref": "cap.gmail.search",
                "capability_slug": "gmail-search",
                "capability_kind": "integration_action",
                "title": "Search Gmail",
                "summary": "Search live mailbox threads.",
                "route": "gmail/search",
                "reference_slugs": ["@gmail/search"],
            }
        ],
    )
    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/source-options",
    )

    workflow_query._handle_source_options_get(request, "/api/source-options")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    option_ids = {option["id"] for option in payload["source_options"]}
    assert "#account" in option_ids
    assert "@gmail/search" in option_ids
    assert "integration:gmail" in option_ids
    assert "web_search" in option_ids
    gmail_option = next(option for option in payload["source_options"] if option["id"] == "@gmail/search")
    assert gmail_option["availability"] == "ready"
    assert gmail_option["activation"] == "attach"
    assert "Search live mailbox threads." in gmail_option["description"]


def test_handle_source_options_get_orders_manifest_tab_options() -> None:
    pg = _RecordingPg(
        reference_rows=[
            {
                "slug": "#account",
                "ref_type": "object",
                "display_name": "Account",
                "description": "Workspace accounts",
                "resolved_table": "object_types",
                "resolved_id": "account",
            }
        ],
        integration_rows=[],
        capability_rows=[],
        manifest_rows={
            "manifest_123": {
                "id": "manifest_123",
                "name": "Support Workspace",
                "description": "Workspace description",
                "manifest": {
                    "version": 4,
                    "kind": "helm_surface_bundle",
                    "title": "Support Workspace",
                    "default_tab_id": "main",
                    "tabs": [
                        {
                            "id": "main",
                            "label": "Overview",
                            "surface_id": "main",
                            "source_option_ids": ["#account", "web_search", "external_api"],
                        }
                    ],
                    "surfaces": {
                        "main": {
                            "id": "main",
                            "title": "Overview",
                            "kind": "quadrant_manifest",
                            "manifest": {
                                "version": 2,
                                "grid": "4x4",
                                "quadrants": {
                                    "A1": {"module": "metric", "config": {"label": "Inbox", "value": "12"}}
                                },
                            },
                        }
                    },
                    "source_options": {
                        "external_api": {
                            "label": "External API",
                            "family": "external",
                            "kind": "api",
                            "availability": "setup_required",
                            "activation": "configure",
                            "setup_intent": "Set up the external support API.",
                        }
                    },
                },
            }
        },
    )
    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/source-options?manifest_id=manifest_123&tab_id=main",
    )

    workflow_query._handle_source_options_get(request, "/api/source-options")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["tab_id"] == "main"
    assert [option["id"] for option in payload["source_options"]] == [
        "#account",
        "web_search",
        "external_api",
    ]
    assert payload["source_options"][2]["availability"] == "setup_required"
    assert payload["source_options"][2]["activation"] == "configure"


def test_handle_manifests_get_returns_compact_listing() -> None:
    pg = _RecordingPg(
        manifest_rows={
            "plan_123": {
                "id": "plan_123",
                "name": "Data Cleanup Plan",
                "description": "Plan for cleanup",
                "status": "draft",
                "updated_at": "2026-04-15T12:00:00+00:00",
                "manifest": {
                    "kind": "praxis_control_manifest",
                    "manifest_family": "control_plane",
                    "manifest_type": "data_plan",
                },
            },
            "manifest_456": {
                "id": "manifest_456",
                "name": "Support Workspace",
                "description": "Workspace bundle",
                "status": "active",
                "updated_at": "2026-04-14T12:00:00+00:00",
                "manifest": {
                    "kind": "helm_surface_bundle",
                    "tabs": [{"id": "main"}],
                },
            },
        }
    )
    request = _RequestStub(subsystems=SimpleNamespace(get_pg_conn=lambda: pg), path="/api/manifests")

    workflow_query._handle_manifests_get(request, "/api/manifests")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["count"] == 2
    assert [row["id"] for row in payload["manifests"]] == ["plan_123", "manifest_456"]
    assert payload["manifests"][0]["manifest_family"] == "control_plane"
    assert payload["manifests"][0]["manifest_type"] == "data_plan"
    assert payload["manifests"][1]["manifest_family"] is None
    assert payload["manifests"][1]["manifest_type"] is None
    assert any("FROM app_manifests" in query for query, _ in pg.executed)


def test_handle_manifests_get_filters_control_plane_rows() -> None:
    pg = _RecordingPg(
        manifest_rows={
            "plan_123": {
                "id": "plan_123",
                "name": "Data Cleanup Plan",
                "description": "Plan for cleanup",
                "status": "draft",
                "updated_at": "2026-04-15T12:00:00+00:00",
                "manifest": {
                    "kind": "praxis_control_manifest",
                    "manifest_family": "control_plane",
                    "manifest_type": "data_plan",
                },
            },
            "manifest_456": {
                "id": "manifest_456",
                "name": "Support Workspace",
                "description": "Workspace bundle",
                "status": "active",
                "updated_at": "2026-04-14T12:00:00+00:00",
                "manifest": {
                    "kind": "helm_surface_bundle",
                    "tabs": [{"id": "main"}],
                },
            },
        }
    )
    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/manifests?manifest_family=control_plane&status=draft&limit=10",
    )

    workflow_query._handle_manifests_get(request, "/api/manifests")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["count"] == 1
    assert payload["manifests"][0]["id"] == "plan_123"
    assert payload["manifests"][0]["manifest_family"] == "control_plane"
    assert payload["filters"]["manifest_family"] == "control_plane"
    assert payload["filters"]["status"] == "draft"


def test_handle_manifests_get_combines_search_and_manifest_filters() -> None:
    pg = _RecordingPg(
        manifest_rows={
            "plan_123": {
                "id": "plan_123",
                "name": "Data Cleanup Plan",
                "description": "Cleanup and repair plan",
                "status": "draft",
                "updated_at": "2026-04-15T12:00:00+00:00",
                "manifest": {
                    "kind": "praxis_control_manifest",
                    "manifest_family": "control_plane",
                    "manifest_type": "data_plan",
                },
            },
            "plan_456": {
                "id": "plan_456",
                "name": "Approval Record",
                "description": "Approval companion manifest",
                "status": "draft",
                "updated_at": "2026-04-14T12:00:00+00:00",
                "manifest": {
                    "kind": "praxis_control_manifest",
                    "manifest_family": "control_plane",
                    "manifest_type": "data_approval",
                },
            },
        }
    )
    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/manifests?q=cleanup&manifest_family=control_plane&manifest_type=data_plan&limit=5",
    )

    workflow_query._handle_manifests_get(request, "/api/manifests")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["count"] == 1
    assert payload["manifests"][0]["id"] == "plan_123"
    assert payload["manifests"][0]["manifest_type"] == "data_plan"
    assert payload["filters"]["q"] == "cleanup"
    assert payload["filters"]["manifest_type"] == "data_plan"


def test_handle_manifest_heads_get_filters_control_plane_rows() -> None:
    pg = _RecordingPg(
        manifest_rows={
            "bundle_789": {
                "id": "bundle_789",
                "name": "Workspace Bundle",
                "description": "Not a control manifest",
                "status": "active",
                "version": 1,
                "parent_manifest_id": None,
                "created_at": "2026-04-15T12:00:00+00:00",
                "updated_at": "2026-04-15T12:00:00+00:00",
                "manifest": {
                    "kind": "helm_surface_bundle",
                    "tabs": [{"id": "main"}],
                },
            },
        },
        head_rows=[
            {
                "workspace_ref": "workspace.alpha",
                "scope_ref": "scope.beta",
                "manifest_type": "data_plan",
                "head_manifest_id": "plan_123",
                "head_status": "draft",
                "recorded_at": "2026-04-15T12:05:00+00:00",
                "id": "plan_123",
                "name": "Data Cleanup Plan",
                "description": "Plan for cleanup",
                "status": "draft",
                "version": 3,
                "parent_manifest_id": None,
                "created_at": "2026-04-15T12:00:00+00:00",
                "updated_at": "2026-04-15T12:05:00+00:00",
                "manifest": {
                    "kind": "praxis_control_manifest",
                    "manifest_family": "control_plane",
                    "manifest_type": "data_plan",
                    "workspace_ref": "workspace.alpha",
                    "scope_ref": "scope.beta",
                    "status": "draft",
                },
            },
            {
                "workspace_ref": "workspace.other",
                "scope_ref": "scope.beta",
                "manifest_type": "data_plan",
                "head_manifest_id": "plan_456",
                "head_status": "draft",
                "recorded_at": "2026-04-15T12:00:30+00:00",
                "id": "plan_456",
                "name": "Other Plan",
                "description": "Different workspace",
                "status": "draft",
                "version": 1,
                "parent_manifest_id": None,
                "created_at": "2026-04-15T12:00:00+00:00",
                "updated_at": "2026-04-15T12:00:30+00:00",
                "manifest": {
                    "kind": "praxis_control_manifest",
                    "manifest_family": "control_plane",
                    "manifest_type": "data_plan",
                    "workspace_ref": "workspace.other",
                    "scope_ref": "scope.beta",
                    "status": "draft",
                },
            },
        ],
    )
    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/manifest-heads?workspace_ref=workspace.alpha&scope_ref=scope.beta&manifest_type=data_plan&status=draft&limit=10",
    )

    workflow_query._handle_manifest_heads_get(request, "/api/manifest-heads")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["count"] == 1
    assert payload["heads"][0]["id"] == "plan_123"
    assert payload["heads"][0]["manifest_family"] == "control_plane"
    assert payload["heads"][0]["manifest_type"] == "data_plan"
    assert payload["heads"][0]["workspace_ref"] == "workspace.alpha"
    assert payload["heads"][0]["scope_ref"] == "scope.beta"
    assert payload["heads"][0]["head_manifest_id"] == "plan_123"
    assert payload["heads"][0]["head_status"] == "draft"
    assert payload["filters"]["workspace_ref"] == "workspace.alpha"
    assert payload["filters"]["scope_ref"] == "scope.beta"
    assert payload["filters"]["status"] == "draft"


def test_handle_manifest_history_get_filters_control_plane_rows() -> None:
    pg = _RecordingPg(
        history_rows=[
            {
                "id": "hist_123",
                "manifest_id": "plan_123",
                "version": 2,
                "manifest_snapshot": {
                    "kind": "praxis_control_manifest",
                    "manifest_family": "control_plane",
                    "manifest_type": "data_approval",
                    "workspace_ref": "workspace.alpha",
                    "scope_ref": "scope.beta",
                    "status": "approved",
                },
                "change_description": "Approved control manifest",
                "changed_by": "ops",
                "created_at": "2026-04-15T12:10:00+00:00",
            },
            {
                "id": "hist_999",
                "manifest_id": "bundle_789",
                "version": 1,
                "manifest_snapshot": {
                    "kind": "helm_surface_bundle",
                    "status": "active",
                    "workspace_ref": "workspace.alpha",
                },
                "change_description": "Bundle history",
                "changed_by": "system",
                "created_at": "2026-04-15T12:00:00+00:00",
            },
        ]
    )
    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/manifests/history?workspace_ref=workspace.alpha&scope_ref=scope.beta&manifest_type=data_approval&status=approved&limit=10",
    )

    workflow_query._handle_manifest_history_get(request, "/api/manifests/history")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["count"] == 1
    assert payload["history"][0]["id"] == "hist_123"
    assert payload["history"][0]["manifest_id"] == "plan_123"
    assert payload["history"][0]["manifest_family"] == "control_plane"
    assert payload["history"][0]["manifest_type"] == "data_approval"


def test_handle_handoff_latest_get_dispatches_through_api_routes(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _fake_latest(query, subsystems):
        captured["query"] = query
        captured["subsystems"] = subsystems
        return {
            "artifact": {
                "artifact_kind": query.artifact_kind,
                "revision_ref": "definition-2",
            },
            "history": [],
            "count": 1,
            "scope": "latest",
        }

    monkeypatch.setattr(handoff_queries, "handle_query_handoff_latest", _fake_latest)
    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: object()),
        path="/api/handoff/latest?artifact_kind=definition&artifact_ref=definition-1&input_fingerprint=fp-123",
    )

    handled = api_handlers.handle_get_request(request, "/api/handoff/latest")

    assert handled is True
    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["artifact"]["revision_ref"] == "definition-2"
    assert payload["filters"] == {
        "artifact_kind": "definition",
        "artifact_ref": "definition-1",
        "input_fingerprint": "fp-123",
    }
    assert captured["query"].artifact_kind == "definition"
    assert captured["query"].artifact_ref == "definition-1"
    assert captured["query"].input_fingerprint == "fp-123"


def test_handle_handoff_status_get_requires_run_id() -> None:
    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: object()),
        path="/api/handoff/status?subscription_id=sub-123",
    )

    handled = api_handlers.handle_get_request(request, "/api/handoff/status")

    assert handled is True
    assert request.sent is not None
    status, payload = request.sent
    assert status == 400
    assert payload["error"] == "run_id is required for handoff queries"


def test_api_rest_registers_control_manifest_history_routes() -> None:
    from surfaces.api import rest as api_rest

    paths = {route.path for route in api_rest.app.routes if getattr(route, "path", None)}
    assert "/api/manifest-heads" in paths
    assert "/api/manifests/history" in paths


def test_handle_decompose_returns_estimates_and_files() -> None:
    result = workflow_query._handle_decompose(
        None,
        {
            "objective": "Fix the dashboard",
            "scope_files": ["Code&DBs/Workflow/surfaces/app/src/dashboard/Dashboard.tsx"],
        },
    )

    assert result["total_sprints"] == 1
    assert result["sprints"][0]["estimate_minutes"] == 10
    assert result["sprints"][0]["files"] == [
        "Code&DBs/Workflow/surfaces/app/src/dashboard/Dashboard.tsx"
    ]


def test_handle_query_quality_rollup_missing_is_structured() -> None:
    subs = SimpleNamespace(
        get_quality_views_mod=lambda: SimpleNamespace(QualityWindow=SimpleNamespace(DAILY="daily")),
        get_quality_materializer=lambda: SimpleNamespace(latest_rollup=lambda _window: None),
    )

    result = workflow_query_core.handle_query(subs, {"question": "pass rate"})

    assert result["routed_to"] == "quality_views"
    assert result["status"] == "empty"
    assert result["reason_code"] == "quality_views.no_rollup_data"
    assert result["rollup"] is None


@pytest.mark.parametrize("question", ["issue", "issues", "open issues", "issue backlog"])
def test_handle_query_routes_issue_backlog(monkeypatch, question: str) -> None:
    captured: dict[str, Any] = {}

    def _execute(subsystems, *, operation_name: str, payload: dict[str, Any]):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {
            "kind": "issue_backlog",
            "count": 1,
            "issues": [{"issue_id": "issue.alpha"}],
        }

    monkeypatch.setattr(
        "runtime.operation_catalog_gateway.execute_operation_from_subsystems",
        _execute,
    )

    result = workflow_query_core.handle_query(
        SimpleNamespace(
            get_bug_tracker=lambda: pytest.fail(
                "bug tracker should not be consulted for issue backlog intent"
            )
        ),
        {"question": question},
    )

    assert result["routed_to"] == "issue_backlog"
    assert result["kind"] == "issue_backlog"
    assert result["issues"][0]["issue_id"] == "issue.alpha"
    assert captured["operation_name"] == "operator.issue_backlog"
    assert captured["payload"] == {"limit": 25, "open_only": True}


def test_handle_query_keeps_failure_questions_out_of_issue_backlog() -> None:
    class _ReceiptIngester:
        def load_recent(self, *, since_hours: int):
            assert since_hours == 24
            return [{"failure_code": "runtime_failed"}]

        def top_failure_codes(self, receipts):
            assert receipts == [{"failure_code": "runtime_failed"}]
            return [{"failure_code": "runtime_failed", "count": 1}]

    result = workflow_query_core.handle_query(
        SimpleNamespace(get_receipt_ingester=lambda: _ReceiptIngester()),
        {"question": "what issue caused this failure"},
    )

    assert result["routed_to"] == "failures"
    assert result["top_failure_codes"] == [{"failure_code": "runtime_failed", "count": 1}]
    assert result["total_receipts_checked"] == 1


def test_handle_query_routes_bug_questions_to_bug_tracker(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class _BugTracker:
        def list_bugs(self, *, limit: int):
            captured["limit"] = limit
            return [SimpleNamespace(bug_id="BUG-001")]

    monkeypatch.setattr(
        workflow_query_core,
        "_annotate_bug_dicts_with_replay_state",
        lambda bt, bugs, **kwargs: [{"bug_id": "BUG-001", "replay_ready": False}],
    )

    result = workflow_query_core.handle_query(
        SimpleNamespace(get_bug_tracker=lambda: _BugTracker()),
        {"question": "bug"},
    )

    assert captured == {"limit": 20}
    assert result == {
        "routed_to": "bug_tracker",
        "bugs": [{"bug_id": "BUG-001", "replay_ready": False}],
        "count": 1,
    }


def test_handle_query_routes_operator_graph_view(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _handle_operator_view(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
        captured["subsystems"] = subs
        captured["body"] = dict(body)
        return {
            "view": "operator_graph",
            "payload": {"semantic_authority_state": "ready"},
        }

    monkeypatch.setattr(workflow_query_core, "handle_operator_view", _handle_operator_view)
    subs = SimpleNamespace()

    result = workflow_query_core.handle_query(
        subs,
        {
            "question": "show me the operator graph",
            "as_of": "2026-04-16T20:05:00+00:00",
        },
    )

    assert captured["subsystems"] is subs
    assert captured["body"] == {
        "view": "operator_graph",
        "as_of": "2026-04-16T20:05:00+00:00",
    }
    assert result["view"] == "operator_graph"
    assert result["payload"]["semantic_authority_state"] == "ready"


def test_handle_query_routes_semantic_assertions_view(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _handle_operator_view(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
        captured["subsystems"] = subs
        captured["body"] = dict(body)
        return {
            "view": "semantics",
            "returned_count": 1,
            "semantic_assertions": [{"predicate_slug": "grouped_in"}],
        }

    monkeypatch.setattr(workflow_query_core, "handle_operator_view", _handle_operator_view)
    subs = SimpleNamespace()

    result = workflow_query_core.handle_query(
        subs,
        {
            "question": "show me semantic assertions",
            "predicate_slug": "grouped_in",
            "subject_kind": "roadmap_item",
            "active_only": False,
            "limit": 7,
        },
    )

    assert captured["subsystems"] is subs
    assert captured["body"] == {
        "view": "semantics",
        "predicate_slug": "grouped_in",
        "subject_kind": "roadmap_item",
        "subject_ref": None,
        "object_kind": None,
        "object_ref": None,
        "source_kind": None,
        "source_ref": None,
        "active_only": False,
        "as_of": None,
        "limit": 7,
    }
    assert result["view"] == "semantics"
    assert result["returned_count"] == 1


def test_handle_query_rejects_diagnose_query_alias() -> None:
    subs = SimpleNamespace(
        get_knowledge_graph=lambda: SimpleNamespace(search=lambda *_args, **_kwargs: []),
    )

    result = workflow_query_core.handle_query(subs, {"question": "diagnose run run_abc123"})

    assert result["routed_to"] == "workflow_diagnose"
    assert result["status"] == "unsupported_query_alias"
    assert result["reason_code"] == "workflow_query.diagnose_alias_removed"
    assert result["run_id"] == "run_abc123"
    assert "praxis_diagnose" in result["message"]


def test_handle_query_knowledge_graph_error_is_structured() -> None:
    class _BoomGraph:
        def search(self, *_args, **_kwargs):
            raise RuntimeError("boom")

    subs = SimpleNamespace(get_knowledge_graph=lambda: _BoomGraph())

    result = workflow_query_core.handle_query(subs, {"question": "needle"})

    assert result["routed_to"] == "knowledge_graph"
    assert result["results"] == []
    assert result["status"] == "unavailable"
    assert result["reason_code"] == "knowledge_graph.unavailable"
    assert result["error_type"] == "RuntimeError"
    assert result["error_message"] == "boom"


def test_handle_query_lane_catalog_uses_workflow_bridge(monkeypatch) -> None:
    as_of = datetime(2026, 4, 14, tzinfo=timezone.utc)
    lane = WorkflowLaneAuthorityRecord(
        workflow_lane_id="workflow_lane.review",
        lane_name="review",
        lane_kind="review",
        status="active",
        concurrency_cap=1,
        default_route_kind="review",
        review_required=True,
        retry_policy={"max_attempts": 1},
        effective_from=as_of,
        effective_to=None,
        created_at=as_of,
    )
    policy = WorkflowLanePolicyAuthorityRecord(
        workflow_lane_policy_id="workflow_lane_policy.review",
        workflow_lane_id="workflow_lane.review",
        policy_scope="workflow.review",
        work_kind="review",
        match_rules={"work_kind": "review"},
        lane_parameters={"route_kind": "review"},
        decision_ref="decision:lane-policy:review",
        effective_from=as_of,
        effective_to=None,
        created_at=as_of,
    )

    class _Bridge:
        async def inspect_lane_catalog(self, *, as_of: datetime):
            assert as_of.tzinfo is not None
            return WorkflowLaneCatalog(
                lane_records=(lane,),
                lane_policy_records=(policy,),
                as_of=as_of,
            )

    subs = SimpleNamespace(get_pg_conn=lambda: object())
    monkeypatch.setattr(workflow_query_core, "_build_workflow_bridge", lambda _subs: _Bridge())

    result = workflow_query_core.handle_query(subs, {"question": "lane catalog"})

    assert result["routed_to"] == "workflow_bridge"
    assert result["view"] == "lane_catalog"
    assert result["lane_count"] == 1
    assert result["policy_count"] == 1
    assert result["lane_names"] == ["review"]
    assert result["policy_keys"] == [["workflow.review", "review"]]
    assert result["catalog"]["lane_records"][0]["lane_name"] == "review"
    assert result["catalog"]["lane_policy_records"][0]["work_kind"] == "review"


def test_handle_constraints_empty_response_is_machine_first() -> None:
    subs = SimpleNamespace(get_constraint_ledger=lambda: SimpleNamespace(list_all=lambda **_kwargs: []))

    result = workflow_query_core.handle_constraints(subs, {"action": "list"})

    assert result["status"] == "empty"
    assert result["reason_code"] == "constraints.none_found"
    assert result["count"] == 0
    assert result["constraints"] == []


def test_handle_operator_view_requires_run_id_for_run_scoped_views() -> None:
    try:
        workflow_query_core.handle_operator_view(None, {"view": "status"})
    except workflow_query_core._ClientError as exc:
        assert str(exc) == "run_id is required for operator view 'status'"
    else:  # pragma: no cover - defensive
        raise AssertionError("expected run_id validation error")


def test_handle_operator_view_status_returns_direct_payload(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _execute(subsystems, *, operation_name: str, payload: dict[str, Any]):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"view": "status", "run_id": payload["run_id"]}

    monkeypatch.setattr(
        "runtime.operation_catalog_gateway.execute_operation_from_subsystems",
        _execute,
    )

    result = workflow_query_core.handle_operator_view(
        SimpleNamespace(),
        {"view": "status", "run_id": "run_123"},
    )

    assert result == {"view": "status", "run_id": "run_123"}
    assert captured["operation_name"] == "operator.run_status"
    assert captured["payload"] == {"run_id": "run_123"}


def test_handle_operator_view_replay_ready_bugs_returns_direct_payload(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _execute(subsystems, *, operation_name: str, payload: dict[str, Any]):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"view": "replay_ready_bugs", "bugs": [], "returned_count": 0}

    monkeypatch.setattr(
        "runtime.operation_catalog_gateway.execute_operation_from_subsystems",
        _execute,
    )

    result = workflow_query_core.handle_operator_view(
        SimpleNamespace(),
        {"view": "replay_ready_bugs", "limit": 10},
    )

    assert result["view"] == "replay_ready_bugs"
    assert captured["operation_name"] == "operator.replay_ready_bugs"
    assert captured["payload"] == {"limit": 10}


def test_handle_operator_view_replay_ready_bugs_rejects_refresh_backfill() -> None:
    with pytest.raises(workflow_query_core._ClientError, match="read-only"):
        workflow_query_core.handle_operator_view(
            SimpleNamespace(),
            {"view": "replay_ready_bugs", "limit": 10, "refresh_backfill": True},
        )


def test_handle_operator_view_semantics_uses_unified_semantic_substrate(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _execute(subsystems, *, operation_name: str, payload: dict[str, Any]):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {
            "semantic_assertions": [
                {
                    "semantic_assertion_id": "semantic_assertion.grouped_in.abc123",
                    "predicate": {"slug": "grouped_in"},
                    "subject": {"kind": "bug", "ref": "bug.semantic.checkout"},
                    "object": {
                        "kind": "functional_area",
                        "ref": "functional_area.checkout",
                    },
                }
            ],
            "projection_source": "semantic_current_assertions",
            "active_only": True,
            "as_of": "2026-04-16T20:05:00+00:00",
            "filters": {"predicate_slug": "grouped_in", "limit": 7},
        }

    monkeypatch.setattr(
        "runtime.operation_catalog_gateway.execute_operation_from_subsystems",
        _execute,
    )

    result = workflow_query_core.handle_operator_view(
        SimpleNamespace(),
        {
            "view": "semantics",
            "predicate_slug": "grouped_in",
            "subject_kind": "bug",
            "active_only": True,
            "as_of": "2026-04-16T20:05:00+00:00",
            "limit": 7,
        },
    )

    assert captured["operation_name"] == "semantic_assertions.list"
    assert captured["payload"]["predicate_slug"] == "grouped_in"
    assert captured["payload"]["subject_kind"] == "bug"
    assert captured["payload"]["limit"] == 7
    assert captured["payload"]["as_of"].isoformat() == "2026-04-16T20:05:00+00:00"
    assert len(result["semantic_assertions"]) == 1
    assert result["projection_source"] == "semantic_current_assertions"
    assert result["semantic_assertions"][0]["subject"]["ref"] == "bug.semantic.checkout"


def test_handle_operator_view_operator_graph_uses_semantic_projection(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _execute(subsystems, *, operation_name: str, payload: dict[str, Any]):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {
            "view": "operator_graph",
            "payload": {"semantic_authority_state": "ready"},
        }

    monkeypatch.setattr(
        "runtime.operation_catalog_gateway.execute_operation_from_subsystems",
        _execute,
    )

    result = workflow_query_core.handle_operator_view(
        SimpleNamespace(),
        {
            "view": "operator_graph",
            "as_of": "2026-04-16T20:05:00+00:00",
        },
    )

    assert captured["operation_name"] == "operator.graph_projection"
    assert captured["payload"]["as_of"].isoformat() == "2026-04-16T20:05:00+00:00"
    assert result["view"] == "operator_graph"


def test_handle_bugs_resolve_fixed_requires_validates_fix_evidence() -> None:
    class _BugTracker:
        def resolve(self, bug_id: str, status: str):
            assert bug_id == "BUG-123"
            assert status == "FIXED"
            raise ValueError(
                "resolve() status FIXED requires validates_fix evidence for BUG-123"
            )

    class _BugTrackerMod:
        class BugStatus:
            FIXED = "FIXED"
            WONT_FIX = "WONT_FIX"
            DEFERRED = "DEFERRED"

    subs = SimpleNamespace(
        get_bug_tracker=lambda: _BugTracker(),
        get_bug_tracker_mod=lambda: _BugTrackerMod(),
    )

    try:
        workflow_query_core.handle_bugs(
            subs,
            {
                "action": "resolve",
                "bug_id": "BUG-123",
                "status": "FIXED",
            },
            parse_bug_status=lambda _mod, raw: raw,
            parse_bug_severity=lambda _mod, raw: raw,
            parse_bug_category=lambda _mod, raw: raw,
        )
    except workflow_query_core._ClientError as exc:
        assert "validates_fix" in str(exc)
    else:
        raise AssertionError("expected resolve to fail closed without validates_fix evidence")


def test_handle_bugs_list_uses_injected_parser_contract() -> None:
    captured: dict[str, Any] = {}

    class _Bug:
        bug_id = "BUG-123"
        title = "authority drift"
        severity = "P2"
        category = "ARCHITECTURE"
        status = "OPEN"
        description = ""
        filed_by = "workflow_api"
        source_kind = "workflow_api"
        decision_ref = ""
        discovered_in_run_id = None
        discovered_in_receipt_id = None
        owner_ref = None
        tags = ()
        created_at = None
        updated_at = None
        resolved_at = None
        resolution_summary = None
        assigned_to = None
        resume_context = None

    class _BugTracker:
        def count_bugs(self, **kwargs):
            captured["count_bugs"] = kwargs
            return 1

        def list_bugs(self, **kwargs):
            captured["list_bugs"] = kwargs
            return [_Bug()]

        def replay_hint(
            self,
            bug_id: str,
            *,
            receipt_limit: int = 1,
            allow_backfill: bool = True,
        ):
            captured["replay_hint"] = {"bug_id": bug_id, "receipt_limit": receipt_limit}
            return {}

    class _BugTrackerMod:
        class BugStatus:
            FIXED = "FIXED"
            WONT_FIX = "WONT_FIX"
            DEFERRED = "DEFERRED"

    subs = SimpleNamespace(
        get_bug_tracker=lambda: _BugTracker(),
        get_bug_tracker_mod=lambda: _BugTrackerMod(),
    )

    result = workflow_query_core.handle_bugs(
        subs,
        {
            "action": "list",
            "status": "openish",
            "severity": "seriousish",
            "category": "archish",
            "limit": 1,
        },
        parse_bug_status=lambda _mod, raw: f"parsed-status:{raw}",
        parse_bug_severity=lambda _mod, raw: f"parsed-severity:{raw}",
        parse_bug_category=lambda _mod, raw: f"parsed-category:{raw}",
    )

    assert captured["count_bugs"]["status"] == "parsed-status:openish"
    assert captured["count_bugs"]["severity"] == "parsed-severity:seriousish"
    assert captured["count_bugs"]["category"] == "parsed-category:archish"
    assert captured["list_bugs"]["status"] == "parsed-status:openish"
    assert captured["list_bugs"]["severity"] == "parsed-severity:seriousish"
    assert captured["list_bugs"]["category"] == "parsed-category:archish"
    assert result["returned_count"] == 1
    assert "replay_hint" not in captured
    assert "replay_ready" not in result["bugs"][0]


def test_handle_bugs_search_include_replay_state_uses_read_only_hint() -> None:
    captured: dict[str, Any] = {}

    class _Bug:
        bug_id = "BUG-321"
        title = "replayable drift"
        severity = "P2"
        category = "RUNTIME"
        status = "OPEN"
        description = ""
        filed_by = "workflow_api"
        source_kind = "workflow_api"
        decision_ref = ""
        discovered_in_run_id = None
        discovered_in_receipt_id = None
        owner_ref = None
        tags = ()
        created_at = None
        updated_at = None
        resolved_at = None
        resolution_summary = None
        assigned_to = None
        resume_context = None

    class _BugTracker:
        def search(self, *args, **kwargs):
            captured["search"] = {"args": args, "kwargs": kwargs}
            return [_Bug()]

        def replay_hint(
            self,
            bug_id: str,
            *,
            receipt_limit: int = 1,
            allow_backfill: bool = True,
        ):
            captured["replay_hint"] = {
                "bug_id": bug_id,
                "receipt_limit": receipt_limit,
                "allow_backfill": allow_backfill,
            }
            return {
                "available": True,
                "reason_code": "bug.replay_ready",
                "run_id": "run-321",
                "receipt_id": "receipt-321",
                "automatic": False,
            }

    class _BugTrackerMod:
        class BugStatus:
            FIXED = "FIXED"
            WONT_FIX = "WONT_FIX"
            DEFERRED = "DEFERRED"

    subs = SimpleNamespace(
        get_bug_tracker=lambda: _BugTracker(),
        get_bug_tracker_mod=lambda: _BugTrackerMod(),
    )

    result = workflow_query_core.handle_bugs(
        subs,
        {
            "action": "search",
            "title": "replayable drift",
            "limit": 1,
            "include_replay_state": True,
        },
        parse_bug_status=lambda _mod, raw: raw,
        parse_bug_severity=lambda _mod, raw: raw,
        parse_bug_category=lambda _mod, raw: raw,
    )

    assert captured["search"]["args"] == ("replayable drift",)
    assert captured["replay_hint"] == {
        "bug_id": "BUG-321",
        "receipt_limit": 1,
        "allow_backfill": False,
    }
    assert result["bugs"][0]["replay_ready"] is True


def test_handle_operator_view_issue_backlog_returns_direct_payload(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _execute(subsystems, *, operation_name: str, payload: dict[str, Any]):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {
            "view": "issue_backlog",
            "count": 1,
            "issues": [{"issue_id": "issue.alpha", "status": "open"}],
        }

    monkeypatch.setattr(
        "runtime.operation_catalog_gateway.execute_operation_from_subsystems",
        _execute,
    )

    result = workflow_query_core.handle_operator_view(
        SimpleNamespace(),
        {"view": "issue_backlog", "limit": 7, "open_only": False, "status": "open"},
    )

    assert result["view"] == "issue_backlog"
    assert result["count"] == 1
    assert result["issues"][0]["issue_id"] == "issue.alpha"
    assert captured["operation_name"] == "operator.issue_backlog"
    assert captured["payload"] == {"limit": 7, "open_only": False, "status": "open"}


def test_handle_friction_empty_stats_is_machine_first() -> None:
    class _Ledger:
        def stats(self, *, include_test: bool):
            assert include_test is False
            return SimpleNamespace(total=0)

    subs = SimpleNamespace(get_friction_ledger=lambda: _Ledger())

    result = workflow_query_core.handle_friction(subs, {"action": "stats"})

    assert result["status"] == "empty"
    assert result["reason_code"] == "friction.none_recorded"
    assert result["total"] == 0
    assert result["by_type"] == {}
    assert result["by_source"] == {}


def test_handle_artifacts_empty_stats_is_machine_first() -> None:
    subs = SimpleNamespace(get_artifact_store=lambda: SimpleNamespace(stats=lambda: {"total_artifacts": 0, "by_type": {}}))

    result = workflow_query_core.handle_artifacts(subs, {"action": "stats"})

    assert result["status"] == "empty"
    assert result["reason_code"] == "artifacts.none_recorded"
    assert result["total_artifacts"] == 0


def test_handle_research_no_hits_is_machine_first() -> None:
    class _Executor:
        def search_local(self, _query: str):
            return SimpleNamespace(hits=[])

    subs = SimpleNamespace(get_memory_engine=lambda: object())

    with patch("memory.research_runtime.ResearchExecutor", return_value=_Executor()):
        result = workflow_query_core.handle_research(subs, {"action": "search", "query": "needle"})

    assert result["status"] == "empty"
    assert result["reason_code"] == "research.no_hits"
    assert result["count"] == 0
    assert result["hits"] == []


def test_handle_decompose_empty_result_is_machine_first() -> None:
    class _Decomposer:
        def decompose(self, _objective: str, _scope_files: list[str]):
            return []

    with patch("runtime.sprint_decomposer.SprintDecomposer", return_value=_Decomposer()):
        result = workflow_query_core.handle_decompose(
            None,
            {
                "objective": "Fix the dashboard",
                "scope_files": ["Code&DBs/Workflow/surfaces/app/src/dashboard/Dashboard.tsx"],
            },
        )

    assert result["status"] == "empty"
    assert result["reason_code"] == "decompose.no_sprints"
    assert result["sprints"] == []
