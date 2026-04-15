from __future__ import annotations

import io
import json
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from runtime import canonical_workflows
from runtime.compile_index import CompileIndexAuthorityError
from surfaces.api.handlers import workflow_query
from surfaces.api.handlers import workflow_query_core


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
        workflow_rows: dict[str, dict[str, Any]] | None = None,
        market_rows: list[dict[str, Any]] | None = None,
        binding_rows: list[dict[str, Any]] | None = None,
        workflow_run_rows: list[dict[str, Any]] | None = None,
        trigger_rows: list[dict[str, Any]] | None = None,
        packet_query_rows: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.reference_rows = reference_rows or []
        self.integration_rows = integration_rows or []
        self.capability_rows = capability_rows or []
        self.manifest_rows = manifest_rows or {}
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
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, query: str, *params: Any) -> list[dict[str, Any]]:
        self.executed.append((query, params))
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
            row = {
                "id": params[0],
                "workflow_id": params[1],
                "event_type": params[2],
                "filter": json.loads(params[3]),
                "enabled": params[4],
                "cron_expression": params[5],
            }
            self.trigger_rows.append(row)
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
            workflow["description"] = params[1]
            workflow["definition"] = json.loads(params[2])
            workflow["compiled_spec"] = json.loads(params[3]) if params[3] is not None else None
            workflow["version"] = int(workflow.get("version") or 1) + 1
            workflow["updated_at"] = "2026-04-09T20:00:00Z"
            return workflow
        return super().fetchrow(query, *params)

    def execute(self, query: str, *params: Any) -> list[dict[str, Any]]:
        if query.strip().startswith("DELETE FROM workflow_triggers WHERE workflow_id = $1"):
            self.trigger_rows = [row for row in self.trigger_rows if str(row.get("workflow_id")) != str(params[0])]
            return []
        if query.strip().startswith("INSERT INTO workflow_triggers"):
            self.trigger_rows.append(
                {
                    "id": params[0],
                    "workflow_id": params[1],
                    "event_type": params[2],
                    "filter": json.loads(params[3]),
                    "cron_expression": params[4],
                    "enabled": True,
                }
            )
            return []
        return super().execute(query, *params)


def test_handle_compile_post_returns_definition_only_and_preserves_nonfatal_error() -> None:
    pg = SimpleNamespace(label="compile-test-pg")
    snapshot = SimpleNamespace(
        compile_index_ref="compile_index.compiler.test",
        compile_surface_revision="compile_surface.compiler.test",
    )
    request = _RequestStub(
        {"prose": "Route support mail with @gmail/search", "title": "Support Mail"},
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
    )
    result = {
        "definition": {
            "type": "operating_model",
            "source_prose": "Route support mail with @gmail/search",
            "compiled_prose": "Route support mail with @gmail/search",
            "narrative_blocks": [],
            "references": [],
            "capabilities": [{"slug": "research/local-knowledge", "title": "Local knowledge recall"}],
            "authority": "",
            "sla": {},
            "trigger_intent": [],
            "draft_flow": [],
            "definition_revision": "def_1234",
            "execution_setup": {
                "setup_version": 1,
                "setup_state": "compiled_preview",
                "planner_required": True,
                "title": "Support Mail",
                "task_class": "research",
                "runtime_profile_ref": "compile/research.grounded",
                "method": {
                    "key": "grounded_research",
                    "label": "Grounded Research",
                    "summary": "Ground the work before planning.",
                },
                "constraints": {},
                "budget_policy": {},
                "phases": [],
                "reference_slugs": [],
                "capability_slugs": [],
            },
            "surface_manifest": {
                "version": 1,
                "headline": "Built a Grounded Research setup for Support Mail.",
                "badges": ["Grounded Research"],
                "surface_now": {
                    "metrics": [],
                    "approaches": [],
                    "objects": [],
                    "workflows": [],
                    "commands": [],
                },
            },
            "build_receipt": {
                "version": 1,
                "summary": "Built a Grounded Research setup with 0 phases.",
                "explanation": "Compile built the setup and kept planning explicit.",
                "decisions": [],
                "tradeoffs": [],
                "authority_refs": [],
                "data_audit": {"transport_mode": "definition_embedded"},
                "data_gaps": [],
            },
        },
        "unresolved": [],
        "error": "catalog_load_failed: missing seed data",
    }

    with patch("runtime.compile_index.load_compile_index_snapshot", return_value=snapshot) as load_mock, patch(
        "runtime.compiler.compile_prose",
        return_value=result,
    ) as compile_mock:
        workflow_query._handle_compile_post(request, "/api/compile")

    load_mock.assert_called_once_with(
        pg,
        surface_name="compiler",
        require_fresh=True,
        repo_root=workflow_query.REPO_ROOT,
    )
    compile_mock.assert_called_once_with(
        "Route support mail with @gmail/search",
        title="Support Mail",
        conn=pg,
        compile_index_snapshot=snapshot,
    )
    assert request.sent == (200, result)


def test_load_compile_index_snapshot_for_request_refreshes_missing_snapshot() -> None:
    pg = SimpleNamespace(label="compile-missing-test-pg")
    snapshot = SimpleNamespace(
        compile_index_ref="compile_index.compiler.refreshed",
        compile_surface_revision="compile_surface.compiler.refreshed",
    )
    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/compile",
    )

    with patch(
        "runtime.compile_index.load_compile_index_snapshot",
        side_effect=CompileIndexAuthorityError(
            "compile_index.snapshot_missing",
            "compile index snapshot is missing",
        ),
    ) as load_mock, patch(
        "runtime.compile_index.refresh_compile_index",
        return_value=snapshot,
    ) as refresh_mock:
        conn, hydrated_snapshot = workflow_query._load_compile_index_snapshot_for_request(request)

    assert conn is pg
    assert hydrated_snapshot is snapshot
    load_mock.assert_called_once_with(
        pg,
        surface_name="compiler",
        require_fresh=True,
        repo_root=workflow_query.REPO_ROOT,
    )
    refresh_mock.assert_called_once_with(
        pg,
        repo_root=workflow_query.REPO_ROOT,
        surface_name="compiler",
    )


def test_handle_compile_post_refreshes_stale_compile_index_snapshot() -> None:
    pg = SimpleNamespace(label="compile-stale-test-pg")
    snapshot = SimpleNamespace(
        compile_index_ref="compile_index.compiler.refreshed",
        compile_surface_revision="compile_surface.compiler.refreshed",
    )
    request = _RequestStub(
        {"prose": "Route support mail with @gmail/search", "title": "Support Mail"},
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/compile",
    )
    result = {
        "definition": {
            "type": "operating_model",
            "source_prose": "Route support mail with @gmail/search",
            "compiled_prose": "Route support mail with @gmail/search",
            "narrative_blocks": [],
            "references": [],
            "capabilities": [],
            "authority": "",
            "sla": {},
            "trigger_intent": [],
            "draft_flow": [],
            "definition_revision": "def_refresh",
        },
        "unresolved": [],
        "error": None,
    }

    with patch(
        "runtime.compile_index.load_compile_index_snapshot",
        side_effect=CompileIndexAuthorityError(
            "compile_index.snapshot_stale",
            "compile index snapshot is stale",
            details={"freshness_state": "stale"},
        ),
    ) as load_mock, patch(
        "runtime.compile_index.refresh_compile_index",
        return_value=snapshot,
    ) as refresh_mock, patch(
        "runtime.compiler.compile_prose",
        return_value=result,
    ) as compile_mock:
        workflow_query._handle_compile_post(request, "/api/compile")

    load_mock.assert_called_once_with(
        pg,
        surface_name="compiler",
        require_fresh=True,
        repo_root=workflow_query.REPO_ROOT,
    )
    refresh_mock.assert_called_once_with(
        pg,
        repo_root=workflow_query.REPO_ROOT,
        surface_name="compiler",
    )
    compile_mock.assert_called_once_with(
        "Route support mail with @gmail/search",
        title="Support Mail",
        conn=pg,
        compile_index_snapshot=snapshot,
    )
    assert request.sent == (200, result)


def test_handle_compile_post_returns_explicit_error_when_refresh_fails() -> None:
    pg = SimpleNamespace(label="compile-refresh-fail-pg")
    request = _RequestStub(
        {"prose": "Route support mail with @gmail/search", "title": "Support Mail"},
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/compile",
    )

    with patch(
        "runtime.compile_index.load_compile_index_snapshot",
        side_effect=CompileIndexAuthorityError(
            "compile_index.snapshot_stale",
            "compile index snapshot is stale",
            details={"freshness_state": "stale"},
        ),
    ) as load_mock, patch(
        "runtime.compile_index.refresh_compile_index",
        side_effect=CompileIndexAuthorityError(
            "compile_index.snapshot_stale",
            "compile index snapshot is stale",
            details={"freshness_state": "stale", "reason": "surface_manifest_mismatch"},
        ),
    ) as refresh_mock:
        workflow_query._handle_compile_post(request, "/api/compile")

    load_mock.assert_called_once_with(
        pg,
        surface_name="compiler",
        require_fresh=True,
        repo_root=workflow_query.REPO_ROOT,
    )
    refresh_mock.assert_called_once_with(
        pg,
        repo_root=workflow_query.REPO_ROOT,
        surface_name="compiler",
    )
    assert request.sent == (
        409,
        {
            "error": "compile index snapshot is stale",
            "reason_code": "compile_index.snapshot_stale",
            "details": {
                "freshness_state": "stale",
                "reason": "surface_manifest_mismatch",
            },
        },
    )


def test_load_compile_index_snapshot_for_request_preserves_non_refreshable_errors() -> None:
    pg = SimpleNamespace(label="compile-corrupt-test-pg")
    request = _RequestStub(
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/compile",
    )

    with patch(
        "runtime.compile_index.load_compile_index_snapshot",
        side_effect=CompileIndexAuthorityError(
            "compile_index.invalid_row",
            "compile index row is malformed",
        ),
    ) as load_mock, patch("runtime.compile_index.refresh_compile_index") as refresh_mock:
        try:
            workflow_query._load_compile_index_snapshot_for_request(request)
        except CompileIndexAuthorityError as exc:
            assert exc.reason_code == "compile_index.invalid_row"
        else:
            raise AssertionError("expected compile index loader to preserve malformed-row failures")

    load_mock.assert_called_once_with(
        pg,
        surface_name="compiler",
        require_fresh=True,
        repo_root=workflow_query.REPO_ROOT,
    )
    refresh_mock.assert_not_called()


def test_handle_refine_definition_post_uses_explicit_llm_compile_lane() -> None:
    pg = SimpleNamespace(label="refine-test-pg")
    snapshot = SimpleNamespace(
        compile_index_ref="compile_index.compiler.test",
        compile_surface_revision="compile_surface.compiler.test",
    )
    request = _RequestStub(
        {"prose": "Route support mail with @gmail/search", "title": "Support Mail"},
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
    )
    result = {
        "definition": {
            "type": "operating_model",
            "source_prose": "Route support mail with @gmail/search",
            "compiled_prose": "Use @gmail/search to route support mail before triage-agent reviews the thread.",
            "narrative_blocks": [],
            "references": [],
            "capabilities": [],
            "authority": "",
            "sla": {},
            "trigger_intent": [],
            "draft_flow": [],
            "definition_revision": "def_refined_1234",
        },
        "unresolved": [],
        "error": None,
        "refinement": {
            "requested": True,
            "applied": True,
            "used_llm": True,
            "status": "refined",
            "message": "Refine improved the definition articulation and rebuilt the definition from source prose.",
            "reason": None,
        },
    }

    with patch("runtime.compile_index.load_compile_index_snapshot", return_value=snapshot) as load_mock, patch(
        "runtime.compiler.compile_prose",
        return_value=result,
    ) as compile_mock:
        workflow_query._handle_refine_definition_post(request, "/api/refine-definition")

    load_mock.assert_called_once_with(
        pg,
        surface_name="compiler",
        require_fresh=True,
        repo_root=workflow_query.REPO_ROOT,
    )
    compile_mock.assert_called_once_with(
        "Route support mail with @gmail/search",
        title="Support Mail",
        enable_llm=True,
        conn=pg,
        compile_index_snapshot=snapshot,
    )
    assert request.sent == (200, result)


def test_handle_plan_post_stamps_definition_revision_into_compiled_spec() -> None:
    definition = {
        "type": "operating_model",
        "source_prose": "Review support inbox",
        "compiled_prose": "triage-agent reviews the support inbox when new email arrives.",
        "narrative_blocks": [
            {
                "id": "block-001",
                "title": "Review support inbox",
                "summary": "triage-agent reviews the support inbox when new email arrives.",
                "text": "triage-agent reviews the support inbox when new email arrives.",
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
                "summary": "when new email arrives",
                "event_type": "email.received",
                "filter": {"mailbox": "support"},
                "source_block_ids": ["block-001"],
                "reference_slugs": [],
            }
        ],
        "draft_flow": [
            {
                "id": "step-001",
                "title": "Review support inbox",
                "summary": "Review support inbox",
                "source_block_ids": ["block-001"],
                "reference_slugs": ["triage-agent"],
                "capability_slugs": [],
                "depends_on": [],
                "order": 1,
            }
        ],
        "definition_revision": "def_plan_123",
    }
    request = _RequestStub({"title": "Inbox Triage", "definition": definition})

    workflow_query._handle_plan_post(request, "/api/plan")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["compiled_spec"]["definition_revision"] == "def_plan_123"
    assert payload["compiled_spec"]["jobs"][0]["agent"] == "auto/review"
    assert payload["compiled_spec"]["triggers"] == [
        {
            "event_type": "email.received",
            "filter": {"mailbox": "support"},
            "source_trigger_id": "trigger-001",
        }
    ]


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
    assert payload["build_state"] == "ready"
    trigger_nodes = [node for node in payload["build_graph"]["nodes"] if node.get("route") == "trigger"]
    assert trigger_nodes
    assert trigger_nodes[0]["summary"] == "Start when a new support email arrives."
    assert trigger_nodes[0]["trigger"] == {
        "event_type": "email.received",
        "cron_expression": "",
        "source_ref": "@gmail/search",
        "filter": {"mailbox": "support"},
    }
    assert payload["compiled_spec_projection"]["graph_id"] == payload["build_graph"]["graph_id"]
    assert payload["compiled_spec_projection"]["compiled_spec"]["triggers"] == [
        {
            "event_type": "email.received",
            "filter": {"mailbox": "support"},
            "source_trigger_id": "trigger-001",
            "source_ref": "@gmail/search",
        }
    ]


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
    assert payload["compiled_spec"]["jobs"][0]["source_step_id"] == "step-001"
    assert payload["compiled_spec_projection"]["graph_id"] == payload["build_graph"]["graph_id"]
    assert payload["compiled_spec_projection"]["compiled_spec"]["jobs"][0]["source_node_id"] == "step-001"
    persisted_definition = pg.workflow_rows["wf_build_mutation"]["definition"]
    assert persisted_definition["authority_attachments"][0]["authority_ref"] == "@gmail/search"


def test_handle_workflow_build_post_wires_on_failure_edge_gate_into_compiled_spec() -> None:
    workflow_row = {
        "id": "wf_build_failure_gate",
        "name": "Failure Gate",
        "description": "Compile failure-path flow",
        "definition": {
            "type": "operating_model",
            "source_prose": "Run a fallback when the primary step fails.",
            "compiled_prose": "Run a fallback when the primary step fails.",
            "narrative_blocks": [],
            "references": [],
            "capabilities": [],
            "authority": "",
            "sla": {},
            "trigger_intent": [],
            "draft_flow": [
                {
                    "id": "step-001",
                    "title": "Primary step",
                    "summary": "Run the primary step.",
                    "source_block_ids": [],
                    "reference_slugs": [],
                    "capability_slugs": [],
                    "depends_on": [],
                    "order": 1,
                },
                {
                    "id": "step-002",
                    "title": "Fallback step",
                    "summary": "Run when the primary step fails.",
                    "source_block_ids": [],
                    "reference_slugs": [],
                    "capability_slugs": [],
                    "depends_on": ["step-001"],
                    "order": 2,
                },
            ],
            "definition_revision": "def_build_failure_gate",
        },
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-09T19:00:00Z",
    }
    pg = _MutableWorkflowPg(workflow_rows={"wf_build_failure_gate": workflow_row})
    request = _RequestStub(
        {
            "nodes": [
                {
                    "node_id": "step-001",
                    "kind": "step",
                    "title": "Primary step",
                    "route": "auto/build",
                    "status": "ready",
                    "summary": "Run the primary step.",
                },
                {
                    "node_id": "step-002",
                    "kind": "step",
                    "title": "Fallback step",
                    "route": "auto/build",
                    "status": "ready",
                    "summary": "Run when the primary step fails.",
                },
            ],
            "edges": [
                {
                    "edge_id": "edge-step-001-step-002",
                    "kind": "sequence",
                    "from_node_id": "step-001",
                    "to_node_id": "step-002",
                    "gate": {
                        "state": "configured",
                        "label": "On Failure",
                        "family": "after_failure",
                    },
                }
            ],
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_failure_gate/build/build_graph",
    )

    workflow_query._handle_workflow_build_post(request, "/api/workflows/wf_build_failure_gate/build/build_graph")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["compiled_spec"]["jobs"][1]["depends_on"] == ["primary-step"]
    assert payload["compiled_spec"]["jobs"][1]["dependency_edges"] == [
        {
            "label": "primary-step",
            "edge_type": "after_failure",
        }
    ]
    persisted_definition = pg.workflow_rows["wf_build_failure_gate"]["definition"]
    assert persisted_definition["execution_setup"]["edge_gates"] == [
        {
            "edge_id": "edge-step-001-step-002",
            "from_node_id": "step-001",
            "to_node_id": "step-002",
            "family": "after_failure",
            "label": "On Failure",
        }
    ]


def test_handle_workflow_build_post_treats_trigger_nodes_as_trigger_intent() -> None:
    workflow_row = {
        "id": "wf_build_trigger_nodes",
        "name": "Triggered Flow",
        "description": "Compile graph-authored trigger flow",
        "definition": {
            "type": "operating_model",
            "source_prose": "Start manually, draft a summary, then invoke the downstream workflow.",
            "compiled_prose": "Start manually, draft a summary, then invoke the downstream workflow.",
            "narrative_blocks": [],
            "references": [],
            "capabilities": [],
            "authority": "",
            "sla": {},
            "trigger_intent": [],
            "draft_flow": [],
            "definition_revision": "def_build_trigger_nodes",
        },
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-09T19:00:00Z",
    }
    pg = _MutableWorkflowPg(workflow_rows={"wf_build_trigger_nodes": workflow_row})
    request = _RequestStub(
        {
            "nodes": [
                {
                    "node_id": "node-trigger",
                    "kind": "step",
                    "title": "Manual",
                    "route": "trigger",
                    "status": "ready",
                    "summary": "Run this workflow manually.",
                },
                {
                    "node_id": "step-001",
                    "kind": "step",
                    "title": "Draft summary",
                    "route": "auto/draft",
                    "status": "ready",
                    "summary": "Draft the summary.",
                },
                {
                    "node_id": "step-002",
                    "kind": "step",
                    "title": "Invoke workflow",
                    "route": "@workflow/invoke",
                    "status": "ready",
                    "summary": "Invoke the downstream workflow.",
                },
            ],
            "edges": [
                {
                    "edge_id": "edge-trigger-step-001",
                    "kind": "sequence",
                    "from_node_id": "node-trigger",
                    "to_node_id": "step-001",
                },
                {
                    "edge_id": "edge-step-001-step-002",
                    "kind": "sequence",
                    "from_node_id": "step-001",
                    "to_node_id": "step-002",
                },
            ],
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_trigger_nodes/build/build_graph",
    )

    workflow_query._handle_workflow_build_post(request, "/api/workflows/wf_build_trigger_nodes/build/build_graph")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["compiled_spec"]["triggers"] == [
        {
            "event_type": "manual",
            "filter": {},
            "source_trigger_id": "trigger-001",
        }
    ]
    assert [job["agent"] for job in payload["compiled_spec"]["jobs"]] == [
        "auto/draft",
        "@workflow/invoke",
    ]
    persisted_definition = pg.workflow_rows["wf_build_trigger_nodes"]["definition"]
    assert persisted_definition["trigger_intent"] == [
        {
            "id": "trigger-001",
            "title": "Manual",
            "summary": "Run this workflow manually.",
            "event_type": "manual",
            "filter": {},
            "source_node_id": "node-trigger",
            "source_block_ids": [],
            "reference_slugs": [],
        }
    ]
    assert [step["id"] for step in persisted_definition["draft_flow"]] == ["step-001", "step-002"]


def test_handle_workflow_build_post_preserves_trigger_node_configuration() -> None:
    workflow_row = {
        "id": "wf_build_trigger_config",
        "name": "Configured Trigger Flow",
        "description": "Compile graph-authored configured triggers",
        "definition": {
            "type": "operating_model",
            "source_prose": "Run on a schedule or webhook, then draft a summary.",
            "compiled_prose": "Run on a schedule or webhook, then draft a summary.",
            "narrative_blocks": [],
            "references": [],
            "capabilities": [],
            "authority": "",
            "sla": {},
            "trigger_intent": [],
            "draft_flow": [],
            "definition_revision": "def_build_trigger_config",
        },
        "compiled_spec": None,
        "version": 1,
        "updated_at": "2026-04-09T19:00:00Z",
    }
    pg = _MutableWorkflowPg(workflow_rows={"wf_build_trigger_config": workflow_row})
    request = _RequestStub(
        {
            "nodes": [
                {
                    "node_id": "node-schedule",
                    "kind": "step",
                    "title": "Weekday schedule",
                    "route": "trigger/schedule",
                    "status": "ready",
                    "summary": "Run each weekday morning.",
                    "trigger": {
                        "event_type": "schedule",
                        "cron_expression": "0 9 * * 1-5",
                        "filter": {"timezone": "America/Los_Angeles"},
                    },
                },
                {
                    "node_id": "node-webhook",
                    "kind": "step",
                    "title": "Lead webhook",
                    "route": "trigger/webhook",
                    "status": "ready",
                    "summary": "Run when a lead webhook lands.",
                    "trigger": {
                        "event_type": "db.webhook_events.insert",
                        "source_ref": "@db/webhook-events",
                        "filter": {"table": "crm_leads"},
                    },
                },
                {
                    "node_id": "step-001",
                    "kind": "step",
                    "title": "Draft summary",
                    "route": "auto/draft",
                    "status": "ready",
                    "summary": "Draft the summary.",
                },
            ],
            "edges": [
                {
                    "edge_id": "edge-schedule-step-001",
                    "kind": "sequence",
                    "from_node_id": "node-schedule",
                    "to_node_id": "step-001",
                },
                {
                    "edge_id": "edge-webhook-step-001",
                    "kind": "sequence",
                    "from_node_id": "node-webhook",
                    "to_node_id": "step-001",
                },
            ],
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
        path="/api/workflows/wf_build_trigger_config/build/build_graph",
    )

    workflow_query._handle_workflow_build_post(request, "/api/workflows/wf_build_trigger_config/build/build_graph")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["compiled_spec"]["triggers"] == [
        {
            "event_type": "schedule",
            "filter": {"timezone": "America/Los_Angeles"},
            "source_trigger_id": "trigger-001",
            "cron_expression": "0 9 * * 1-5",
        },
        {
            "event_type": "db.webhook_events.insert",
            "filter": {"table": "crm_leads"},
            "source_trigger_id": "trigger-002",
            "source_ref": "@db/webhook-events",
        },
    ]
    persisted_definition = pg.workflow_rows["wf_build_trigger_config"]["definition"]
    assert persisted_definition["trigger_intent"] == [
        {
            "id": "trigger-001",
            "title": "Weekday schedule",
            "summary": "Run each weekday morning.",
            "event_type": "schedule",
            "filter": {"timezone": "America/Los_Angeles"},
            "cron_expression": "0 9 * * 1-5",
            "source_node_id": "node-schedule",
            "source_block_ids": [],
            "reference_slugs": [],
        },
        {
            "id": "trigger-002",
            "title": "Lead webhook",
            "summary": "Run when a lead webhook lands.",
            "event_type": "db.webhook_events.insert",
            "filter": {"table": "crm_leads"},
            "source_ref": "@db/webhook-events",
            "source_node_id": "node-webhook",
            "source_block_ids": [],
            "reference_slugs": [],
        },
    ]
    assert [step["id"] for step in persisted_definition["draft_flow"]] == ["step-001"]


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
            "admitted_target": {
                "target_ref": "#escalation-policy",
                "label": "Escalation Policy",
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
    assert payload["import_snapshots"][0]["approval_state"] == "admitted"
    assert payload["import_snapshots"][0]["admitted_targets"][0]["target_ref"] == "#escalation-policy"
    assert payload["compiled_spec_projection"]["compiled_spec"]["jobs"][0]["source_node_id"] == "step-001"
    persisted_definition = pg.workflow_rows["wf_build_materialize"]["definition"]
    assert persisted_definition["authority_attachments"][0]["authority_ref"] == "#escalation-policy"
    assert persisted_definition["import_snapshots"][0]["approval_state"] == "admitted"


def test_handle_commit_post_delegates_to_runtime_owner() -> None:
    request = _RequestStub(
        {
            "title": "Inbox Triage",
            "definition": {"type": "operating_model"},
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: object()),
    )

    with patch.object(
        workflow_query,
        "commit_workflow",
        return_value={"workflow_id": "wf_123", "status": "committed"},
    ) as commit_mock:
        workflow_query._handle_commit_post(request, "/api/commit")

    commit_mock.assert_called_once()
    assert request.sent == (200, {"workflow_id": "wf_123", "status": "committed"})


def test_handle_workflow_build_post_delegates_to_runtime_owner() -> None:
    request = _RequestStub(
        {"node_id": "step-001", "authority_kind": "reference", "authority_ref": "@gmail/search"},
        subsystems=SimpleNamespace(get_pg_conn=lambda: object()),
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

    with patch.object(workflow_query, "mutate_workflow_build", return_value=runtime_result) as mutate_mock:
        workflow_query._handle_workflow_build_post(request, "/api/workflows/wf_build/build/attachments")

    mutate_mock.assert_called_once()
    assert request.sent is not None
    assert request.sent[0] == 200
    assert request.sent[1]["workflow"]["id"] == "wf_build"


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


def test_handle_commit_post_persists_current_plan_and_trigger_rows() -> None:
    pg = _RecordingPg()
    request = _RequestStub(
        {
            "title": "Inbox Triage",
            "definition": {
                "type": "operating_model",
                "source_prose": "Handle support email",
                "compiled_prose": "Use @gmail/search before triage-agent responds.",
                "narrative_blocks": [],
                "references": [{"slug": "@gmail/search", "resolved": True, "resolved_to": "integration_registry:gmail/search"}],
                "capabilities": [{"slug": "research/local-knowledge", "title": "Local knowledge recall"}],
                "authority": "",
                "sla": {},
                "trigger_intent": [],
                "draft_flow": [],
                "definition_revision": "def_current_123",
            },
            "compiled_spec": {
                "jobs": [{"label": "search-inbox", "agent": "integration/gmail"}],
                "triggers": [{"event_type": "email.received"}],
                "definition_revision": "def_current_123",
            },
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
    )

    workflow_query._handle_commit_post(request, "/api/commit")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["status"] == "committed"
    assert payload["title"] == "Inbox Triage"
    assert payload["jobs"] == 1
    assert payload["triggers"] == 1


def test_handle_commit_post_keeps_definition_only_workflows_without_current_plan() -> None:
    pg = _RecordingPg()
    request = _RequestStub(
        {
            "title": "Explicit Inbox Triage",
            "definition": {
                "type": "operating_model",
                "source_prose": "Run the explicit inbox flow",
                "compiled_prose": "Run the explicit inbox flow",
                "narrative_blocks": [],
                "references": [],
                "capabilities": [],
                "authority": "",
                "sla": {},
                "trigger_intent": [{"event_type": "email.received"}],
                "jobs": [{"label": "search-inbox", "prompt": "Search the inbox"}],
                "definition_revision": "def_explicit_123",
            },
            "compiled_spec": None,
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
    )

    workflow_query._handle_commit_post(request, "/api/commit")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["jobs"] == 0
    assert payload["triggers"] == 0
    assert payload["has_current_plan"] is False
    workflow_write = next(params for query, params in pg.executed if "INSERT INTO public.workflows" in query)
    compiled_spec_json = workflow_write[4]
    assert compiled_spec_json is None
    assert any("DELETE FROM workflow_triggers WHERE workflow_id = $1" in query for query, _ in pg.executed)
    assert not any("INSERT INTO workflow_triggers" in query for query, _ in pg.executed)


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


def test_handle_commit_post_clears_stale_plan_and_trigger_rows() -> None:
    pg = _RecordingPg()
    request = _RequestStub(
        {
            "title": "Inbox Triage",
            "definition": {
                "type": "operating_model",
                "source_prose": "Handle support email",
                "compiled_prose": "Use @gmail/search before triage-agent responds.",
                "narrative_blocks": [],
                "references": [{"slug": "@gmail/search", "resolved": True, "resolved_to": "integration_registry:gmail/search"}],
                "capabilities": [],
                "authority": "",
                "sla": {},
                "trigger_intent": [],
                "draft_flow": [],
                "definition_revision": "def_fresh_123",
            },
            "compiled_spec": {
                "jobs": [{"label": "search-inbox", "agent": "integration/gmail"}],
                "triggers": [{"event_type": "email.received"}],
                "definition_revision": "def_stale_123",
            },
        },
        subsystems=SimpleNamespace(get_pg_conn=lambda: pg),
    )

    workflow_query._handle_commit_post(request, "/api/commit")

    assert request.sent is not None
    status, payload = request.sent
    assert status == 200
    assert payload["jobs"] == 0
    assert payload["triggers"] == 0
    assert payload["has_current_plan"] is False
    workflow_write = next(params for query, params in pg.executed if "INSERT INTO public.workflows" in query)
    assert workflow_write[4] is None
    assert any("DELETE FROM workflow_triggers WHERE workflow_id = $1" in query for query, _ in pg.executed)
    assert not any("INSERT INTO workflow_triggers" in query for query, _ in pg.executed)


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
        {"error": "Workflow 'Inbox Triage' has no current execution plan. Generate plan first."},
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
    }
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
        {"error": "Workflow 'Explicit Inbox Triage' has no current execution plan. Generate plan first."},
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
        {"error": "Workflow 'wf_name_only' has no current execution plan. Generate plan first."},
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


def test_handle_constraints_empty_response_is_machine_first() -> None:
    subs = SimpleNamespace(get_constraint_ledger=lambda: SimpleNamespace(list_all=lambda **_kwargs: []))

    result = workflow_query_core.handle_constraints(subs, {"action": "list"})

    assert result["status"] == "empty"
    assert result["reason_code"] == "constraints.none_found"
    assert result["count"] == 0
    assert result["constraints"] == []


def test_handle_operator_view_returns_structured_cli_contract() -> None:
    result = workflow_query_core.handle_operator_view(None, {"view": "status"})

    assert result == {
        "view": "status",
        "requires": {
            "runtime": "async_postgres",
            "driver": "asyncpg",
        },
        "cli_command": "workflow native-operator inspect",
    }


def test_handle_operator_view_replay_ready_bugs_returns_direct_payload() -> None:
    class _BugTracker:
        def bulk_backfill_replay_provenance(self, *, open_only: bool, receipt_limit: int):
            assert open_only is True
            assert receipt_limit == 1
            return {
                "scanned_count": 1,
                "backfilled_count": 1,
                "linked_count": 2,
                "replay_ready_count": 1,
                "replay_blocked_count": 0,
                "bugs": [],
            }

        def count_bugs(self, **_kwargs):
            return 1

        def list_bugs(self, *, limit: int, **_kwargs):
            assert limit == 10
            return [
                SimpleNamespace(
                    bug_id="BUG-001",
                    bug_key="bug_001",
                    title="Replay-ready bug",
                    status="OPEN",
                    severity="P2",
                    priority="P2",
                    category="RUNTIME",
                    description="ready",
                    summary="ready",
                    filed_at="2026-04-10T00:00:00+00:00",
                    created_at="2026-04-10T00:00:00+00:00",
                    updated_at="2026-04-10T00:00:00+00:00",
                    resolved_at=None,
                    filed_by="test",
                    assigned_to=None,
                    tags=(),
                    source_kind="manual",
                    discovered_in_run_id=None,
                    discovered_in_receipt_id=None,
                    owner_ref=None,
                    decision_ref="",
                    resolution_summary=None,
                )
            ]

        def replay_hint(self, bug_id: str, *, receipt_limit: int):
            assert bug_id == "BUG-001"
            assert receipt_limit == 1
            return {
                "available": True,
                "reason_code": "bug.replay_ready",
                "run_id": "run-123",
                "receipt_id": "receipt-123",
            }

    class _BugTrackerMod:
        class BugStatus:
            FIXED = "FIXED"
            WONT_FIX = "WONT_FIX"
            DEFERRED = "DEFERRED"

        class BugTracker:
            @staticmethod
            def _normalize_status(raw, default=None):
                return default

            @staticmethod
            def _normalize_severity(raw, default=None):
                return default

            @staticmethod
            def _normalize_category(raw, default=None):
                return default

    subs = SimpleNamespace(
        get_bug_tracker=lambda: _BugTracker(),
        get_bug_tracker_mod=lambda: _BugTrackerMod(),
    )

    result = workflow_query_core.handle_operator_view(
        subs,
        {"view": "replay_ready_bugs", "limit": 10},
    )

    assert result["view"] == "replay_ready_bugs"
    assert result["maintenance"]["backfilled_count"] == 1
    assert result["bugs"][0]["replay_ready"] is True
    assert result["returned_count"] == 1


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
