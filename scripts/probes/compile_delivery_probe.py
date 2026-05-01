#!/usr/bin/env python3
"""Deterministic compile -> Moon delivery probe.

The default probe does not call a live LLM. It mounts the catalog-owned
compile HTTP routes in-process, spoofs the inner compose output into a small
workflow graph, then proves Moon chat can read and mutate that same durable
workflow through the shared workflow build authority.
"""
from __future__ import annotations

import json
import sys
import time
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_ROOT = REPO_ROOT / "Code&DBs" / "Workflow"
if str(WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKFLOW_ROOT))


class _FakeRecognition:
    def __init__(self, intent: str) -> None:
        self.intent = intent

    def to_dict(self) -> dict[str, Any]:
        return {
            "intent": self.intent,
            "spans": [{"text": "Search issues"}, {"text": "Draft summary"}, {"text": "Notify Slack"}],
            "suggested_steps": [
                {"title": "Search issues", "route": "search.github"},
                {"title": "Draft summary", "route": "draft.summary"},
                {"title": "Notify Slack", "route": "notify.slack"},
            ],
            "gaps": [],
            "matches": [],
        }


class _FakeTransaction:
    def __init__(self, conn: "_ProbeConn") -> None:
        self.conn = conn

    def __enter__(self) -> "_ProbeConn":
        self.conn.transaction_enters += 1
        return self.conn

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.conn.transaction_commits += 1
        else:
            self.conn.transaction_rollbacks += 1


class _ProbeConn:
    def __init__(self) -> None:
        self.receipts: dict[str, dict[str, Any]] = {}
        self.events: dict[str, dict[str, Any]] = {}
        self.workflows: dict[str, dict[str, Any]] = {}
        self.build_payloads: dict[str, dict[str, Any]] = {}
        self.transaction_enters = 0
        self.transaction_commits = 0
        self.transaction_rollbacks = 0

    def fetchrow(self, query: str, *args: object) -> dict[str, Any] | None:
        normalized = " ".join(str(query).split())
        if (
            "FROM authority_operation_receipts" in normalized
            and "WHERE receipt_id = $1::uuid" in normalized
        ):
            return self.receipts.get(str(args[0]))
        if "FROM authority_operation_receipts" in normalized:
            return None
        if "FROM public.workflows WHERE id = $1" in normalized:
            return self.workflows.get(str(args[0]))
        return None

    def execute(self, query: str, *args: object) -> str:
        def _json_arg(index: int, default: Any) -> Any:
            raw = args[index]
            if raw is None:
                return default
            if isinstance(raw, str):
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    return default
            return raw

        if "INSERT INTO authority_operation_receipts" in query:
            receipt_id = str(args[0])
            self.receipts[receipt_id] = {
                "receipt_id": receipt_id,
                "operation_ref": args[1],
                "operation_name": args[2],
                "operation_kind": args[3],
                "authority_domain_ref": args[4],
                "authority_ref": args[5],
                "projection_ref": args[6],
                "storage_target_ref": args[7],
                "input_hash": args[8],
                "output_hash": args[9],
                "idempotency_key": args[10],
                "caller_ref": args[11],
                "execution_status": args[12],
                "result_status": args[13],
                "error_code": args[14],
                "error_detail": args[15],
                "event_ids": list(_json_arg(16, [])),
                "projection_freshness": dict(_json_arg(17, {})),
                "result_payload": _json_arg(18, None),
                "duration_ms": args[19],
                "binding_revision": args[20],
                "decision_ref": args[21],
                "cause_receipt_id": args[22],
                "correlation_id": args[23],
            }
            return "INSERT 0 1"
        if "INSERT INTO authority_events" in query:
            event_id = str(args[0])
            self.events[event_id] = {
                "event_id": event_id,
                "authority_domain_ref": args[1],
                "aggregate_ref": args[2],
                "event_type": args[3],
                "event_payload": json.loads(str(args[4])),
                "operation_ref": args[6],
                "receipt_id": str(args[7]),
                "correlation_id": str(args[9]) if args[9] is not None else None,
            }
            return "INSERT 0 1"
        if "UPDATE authority_events" in query:
            receipt_id = str(args[0])
            event_ids = [str(value) for value in (args[1] or [])]
            for event_id in event_ids:
                if event_id in self.events:
                    self.events[event_id]["receipt_id"] = receipt_id
            return "UPDATE 1"
        return "OK"

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self)


def _binding(
    *,
    operation_ref: str,
    operation_name: str,
    operation_kind: str,
    http_method: str,
    http_path: str,
    command_class: Any,
    handler: Any,
    event_required: bool,
    event_type: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        operation_ref=operation_ref,
        operation_name=operation_name,
        source_kind=f"operation_{operation_kind}",
        operation_kind=operation_kind,
        http_method=http_method,
        http_path=http_path,
        command_class=command_class,
        handler=handler,
        authority_ref="authority.workflow_build",
        authority_domain_ref="authority.workflow_build",
        projection_ref=None,
        storage_target_ref="praxis.primary_postgres",
        input_schema_ref="",
        output_schema_ref="",
        idempotency_key_fields=[],
        required_capabilities={},
        allowed_callers=[],
        timeout_ms=360000,
        receipt_required=True,
        event_required=event_required,
        event_type=event_type,
        projection_freshness_policy_ref=None,
        posture="observe" if operation_kind == "query" else "operate",
        idempotency_policy="read_only" if operation_kind == "query" else "non_idempotent",
        binding_revision=f"binding.{operation_ref}.probe",
        decision_ref=f"decision.{operation_ref}.probe",
        summary=operation_name,
    )


def _workflow_row(conn: _ProbeConn, workflow_id: str) -> dict[str, Any]:
    return conn.workflows[workflow_id]


def _graph() -> dict[str, Any]:
    return {
        "graph_id": "probe_graph",
        "schema_version": 1,
        "nodes": [
            {"node_id": "n1", "kind": "step", "title": "Search issues", "route": "search.github"},
            {"node_id": "n2", "kind": "step", "title": "Draft summary", "route": "draft.summary"},
            {"node_id": "n3", "kind": "step", "title": "Notify Slack", "route": "notify.slack"},
        ],
        "edges": [
            {"edge_id": "e1", "kind": "after_success", "from_node_id": "n1", "to_node_id": "n2"},
            {"edge_id": "e2", "kind": "after_success", "from_node_id": "n2", "to_node_id": "n3"},
        ],
    }


def _fake_build_payload(
    probe_conn: _ProbeConn,
    row: dict[str, Any],
    *,
    definition: dict[str, Any] | None = None,
    materialized_spec: dict[str, Any] | None = None,
    build_bundle: dict[str, Any] | None = None,
    planning_notes: list[str] | None = None,
    compile_preview: dict[str, Any] | None = None,
    **_kwargs: Any,
) -> dict[str, Any]:
    workflow_id = str(row["id"])
    if workflow_id in probe_conn.build_payloads and build_bundle is None:
        return dict(probe_conn.build_payloads[workflow_id])
    graph = (build_bundle or {}).get("build_graph") or _graph()
    payload = {
        "ok": True,
        "workflow_id": workflow_id,
        "workflow": {
            "id": workflow_id,
            "name": row.get("name") or "Compile delivery probe",
            "description": row.get("description"),
        },
        "definition": definition or row.get("definition") or {},
        "materialized_spec": materialized_spec or row.get("materialized_spec") or {},
        "planning_notes": planning_notes or [],
        "build_state": "ready",
        "build_graph": graph,
        "projection_status": {"state": "ready"},
        "candidate_resolution_manifest": {"execution_readiness": "ready"},
        "execution_manifest": {"status": "ready"},
        "compile_preview": compile_preview,
    }
    probe_conn.build_payloads[workflow_id] = dict(payload)
    return payload


def _install_spoofs(stack: ExitStack, conn: _ProbeConn) -> None:
    from unittest.mock import patch

    import runtime.materialize_cqrs as compile_cqrs
    import runtime.canonical_workflows as canonical_workflows
    import runtime.workflow_build_moment as workflow_build_moment
    import runtime.operations.queries.workflow_build_get as workflow_build_get
    import runtime.operations.commands.workflow_build as workflow_build_command
    import storage.postgres.workflow_runtime_repository as workflow_runtime_repository

    def _save_workflow(_conn: _ProbeConn, *, workflow_id: str | None, body: dict[str, Any]) -> dict[str, Any]:
        saved_id = str(body.get("id") or workflow_id)
        row = {
            "id": saved_id,
            "name": body.get("name") or "Compile delivery probe",
            "description": body.get("description"),
            "definition": body.get("definition") or {},
            "materialized_spec": body.get("materialized_spec") or {},
            "version": 1,
            "updated_at": "2026-04-29T00:00:00Z",
        }
        conn.workflows[saved_id] = row
        return row

    def _mutate_workflow_build(
        _conn: _ProbeConn,
        *,
        workflow_id: str,
        subpath: str,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        row = _workflow_row(conn, workflow_id)
        if subpath == "bootstrap":
            graph = _graph()
            title = str(body.get("title") or row.get("name") or "Compile delivery probe")
            row["name"] = title
            definition = {
                "workflow_id": workflow_id,
                "source_prose": body.get("prose"),
                "materialized_prose": body.get("prose"),
                "compose_provenance": {"ok": True, "mode": "deterministic_probe"},
            }
            row["definition"] = definition
            row["materialized_spec"] = {"jobs": [{"id": node["node_id"], "title": node["title"]} for node in graph["nodes"]]}
        elif subpath.startswith("nodes/"):
            node_id = subpath.split("/", 1)[1]
            graph = dict(conn.build_payloads[workflow_id]["build_graph"])
            graph["nodes"] = [dict(node) for node in graph["nodes"]]
            for node in graph["nodes"]:
                if node.get("node_id") == node_id:
                    node.update(body)
            definition = row.get("definition") or {}
        else:
            raise RuntimeError(f"probe does not support mutation subpath: {subpath}")

        build_bundle = {
            "build_graph": graph,
            "projection_status": {"state": "ready"},
        }
        mutation = {
            "row": row,
            "definition": definition,
            "materialized_spec": row.get("materialized_spec") or {},
            "build_bundle": build_bundle,
            "planning_notes": [],
            "candidate_resolution_manifest": {"execution_readiness": "ready"},
            "execution_manifest": {"status": "ready"},
            "intent_brief": row.get("description"),
        }
        _fake_build_payload(
            conn,
            row,
            definition=definition,
            materialized_spec=mutation["materialized_spec"],
            build_bundle=build_bundle,
            planning_notes=[],
        )
        return mutation

    stack.enter_context(patch.object(compile_cqrs, "recognize_intent", lambda intent, conn, match_limit=5: _FakeRecognition(intent)))
    stack.enter_context(patch.object(workflow_runtime_repository, "load_workflow_record", lambda _conn, workflow_id: conn.workflows.get(workflow_id)))
    stack.enter_context(patch.object(canonical_workflows, "save_workflow", _save_workflow))
    stack.enter_context(patch.object(canonical_workflows, "mutate_workflow_build", _mutate_workflow_build))
    stack.enter_context(patch.object(workflow_build_moment, "build_workflow_build_moment", lambda row, **kwargs: _fake_build_payload(conn, dict(row), **kwargs)))
    stack.enter_context(patch.object(workflow_build_get, "build_workflow_build_moment", lambda row, **kwargs: _fake_build_payload(conn, dict(row), **kwargs)))
    stack.enter_context(patch.object(workflow_build_command, "build_workflow_build_moment", lambda row, **kwargs: _fake_build_payload(conn, dict(row), **kwargs)))


def _stage(
    stages: list[dict[str, Any]],
    name: str,
    func: Any,
) -> Any:
    started = time.perf_counter()
    try:
        result = func()
    except Exception as exc:
        stages.append(
            {
                "name": name,
                "ok": False,
                "duration_ms": round((time.perf_counter() - started) * 1000, 3),
                "error": f"{exc.__class__.__name__}: {exc}",
            }
        )
        raise
    if isinstance(result, dict) and result.get("ok") is False:
        stage_payload = {
            "error": result.get("error"),
            "error_code": result.get("error_code") or result.get("reason_code"),
        }
    else:
        stage_payload = {}
    stage_payload.setdefault("name", name)
    stage_payload.setdefault("ok", True)
    stage_payload["duration_ms"] = round((time.perf_counter() - started) * 1000, 3)
    stages.append(stage_payload)
    return result


def run_probe(*, mode: str = "deterministic") -> dict[str, Any]:
    if mode != "deterministic":
        return {
            "ok": False,
            "results": {"spoof_mode": mode},
            "errors": ["compile-delivery-probe currently supports only deterministic mode"],
            "warnings": [],
        }

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    import runtime.chat_tools as chat_tools
    import runtime.operation_catalog_gateway as gateway
    import surfaces.api.rest as rest
    from runtime.operations.commands.compile_materialize import (
        CompileMaterializeCommand,
        handle_compile_materialize,
    )
    from runtime.operations.commands.workflow_build import (
        MutateWorkflowBuildCommand,
        handle_mutate_workflow_build,
    )
    from runtime.operations.queries.compile_preview import (
        CompilePreviewQuery,
        handle_compile_preview,
    )
    from runtime.operations.queries.workflow_build_get import (
        GetWorkflowBuildCommand,
        handle_get_workflow_build,
    )

    conn = _ProbeConn()
    subsystems = SimpleNamespace(get_pg_conn=lambda: conn)
    stages: list[dict[str, Any]] = []
    errors: list[str] = []
    warnings: list[str] = []
    intent = "Search GitHub issues, draft a summary, and notify Slack."

    preview_binding = _binding(
        operation_ref="compile.preview",
        operation_name="compile_preview",
        operation_kind="query",
        http_method="POST",
        http_path="/api/compile/preview",
        command_class=CompilePreviewQuery,
        handler=handle_compile_preview,
        event_required=False,
    )
    materialize_binding = _binding(
        operation_ref="compile.materialize",
        operation_name="compile_materialize",
        operation_kind="command",
        http_method="POST",
        http_path="/api/compile/materialize",
        command_class=CompileMaterializeCommand,
        handler=handle_compile_materialize,
        event_required=True,
        event_type="compile.materialized",
    )
    build_get_binding = _binding(
        operation_ref="workflow_build.get",
        operation_name="workflow_build_get",
        operation_kind="query",
        http_method="POST",
        http_path="/api/workflows/{workflow_id}/build",
        command_class=GetWorkflowBuildCommand,
        handler=handle_get_workflow_build,
        event_required=False,
    )
    mutate_binding = _binding(
        operation_ref="workflow_build.mutate",
        operation_name="workflow_build.mutate",
        operation_kind="command",
        http_method="POST",
        http_path="/api/workflows/{workflow_id}/build/{subpath:path}",
        command_class=MutateWorkflowBuildCommand,
        handler=handle_mutate_workflow_build,
        event_required=True,
        event_type="workflow_build.mutated",
    )
    bindings = {
        "compile_preview": preview_binding,
        "compile_materialize": materialize_binding,
        "workflow_build_get": build_get_binding,
        "workflow_build.mutate": mutate_binding,
    }

    try:
        with ExitStack() as stack:
            _install_spoofs(stack, conn)
            target_app = FastAPI()
            stack.enter_context(__import__("unittest.mock").mock.patch.object(rest, "app", target_app))
            stack.enter_context(__import__("unittest.mock").mock.patch.object(rest, "_ensure_shared_subsystems", lambda _app: subsystems))
            stack.enter_context(
                __import__("unittest.mock").mock.patch.object(
                    rest,
                    "list_resolved_operation_definitions",
                    lambda _conn, include_disabled=False, limit=500: [
                        SimpleNamespace(operation_name="compile_preview"),
                        SimpleNamespace(operation_name="compile_materialize"),
                    ],
                )
            )
            stack.enter_context(
                __import__("unittest.mock").mock.patch.object(
                    rest,
                    "resolve_http_operation_binding",
                    lambda definition: bindings[definition.operation_name],
                )
            )
            rest.mount_capabilities(target_app)
            client = TestClient(target_app)

            preview = _stage(
                stages,
                "preview",
                lambda: client.post("/api/compile/preview", json={"intent": intent}).json(),
            )
            preview_receipt = preview["operation_receipt"]
            if preview_receipt["operation_kind"] != "query" or preview_receipt["event_ids"]:
                raise AssertionError("compile_preview must be a receipt-backed query with no events")
            if not (preview.get("enough_structure") or preview.get("scope_packet", {}).get("gaps")):
                raise AssertionError("compile_preview returned neither enough_structure nor typed gaps")
            stages[-1].update({"receipt_id": preview_receipt["receipt_id"]})

            materialized = _stage(
                stages,
                "materialize",
                lambda: client.post(
                    "/api/compile/materialize",
                    json={"intent": intent, "title": "Compile delivery probe", "enable_llm": True, "enable_full_compose": True},
                ).json(),
            )
            if materialized.get("ok") is not True:
                raise AssertionError(f"materialize failed: {materialized}")
            workflow_id = str(materialized.get("workflow_id") or "")
            graph_summary = materialized.get("graph_summary") or {}
            materialize_receipt = materialized["operation_receipt"]
            event_ids = list(materialize_receipt.get("event_ids") or [])
            if not workflow_id or graph_summary.get("node_count", 0) < 1 or not event_ids:
                raise AssertionError("materialize did not persist workflow, graph, and event")
            event_types = [conn.events[event_id]["event_type"] for event_id in event_ids if event_id in conn.events]
            if "compile.materialized" not in event_types:
                raise AssertionError("compile.materialized authority event was not emitted")
            stages[-1].update(
                {
                    "workflow_id": workflow_id,
                    "receipt_id": materialize_receipt["receipt_id"],
                    "event_ids": event_ids,
                    "graph_counts": {
                        "nodes": graph_summary.get("node_count"),
                        "edges": graph_summary.get("edge_count"),
                    },
                }
            )

            build = _stage(
                stages,
                "build_get",
                lambda: gateway.execute_operation_binding(
                    build_get_binding,
                    payload={"workflow_id": workflow_id},
                    subsystems=subsystems,
                ),
            )
            graph = build.get("build_graph") or {}
            nodes = graph.get("nodes") or []
            edges = graph.get("edges") or []
            if len(nodes) < 3 or len(edges) < 2 or build.get("projection_status", {}).get("state") != "ready":
                raise AssertionError("workflow_build_get did not return a ready non-empty graph")
            stages[-1].update(
                {
                    "workflow_id": workflow_id,
                    "receipt_id": build["operation_receipt"]["receipt_id"],
                    "graph_counts": {"nodes": len(nodes), "edges": len(edges)},
                    "readiness": build.get("projection_status", {}).get("state"),
                }
            )

            handoff_context = _stage(
                stages,
                "handoff_context",
                lambda: [
                    {
                        "kind": "moon_context",
                        "workflow_id": workflow_id,
                        "workflow_title": build.get("workflow", {}).get("name"),
                        "materialize_status": "ready",
                        "operation_receipt_id": materialize_receipt["receipt_id"],
                        "correlation_id": materialize_receipt.get("correlation_id"),
                        "graph_summary": graph_summary,
                    },
                    {
                        "kind": "moon_materialize_handoff",
                        "workflow_id": workflow_id,
                        "status": "ready",
                        "operation_receipt_id": materialize_receipt["receipt_id"],
                        "graph_summary": graph_summary,
                    },
                ],
            )
            stages[-1].update({"context_kinds": [item["kind"] for item in handoff_context]})

            def _dispatch_op(_pg_conn: Any, operation_name: str, payload: dict[str, Any]) -> dict[str, Any]:
                return gateway.execute_operation_binding(
                    bindings[operation_name],
                    payload=payload,
                    subsystems=subsystems,
                )

            stack.enter_context(__import__("unittest.mock").mock.patch.object(chat_tools, "_dispatch_op", _dispatch_op))

            def _chat_loop() -> dict[str, Any]:
                get_result = chat_tools.execute_tool(
                    "moon_get_build",
                    {},
                    pg_conn=conn,
                    repo_root=str(REPO_ROOT),
                    selection_context=handoff_context,
                )
                if get_result.get("type") != "status":
                    raise AssertionError(f"moon_get_build failed: {get_result}")
                loaded = get_result["data"]
                assistant_response = (
                    f"Loaded {loaded.get('node_count')} nodes for "
                    f"{loaded.get('name') or workflow_id}; I can edit the graph now."
                )
                mutate_result = chat_tools.execute_tool(
                    "moon_mutate_field",
                    {"subpath": "nodes/n1", "body": {"title": "Probe-modified"}},
                    pg_conn=conn,
                    repo_root=str(REPO_ROOT),
                    selection_context=handoff_context,
                )
                if mutate_result.get("type") != "status":
                    raise AssertionError(f"moon_mutate_field failed: {mutate_result}")
                return {
                    "tools_called": ["moon_get_build", "moon_mutate_field"],
                    "assistant_response": assistant_response,
                    "receipt_ids": [
                        get_result["data"]["full_payload"]["operation_receipt"]["receipt_id"],
                        mutate_result["data"]["full_payload"]["operation_receipt"]["receipt_id"],
                    ],
                    "moon_chat_response_summary": assistant_response,
                }

            chat_loop = _stage(stages, "chat_tool_loop", _chat_loop)
            stages[-1].update(chat_loop)

            verified = _stage(
                stages,
                "verify_mutation",
                lambda: gateway.execute_operation_binding(
                    build_get_binding,
                    payload={"workflow_id": workflow_id},
                    subsystems=subsystems,
                ),
            )
            actual_title = ""
            for node in verified.get("build_graph", {}).get("nodes", []):
                if node.get("node_id") == "n1":
                    actual_title = str(node.get("title") or "")
                    break
            if actual_title != "Probe-modified":
                raise AssertionError(f"saved mutation was not durable: {actual_title!r}")
            stages[-1].update(
                {
                    "workflow_id": workflow_id,
                    "expected_title": "Probe-modified",
                    "actual_title": actual_title,
                    "receipt_id": verified["operation_receipt"]["receipt_id"],
                }
            )

            return {
                "ok": True,
                "results": {
                    "spoof_mode": mode,
                    "workflow_id": workflow_id,
                    "receipt_ids": {
                        "preview": preview_receipt["receipt_id"],
                        "materialize": materialize_receipt["receipt_id"],
                    },
                    "event_ids": event_ids,
                    "stages": stages,
                    "moon_chat_response_summary": chat_loop["moon_chat_response_summary"],
                },
                "errors": [],
                "warnings": warnings,
            }
    except Exception as exc:
        errors.append(f"{exc.__class__.__name__}: {exc}")
        return {
            "ok": False,
            "results": {
                "spoof_mode": mode,
                "stages": stages,
            },
            "errors": errors,
            "warnings": warnings,
        }


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    mode = "deterministic"
    for arg in args:
        if arg.startswith("--mode="):
            mode = arg.split("=", 1)[1]
    payload = run_probe(mode=mode)
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
