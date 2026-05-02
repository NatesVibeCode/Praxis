from __future__ import annotations

from types import SimpleNamespace

from runtime import materialize_cqrs
from runtime.intent_recognition import recognize_intent


def test_preview_compile_is_query_only(monkeypatch) -> None:
    calls: list[object] = []

    def _fake_recognize(intent: str, *, conn, match_limit: int):
        calls.append(conn)
        return SimpleNamespace(
            to_dict=lambda: {
                "spans": [{"text": "app domain", "normalized": "app_domain"}],
                "matches": [{"label": "App domain"}],
                "suggested_steps": [{"label": "discover"}],
                "gaps": [],
            }
        )

    monkeypatch.setattr(materialize_cqrs, "recognize_intent", _fake_recognize)

    preview = materialize_cqrs.preview_compile("build an app integration", conn="db").to_dict()

    assert calls == ["db"]
    assert preview["kind"] == "materialize_preview"
    assert preview["cqrs_role"] == "query"
    assert preview["scope_packet"]["suggested_steps"][0]["label"] == "discover"
    assert preview["next_actions"][-1]["action"] == "materialize_workflow"


def test_materialize_workflow_uses_command_side_runtime(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        materialize_cqrs,
        "recognize_intent",
        lambda intent, *, conn, match_limit: SimpleNamespace(
            to_dict=lambda: {
                "spans": [{"text": "integration", "normalized": "custom_integration"}],
                "matches": [],
                "suggested_steps": [],
                "gaps": [{"kind": "auth"}],
            }
        ),
    )
    monkeypatch.setattr(
        "storage.postgres.workflow_runtime_repository.load_workflow_record",
        lambda conn, workflow_id: None,
    )

    def _fake_save(conn, workflow_id, body):
        captured["saved_body"] = body
        return {"id": body["id"], "name": body["name"]}

    def _fake_mutate(conn, *, workflow_id, subpath, body):
        captured["mutated"] = {
            "conn": conn,
            "workflow_id": workflow_id,
            "subpath": subpath,
            "body": body,
        }
        return {
            "row": {"id": workflow_id, "name": body["title"]},
            "definition": {"workflow_id": workflow_id},
            "materialized_spec": {},
            "build_bundle": {
                "build_graph": {"nodes": [{"node_id": "node-1"}], "edges": []},
                "projection_status": {"state": "ready"},
            },
            "planning_notes": [],
            "candidate_resolution_manifest": {"execution_readiness": "ready"},
        }

    monkeypatch.setattr("runtime.canonical_workflows.save_workflow", _fake_save)
    monkeypatch.setattr("runtime.canonical_workflows.mutate_workflow_build", _fake_mutate)
    monkeypatch.setattr(
        "runtime.workflow_build_moment.build_workflow_build_moment",
        lambda row, **kwargs: {
            "workflow": {"id": row["id"], "name": row.get("name")},
            "definition": kwargs.get("definition") or {},
            "materialized_spec": kwargs.get("materialized_spec") or {},
            "build_graph": (kwargs.get("build_bundle") or {}).get("build_graph"),
            "materialize_preview": kwargs.get("materialize_preview"),
        },
    )

    result = materialize_cqrs.materialize_workflow(
        "make a custom integration",
        conn="db",
        workflow_id="wf_compile_test",
        title="Integration workflow",
        enable_llm=False,
        enable_full_compose=False,
    )

    assert result["kind"] == "compile_materialization"
    assert result["cqrs_role"] == "command"
    assert result["workflow_id"] == "wf_compile_test"
    assert result["materialize_preview"]["kind"] == "materialize_preview"
    assert captured["saved_body"]["definition"]["materialize_cqrs"]["state"] == "started"
    assert captured["mutated"] == {
        "conn": "db",
        "workflow_id": "wf_compile_test",
        "subpath": "bootstrap",
        "body": {
            "prose": "make a custom integration",
            "title": "Integration workflow",
            "enable_llm": False,
            "enable_full_compose": False,
        },
    }
    assert result["graph_summary"]["node_count"] == 1
    assert result["build_payload"]["workflow"]["id"] == "wf_compile_test"


def test_materialize_workflow_fails_closed_on_empty_graph(monkeypatch) -> None:
    monkeypatch.setattr(
        materialize_cqrs,
        "recognize_intent",
        lambda intent, *, conn, match_limit: SimpleNamespace(
            to_dict=lambda: {
                "spans": [{"text": "integration", "normalized": "custom_integration"}],
                "matches": [],
                "suggested_steps": [],
                "gaps": [],
            }
        ),
    )
    monkeypatch.setattr(
        "storage.postgres.workflow_runtime_repository.load_workflow_record",
        lambda conn, workflow_id: {"id": workflow_id, "name": "Empty Graph"},
    )
    monkeypatch.setattr(
        "runtime.canonical_workflows.mutate_workflow_build",
        lambda *args, **kwargs: {
            "row": {"id": kwargs["workflow_id"], "name": "Empty Graph"},
            "definition": {"workflow_id": kwargs["workflow_id"]},
            "materialized_spec": {},
            "build_bundle": {
                "build_graph": {"nodes": [], "edges": []},
                "projection_status": {"state": "ready"},
            },
            "planning_notes": [],
            "candidate_resolution_manifest": {"execution_readiness": "ready"},
        },
    )

    try:
        materialize_cqrs.materialize_workflow(
            "make a custom integration",
            conn="db",
            workflow_id="wf_empty",
            enable_full_compose=False,
        )
    except materialize_cqrs.MaterializationError as exc:
        assert exc.reason_code == "compile.materialize.empty_graph"
        assert exc.details["graph_summary"]["node_count"] == 0
    else:  # pragma: no cover - explicit fail branch for assertion clarity
        raise AssertionError("empty materialization should fail closed")


def test_materialize_workflow_does_not_save_when_full_compose_fails(monkeypatch) -> None:
    save_calls: list[object] = []
    mutate_calls: list[object] = []

    monkeypatch.setattr(
        materialize_cqrs,
        "recognize_intent",
        lambda intent, *, conn, match_limit: SimpleNamespace(
            to_dict=lambda: {
                "spans": [{"text": "integration", "normalized": "custom_integration"}],
                "matches": [],
                "suggested_steps": [{"label": "research API docs"}],
                "gaps": [],
            }
        ),
    )
    monkeypatch.setattr(
        "runtime.compose_plan_via_llm.compose_plan_via_llm",
        lambda *args, **kwargs: SimpleNamespace(
            ok=False,
            to_dict=lambda: {
                "ok": False,
                "reason_code": "synthesis.llm_call_failed",
                "error": "provider timed out",
            },
        ),
    )
    monkeypatch.setattr(
        "storage.postgres.workflow_runtime_repository.load_workflow_record",
        lambda conn, workflow_id: None,
    )
    monkeypatch.setattr(
        "runtime.canonical_workflows.save_workflow",
        lambda *args, **kwargs: save_calls.append(kwargs) or {"id": "should_not_save"},
    )
    monkeypatch.setattr(
        "runtime.canonical_workflows.mutate_workflow_build",
        lambda *args, **kwargs: mutate_calls.append(kwargs) or {},
    )

    try:
        materialize_cqrs.materialize_workflow("make a custom integration", conn="db")
    except materialize_cqrs.MaterializationError as exc:
        assert exc.reason_code == "synthesis.llm_call_failed"
        assert exc.details["compose_provenance"]["error"] == "provider timed out"
    else:  # pragma: no cover - explicit fail branch for assertion clarity
        raise AssertionError("failed full compose must not persist a workflow")

    assert save_calls == []
    assert mutate_calls == []


def test_intent_recognition_suggests_missing_integration_control_steps(monkeypatch) -> None:
    monkeypatch.setattr("runtime.intent_recognition.list_object_kinds", lambda conn: [])

    recognition = recognize_intent(
        "A repeatable workflow where we feed in an app name or app domain and it gets "
        "broken up into multiple steps to plan search, retrieve, evaluate and then "
        "attempt to build a custom integration for an application.",
        conn="db",
    ).to_dict()

    span_kinds = {span["normalized"] for span in recognition["spans"]}
    suggestion_labels = {step["label"] for step in recognition["suggested_steps"]}
    assert {"app_name", "app_domain", "plan", "search", "retrieve", "evaluate", "build", "custom_integration"}.issubset(span_kinds)
    assert "decide built-in vs custom integration path" in suggestion_labels
    assert "decide whether research should fan out" in suggestion_labels
    assert "verify integration with a smoke run" in suggestion_labels
