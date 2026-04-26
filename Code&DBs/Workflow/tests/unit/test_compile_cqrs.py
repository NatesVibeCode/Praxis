from __future__ import annotations

from types import SimpleNamespace

from runtime import compile_cqrs
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

    monkeypatch.setattr(compile_cqrs, "recognize_intent", _fake_recognize)

    preview = compile_cqrs.preview_compile("build an app integration", conn="db").to_dict()

    assert calls == ["db"]
    assert preview["kind"] == "compile_preview"
    assert preview["cqrs_role"] == "query"
    assert preview["scope_packet"]["suggested_steps"][0]["label"] == "discover"
    assert preview["next_actions"][-1]["action"] == "materialize_workflow"


def test_materialize_workflow_uses_command_side_runtime(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        compile_cqrs,
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
            "compiled_spec": {},
            "build_bundle": {},
            "planning_notes": [],
        }

    monkeypatch.setattr("runtime.canonical_workflows.save_workflow", _fake_save)
    monkeypatch.setattr("runtime.canonical_workflows.mutate_workflow_build", _fake_mutate)

    result = compile_cqrs.materialize_workflow(
        "make a custom integration",
        conn="db",
        workflow_id="wf_compile_test",
        title="Integration workflow",
        enable_llm=False,
    )

    assert result["kind"] == "compile_materialization"
    assert result["cqrs_role"] == "command"
    assert result["workflow_id"] == "wf_compile_test"
    assert result["compile_preview"]["kind"] == "compile_preview"
    assert captured["saved_body"]["definition"]["compile_cqrs"]["state"] == "started"
    assert captured["mutated"] == {
        "conn": "db",
        "workflow_id": "wf_compile_test",
        "subpath": "bootstrap",
        "body": {
            "prose": "make a custom integration",
            "title": "Integration workflow",
            "enable_llm": False,
        },
    }


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
