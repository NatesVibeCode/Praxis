"""Unit tests for the Moon graph authoring chat tools.

The five ``moon_*`` chat tools are thin wrappers over registered CQRS
operations. We verify three things:

1. The right ``operation_name`` is dispatched with the right payload.
2. The result shape matches the chat tool contract (``type``, ``data``,
   ``selectable``, ``summary``) so ``ToolResultRenderer`` can render it.
3. The summary string contains JSON-compact graph state — that's what
   the chat orchestrator forwards back to the LLM, so it has to carry
   enough detail for the LLM to reason about the next edit.
"""
from __future__ import annotations

import json
from typing import Any

import pytest

import runtime.chat_tools as chat_tools


class _FakeDispatchRecorder:
    """Capture (operation_name, payload) calls and return canned results."""

    def __init__(self, response_for: dict[str, Any]):
        self._response_for = response_for
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def __call__(self, pg_conn: Any, operation_name: str, payload: dict[str, Any]) -> Any:
        self.calls.append((operation_name, dict(payload)))
        if operation_name not in self._response_for:
            raise AssertionError(f"unexpected op: {operation_name}")
        return self._response_for[operation_name]


def _patch_dispatch(monkeypatch: pytest.MonkeyPatch, response_for: dict[str, Any]) -> _FakeDispatchRecorder:
    recorder = _FakeDispatchRecorder(response_for)
    monkeypatch.setattr(chat_tools, "_dispatch_op", recorder)
    return recorder


def _summary_payload(result: dict[str, Any]) -> dict[str, Any]:
    summary = result["summary"]
    _, _, payload = summary.partition(": ")
    assert payload, f"summary missing JSON payload: {summary}"
    return json.loads(payload)


# ---------------------------------------------------------------------------
# moon_get_build
# ---------------------------------------------------------------------------

def test_moon_get_build_dispatches_workflow_build_get_and_returns_compact_summary(monkeypatch):
    recorder = _patch_dispatch(monkeypatch, {
        "workflow_build_get": {
            "workflow_id": "wf_abc",
            "name": "demo",
            "build_graph": {
                "nodes": [
                    {"node_id": "n1", "title": "Trigger", "route": "trigger", "fields": {"event": "x"}},
                    {"node_id": "n2", "title": "Send Slack", "route": "integration/slack/send", "fields": {"channel": "#ops"}},
                ],
                "edges": [
                    {"edge_id": "e1", "source": "n1", "target": "n2"},
                ],
            },
            "outcome": {"outcome_goal": "ping ops", "verify_command": "echo ok"},
            "issues": ["missing pill bind for slack.channel"],
        },
    })

    result = chat_tools.execute_tool("moon_get_build", {"workflow_id": "wf_abc"}, pg_conn=object(), repo_root="/tmp")

    assert recorder.calls == [("workflow_build_get", {"workflow_id": "wf_abc"})]
    assert result["type"] == "status"
    assert result["selectable"] is False
    payload = _summary_payload(result)
    assert payload["workflow_id"] == "wf_abc"
    assert payload["node_count"] == 2
    assert payload["edge_count"] == 1
    assert {n["title"] for n in payload["nodes"]} == {"Trigger", "Send Slack"}
    assert payload["outcome"] == {"outcome_goal": "ping ops", "verify_command": "echo ok"}
    assert payload["issues_top3"] == ["missing pill bind for slack.channel"]
    assert "full_payload" in result["data"]


def test_moon_get_build_requires_workflow_id():
    result = chat_tools.execute_tool("moon_get_build", {}, pg_conn=object(), repo_root="/tmp")
    assert result["type"] == "error"
    assert "workflow_id is required" in result["summary"]


def test_moon_get_build_surfaces_dispatch_errors(monkeypatch):
    def _explode(*args, **kwargs):
        raise RuntimeError("postgres unreachable")

    monkeypatch.setattr(chat_tools, "_dispatch_op", _explode)
    result = chat_tools.execute_tool("moon_get_build", {"workflow_id": "wf_x"}, pg_conn=object(), repo_root="/tmp")
    assert result["type"] == "error"
    assert "postgres unreachable" in result["summary"]


# ---------------------------------------------------------------------------
# moon_compose_from_prose
# ---------------------------------------------------------------------------

def test_moon_compose_from_prose_creates_draft_and_bootstraps_when_no_workflow_id(monkeypatch):
    """Path A: no active Moon workflow → chain create_draft + bootstrap."""
    recorder = _patch_dispatch(monkeypatch, {
        "workflow_create_draft": {
            "ok": True,
            "workflow_id": "wf_new_draft",
            "name": "demo plan",
        },
        "workflow_build.mutate": {
            "workflow": {"id": "wf_new_draft", "name": "demo plan"},
            "build_graph": {
                "nodes": [
                    {"node_id": "n1", "title": "Search GitHub"},
                    {"node_id": "n2", "title": "Draft summary"},
                    {"node_id": "n3", "title": "Notify Slack"},
                ],
                "edges": [],
            },
        },
    })

    result = chat_tools.execute_tool(
        "moon_compose_from_prose",
        {"intent": "Search GH issues, draft summary, notify Slack", "plan_name": "demo plan", "concurrency": 8},
        pg_conn=object(),
        repo_root="/tmp",
        selection_context=None,
    )

    # Two ops, in order: create draft, then bootstrap on the new id
    assert [c[0] for c in recorder.calls] == ["workflow_create_draft", "workflow_build.mutate"]
    assert recorder.calls[0][1] == {"name": "demo plan"}
    bootstrap_call = recorder.calls[1][1]
    assert bootstrap_call["workflow_id"] == "wf_new_draft"
    assert bootstrap_call["subpath"] == "bootstrap"
    assert bootstrap_call["body"]["prose"] == "Search GH issues, draft summary, notify Slack"
    assert bootstrap_call["body"]["enable_llm"] is True
    assert bootstrap_call["body"]["enable_full_compose"] is True
    assert bootstrap_call["body"]["concurrency"] == 8
    assert bootstrap_call["body"]["title"] == "demo plan"

    assert result["type"] == "status"
    payload = _summary_payload(result)
    assert payload["workflow_id"] == "wf_new_draft"
    assert payload["created_new_draft"] is True
    assert payload["node_count"] == 3


def test_moon_compose_from_prose_bootstraps_existing_workflow_from_context(monkeypatch):
    """Path B: active Moon workflow in context → bootstrap that one, no create."""
    recorder = _patch_dispatch(monkeypatch, {
        "workflow_build.mutate": {
            "workflow": {"id": "wf_active", "name": "moon-demo"},
            "build_graph": {"nodes": [{"node_id": "n1"}], "edges": []},
        },
    })

    result = chat_tools.execute_tool(
        "moon_compose_from_prose",
        {"intent": "add a Slack notify step"},
        pg_conn=object(),
        repo_root="/tmp",
        selection_context=_MOON_CTX,
    )

    # Only one op: bootstrap on the existing workflow_id
    assert [c[0] for c in recorder.calls] == ["workflow_build.mutate"]
    assert recorder.calls[0][1]["workflow_id"] == "wf_active"
    assert recorder.calls[0][1]["subpath"] == "bootstrap"
    payload = _summary_payload(result)
    assert payload["workflow_id"] == "wf_active"
    assert payload["created_new_draft"] is False
    assert payload["targeted_via"] == "moon_context"


def test_moon_compose_from_prose_requires_intent():
    result = chat_tools.execute_tool("moon_compose_from_prose", {}, pg_conn=object(), repo_root="/tmp")
    assert result["type"] == "error"


def test_moon_compose_from_prose_clamps_concurrency(monkeypatch):
    recorder = _patch_dispatch(monkeypatch, {
        "workflow_create_draft": {"ok": True, "workflow_id": "wf_x"},
        "workflow_build.mutate": {"workflow": {"id": "wf_x"}, "build_graph": {"nodes": [], "edges": []}},
    })
    chat_tools.execute_tool(
        "moon_compose_from_prose",
        {"intent": "anything", "concurrency": 999},
        pg_conn=object(),
        repo_root="/tmp",
    )
    bootstrap_body = recorder.calls[1][1]["body"]
    assert bootstrap_body["concurrency"] == 100


# ---------------------------------------------------------------------------
# moon_mutate_field
# ---------------------------------------------------------------------------

def test_moon_mutate_field_dispatches_workflow_build_mutate_with_subpath(monkeypatch):
    recorder = _patch_dispatch(monkeypatch, {
        "workflow_build.mutate": {
            "workflow_id": "wf_abc",
            "name": "demo",
            "build_graph": {"nodes": [{"node_id": "n1", "title": "Trigger"}], "edges": []},
        },
    })

    result = chat_tools.execute_tool(
        "moon_mutate_field",
        {"workflow_id": "wf_abc", "subpath": "nodes/n1", "body": {"title": "Trigger v2"}},
        pg_conn=object(),
        repo_root="/tmp",
    )

    assert recorder.calls == [(
        "workflow_build.mutate",
        {"workflow_id": "wf_abc", "subpath": "nodes/n1", "body": {"title": "Trigger v2"}},
    )]
    assert result["type"] == "status"
    payload = _summary_payload(result)
    assert payload["mutated_subpath"] == "nodes/n1"
    assert payload["node_count"] == 1


def test_moon_mutate_field_validates_inputs():
    result = chat_tools.execute_tool(
        "moon_mutate_field",
        {"workflow_id": "wf_abc", "subpath": "", "body": {}},
        pg_conn=object(),
        repo_root="/tmp",
    )
    assert result["type"] == "error"

    result = chat_tools.execute_tool(
        "moon_mutate_field",
        {"workflow_id": "wf_abc", "subpath": "nodes/n1", "body": "not a dict"},
        pg_conn=object(),
        repo_root="/tmp",
    )
    assert result["type"] == "error"


# ---------------------------------------------------------------------------
# moon_suggest_next
# ---------------------------------------------------------------------------

def test_moon_suggest_next_calls_get_then_suggest(monkeypatch):
    recorder = _patch_dispatch(monkeypatch, {
        "workflow_build_get": {
            "workflow_id": "wf_abc",
            "build_graph": {"nodes": [{"node_id": "n1"}], "edges": []},
        },
        "workflow_build.suggest_next": {
            "likely_next_steps": [
                {"title": "Slack send", "capability_slug": "slack.send"},
                {"title": "Github issue", "capability_slug": "github.issue.create"},
            ],
            "possible_next_steps": [{"title": "Email"}],
            "blocked_next_steps": [],
        },
    })

    result = chat_tools.execute_tool(
        "moon_suggest_next",
        {"workflow_id": "wf_abc", "node_id": "n1"},
        pg_conn=object(),
        repo_root="/tmp",
    )

    assert [c[0] for c in recorder.calls] == ["workflow_build_get", "workflow_build.suggest_next"]
    assert recorder.calls[1][1]["body"]["node_id"] == "n1"
    assert recorder.calls[1][1]["body"]["build_graph"]["nodes"] == [{"node_id": "n1"}]
    payload = _summary_payload(result)
    assert payload["likely_count"] == 2
    assert payload["likely_titles"] == ["Slack send", "Github issue"]


# ---------------------------------------------------------------------------
# moon_launch
# ---------------------------------------------------------------------------

def test_moon_launch_dispatches_launch_plan(monkeypatch):
    recorder = _patch_dispatch(monkeypatch, {
        "launch_plan": {"run_id": "run_xyz", "status": "queued", "workflow_id": "wf_abc"},
    })

    result = chat_tools.execute_tool(
        "moon_launch",
        {"workflow_id": "wf_abc", "approved_by": "nate@praxis"},
        pg_conn=object(),
        repo_root="/tmp",
    )

    assert recorder.calls == [(
        "launch_plan",
        {"workflow_id": "wf_abc", "approved_by": "nate@praxis"},
    )]
    payload = _summary_payload(result)
    assert payload["run_id"] == "run_xyz"
    assert payload["workflow_id"] == "wf_abc"


# ---------------------------------------------------------------------------
# Selection-context defaulting (Moon co-pilot wiring)
# ---------------------------------------------------------------------------

_MOON_CTX = [{
    "kind": chat_tools.MOON_CONTEXT_KIND,
    "workflow_id": "wf_active",
    "workflow_name": "moon-demo",
    "selected_node_id": "node-2",
    "selected_edge_id": None,
    "view_mode": "build",
}]


def test_extract_moon_context_finds_entry_among_other_selection_items():
    ctx = [
        {"kind": "row", "id": 1},
        {"kind": chat_tools.MOON_CONTEXT_KIND, "workflow_id": "wf_x"},
        {"kind": "row", "id": 2},
    ]
    found = chat_tools._extract_moon_context(ctx)
    assert found is not None
    assert found["workflow_id"] == "wf_x"


def test_extract_moon_context_returns_none_when_absent():
    assert chat_tools._extract_moon_context(None) is None
    assert chat_tools._extract_moon_context([]) is None
    assert chat_tools._extract_moon_context([{"kind": "row"}]) is None


def test_resolve_workflow_id_prefers_explicit_args_over_context():
    workflow_id, from_ctx = chat_tools._resolve_workflow_id(
        {"workflow_id": "wf_explicit"}, _MOON_CTX
    )
    assert workflow_id == "wf_explicit"
    assert from_ctx is False


def test_resolve_workflow_id_falls_back_to_moon_context():
    workflow_id, from_ctx = chat_tools._resolve_workflow_id({}, _MOON_CTX)
    assert workflow_id == "wf_active"
    assert from_ctx is True


def test_moon_get_build_default_targets_active_workflow_from_context(monkeypatch):
    recorder = _patch_dispatch(monkeypatch, {
        "workflow_build_get": {
            "workflow_id": "wf_active",
            "name": "moon-demo",
            "build_graph": {"nodes": [{"node_id": "n1"}], "edges": []},
        },
    })
    # Note: NO workflow_id in args
    result = chat_tools.execute_tool(
        "moon_get_build",
        {},
        pg_conn=object(),
        repo_root="/tmp",
        selection_context=_MOON_CTX,
    )
    assert recorder.calls == [("workflow_build_get", {"workflow_id": "wf_active"})]
    assert result["type"] == "status"
    payload = _summary_payload(result)
    assert payload["targeted_via"] == "moon_context"


def test_moon_get_build_errors_when_no_explicit_id_and_no_context(monkeypatch):
    monkeypatch.setattr(chat_tools, "_dispatch_op", lambda *a, **k: pytest.fail("should not dispatch"))
    result = chat_tools.execute_tool(
        "moon_get_build",
        {},
        pg_conn=object(),
        repo_root="/tmp",
        selection_context=None,
    )
    assert result["type"] == "error"
    assert "no active Moon workflow" in result["summary"]


def test_moon_mutate_field_default_targets_workflow_from_context(monkeypatch):
    recorder = _patch_dispatch(monkeypatch, {
        "workflow_build.mutate": {
            "workflow_id": "wf_active",
            "build_graph": {"nodes": [], "edges": []},
        },
    })
    result = chat_tools.execute_tool(
        "moon_mutate_field",
        {"subpath": "nodes/node-2", "body": {"title": "new title"}},
        pg_conn=object(),
        repo_root="/tmp",
        selection_context=_MOON_CTX,
    )
    assert recorder.calls == [(
        "workflow_build.mutate",
        {"workflow_id": "wf_active", "subpath": "nodes/node-2", "body": {"title": "new title"}},
    )]
    payload = _summary_payload(result)
    assert payload["targeted_via"] == "moon_context"
    assert payload["mutated_subpath"] == "nodes/node-2"


def test_moon_suggest_next_default_anchors_on_selected_node(monkeypatch):
    recorder = _patch_dispatch(monkeypatch, {
        "workflow_build_get": {"build_graph": {"nodes": [{"node_id": "node-2"}], "edges": []}},
        "workflow_build.suggest_next": {
            "likely_next_steps": [{"title": "Slack send"}],
            "possible_next_steps": [],
            "blocked_next_steps": [],
        },
    })
    result = chat_tools.execute_tool(
        "moon_suggest_next",
        {},
        pg_conn=object(),
        repo_root="/tmp",
        selection_context=_MOON_CTX,
    )
    assert recorder.calls[0][0] == "workflow_build_get"
    assert recorder.calls[1][0] == "workflow_build.suggest_next"
    # both calls used the context workflow_id
    assert recorder.calls[0][1]["workflow_id"] == "wf_active"
    # suggest_next anchored on the selected node from moon_context
    assert recorder.calls[1][1]["body"]["node_id"] == "node-2"
    payload = _summary_payload(result)
    assert payload["anchor_node_id"] == "node-2"
    assert payload["targeted_via"] == "moon_context"


def test_moon_launch_default_targets_workflow_from_context(monkeypatch):
    recorder = _patch_dispatch(monkeypatch, {
        "launch_plan": {"run_id": "run_z", "status": "queued"},
    })
    result = chat_tools.execute_tool(
        "moon_launch",
        {"approved_by": "nate"},
        pg_conn=object(),
        repo_root="/tmp",
        selection_context=_MOON_CTX,
    )
    assert recorder.calls == [(
        "launch_plan",
        {"workflow_id": "wf_active", "approved_by": "nate"},
    )]
    payload = _summary_payload(result)
    assert payload["targeted_via"] == "moon_context"


def test_explicit_workflow_id_overrides_context(monkeypatch):
    """When user names a different workflow, ignore the context."""
    recorder = _patch_dispatch(monkeypatch, {
        "workflow_build_get": {"build_graph": {"nodes": [], "edges": []}},
    })
    result = chat_tools.execute_tool(
        "moon_get_build",
        {"workflow_id": "wf_other"},
        pg_conn=object(),
        repo_root="/tmp",
        selection_context=_MOON_CTX,
    )
    assert recorder.calls == [("workflow_build_get", {"workflow_id": "wf_other"})]
    payload = _summary_payload(result)
    assert "targeted_via" not in payload  # explicit, not from context


# ---------------------------------------------------------------------------
# Catalog parity
# ---------------------------------------------------------------------------

def test_chat_tools_registers_all_five_moon_entries():
    names = {t["name"] for t in chat_tools.CHAT_TOOLS}
    assert {"moon_get_build", "moon_compose_from_prose", "moon_mutate_field", "moon_suggest_next", "moon_launch"} <= names
