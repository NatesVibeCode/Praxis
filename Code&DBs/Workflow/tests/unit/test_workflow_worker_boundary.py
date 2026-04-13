from __future__ import annotations

import sys
import types
from typing import Any

from runtime.workflow.worker import WorkflowWorker


class _NoopConn:
    def execute(self, _query: str, *_args):
        raise AssertionError("worker should not write persistence directly when repositories are injected")


class _FakeRunNodeRepository:
    def __init__(self, *, claimed: bool = True) -> None:
        self.claimed = claimed
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def list_ready_card_nodes(self) -> list[dict[str, Any]]:
        self.calls.append(("list_ready_card_nodes", (), {}))
        return []

    def claim_ready_run_node(self, *, run_node_id: str) -> bool:
        self.calls.append(("claim_ready_run_node", (), {"run_node_id": run_node_id}))
        return self.claimed

    def mark_terminal_state(self, **kwargs) -> bool:
        self.calls.append(("mark_terminal_state", (), kwargs))
        return True

    def mark_failed(self, **kwargs) -> bool:
        self.calls.append(("mark_failed", (), kwargs))
        return True


class _FakeNotificationRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def emit_notification(self, **kwargs) -> None:
        self.calls.append(("emit_notification", kwargs))


def test_workflow_worker_delegates_run_node_state_and_notifications(monkeypatch) -> None:
    run_nodes = _FakeRunNodeRepository()
    notifications = _FakeNotificationRepository()
    released: list[tuple[str, str]] = []
    worker = WorkflowWorker(
        _NoopConn(),
        "/repo",
        run_node_repository=run_nodes,
        notification_repository=notifications,
    )

    monkeypatch.setitem(
        sys.modules,
        "runtime.model_executor",
        types.SimpleNamespace(
            execute_card=lambda conn, row, repo_root: {
                "status": "succeeded",
                "outputs": {"artifact": "ok"},
                "failure_code": "",
            },
            release_downstream=lambda conn, run_id, node_id: released.append((run_id, node_id)),
        ),
    )

    worker._execute_card_node(
        {
            "run_node_id": "node-1",
            "run_id": "run-1",
            "node_id": "card.plan",
            "node_type": "card_step",
            "input_payload": {"executor": {"kind": "app"}},
        }
    )

    assert run_nodes.calls == [
        ("claim_ready_run_node", (), {"run_node_id": "node-1"}),
        (
            "mark_terminal_state",
            (),
            {
                "run_node_id": "node-1",
                "state": "succeeded",
                "output_payload": {"artifact": "ok"},
                "failure_code": "",
            },
        ),
    ]
    assert notifications.calls == [
        (
            "emit_notification",
            {
                "run_id": "run-1",
                "job_label": "card.plan",
                "spec_name": "model_run",
                "agent_slug": "card_executor",
                "status": "succeeded",
                "failure_code": "",
                "duration_seconds": 0.0,
            },
        )
    ]
    assert released == [("run-1", "card.plan")]


def test_workflow_worker_delegates_failure_persistence_to_repository(monkeypatch) -> None:
    run_nodes = _FakeRunNodeRepository()
    notifications = _FakeNotificationRepository()
    worker = WorkflowWorker(
        _NoopConn(),
        "/repo",
        run_node_repository=run_nodes,
        notification_repository=notifications,
    )

    def _raise(_conn, _row, _repo_root):
        raise RuntimeError("explode")

    monkeypatch.setitem(
        sys.modules,
        "runtime.model_executor",
        types.SimpleNamespace(
            execute_card=_raise,
            release_downstream=lambda *_args, **_kwargs: None,
        ),
    )

    try:
        worker._execute_card_node(
            {
                "run_node_id": "node-2",
                "run_id": "run-2",
                "node_id": "card.build",
                "node_type": "card_step",
                "input_payload": {"executor": {"kind": "app"}},
            }
        )
    except RuntimeError as exc:
        assert str(exc) == "explode"
    else:  # pragma: no cover - fail closed if the worker swallows the error
        raise AssertionError("worker should re-raise execution failures")

    assert run_nodes.calls == [
        ("claim_ready_run_node", (), {"run_node_id": "node-2"}),
        (
            "mark_failed",
            (),
            {"run_node_id": "node-2", "failure_code": "explode"},
        ),
    ]
    assert notifications.calls == []
