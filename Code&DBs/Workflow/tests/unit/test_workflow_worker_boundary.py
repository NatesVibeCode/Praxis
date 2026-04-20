from __future__ import annotations

import sys
import types
from typing import Any

import storage.postgres.workflow_orchestration_repository as run_node_repo_mod
import runtime.workflow.worker as workflow_worker
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

    def mark_awaiting_human(self, **kwargs) -> bool:
        self.calls.append(("mark_awaiting_human", (), kwargs))
        return True

    def mark_failed(self, **kwargs) -> bool:
        self.calls.append(("mark_failed", (), kwargs))
        return True


def test_workflow_worker_delegates_terminal_state_to_receipt_backed_repository(monkeypatch) -> None:
    run_nodes = _FakeRunNodeRepository()
    released: list[tuple[str, str]] = []
    worker = WorkflowWorker(
        _NoopConn(),
        "/repo",
        run_node_repository=run_nodes,
    )
    receipt_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        workflow_worker,
        "write_run_node_receipt",
        lambda _conn, **kwargs: receipt_calls.append(dict(kwargs)) or "receipt:run-1:card.plan:1:terminal",
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
                "receipt_id": "receipt:run-1:card.plan:1:terminal",
            },
        ),
    ]
    assert receipt_calls == [
        {
            "run_node_id": "node-1",
            "phase": "terminal",
            "receipt_type": "node_execution_receipt",
            "status": "succeeded",
            "outputs": {"artifact": "ok"},
            "failure_code": "",
            "agent_slug": "card_executor",
            "executor_type": "runtime.workflow.worker",
        }
    ]
    assert released == [("run-1", "card.plan")]


def test_workflow_worker_delegates_failure_persistence_to_repository(monkeypatch) -> None:
    run_nodes = _FakeRunNodeRepository()
    worker = WorkflowWorker(
        _NoopConn(),
        "/repo",
        run_node_repository=run_nodes,
    )
    monkeypatch.setattr(
        workflow_worker,
        "write_run_node_receipt",
        lambda _conn, **kwargs: "receipt:run-2:card.build:1:terminal",
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
            {
                "run_node_id": "node-2",
                "failure_code": "worker_exception",
                "receipt_id": "receipt:run-2:card.build:1:terminal",
            },
        ),
    ]


def test_run_node_failure_code_normalizer_does_not_truncate() -> None:
    code = "worker_exception." + ("x" * 240)

    assert run_node_repo_mod._normalize_failure_code(code) == code


def test_workflow_worker_records_awaiting_human_with_canonical_receipt(monkeypatch) -> None:
    run_nodes = _FakeRunNodeRepository()
    worker = WorkflowWorker(
        _NoopConn(),
        "/repo",
        run_node_repository=run_nodes,
    )
    receipt_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        workflow_worker,
        "write_run_node_receipt",
        lambda _conn, **kwargs: receipt_calls.append(dict(kwargs)) or "receipt:run-3:card.review:1:awaiting_human",
    )

    monkeypatch.setitem(
        sys.modules,
        "runtime.model_executor",
        types.SimpleNamespace(
            execute_card=lambda conn, row, repo_root: {
                "status": "awaiting_human",
                "outputs": {"reason": "Human approval required"},
            },
            release_downstream=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("awaiting_human should not release downstream cards")
            ),
        ),
    )

    worker._execute_card_node(
        {
            "run_node_id": "node-3",
            "run_id": "run-3",
            "node_id": "card.review",
            "node_type": "card_decision",
            "input_payload": {"executor": {"kind": "human"}},
        }
    )

    assert run_nodes.calls == [
        ("claim_ready_run_node", (), {"run_node_id": "node-3"}),
        (
            "mark_awaiting_human",
            (),
            {
                "run_node_id": "node-3",
                "output_payload": {"reason": "Human approval required"},
                "receipt_id": "receipt:run-3:card.review:1:awaiting_human",
            },
        ),
    ]
    assert receipt_calls == [
        {
            "run_node_id": "node-3",
            "phase": "awaiting_human",
            "receipt_type": "node_awaiting_human_receipt",
            "status": "awaiting_human",
            "outputs": {"reason": "Human approval required"},
            "agent_slug": "human",
            "executor_type": "runtime.workflow.worker",
        }
    ]
