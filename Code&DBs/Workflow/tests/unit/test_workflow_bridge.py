from __future__ import annotations

import asyncpg

from runtime.claims import ClaimLeaseProposalRuntime
from runtime.domain import RunState, RuntimeBoundaryError
import runtime.outbox as outbox_mod
import runtime.subscription_repository as subscription_repo_mod
import runtime.subscriptions as subscriptions_mod
import storage.postgres.connection as connection_mod
from surfaces import workflow_bridge as bridge_mod
from surfaces.workflow_bridge import build_live_workflow_bridge


class _FakeConn:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    async def fetchrow(self, query: str, *args):
        if "FROM workflow_runs" in query:
            return {
                "run_id": args[0],
                "workflow_id": "workflow-1",
                "request_id": "request-1",
                "current_state": "queued",
                "request_envelope": {"claim_id": f"claim:{args[0]}"},
                "attempt_no": 1,
                "last_event_id": "event-1",
            }
        raise AssertionError(query)


class _FakeSubscriber:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url


class _FakeRepo:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url


class _FakeWorkerSubscription:
    def __init__(self, *, subscriber, repository) -> None:
        self.subscriber = subscriber
        self.repository = repository


def test_live_workflow_bridge_falls_back_to_workflow_run_when_route_row_missing(monkeypatch) -> None:
    bridge_mod._BOOTSTRAPPED_BRIDGE_DB = None
    monkeypatch.setattr(connection_mod, "ensure_postgres_available", lambda env=None: object())
    monkeypatch.setattr(asyncpg, "connect", _fake_connect)
    monkeypatch.setattr(
        ClaimLeaseProposalRuntime,
        "inspect_route",
        lambda self, conn, *, run_id: (_ for _ in ()).throw(
            RuntimeBoundaryError(f"runtime route {run_id!r} is missing")
        ),
    )
    monkeypatch.setattr(outbox_mod, "PostgresWorkflowOutboxSubscriber", _FakeSubscriber)
    monkeypatch.setattr(subscription_repo_mod, "PostgresEventSubscriptionRepository", _FakeRepo)
    monkeypatch.setattr(subscriptions_mod, "WorkflowWorkerSubscription", _FakeWorkerSubscription)

    bridge = build_live_workflow_bridge("postgresql://repo.test/workflow")
    snapshot = bridge.routes.inspect_route(run_id="run-queued")

    assert snapshot.run_id == "run-queued"
    assert snapshot.workflow_id == "workflow-1"
    assert snapshot.request_id == "request-1"
    assert snapshot.current_state is RunState.QUEUED
    assert snapshot.claim_id == "claim:run-queued"
    assert snapshot.last_event_id == "event-1"


async def _fake_connect(*_args, **_kwargs) -> _FakeConn:
    return _FakeConn()
