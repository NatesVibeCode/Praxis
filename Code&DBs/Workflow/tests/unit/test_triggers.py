"""Tests for runtime.triggers."""

from __future__ import annotations

import importlib
import importlib.util
import json
import re
import sys
import types
from pathlib import Path

import pytest


def _import_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
triggers = _import_module("runtime.triggers", _WORKFLOW_ROOT / "runtime" / "triggers.py")


class _Conn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.workflow_events: list[dict[str, object]] = []
        self.workflow_triggers: list[dict[str, object]] = []
        self.subscriptions: list[dict[str, object]] = []
        self.subscription_checkpoints: list[dict[str, object]] = []
        self.subscription_events: list[dict[str, object]] = []

    def execute(self, query: str, *args):
        self.calls.append((query, args))
        normalized = " ".join(query.split())
        if (
            normalized.startswith("SELECT id, event_type, source_id, source_type, payload FROM system_events")
            or normalized.startswith("SELECT id, event_type, source_id, source_type, payload FROM public.system_events")
        ):
            rows = list(self.workflow_events)
            id_match = re.search(r"id > \$(\d+)", normalized)
            if id_match:
                rows = [
                    row for row in rows
                    if row["id"] > args[int(id_match.group(1)) - 1]
                ]
            return rows
        if (
            normalized.startswith("SELECT id, event_type, source_id, source_type, payload, created_at FROM system_events")
            or normalized.startswith("SELECT id, event_type, source_id, source_type, payload, created_at FROM public.system_events")
        ):
            rows = (
                list(self.subscription_events)
                if (
                    "created_at >= now() - interval '24 hours'" in normalized
                    or "event_type =" in normalized
                    or "event_type LIKE" in normalized
                    or "event_type = ANY" in normalized
                    or "source_id =" in normalized
                    or "source_type =" in normalized
                )
                else list(self.workflow_events)
            )
            id_match = re.search(r"id > \$(\d+)", normalized)
            if id_match:
                rows = [
                    row for row in rows
                    if row["id"] > args[int(id_match.group(1)) - 1]
                ]
            exact_match = re.search(r"event_type = \$(\d+)", normalized)
            if exact_match:
                expected = args[int(exact_match.group(1)) - 1]
                rows = [row for row in rows if row["event_type"] == expected]
            like_match = re.search(r"event_type LIKE \$(\d+)", normalized)
            if like_match:
                expected = re.escape(str(args[int(like_match.group(1)) - 1]))
                expected = expected.replace("%", ".*").replace("_", ".")
                rows = [
                    row for row in rows
                    if re.fullmatch(expected, row["event_type"])
                ]
            return rows
        if "FROM workflow_triggers" in normalized or "FROM public.workflow_triggers" in normalized:
            return self.workflow_triggers
        if "FROM event_subscriptions" in normalized or "FROM public.event_subscriptions" in normalized:
            return self.subscriptions
        if (
            "FROM subscription_checkpoints" in normalized
            or "FROM public.subscription_checkpoints" in normalized
        ):
            rows = list(self.subscription_checkpoints)
            if len(args) >= 2:
                rows = [
                    row
                    for row in rows
                    if row["subscription_id"] == args[0] and row["run_id"] == args[1]
                ]
            if "LIMIT 1" in normalized:
                return rows[-1:] if rows else []
            return rows
        if (
            "UPDATE public.workflow_triggers" in normalized
            or "UPDATE workflow_triggers" in normalized
        ):
            return [{"id": args[0]}]
        return []

    def fetchrow(self, query: str, *args):
        self.calls.append((query, args))
        normalized = " ".join(query.split())
        if (
            "INSERT INTO public.event_subscriptions" in normalized
            or "INSERT INTO event_subscriptions" in normalized
        ):
            row = {
                "subscription_id": args[0],
                "subscription_name": args[1],
                "consumer_kind": args[2],
                "envelope_kind": args[3],
                "workflow_id": args[4],
                "run_id": args[5],
                "cursor_scope": args[6],
                "status": args[7],
                "delivery_policy": json.loads(args[8]),
                "filter_policy": json.loads(args[9]),
                "created_at": args[10],
            }
            self.subscriptions = [
                existing
                for existing in self.subscriptions
                if existing["subscription_id"] != row["subscription_id"]
            ]
            self.subscriptions.append(row)
            return row
        if (
            "INSERT INTO public.subscription_checkpoints" in normalized
            or "INSERT INTO subscription_checkpoints" in normalized
        ):
            checkpoint_row = {
                "checkpoint_id": args[0],
                "subscription_id": args[1],
                "run_id": args[2],
                "last_evidence_seq": args[3],
                "last_authority_id": args[4],
                "checkpoint_status": args[5],
                "checkpointed_at": args[6],
                "metadata": json.loads(args[7]),
            }
            self.subscription_checkpoints = [
                row
                for row in self.subscription_checkpoints
                if not (
                    row["subscription_id"] == checkpoint_row["subscription_id"]
                    and row["run_id"] == checkpoint_row["run_id"]
                )
            ]
            self.subscription_checkpoints.append(checkpoint_row)
            return checkpoint_row
        if (
            "INSERT INTO public.system_events" in normalized
            or "INSERT INTO system_events" in normalized
        ):
            row = {
                "id": len(self.workflow_events) + len(self.subscription_events) + 1,
                "event_type": args[0],
                "source_id": args[1],
                "source_type": args[2],
                "payload": json.loads(args[3]),
            }
            return row
        return None


def _install_submit_stub(monkeypatch: pytest.MonkeyPatch, handler):
    runtime_pkg = importlib.import_module("runtime")
    workflow_pkg = importlib.import_module("runtime.workflow")
    unified_module = types.ModuleType("runtime.workflow.unified")
    unified_module.submit_workflow_inline = handler
    monkeypatch.setitem(sys.modules, "runtime.workflow.unified", unified_module)
    monkeypatch.setattr(runtime_pkg, "workflow", workflow_pkg, raising=False)
    monkeypatch.setattr(workflow_pkg, "unified", unified_module, raising=False)


def test_evaluate_triggers_emits_depth_exceeded_event(caplog):
    conn = _Conn()
    conn.workflow_events = [
        {
            "id": 1,
            "event_type": "run.succeeded",
            "source_id": "run-1",
            "source_type": "workflow_run",
            "payload": {"trigger_depth": triggers.MAX_TRIGGER_DEPTH},
        }
    ]
    conn.workflow_triggers = [
        {
            "id": "trig-1",
            "workflow_id": "wf-1",
            "filter": {},
            "definition": {"definition_revision": "rev-1"},
            "compiled_spec": {
                "definition_revision": "rev-1",
                "jobs": [{"prompt": "do work"}],
            },
            "workflow_name": "Workflow 1",
        }
    ]

    with caplog.at_level("WARNING"):
        fired = triggers.evaluate_triggers(conn)

    assert fired == 0
    insert_calls = [
        args
        for query, args in conn.calls
        if "INSERT INTO public.system_events" in query and args and args[0] == "trigger.depth_exceeded"
    ]
    assert len(insert_calls) == 1
    payload = json.loads(insert_calls[0][3])
    assert payload == {
        "trigger_id": "trig-1",
        "workflow_id": "wf-1",
        "depth": triggers.MAX_TRIGGER_DEPTH,
        "max_depth": triggers.MAX_TRIGGER_DEPTH,
    }
    assert "trigger.depth_exceeded" in caplog.text
    assert not any("UPDATE public.system_events SET processed = TRUE" in query for query, _ in conn.calls)
    checkpoint_call = next(
        args for query, args in conn.calls if "INSERT INTO public.subscription_checkpoints" in query
    )
    assert checkpoint_call[1] == "trigger_evaluator"
    assert checkpoint_call[2] == "trigger_evaluator"
    assert checkpoint_call[3] == 1
    assert checkpoint_call[4] == "system_event:1"
    assert checkpoint_call[5] == "committed"
    assert json.loads(checkpoint_call[7])["processor"] == "runtime.triggers._evaluate_workflow_triggers"


def test_evaluate_triggers_bootstraps_durable_trigger_evaluator_subscription(monkeypatch):
    conn = _Conn()
    conn.workflow_events = [
        {
            "id": 11,
            "event_type": "run.succeeded",
            "source_id": "run-11",
            "source_type": "workflow_run",
            "payload": {"trigger_depth": 0},
        }
    ]
    conn.workflow_triggers = [
        {
            "id": "trig-2",
            "workflow_id": "wf-2",
            "filter": {},
            "definition": {"definition_revision": "rev-2"},
            "compiled_spec": {
                "definition_revision": "rev-2",
                "jobs": [{"prompt": "do fallback work"}],
            },
            "workflow_name": "Workflow 2",
        }
    ]
    submitted: list[dict[str, object]] = []

    def _submit(_conn, spec_dict, run_id=None, parent_run_id=None, trigger_depth=0):
        submitted.append(
            {
                "spec": spec_dict,
                "parent_run_id": parent_run_id,
                "trigger_depth": trigger_depth,
            }
        )
        return {"run_id": "dispatch_child"}

    _install_submit_stub(monkeypatch, _submit)

    fired = triggers.evaluate_triggers(conn)

    assert fired == 1
    assert submitted[0]["parent_run_id"] == "run-11"
    assert submitted[0]["trigger_depth"] == 1
    bootstrap_call = next(
        args
        for query, args in conn.calls
        if "INSERT INTO public.event_subscriptions" in query
    )
    assert bootstrap_call[0] == "trigger_evaluator"
    assert bootstrap_call[1] == "Workflow Trigger Evaluator"
    assert bootstrap_call[2] == "system"
    assert bootstrap_call[3] == "system_event"
    assert bootstrap_call[4] is None
    assert bootstrap_call[5] is None
    assert bootstrap_call[6] == "global"
    assert not any("processed = FALSE" in query for query, _ in conn.calls)


def test_evaluate_triggers_advances_checkpoint_and_skips_old_events_on_replay(monkeypatch):
    conn = _Conn()
    conn.workflow_events = [
        {
            "id": 1,
            "event_type": "run.succeeded",
            "source_id": "run-1",
            "source_type": "workflow_run",
            "payload": {"trigger_depth": 0},
        }
    ]
    conn.workflow_triggers = [
        {
            "id": "trig-replay",
            "workflow_id": "wf-replay",
            "filter": {},
            "definition": {"definition_revision": "rev-replay"},
            "compiled_spec": {
                "definition_revision": "rev-replay",
                "jobs": [{"prompt": "do replay work"}],
            },
            "workflow_name": "Replay Workflow",
        }
    ]
    submitted_parent_ids: list[str] = []

    def _submit(_conn, spec_dict, run_id=None, parent_run_id=None, trigger_depth=0):
        del spec_dict, run_id, trigger_depth
        submitted_parent_ids.append(str(parent_run_id))
        return {"run_id": f"dispatch_child_{len(submitted_parent_ids)}"}

    _install_submit_stub(monkeypatch, _submit)

    first_fired = triggers.evaluate_triggers(conn)

    assert first_fired == 1
    assert submitted_parent_ids == ["run-1"]
    assert conn.subscription_checkpoints[0]["last_evidence_seq"] == 1

    conn.workflow_events.append(
        {
            "id": 2,
            "event_type": "run.succeeded",
            "source_id": "run-2",
            "source_type": "workflow_run",
            "payload": {"trigger_depth": 0},
        }
    )

    second_fired = triggers.evaluate_triggers(conn)

    assert second_fired == 1
    assert submitted_parent_ids == ["run-1", "run-2"]
    assert conn.subscription_checkpoints[0]["last_evidence_seq"] == 2

    third_fired = triggers.evaluate_triggers(conn)

    assert third_fired == 0
    assert submitted_parent_ids == ["run-1", "run-2"]


def test_evaluate_triggers_matches_schedule_fired_event_against_schedule_trigger(monkeypatch):
    conn = _Conn()
    conn.workflow_events = [
        {
            "id": 9,
            "event_type": "schedule.fired",
            "source_id": "daily-report",
            "source_type": "scheduler_state",
            "payload": {"trigger_depth": 0, "job_name": "daily-report"},
        }
    ]
    conn.workflow_triggers = [
        {
            "id": "trig-schedule",
            "workflow_id": "wf-schedule",
            "filter": {},
            "event_type": "schedule",
            "definition": {"definition_revision": "rev-schedule"},
            "compiled_spec": {
                "definition_revision": "rev-schedule",
                "jobs": [{"prompt": "do scheduled work"}],
            },
            "workflow_name": "Scheduled Workflow",
        }
    ]
    submitted: list[dict[str, object]] = []

    def _submit(_conn, spec_dict, run_id=None, parent_run_id=None, trigger_depth=0):
        submitted.append(
            {
                "spec": spec_dict,
                "parent_run_id": parent_run_id,
                "trigger_depth": trigger_depth,
            }
        )
        return {"run_id": "dispatch_schedule_child"}

    _install_submit_stub(monkeypatch, _submit)

    fired = triggers.evaluate_triggers(conn)

    assert fired == 1
    assert submitted[0]["parent_run_id"] == "daily-report"
    assert submitted[0]["trigger_depth"] == 1
    assert conn.subscription_checkpoints[0]["last_evidence_seq"] == 9


def test_evaluate_triggers_processes_event_subscriptions_without_checkpoint(monkeypatch):
    conn = _Conn()
    conn.subscriptions = [
        {
            "subscription_id": "sub-1",
            "subscription_name": "subscription 1",
            "workflow_id": "wf-1",
            "run_id": None,
            "filter_policy": {"event_type": "workflow.%", "payload": {"status": "ok"}},
            "definition": {"definition_revision": "rev-1"},
            "compiled_spec": {
                "definition_revision": "rev-1",
                "jobs": [{"prompt": "do work"}],
            },
            "workflow_name": "Workflow 1",
        }
    ]
    conn.subscription_events = [
        {
            "id": 42,
            "event_type": "workflow.completed",
            "source_id": "run-42",
            "source_type": "workflow_run",
            "payload": {"status": "ok"},
            "created_at": "2026-04-07T00:00:00Z",
        }
    ]
    submitted: list[dict[str, object]] = []

    def _submit(_conn, spec_dict, run_id=None, parent_run_id=None, trigger_depth=0):
        submitted.append(
            {
                "spec": spec_dict,
                "parent_run_id": parent_run_id,
                "trigger_depth": trigger_depth,
            }
        )
        return {"run_id": "dispatch_child"}

    _install_submit_stub(monkeypatch, _submit)

    fired = triggers.evaluate_triggers(conn)

    assert fired == 1
    assert submitted[0]["parent_run_id"] == "run-42"
    assert submitted[0]["trigger_depth"] == 1
    prompt = submitted[0]["spec"]["jobs"][0]["prompt"]
    assert "## Subscription Context" in prompt
    assert "Subscription: sub-1" in prompt
    event_query = next(
        query for query, _ in conn.calls
        if "payload, created_at FROM public.system_events" in query
    )
    assert "created_at >= now() - interval '24 hours'" in event_query
    checkpoint_call = next(
        args for query, args in conn.calls
        if "INSERT INTO public.subscription_checkpoints" in query and args[1] == "sub-1"
    )
    assert checkpoint_call[0] == "checkpoint:sub-1:subscription:sub-1"
    assert checkpoint_call[1] == "sub-1"
    assert checkpoint_call[2] == "subscription:sub-1"
    assert checkpoint_call[3] == 42
    assert checkpoint_call[4] == "system_event:42"
    assert checkpoint_call[5] == "committed"


def test_event_subscriptions_resume_from_checkpoint_without_double_processing(monkeypatch):
    first_conn = _Conn()
    first_conn.subscriptions = [
        {
            "subscription_id": "sub-replay",
            "subscription_name": "subscription replay",
            "workflow_id": "wf-replay",
            "run_id": None,
            "filter_policy": {"event_type": "workflow.completed"},
            "definition": {"definition_revision": "rev-replay"},
            "compiled_spec": {
                "definition_revision": "rev-replay",
                "jobs": [{"prompt": "do replay work"}],
            },
            "workflow_name": "Replay Workflow",
        }
    ]
    first_conn.subscription_events = [
        {
            "id": 41,
            "event_type": "workflow.completed",
            "source_id": "run-41",
            "source_type": "workflow_run",
            "payload": {"status": "ok"},
            "created_at": "2026-04-07T00:00:00Z",
        }
    ]
    submitted_parent_ids: list[str] = []

    def _submit(_conn, spec_dict, run_id=None, parent_run_id=None, trigger_depth=0):
        del spec_dict, run_id, trigger_depth
        submitted_parent_ids.append(str(parent_run_id))
        return {"run_id": f"dispatch_child_{len(submitted_parent_ids)}"}

    _install_submit_stub(monkeypatch, _submit)

    first_fired = triggers.evaluate_event_subscriptions(first_conn)

    assert first_fired == 1
    assert submitted_parent_ids == ["run-41"]
    assert first_conn.subscription_checkpoints[0]["last_evidence_seq"] == 41

    replay_conn = _Conn()
    replay_conn.subscriptions = [dict(row) for row in first_conn.subscriptions]
    replay_conn.subscription_checkpoints = [
        dict(row) for row in first_conn.subscription_checkpoints
    ]
    replay_conn.subscription_events = [
        {
            "id": 41,
            "event_type": "workflow.completed",
            "source_id": "run-41",
            "source_type": "workflow_run",
            "payload": {"status": "ok"},
            "created_at": "2026-04-07T00:00:00Z",
        },
        {
            "id": 42,
            "event_type": "workflow.completed",
            "source_id": "run-42",
            "source_type": "workflow_run",
            "payload": {"status": "ok"},
            "created_at": "2026-04-07T00:00:01Z",
        },
    ]

    second_fired = triggers.evaluate_event_subscriptions(replay_conn)

    assert second_fired == 1
    assert submitted_parent_ids == ["run-41", "run-42"]
    replay_query = next(
        query
        for query, _ in replay_conn.calls
        if "payload, created_at FROM public.system_events" in query
    )
    assert "id > $1" in replay_query
    assert "created_at >= now() - interval '24 hours'" not in replay_query
    assert replay_conn.subscription_checkpoints[0]["last_evidence_seq"] == 42

    third_fired = triggers.evaluate_event_subscriptions(replay_conn)

    assert third_fired == 0
    assert submitted_parent_ids == ["run-41", "run-42"]


def test_event_subscription_failure_does_not_block_other_subscriptions(monkeypatch, caplog):
    conn = _Conn()
    conn.subscriptions = [
        {
            "subscription_id": "sub-fail",
            "subscription_name": "subscription fail",
            "workflow_id": "wf-fail",
            "run_id": "run-fail",
            "filter_policy": {"event_type": "workflow.failed"},
            "definition": {"definition_revision": "rev-fail"},
            "compiled_spec": {
                "definition_revision": "rev-fail",
                "jobs": [{"prompt": "fail"}],
            },
            "workflow_name": "Workflow fail",
        },
        {
            "subscription_id": "sub-ok",
            "subscription_name": "subscription ok",
            "workflow_id": "wf-ok",
            "run_id": "run-ok",
            "filter_policy": {"event_type": "workflow.completed"},
            "definition": {"definition_revision": "rev-ok"},
            "compiled_spec": {
                "definition_revision": "rev-ok",
                "jobs": [{"prompt": "ok"}],
            },
            "workflow_name": "Workflow ok",
        },
    ]
    conn.subscription_events = [
        {
            "id": 7,
            "event_type": "workflow.failed",
            "source_id": "run-7",
            "source_type": "workflow_run",
            "payload": {},
            "created_at": "2026-04-07T00:00:00Z",
        },
        {
            "id": 8,
            "event_type": "workflow.completed",
            "source_id": "run-8",
            "source_type": "workflow_run",
            "payload": {},
            "created_at": "2026-04-07T00:00:01Z",
        },
    ]
    submit_calls: list[str] = []

    def _submit(_conn, spec_dict, run_id=None, parent_run_id=None, trigger_depth=0):
        prompt = spec_dict["jobs"][0]["prompt"]
        if "Subscription: sub-fail" in prompt:
            raise ValueError("boom")
        submit_calls.append(parent_run_id)
        return {"run_id": "dispatch_child_ok"}

    _install_submit_stub(monkeypatch, _submit)

    with caplog.at_level("ERROR"):
        fired = triggers.evaluate_event_subscriptions(conn)

    assert fired == 1
    assert submit_calls == ["run-8"]
    assert "Event subscription sub-fail failed on event 7" in caplog.text
    checkpoint_calls = [
        args for query, args in conn.calls
        if "INSERT INTO public.subscription_checkpoints" in query
    ]
    assert len(checkpoint_calls) == 1
    assert checkpoint_calls[0][1] == "sub-ok"


def test_evaluate_event_subscriptions_ignores_worker_run_subscriptions():
    conn = _Conn()
    conn.subscriptions = [
        {
            "subscription_id": "sub-worker",
            "subscription_name": "worker subscription",
            "workflow_id": "wf-worker",
            "run_id": "run-worker",
            "consumer_kind": "worker",
            "cursor_scope": "run",
            "filter_policy": {"event_type": "workflow.%"},
            "definition": None,
            "compiled_spec": None,
            "workflow_name": "Worker Workflow",
        }
    ]

    fired = triggers.evaluate_event_subscriptions(conn)

    assert fired == 0
    subscription_query = next(
        query for query, _ in conn.calls
        if "FROM public.event_subscriptions" in query
    )
    assert "COALESCE(s.consumer_kind, '') = 'worker'" in subscription_query
    assert "COALESCE(s.cursor_scope, '') = 'run'" in subscription_query
    assert not any("FROM public.subscription_checkpoints" in query for query, _ in conn.calls)
