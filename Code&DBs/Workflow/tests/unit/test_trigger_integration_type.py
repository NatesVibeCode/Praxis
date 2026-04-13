"""Tests for trigger_type='integration' in runtime.triggers."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from runtime.triggers import (
    MAX_TRIGGER_DEPTH,
    _evaluate_workflow_triggers_for_event,
    _fire_integration_trigger,
    _filter_matches,
)


# ---------------------------------------------------------------------------
# Fake Postgres connection
# ---------------------------------------------------------------------------

class _FakeConn:
    def __init__(self, trigger_rows=None):
        self._trigger_rows = trigger_rows or []
        self.executed: list[tuple] = []

    def execute(self, sql: str, *args):
        self.executed.append((sql, *args))
        if "workflow_triggers" in sql and "SELECT" in sql:
            return list(self._trigger_rows)
        return []


# ---------------------------------------------------------------------------
# _fire_integration_trigger
# ---------------------------------------------------------------------------

class TestFireIntegrationTrigger:
    def test_fires_execute_integration(self):
        conn = _FakeConn()
        trigger = {
            "id": "trig-1",
            "integration_id": "notifications",
            "integration_action": "send",
            "integration_args": {"channel": "alerts"},
        }
        payload = {"event_type": "run.succeeded", "run_id": "r1"}

        with patch("runtime.integrations.execute_integration") as mock_exec:
            mock_exec.return_value = {"status": "succeeded"}
            result = _fire_integration_trigger(
                conn, trigger=trigger, event={"id": 1}, payload=payload,
            )

        assert result is True
        mock_exec.assert_called_once()
        call_args = mock_exec.call_args
        assert call_args[0][0] == "notifications"
        assert call_args[0][1] == "send"
        merged = call_args[0][2]
        assert merged["channel"] == "alerts"
        assert merged["_trigger_event"] == payload

    def test_missing_integration_id_returns_zero(self):
        conn = _FakeConn()
        trigger = {
            "id": "trig-2",
            "integration_id": "",
            "integration_action": "send",
        }
        result = _fire_integration_trigger(
            conn, trigger=trigger, event={"id": 1}, payload={},
        )
        assert result is False

    def test_missing_integration_action_returns_false(self):
        conn = _FakeConn()
        trigger = {
            "id": "trig-3",
            "integration_id": "webhook",
            "integration_action": "",
        }
        result = _fire_integration_trigger(
            conn, trigger=trigger, event={"id": 1}, payload={},
        )
        assert result is False

    def test_string_integration_args_parsed(self):
        conn = _FakeConn()
        trigger = {
            "id": "trig-4",
            "integration_id": "webhook",
            "integration_action": "post",
            "integration_args": json.dumps({"url": "https://example.com"}),
        }

        with patch("runtime.integrations.execute_integration") as mock_exec:
            mock_exec.return_value = {"status": "succeeded"}
            _fire_integration_trigger(
                conn, trigger=trigger, event={"id": 1}, payload={},
            )

        merged = mock_exec.call_args[0][2]
        assert merged["url"] == "https://example.com"

    def test_returns_true_on_success(self):
        conn = _FakeConn()
        trigger = {
            "id": "trig-5",
            "integration_id": "notifications",
            "integration_action": "send",
            "integration_args": {},
        }

        with patch("runtime.integrations.execute_integration") as mock_exec:
            mock_exec.return_value = {"status": "succeeded"}
            result = _fire_integration_trigger(
                conn, trigger=trigger, event={"id": 1}, payload={},
            )

        assert result is True


# ---------------------------------------------------------------------------
# Depth limiting applies to integration triggers
# ---------------------------------------------------------------------------

class TestFireExceptionHandling:
    def test_exception_in_fire_does_not_kill_loop(self):
        """If one trigger raises, the next trigger still fires."""
        trigger_rows = [
            {
                "id": "trig-boom",
                "workflow_id": None,
                "filter": "{}",
                "trigger_type": "integration",
                "integration_id": "bad-int",
                "integration_action": "explode",
                "integration_args": "{}",
                "definition": None,
                "compiled_spec": None,
                "workflow_name": None,
            },
            {
                "id": "trig-ok",
                "workflow_id": None,
                "filter": "{}",
                "trigger_type": "integration",
                "integration_id": "good-int",
                "integration_action": "send",
                "integration_args": "{}",
                "definition": None,
                "compiled_spec": None,
                "workflow_name": None,
            },
        ]
        conn = _FakeConn(trigger_rows)
        event = {
            "id": 1,
            "event_type": "run.succeeded",
            "payload": {"trigger_depth": 0},
        }

        call_count = 0
        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("boom")
            return {"status": "succeeded"}

        with patch("runtime.integrations.execute_integration", side_effect=side_effect):
            fired = _evaluate_workflow_triggers_for_event(conn, event=event)

        assert fired == 1  # second trigger still fired

    def test_filter_mismatch_skips_integration_trigger(self):
        """Integration triggers respect filter matching."""
        trigger_rows = [
            {
                "id": "trig-filtered",
                "workflow_id": None,
                "filter": json.dumps({"status": "failed"}),
                "trigger_type": "integration",
                "integration_id": "notifications",
                "integration_action": "send",
                "integration_args": "{}",
                "definition": None,
                "compiled_spec": None,
                "workflow_name": None,
            },
        ]
        conn = _FakeConn(trigger_rows)
        event = {
            "id": 1,
            "event_type": "run.succeeded",
            "payload": {"status": "succeeded"},  # doesn't match filter
        }

        with patch("runtime.integrations.execute_integration") as mock_exec:
            fired = _evaluate_workflow_triggers_for_event(conn, event=event)

        assert fired == 0
        mock_exec.assert_not_called()

    def test_string_source_depth_coerced_to_int(self):
        """String trigger_depth in payload is coerced to int, not crash."""
        trigger_rows = [
            {
                "id": "trig-str-depth",
                "workflow_id": None,
                "filter": "{}",
                "trigger_type": "integration",
                "integration_id": "test-int",
                "integration_action": "ping",
                "integration_args": "{}",
                "definition": None,
                "compiled_spec": None,
                "workflow_name": None,
            },
        ]
        conn = _FakeConn(trigger_rows)
        event = {
            "id": 1,
            "event_type": "run.succeeded",
            "payload": {"trigger_depth": "2"},  # string, not int
        }

        with patch("runtime.integrations.execute_integration") as mock_exec:
            mock_exec.return_value = {"status": "succeeded"}
            fired = _evaluate_workflow_triggers_for_event(conn, event=event)

        assert fired == 1


class TestDepthLimiting:
    def test_integration_trigger_suppressed_at_max_depth(self):
        trigger_rows = [
            {
                "id": "trig-deep",
                "workflow_id": None,
                "filter": "{}",
                "trigger_type": "integration",
                "integration_id": "notifications",
                "integration_action": "send",
                "integration_args": "{}",
                "definition": None,
                "compiled_spec": None,
                "workflow_name": None,
            }
        ]
        conn = _FakeConn(trigger_rows)
        event = {
            "id": 99,
            "event_type": "run.succeeded",
            "payload": {"trigger_depth": MAX_TRIGGER_DEPTH},
        }

        with patch("runtime.integrations.execute_integration") as mock_exec:
            fired = _evaluate_workflow_triggers_for_event(conn, event=event)

        assert fired == 0
        mock_exec.assert_not_called()


# ---------------------------------------------------------------------------
# Backward compatibility: workflow triggers still work
# ---------------------------------------------------------------------------

class TestWorkflowTriggerCompat:
    def test_workflow_trigger_type_default(self):
        """Triggers without trigger_type field default to 'workflow'."""
        trigger_rows = [
            {
                "id": "trig-wf",
                "workflow_id": "wf-1",
                "filter": "{}",
                "trigger_type": "workflow",
                "integration_id": None,
                "integration_action": None,
                "integration_args": "{}",
                "definition": None,
                "compiled_spec": '{"name": "test", "jobs": [{"label": "j1", "prompt": "do thing"}]}',
                "workflow_name": "Test WF",
            }
        ]
        conn = _FakeConn(trigger_rows)
        event = {
            "id": 100,
            "event_type": "run.succeeded",
            "payload": {"trigger_depth": 0},
        }

        with patch("runtime.workflow.unified.submit_workflow_inline") as mock_submit:
            mock_submit.return_value = {"run_id": "r-new"}
            with patch("runtime.operating_model_planner.current_compiled_spec") as mock_spec:
                mock_spec.return_value = {"name": "test", "jobs": [{"label": "j1", "prompt": "do thing"}]}
                fired = _evaluate_workflow_triggers_for_event(conn, event=event)

        assert fired == 1
        mock_submit.assert_called_once()
