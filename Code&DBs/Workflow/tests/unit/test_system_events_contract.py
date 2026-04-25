from __future__ import annotations

import ast
import inspect
from pathlib import Path

from runtime import system_events


def test_emit_system_event_signature_stays_keyword_only() -> None:
    signature = inspect.signature(system_events.emit_system_event)
    parameters = list(signature.parameters.values())

    assert [parameter.name for parameter in parameters] == list(
        system_events.SYSTEM_EVENT_SIGNATURE_FIELDS
    )
    assert parameters[0].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for parameter in parameters[1:]
    )
    assert system_events.SYSTEM_EVENT_SIGNATURE_FIELDS == (
        "conn",
        "event_type",
        "source_id",
        "source_type",
        "payload",
    )
    assert system_events.SYSTEM_EVENT_ENVELOPE_FIELDS == (
        "event_type",
        "source_id",
        "source_type",
        "payload",
    )


def test_emit_system_event_delegates_to_storage_authority(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_record_system_event(conn, **kwargs):
        captured["conn"] = conn
        captured["kwargs"] = kwargs

    monkeypatch.setattr(system_events, "record_system_event", _fake_record_system_event)

    conn = object()
    payload = {"run_id": "run-123"}
    system_events.emit_system_event(
        conn,
        event_type="workflow.completed",
        source_id="run-123",
        source_type="workflow_run",
        payload=payload,
    )

    assert captured == {
        "conn": conn,
        "kwargs": {
            "event_type": "workflow.completed",
            "source_id": "run-123",
            "source_type": "workflow_run",
            "payload": payload,
        },
    }


def test_emit_system_event_call_sites_use_the_canonical_keywords() -> None:
    workflow_root = Path(__file__).resolve().parents[2]
    expected_call_sites = [
        ("runtime/command_handlers.py", 73),
        ("runtime/cron_scheduler.py", 103),
        ("runtime/cron_scheduler.py", 134),
        ("runtime/database_maintenance.py", 1629),
        ("runtime/integrations/webhook_receiver.py", 138),
        ("runtime/intent_composition.py", 57),
        ("runtime/receipt_store.py", 288),
        ("runtime/scheduler.py", 262),
        ("runtime/scheduler.py", 278),
        ("runtime/spec_compiler.py", 788),
        ("runtime/typed_gap_events.py", 73),
        ("runtime/workflow/_worker_loop.py", 309),
        ("runtime/workflow/_worker_loop.py", 316),
        ("runtime/workflow/_workflow_state.py", 337),
        ("runtime/workflow/_workflow_state.py", 344),
        ("runtime/workflow_chain.py", 381),
        ("surfaces/mcp/tools/health.py", 323),
    ]
    observed_call_sites: list[tuple[str, int]] = []

    for relative_root in ("runtime", "surfaces"):
        for path in sorted((workflow_root / relative_root).rglob("*.py")):
            tree = ast.parse(path.read_text(), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                if not isinstance(node.func, ast.Name) or node.func.id != "emit_system_event":
                    continue
                assert len(node.args) == 1
                assert tuple(keyword.arg for keyword in node.keywords) == (
                    system_events.SYSTEM_EVENT_ENVELOPE_FIELDS
                )
                observed_call_sites.append(
                    (str(path.relative_to(workflow_root)), node.lineno)
                )

    assert observed_call_sites == expected_call_sites
