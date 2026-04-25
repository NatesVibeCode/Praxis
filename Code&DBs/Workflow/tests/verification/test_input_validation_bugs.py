from __future__ import annotations

from datetime import datetime, timezone
import io
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from surfaces.mcp.tools import operator as operator_tool
from surfaces.mcp.tools import workflow as workflow_tool
from surfaces.api.handlers import _shared as shared
from surfaces.api.handlers import _dispatch as dispatch



def test_operator_positive_int_and_boolean_parsing() -> None:
    assert operator_tool._parse_positive_int(17, field_name="limit") == 17
    assert operator_tool._parse_positive_int("17", field_name="limit") == 17

    with pytest.raises(ValueError):
        operator_tool._parse_positive_int(True, field_name="open_only")
    with pytest.raises(ValueError):
        operator_tool._parse_positive_int(0, field_name="limit")
    with pytest.raises(ValueError):
        operator_tool._parse_positive_int(-1, field_name="limit")
    with pytest.raises(ValueError):
        operator_tool._parse_positive_int("abc", field_name="limit")
    with pytest.raises(ValueError):
        operator_tool._parse_positive_int(False, field_name="limit")

    assert operator_tool._parse_bool("true", field_name="confirm") is True
    assert operator_tool._parse_bool("FALSE", field_name="confirm") is False
    assert operator_tool._parse_bool(True, field_name="confirm") is True
    with pytest.raises(ValueError):
        operator_tool._parse_bool("maybe", field_name="confirm")


def test_operator_bounded_limit_respects_maximum() -> None:
    params = {"limit": "9999"}
    assert operator_tool._bounded_limit(params, default=80) == 500
    params = {"limit": "12"}
    assert operator_tool._bounded_limit(params, default=80) == 12
    assert operator_tool._bounded_limit({}, default=80) == 80



def test_workflow_action_enum_validation() -> None:
    assert workflow_tool._parse_workflow_action("run") == "run"
    assert workflow_tool._parse_workflow_action("  preview  ") == "preview"
    assert workflow_tool._parse_workflow_action("wait") == "wait"
    with pytest.raises(ValueError):
        workflow_tool._parse_workflow_action("deploy")
    with pytest.raises(ValueError):
        workflow_tool._parse_workflow_action(True)


def test_request_body_size_ceiling_is_1mb() -> None:
    assert shared.MAX_REQUEST_BODY_BYTES == 1024 * 1024


class _FakeRequest:
    def __init__(self, *, headers: dict[str, str], body_bytes: bytes) -> None:
        self.headers = headers
        self.rfile = io.BytesIO(body_bytes)


def test_dispatch_post_rejects_empty_body_for_required_routes() -> None:
    seen: dict[str, object] = {}

    class Request:
        def __init__(self) -> None:
            self._payload = None

        def _send_json(self, status: int, payload) -> None:
            seen["status"] = status
            seen["payload"] = payload

    fake_request = Request()
    fake_request.headers = {"Content-Length": "0"}
    fake_request.rfile = io.BytesIO(b"")
    fake_request.subsystems = object()

    called = []

    def handler(_subs, body):
        called.append(body)
        return {"ok": True}

    def record(*_args, **_kwargs):
        called.append("record")

    # path included in required_body_paths must reject empty object
    dispatch._dispatch_standard_post(
        fake_request,
        "/test/required",
        {"/test/required": handler},
        record_api_route_usage=record,
        required_body_paths={"/test/required"},
    )

    assert seen["status"] == 400
    assert "must be a non-empty JSON object" in str(seen["payload"])  # type: ignore[arg-type]

    # Non-empty payload reaches handler
    payload_body = b'{"value": 1}'
    fake_request = Request()
    fake_request.headers = {"Content-Length": str(len(payload_body))}
    fake_request.rfile = io.BytesIO(payload_body)
    fake_request.subsystems = object()
    dispatch._dispatch_standard_post(
        fake_request,
        "/test/required",
        {"/test/required": handler},
        record_api_route_usage=record,
        required_body_paths={"/test/required"},
    )

    assert seen["status"] == 200
