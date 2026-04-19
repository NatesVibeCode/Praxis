"""Bug route family for the workflow query surface."""

from __future__ import annotations

from typing import Any

from .._payload_contract import (
    coerce_query_bool,
    coerce_query_int,
    coerce_query_text,
    coerce_text_sequence,
)
from . import _bug_surface_contract as _bug_contract
from . import workflow_query_core as _workflow_query_core
from ._shared import _ClientError, _query_params


def _parse_bug_status(bt_mod: Any, raw_status: object):
    try:
        return _bug_contract.parse_bug_status(bt_mod, raw_status)
    except ValueError as exc:
        raise _ClientError(str(exc)) from exc


def _parse_bug_severity(bt_mod: Any, raw_severity: object):
    try:
        return _bug_contract.parse_bug_severity(bt_mod, raw_severity)
    except ValueError as exc:
        raise _ClientError(str(exc)) from exc


def _parse_bug_category(bt_mod: Any, raw_category: object):
    try:
        return _bug_contract.parse_bug_category(bt_mod, raw_category)
    except ValueError as exc:
        raise _ClientError(str(exc)) from exc


def _handle_bugs(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    return _workflow_query_core.handle_bugs(
        subs,
        body,
        parse_bug_status=_parse_bug_status,
        parse_bug_severity=_parse_bug_severity,
        parse_bug_category=_parse_bug_category,
    )


def _handle_bugs_get(request: Any, path: str) -> None:
    try:
        params = _query_params(request.path)
        limit = coerce_query_int(
            params.get("limit"),
            field_name="limit",
            default=50,
            minimum=1,
            strict=True,
        )
        replay_ready_only = coerce_query_bool(
            params.get("replay_ready_only"),
            field_name="replay_ready_only",
            default=False,
        )
        open_only = coerce_query_bool(
            params.get("open_only"),
            field_name="open_only",
            default=False,
        )
        status = coerce_query_text(
            params.get("status"),
            field_name="status",
        )
        severity = coerce_query_text(
            params.get("severity"),
            field_name="severity",
        )
        category = coerce_query_text(
            params.get("category"),
            field_name="category",
        )
        title_like = coerce_query_text(
            params.get("title_like"),
            field_name="title_like",
        )
        tags = coerce_text_sequence(
            params.get("tags"),
            field_name="tags",
        )
        exclude_tags = coerce_text_sequence(
            params.get("exclude_tags"),
            field_name="exclude_tags",
        )
        source_issue_id = coerce_query_text(
            params.get("source_issue_id"),
            field_name="source_issue_id",
        )
        include_replay_state = coerce_query_bool(
            params.get("include_replay_state"),
            field_name="include_replay_state",
            default=False,
        )
        result = _handle_bugs(
            request.subsystems,
            {
                "action": "list",
                "limit": limit,
                "status": status,
                "severity": severity,
                "category": category,
                "title_like": title_like,
                "tags": tags,
                "exclude_tags": exclude_tags,
                "source_issue_id": source_issue_id,
                "include_replay_state": include_replay_state,
                "replay_ready_only": replay_ready_only,
                "open_only": open_only,
            },
        )
        request._send_json(200, result)
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_bugs_replay_ready_get(request: Any, path: str) -> None:
    try:
        params = _query_params(request.path)
        limit = coerce_query_int(
            params.get("limit"),
            field_name="limit",
            default=50,
            minimum=1,
            strict=True,
        )
        refresh_backfill = coerce_query_bool(
            params.get("refresh_backfill"),
            field_name="refresh_backfill",
            default=False,
        )
        result = _workflow_query_core.handle_operator_view(
            request.subsystems,
            {
                "view": "replay_ready_bugs",
                "limit": limit,
                "refresh_backfill": refresh_backfill,
            },
        )
        request._send_json(200, result)
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


__all__ = [
    "_handle_bugs",
    "_handle_bugs_get",
    "_handle_bugs_replay_ready_get",
]
