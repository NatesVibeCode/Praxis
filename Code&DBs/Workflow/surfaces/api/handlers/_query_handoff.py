"""Handoff query route family for the workflow HTTP API."""

from __future__ import annotations

from typing import Any

from runtime.operations.queries import handoff as _handoff

from .._payload_contract import coerce_query_int, coerce_query_text
from ._shared import _query_params, _serialize


def _query_text(params: dict[str, list[str]], field_name: str, *, required: bool = False) -> str | None:
    value = coerce_query_text(params.get(field_name), field_name=field_name)
    if required and not value:
        raise ValueError(f"{field_name} is required for handoff queries")
    return value


def _send_handoff_query(request: Any, payload: dict[str, Any]) -> None:
    request._send_json(200, _serialize(payload))


def _handle_handoff_latest_get(request: Any, path: str) -> None:
    try:
        params = _query_params(request.path)
        artifact_kind = _query_text(params, "artifact_kind", required=True)
        artifact_ref = _query_text(params, "artifact_ref")
        input_fingerprint = _query_text(params, "input_fingerprint")
        payload = _handoff.handle_query_handoff_latest(
            _handoff.QueryHandoffLatestArtifact(
                artifact_kind=str(artifact_kind),
                artifact_ref=artifact_ref,
                input_fingerprint=input_fingerprint,
            ),
            request.subsystems,
        )
        payload["filters"] = {
            "artifact_kind": artifact_kind,
            "artifact_ref": artifact_ref,
            "input_fingerprint": input_fingerprint,
        }
        _send_handoff_query(request, payload)
    except ValueError as exc:
        request._send_json(400, {"error": str(exc)})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_handoff_lineage_get(request: Any, path: str) -> None:
    try:
        params = _query_params(request.path)
        artifact_kind = _query_text(params, "artifact_kind", required=True)
        revision_ref = _query_text(params, "revision_ref", required=True)
        payload = _handoff.handle_query_handoff_artifact_lineage(
            _handoff.QueryHandoffArtifactLineage(
                artifact_kind=str(artifact_kind),
                revision_ref=str(revision_ref),
            ),
            request.subsystems,
        )
        payload["filters"] = {
            "artifact_kind": artifact_kind,
            "revision_ref": revision_ref,
        }
        _send_handoff_query(request, payload)
    except ValueError as exc:
        request._send_json(400, {"error": str(exc)})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_handoff_status_get(request: Any, path: str) -> None:
    try:
        params = _query_params(request.path)
        subscription_id = _query_text(params, "subscription_id", required=True)
        run_id = _query_text(params, "run_id", required=True)
        limit = coerce_query_int(
            params.get("limit"),
            field_name="limit",
            default=20,
            minimum=1,
            maximum=100,
        )
        payload = _handoff.handle_query_handoff_consumer_status(
            _handoff.QueryHandoffConsumerStatus(
                subscription_id=str(subscription_id),
                run_id=str(run_id),
                limit=limit,
            ),
            request.subsystems,
        )
        payload["filters"] = {
            "subscription_id": subscription_id,
            "run_id": run_id,
            "limit": limit,
        }
        _send_handoff_query(request, payload)
    except ValueError as exc:
        request._send_json(400, {"error": str(exc)})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_handoff_history_get(request: Any, path: str) -> None:
    try:
        params = _query_params(request.path)
        artifact_kind = _query_text(params, "artifact_kind", required=True)
        artifact_ref = _query_text(params, "artifact_ref")
        input_fingerprint = _query_text(params, "input_fingerprint")
        limit = coerce_query_int(
            params.get("limit"),
            field_name="limit",
            default=20,
            minimum=1,
            maximum=100,
        )
        payload = _handoff.handle_query_handoff_artifact_history(
            _handoff.QueryHandoffArtifactHistory(
                artifact_kind=str(artifact_kind),
                artifact_ref=artifact_ref,
                input_fingerprint=input_fingerprint,
                limit=limit,
            ),
            request.subsystems,
        )
        payload["filters"] = {
            "artifact_kind": artifact_kind,
            "artifact_ref": artifact_ref,
            "input_fingerprint": input_fingerprint,
            "limit": limit,
        }
        _send_handoff_query(request, payload)
    except ValueError as exc:
        request._send_json(400, {"error": str(exc)})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


__all__ = [
    "_handle_handoff_history_get",
    "_handle_handoff_latest_get",
    "_handle_handoff_lineage_get",
    "_handle_handoff_status_get",
]
