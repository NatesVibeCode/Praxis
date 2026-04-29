"""Notification, async-task, and streaming handlers for the workflow HTTP API."""

from __future__ import annotations

import json
import sys
from typing import Any

from ._shared import (
    REPO_ROOT,
    WORKFLOW_ROOT,
    RouteEntry,
    _ClientError,
    _exact,
    _prefix_suffix,
    _read_json_body,
)

def _save_chat_carry_forward(
    subsystems: Any,
    *,
    objective: str,
    assistant_content: str,
    tool_results: list[dict[str, Any]] | None = None,
):
    if not objective.strip() or not assistant_content.strip():
        return None
    try:
        from runtime.session_carry import (
            build_interaction_pack,
            load_effective_provider_job_catalog_for_carry,
        )

        manager = subsystems.get_session_carry_mgr()
        try:
            effective_catalog = load_effective_provider_job_catalog_for_carry(
                subsystems.get_pg_conn()
            )
        except Exception:
            return None
        pack = build_interaction_pack(
            manager,
            objective=objective,
            assistant_content=assistant_content,
            tool_results=tool_results or (),
            effective_provider_job_catalog=effective_catalog,
        )
        if pack is None:
            return None
        manager.save(pack)
        return pack
    except Exception:
        return None


def _handle_chat_conversations_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
    except (json.JSONDecodeError, ValueError) as exc:
        request._send_json(400, {"error": f"Invalid JSON: {exc}"})
        return

    try:
        pg = request.subsystems.get_pg_conn()
        from runtime.chat_orchestrator import ChatOrchestrator

        chat = ChatOrchestrator(pg, str(REPO_ROOT))
        title = body.get("title")
        conversation_id = chat.create_conversation(title)
        request._send_json(200, {"id": conversation_id})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_chat_messages_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
    except (json.JSONDecodeError, ValueError) as exc:
        request._send_json(400, {"error": f"Invalid JSON: {exc}"})
        return

    try:
        from runtime.chat_orchestrator import ChatOrchestrator

        pg = request.subsystems.get_pg_conn()
        conversation_id = path.split("/")[4]
        content = body.get("content", "")
        selection = body.get("selection_context")
        model_override = body.get("model")
        max_tokens = body.get("max_tokens", body.get("maxTokens"))
        chat = ChatOrchestrator(pg, str(REPO_ROOT))

        accept = request.headers.get("Accept", "")
        if "text/event-stream" in accept:
            request.send_response(200)
            request.send_header("Content-Type", "text/event-stream")
            request.send_header("Cache-Control", "no-cache")
            request.send_header("Connection", "keep-alive")
            request.send_header("Access-Control-Allow-Origin", "*")
            request.end_headers()

            streamed_text_parts: list[str] = []
            streamed_tool_results: list[dict[str, Any]] = []
            for event in chat.send_message_streaming(
                conversation_id,
                content,
                selection,
                model_override=model_override,
                max_tokens=max_tokens,
            ):
                event_type = event.get("event", "message")
                data = json.dumps(event.get("data", {}), default=str)
                if event_type == "text_delta":
                    streamed_text_parts.append(event.get("data", {}).get("text", ""))
                elif event_type == "tool_result":
                    streamed_tool_results.append({"result": event.get("data", {})})
                request.wfile.write(f"event: {event_type}\ndata: {data}\n\n".encode())
                request.wfile.flush()

            pack = _save_chat_carry_forward(
                request.subsystems,
                objective=content,
                assistant_content="".join(streamed_text_parts),
                tool_results=streamed_tool_results,
            )
            if pack is not None:
                payload = json.dumps({"pack_id": pack.pack_id}, default=str)
                request.wfile.write(f"event: carry_forward\ndata: {payload}\n\n".encode())
                request.wfile.flush()
            return

        result = chat.send_message(
            conversation_id,
            content,
            selection,
            model_override=model_override,
            max_tokens=max_tokens,
        )
        pack = _save_chat_carry_forward(
            request.subsystems,
            objective=content,
            assistant_content=result.get("content", ""),
            tool_results=result.get("tool_results", []),
        )
        if pack is not None:
            result["carry_forward_pack_id"] = pack.pack_id
        request._send_json(200, result)
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_chat_conversations_get(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        from runtime.chat_orchestrator import ChatOrchestrator

        chat = ChatOrchestrator(pg, str(REPO_ROOT))
        conversations = chat.list_conversations()
        request._send_json(200, {"conversations": conversations})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_chat_conversation_get(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        conversation_id = path.split("/api/chat/conversations/")[-1]
        from runtime.chat_orchestrator import ChatOrchestrator

        chat = ChatOrchestrator(pg, str(REPO_ROOT))
        conversation = chat.get_conversation(conversation_id)
        if conversation:
            request._send_json(200, conversation)
            return
        request._send_json(404, {"error": "Conversation not found"})
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_heartbeat(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    action = body.get("action", "status")
    detail = str(body.get("detail", "summary") or "summary").strip().lower()

    if action == "run":
        runner = subs.get_heartbeat_runner()
        result = runner.run_once()
        from runtime.heartbeat_runner import summarize_cycle_result

        return summarize_cycle_result(result)

    if action == "status":
        from runtime.heartbeat_runner import latest_heartbeat_status

        snapshot = latest_heartbeat_status(conn=subs.get_pg_conn())
        if snapshot is None:
            return {"message": "No heartbeat cycles have run yet."}
        return {"latest_cycle": snapshot.cycle_id, "summary": dict(snapshot.summary)}

    raise _ClientError(f"Unknown heartbeat action: {action}")


def _handle_session(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    action = body.get("action", "latest")
    mgr = subs.get_session_carry_mgr()
    from runtime.session_carry import (
        filter_pack_for_effective_provider_catalog,
        load_effective_provider_job_catalog_for_carry,
        pack_to_summary_dict,
    )

    try:
        effective_catalog = load_effective_provider_job_catalog_for_carry(
            subs.get_pg_conn()
        )
    except Exception as exc:
        return {
            "error_code": "session_provider_catalog_unavailable",
            "error": f"provider catalog unavailable for session carry-forward: {exc}",
        }

    if action == "latest":
        pack = mgr.latest()
        if pack is None:
            return {"message": "No carry-forward packs saved yet."}
        pack = filter_pack_for_effective_provider_catalog(
            pack,
            effective_provider_job_catalog=effective_catalog,
        )
        return pack_to_summary_dict(pack)

    if action == "validate":
        pack_id = body.get("pack_id", "")
        pack = mgr.load(pack_id) if pack_id else mgr.latest()
        if pack is None:
            return {"message": "Pack not found."}
        pack = filter_pack_for_effective_provider_catalog(
            pack,
            effective_provider_job_catalog=effective_catalog,
        )
        issues = mgr.validate(pack)
        if not issues:
            return {"valid": True, "pack": pack_to_summary_dict(pack)}
        return {"valid": False, "pack": pack_to_summary_dict(pack), "issues": issues}

    raise _ClientError(f"Unknown session action: {action}")


NOTIFY_POST_ROUTES: list[RouteEntry] = [
    (_exact("/api/chat/conversations"), _handle_chat_conversations_post),
    (_prefix_suffix("/api/chat/conversations/", "/messages"), _handle_chat_messages_post),
]

NOTIFY_GET_ROUTES: list[RouteEntry] = [
    (_exact("/api/chat/conversations"), _handle_chat_conversations_get),
    (
        lambda candidate: candidate.startswith("/api/chat/conversations/")
        and not candidate.endswith("/messages"),
        _handle_chat_conversation_get,
    ),
]

NOTIFY_ROUTES: dict[str, object] = {
    "/heartbeat": _handle_heartbeat,
    "/session": _handle_session,
}


__all__ = [
    "NOTIFY_GET_ROUTES",
    "NOTIFY_POST_ROUTES",
    "NOTIFY_ROUTES",
]
