"""Model Eval matrix runner."""

from __future__ import annotations

import json
import hashlib
import time
import uuid
from pathlib import Path
from typing import Any

from .catalog import build_suite_plan, catalog_version_hash
from .openrouter import BLOCKED_PROVIDER_SLUGS, OpenRouterError, build_lab_request, chat_completion
from .pins import (
    MODEL_EVAL_WORKER_TASK_TYPE,
    PinnedModelEvalRouteError,
    pinned_candidate_ref_from_model_config,
    validate_model_eval_model_config,
)
from .validators import validate_task_output


BASE_SYSTEM_PROMPT = (
    "You are running Praxis Model Eval. Consistency matters more than style. "
    "Use the supplied fixture exactly. Return only machine-checkable output. "
    "Do not use Markdown fences. Do not claim that production routing changed."
)


def _repo_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / ".git").exists():
            return parent
    return Path.cwd()


def _safe_artifact_path(root: Path, relative_path: str) -> Path:
    cleaned = Path(relative_path)
    if cleaned.is_absolute():
        raise ValueError("artifact paths must be relative")
    target = (root / cleaned).resolve()
    root_resolved = root.resolve()
    if root_resolved not in target.parents and target != root_resolved:
        raise ValueError("artifact path escapes run root")
    return target


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _provider_requested(provider_order: list[str]) -> str:
    return ",".join(provider_order)


def _provider_root(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return ""
    return normalized.split("/", 1)[0]


def _served_provider_check(provider_order: list[str], served_provider: Any) -> dict[str, Any]:
    served_root = _provider_root(served_provider)
    if not served_root:
        return {
            "ok": False,
            "check": "served provider recorded",
            "detail": "OpenRouter response did not include a served provider.",
        }
    blocked = {_provider_root(item) for item in BLOCKED_PROVIDER_SLUGS}
    if served_root in blocked:
        return {
            "ok": False,
            "check": "served provider not blocked",
            "detail": f"served provider {served_root!r} is blocked",
        }
    requested_roots = {_provider_root(item) for item in provider_order if _provider_root(item)}
    if not requested_roots:
        return {
            "ok": False,
            "check": "requested provider explicit",
            "detail": "No requested provider order was supplied.",
        }
    if served_root not in requested_roots:
        return {
            "ok": False,
            "check": "served provider matches requested route",
            "detail": {
                "served_provider": served_provider,
                "requested_provider_order": provider_order,
            },
        }
    return {
        "ok": True,
        "check": "served provider matches requested route",
        "detail": {
            "served_provider": served_provider,
            "requested_provider_order": provider_order,
        },
    }


def _parse_message(raw: dict[str, Any], *, tool_task: bool) -> tuple[dict[str, Any], str | None]:
    choice = ((raw.get("choices") or [{}])[0] or {}) if isinstance(raw, dict) else {}
    message = choice.get("message") if isinstance(choice, dict) else {}
    if not isinstance(message, dict):
        return {}, "missing message"
    if tool_task:
        return {"tool_calls": message.get("tool_calls") or []}, None
    content = message.get("content")
    if not isinstance(content, str):
        return {}, "missing content"
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as exc:
        return {"raw_content": content}, f"non_json_content: {exc}"
    if not isinstance(parsed, dict):
        return {"raw_content": content}, "content_json_not_object"
    return parsed, None


def _choice_message(raw: dict[str, Any]) -> dict[str, Any]:
    choice = ((raw.get("choices") or [{}])[0] or {}) if isinstance(raw, dict) else {}
    message = choice.get("message") if isinstance(choice, dict) else {}
    return message if isinstance(message, dict) else {}


def _tool_call_arguments(call: dict[str, Any]) -> dict[str, Any]:
    function = call.get("function") if isinstance(call.get("function"), dict) else {}
    raw_arguments = function.get("arguments") or call.get("arguments") or {}
    if isinstance(raw_arguments, dict):
        return dict(raw_arguments)
    if isinstance(raw_arguments, str) and raw_arguments.strip():
        try:
            parsed = json.loads(raw_arguments)
        except json.JSONDecodeError:
            return {"raw_arguments": raw_arguments}
        return dict(parsed) if isinstance(parsed, dict) else {"value": parsed}
    return {}


def _tool_call_name(call: dict[str, Any]) -> str:
    function = call.get("function") if isinstance(call.get("function"), dict) else {}
    return str(function.get("name") or call.get("name") or "").strip()


def _dispatch_tool_call(
    subsystems: Any | None,
    *,
    tool_name: str,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    if subsystems is None:
        return {
            "ok": False,
            "status": "permission_refused",
            "error": "tool_execution_loop requires gateway subsystems for tool dispatch",
        }
    from runtime.operation_catalog_gateway import execute_operation_from_subsystems

    operation_name = ""
    requested_mode = "query"
    payload = dict(arguments)
    if tool_name == "praxis_search":
        operation_name = "search.federated"
        payload.setdefault("query", "model eval authority")
        payload.setdefault("sources", ["code", "decisions", "knowledge", "bugs"])
    elif tool_name == "praxis_bugs":
        operation_name = "search.bugs"
        payload = {"query": str(arguments.get("query") or "model eval"), "limit": int(arguments.get("limit") or 10)}
    elif tool_name == "praxis_operator_decisions":
        operation_name = "operator.decision_list"
        payload.pop("action", None)
        payload.setdefault("active_only", True)
    elif tool_name == "praxis_model_eval":
        action = str(arguments.get("action") or "plan").strip().lower()
        operation_name = {
            "plan": "model_eval_plan",
            "inspect": "model_eval_inspect",
            "compare": "model_eval_compare",
            "export": "model_eval_export",
        }.get(action, "")
        if not operation_name:
            return {
                "ok": False,
                "status": "permission_refused",
                "error": f"model eval tool action {action!r} is not admitted inside tool loop",
            }
        payload.pop("action", None)
    else:
        return {
            "ok": False,
            "status": "permission_refused",
            "error": f"tool {tool_name!r} has no admitted Model Eval dispatcher",
        }
    try:
        return execute_operation_from_subsystems(
            subsystems,
            operation_name=operation_name,
            payload=payload,
            requested_mode=requested_mode,
        )
    except Exception as exc:  # noqa: BLE001 - failure is eval evidence.
        return {
            "ok": False,
            "status": "tool_error",
            "operation_name": operation_name,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _truncate_tool_content(value: Any, *, limit: int = 6000) -> str:
    text = json.dumps(value, sort_keys=True, default=str)
    if len(text) <= limit:
        return text
    return text[:limit] + "...[truncated]"


def _write_artifacts(run_dir: Path, payload: dict[str, Any]) -> list[str]:
    written: list[str] = []
    for artifact in payload.get("artifacts") or []:
        if not isinstance(artifact, dict):
            continue
        path = str(artifact.get("path") or "").strip()
        if not path:
            continue
        target = _safe_artifact_path(run_dir, path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(str(artifact.get("content") or ""), encoding="utf-8")
        written.append(str(target))
    return written


def _content_addressed_artifact(content: str, *, media_type: str, logical_path: str) -> dict[str, Any]:
    data = content.encode("utf-8")
    content_hash = hashlib.sha256(data).hexdigest()
    target = _repo_root() / "artifacts" / "model_eval" / content_hash[:2] / content_hash
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        target.write_bytes(data)
    return {
        "artifact_kind": "model_eval.emitted_file",
        "path": str(target),
        "logical_path": logical_path,
        "media_type": media_type,
        "content_hash": content_hash,
        "bytes": len(data),
    }


def _artifact_refs(payload: dict[str, Any]) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    for artifact in payload.get("artifacts") or []:
        if not isinstance(artifact, dict):
            continue
        refs.append(
            _content_addressed_artifact(
                str(artifact.get("content") or ""),
                media_type=str(artifact.get("media_type") or "text/plain"),
                logical_path=str(artifact.get("path") or ""),
            )
        )
    return refs


def _content_addressed_json(value: Any, *, artifact_kind: str, logical_path: str) -> dict[str, Any]:
    encoded = json.dumps(value, indent=2, sort_keys=True, default=str) + "\n"
    ref = _content_addressed_artifact(
        encoded,
        media_type="application/json",
        logical_path=logical_path,
    )
    ref["artifact_kind"] = artifact_kind
    return ref


def _run_mode(task: dict[str, Any]) -> str:
    mode = str(task.get("run_mode") or "").strip()
    if mode:
        return mode
    if task.get("tools"):
        return "tool_choice_static"
    family = str(task.get("family") or "")
    if family == "swarm_coordination":
        return "swarm"
    if family == "imported_workflow":
        return "workflow_import"
    return "structured_output"


def _seed(config_id: str, task_id: str, variant_id: str) -> int:
    material = f"{config_id}\0{task_id}\0{variant_id}"
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    return int(digest[:16], 16) % 2_000_000_000


def _run_tool_execution_loop(
    *,
    task: dict[str, Any],
    model_config: dict[str, Any],
    prompt_variant: dict[str, Any],
    manifest: dict[str, Any],
    run_dir: Path,
    system_prompt: str,
    provider_order: list[str],
    timeout_seconds: int,
    dry_run: bool,
    supports_seed: bool,
    subsystems: Any | None,
) -> dict[str, Any]:
    config_id = str(manifest.get("config_id") or "")
    task_id = str(manifest.get("task_id") or "")
    variant_id = str(manifest.get("prompt_variant_id") or "default")
    tools = task.get("tools") if isinstance(task.get("tools"), list) else []
    transcript: dict[str, Any] = {
        "task_id": task_id,
        "run_mode": "tool_execution_loop",
        "steps": [],
    }
    try:
        request = build_lab_request(
            model_slug=str(model_config["model_slug"]),
            provider_order=provider_order,
            system_prompt=system_prompt,
            user_prompt=str(task.get("prompt") or ""),
            max_tokens=int(task.get("max_tokens") or 3000),
            temperature=model_config.get("temperature"),
            reasoning_effort=model_config.get("reasoning_effort"),
            tools=tools,
            seed=_seed(config_id, task_id, variant_id) if supports_seed else None,
        )
    except OpenRouterError as exc:
        manifest.update({"ok": False, "status": "privacy_rejected", "error": str(exc), "cost": 0.0})
        _write_json(run_dir / "_manifest.json", manifest)
        return manifest
    messages = list(request.get("messages") or [])
    manifest["request_hash"] = _stable_hash(request)
    manifest["request_artifact"] = _content_addressed_json(
        request,
        artifact_kind="model_eval.request",
        logical_path="_request.json",
    )
    manifest["request_preview"] = {
        "agent": manifest.get("agent"),
        "task_type": MODEL_EVAL_WORKER_TASK_TYPE,
        "model_eval_candidate_ref": manifest.get("model_eval_candidate_ref"),
        "model": request.get("model"),
        "provider": request.get("provider"),
        "has_tools": bool(request.get("tools")),
        "max_tokens": request.get("max_tokens") or request.get("max_completion_tokens"),
        "reasoning": request.get("reasoning"),
        "seed": request.get("seed"),
        "supports_seed": supports_seed,
        "max_steps": int(task.get("max_steps") or 8),
    }
    if dry_run:
        transcript["steps"].append({"kind": "planned", "tools": [_tool_call_name({"function": tool.get("function", {})}) for tool in tools if isinstance(tool, dict)]})
        manifest.update({"ok": True, "status": "planned", "cost": 0.0, "tool_transcript": transcript})
        _write_json(run_dir / "_manifest.json", manifest)
        return manifest

    started = time.perf_counter()
    total_cost = 0.0
    raw_responses: list[dict[str, Any]] = []
    final_payload: dict[str, Any] | None = None
    max_steps = max(1, min(int(task.get("max_steps") or 8), 12))
    for step_index in range(max_steps):
        step_request = dict(request)
        step_request["messages"] = messages
        raw = chat_completion(step_request, timeout_seconds=min(timeout_seconds, 30))
        raw_responses.append(raw)
        total_cost += float(((raw.get("usage") or {}).get("cost") or 0.0))
        message = _choice_message(raw)
        transcript["steps"].append(
            {
                "kind": "model_turn",
                "step": step_index + 1,
                "served_provider": raw.get("provider"),
                "served_model": raw.get("model"),
                "content_present": bool(message.get("content")),
                "tool_call_count": len(message.get("tool_calls") or []),
            }
        )
        if raw.get("ok") is False or raw.get("error"):
            manifest.update({"ok": False, "status": "api_error", "error": raw.get("error"), "cost": round(total_cost, 8)})
            break
        route_check = _served_provider_check(provider_order, raw.get("provider"))
        if not route_check.get("ok"):
            manifest.update({"ok": False, "status": "route_mismatch", "error": route_check.get("detail"), "route_check": route_check, "cost": round(total_cost, 8)})
            break
        tool_calls = message.get("tool_calls") or []
        if tool_calls:
            messages.append(message)
            for call in tool_calls:
                if not isinstance(call, dict):
                    continue
                tool_name = _tool_call_name(call)
                arguments = _tool_call_arguments(call)
                tool_call_id = str(call.get("id") or f"tool_call_{step_index + 1}")
                transcript["steps"].append(
                    {
                        "kind": "tool_call",
                        "step": step_index + 1,
                        "tool_name": tool_name,
                        "arguments": arguments,
                    }
                )
                tool_result = _dispatch_tool_call(
                    subsystems,
                    tool_name=tool_name,
                    arguments=arguments,
                )
                receipt = tool_result.get("operation_receipt") if isinstance(tool_result, dict) else None
                receipt_id = receipt.get("receipt_id") if isinstance(receipt, dict) else None
                transcript["steps"].append(
                    {
                        "kind": "tool_result",
                        "step": step_index + 1,
                        "tool_name": tool_name,
                        "ok": bool(tool_result.get("ok")) if isinstance(tool_result, dict) else False,
                        "receipt_id": receipt_id,
                        "operation_name": tool_result.get("operation_name") if isinstance(tool_result, dict) else None,
                        "error": tool_result.get("error") if isinstance(tool_result, dict) else None,
                    }
                )
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call_id,
                        "content": _truncate_tool_content(tool_result),
                    }
                )
            continue
        content = message.get("content")
        if isinstance(content, str) and content.strip():
            try:
                parsed = json.loads(content)
                final_payload = parsed if isinstance(parsed, dict) else None
            except json.JSONDecodeError:
                final_payload = None
        break
    else:
        manifest.update({"ok": False, "status": "timeout", "error": "tool execution loop reached max_steps"})

    transcript_artifact = {
        "path": "tool_transcript.json",
        "media_type": "application/json",
        "content": json.dumps(transcript, indent=2, sort_keys=True, default=str),
    }
    payload = final_payload if isinstance(final_payload, dict) else {
        "task_id": task_id,
        "answer": "tool execution loop transcript recorded",
        "artifacts": [],
    }
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, list):
        artifacts = []
    artifacts.append(transcript_artifact)
    payload["artifacts"] = artifacts
    manifest["duration_s"] = round(time.perf_counter() - started, 3)
    manifest["latency_ms"] = int(round(float(manifest["duration_s"]) * 1000))
    manifest["served_provider"] = raw_responses[-1].get("provider") if raw_responses else None
    manifest["served_model"] = raw_responses[-1].get("model") if raw_responses else None
    manifest["cost"] = round(total_cost, 8)
    manifest["raw_response_hash"] = _stable_hash(raw_responses)
    manifest["raw_response_artifact"] = _content_addressed_json(
        raw_responses,
        artifact_kind="model_eval.raw_response",
        logical_path="_raw.json",
    )
    _write_json(run_dir / "_raw.json", {"responses": raw_responses, "transcript": transcript})
    _write_json(run_dir / "_payload.json", payload)
    manifest["payload_hash"] = _stable_hash(payload)
    manifest["payload_artifact"] = _content_addressed_json(
        payload,
        artifact_kind="model_eval.payload",
        logical_path="_payload.json",
    )
    if "status" not in manifest:
        try:
            written = _write_artifacts(run_dir, payload)
            artifact_refs = _artifact_refs(payload)
        except ValueError as exc:
            manifest.update({"ok": False, "status": "artifact_error", "error": str(exc)})
        else:
            verification = validate_task_output(task, payload)
            manifest.update(
                {
                    "ok": bool(verification.get("ok")),
                    "status": "verified" if verification.get("ok") else "verification_failed",
                    "score": verification.get("score"),
                    "verification": verification,
                    "artifact_paths": written,
                    "artifact_refs": artifact_refs,
                    "tool_transcript": transcript,
                }
            )
    _write_json(run_dir / "_manifest.json", manifest)
    return manifest


def _run_one(
    *,
    task: dict[str, Any],
    model_config: dict[str, Any],
    prompt_variant: dict[str, Any],
    output_root: Path,
    timeout_seconds: int,
    dry_run: bool,
    subsystems: Any | None = None,
) -> dict[str, Any]:
    config_id = str(model_config.get("config_id") or model_config.get("model_slug") or "model")
    task_id = str(task.get("task_id") or "task")
    variant_id = str(prompt_variant.get("prompt_variant_id") or "default")
    run_id = f"{config_id}__{task_id}__{variant_id}".replace("/", "_").replace(":", "_")
    run_dir = output_root / run_id
    system_prompt = BASE_SYSTEM_PROMPT
    suffix = str(prompt_variant.get("system_suffix") or "").strip()
    if suffix:
        system_prompt = f"{system_prompt} {suffix}"
    provider_order = [
        str(item).strip()
        for item in (model_config.get("provider_order") or [])
        if str(item).strip()
    ]
    pinned_agent: str | None = None
    candidate_ref = pinned_candidate_ref_from_model_config(model_config)
    manifest: dict[str, Any] = {
        "run_id": run_id,
        "task_id": task_id,
        "task_family": task.get("family"),
        "suite_slug": task.get("suite_slug"),
        "task_type": MODEL_EVAL_WORKER_TASK_TYPE,
        "config_id": config_id,
        "model_slug": model_config.get("model_slug"),
        "agent": model_config.get("agent") or model_config.get("agent_slug"),
        "model_eval_candidate_ref": candidate_ref,
        "provider_order": provider_order,
        "prompt_variant_id": variant_id,
        "run_mode": _run_mode(task),
        "prompt_hash": _hash_text(str(task.get("prompt") or "")),
        "fixture_hash": _stable_hash(
            {
                "task_id": task_id,
                "prompt": task.get("prompt"),
                "tools": task.get("tools"),
                "source_workflow": task.get("source_workflow"),
            }
        ),
        "catalog_version_hash": catalog_version_hash(),
        "dry_run": dry_run,
    }
    try:
        pinned_agent = validate_model_eval_model_config(model_config)
    except PinnedModelEvalRouteError as exc:
        status = "privacy_rejected" if "blocked" in str(exc).lower() else "permission_refused"
        manifest.update(
            {
                "ok": False,
                "status": status,
                "error": str(exc),
                "cost": 0.0,
            }
        )
        _write_json(run_dir / "_manifest.json", manifest)
        return manifest
    manifest["agent"] = pinned_agent
    supports_seed = bool(model_config.get("supports_seed", True))
    if _run_mode(task) == "tool_execution_loop":
        return _run_tool_execution_loop(
            task=task,
            model_config=model_config,
            prompt_variant=prompt_variant,
            manifest=manifest,
            run_dir=run_dir,
            system_prompt=system_prompt,
            provider_order=provider_order,
            timeout_seconds=timeout_seconds,
            dry_run=dry_run,
            supports_seed=supports_seed,
            subsystems=subsystems,
        )
    try:
        request = build_lab_request(
            model_slug=str(model_config["model_slug"]),
            provider_order=provider_order,
            system_prompt=system_prompt,
            user_prompt=str(task.get("prompt") or ""),
            max_tokens=int(task.get("max_tokens") or 3000),
            temperature=model_config.get("temperature"),
            reasoning_effort=model_config.get("reasoning_effort"),
            tools=task.get("tools") if isinstance(task.get("tools"), list) else None,
            seed=_seed(config_id, task_id, variant_id) if supports_seed else None,
        )
    except OpenRouterError as exc:
        manifest.update({"ok": False, "status": "privacy_rejected", "error": str(exc), "cost": 0.0})
        _write_json(run_dir / "_manifest.json", manifest)
        return manifest
    manifest["request_hash"] = _stable_hash(request)
    manifest["request_artifact"] = _content_addressed_json(
        request,
        artifact_kind="model_eval.request",
        logical_path="_request.json",
    )
    manifest["request_preview"] = {
        "agent": pinned_agent,
        "task_type": MODEL_EVAL_WORKER_TASK_TYPE,
        "model_eval_candidate_ref": candidate_ref,
        "model": request.get("model"),
        "provider": request.get("provider"),
        "has_tools": bool(request.get("tools")),
        "max_tokens": request.get("max_tokens") or request.get("max_completion_tokens"),
        "reasoning": request.get("reasoning"),
        "seed": request.get("seed"),
        "response_format": bool(request.get("response_format")),
        "supports_seed": supports_seed,
    }
    if dry_run:
        manifest.update({"ok": True, "status": "planned", "cost": 0.0})
        _write_json(run_dir / "_manifest.json", manifest)
        return manifest

    started = time.perf_counter()
    raw = chat_completion(request, timeout_seconds=timeout_seconds)
    manifest["duration_s"] = round(time.perf_counter() - started, 3)
    manifest["latency_ms"] = int(round(float(manifest["duration_s"]) * 1000))
    manifest["served_provider"] = raw.get("provider")
    manifest["served_model"] = raw.get("model")
    manifest["usage"] = raw.get("usage")
    manifest["cost"] = (raw.get("usage") or {}).get("cost")
    manifest["raw_response_hash"] = _stable_hash(raw)
    manifest["raw_response_artifact"] = _content_addressed_json(
        raw,
        artifact_kind="model_eval.raw_response",
        logical_path="_raw.json",
    )
    if raw.get("ok") is False or raw.get("error"):
        manifest.update({"ok": False, "status": "api_error", "error": raw.get("error")})
        _write_json(run_dir / "_raw.json", raw)
        _write_json(run_dir / "_manifest.json", manifest)
        return manifest
    route_check = _served_provider_check(provider_order, raw.get("provider"))
    manifest["route_check"] = route_check
    if not route_check.get("ok"):
        manifest.update({"ok": False, "status": "route_mismatch", "error": route_check.get("detail")})
        _write_json(run_dir / "_raw.json", raw)
        _write_json(run_dir / "_manifest.json", manifest)
        return manifest

    payload, parse_error = _parse_message(raw, tool_task=bool(task.get("tools")))
    _write_json(run_dir / "_raw.json", raw)
    _write_json(run_dir / "_payload.json", payload)
    manifest["payload_hash"] = _stable_hash(payload)
    manifest["payload_artifact"] = _content_addressed_json(
        payload,
        artifact_kind="model_eval.payload",
        logical_path="_payload.json",
    )
    if parse_error:
        manifest.update({"ok": False, "status": "parse_error", "error": parse_error})
        _write_json(run_dir / "_manifest.json", manifest)
        return manifest

    try:
        written = _write_artifacts(run_dir, payload)
        artifact_refs = _artifact_refs(payload)
    except ValueError as exc:
        manifest.update({"ok": False, "status": "artifact_error", "error": str(exc)})
        _write_json(run_dir / "_manifest.json", manifest)
        return manifest
    try:
        verification = validate_task_output(task, payload)
    except Exception as exc:
        verification = {
            "ok": False,
            "score": 0.0,
            "checks": [
                {
                    "ok": False,
                    "check": "validator exception",
                    "detail": f"{type(exc).__name__}: {exc}",
                }
            ],
        }
    manifest.update(
        {
            "ok": bool(verification.get("ok")),
            "status": "verified" if verification.get("ok") else "verification_failed",
            "score": verification.get("score"),
            "verification": verification,
            "artifact_paths": written,
            "artifact_refs": artifact_refs,
        }
    )
    _write_json(run_dir / "_manifest.json", manifest)
    return manifest


def run_model_eval_case(
    *,
    task: dict[str, Any],
    model_config: dict[str, Any],
    prompt_variant: dict[str, Any],
    output_root: str | Path,
    timeout_seconds: int,
    dry_run: bool,
    trial_number: int = 1,
    subsystems: Any | None = None,
) -> dict[str, Any]:
    result = _run_one(
        task=task,
        model_config=model_config,
        prompt_variant=prompt_variant,
        output_root=Path(output_root),
        timeout_seconds=timeout_seconds,
        dry_run=dry_run,
        subsystems=subsystems,
    )
    result["trial_number"] = trial_number
    return result


def _result_tokens(result: dict[str, Any]) -> tuple[int | None, int | None]:
    usage = result.get("usage") if isinstance(result.get("usage"), dict) else {}
    input_tokens = usage.get("prompt_tokens")
    output_tokens = usage.get("completion_tokens")
    return (
        int(input_tokens) if isinstance(input_tokens, int) else None,
        int(output_tokens) if isinstance(output_tokens, int) else None,
    )


def _persist_case_run(
    subsystems: Any,
    *,
    matrix_receipt_id: str,
    result: dict[str, Any],
    task: dict[str, Any],
    model_config: dict[str, Any],
    prompt_variant: dict[str, Any],
    child_receipt_id: str | None = None,
) -> None:
    if not matrix_receipt_id:
        return
    get_pg_conn = getattr(subsystems, "get_pg_conn", None)
    if not callable(get_pg_conn):
        return
    input_tokens, output_tokens = _result_tokens(result)
    provider_order = [
        str(item).strip()
        for item in (model_config.get("provider_order") or result.get("provider_order") or [])
        if str(item).strip()
    ]
    checks = ((result.get("verification") or {}).get("checks") or []) if isinstance(result.get("verification"), dict) else []
    status = str(result.get("status") or ("verified" if result.get("ok") else "verification_failed"))
    if status == "planned":
        return
    case_run_id = str(result.get("case_run_id") or uuid.uuid4())
    result["case_run_id"] = case_run_id
    artifact_refs = []
    for key in ("request_artifact", "raw_response_artifact", "payload_artifact"):
        value = result.get(key)
        if isinstance(value, dict):
            artifact_refs.append(value)
    artifact_refs.extend(ref for ref in (result.get("artifact_refs") or []) if isinstance(ref, dict))
    try:
        conn = get_pg_conn()
        _persist_compile_artifacts(conn, result)
        conn.execute(
            """
            INSERT INTO model_eval_case_runs (
                case_run_id,
                matrix_receipt_id,
                task_id,
                suite_slug,
                family,
                config_id,
                prompt_variant_id,
                model_config_json,
                prompt_variant_json,
                prompt_hash,
                fixture_hash,
                provider_requested,
                provider_served,
                model_served,
                status,
                score,
                checks_json,
                input_tokens,
                output_tokens,
                cost_usd,
                latency_ms,
                artifact_refs_json,
                raw_response_hash,
                child_receipt_id,
                catalog_version_hash,
                trial_number
            ) VALUES (
                $1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb,
                $10, $11, $12, $13, $14, $15, $16, $17::jsonb, $18, $19,
                $20, $21, $22::jsonb, $23, $24::uuid, $25, $26
            )
            ON CONFLICT (
                matrix_receipt_id,
                task_id,
                config_id,
                prompt_variant_id,
                trial_number
            ) DO UPDATE SET
                provider_served = EXCLUDED.provider_served,
                model_served = EXCLUDED.model_served,
                status = EXCLUDED.status,
                score = EXCLUDED.score,
                checks_json = EXCLUDED.checks_json,
                input_tokens = EXCLUDED.input_tokens,
                output_tokens = EXCLUDED.output_tokens,
                cost_usd = EXCLUDED.cost_usd,
                latency_ms = EXCLUDED.latency_ms,
                artifact_refs_json = EXCLUDED.artifact_refs_json,
                raw_response_hash = EXCLUDED.raw_response_hash,
                child_receipt_id = COALESCE(EXCLUDED.child_receipt_id, model_eval_case_runs.child_receipt_id)
            """,
            case_run_id,
            matrix_receipt_id,
            str(result.get("task_id") or task.get("task_id") or ""),
            str(result.get("suite_slug") or task.get("suite_slug") or ""),
            str(result.get("task_family") or task.get("family") or ""),
            str(model_config.get("config_id") or result.get("config_id") or ""),
            str(prompt_variant.get("prompt_variant_id") or result.get("prompt_variant_id") or ""),
            json.dumps(model_config, sort_keys=True, default=str),
            json.dumps(prompt_variant, sort_keys=True, default=str),
            str(result.get("prompt_hash") or _hash_text(str(task.get("prompt") or ""))),
            str(result.get("fixture_hash") or _stable_hash(task)),
            _provider_requested(provider_order),
            result.get("served_provider"),
            result.get("served_model"),
            status,
            result.get("score"),
            json.dumps(checks, sort_keys=True, default=str),
            input_tokens,
            output_tokens,
            result.get("cost"),
            result.get("latency_ms"),
            json.dumps(artifact_refs, sort_keys=True, default=str),
            result.get("raw_response_hash"),
            child_receipt_id,
            str(result.get("catalog_version_hash") or catalog_version_hash()),
            int(result.get("trial_number") or 1),
        )
    except Exception as exc:  # noqa: BLE001 - DB table may not exist in fresh-clone fallback.
        result.setdefault("persistence_warnings", []).append(
            f"model_eval_case_runs write skipped: {type(exc).__name__}: {exc}"
        )


def _persist_compile_artifacts(conn: Any, result: dict[str, Any]) -> None:
    try:
        from storage.postgres.compile_artifact_repository import PostgresCompileArtifactRepository
    except Exception:
        return
    repo = PostgresCompileArtifactRepository(conn)
    refs: list[dict[str, Any]] = []
    for key in ("request_artifact", "raw_response_artifact", "payload_artifact"):
        value = result.get(key)
        if isinstance(value, dict):
            refs.append(value)
    refs.extend(ref for ref in (result.get("artifact_refs") or []) if isinstance(ref, dict))
    for ref in refs:
        content_hash = str(ref.get("content_hash") or "")
        artifact_kind = str(ref.get("artifact_kind") or "model_eval.emitted_file")
        if not content_hash:
            continue
        artifact_ref = f"{artifact_kind}.{content_hash}"
        try:
            repo.upsert_compile_artifact(
                compile_artifact_id=f"compile_artifact.{artifact_kind}.{content_hash}",
                artifact_kind=artifact_kind,
                artifact_ref=artifact_ref,
                revision_ref=artifact_ref,
                parent_artifact_ref=None,
                input_fingerprint=content_hash,
                content_hash=content_hash,
                authority_refs=["authority.model_eval"],
                payload={
                    "path": ref.get("path"),
                    "logical_path": ref.get("logical_path"),
                    "media_type": ref.get("media_type"),
                    "bytes": ref.get("bytes"),
                    "case_run_id": result.get("case_run_id"),
                },
                decision_ref="operator_decision.architecture_policy.model_eval.model_eval_authority_supersedes_scratch_lab",
            )
        except Exception as exc:  # noqa: BLE001 - migration may not have widened artifact_kind yet.
            result.setdefault("persistence_warnings", []).append(
                f"compile_artifacts write skipped: {type(exc).__name__}: {exc}"
            )


def _can_fallback_to_inline_case_run(exc: Exception) -> bool:
    text = f"{type(exc).__name__}: {exc}"
    return "model_eval_run_case" in text and (
        "Operation not found" in text or "OperationCatalogBoundaryError" in text
    )
def persist_model_eval_case_run(
    subsystems: Any,
    *,
    matrix_receipt_id: str,
    result: dict[str, Any],
    task: dict[str, Any],
    model_config: dict[str, Any],
    prompt_variant: dict[str, Any],
    child_receipt_id: str | None = None,
) -> None:
    _persist_case_run(
        subsystems,
        matrix_receipt_id=matrix_receipt_id,
        result=result,
        task=task,
        model_config=model_config,
        prompt_variant=prompt_variant,
        child_receipt_id=child_receipt_id,
    )


def _persist_scorecards(subsystems: Any, *, matrix_receipt_id: str, results: list[dict[str, Any]]) -> None:
    if not matrix_receipt_id:
        return
    get_pg_conn = getattr(subsystems, "get_pg_conn", None)
    if not callable(get_pg_conn):
        return
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for result in results:
        config_id = str(result.get("config_id") or "")
        model_slug = str(result.get("model_slug") or "")
        family = str(result.get("task_family") or "")
        if not config_id or not family:
            continue
        grouped.setdefault((config_id, model_slug, family), []).append(result)
    try:
        conn = get_pg_conn()
        for (config_id, model_slug, family), rows in grouped.items():
            scores = [float(row.get("score") or 0.0) for row in rows]
            costs = [float(row.get("cost") or 0.0) for row in rows if row.get("cost") is not None]
            latencies = [int(row.get("latency_ms") or 0) for row in rows if row.get("latency_ms") is not None]
            trials = len(rows)
            pass_count = sum(1 for row in rows if row.get("ok"))
            mean_score = sum(scores) / max(1, len(scores))
            variance = sum((score - mean_score) ** 2 for score in scores) / max(1, len(scores))
            failures: dict[str, int] = {}
            for row in rows:
                status = str(row.get("status") or "unknown")
                if row.get("ok"):
                    continue
                failures[status] = failures.get(status, 0) + 1
            conn.execute(
                """
                INSERT INTO model_eval_scorecards (
                    matrix_receipt_id,
                    config_id,
                    model_slug,
                    family,
                    trials,
                    pass_count,
                    pass_at_1,
                    mean_score,
                    score_variance,
                    mean_cost_usd,
                    mean_latency_ms,
                    failure_counts_json
                ) VALUES (
                    $1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb
                )
                ON CONFLICT (matrix_receipt_id, config_id, family) DO UPDATE SET
                    trials = EXCLUDED.trials,
                    pass_count = EXCLUDED.pass_count,
                    pass_at_1 = EXCLUDED.pass_at_1,
                    mean_score = EXCLUDED.mean_score,
                    score_variance = EXCLUDED.score_variance,
                    mean_cost_usd = EXCLUDED.mean_cost_usd,
                    mean_latency_ms = EXCLUDED.mean_latency_ms,
                    failure_counts_json = EXCLUDED.failure_counts_json
                """,
                matrix_receipt_id,
                config_id,
                model_slug,
                family,
                trials,
                pass_count,
                pass_count / max(1, trials),
                round(mean_score, 4),
                round(variance, 8),
                round(sum(costs) / len(costs), 8) if costs else None,
                int(round(sum(latencies) / len(latencies))) if latencies else None,
                json.dumps(failures, sort_keys=True),
            )
    except Exception:
        return


def run_model_eval_matrix(
    *,
    suite_slugs: list[str] | None = None,
    workflow_spec_paths: list[str] | None = None,
    model_configs: list[dict[str, Any]] | None = None,
    prompt_variants: list[dict[str, Any]] | None = None,
    budget_cap_usd: float = 5.0,
    max_runs: int = 30,
    max_workflow_jobs: int = 20,
    timeout_seconds: int = 90,
    dry_run: bool = False,
    run_label: str | None = None,
    trials_per_case: int = 1,
    run_mode: str | None = None,
    subsystems: Any | None = None,
) -> dict[str, Any]:
    plan = build_suite_plan(
        suite_slugs=suite_slugs,
        workflow_spec_paths=workflow_spec_paths,
        prompt_variants=prompt_variants,
        model_configs=model_configs,
        max_workflow_jobs=max_workflow_jobs,
        run_mode=run_mode,
    )
    lab_run_id = run_label or f"model-eval-{uuid.uuid4().hex[:12]}"
    output_root = _repo_root() / "scratch" / "model-eval" / lab_run_id
    output_root.mkdir(parents=True, exist_ok=True)

    tasks = list(plan["tasks"])
    configs = list(plan["model_configs"])
    variants = list(plan["prompt_variants"])
    results: list[dict[str, Any]] = []
    total_cost = 0.0
    stopped_reason = None
    matrix_receipt_id = None
    if subsystems is not None:
        try:
            from runtime.operation_catalog_gateway import current_caller_context

            context = current_caller_context()
            matrix_receipt_id = context.cause_receipt_id if context else None
        except Exception:
            matrix_receipt_id = None
    for task in tasks:
        for config in configs:
            for variant in variants:
                for trial_number in range(1, max(1, int(trials_per_case)) + 1):
                    if len(results) >= max_runs:
                        stopped_reason = "max_runs"
                        break
                    if total_cost >= budget_cap_usd:
                        stopped_reason = "budget_cap_usd"
                        break
                    if subsystems is not None:
                        from runtime.operation_catalog_gateway import execute_operation_from_subsystems

                        try:
                            result = execute_operation_from_subsystems(
                                subsystems,
                                operation_name="model_eval_run_case",
                                payload={
                                    "task": task,
                                    "model_config": config,
                                    "prompt_variant": variant,
                                    "output_root": str(output_root),
                                    "timeout_seconds": timeout_seconds,
                                    "dry_run": dry_run,
                                    "trial_number": trial_number,
                                    "matrix_receipt_id": matrix_receipt_id,
                                },
                                requested_mode="command",
                            )
                        except Exception as exc:
                            if not _can_fallback_to_inline_case_run(exc):
                                raise
                            result = _run_one(
                                task=task,
                                model_config=config,
                                prompt_variant=variant,
                                output_root=output_root,
                                timeout_seconds=timeout_seconds,
                                dry_run=dry_run,
                                subsystems=subsystems,
                            )
                            result["trial_number"] = trial_number
                            result.setdefault("persistence_warnings", []).append(
                                "model_eval_run_case operation missing; ran inline without child receipt"
                            )
                        else:
                            child_receipt = result.get("operation_receipt") if isinstance(result, dict) else None
                            child_receipt_id = child_receipt.get("receipt_id") if isinstance(child_receipt, dict) else None
                            if isinstance(result, dict) and matrix_receipt_id:
                                _persist_case_run(
                                    subsystems,
                                    matrix_receipt_id=matrix_receipt_id,
                                    result=result,
                                    task=task,
                                    model_config=config,
                                    prompt_variant=variant,
                                    child_receipt_id=child_receipt_id,
                                )
                    else:
                        result = _run_one(
                            task=task,
                            model_config=config,
                            prompt_variant=variant,
                            output_root=output_root,
                            timeout_seconds=timeout_seconds,
                            dry_run=dry_run,
                            subsystems=subsystems,
                        )
                        result["trial_number"] = trial_number
                    results.append(result)
                    try:
                        total_cost += float(result.get("cost") or 0.0)
                    except (TypeError, ValueError):
                        pass
                if stopped_reason:
                    break
            if stopped_reason:
                break
        if stopped_reason:
            break
    if subsystems is not None and matrix_receipt_id:
        _persist_scorecards(subsystems, matrix_receipt_id=matrix_receipt_id, results=results)

    passed = sum(1 for item in results if item.get("ok"))
    summary = {
        "ok": (
            not plan.get("import_errors")
            and not plan.get("model_config_errors")
            and (dry_run or passed == len(results))
        ),
        "lab_run_id": lab_run_id,
        "authority": "authority.model_eval",
        "artifact_root": str(output_root),
        "dry_run": dry_run,
        "budget_cap_usd": budget_cap_usd,
        "total_cost_usd": round(total_cost, 6),
        "planned_matrix_count": plan["matrix_count"],
        "executed_count": len(results),
        "passed_count": passed,
        "failed_count": len(results) - passed,
        "stopped_reason": stopped_reason,
        "results": results,
        "plan": {
            "task_count": plan["task_count"],
            "model_config_count": plan["model_config_count"],
            "prompt_variant_count": plan["prompt_variant_count"],
            "catalog_version_hash": plan.get("catalog_version_hash"),
            "import_errors": plan["import_errors"],
            "model_config_errors": plan.get("model_config_errors") or [],
            "consistency_contract": plan["consistency_contract"],
        },
    }
    _write_json(output_root / "_summary.json", summary)
    return summary


__all__ = ["persist_model_eval_case_run", "run_model_eval_case", "run_model_eval_matrix"]
