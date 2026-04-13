"""Run and validation handlers for the workflow HTTP API."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import traceback
import uuid
from typing import Any

from runtime.canonical_checkpoints import (
    AuthorityCheckpointBoundaryError,
    request_authority_checkpoint,
    resolve_authority_checkpoint,
)
from runtime.canonical_manifests import (
    ManifestRuntimeBoundaryError,
    generate_manifest,
    generate_manifest_quick,
    refine_manifest,
    save_manifest,
    save_manifest_as,
)
from runtime.helm_manifest import normalize_helm_bundle

from ._shared import (
    REPO_ROOT,
    WORKFLOW_ROOT,
    RouteEntry,
    _ClientError,
    _exact,
    _prefix,
    _prefix_suffix,
    _query_params,
    _read_json_body,
    _serialize,
)


def _workflow_spec_mod():
    import runtime.workflow_spec as spec_mod

    return spec_mod


def _load_manifest_payload(raw_manifest: Any) -> dict[str, Any]:
    if isinstance(raw_manifest, str):
        try:
            raw_manifest = json.loads(raw_manifest)
        except (json.JSONDecodeError, TypeError) as exc:
            raise _ClientError(f"manifest payload is not valid JSON: {exc}") from exc
    if isinstance(raw_manifest, dict):
        return dict(raw_manifest)
    return {}


def _normalize_manifest_record(
    *,
    manifest_id: str,
    name: str | None,
    description: str | None,
    manifest: Any,
) -> dict[str, Any]:
    return normalize_helm_bundle(
        _load_manifest_payload(manifest),
        manifest_id=manifest_id,
        name=name or manifest_id,
        description=description or "",
    )


def _extract_manifest_save_payload(body: Any) -> tuple[str, str, str, dict[str, Any]]:
    if not isinstance(body, dict):
        raise _ClientError("manifest save body must be a JSON object")

    raw_manifest = body.get("manifest") if isinstance(body.get("manifest"), dict) else body
    manifest = _load_manifest_payload(raw_manifest)
    manifest_id = str(body.get("id") or manifest.get("id") or "").strip()
    name = str(body.get("name") or manifest.get("name") or manifest.get("title") or manifest_id).strip()
    description = str(body.get("description") or manifest.get("description") or "").strip()

    if not manifest_id:
        raise _ClientError("id is required")
    if not name:
        raise _ClientError("name is required")

    normalized = normalize_helm_bundle(
        manifest,
        manifest_id=manifest_id,
        name=name,
        description=description,
    )
    return manifest_id, name, description, normalized


def _handle_workflow(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    spec_path = body.get("spec_path")
    if not spec_path:
        raise _ClientError("spec_path is required")
    dry_run = body.get("dry_run", False)

    spec_mod = _workflow_spec_mod()
    spec = spec_mod.WorkflowSpec.load(spec_path)

    from runtime.workflow_graph_compiler import spec_uses_graph_runtime

    if dry_run:
        from runtime.workflow.dry_run import dry_run_workflow

        result = dry_run_workflow(spec)
        return {
            "spec_name": result.spec_name,
            "total_jobs": result.total_jobs,
            "succeeded": result.succeeded,
            "failed": result.failed,
            "skipped": result.skipped,
            "blocked": result.blocked,
            "duration_seconds": result.duration_seconds,
            "receipts_written": list(result.receipts_written),
            "job_results": [
                {
                    "job_label": jr.job_label,
                    "agent_slug": jr.agent_slug,
                    "status": jr.status,
                    "exit_code": jr.exit_code,
                    "duration_seconds": jr.duration_seconds,
                    "verify_passed": jr.verify_passed,
                    "retry_count": jr.retry_count,
                }
                for jr in result.job_results
            ],
        }

    if spec_uses_graph_runtime(spec._raw):
        from runtime.workflow.unified import submit_workflow_inline

        result = submit_workflow_inline(subs.get_pg_conn(), spec._raw)
        return {
            "run_id": result["run_id"],
            "status": result["status"],
            "spec_name": result["spec_name"],
            "total_jobs": result["total_jobs"],
            "stream_url": f"/api/workflow-runs/{result['run_id']}/stream",
            "status_url": f"/api/workflow-runs/{result['run_id']}/status",
            "execution_mode": result.get("execution_mode"),
        }

    result = _submit_workflow_via_service_bus(
        subs,
        spec_path=spec_path,
        spec_name=getattr(spec, "name", spec_path),
        total_jobs=len(getattr(spec, "jobs", [])),
        requested_by_kind="http",
        requested_by_ref="workflow_run",
    )
    if result.get("error"):
        return result

    return {
        "run_id": result["run_id"],
        "status": "queued",
        "spec_name": result["spec_name"],
        "total_jobs": result["total_jobs"],
        "command_id": result["command_id"],
        "stream_url": f"/api/workflow-runs/{result['run_id']}/stream",
        "status_url": f"/api/workflow-runs/{result['run_id']}/status",
    }


def _submit_workflow_via_service_bus(
    subs: Any,
    *,
    spec_path: str,
    spec_name: str,
    total_jobs: int,
    requested_by_kind: str,
    requested_by_ref: str,
) -> dict[str, Any]:
    from runtime.control_commands import (
        render_workflow_submit_response,
        request_workflow_submit_command,
    )

    command = request_workflow_submit_command(
        subs.get_pg_conn(),
        requested_by_kind=requested_by_kind,
        requested_by_ref=requested_by_ref,
        spec_path=spec_path,
        repo_root=str(REPO_ROOT),
    )
    return render_workflow_submit_response(
        command,
        spec_name=spec_name,
        total_jobs=total_jobs,
    )


def _handle_validate(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    spec_path = body.get("spec_path")
    if not spec_path:
        raise _ClientError("spec_path is required")

    spec_mod = _workflow_spec_mod()
    try:
        spec = spec_mod.WorkflowSpec.load(spec_path)
        from runtime.workflow_validation import (
            _authority_error_result,
            validate_workflow_spec,
        )

        try:
            pg_conn = subs.get_pg_conn()
        except Exception as exc:
            return _authority_error_result(spec, f"{type(exc).__name__}: {exc}")
        return validate_workflow_spec(spec, pg_conn=pg_conn)
    except spec_mod.WorkflowSpecError as exc:
        return {"valid": False, "error": str(exc)}


def _handle_status(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    since_hours = body.get("since_hours", 24)
    ingester = subs.get_receipt_ingester()
    receipts = ingester.load_recent(since_hours=since_hours)
    pass_rate = ingester.compute_pass_rate(receipts)
    top_failures = ingester.top_failure_codes(receipts)

    return {
        "total_workflows": len(receipts),
        "pass_rate": round(pass_rate, 4),
        "top_failure_codes": top_failures,
        "since_hours": since_hours,
    }


def _handle_wave(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    action = body.get("action", "observe")
    orch = subs.get_wave_orchestrator()

    if action == "observe":
        state = orch.observe()
        return {
            "orch_id": state.orch_id,
            "current_wave": state.current_wave,
            "created_at": state.created_at.isoformat(),
            "waves": [
                {
                    "wave_id": w.wave_id,
                    "status": w.status.value,
                    "started_at": w.started_at.isoformat() if w.started_at else None,
                    "completed_at": (
                        w.completed_at.isoformat() if w.completed_at else None
                    ),
                    "jobs": [
                        {
                            "job_label": j.job_label,
                            "status": j.status,
                            "depends_on": list(j.depends_on),
                        }
                        for j in w.jobs
                    ],
                    "gate_verdict": _serialize(w.gate_verdict) if w.gate_verdict else None,
                }
                for w in state.waves
            ],
        }

    if action == "start":
        wave_id = str(body.get("wave_id", "") or "").strip()
        if wave_id == "wave_abc123":
            wave_id = ""
        if not wave_id:
            try:
                wave_id = orch.resolve_default_wave_id(action=action)
            except KeyError:
                wave_id = ""
        if not wave_id:
            raise _ClientError("wave_id is required for start because no default wave is available")
        try:
            wave_state = orch.start_wave(wave_id)
            return {
                "wave_id": wave_state.wave_id,
                "status": wave_state.status.value,
                "started": True,
            }
        except RuntimeError as exc:
            return {"error": str(exc)}

    if action == "next":
        wave_id = str(body.get("wave_id", "") or "").strip()
        if wave_id == "wave_abc123":
            wave_id = ""
        if not wave_id:
            try:
                wave_id = orch.resolve_default_wave_id(action=action)
            except KeyError:
                wave_id = ""
        if not wave_id:
            raise _ClientError("wave_id is required for next because no default wave is available")
        try:
            runnable = orch.next_runnable_jobs(wave_id)
            return {"wave_id": wave_id, "runnable_jobs": runnable}
        except KeyError:
            return {"error": f"Wave {wave_id} not found"}

    if action == "record":
        wave_id = str(body.get("wave_id", "") or "").strip()
        if wave_id == "wave_abc123":
            wave_id = ""
        if not wave_id:
            try:
                wave_id = orch.resolve_default_wave_id(action=action)
            except KeyError:
                wave_id = ""
        jobs_str = body.get("jobs", "")
        if not wave_id or not jobs_str:
            raise _ClientError(
                "wave_id and jobs (format: 'label:pass,label2:fail') are required"
            )
        results = []
        for entry in jobs_str.split(","):
            entry = entry.strip()
            if ":" not in entry:
                continue
            label, outcome = entry.split(":", 1)
            succeeded = outcome.strip().lower() in (
                "pass",
                "true",
                "succeeded",
                "ok",
                "1",
            )
            orch.record_job_result(wave_id, label.strip(), succeeded)
            results.append({"job_label": label.strip(), "succeeded": succeeded})
        return {"wave_id": wave_id, "recorded": results}

    raise _ClientError(f"Unknown wave action: {action}")


def _handle_manifest_generate(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    intent = body.get("intent", "")
    if not intent:
        raise _ClientError("intent is required")

    try:
        result = generate_manifest(
            subs.get_pg_conn(),
            matcher=subs.get_intent_matcher(),
            generator=subs.get_manifest_generator(),
            intent=intent,
        )
    except ManifestRuntimeBoundaryError as exc:
        raise _ClientError(str(exc)) from exc

    return {
        "manifest_id": result.manifest_id,
        "manifest": result.manifest,
        "version": result.version,
        "confidence": result.confidence,
        "explanation": result.explanation,
    }


def _handle_manifest_refine(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    manifest_id = body.get("manifest_id", "")
    feedback = body.get("feedback", "")
    if not manifest_id or not feedback:
        raise _ClientError("manifest_id and feedback are required")

    try:
        result = refine_manifest(
            subs.get_pg_conn(),
            generator=subs.get_manifest_generator(),
            manifest_id=manifest_id,
            instruction=feedback,
        )
    except ManifestRuntimeBoundaryError as exc:
        raise _ClientError(str(exc)) from exc

    manifest = normalize_helm_bundle(
        result.manifest,
        manifest_id=result.manifest_id,
        description=result.explanation,
    )

    return {
        "manifest_id": result.manifest_id,
        "manifest": manifest,
        "version": result.version,
        "confidence": result.confidence,
        "explanation": result.explanation,
    }


def _handle_manifest_get(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    manifest_id = body.get("manifest_id", "")
    if not manifest_id:
        raise _ClientError("manifest_id is required")

    generator = subs.get_manifest_generator()
    result = generator.get(manifest_id)
    if result is None:
        raise _ClientError(f"Manifest not found: {manifest_id}")

    manifest = normalize_helm_bundle(
        result.manifest,
        manifest_id=result.manifest_id,
        description=result.explanation,
    )
    return {
        "manifest_id": result.manifest_id,
        "manifest": manifest,
        "version": result.version,
        "confidence": result.confidence,
        "explanation": result.explanation,
    }


def _handle_manifest_generate_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
        intent = body.get("intent", "")
        if not intent:
            request._send_json(400, {"error": "intent is required"})
            return
        result = generate_manifest(
            request.subsystems.get_pg_conn(),
            matcher=request.subsystems.get_intent_matcher(),
            generator=request.subsystems.get_manifest_generator(),
            intent=intent,
        )

        request._send_json(
            200,
            {
                "manifest_id": result.manifest_id,
                "manifest": result.manifest,
                "version": result.version,
                "confidence": result.confidence,
                "explanation": result.explanation,
            },
        )
    except ManifestRuntimeBoundaryError as exc:
        request._send_json(exc.status_code, {"error": str(exc)})
    except Exception as exc:
        request._send_json(
            500,
            {"error": str(exc), "trace": traceback.format_exc()},
        )


def _handle_manifest_generate_quick_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
        intent = body.get("intent", "")
        if not intent:
            request._send_json(400, {"error": "intent is required"})
            return
        payload = generate_manifest_quick(
            request.subsystems.get_pg_conn(),
            matcher=request.subsystems.get_intent_matcher(),
            generator=request.subsystems.get_manifest_generator(),
            intent=intent,
            template_id=body.get("template_id") or None,
        )
        request._send_json(200, payload)
    except ManifestRuntimeBoundaryError as exc:
        request._send_json(exc.status_code, {"error": str(exc)})
    except Exception as exc:
        request._send_json(
            500,
            {"error": str(exc), "trace": traceback.format_exc()},
        )


def _handle_manifest_refine_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
        manifest_id = body.get("manifest_id", "")
        instruction = body.get("instruction", "")
        if not manifest_id or not instruction:
            request._send_json(
                400,
                {"error": "manifest_id and instruction are required"},
            )
            return
        result = refine_manifest(
            request.subsystems.get_pg_conn(),
            generator=request.subsystems.get_manifest_generator(),
            manifest_id=manifest_id,
            instruction=instruction,
        )
        manifest = normalize_helm_bundle(
            result.manifest,
            manifest_id=result.manifest_id,
            description=result.explanation,
        )
        request._send_json(
            200,
            {
                "manifest_id": result.manifest_id,
                "manifest": manifest,
                "version": result.version,
                "explanation": result.explanation,
                "changelog": result.changelog,
            },
        )
    except ManifestRuntimeBoundaryError as exc:
        request._send_json(exc.status_code, {"error": str(exc)})
    except Exception as exc:
        request._send_json(
            500,
            {"error": str(exc), "trace": traceback.format_exc()},
        )


def _handle_models_run_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
        if not isinstance(body, dict):
            request._send_json(400, {"error": "Request body must be a JSON object"})
            return
    except (json.JSONDecodeError, ValueError) as exc:
        request._send_json(400, {"error": f"Invalid JSON: {exc}"})
        return

    try:
        pg = request.subsystems.get_pg_conn()

        if path == "/api/models/run":
            model = body.get("model")
            if not isinstance(model, dict):
                request._send_json(400, {"error": "model is required"})
                return

            from runtime.model_executor import start_model_run

            result = start_model_run(pg, model)
            request._send_json(
                200,
                {
                    "run_id": result["run_id"],
                    "total_cards": result["total_cards"],
                    "ready_cards": result["ready_cards"],
                },
            )
            return

        parts = [part for part in path.split("/") if part]
        if (
            len(parts) != 6
            or parts[0] != "api"
            or parts[1] != "models"
            or parts[2] != "runs"
            or parts[4] != "approve"
        ):
            request._send_json(404, {"error": f"Not found: {path}"})
            return

        run_id = parts[3]
        card_id = parts[5]
        decision = body.get("decision", "")
        notes = body.get("notes", "")

        if not run_id:
            request._send_json(400, {"error": "run_id is required"})
            return
        if not card_id:
            request._send_json(400, {"error": "card_id is required"})
            return
        if decision not in {"approved", "rejected"}:
            request._send_json(
                400,
                {"error": "decision must be one of: approved, rejected"},
            )
            return
        if notes is not None and not isinstance(notes, str):
            request._send_json(400, {"error": "notes must be a string"})
            return

        from runtime.model_executor import approve_card

        result = approve_card(
            pg,
            run_id,
            card_id,
            decision,
            notes if isinstance(notes, str) else "",
        )
        request._send_json(
            200,
            {
                "status": result["status"],
                "released_cards": result["released_cards"],
            },
        )
    except Exception as exc:
        request._send_json(
            500,
            {"error": str(exc), "trace": traceback.format_exc()},
        )


def _handle_checkpoints_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
        if not isinstance(body, dict):
            request._send_json(400, {"error": "Request body must be a JSON object"})
            return
    except (json.JSONDecodeError, ValueError) as exc:
        request._send_json(400, {"error": f"Invalid JSON: {exc}"})
        return

    try:
        pg = request.subsystems.get_pg_conn()

        if path == "/api/checkpoints":
            row = request_authority_checkpoint(
                pg,
                card_id=body.get("card_id"),
                model_id=body.get("model_id"),
                authority_level=body.get("authority_level"),
                question=body.get("question"),
            )
            request._send_json(
                200,
                {
                    "checkpoint_id": row["checkpoint_id"],
                    "status": row["status"],
                },
            )
            return

        checkpoint_parts = [part for part in path.split("/") if part]
        if len(checkpoint_parts) != 4:
            request._send_json(400, {"error": "checkpoint_id is required"})
            return

        checkpoint_id = checkpoint_parts[2]
        row = resolve_authority_checkpoint(
            pg,
            checkpoint_id=checkpoint_id,
            decision=body.get("decision"),
            decided_by=body.get("decided_by"),
            notes=body.get("notes"),
        )
        request._send_json(200, _serialize(dict(row)))
    except AuthorityCheckpointBoundaryError as exc:
        request._send_json(exc.status_code, {"error": str(exc)})
    except Exception as exc:
        request._send_json(
            500,
            {"error": str(exc), "trace": traceback.format_exc()},
        )


def _handle_workflow_async_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
    except (json.JSONDecodeError, ValueError) as exc:
        request._send_json(400, {"error": f"Invalid JSON: {exc}"})
        return

    try:
        jobs = body.get("jobs", [])
        name = body.get("name", "browser-workflow")
        if not jobs:
            request._send_json(400, {"error": "jobs array is required"})
            return

        spec_obj = {
            "name": name,
            "workflow_id": body.get("workflow_id", f"ui-{uuid.uuid4().hex[:8]}"),
            "phase": body.get("phase", "interactive"),
            "outcome_goal": body.get("outcome_goal", ""),
            "jobs": jobs,
        }

        temp_dir = REPO_ROOT / "artifacts" / "workflow"
        temp_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".queue.json",
            dir=str(temp_dir),
            delete=False,
            prefix="ui_",
        ) as handle:
            json.dump(spec_obj, handle)
            spec_file = handle.name

        try:
            result = _submit_workflow_via_service_bus(
                request.subsystems,
                spec_path=os.path.relpath(spec_file, str(REPO_ROOT)),
                spec_name=str(spec_obj.get("name") or "browser-workflow"),
                total_jobs=len(jobs),
                requested_by_kind="http",
                requested_by_ref="workflow_browser_run",
            )
            if result.get("error"):
                request._send_json(500, result)
                return
            request._send_json(200, result)
        except Exception as exc:
            request._send_json(500, {"error": str(exc)})
        finally:
            os.unlink(spec_file)
    except Exception as exc:
        request._send_json(
            500,
            {"error": str(exc), "trace": traceback.format_exc()},
        )


def _handle_workflows_run_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
    except (json.JSONDecodeError, ValueError) as exc:
        request._send_json(400, {"error": f"Invalid JSON: {exc}"})
        return

    try:
        steps = body.get("steps", [])
        if not steps:
            request._send_json(400, {"error": "steps are required"})
            return

        workflow_run_id = "wf-" + uuid.uuid4().hex[:8]
        jobs = []
        for index, step in enumerate(steps):
            prompt = step.get("prompt", "")
            model = step.get("model", "auto/build")
            depends_on = step.get("depends_on", [])
            jobs.append(
                {
                    "label": f"step-{index}",
                    "agent": model,
                    "prompt": prompt,
                    "depends_on": depends_on,
                }
            )

        spec = {
            "name": f"workflow-{workflow_run_id}",
            "workflow_id": workflow_run_id,
            "phase": "interactive",
            "jobs": jobs,
        }

        temp_dir = REPO_ROOT / "artifacts" / "workflow"
        temp_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".queue.json", delete=False) as handle:
            json.dump(spec, handle)
            spec_path = handle.name

        try:
            result = _submit_workflow_via_service_bus(
                request.subsystems,
                spec_path=os.path.relpath(spec_path, str(REPO_ROOT)),
                spec_name=str(spec.get("name") or f"workflow-{workflow_run_id}"),
                total_jobs=len(jobs),
                requested_by_kind="http",
                requested_by_ref="workflow_run",
            )
            if result.get("error"):
                request._send_json(500, result)
                return
            request._send_json(
                200,
                {
                    "workflow_run_id": result["run_id"],
                    "status": result["status"],
                    "stream_url": f"/api/workflow-runs/{result['run_id']}/stream",
                    "status_url": f"/api/workflow-runs/{result['run_id']}/status",
                },
            )
        finally:
            os.unlink(spec_path)
    except Exception as exc:
        request._send_json(
            500,
            {"error": str(exc), "trace": traceback.format_exc()},
        )


def _handle_workflow_job_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
    except (json.JSONDecodeError, ValueError) as exc:
        request._send_json(400, {"error": f"Invalid JSON: {exc}"})
        return

    try:
        prompt = body.get("prompt", "")
        if not prompt:
            request._send_json(400, {"error": "prompt is required"})
            return

        model = body.get("model", "auto/build")
        job_id = uuid.uuid4().hex[:8]
        spec = {
            "name": f"ui-workflow-{job_id}",
            "workflow_id": f"ui-{job_id}",
            "phase": "interactive",
            "jobs": [
                {
                    "label": f"interactive-{job_id}",
                    "agent": model,
                    "prompt": prompt,
                }
            ],
        }

        temp_dir = REPO_ROOT / "artifacts" / "workflow"
        temp_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".queue.json", dir=str(temp_dir), delete=False) as handle:
            json.dump(spec, handle)
            spec_path = handle.name
        try:
            result = _submit_workflow_via_service_bus(
                request.subsystems,
                spec_path=os.path.relpath(spec_path, str(REPO_ROOT)),
                spec_name=str(spec.get("name") or f"ui-workflow-{job_id}"),
                total_jobs=1,
                requested_by_kind="http",
                requested_by_ref="workflow_job",
            )
            if result.get("error"):
                request._send_json(500, result)
                return
            request._send_json(
                200,
                {
                    "run_id": result["run_id"],
                    "status": result["status"],
                    "stream_url": f"/api/workflow-runs/{result['run_id']}/stream",
                    "status_url": f"/api/workflow-runs/{result['run_id']}/status",
                },
            )
        finally:
            os.unlink(spec_path)
    except Exception as exc:
        request._send_json(
            500,
            {"error": str(exc), "trace": traceback.format_exc()},
        )


def _handle_manifest_save_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
    except (json.JSONDecodeError, ValueError) as exc:
        request._send_json(400, {"error": f"Invalid JSON: {exc}"})
        return

    try:
        manifest_id, name, description, manifest = _extract_manifest_save_payload(body)
        saved = save_manifest(
            request.subsystems.get_pg_conn(),
            manifest_id=manifest_id,
            name=name,
            description=description,
            manifest=manifest,
        )
        request._send_json(
            200,
            {
                "saved": True,
                "id": saved["id"],
                "name": saved["name"],
                "description": saved["description"],
                "version": saved["version"],
                "manifest": saved["manifest"],
            },
        )
    except _ClientError as exc:
        request._send_json(400, {"error": str(exc)})
    except ManifestRuntimeBoundaryError as exc:
        request._send_json(exc.status_code, {"error": str(exc)})
    except Exception as exc:
        request._send_json(
            500,
            {"error": str(exc), "trace": traceback.format_exc()},
        )


def _handle_manifest_save_as_post(request: Any, path: str) -> None:
    try:
        body = _read_json_body(request)
    except (json.JSONDecodeError, ValueError) as exc:
        request._send_json(400, {"error": f"Invalid JSON: {exc}"})
        return

    try:
        name = body.get("name", "")
        if not name:
            request._send_json(400, {"error": "name is required"})
            return
        description = str(body.get("description") or "").strip()
        saved = save_manifest_as(
            request.subsystems.get_pg_conn(),
            name=name,
            description=description,
            manifest=_load_manifest_payload(body.get("manifest")),
        )
        request._send_json(
            200,
            {
                "saved": True,
                "id": saved["id"],
                "name": saved["name"],
                "description": saved["description"],
                "manifest": saved["manifest"],
            },
        )
    except ManifestRuntimeBoundaryError as exc:
        request._send_json(exc.status_code, {"error": str(exc)})
    except Exception as exc:
        request._send_json(
            500,
            {"error": str(exc), "trace": traceback.format_exc()},
        )


def _handle_checkpoints_get(request: Any, path: str) -> None:
    if path == "/api/checkpoints":
        try:
            params = _query_params(request.path)
            model_id = (params.get("model_id") or [""])[0].strip()
            if not model_id:
                request._send_json(400, {"error": "model_id query parameter is required"})
                return

            pg = request.subsystems.get_pg_conn()
            rows = pg.execute(
                "SELECT * FROM authority_checkpoints "
                "WHERE model_id = $1 ORDER BY created_at",
                model_id,
            )
            checkpoints = [_serialize(dict(row)) for row in rows]
            request._send_json(
                200,
                {
                    "checkpoints": checkpoints,
                    "count": len(checkpoints),
                    "model_id": model_id,
                },
            )
        except Exception as exc:
            request._send_json(500, {"error": str(exc)})
        return

    checkpoint_id = path.split("/api/checkpoints/")[-1]
    if checkpoint_id:
        try:
            pg = request.subsystems.get_pg_conn()
            row = pg.fetchrow(
                "SELECT * FROM authority_checkpoints WHERE checkpoint_id = $1",
                checkpoint_id,
            )
            if row is None:
                request._send_json(
                    404,
                    {"error": f"Checkpoint not found: {checkpoint_id}"},
                )
                return
            request._send_json(200, _serialize(dict(row)))
        except Exception as exc:
            request._send_json(500, {"error": str(exc)})


def _handle_manifest_get_api(request: Any, path: str) -> None:
    manifest_id = path.split("/api/manifests/")[-1]
    try:
        pg = request.subsystems.get_pg_conn()
        row = pg.fetchrow(
            "SELECT id, name, description, manifest FROM app_manifests WHERE id = $1",
            manifest_id,
        )
        if row is None:
            request._send_json(404, {"error": f"Manifest not found: {manifest_id}"})
            return
        manifest = _normalize_manifest_record(
            manifest_id=row["id"],
            name=row.get("name"),
            description=row.get("description"),
            manifest=row["manifest"],
        )
        request._send_json(200, manifest)
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_status_alias_get(request: Any, path: str) -> None:
    try:
        result = _handle_status(request.subsystems, {"since_hours": 24})
        request._send_json(200, result)
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_model_run_status_get(request: Any, path: str) -> None:
    try:
        parts = [part for part in path.split("/") if part]
        if (
            len(parts) != 5
            or parts[0] != "api"
            or parts[1] != "models"
            or parts[2] != "runs"
            or parts[4] != "status"
        ):
            request._send_json(404, {"error": f"Not found: {path}"})
            return

        run_id = parts[3]
        if not run_id:
            request._send_json(400, {"error": "run_id is required"})
            return

        from runtime.model_executor import get_run_status

        pg = request.subsystems.get_pg_conn()
        result = get_run_status(pg, run_id)
        request._send_json(200, result)
    except Exception as exc:
        request._send_json(
            500,
            {"error": str(exc), "trace": traceback.format_exc()},
        )


class _RunWakeupListener:
    """LISTEN on 'job_completed' pg_notify channel and wake a threading.Event.

    Runs on a daemon thread using a dedicated asyncpg connection so the
    SSE handler's iter_run() poll loop wakes immediately on each job
    completion instead of waiting the full poll_interval.
    """

    def __init__(self, database_url: str, wakeup_event: threading.Event) -> None:
        self._database_url = database_url
        self._wakeup_event = wakeup_event
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="sse-run-wakeup-listener"
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._wakeup_event.set()
        self._thread.join(timeout=3)

    def _on_notify(self, _conn, _pid, _channel: str, _payload: str) -> None:
        self._wakeup_event.set()

    def _run(self) -> None:
        import asyncio
        import asyncpg

        async def _listen() -> None:
            while not self._stop_event.is_set():
                conn = None
                try:
                    conn = await asyncpg.connect(self._database_url, timeout=5.0)
                    await conn.add_listener("job_completed", self._on_notify)
                    while not self._stop_event.is_set():
                        await asyncio.sleep(1.0)
                except Exception:
                    if not self._stop_event.is_set():
                        await asyncio.sleep(2.0)
                finally:
                    if conn is not None:
                        await conn.close()

        asyncio.run(_listen())


def _handle_workflow_stream(request: Any, path: str) -> None:
    """SSE endpoint: GET /api/workflow-runs/{run_id}/stream

    Streams per-job completion events as they arrive, then sends a
    final summary event and closes. Any UI can subscribe to this for
    real-time workflow progress (similar to a sub-agent activity feed).

    Events:
        event: job
        data: {"job_label": "...", "status": "succeeded", "agent_slug": "...", ...}

        event: progress
        data: {"completed": 3, "total": 5, "passed": 2, "failed": 1}

        event: done
        data: {"status": "succeeded", "passed": 4, "failed": 1, ...}
    """
    # Extract run_id from path: /api/workflow-runs/{run_id}/stream
    parts = path.split("/")
    # ['', 'api', 'workflow-runs', '{run_id}', 'stream']
    if len(parts) < 5:
        request._send_json(400, {"error": "Invalid path — expected /api/workflow-runs/{run_id}/stream"})
        return

    run_id = parts[3]

    try:
        pg = request.subsystems.get_pg_conn()
        if str(WORKFLOW_ROOT) not in sys.path:
            sys.path.insert(0, str(WORKFLOW_ROOT))

        from runtime.workflow.unified import get_run_status
        from runtime.workflow_notifications import WorkflowNotificationConsumer

        initial = get_run_status(pg, run_id)
        if initial is None:
            request._send_json(404, {"error": f"Run {run_id} not found"})
            return

        total_jobs = initial.get("total_jobs", 0)
        spec_name = initial.get("spec_name", "")

        # If already terminal, return final status as a single SSE event
        if initial["status"] in ("succeeded", "failed", "dead_letter", "cancelled"):
            request.send_response(200)
            request.send_header("Content-Type", "text/event-stream")
            request.send_header("Cache-Control", "no-cache")
            request.send_header("Connection", "keep-alive")
            request.send_header("Access-Control-Allow-Origin", "*")
            request.end_headers()

            jobs = initial.get("jobs", [])
            passed = sum(1 for j in jobs if j["status"] == "succeeded")
            failed = sum(1 for j in jobs if j["status"] in ("failed", "dead_letter"))
            done_data = json.dumps({
                "status": initial["status"], "spec_name": spec_name,
                "total_jobs": total_jobs, "passed": passed, "failed": failed,
            }, default=str)
            request.wfile.write(f"event: done\ndata: {done_data}\n\n".encode())
            request.wfile.flush()
            return

        # Stream SSE events as jobs complete
        request.send_response(200)
        request.send_header("Content-Type", "text/event-stream")
        request.send_header("Cache-Control", "no-cache")
        request.send_header("Connection", "keep-alive")
        request.send_header("Access-Control-Allow-Origin", "*")
        request.end_headers()

        # Send initial event
        start_data = json.dumps({
            "run_id": run_id, "spec_name": spec_name, "total_jobs": total_jobs,
        }, default=str)
        request.wfile.write(f"event: start\ndata: {start_data}\n\n".encode())
        request.wfile.flush()

        consumer = WorkflowNotificationConsumer(pg)
        passed = 0
        failed = 0
        count = 0

        wakeup = threading.Event()
        database_url = os.environ.get("WORKFLOW_DATABASE_URL", "")
        listener = _RunWakeupListener(database_url, wakeup) if database_url else None
        if listener is not None:
            listener.start()
        try:
            for notif in consumer.iter_run(run_id, total_jobs, timeout_seconds=None, wakeup_event=wakeup):
                count += 1
                succeeded = notif.status == "succeeded"
                if succeeded:
                    passed += 1
                else:
                    failed += 1

                # Per-job event
                job_data = json.dumps({
                    "job_label": notif.job_label,
                    "status": notif.status,
                    "agent_slug": notif.agent_slug,
                    "duration_seconds": round(notif.duration_seconds, 1),
                    "failure_code": notif.failure_code or None,
                    "cpu_percent": notif.cpu_percent,
                    "mem_bytes": notif.mem_bytes,
                }, default=str)
                request.wfile.write(f"event: job\ndata: {job_data}\n\n".encode())

                # Progress event
                progress_data = json.dumps({
                    "completed": count, "total": total_jobs,
                    "passed": passed, "failed": failed,
                })
                request.wfile.write(f"event: progress\ndata: {progress_data}\n\n".encode())
                request.wfile.flush()
        finally:
            if listener is not None:
                listener.stop()

        # Final summary
        final = get_run_status(pg, run_id)
        final_status = "timeout" if count < total_jobs else (final["status"] if final else "unknown")
        done_data = json.dumps({
            "status": final_status, "spec_name": spec_name,
            "total_jobs": total_jobs, "passed": passed, "failed": failed,
        }, default=str)
        request.wfile.write(f"event: done\ndata: {done_data}\n\n".encode())
        request.wfile.flush()

    except (BrokenPipeError, ConnectionResetError):
        pass  # Client disconnected — normal for SSE
    except Exception as exc:
        try:
            err_data = json.dumps({"error": str(exc)})
            request.wfile.write(f"event: error\ndata: {err_data}\n\n".encode())
            request.wfile.flush()
        except Exception:
            pass


def _handle_workflow_status(request: Any, path: str) -> None:
    """GET /api/workflow-runs/{run_id}/status"""
    parts = [part for part in path.split("/") if part]
    if (
        len(parts) != 4
        or parts[0] != "api"
        or parts[1] != "workflow-runs"
        or parts[3] != "status"
    ):
        request._send_json(404, {"error": f"Not found: {path}"})
        return

    run_id = parts[2]
    if not run_id:
        request._send_json(400, {"error": "run_id is required"})
        return

    try:
        from runtime.workflow.unified import get_run_status

        pg = request.subsystems.get_pg_conn()
        result = get_run_status(pg, run_id)
        if result is None:
            request._send_json(404, {"error": f"Run {run_id} not found"})
            return

        request._send_json(200, _serialize(result))
    except Exception as exc:
        request._send_json(
            500,
            {"error": str(exc), "trace": traceback.format_exc()},
        )


RUN_POST_ROUTES: list[RouteEntry] = [
    (_exact("/api/manifests/generate"), _handle_manifest_generate_post),
    (_exact("/api/manifests/generate-quick"), _handle_manifest_generate_quick_post),
    (_exact("/api/manifests/refine"), _handle_manifest_refine_post),
    (
        lambda candidate: candidate == "/api/models/run"
        or (candidate.startswith("/api/models/runs/") and "/approve/" in candidate),
        _handle_models_run_post,
    ),
    (
        lambda candidate: candidate == "/api/checkpoints"
        or (
            candidate.startswith("/api/checkpoints/")
            and candidate.endswith("/approve")
        ),
        _handle_checkpoints_post,
    ),
    (_exact("/api/workflow-runs"), _handle_workflow_async_post),
    (_exact("/api/workflows/run"), _handle_workflows_run_post),
    (_exact("/api/workflow-job"), _handle_workflow_job_post),
    (_exact("/api/manifests/save"), _handle_manifest_save_post),
    (_exact("/api/manifests/save-as"), _handle_manifest_save_as_post),
]

RUN_GET_ROUTES: list[RouteEntry] = [
    (_prefix_suffix("/api/workflow-runs/", "/stream"), _handle_workflow_stream),
    (_prefix_suffix("/api/workflow-runs/", "/status"), _handle_workflow_status),
    (_exact("/api/checkpoints"), _handle_checkpoints_get),
    (_prefix("/api/checkpoints/"), _handle_checkpoints_get),
    (_prefix("/api/manifests/"), _handle_manifest_get_api),
    (_exact("/api/workflow-status"), _handle_status_alias_get),
    (_exact("/api/status"), _handle_status_alias_get),
    (_prefix_suffix("/api/models/runs/", "/status"), _handle_model_run_status_get),
]

RUN_ROUTES: dict[str, object] = {
    "/workflow-runs": _handle_workflow,
    "/workflow-validate": _handle_validate,
    "/status": _handle_status,
    "/wave": _handle_wave,
    "/manifest/generate": _handle_manifest_generate,
    "/manifest/refine": _handle_manifest_refine,
    "/manifest/get": _handle_manifest_get,
}


__all__ = [
    "RUN_GET_ROUTES",
    "RUN_POST_ROUTES",
    "RUN_ROUTES",
    "_handle_status",
]
