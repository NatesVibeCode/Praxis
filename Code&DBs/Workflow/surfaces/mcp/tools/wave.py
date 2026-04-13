"""Tools: praxis_wave."""
from __future__ import annotations

from typing import Any

from ..subsystems import _subs
from ..helpers import _serialize


_PLACEHOLDER_WAVE_IDS = frozenset({"wave_abc123"})


def _resolve_wave_id(orch, params: dict, *, action: str) -> tuple[str, str | None]:
    requested = str(params.get("wave_id", "") or "").strip()
    if requested and requested not in _PLACEHOLDER_WAVE_IDS:
        return requested, None
    try:
        resolved = orch.resolve_default_wave_id(action=action)
    except KeyError:
        return "", "wave_id is required because there is no single obvious default wave"
    if requested in _PLACEHOLDER_WAVE_IDS:
        return resolved, f"{requested} is a placeholder; using {resolved} instead"
    return resolved, f"wave_id omitted; using {resolved}"


def tool_praxis_wave(params: dict) -> dict:
    """Wave orchestration operations: observe, start, next, record."""
    action = params.get("action", "observe")
    orch = _subs.get_wave_orchestrator()

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
                    "completed_at": w.completed_at.isoformat() if w.completed_at else None,
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
        wave_id, note = _resolve_wave_id(orch, params, action=action)
        if not wave_id:
            return {"error": note or "wave_id is required for start"}
        try:
            ws = orch.start_wave(wave_id)
            payload = {"wave_id": ws.wave_id, "status": ws.status.value, "started": True}
            if note:
                payload["note"] = note
            return payload
        except RuntimeError as e:
            return {"error": str(e)}

    if action == "next":
        wave_id, note = _resolve_wave_id(orch, params, action=action)
        if not wave_id:
            return {"error": note or "wave_id is required for next"}
        try:
            runnable = orch.next_runnable_jobs(wave_id)
            payload = {"wave_id": wave_id, "runnable_jobs": runnable}
            if note:
                payload["note"] = note
            return payload
        except KeyError:
            return {"error": f"Wave {wave_id} not found"}

    if action == "record":
        wave_id, note = _resolve_wave_id(orch, params, action=action)
        jobs_str = params.get("jobs", "")
        if not wave_id or not jobs_str:
            return {
                "error": (
                    note
                    or "wave_id and jobs (format: 'label:pass,label2:fail') are required for record"
                )
            }
        results = []
        for entry in jobs_str.split(","):
            entry = entry.strip()
            if ":" not in entry:
                continue
            label, outcome = entry.split(":", 1)
            succeeded = outcome.strip().lower() in ("pass", "true", "succeeded", "ok", "1")
            orch.record_job_result(wave_id, label.strip(), succeeded)
            results.append({"job_label": label.strip(), "succeeded": succeeded})
        payload = {"wave_id": wave_id, "recorded": results}
        if note:
            payload["note"] = note
        return payload

    return {"error": f"Unknown wave action: {action}"}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_wave": (
        tool_praxis_wave,
        {
            "description": (
                "Manage execution waves — groups of jobs with dependency ordering. "
                "Waves track which jobs are runnable (all dependencies met) and which are blocked.\n\n"
                "USE WHEN: you're orchestrating multi-phase work where later jobs depend on earlier "
                "ones completing successfully.\n\n"
                "WORKFLOW:\n"
                "  1. praxis_wave(action='observe')                    — see current wave state\n"
                "  2. praxis_wave(action='start', wave_id='wave_1')    — begin a new wave\n"
                "  3. praxis_wave(action='next')                       — get jobs ready on the current/only wave\n"
                "  4. praxis_wave(action='record', jobs='build:pass,test:fail')  — record results on the current/only wave\n\n"
                "DO NOT USE: for simple flat workflow launches (use praxis_workflow). Waves are for complex "
                "multi-step pipelines with explicit dependency tracking."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Operation: observe, start, next, record.",
                        "enum": ["observe", "start", "next", "record"],
                    },
                    "wave_id": {"type": "string", "description": "Wave identifier (for start/next/record)."},
                    "jobs": {"type": "string", "description": "Job results for record, format: 'label:pass,label2:fail'."},
                },
                "required": ["action"],
            },
        },
    ),
}
