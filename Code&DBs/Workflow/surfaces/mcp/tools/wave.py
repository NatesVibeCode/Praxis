"""Tools: praxis_wave."""
from __future__ import annotations

from typing import Any

from surfaces.placeholder_ids import is_demo_placeholder, placeholder_error

from ..subsystems import _subs
from ..helpers import _serialize


def _parse_start_jobs(jobs_spec: str) -> list[dict]:
    """Parse the start-action jobs grammar.

    BUG-B9325BED: praxis_wave start previously ignored jobs= and raised
    KeyError for any wave_id that had not been pre-defined via add_wave.
    Per architecture-policy::wave-orchestration::start-accepts-jobs-string
    the tool must auto-define the wave from the jobs list.

    Grammar:
        "a1,a2,a3"                — three jobs, no intra-wave deps
        "a1,a2|a1,a3|a2"          — a2 depends on a1; a3 depends on a2

    The pipe separator is used instead of ':' because ':' is already the
    outcome separator in the record action's jobs grammar. Labels are
    stripped; empty entries are ignored.
    """
    jobs: list[dict] = []
    for raw in (jobs_spec or "").split(","):
        raw = raw.strip()
        if not raw:
            continue
        if "|" in raw:
            parts = [p.strip() for p in raw.split("|") if p.strip()]
            if not parts:
                continue
            label, *deps = parts
            jobs.append({"label": label, "depends_on": deps})
        else:
            jobs.append({"label": raw, "depends_on": []})
    return jobs


def _resolve_wave_id(params: dict, *, action: str) -> tuple[str, dict | None]:
    requested = str(params.get("wave_id", "") or "").strip()
    if is_demo_placeholder("wave_id", requested):
        return "", placeholder_error("wave_id", requested)
    if not requested:
        return "", {
            "error": f"wave_id is required for {action}",
            "reason_code": "wave_id.required",
        }
    return requested, None


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
        wave_id, error_payload = _resolve_wave_id(params, action=action)
        if not wave_id:
            return error_payload or {"error": "wave_id is required for start"}
        # BUG-B9325BED: if the wave isn't defined yet, auto-define it from the
        # jobs= grammar instead of raising KeyError. This implements
        # architecture-policy::wave-orchestration::start-accepts-jobs-string.
        defined_note: str | None = None
        if not orch.is_wave_defined(wave_id):
            jobs_spec = str(params.get("jobs", "") or "").strip()
            if not jobs_spec:
                return {
                    "error": (
                        f"Wave {wave_id} is not defined and no jobs= string was supplied. "
                        "Supply jobs='a1,a2,a3' (optionally 'a1,a2|a1' for deps) to auto-define, "
                        "or call add_wave explicitly first."
                    ),
                    "reason_code": "wave.start.undefined_and_no_jobs",
                }
            parsed_jobs = _parse_start_jobs(jobs_spec)
            if not parsed_jobs:
                return {
                    "error": f"jobs='{jobs_spec}' did not parse to any job labels",
                    "reason_code": "wave.start.jobs_parse_empty",
                }
            orch.add_wave(wave_id=wave_id, jobs=parsed_jobs)
            defined_note = (
                f"auto-defined wave {wave_id} with {len(parsed_jobs)} job(s) "
                "per wave-orchestration start-accepts-jobs-string policy"
            )
        try:
            ws = orch.start_wave(wave_id)
            payload: dict[str, Any] = {
                "wave_id": ws.wave_id,
                "status": ws.status.value,
                "started": True,
            }
            if defined_note:
                payload["note"] = defined_note
            return payload
        except RuntimeError as e:
            return {"error": str(e)}

    if action == "next":
        wave_id, error_payload = _resolve_wave_id(params, action=action)
        if not wave_id:
            return error_payload or {"error": "wave_id is required for next"}
        try:
            runnable = orch.next_runnable_jobs(wave_id)
            return {"wave_id": wave_id, "runnable_jobs": runnable}
        except KeyError:
            return {"error": f"Wave {wave_id} not found"}

    if action == "record":
        wave_id, error_payload = _resolve_wave_id(params, action=action)
        jobs_str = params.get("jobs", "")
        if not wave_id or not jobs_str:
            if error_payload:
                return error_payload
            return {"error": "wave_id and jobs (format: 'label:pass,label2:fail') are required for record"}
        results = []
        for entry in jobs_str.split(","):
            entry = entry.strip()
            if ":" not in entry:
                continue
            label, outcome = entry.split(":", 1)
            succeeded = outcome.strip().lower() in ("pass", "true", "succeeded", "ok", "1")
            orch.record_job_result(wave_id, label.strip(), succeeded)
            results.append({"job_label": label.strip(), "succeeded": succeeded})
        return {"wave_id": wave_id, "recorded": results}

    return {"error": f"Unknown wave action: {action}"}


# Retired from the MCP catalog on 2026-05-01.
#
# The in-memory wave tool was not a durable multi-workflow authority: a fresh
# process could lose a started wave. The operator-facing replacement is
# praxis_solution, backed by workflow_chain DB authority. Keep the helper
# functions in this module temporarily for old unit coverage and archaeology,
# but expose no tool binding from this file.
TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {}
