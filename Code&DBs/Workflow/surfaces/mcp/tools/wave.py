"""Tools: praxis_wave."""
from __future__ import annotations

from typing import Any

from ..subsystems import _subs
from ..helpers import _serialize


_PLACEHOLDER_WAVE_IDS = frozenset({"wave_abc123"})


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
        # BUG-B9325BED: if the wave isn't defined yet, auto-define it from the
        # jobs= grammar instead of raising KeyError. This implements
        # architecture-policy::wave-orchestration::start-accepts-jobs-string.
        defined_note: str | None = None
        if wave_id not in orch._waves:  # inspect the orch's internal registry
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
            if defined_note and note:
                payload["note"] = f"{defined_note}; {note}"
            elif defined_note:
                payload["note"] = defined_note
            elif note:
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
                "  1. praxis_wave(action='observe')                                          — see current wave state\n"
                "  2. praxis_wave(action='start', wave_id='wave_1', jobs='a,b,c|a')          — auto-define + begin a new wave\n"
                "     (jobs grammar for start: comma-separated labels; 'b|a' means b depends on a)\n"
                "  3. praxis_wave(action='next')                                             — get jobs ready on the current/only wave\n"
                "  4. praxis_wave(action='record', jobs='build:pass,test:fail')              — record results on the current/only wave\n\n"
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
                    "jobs": {
                        "type": "string",
                        "description": (
                            "For start: comma-separated job labels to auto-define the wave, "
                            "e.g. 'a1,a2,a3'. Use '|' to declare intra-wave deps: 'a1,a2|a1,a3|a2'. "
                            "For record: job outcomes, format 'label:pass,label2:fail'."
                        ),
                    },
                },
                "required": ["action"],
            },
        },
    ),
}
