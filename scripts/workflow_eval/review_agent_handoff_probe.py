#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _job_review_mode(spec: dict[str, Any], review_label: str) -> str:
    jobs = spec.get("jobs")
    if not isinstance(jobs, list):
        return "missing"
    for job in jobs:
        if not isinstance(job, dict):
            continue
        if str(job.get("label") or "").strip() != review_label:
            continue
        if isinstance(job.get("agent"), str) and job["agent"].strip():
            return "agent"
        adapter_type = str(job.get("adapter_type") or "").strip()
        return adapter_type or "missing"
    return "missing"


def _path_exists(path_text: str) -> bool:
    return Path(path_text).exists()


def _planned_output_rows(paths: list[str]) -> list[dict[str, Any]]:
    return [
        {
            "path": path_text,
            "exists": _path_exists(path_text),
        }
        for path_text in paths
    ]


def build_reviews(contract_path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    contract = _load_json(contract_path)
    spec = _load_json(Path(contract["spec_path"]))
    report = _load_json(Path(contract["observed_report_path"]))
    blocker_path = Path(contract["validation_blocker_path"])
    blocker = _load_json(blocker_path) if blocker_path.exists() else {}
    continuation_path = Path(contract["continuation_packet_path"])
    continuation = _load_json(continuation_path) if continuation_path.exists() else {}

    planned_outputs = [str(path) for path in contract.get("planned_workflow_outputs", [])]
    review_label = str(contract.get("workflow_review_job_label") or "probe_review_contract")
    expected_review_mode = str(contract.get("expected_internal_review_mode") or "").strip()
    review_mode = _job_review_mode(spec, review_label)
    scheduler_candidates = [str(path) for path in contract.get("scheduler_config_candidates", [])]
    scheduler_rows = _planned_output_rows(scheduler_candidates)
    scheduler_config_present = any(row["exists"] for row in scheduler_rows)
    scheduling_authority = str(contract.get("scheduling_authority") or "workflow_triggers").strip()
    trigger_payload = (
        dict(contract.get("trigger_payload") or {})
        if isinstance(contract.get("trigger_payload"), dict)
        else {}
    )
    db_trigger_expected = scheduling_authority == "workflow_triggers" and bool(trigger_payload)

    run_step = report.get("steps", {}).get("run", {})
    validate_step = report.get("steps", {}).get("validate", {})
    preview_step = report.get("steps", {}).get("preview", {})
    search_step = report.get("steps", {}).get("search", {})
    db_step = report.get("steps", {}).get("db_action", {})

    run_id = run_step.get("run_id")
    validate_ok = bool(validate_step.get("ok"))
    preview_ok = bool(preview_step.get("ok"))
    run_ok = bool(run_step.get("ok"))
    live_run_submitted = bool(run_step.get("ok")) and isinstance(run_id, str) and run_id.strip() != ""
    live_outputs = _planned_output_rows(planned_outputs)
    live_outputs_proven = live_run_submitted and all(row["exists"] for row in live_outputs)

    review_is_workflow_internal_deterministic = review_mode in {"deterministic_task", "verifier"}
    review_mode_matches_contract = (
        not expected_review_mode or review_mode == expected_review_mode
    )
    post_run_deterministic_review_ready = True

    validation_front_door_ready = validate_ok and preview_ok
    command_write_blocked = not run_ok
    db_trigger_attached = bool(run_step.get("db_trigger_attached"))
    if db_trigger_attached:
        db_trigger_reason = "Observed packet recorded a DB-native workflow trigger attachment."
    elif not db_trigger_expected:
        db_trigger_reason = "The output contract does not declare DB-native trigger scheduling."
    elif live_run_submitted:
        db_trigger_reason = "No trigger evidence was recorded in the observed packet."
    else:
        db_trigger_reason = (
            "No persisted workflow run exists, so no DB-native trigger attachment is proven."
        )

    regular_outputs_enabled = db_trigger_attached if db_trigger_expected else scheduler_config_present
    regular_outputs_proven = regular_outputs_enabled and live_run_submitted

    deterministic_review = {
        "review_type": "deterministic_probe_review",
        "reviewed_at": _utc_now(),
        "probe_id": str(contract["probe_id"]),
        "current_outputs": {
            "observed_files": [
                {
                    "path": str(contract["observed_report_path"]),
                    "exists": _path_exists(str(contract["observed_report_path"])),
                    "proof_class": "observed",
                },
                {
                    "path": str(contract["continuation_packet_path"]),
                    "exists": continuation_path.exists(),
                    "proof_class": str(continuation.get("proof_class") or "unknown"),
                },
                {
                    "path": str(contract["validation_blocker_path"]),
                    "exists": blocker_path.exists(),
                    "proof_class": str(blocker.get("proof_class") or "unknown"),
                },
            ],
            "planned_live_workflow_files": live_outputs,
        },
        "answers": {
            "what_is_it_outputting_now": [
                "observed proof report",
                "simulated continuation packet",
                "validation blocker packet",
            ],
            "workflow_live_outputs_on_success": [
                "handoff markdown artifact",
                "review json artifact",
            ],
            "regular_outputs_scheduled": regular_outputs_enabled,
            "deterministic_review_present": post_run_deterministic_review_ready,
            "workflow_internal_review_is_deterministic": review_is_workflow_internal_deterministic,
            "workflow_internal_review_matches_contract": review_mode_matches_contract,
        },
        "evidence": {
            "search_exercised": bool(search_step.get("ok")),
            "db_action_exercised": bool(db_step.get("ok")),
            "validate_currently_ok": validate_ok,
            "preview_currently_ok": preview_ok,
            "run_currently_ok": run_ok,
            "run_submitted": live_run_submitted,
            "run_id": run_id,
            "workflow_internal_review_mode": review_mode,
            "workflow_internal_review_expected_mode": expected_review_mode or None,
            "validation_front_door_ready": validation_front_door_ready,
            "command_write_blocked": command_write_blocked,
            "historical_validation_blocker_present": bool(blocker.get("blocked")),
            "scheduling_authority": scheduling_authority,
            "trigger_payload": trigger_payload or None,
        },
        "judgement": {
            "regular_outputs_proven": regular_outputs_proven,
            "live_outputs_proven": live_outputs_proven,
            "post_run_deterministic_review_ready": post_run_deterministic_review_ready,
            "workflow_internal_review_deterministic": (
                review_is_workflow_internal_deterministic and review_mode_matches_contract
            ),
        },
        "gaps": [],
        "required_next_actions": [
            "Restore workflow front-door health so validate, preview, and bug filing work again.",
            "Persist the probe as a first-class workflow definition and attach a DB-native cron trigger.",
            "Require a non-null run_id plus durable receipts before calling the probe end-to-end.",
        ],
    }
    if not regular_outputs_enabled:
        deterministic_review["gaps"].append("The probe is not attached to a recurring schedule yet.")
    if not review_is_workflow_internal_deterministic:
        deterministic_review["gaps"].append(
            "The workflow's internal review step is not using a deterministic machine-checkable adapter."
        )
    elif not review_mode_matches_contract:
        deterministic_review["gaps"].append(
            f"The workflow's internal review mode is {review_mode!r}, not the contracted {expected_review_mode!r}."
        )
    if not live_run_submitted:
        deterministic_review["gaps"].append(
            "A real workflow run has not produced a durable run_id in the observed packet."
        )
    if review_is_workflow_internal_deterministic and review_mode_matches_contract:
        deterministic_review["required_next_actions"] = [
            action
            for action in deterministic_review["required_next_actions"]
            if "Replace the internal agent review step" not in action
        ]

    schedule_status = {
        "review_type": "probe_schedule_status",
        "reviewed_at": deterministic_review["reviewed_at"],
        "probe_id": str(contract["probe_id"]),
        "regular_outputs_enabled": regular_outputs_enabled,
        "regular_outputs_proven": regular_outputs_proven,
        "scheduler_config_candidates": scheduler_rows,
        "db_native_trigger_attached": db_trigger_attached,
        "db_native_trigger_reason": db_trigger_reason,
        "scheduling_authority": scheduling_authority,
        "trigger_payload": trigger_payload or None,
        "validate_preview_ready": validation_front_door_ready,
        "command_write_blocked": command_write_blocked,
        "command_write_blocker": run_step.get("failure_reason")
        or run_step.get("status")
        or "workflow run did not complete successfully",
        "historical_validation_blocker": blocker.get("failure_summary"),
        "next_attachment_path": [
            "persist a workflow definition with a durable workflow_id",
            "attach a cron trigger through workflow_triggers",
            "tick the scheduler or trigger loop and verify schedule.fired plus run submission",
        ],
    }

    return deterministic_review, schedule_status


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Deterministically review the agent handoff probe outputs.",
    )
    parser.add_argument(
        "--contract",
        required=True,
        help="Path to the probe output contract JSON.",
    )
    args = parser.parse_args()

    contract_path = Path(args.contract).resolve()
    contract = _load_json(contract_path)
    deterministic_review, schedule_status = build_reviews(contract_path)

    _write_json(Path(contract["deterministic_review_output_path"]), deterministic_review)
    _write_json(Path(contract["schedule_status_output_path"]), schedule_status)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
