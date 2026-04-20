"""CLI entry point for the Praxis Engine workflow runner.

Usage:
    python workflow_cli.py commands
    python workflow_cli.py help [<command>]
    python workflow_cli.py run <spec.json> [--dry-run]
    python workflow_cli.py spawn <parent_run_id> <spec.json> [--reason <reason>]
    python workflow_cli.py validate <spec.json>
    python workflow_cli.py chain <coordination.json>
    python workflow_cli.py chain-status [<chain_id>] [--limit N]
    python workflow_cli.py status [--since-hours N]
    python workflow_cli.py active
    python workflow_cli.py diagnose <run_id>
    python workflow_cli.py retry <run_id> <label>
    python workflow_cli.py cancel <run_id>
    python workflow_cli.py repair <run_id>
"""

from __future__ import annotations

import os
import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from runtime.canonical_manifests import generate_manifest, load_app_manifest_record, ManifestRuntimeBoundaryError
from runtime.control_commands import submit_workflow_command
from runtime.spec_compiler import PromptLaunchSpec, compile_prompt_launch_spec
from surfaces.cli.mcp_tools import run_cli_tool

_WORKFLOW_ROOT = str(Path(__file__).resolve().parent.parent.parent)


def _workflow_subsystems():
    from surfaces.mcp.subsystems import _subs

    return _subs

def _repo_root() -> str:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "Code&DBs" / "Workflow").exists():
            return str(parent)
    return str(current.parent.parent.parent.parent.parent)


def _get_pg_conn():
    return _workflow_subsystems().get_pg_conn()


def _write_result_file(path: str, payload: dict) -> None:
    result_path = Path(path)
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(json.dumps(payload, default=str, indent=2), encoding="utf-8")


def _format_status_counts(counts: object) -> str:
    if not isinstance(counts, dict):
        return ""
    parts = [
        f"{str(status)}={int(count)}"
        for status, count in sorted(counts.items())
        if int(count) > 0
    ]
    return ", ".join(parts)


def _delegate_modern_workflow_cli(argv: list[str]) -> int:
    from surfaces.cli.main import main as modern_workflow_cli

    return modern_workflow_cli(argv, stdout=sys.stdout)


def _submit_workflow_launch(
    *,
    spec_path: str | None = None,
    prompt_launch_spec: PromptLaunchSpec | None = None,
    preview_execution: bool = False,
    dry_run: bool = False,
    fresh: bool = False,
    job_id: str | None = None,
    run_id: str | None = None,
    result_file: str | None = None,
    requested_by_kind: str = "cli",
    requested_by_ref: str = "workflow_cli.run",
) -> int:
    if spec_path is None and prompt_launch_spec is None:
        print("ERROR: workflow launch requires a spec path or inline spec", file=sys.stderr)
        return 1
    if preview_execution and dry_run:
        print("ERROR: --preview-execution cannot be combined with --dry-run", file=sys.stderr)
        return 1

    if spec_path is not None:
        from runtime.workflow_spec import WorkflowSpec, WorkflowSpecError

        try:
            spec = WorkflowSpec.load(spec_path)
        except WorkflowSpecError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
        workflow_id = spec.workflow_id
        spec_name = spec.name
        total_jobs = len(spec.jobs)
    else:
        assert prompt_launch_spec is not None
        spec = prompt_launch_spec
        workflow_id = spec.workflow_id
        spec_name = spec.name
        total_jobs = len(spec.jobs)

    if preview_execution:
        from runtime.workflow.unified import preview_workflow_execution

        try:
            preview_payload = preview_workflow_execution(
                _get_pg_conn(),
                spec_path=spec_path,
                inline_spec=None if prompt_launch_spec is None else prompt_launch_spec.to_inline_spec_dict(),
                repo_root=_repo_root(),
            )
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "error": str(exc),
                        "error_code": "workflow.preview.failed",
                    },
                    indent=2,
                ),
                file=sys.stderr,
            )
            return 1
        if result_file:
            _write_result_file(result_file, preview_payload)
            print(f"Result written to: {result_file}")
        print(json.dumps(preview_payload, indent=2))
        return 0

    mode_label = "DRY-RUN" if dry_run else "ASYNC"
    print(f"=== Workflow {mode_label}: {spec_name} ===")
    print(f"Phase: {spec.phase}  |  Jobs: {total_jobs}  |  Workflow ID: {workflow_id}")
    print()

    if dry_run:
        from runtime.workflow.dry_run import dry_run_workflow

        result = dry_run_workflow(spec)
        for jr in result.job_results:
            status_icon = {"succeeded": "+", "failed": "X", "blocked": "!", "skipped": "-"}.get(str(jr.status or ""), "?")
            verify_passed = jr.verify_passed
            verify_str = f"  verify={'PASS' if verify_passed else 'FAIL'}" if verify_passed is not None else ""
            print(
                f"  [{status_icon}] {str(jr.job_label or ''):<50} "
                f"{str(jr.status or ''):<10} {float(jr.duration_seconds or 0.0):>6.1f}s{verify_str}"
            )

        print()
        print("--- Summary ---")
        print(f"Total: {result.total_jobs}  |  OK: {result.succeeded}  |  "
              f"Failed: {result.failed}  |  Blocked: {result.blocked}  |  "
              f"Skipped: {result.skipped}")
        print(f"Duration: {float(result.duration_seconds or 0.0):.2f}s")
        print(f"Receipts: {len(result.receipts_written)} written")
        return 0 if result.failed == 0 and result.blocked == 0 else 1

    result = submit_workflow_command(
        _get_pg_conn(),
        requested_by_kind=requested_by_kind,
        requested_by_ref=requested_by_ref,
        spec_path=spec_path,
        inline_spec=None if prompt_launch_spec is None else prompt_launch_spec.to_inline_spec_dict(),
        repo_root=_repo_root(),
        run_id=run_id,
        force_fresh_run=fresh,
        spec_name=spec_name,
        total_jobs=total_jobs,
    )
    if result.get("error") or not result.get("run_id"):
        print(json.dumps(result, indent=2), file=sys.stderr)
        return 1

    print(f"Submitted workflow: {result['run_id']}")
    print(f"Workflow ID: {workflow_id}")
    print(f"Submission status: {result.get('status', 'queued')}")
    status_source = str(result.get("status_source") or "").strip()
    if status_source:
        print(f"Status source: {status_source}")
    terminal_reason = str(result.get("terminal_reason") or "").strip()
    if terminal_reason:
        print(f"Terminal reason: {terminal_reason}")
    run_metrics = result.get("run_metrics")
    if isinstance(run_metrics, dict):
        completed_jobs = int(run_metrics.get("completed_jobs") or 0)
        total_metric_jobs = int(run_metrics.get("total_jobs") or total_jobs)
        health_state = str(run_metrics.get("health_state") or "unknown")
        elapsed_seconds = float(run_metrics.get("elapsed_seconds") or 0.0)
        status_counts = _format_status_counts(run_metrics.get("job_status_counts"))
        total_cost_usd = float(run_metrics.get("total_cost_usd") or 0.0)
        total_tokens_in = int(run_metrics.get("total_tokens_in") or 0)
        total_tokens_out = int(run_metrics.get("total_tokens_out") or 0)
        should_render_metrics = (
            completed_jobs > 0
            or health_state != "unknown"
            or status_counts != ""
            or total_cost_usd > 0
            or total_tokens_in > 0
            or total_tokens_out > 0
            or terminal_reason != ""
            or result.get("status") not in {"queued"}
        )
        if should_render_metrics:
            print(
                "Run metrics: "
                f"{completed_jobs}/{total_metric_jobs} completed | "
                f"health={health_state} | elapsed={elapsed_seconds:.1f}s"
            )
        if status_counts:
            print(f"Job states: {status_counts}")
        if total_cost_usd > 0 or total_tokens_in > 0 or total_tokens_out > 0:
            print(
                "Usage: "
                f"cost=${total_cost_usd:.4f} | "
                f"tokens_in={total_tokens_in} | "
                f"tokens_out={total_tokens_out}"
            )
    if result_file:
        result_payload = dict(result)
        result_payload.update(
            {
                "job_id": job_id or "cli",
                "workflow_id": workflow_id,
            }
        )
        _write_result_file(
            result_file,
            result_payload,
        )
        print(f"Result written to: {result_file}")
    print("Observe via:")
    print(f"  ./scripts/praxis workflow stream {result['run_id']}")
    print(f"  GET {result['stream_url']}")
    print(f"  GET {result['status_url']}")
    return 0


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def _json_merge(target: dict, source: dict) -> dict:
    """Recursively merges source dict into target dict."""
    for key, value in source.items():
        if key in target and isinstance(target[key], dict) and isinstance(value, dict):
            target[key] = _json_merge(target[key], value)
        else:
            target[key] = value
    return target


def cmd_generate(args: argparse.Namespace) -> int:
    """Generate a workflow spec from a manifest file."""
    if _WORKFLOW_ROOT not in sys.path:
        sys.path.insert(0, _WORKFLOW_ROOT)

    output_path = Path(args.output)
    if output_path.exists():
        if args.strict:
            print(f"ERROR: Output file already exists at {output_path} (strict mode enabled)", file=sys.stderr)
            return 1
        if not args.merge:
            print(f"ERROR: Output file already exists at {output_path}. Use --merge to merge or remove the file.", file=sys.stderr)
            return 1

    try:
        manifest_content = json.loads(Path(args.manifest_file).read_text())
    except (json.JSONDecodeError, FileNotFoundError) as exc:
        print(f"ERROR: Could not read or parse manifest file: {exc}", file=sys.stderr)
        return 1

    intent = manifest_content.get("intent")
    if not intent:
        print("ERROR: Manifest file must contain an 'intent' field.", file=sys.stderr)
        return 1

    pg_conn = _get_pg_conn()
    subsystems = _workflow_subsystems()
    try:
        result = generate_manifest(
            pg_conn,
            matcher=subsystems.get_intent_matcher(),
            generator=subsystems.get_manifest_generator(),
            intent=intent,
        )
        
        generated_manifest_content = result.manifest

        final_content = generated_manifest_content
        if args.merge and output_path.exists():
            try:
                existing_content = json.loads(output_path.read_text())
                final_content = _json_merge(existing_content, generated_manifest_content)
            except json.JSONDecodeError as exc:
                print(f"ERROR: Could not parse existing output file for merging: {exc}", file=sys.stderr)
                return 1

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(final_content, indent=2), encoding="utf-8")
        print(f"Successfully generated workflow spec to {output_path}")
        return 0
    except ManifestRuntimeBoundaryError as exc:
        print(f"ERROR: Manifest generation failed: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR: An unexpected error occurred during generation: {exc}", file=sys.stderr)
        return 1


def cmd_run(args: argparse.Namespace) -> int:
    if _WORKFLOW_ROOT not in sys.path:
        sys.path.insert(0, _WORKFLOW_ROOT)
    return _submit_workflow_launch(
        spec_path=args.spec,
        preview_execution=bool(getattr(args, "preview_execution", False)),
        dry_run=bool(args.dry_run),
        fresh=bool(getattr(args, "fresh", False)),
        job_id=args.job_id,
        run_id=args.run_id,
        result_file=args.result_file,
        requested_by_kind="cli",
        requested_by_ref="workflow_cli.run",
    )


def cmd_spawn(args: argparse.Namespace) -> int:
    if _WORKFLOW_ROOT not in sys.path:
        sys.path.insert(0, _WORKFLOW_ROOT)

    from runtime.workflow_spec import WorkflowSpec, WorkflowSpecError

    try:
        spec = WorkflowSpec.load(args.spec)
    except WorkflowSpecError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"=== Workflow SPAWN: {spec.name} ===")
    print(f"Parent Run: {args.parent_run_id}  |  Phase: {spec.phase}  |  Jobs: {len(spec.jobs)}  |  Workflow ID: {spec.workflow_id}")
    print()

    action_payload: dict[str, object] = {
        "action": "spawn",
        "spec_path": args.spec,
        "parent_run_id": args.parent_run_id,
        "dispatch_reason": args.reason,
    }
    if args.parent_job_label:
        action_payload["parent_job_label"] = args.parent_job_label
    if args.run_id:
        action_payload["run_id"] = args.run_id
    if getattr(args, "fresh", False):
        action_payload["force_fresh_run"] = True
    if args.lineage_depth is not None:
        action_payload["lineage_depth"] = args.lineage_depth

    exit_code, result = run_cli_tool("praxis_workflow", action_payload)
    if exit_code != 0:
        print(json.dumps(result, indent=2), file=sys.stderr)
        return 1

    print(f"Spawned child workflow: {result['run_id']}")
    print(f"Parent run: {args.parent_run_id}")
    print(f"Workflow ID: {spec.workflow_id}")
    print(f"Submission status: {result.get('status', 'queued')}")
    if args.result_file:
        result_payload = dict(result)
        result_payload.update(
            {
                "job_id": args.job_id or "cli",
                "workflow_id": spec.workflow_id,
                "parent_run_id": args.parent_run_id,
                "dispatch_reason": args.reason,
            }
        )
        _write_result_file(
            args.result_file,
            result_payload,
        )
        print(f"Result written to: {args.result_file}")
    print("Observe via:")
    print(f"  ./scripts/praxis workflow stream {result['run_id']}")
    print(f"  GET {result['stream_url']}")
    print(f"  GET {result['status_url']}")
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    """Validate a workflow spec without running it."""
    if _WORKFLOW_ROOT not in sys.path:
        sys.path.insert(0, _WORKFLOW_ROOT)

    from runtime.workflow_spec import WorkflowSpec, WorkflowSpecError, load_raw, validate_authoring_spec, _is_new_authoring_format
    from runtime.workflow_validation import validate_workflow_spec

    try:
        raw = load_raw(args.spec)
        if _is_new_authoring_format(raw):
            ok, errors = validate_authoring_spec(raw)
            if not ok:
                print("INVALID (authoring schema):", file=sys.stderr)
                for err in errors:
                    print(f"  - {err}", file=sys.stderr)
                return 1
        spec = WorkflowSpec.load(args.spec)
    except WorkflowSpecError as exc:
        print(f"INVALID: {exc}", file=sys.stderr)
        return 1

    result = validate_workflow_spec(spec, pg_conn=_get_pg_conn())
    summary = result["summary"]
    print(f"=== Spec Validation: {'PASSED' if result.get('valid', False) else 'FAILED'} ===")
    print(f"Name:             {summary['name']}")
    print(f"Workflow ID:      {summary['workflow_id']}")
    print(f"Phase:            {summary['phase']}")
    goal = summary['outcome_goal']
    print(f"Outcome Goal:     {goal[:80]}..." if len(goal) > 80 else f"Outcome Goal:     {goal}")
    print(f"Jobs:             {summary['job_count']}")
    print()
    print("Jobs:")
    for label in summary['job_labels']:
        print(f"  - {label}")

    print()
    print("Agent Resolution:")
    for detail in result.get("agent_resolution_details", []):
        requested = detail.get("requested_slug") or ""
        resolved = detail.get("resolved_slug")
        status = str(detail.get("status") or "unresolved")
        if status == "resolved":
            suffix = "OK"
        elif status == "aliased":
            suffix = f"ALIASED -> {resolved}"
        else:
            message = detail.get("message")
            suffix = f"NOT FOUND ({message})" if message else "NOT FOUND"
        print(f"  {detail.get('label')}: {requested} -> {suffix}")

    if not result.get("valid", False):
        print()
        print(result.get("error") or "Invalid workflow: one or more agent routes could not be resolved")
        return 1

    if getattr(args, "check_gates", False):
        from runtime.workflow.capability_preflight import (
            any_blocking,
            preflight_spec_jobs,
        )

        jobs = list(raw.get("jobs") or [])
        worker_image = getattr(args, "worker_image", None) or "praxis-worker:latest"
        print()
        print(f"Capability preflight (worker={worker_image}):")
        reports = preflight_spec_jobs(jobs, worker_image=worker_image)
        if not reports:
            print("  (no verify_commands to check)")
        for report in reports:
            verdict = report.result.verdict.upper()
            print(f"  [{verdict}] {report.label}: {report.result.reason}")
            if report.result.verdict == "reject":
                tail = (report.result.stderr_tail or report.result.stdout_tail or "").strip()
                if tail:
                    first = tail.splitlines()[-1][:200]
                    print(f"      last: {first}")
        if any_blocking(reports):
            print()
            print("Capability preflight REJECTED one or more verify_commands.")
            return 1

    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """Show recent workflow status from Postgres."""
    try:
        from runtime.receipt_store import list_receipts

        rows = list_receipts(limit=50, since_hours=args.since_hours)

        if not rows:
            print(f"No receipts found in the last {args.since_hours} hours.")
            return 0

        print(f"=== Workflow Status (last {args.since_hours}h) ===")
        print(f"{'Job Label':<50} {'Status':<12} {'Agent':<30}")
        print("-" * 92)
        for row in rows:
            print(f"{row.label:<50} {row.status:<12} {row.agent:<30}")

        total = len(rows)
        ok = sum(1 for row in rows if row.status == "succeeded")
        fail = sum(1 for row in rows if row.status == "failed")
        print()
        print(f"Total: {total}  |  Succeeded: {ok}  |  Failed: {fail}")
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    return 0


def cmd_active(args: argparse.Namespace) -> int:
    """Show active workflow runs from Postgres authority."""
    del args
    try:
        pg_conn = _get_pg_conn()
        rows = pg_conn.execute(
            """SELECT r.run_id,
                      r.workflow_id,
                      r.current_state,
                      r.requested_at,
                      r.started_at,
                      COALESCE(job_counts.nonterminal_jobs, 0) AS nonterminal_jobs
               FROM workflow_runs r
               LEFT JOIN LATERAL (
                   SELECT COUNT(*) FILTER (
                              WHERE status IN ('pending', 'ready', 'claimed', 'running')
                          ) AS nonterminal_jobs
                   FROM workflow_jobs j
                   WHERE j.run_id = r.run_id
               ) job_counts ON TRUE
               WHERE r.current_state IN ('queued', 'running')
                 AND (
                     COALESCE(job_counts.nonterminal_jobs, 0) > 0
                     OR r.requested_at >= now() - interval '2 minutes'
                 )
               ORDER BY requested_at DESC
               LIMIT 50"""
        )
        print(json.dumps([dict(row) for row in (rows or [])], default=str, indent=2))
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


def cmd_stream(args: argparse.Namespace) -> int:
    """Stream one workflow run's progress through the terminal."""
    try:
        pg_conn = _get_pg_conn()
        from runtime.workflow.unified import get_run_status

        initial = get_run_status(pg_conn, args.run_id)
        if initial is None:
            print(f"ERROR: Run {args.run_id} not found", file=sys.stderr)
            return 1

        total_jobs = int(initial.get("total_jobs") or 0)
        spec_name = initial.get("spec_name", "")
        status = initial.get("status", "unknown")
        terminal_states = {"succeeded", "failed", "dead_letter", "cancelled"}
        terminal_job_states = {
            "succeeded",
            "failed",
            "blocked",
            "dead_letter",
            "cancelled",
            "parent_failed",
        }

        def _job_label(job: dict[str, object]) -> str:
            return str(job.get("label") or job.get("job_label") or "")

        def _job_agent(job: dict[str, object]) -> str:
            return str(job.get("resolved_agent") or job.get("agent_slug") or "")

        def _duration_seconds(job: dict[str, object]) -> float:
            try:
                return float(job.get("duration_ms") or 0) / 1000.0
            except (TypeError, ValueError):
                return 0.0

        def _counts(status_data: dict[str, object]) -> tuple[int, int, int, int, int]:
            rows = [job for job in status_data.get("jobs", []) if isinstance(job, dict)]
            passed = sum(1 for job in rows if job.get("status") == "succeeded")
            failed = sum(1 for job in rows if job.get("status") in {"failed", "blocked", "dead_letter", "parent_failed"})
            active = sum(1 for job in rows if job.get("status") in {"claimed", "running"})
            waiting = sum(1 for job in rows if job.get("status") in {"pending", "ready"})
            completed = int(status_data.get("completed_jobs") or passed + failed)
            return completed, passed, failed, active, waiting

        def _elapsed_seconds(status_data: dict[str, object]) -> float:
            started = status_data.get("created_at") or status_data.get("requested_at")
            finished = status_data.get("finished_at")
            if not isinstance(started, datetime):
                return 0.0
            end = finished if isinstance(finished, datetime) else datetime.now(timezone.utc)
            return max(0.0, (end - started).total_seconds())

        def _status_signature(status_data: dict[str, object]) -> tuple[object, ...]:
            rows = [job for job in status_data.get("jobs", []) if isinstance(job, dict)]
            return (
                status_data.get("status"),
                status_data.get("completed_jobs"),
                tuple(
                    (
                        _job_label(job),
                        job.get("status"),
                        job.get("attempt"),
                        job.get("claimed_by"),
                        job.get("heartbeat_at"),
                        job.get("finished_at"),
                        job.get("last_error_code"),
                    )
                    for job in rows
                ),
            )

        def _print_snapshot(status_data: dict[str, object]) -> None:
            completed, passed, failed, active, waiting = _counts(status_data)
            current_status = str(status_data.get("status") or "unknown")
            elapsed = _elapsed_seconds(status_data)
            print(
                "status "
                f"state={current_status} completed={completed}/{total_jobs} "
                f"passed={passed} failed={failed} active={active} waiting={waiting} "
                f"elapsed_s={elapsed:.1f}"
            )
            for job in status_data.get("jobs", []):
                if not isinstance(job, dict):
                    continue
                job_status = str(job.get("status") or "")
                if job_status not in {"claimed", "running", "ready"}:
                    continue
                detail = (
                    "active "
                    f"label={_job_label(job)} status={job_status} agent={_job_agent(job)}"
                )
                if job.get("attempt"):
                    detail += f" attempt={job.get('attempt')}"
                if job.get("claimed_by"):
                    detail += f" claimed_by={job.get('claimed_by')}"
                heartbeat_at = job.get("heartbeat_at")
                if isinstance(heartbeat_at, datetime):
                    age = (datetime.now(timezone.utc) - heartbeat_at).total_seconds()
                    detail += f" heartbeat_age_s={max(0.0, age):.1f}"
                print(detail)

        def _print_terminal_jobs(status_data: dict[str, object], emitted_labels: set[str]) -> None:
            completed, passed, failed, _, _ = _counts(status_data)
            for job in status_data.get("jobs", []):
                if not isinstance(job, dict):
                    continue
                label = _job_label(job)
                job_status = str(job.get("status") or "")
                if not label or label in emitted_labels or job_status not in terminal_job_states:
                    continue
                emitted_labels.add(label)
                failure_code = str(
                    job.get("failure_category")
                    or job.get("last_error_code")
                    or job.get("error_code")
                    or ""
                )
                line = (
                    "job    "
                    f"label={label} status={job_status} agent={_job_agent(job)} "
                    f"duration_s={_duration_seconds(job):.1f}"
                )
                if failure_code:
                    line += f" failure_code={failure_code}"
                print(line)
                print(f"progress completed={completed} total={total_jobs} passed={passed} failed={failed}")

        print(f"start  run_id={args.run_id} spec={spec_name} total_jobs={total_jobs} status={status}")
        _print_snapshot(initial)

        if status in terminal_states:
            emitted_terminal_jobs: set[str] = set()
            _print_terminal_jobs(initial, emitted_terminal_jobs)
            _, terminal_passed, terminal_failed, _, _ = _counts(initial)
            print(f"done   status={status} passed={terminal_passed} failed={terminal_failed} total={total_jobs}")
            return 0 if status in ("succeeded", "cancelled") else 1

        deadline = time.monotonic() + float(args.timeout) if args.timeout is not None else None
        last_signature = _status_signature(initial)
        emitted_terminal_jobs: set[str] = set()
        final = initial
        while deadline is None or time.monotonic() < deadline:
            sleep_for = max(0.1, float(args.poll_interval or 2.0))
            if deadline is not None:
                sleep_for = min(sleep_for, max(0.0, deadline - time.monotonic()))
            if sleep_for > 0:
                time.sleep(sleep_for)

            current = get_run_status(pg_conn, args.run_id)
            if current is None:
                final = None
                break
            final = current
            signature = _status_signature(current)
            if signature != last_signature:
                _print_snapshot(current)
                last_signature = signature

            completed, passed, failed, _, _ = _counts(current)
            _print_terminal_jobs(current, emitted_terminal_jobs)

            current_status = str(current.get("status") or "unknown")
            if current_status in terminal_states:
                print(f"done   status={current_status} passed={passed} failed={failed} total={total_jobs}")
                return 0 if current_status in ("succeeded", "cancelled") else 1

        if final is None:
            print("done   status=not_found passed=0 failed=0 total=0")
            return 1
        completed, passed, failed, _, _ = _counts(final)
        final_status = str(final.get("status") or "timeout")
        if final_status not in terminal_states and completed < total_jobs:
            final_status = "timeout"
        print(f"done   status={final_status} passed={passed} failed={failed} total={total_jobs}")
        return 0 if final_status in ("succeeded", "cancelled") else 1
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


def cmd_cancel(args: argparse.Namespace) -> int:
    """Cancel a workflow run via DB-backed workflow authority."""
    try:
        pg_conn = _get_pg_conn()
        from runtime.control_commands import (
            ControlCommandType,
            ControlIntent,
            execute_control_intent,
            render_control_command_response,
        )

        command = execute_control_intent(
            pg_conn,
            ControlIntent(
                command_type=ControlCommandType.WORKFLOW_CANCEL,
                requested_by_kind="cli",
                requested_by_ref="workflow_cli.cancel",
                idempotency_key=f"workflow.cancel.cli.{args.run_id}",
                payload={"run_id": args.run_id, "include_running": True},
            ),
            approved_by="cli.workflow.cancel",
        )
        result = render_control_command_response(
            pg_conn,
            command,
            action="cancel",
            run_id=args.run_id,
        )
        print(json.dumps(result, default=str, indent=2))
        return 0 if result.get("status") == "cancelled" else 1
    except Exception as exc:
        try:
            from runtime.control_commands import render_control_command_failure

            details = getattr(exc, "details", None)
            failure = render_control_command_failure(
                error_code=getattr(exc, "reason_code", "control.command.execution_failed"),
                error_detail=str(exc),
                run_id=args.run_id,
                details=details if isinstance(details, dict) else None,
            )
            print(json.dumps(failure, default=str, indent=2))
        except Exception:
            print(f"ERROR: {exc}", file=sys.stderr)
        return 1


def cmd_retry(args: argparse.Namespace) -> int:
    """Retry one failed workflow job via DB-backed workflow authority."""
    try:
        pg_conn = _get_pg_conn()
        from runtime.control_commands import (
            ControlCommandType,
            ControlIntent,
            execute_control_intent,
            render_control_command_response,
        )

        command = execute_control_intent(
            pg_conn,
            ControlIntent(
                command_type=ControlCommandType.WORKFLOW_RETRY,
                requested_by_kind="cli",
                requested_by_ref="workflow_cli.retry",
                idempotency_key=f"workflow.retry.cli.{args.run_id}.{args.label}",
                payload={"run_id": args.run_id, "label": args.label},
            ),
            approved_by="cli.workflow.retry",
        )
        result = render_control_command_response(
            pg_conn,
            command,
            action="retry",
            run_id=args.run_id,
            label=args.label,
        )
        print(json.dumps(result, default=str, indent=2))
        return 0 if result.get("status") == "requeued" else 1
    except Exception as exc:
        try:
            from runtime.control_commands import render_control_command_failure

            details = getattr(exc, "details", None)
            failure = render_control_command_failure(
                error_code=getattr(exc, "reason_code", "control.command.execution_failed"),
                error_detail=str(exc),
                run_id=args.run_id,
                label=args.label,
                details=details if isinstance(details, dict) else None,
            )
            print(json.dumps(failure, default=str, indent=2))
        except Exception:
            print(f"ERROR: {exc}", file=sys.stderr)
        return 1


def cmd_repair(args: argparse.Namespace) -> int:
    """Repair the post-workflow sync state for one workflow run."""
    try:
        pg_conn = _get_pg_conn()
        from runtime.control_commands import (
            ControlCommandType,
            ControlIntent,
            execute_control_intent,
            render_control_command_response,
        )

        command = execute_control_intent(
            pg_conn,
            ControlIntent(
                command_type=ControlCommandType.SYNC_REPAIR,
                requested_by_kind="cli",
                requested_by_ref="workflow_cli.repair",
                idempotency_key=f"sync.repair.cli.{args.run_id}",
                payload={"run_id": args.run_id},
            ),
            approved_by="cli.workflow.repair",
        )
        result = render_control_command_response(
            pg_conn,
            command,
            action="repair",
            run_id=args.run_id,
        )
        print(json.dumps(result, default=str, indent=2))
        return 0 if result.get("status") == "repaired" else 1
    except Exception as exc:
        try:
            from runtime.control_commands import render_control_command_failure

            details = getattr(exc, "details", None)
            failure = render_control_command_failure(
                error_code=getattr(exc, "reason_code", "control.command.execution_failed"),
                error_detail=str(exc),
                run_id=args.run_id,
                details=details if isinstance(details, dict) else None,
            )
            print(json.dumps(failure, default=str, indent=2))
        except Exception:
            print(f"ERROR: {exc}", file=sys.stderr)
        return 1


def cmd_chain(args: argparse.Namespace) -> int:
    """Submit one durable multi-wave workflow chain."""
    if _WORKFLOW_ROOT not in sys.path:
        sys.path.insert(0, _WORKFLOW_ROOT)

    from runtime.workflow_chain import (
        WorkflowChainError,
    )
    from runtime.control_commands import (
        render_workflow_chain_submit_response,
        request_workflow_chain_submit_command,
    )

    repo_root = _repo_root()
    pg_conn = _get_pg_conn()

    try:
        result = render_workflow_chain_submit_response(
            pg_conn,
            request_workflow_chain_submit_command(
                pg_conn,
                requested_by_kind="cli",
                requested_by_ref="workflow_cli.chain",
                coordination_path=args.coordination,
                repo_root=repo_root,
                adopt_active=not args.no_adopt_active,
            ),
            coordination_path=args.coordination,
        )
    except WorkflowChainError as exc:
        payload = {"status": "failed", "error": str(exc)}
        if args.result_file:
            _write_result_file(args.result_file, payload)
        print(json.dumps(payload, default=str, indent=2))
        return 1
    except Exception as exc:
        payload = {"status": "failed", "error": str(exc)}
        if args.result_file:
            _write_result_file(args.result_file, payload)
        print(json.dumps(payload, default=str, indent=2))
        return 1
    if args.result_file:
        _write_result_file(args.result_file, result)
    print(json.dumps(result, default=str, indent=2))
    return 0 if result.get("status") not in {"failed", "approval_required"} else 1


def cmd_chain_status(args: argparse.Namespace) -> int:
    """Show durable workflow-chain status or recent chains."""
    if _WORKFLOW_ROOT not in sys.path:
        sys.path.insert(0, _WORKFLOW_ROOT)

    from runtime.workflow_chain import get_workflow_chain_status, list_workflow_chains

    pg_conn = _get_pg_conn()
    try:
        if args.chain_id:
            payload = get_workflow_chain_status(pg_conn, args.chain_id)
            if payload is None:
                print(
                    json.dumps(
                        {
                            "status": "failed",
                            "error": f"workflow chain not found: {args.chain_id}",
                            "chain_id": args.chain_id,
                        },
                        default=str,
                        indent=2,
                    )
                )
                return 1
            print(json.dumps(payload, default=str, indent=2))
            return 0

        payload = list_workflow_chains(pg_conn, limit=args.limit)
        print(json.dumps(payload, default=str, indent=2))
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    return _delegate_modern_workflow_cli(argv)


if __name__ == "__main__":
    sys.exit(main())
