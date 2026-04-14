"""CLI entry point for the Praxis Engine workflow runner.

Usage:
    python workflow_cli.py run <spec.json> [--dry-run]
    python workflow_cli.py validate <spec.json>
    python workflow_cli.py chain <coordination.json>
    python workflow_cli.py chain-status [<chain_id>] [--limit N]
    python workflow_cli.py status [--since-hours N]
    python workflow_cli.py active
    python workflow_cli.py retry <run_id> <label>
    python workflow_cli.py cancel <run_id>
    python workflow_cli.py repair <run_id>
"""

from __future__ import annotations

import os
import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import surfaces.mcp.subsystems
from runtime.canonical_manifests import generate_manifest, load_app_manifest_record, ManifestRuntimeBoundaryError
from surfaces.cli.mcp_tools import run_cli_tool

_WORKFLOW_ROOT = str(Path(__file__).resolve().parent.parent.parent)
subsystems = surfaces.mcp.subsystems._subs


def _repo_root() -> str:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "Code&DBs" / "Workflow").exists():
            return str(parent)
    return str(current.parent.parent.parent.parent.parent)


def _get_pg_conn():
    return subsystems.get_pg_conn()


def _write_result_file(path: str, payload: dict) -> None:
    result_path = Path(path)
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(json.dumps(payload, default=str, indent=2), encoding="utf-8")


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

    from runtime.workflow_spec import WorkflowSpec, WorkflowSpecError

    try:
        spec = WorkflowSpec.load(args.spec)
    except WorkflowSpecError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    mode_label = "DRY-RUN" if args.dry_run else "ASYNC"
    print(f"=== Workflow {mode_label}: {spec.name} ===")
    print(f"Phase: {spec.phase}  |  Jobs: {len(spec.jobs)}  |  Workflow ID: {spec.workflow_id}")
    print()

    action_payload: dict[str, object] = {
        "action": "run",
        "spec_path": args.spec,
    }
    if args.dry_run:
        action_payload["dry_run"] = True
    else:
        action_payload["wait"] = False

    exit_code, result = run_cli_tool("praxis_workflow", action_payload)
    if exit_code != 0:
        print(json.dumps(result, indent=2), file=sys.stderr)
        return 1

    if args.dry_run:
        for jr in result.get("job_results", []):
            if not isinstance(jr, dict):
                continue
            status_icon = {"succeeded": "+", "failed": "X", "blocked": "!", "skipped": "-"}.get(str(jr.get("status") or ""), "?")
            verify_passed = jr.get("verify_passed")
            verify_str = f"  verify={'PASS' if verify_passed else 'FAIL'}" if verify_passed is not None else ""
            print(
                f"  [{status_icon}] {str(jr.get('job_label') or ''):<50} "
                f"{str(jr.get('status') or ''):<10} {float(jr.get('duration_seconds') or 0.0):>6.1f}s{verify_str}"
            )

        print()
        print("--- Summary ---")
        print(f"Total: {result.get('total_jobs', 0)}  |  OK: {result.get('succeeded', 0)}  |  "
              f"Failed: {result.get('failed', 0)}  |  Blocked: {result.get('blocked', 0)}  |  "
              f"Skipped: {result.get('skipped', 0)}")
        print(f"Duration: {float(result.get('duration_seconds') or 0.0):.2f}s")
        print(f"Receipts: {len(result.get('receipts_written') or [])} written")
        return 0 if result.get("failed", 0) == 0 and result.get("blocked", 0) == 0 else 1

    print(f"Submitted workflow: {result['run_id']}")
    print(f"Workflow ID: {spec.workflow_id}")
    print(f"Submission status: {result.get('status', 'queued')}")
    if args.result_file:
        result_payload = dict(result)
        result_payload.update(
            {
                "job_id": args.job_id or "cli",
                "workflow_id": spec.workflow_id,
            }
        )
        _write_result_file(
            args.result_file,
            result_payload,
        )
        print(f"Result written to: {args.result_file}")
    print("Observe via:")
    print(f"  ./scripts/workflow.sh stream {result['run_id']}")
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
        from runtime.workflow_notifications import WorkflowNotificationConsumer

        initial = get_run_status(pg_conn, args.run_id)
        if initial is None:
            print(f"ERROR: Run {args.run_id} not found", file=sys.stderr)
            return 1

        total_jobs = int(initial.get("total_jobs") or 0)
        spec_name = initial.get("spec_name", "")
        status = initial.get("status", "unknown")
        jobs = initial.get("jobs", [])
        terminal_passed = sum(1 for job in jobs if job.get("status") == "succeeded")
        terminal_failed = sum(1 for job in jobs if job.get("status") in ("failed", "dead_letter"))

        print(f"start  run_id={args.run_id} spec={spec_name} total_jobs={total_jobs} status={status}")

        if status in ("succeeded", "failed", "dead_letter", "cancelled"):
            print(f"done   status={status} passed={terminal_passed} failed={terminal_failed} total={total_jobs}")
            return 0

        consumer = WorkflowNotificationConsumer(pg_conn)
        passed = 0
        failed = 0
        count = 0
        for notif in consumer.iter_run(
            args.run_id,
            total_jobs,
            timeout_seconds=args.timeout,
            poll_interval=args.poll_interval,
        ):
            count += 1
            if notif.status == "succeeded":
                passed += 1
            else:
                failed += 1
            print(
                "job    "
                f"label={notif.job_label} status={notif.status} agent={notif.agent_slug} "
                f"duration_s={notif.duration_seconds:.1f}"
                + (f" failure_code={notif.failure_code}" if notif.failure_code else "")
            )
            print(f"progress completed={count} total={total_jobs} passed={passed} failed={failed}")

        final = get_run_status(pg_conn, args.run_id)
        if final is None:
            final_status = "timeout"
        else:
            final_status = final.get("status", "unknown")
            if final_status not in ("succeeded", "failed", "dead_letter", "cancelled") and count < total_jobs:
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

def main() -> int:
    parser = argparse.ArgumentParser(
        prog="workflow_cli",
        description="Praxis Engine workflow CLI (Postgres-backed)",
    )
    sub = parser.add_subparsers(dest="command")

    gen_parser = sub.add_parser("generate", help="Generate a workflow spec from a manifest file")
    gen_parser.add_argument("manifest_file", help="Path to the minimal JSON manifest file (e.g., '{ \"intent\": \"build a new dashboard\" }')")
    gen_parser.add_argument("output", help="Path to the output .queue.json spec file")
    gen_group = gen_parser.add_mutually_exclusive_group()
    gen_group.add_argument("--strict", action="store_true", help="Fail if the output file already exists")
    gen_group.add_argument("--merge", action="store_true", help="Merge with existing output file if it exists")

    run_parser = sub.add_parser("run", help="Run a workflow spec through the workflow pipeline")
    run_parser.add_argument("spec", help="Path to .queue.json spec file")
    run_parser.add_argument("--dry-run", action="store_true", help="Simulate without executing")
    run_parser.add_argument("--job-id", help="Job ID for tracking (written to result file)")
    run_parser.add_argument("--run-id", help="Pre-assigned workflow run_id (for outbox tracking)")
    run_parser.add_argument("--result-file", help="Write JSON result to this path on completion")

    val_parser = sub.add_parser("validate", help="Validate a workflow spec without running")
    val_parser.add_argument("spec", help="Path to .queue.json spec file")

    stat_parser = sub.add_parser("status", help="Show recent workflow status")
    stat_parser.add_argument("--since-hours", type=int, default=24, help="Look back N hours (default: 24)")

    sub.add_parser("active", help="Show currently active workflow runs")

    stream_parser = sub.add_parser("stream", help="Stream one workflow run in the terminal")
    stream_parser.add_argument("run_id", help="Workflow run id to stream")
    stream_parser.add_argument("--timeout", type=float, default=None, help="Stop streaming after N seconds")
    stream_parser.add_argument("--poll-interval", type=float, default=2.0, help="Poll interval in seconds")

    retry_parser = sub.add_parser("retry", help="Retry one failed workflow job")
    retry_parser.add_argument("run_id", help="Workflow run id containing the failed job")
    retry_parser.add_argument("label", help="Workflow job label to retry")

    cancel_parser = sub.add_parser("cancel", help="Cancel a workflow run")
    cancel_parser.add_argument("run_id", help="Workflow run id to cancel")

    repair_parser = sub.add_parser("repair", help="Repair post-run sync state for a workflow run")
    repair_parser.add_argument("run_id", help="Workflow run id to repair")

    chain_parser = sub.add_parser("chain", help="Submit a durable multi-wave workflow chain")
    chain_parser.add_argument("coordination", help="Path to a chain coordination JSON file")
    chain_parser.add_argument("--result-file", help="Write chain submit response to this JSON file")
    chain_parser.add_argument("--no-adopt-active", action="store_true", help="Always submit fresh runs instead of adopting active ones")

    chain_status_parser = sub.add_parser("chain-status", help="Show durable workflow-chain status")
    chain_status_parser.add_argument("chain_id", nargs="?", help="Workflow chain id to inspect")
    chain_status_parser.add_argument("--limit", type=int, default=20, help="List N recent chains when chain_id is omitted")

    args = parser.parse_args()

    if args.command == "generate":
        return cmd_generate(args)
    elif args.command == "run":
        return cmd_run(args)
    elif args.command == "validate":
        return cmd_validate(args)
    elif args.command == "status":
        return cmd_status(args)
    elif args.command == "active":
        return cmd_active(args)
    elif args.command == "stream":
        return cmd_stream(args)
    elif args.command == "retry":
        return cmd_retry(args)
    elif args.command == "cancel":
        return cmd_cancel(args)
    elif args.command == "repair":
        return cmd_repair(args)
    elif args.command == "chain":
        return cmd_chain(args)
    elif args.command == "chain-status":
        return cmd_chain_status(args)
    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())
