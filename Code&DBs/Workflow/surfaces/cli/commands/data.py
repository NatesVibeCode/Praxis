"""CLI front door for deterministic data-plane tooling."""

from __future__ import annotations

import json
from typing import Any, TextIO

from surfaces.cli.mcp_tools import (
    get_definition,
    load_json_file,
    print_json,
    run_cli_tool,
    tool_preflight_lines,
)


def _data_help_text() -> str:
    return "\n".join(
        [
            "usage: workflow data <action> [args]",
            "",
            "Deterministic data operations:",
            "  workflow data profile <input-file> [--json]",
            "  workflow data filter --job-file <job.json> [--json]",
            "  workflow data sort --job-file <job.json> [--json]",
            "  workflow data parse <input-file> [--output-file <path>] [--output-format <fmt>] [--yes]",
            "  workflow data normalize --job-file <job.json> [--yes]",
            "  workflow data repair --job-file <job.json> [--yes]",
            "  workflow data repair_loop --job-file <job.json> [--yes]",
            "  workflow data backfill --job-file <job.json> [--yes]",
            "  workflow data redact --job-file <job.json> [--yes]",
            "  workflow data checkpoint --job-file <job.json> [--json]",
            "  workflow data replay --job-file <job.json> [--json]",
            "  workflow data approve --job-file <job.json> [--yes]",
            "  workflow data apply --job-file <job.json> [--yes]",
            "  workflow data validate --job-file <job.json> [--json]",
            "  workflow data transform --job-file <job.json> [--yes]",
            "  workflow data join --job-file <job.json> [--json]",
            "  workflow data merge --job-file <job.json> [--json]",
            "  workflow data aggregate --job-file <job.json> [--json]",
            "  workflow data split --job-file <job.json> [--json]",
            "  workflow data export --job-file <job.json> [--json]",
            "  workflow data dead_letter --job-file <job.json> [--json]",
            "  workflow data dedupe --job-file <job.json> [--yes]",
            "  workflow data reconcile --job-file <job.json> [--json]",
            "  workflow data sync --job-file <job.json> [--yes]",
            "  workflow data run --job-file <job.json> [--yes]",
            "  workflow data spec --job-file <job.json> [--workflow-spec-file <path>] [--yes]",
            "  workflow data launch --job-file <job.json> [--workflow-spec-file <path>] [--wait] [--yes]",
            "",
            "Common flags:",
            "  --job-file <path>           Load a full deterministic data job JSON file",
            "  --job-json '<json>'         Inline full deterministic data job JSON",
            "  --workspace-root <path>     Narrow execution and writes to one repo-local subtree",
            "  --input-file <path>         Primary dataset (csv/json/jsonl/tsv)",
            "  --secondary-input-file      Secondary dataset for reconcile",
            "  --predicates-file <path>    Filter predicate list JSON",
            "  --predicate-mode all|any    Filter predicate combination mode",
            "  --sort-file <path>          Sort spec JSON array",
            "  --rules-file <path>         Normalize rules JSON object",
            "  --repairs-file <path>       Repair patch JSON object",
            "  --backfill-file <path>      Backfill rules JSON object",
            "  --redactions-file <path>    Redaction rules JSON object",
            "  --checkpoint-file <path>    Checkpoint export or prior receipt (import compatibility only)",
            "  --checkpoint-manifest-id <id> Load a checkpoint from the control-plane registry",
            "  --plan-file <path>          Plan export or prior receipt (import compatibility only)",
            "  --plan-manifest-id <id>     Load a plan from the control-plane registry",
            "  --approval-file <path>      Approval export or prior receipt (import compatibility only)",
            "  --approval-manifest-id <id> Load an approval from the control-plane registry",
            "  --schema-file <path>        Validation schema JSON object",
            "  --checks-file <path>        Validation checks JSON array",
            "  --mapping-file <path>       Transform mapping JSON object",
            "  --field-map-file <path>     Export field rename JSON object",
            "  --fields a,b,c              Exported fields in order",
            "  --drop-fields a,b           Remove fields during repair",
            "  --keys a,b,c                Key fields for dedupe/reconcile",
            "  --left-keys a,b             Left key fields for joins",
            "  --right-keys a,b            Right key fields for joins",
            "  --compare-fields a,b        Explicit compare fields for reconcile",
            "  --cursor-field <field>      Cursor field for checkpoint/replay",
            "  --after <value>             Replay rows strictly after this cursor",
            "  --before <value>            Replay rows up to and including this cursor",
            "  --approved-by <name>        Human or system approving a plan",
            "  --approval-reason <text>    Explicit reason for plan approval",
            "  --join-kind <kind>          inner|left|right|full",
            "  --merge-mode <mode>         inner|left|right|full",
            "  --precedence <side>         left|right",
            "  --left-prefix <prefix>      Prefix all left fields in join output",
            "  --right-prefix <prefix>     Prefix all right fields in join output",
            "  --group-by a,b              Aggregate grouping fields",
            "  --aggregations-file <path>  Aggregate spec JSON array",
            "  --split-by-field <field>    Partition rows by one field value",
            "  --partitions-file <path>    Split partition spec JSON array",
            "  --split-mode <mode>         first_match|all_matches",
            "  --exclude-unmatched         Drop rows that match no partition",
            "  --sync-mode <mode>          upsert|mirror",
            "  --max-passes <n>            Maximum repair loop passes",
            "  --batch-size <n>            Sync batch size for resumable upsert",
            "  --output-file <path>        Write resulting records/report",
            "  --receipt-file <path>       Write machine-readable receipt JSON",
            "  --json                      Force raw JSON output",
            "",
            "Examples:",
            "  workflow data profile artifacts/data/users.csv",
            "  workflow data filter --job-file config/data/filter_active_users.json --json",
            "  workflow data join --job-file config/data/join_users_orders.json --json",
            "  workflow data merge --job-file config/data/merge_users_billing.json --json",
            "  workflow data aggregate --job-file config/data/aggregate_orders.json --json",
            "  workflow data checkpoint --job-file config/data/checkpoint_events.json --json",
            "  workflow data replay --job-file config/data/replay_events.json --json",
            "  workflow data replay --checkpoint-manifest-id checkpoint_xyz789 --input-file artifacts/data/events.json --cursor-field updated_at --json",
            "  workflow data approve --job-file config/data/approve_users.json --yes",
            "  workflow data apply --job-file config/data/apply_users.json --yes",
            "  workflow data approve --plan-manifest-id plan_abc123 --approved-by ops --approval-reason 'Reviewed diff' --yes",
            "  workflow data apply --plan-manifest-id plan_abc123 --approval-manifest-id approval_def456 --secondary-input-file artifacts/data/target.json --keys id --yes",
            "  workflow data split --job-file config/data/split_users_by_status.json --yes",
            "  workflow data export --job-file config/data/export_users_public.json --yes",
            "  workflow data normalize --job-file config/data/normalize_users.json --yes",
            "  workflow data repair --job-file config/data/repair_users.json --yes",
            "  workflow data repair_loop --job-file config/data/repair_loop_users.json --yes",
            "  workflow data backfill --job-file config/data/backfill_users.json --yes",
            "  workflow data dead_letter --job-file config/data/dead_letter_users.json --yes",
            "  workflow data reconcile --job-file config/data/reconcile_users.json --json",
            "  workflow data sync --job-file config/data/sync_users.json --yes",
            "  workflow data launch --job-file config/data/dedupe_users.json --yes",
        ]
    )


def _load_json_any(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _load_job_payload(*, job_file: str | None, job_json: str | None) -> dict[str, Any]:
    if job_file and job_json:
        raise ValueError("pass only one of --job-file or --job-json")
    if job_file:
        payload = _load_json_any(job_file)
    elif job_json:
        payload = json.loads(job_json)
    else:
        return {}
    if not isinstance(payload, dict):
        raise ValueError("job payload must be a JSON object")
    return dict(payload)


def _render_confirmation(
    *,
    action: str,
    params: dict[str, Any],
    confirmed: bool,
    stdout: TextIO,
) -> int | None:
    definition = get_definition("praxis_data")
    if definition is None:
        stdout.write("tool definition not found: praxis_data\n")
        return 2

    job_output = {}
    if isinstance(params.get("job"), dict):
        candidate_output = dict(params["job"]).get("output")
        if isinstance(candidate_output, dict):
            job_output = candidate_output
    writes_to_disk = bool(
        params.get("output_path")
        or params.get("receipt_path")
        or params.get("workflow_spec_path")
        or job_output.get("path")
    )
    risk = definition.risk_for_params({"action": action})
    if writes_to_disk and risk == "read":
        risk = "write"
    if risk not in {"write", "dispatch"} or confirmed:
        return None
    for line in tool_preflight_lines(definition, {"action": action}):
        if line.startswith("risk: "):
            stdout.write(f"risk: {risk}\n")
            continue
        stdout.write(line + "\n")
    if writes_to_disk and risk == "write":
        stdout.write("writes: local files under the workspace root\n")
    stdout.write("confirmation required: rerun with --yes\n")
    return 2


def _render_data_payload(action: str, payload: dict[str, Any], *, stdout: TextIO, as_json: bool) -> None:
    if as_json or payload.get("error"):
        print_json(stdout, payload)
        return

    if action == "profile" and isinstance(payload.get("stats"), dict):
        stats = dict(payload["stats"])
        stdout.write(
            f"rows: {stats.get('row_count', 0)}  fields: {stats.get('field_count', 0)}\n"
        )
        for field in stats.get("fields") or []:
            profile = dict(stats.get("field_profiles", {}).get(field) or {})
            inferred = ", ".join(
                f"{name}:{count}" for name, count in dict(profile.get("inferred_types") or {}).items()
            )
            stdout.write(
                f"  {field}: non_empty={profile.get('non_empty_count', 0)} "
                f"distinct={profile.get('distinct_count', 0)} "
                f"types={inferred}\n"
            )
        return

    if action == "validate" and isinstance(payload.get("violations"), list):
        violations = list(payload["violations"])
        stdout.write(f"violations: {len(violations)}\n")
        for violation in violations[:20]:
            field = violation.get("field", "(record)")
            stdout.write(
                f"  row {violation.get('row_index')}: {field} "
                f"{violation.get('code')} - {violation.get('message')}\n"
            )
        if len(violations) > 20:
            stdout.write(f"... {len(violations) - 20} more violation(s)\n")
        return

    if action in {"reconcile", "sync"} and isinstance(payload.get("plan"), dict):
        plan = dict(payload["plan"])
        stats = dict(payload.get("stats") or {})
        line = (
            f"create={stats.get('create_count', 0)} "
            f"update={stats.get('update_count', 0)} "
            f"delete={stats.get('delete_count', 0)} "
            f"noop={stats.get('noop_count', 0)} "
            f"conflicts={stats.get('conflict_count', 0)}"
        )
        if action == "sync":
            line += f" output_rows={stats.get('output_rows', 0)} mode={stats.get('sync_mode', '')}"
            if isinstance(payload.get("batch_manifest"), list):
                line += f" batches={len(list(payload.get('batch_manifest') or []))}"
            if isinstance(payload.get("checkpoint"), dict) and payload["checkpoint"].get("watermark") is not None:
                line += f" watermark={payload['checkpoint'].get('watermark')}"
        stdout.write(line + "\n")
        for key in ("create", "update", "delete", "conflicts"):
            entries = list(plan.get(key) or [])
            if not entries:
                continue
            stdout.write(f"{key}:\n")
            for item in entries[:10]:
                stdout.write(f"  {json.dumps(item, default=str)}\n")
        return

    if action == "checkpoint" and isinstance(payload.get("checkpoint"), dict):
        checkpoint = dict(payload["checkpoint"])
        line = (
            f"rows: {checkpoint.get('row_count', 0)} "
            f"hash={checkpoint.get('content_hash', '')[:12]}"
        )
        if checkpoint.get("cursor_field"):
            line += (
                f" cursor={checkpoint.get('cursor_field')} "
                f"watermark={checkpoint.get('watermark')}"
            )
        checkpoint_manifest_id = str(payload.get("checkpoint_manifest_id") or "").strip()
        if checkpoint_manifest_id:
            line += f" checkpoint_manifest={checkpoint_manifest_id}"
        stdout.write(line + "\n")
        return

    if action == "approve" and isinstance(payload.get("approval"), dict):
        approval = dict(payload["approval"])
        line = f"approved_by={approval.get('approved_by')} plan={str(approval.get('plan_digest') or '')[:12]}"
        plan_manifest_id = str(payload.get("plan_manifest_id") or "").strip()
        if plan_manifest_id:
            line += f" plan_manifest={plan_manifest_id}"
        stdout.write(line + "\n")
        return

    if action in {"split", "dead_letter", "repair_loop"} and isinstance(payload.get("partition_counts"), dict):
        stats = dict(payload.get("stats") or {})
        if action == "dead_letter":
            stdout.write(
                f"accepted={stats.get('accepted_rows', 0)} "
                f"dead_letter={stats.get('dead_letter_rows', 0)} "
                f"violations={stats.get('violation_count', 0)}\n"
            )
        elif action == "repair_loop":
            stdout.write(
                f"passes={stats.get('pass_count', 0)} "
                f"accepted={stats.get('accepted_rows', 0)} "
                f"dead_letter={stats.get('dead_letter_rows', 0)} "
                f"converged={stats.get('converged', False)}\n"
            )
        else:
            stdout.write(
                f"partitions: {stats.get('partition_count', 0)} "
                f"unmatched={stats.get('unmatched_count', 0)} "
                f"mode={stats.get('split_mode', '')}\n"
            )
        for name, count in sorted(dict(payload.get("partition_counts") or {}).items())[:20]:
            stdout.write(f"  {name}: {count}\n")
        if payload.get("output", {}).get("path"):
            stdout.write(f"output: {payload['output']['path']}\n")
        return

    if action in {"parse", "filter", "sort", "normalize", "repair", "backfill", "redact", "replay", "apply", "transform", "join", "merge", "aggregate", "export", "dedupe"} and payload.get("record_count") is not None:
        line = f"records: {payload.get('record_count', 0)}"
        if action == "merge":
            line += f"  conflicts: {len(list(payload.get('conflicts') or []))}"
        if action == "replay" and isinstance(payload.get("replay_window"), dict):
            window = dict(payload["replay_window"])
            line += f"  cursor: {window.get('cursor_field')} after={window.get('after')}"
        if action == "apply" and payload.get("plan_digest"):
            line += f"  plan: {str(payload.get('plan_digest'))[:12]}"
            plan_manifest_id = str(payload.get("plan_manifest_id") or "").strip()
            approval_manifest_id = str(payload.get("approval_manifest_id") or "").strip()
            if plan_manifest_id:
                line += f" plan_manifest={plan_manifest_id}"
            if approval_manifest_id:
                line += f" approval_manifest={approval_manifest_id}"
        stdout.write(line + "\n")
        preview = list(payload.get("records_preview") or [])
        for row in preview[:10]:
            stdout.write(f"  {json.dumps(row, default=str)}\n")
        if payload.get("records_truncated"):
            stdout.write("... preview truncated\n")
        if payload.get("output", {}).get("path"):
            stdout.write(f"output: {payload['output']['path']}\n")
        return

    print_json(stdout, payload)


def _data_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help", "help"}:
        stdout.write(_data_help_text() + "\n")
        return 2

    action = args[0].strip().lower()
    if action == "spec":
        action = "workflow_spec"
    if action not in {
        "parse",
        "profile",
        "filter",
        "sort",
        "normalize",
        "repair",
        "repair_loop",
        "backfill",
        "redact",
        "checkpoint",
        "replay",
        "approve",
        "apply",
        "validate",
        "transform",
        "join",
        "merge",
        "aggregate",
        "split",
        "export",
        "dead_letter",
        "dedupe",
        "reconcile",
        "sync",
        "run",
        "workflow_spec",
        "launch",
    }:
        stdout.write(f"unknown data action: {action}\n")
        stdout.write(_data_help_text() + "\n")
        return 2

    payload: dict[str, Any] = {"action": action}
    job_file = None
    job_json = None
    as_json = False
    confirmed = False
    positionals: list[str] = []

    i = 1
    while i < len(args):
        token = args[i]
        if token == "--job-file" and i + 1 < len(args):
            job_file = args[i + 1]
            i += 2
        elif token == "--job-json" and i + 1 < len(args):
            job_json = args[i + 1]
            i += 2
        elif token == "--input-file" and i + 1 < len(args):
            payload["input_path"] = args[i + 1]
            i += 2
        elif token == "--workspace-root" and i + 1 < len(args):
            payload["workspace_root"] = args[i + 1]
            i += 2
        elif token == "--input-format" and i + 1 < len(args):
            payload["input_format"] = args[i + 1]
            i += 2
        elif token == "--secondary-input-file" and i + 1 < len(args):
            payload["secondary_input_path"] = args[i + 1]
            i += 2
        elif token == "--secondary-input-format" and i + 1 < len(args):
            payload["secondary_input_format"] = args[i + 1]
            i += 2
        elif token == "--predicates-file" and i + 1 < len(args):
            payload["predicates"] = _load_json_any(args[i + 1])
            i += 2
        elif token == "--predicate-mode" and i + 1 < len(args):
            payload["predicate_mode"] = args[i + 1]
            i += 2
        elif token == "--sort-file" and i + 1 < len(args):
            payload["sort"] = _load_json_any(args[i + 1])
            i += 2
        elif token == "--rules-file" and i + 1 < len(args):
            payload["rules"] = load_json_file(args[i + 1])
            i += 2
        elif token == "--repairs-file" and i + 1 < len(args):
            payload["repairs"] = load_json_file(args[i + 1])
            i += 2
        elif token == "--backfill-file" and i + 1 < len(args):
            payload["backfill"] = load_json_file(args[i + 1])
            i += 2
        elif token == "--redactions-file" and i + 1 < len(args):
            payload["redactions"] = load_json_file(args[i + 1])
            i += 2
        elif token == "--checkpoint-file" and i + 1 < len(args):
            payload["checkpoint_path"] = args[i + 1]
            i += 2
        elif token == "--checkpoint-manifest-id" and i + 1 < len(args):
            payload["checkpoint_manifest_id"] = args[i + 1]
            i += 2
        elif token == "--plan-file" and i + 1 < len(args):
            payload["plan_path"] = args[i + 1]
            i += 2
        elif token == "--plan-manifest-id" and i + 1 < len(args):
            payload["plan_manifest_id"] = args[i + 1]
            i += 2
        elif token == "--approval-file" and i + 1 < len(args):
            payload["approval_path"] = args[i + 1]
            i += 2
        elif token == "--approval-manifest-id" and i + 1 < len(args):
            payload["approval_manifest_id"] = args[i + 1]
            i += 2
        elif token == "--schema-file" and i + 1 < len(args):
            payload["schema"] = load_json_file(args[i + 1])
            i += 2
        elif token == "--checks-file" and i + 1 < len(args):
            payload["checks"] = _load_json_any(args[i + 1])
            i += 2
        elif token == "--mapping-file" and i + 1 < len(args):
            payload["mapping"] = load_json_file(args[i + 1])
            i += 2
        elif token == "--field-map-file" and i + 1 < len(args):
            payload["field_map"] = load_json_file(args[i + 1])
            i += 2
        elif token == "--fields" and i + 1 < len(args):
            payload["fields"] = [part.strip() for part in args[i + 1].split(",") if part.strip()]
            i += 2
        elif token == "--drop-fields" and i + 1 < len(args):
            payload["drop_fields"] = [part.strip() for part in args[i + 1].split(",") if part.strip()]
            i += 2
        elif token == "--keys" and i + 1 < len(args):
            payload["keys"] = [part.strip() for part in args[i + 1].split(",") if part.strip()]
            i += 2
        elif token == "--left-keys" and i + 1 < len(args):
            payload["left_keys"] = [part.strip() for part in args[i + 1].split(",") if part.strip()]
            i += 2
        elif token == "--right-keys" and i + 1 < len(args):
            payload["right_keys"] = [part.strip() for part in args[i + 1].split(",") if part.strip()]
            i += 2
        elif token == "--compare-fields" and i + 1 < len(args):
            payload["compare_fields"] = [part.strip() for part in args[i + 1].split(",") if part.strip()]
            i += 2
        elif token == "--cursor-field" and i + 1 < len(args):
            payload["cursor_field"] = args[i + 1]
            i += 2
        elif token == "--after" and i + 1 < len(args):
            payload["after"] = args[i + 1]
            i += 2
        elif token == "--before" and i + 1 < len(args):
            payload["before"] = args[i + 1]
            i += 2
        elif token == "--approved-by" and i + 1 < len(args):
            payload["approved_by"] = args[i + 1]
            i += 2
        elif token == "--approval-reason" and i + 1 < len(args):
            payload["approval_reason"] = args[i + 1]
            i += 2
        elif token == "--join-kind" and i + 1 < len(args):
            payload["join_kind"] = args[i + 1]
            i += 2
        elif token == "--merge-mode" and i + 1 < len(args):
            payload["merge_mode"] = args[i + 1]
            i += 2
        elif token == "--precedence" and i + 1 < len(args):
            payload["precedence"] = args[i + 1]
            i += 2
        elif token == "--left-prefix" and i + 1 < len(args):
            payload["left_prefix"] = args[i + 1]
            i += 2
        elif token == "--right-prefix" and i + 1 < len(args):
            payload["right_prefix"] = args[i + 1]
            i += 2
        elif token == "--group-by" and i + 1 < len(args):
            payload["group_by"] = [part.strip() for part in args[i + 1].split(",") if part.strip()]
            i += 2
        elif token == "--aggregations-file" and i + 1 < len(args):
            payload["aggregations"] = _load_json_any(args[i + 1])
            i += 2
        elif token == "--split-by-field" and i + 1 < len(args):
            payload["split_by_field"] = args[i + 1]
            i += 2
        elif token == "--partitions-file" and i + 1 < len(args):
            payload["partitions"] = _load_json_any(args[i + 1])
            i += 2
        elif token == "--split-mode" and i + 1 < len(args):
            payload["split_mode"] = args[i + 1]
            i += 2
        elif token == "--exclude-unmatched":
            payload["include_unmatched"] = False
            i += 1
        elif token == "--strategy" and i + 1 < len(args):
            payload["strategy"] = args[i + 1]
            i += 2
        elif token == "--order-field" and i + 1 < len(args):
            payload["order_field"] = args[i + 1]
            i += 2
        elif token == "--sync-mode" and i + 1 < len(args):
            payload["sync_mode"] = args[i + 1]
            i += 2
        elif token == "--max-passes" and i + 1 < len(args):
            payload["max_passes"] = int(args[i + 1])
            i += 2
        elif token == "--batch-size" and i + 1 < len(args):
            payload["batch_size"] = int(args[i + 1])
            i += 2
        elif token == "--output-file" and i + 1 < len(args):
            payload["output_path"] = args[i + 1]
            i += 2
        elif token == "--output-format" and i + 1 < len(args):
            payload["output_format"] = args[i + 1]
            i += 2
        elif token == "--receipt-file" and i + 1 < len(args):
            payload["receipt_path"] = args[i + 1]
            i += 2
        elif token == "--workflow-spec-file" and i + 1 < len(args):
            payload["workflow_spec_path"] = args[i + 1]
            i += 2
        elif token == "--wait":
            payload["wait"] = True
            i += 1
        elif token == "--dry-run":
            payload["dry_run"] = True
            i += 1
        elif token == "--json":
            as_json = True
            i += 1
        elif token == "--yes":
            confirmed = True
            i += 1
        else:
            positionals.append(token)
            i += 1

    if positionals and "input_path" not in payload and action in {"parse", "profile"}:
        payload["input_path"] = positionals[0]
        positionals = positionals[1:]
    if positionals:
        stdout.write(f"unexpected arguments: {' '.join(positionals)}\n")
        return 2

    try:
        job = _load_job_payload(job_file=job_file, job_json=job_json)
    except ValueError as exc:
        stdout.write(f"{exc}\n")
        return 2

    if job:
        payload["job"] = job

    confirmation_action = action
    if action == "run":
        operation = ""
        if isinstance(job, dict):
            operation = str(job.get("operation") or "").strip().lower()
        if operation:
            confirmation_action = operation
    confirmation_result = _render_confirmation(
        action=confirmation_action,
        params=payload,
        confirmed=confirmed,
        stdout=stdout,
    )
    if confirmation_result is not None:
        return confirmation_result

    exit_code, result = run_cli_tool("praxis_data", payload)
    _render_data_payload(confirmation_action, result, stdout=stdout, as_json=as_json)
    return exit_code


__all__ = ["_data_command"]
