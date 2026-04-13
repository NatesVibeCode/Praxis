"""Workflow-oriented CLI command handlers."""

from __future__ import annotations

import os
from typing import TextIO


def _workflow_runtime_conn():
    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

    return SyncPostgresConnection(get_workflow_pool())


def _run_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow run <spec.json>` or `workflow run -p <prompt>`.

    Automatically detects batch specs (kind=workflow_batch) and runs
    all jobs in parallel, printing each result as a JSON line followed by a
    summary object.
    """

    import json as _json
    import time as _time

    from runtime.workflow import (
        WorkflowSpec,
        run_workflow,
        run_workflow_batch_from_file,
        run_workflow_from_spec_file,
    )
    from runtime.workflow_spec import is_batch_spec, load_raw

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow run <spec.json> [--var key=value ...]\n"
            "       workflow run -p <prompt> [options]\n"
            "\n"
            "Run a build task through the platform.\n"
            "The graph compiles context, pipes it to the model via stdin,\n"
            "captures structured output, and writes files.\n"
            "\n"
            "examples:\n"
            '  workflow run -p "add a farewell function" \\\n'
            "    --write greeting.py --workdir .\n"
            "\n"
            '  workflow run -p "refactor this module" \\\n'
            "    --provider google --model gemini-2.5-flash \\\n"
            "    --write runtime/domain.py --workdir .\n"
            "\n"
            '  workflow run -p "review this code" \\\n'
            "    --context src/main.py,src/utils.py \\\n"
            "    --task-type code_review\n"
        )
        return 2

    variables: dict[str, str] = {}
    filtered_args: list[str] = []
    i = 0
    while i < len(args):
        if args[i] == "--var" and i + 1 < len(args):
            kv = args[i + 1]
            eq_pos = kv.find("=")
            if eq_pos < 1:
                stdout.write(f"error: --var value must be key=value, got: {kv}\n")
                return 2
            variables[kv[:eq_pos]] = kv[eq_pos + 1 :]
            i += 2
        else:
            filtered_args.append(args[i])
            i += 1
    args = filtered_args
    variables = variables or None

    if args[0] in {"-p", "--prompt"}:
        if len(args) < 2:
            stdout.write(
                "usage: workflow run -p <prompt> [options]\n"
                "\n"
                "options:\n"
                "  --provider <slug>    Provider: anthropic, openai, google\n"
                "  --model <slug>       Model slug (provider-specific)\n"
                "  --tier <tier>        Route by tier: frontier, mid, economy\n"
                "  --write <paths>      Files the model should modify (comma-separated)\n"
                "  --workdir <dir>      Workspace root for file reads/writes\n"
                "  --context <paths>    Extra files to inject as context (comma-separated)\n"
                "  --timeout <secs>     Execution timeout (default: 300)\n"
                "  --task-type <type>   Task type for routing: code_generation, review, etc.\n"
                "  --system <prompt>    System prompt override\n"
                "  --dry-run            Parse and show the spec without executing\n"
            )
            return 2
        provider = "anthropic"
        model = None
        tier = None
        adapter = "cli_llm"
        scope_write = None
        workdir = None
        context_files = None
        timeout = 300
        task_type = None
        system_prompt = None
        dry_run = False
        clean_args = []
        i = 1
        while i < len(args):
            if args[i] == "--provider" and i + 1 < len(args):
                provider = args[i + 1]
                i += 2
            elif args[i] == "--model" and i + 1 < len(args):
                model = args[i + 1]
                i += 2
            elif args[i] == "--tier" and i + 1 < len(args):
                tier = args[i + 1]
                i += 2
            elif args[i] == "--adapter" and i + 1 < len(args):
                adapter = args[i + 1]
                i += 2
            elif args[i] == "--write" and i + 1 < len(args):
                scope_write = [p.strip() for p in args[i + 1].split(",") if p.strip()]
                i += 2
            elif args[i] == "--workdir" and i + 1 < len(args):
                workdir = args[i + 1]
                i += 2
            elif args[i] == "--context" and i + 1 < len(args):
                context_files = [p.strip() for p in args[i + 1].split(",") if p.strip()]
                i += 2
            elif args[i] == "--timeout" and i + 1 < len(args):
                timeout = int(args[i + 1])
                i += 2
            elif args[i] == "--task-type" and i + 1 < len(args):
                task_type = args[i + 1]
                i += 2
            elif args[i] == "--system" and i + 1 < len(args):
                system_prompt = args[i + 1]
                i += 2
            elif args[i] == "--dry-run":
                dry_run = True
                i += 1
            else:
                clean_args.append(args[i])
                i += 1
        prompt = " ".join(clean_args)

        if scope_write and not workdir:
            import os as _os

            workdir = _os.getcwd()

        context_sections = None
        if context_files or scope_write:
            context_sections = []
            all_paths = list(context_files or []) + list(scope_write or [])
            for fpath in all_paths:
                import os as _os

                abs_path = _os.path.join(workdir or ".", fpath)
                try:
                    with open(abs_path) as fh:
                        content = fh.read()
                    context_sections.append(
                        {
                            "name": f"FILE: {fpath}",
                            "content": content,
                        }
                    )
                except OSError:
                    pass

        if scope_write:
            prompt += (
                "\n\nReturn your response as JSON with this schema:\n"
                '{"code_blocks": [{"file_path": "<path>", '
                '"content": "<FULL FILE>", "language": "python", '
                '"action": "replace"}], '
                '"explanation": "<what you changed>"}'
            )
            if not system_prompt:
                system_prompt = "You are a code editor. Return ONLY valid JSON structured output."

        spec = WorkflowSpec(
            prompt=prompt,
            provider_slug=provider,
            model_slug=model,
            tier=tier,
            adapter_type=adapter,
            timeout=timeout,
            workdir=workdir,
            scope_write=scope_write,
            context_sections=context_sections,
            system_prompt=system_prompt,
            task_type=task_type,
            persist=True,
        )

        if dry_run:
            stdout.write(
                _json.dumps(
                    {
                        "kind": "workflow_spec_preview",
                        "provider_slug": spec.provider_slug,
                        "model_slug": spec.model_slug,
                        "tier": spec.tier,
                        "adapter_type": spec.adapter_type,
                        "timeout": spec.timeout,
                        "workdir": spec.workdir,
                        "scope_write": spec.scope_write,
                        "context_sections_count": len(spec.context_sections) if spec.context_sections else 0,
                        "prompt_preview": spec.prompt[:200],
                        "system_prompt_preview": (spec.system_prompt or "")[:200],
                    },
                    indent=2,
                )
                + "\n"
            )
            return 0

        result = run_workflow(spec)
        stdout.write(_json.dumps(result.to_json(), indent=2) + "\n")
        return 0 if result.status == "succeeded" else 1

    spec_path = args[0]

    try:
        raw = load_raw(spec_path)
    except (OSError, _json.JSONDecodeError) as exc:
        stdout.write(f"error: could not read spec file: {exc}\n")
        return 2

    if is_batch_spec(raw):
        wall_start = _time.monotonic_ns()
        try:
            results = run_workflow_batch_from_file(spec_path, variables=variables)
        except ValueError as exc:
            stdout.write(f"error: {exc}\n")
            return 2
        wall_ms = (_time.monotonic_ns() - wall_start) // 1_000_000

        succeeded = sum(1 for r in results if r.status == "succeeded")
        failed = len(results) - succeeded

        for result in results:
            stdout.write(_json.dumps(result.to_json()) + "\n")

        summary = {
            "kind": "batch_summary",
            "total": len(results),
            "succeeded": succeeded,
            "failed": failed,
            "wall_clock_ms": wall_ms,
        }
        stdout.write(_json.dumps(summary) + "\n")
        return 0 if failed == 0 else 1

    try:
        result = run_workflow_from_spec_file(spec_path, variables=variables)
    except ValueError as exc:
        stdout.write(f"error: {exc}\n")
        return 2

    stdout.write(_json.dumps(result.to_json(), indent=2) + "\n")
    return 0 if result.status == "succeeded" else 1


def _chain_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow chain <spec1.json> <spec2.json> ...`.

    Submits multiple specs as a sequential chain where each job depends
    on the previous one succeeding. Returns JSON array of job_ids.
    """

    import json as _json

    from runtime.workflow_spec import load_workflow_spec
    from runtime.job_dependencies import submit_chain

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow chain <spec1.json> <spec2.json> ...\n"
            "\n"
            "Submit multiple specs as a sequential job chain.\n"
            "Each job depends on the previous one succeeding.\n"
        )
        return 2

    if len(args) < 2:
        stdout.write("error: chain requires at least 2 spec files\n")
        return 2

    specs = []
    for spec_path in args:
        try:
            spec = load_workflow_spec(spec_path)
            specs.append(spec)
        except FileNotFoundError:
            stdout.write(f"error: spec file not found: {spec_path}\n")
            return 1
        except ValueError as exc:
            stdout.write(f"error: invalid spec {spec_path}: {exc}\n")
            return 1

    try:
        job_ids = submit_chain(specs, sequential=True)
    except Exception as exc:
        stdout.write(f"error: failed to submit chain: {exc}\n")
        return 1

    stdout.write(_json.dumps(job_ids, indent=2) + "\n")
    return 0


def _status_command(*, stdout: TextIO) -> int:
    """Handle `workflow status` — print recent workflow summary as JSON."""

    import json as _json

    from runtime.workflow_status import get_workflow_history

    history = get_workflow_history()
    stdout.write(_json.dumps(history.summary(), indent=2) + "\n")
    return 0


def _proof_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow proof` — inspect or backfill proof completeness."""

    import json as _json

    from runtime.post_workflow_sync import backfill_workflow_proof
    from runtime.receipt_store import proof_metrics

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow proof [--since-hours <hours>] [--run-id <run_id>] [--limit <n>] [--backfill]\n"
        )
        return 2

    run_id = None
    limit = None
    since_hours = 0
    backfill = False
    i = 0
    while i < len(args):
        if args[i] == "--run-id" and i + 1 < len(args):
            run_id = args[i + 1]
            i += 2
        elif args[i] == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1])
            i += 2
        elif args[i] == "--since-hours" and i + 1 < len(args):
            since_hours = int(args[i + 1])
            i += 2
        elif args[i] == "--backfill":
            backfill = True
            i += 1
        else:
            stdout.write(f"unknown proof argument: {args[i]}\n")
            return 2

    payload = (
        backfill_workflow_proof(run_id=run_id, limit=limit)
        if backfill
        else proof_metrics(since_hours=since_hours)
    )
    stdout.write(_json.dumps(payload, indent=2, default=str) + "\n")
    return 0


def _verify_platform_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow verify-platform` — run platform verifiers or list authority."""

    import json as _json

    from runtime.verifier_authority import registry_snapshot, run_registered_verifier

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow verify-platform [--verifier-ref <ref>] [--inputs-json <json>] "
            "[--target-kind <kind>] [--target-ref <ref>]\n"
        )
        return 2

    verifier_ref = None
    target_kind = "platform"
    target_ref = ""
    inputs: dict[str, object] = {}
    i = 0
    while i < len(args):
        if args[i] == "--verifier-ref" and i + 1 < len(args):
            verifier_ref = args[i + 1]
            i += 2
        elif args[i] == "--inputs-json" and i + 1 < len(args):
            try:
                parsed = _json.loads(args[i + 1])
            except _json.JSONDecodeError as exc:
                stdout.write(f"invalid --inputs-json: {exc}\n")
                return 2
            if not isinstance(parsed, dict):
                stdout.write("--inputs-json must decode to an object\n")
                return 2
            inputs = dict(parsed)
            i += 2
        elif args[i] == "--target-kind" and i + 1 < len(args):
            target_kind = args[i + 1]
            i += 2
        elif args[i] == "--target-ref" and i + 1 < len(args):
            target_ref = args[i + 1]
            i += 2
        else:
            stdout.write(f"unknown verify-platform argument: {args[i]}\n")
            return 2

    payload = (
        run_registered_verifier(
            verifier_ref,
            inputs=inputs,
            target_kind=target_kind,
            target_ref=target_ref,
        )
        if verifier_ref
        else registry_snapshot()
    )
    stdout.write(_json.dumps(payload, indent=2, default=str) + "\n")
    return 0 if not verifier_ref or payload.get("status") == "passed" else 1


def _heal_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow heal` — run a registered healer against a verifier."""

    import json as _json

    from runtime.verifier_authority import registry_snapshot, run_registered_healer

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow heal --verifier-ref <ref> [--healer-ref <ref>] [--inputs-json <json>] "
            "[--target-kind <kind>] [--target-ref <ref>]\n"
        )
        return 2

    healer_ref = None
    verifier_ref = None
    target_kind = "platform"
    target_ref = ""
    inputs: dict[str, object] = {}
    i = 0
    while i < len(args):
        if args[i] == "--healer-ref" and i + 1 < len(args):
            healer_ref = args[i + 1]
            i += 2
        elif args[i] == "--verifier-ref" and i + 1 < len(args):
            verifier_ref = args[i + 1]
            i += 2
        elif args[i] == "--inputs-json" and i + 1 < len(args):
            try:
                parsed = _json.loads(args[i + 1])
            except _json.JSONDecodeError as exc:
                stdout.write(f"invalid --inputs-json: {exc}\n")
                return 2
            if not isinstance(parsed, dict):
                stdout.write("--inputs-json must decode to an object\n")
                return 2
            inputs = dict(parsed)
            i += 2
        elif args[i] == "--target-kind" and i + 1 < len(args):
            target_kind = args[i + 1]
            i += 2
        elif args[i] == "--target-ref" and i + 1 < len(args):
            target_ref = args[i + 1]
            i += 2
        else:
            stdout.write(f"unknown heal argument: {args[i]}\n")
            return 2

    if not verifier_ref:
        stdout.write(_json.dumps(registry_snapshot(), indent=2, default=str) + "\n")
        return 2

    payload = run_registered_healer(
        healer_ref=healer_ref,
        verifier_ref=verifier_ref,
        inputs=inputs,
        target_kind=target_kind,
        target_ref=target_ref,
    )
    stdout.write(_json.dumps(payload, indent=2, default=str) + "\n")
    return 0 if payload.get("status") == "succeeded" else 1


def _diagnose_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow diagnose <run_id>`."""

    import json as _json

    from runtime.workflow_diagnose import diagnose_run

    if not args or args[0] in {"-h", "--help"}:
        stdout.write("usage: workflow diagnose <run_id>\n")
        return 2

    run_id = args[0]
    diagnosis = diagnose_run(run_id)
    stdout.write(_json.dumps(diagnosis, indent=2) + "\n")
    return 0 if diagnosis.get("status") == "succeeded" else 1


def _verify_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow verify <receipt_id>` — re-run verify commands from a receipt."""

    import json as _json

    from runtime.receipt_store import find_receipt_by_run_id, load_receipt
    from runtime.verification import resolve_verify_commands, run_verify, summarize_verification
    from storage.postgres.connection import ensure_postgres_available

    if not args or args[0] in {"-h", "--help"}:
        stdout.write("usage: workflow verify <receipt_id_or_run_id>\n")
        return 2

    receipt_ref = args[0]
    rec = load_receipt(receipt_ref)
    if rec is None:
        rec = find_receipt_by_run_id(receipt_ref)
    if rec is None:
        stdout.write(f"receipt not found: {receipt_ref}\n")
        return 1
    receipt = rec.to_dict()

    verify_bindings = receipt.get("verify_refs")
    if not verify_bindings:
        stdout.write("no verify refs found in receipt\n")
        return 1

    conn = ensure_postgres_available()
    verify_cmds = resolve_verify_commands(conn, verify_bindings)
    workdir = receipt.get("workdir")
    results = run_verify(verify_cmds, workdir=workdir)
    summary = summarize_verification(results)

    stdout.write(_json.dumps(summary.to_json(), indent=2) + "\n")
    return 0 if summary.all_passed else 1


def _pipeline_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow pipeline <pipeline.json>`.

    The JSON file should contain::

        {
            "steps": [
                {"name": "step1", "prompt": "Do thing 1"},
                {"name": "step2", "prompt": "Do thing 2", "depends_on": ["step1"]},
                ...
            ]
        }

    Each step object supports: name (required), prompt (required),
    adapter_type, provider_slug, model_slug, tier, max_tokens, depends_on.
    """

    import json as _json

    from runtime.workflow import run_workflow_pipeline
    from runtime.workflow_builder import WorkflowStep

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow pipeline <pipeline.json>\n"
            "\n"
            "The JSON file should contain:\n"
            '  {"steps": [{"name": "...", "prompt": "..."}, ...]}\n'
        )
        return 2

    spec_path = args[0]

    try:
        with open(spec_path) as fh:
            raw = _json.load(fh)
    except (OSError, _json.JSONDecodeError) as exc:
        stdout.write(f"error: could not read pipeline file: {exc}\n")
        return 2

    if not isinstance(raw, dict) or "steps" not in raw:
        stdout.write('error: pipeline file must contain a "steps" array\n')
        return 2

    raw_steps = raw["steps"]
    if not isinstance(raw_steps, list) or len(raw_steps) < 1:
        stdout.write("error: pipeline must have at least one step\n")
        return 2

    try:
        steps = [
            WorkflowStep(
                name=step["name"],
                prompt=step["prompt"],
                adapter_type=step.get("adapter_type", "cli_llm"),
                provider_slug=step.get("provider_slug"),
                model_slug=step.get("model_slug"),
                tier=step.get("tier"),
                max_tokens=step.get("max_tokens", 4096),
                depends_on=tuple(step.get("depends_on", ())),
                fan_out=step.get("fan_out", False),
                fan_out_prompt=step.get("fan_out_prompt"),
                fan_out_max_parallel=step.get("fan_out_max_parallel", 4),
            )
            for step in raw_steps
        ]
    except (KeyError, TypeError) as exc:
        stdout.write(f"error: invalid step definition: {exc}\n")
        return 2

    result = run_workflow_pipeline(steps)
    stdout.write(_json.dumps(result.to_json(), indent=2) + "\n")
    return 0 if result.status == "succeeded" else 1


def _scheduler_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow scheduler <status|tick|run> [args]`."""

    import json as _json

    from runtime.scheduler import (
        SchedulerConfig,
        force_run_job,
        run_scheduler_tick,
        scheduler_status,
    )

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow scheduler <status|tick|run> [args]\n"
            "\n"
            "  status            show all scheduled jobs and their last run time\n"
            "  tick              run one scheduler tick (check all jobs due)\n"
            "  tick --dry-run    show which jobs would fire without executing\n"
            "  run <job_name>    force-run a specific job immediately\n"
        )
        return 2

    subcommand = args[0]

    if subcommand == "status":
        try:
            config = SchedulerConfig.load()
        except (OSError, ValueError) as exc:
            stdout.write(f"error loading scheduler config: {exc}\n")
            return 1
        rows = scheduler_status(config)
        stdout.write(_json.dumps(rows, indent=2) + "\n")
        return 0

    if subcommand == "tick":
        dry_run = "--dry-run" in args
        try:
            config = SchedulerConfig.load()
        except (OSError, ValueError) as exc:
            stdout.write(f"error loading scheduler config: {exc}\n")
            return 1
        results = run_scheduler_tick(config, dry_run=dry_run)
        if not results:
            stdout.write("no jobs due\n")
            return 0
        for result in results:
            stdout.write(_json.dumps(result) + "\n")
        return 0

    if subcommand == "run":
        if len(args) < 2:
            stdout.write("usage: workflow scheduler run <job_name>\n")
            return 2
        job_name = args[1]
        try:
            config = SchedulerConfig.load()
        except (OSError, ValueError) as exc:
            stdout.write(f"error loading scheduler config: {exc}\n")
            return 1
        result = force_run_job(job_name, config)
        stdout.write(_json.dumps(result, indent=2) + "\n")
        return 0 if result.get("status") == "succeeded" else 1

    stdout.write(f"unknown scheduler subcommand: {subcommand}\n")
    return 2


def _fan_out_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow fan-out --items "a,b,c" --prompt "Do X with {{item}}"`.

    Runs one spec per item in parallel, prints each result as a
    JSON line, then prints a summary object.
    """

    import json as _json

    from runtime.fan_out import aggregate_fan_out_results, fan_out_dispatch

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            'usage: workflow fan-out --items "a,b,c" --prompt "Analyze: {{item}}"'
            " [--tier mid] [--max-parallel 4]\n"
        )
        return 2

    items_raw: str | None = None
    prompt: str | None = None
    tier = "mid"
    max_parallel = 4

    i = 0
    while i < len(args):
        if args[i] == "--items" and i + 1 < len(args):
            items_raw = args[i + 1]
            i += 2
        elif args[i] == "--prompt" and i + 1 < len(args):
            prompt = args[i + 1]
            i += 2
        elif args[i] == "--tier" and i + 1 < len(args):
            tier = args[i + 1]
            i += 2
        elif args[i] == "--max-parallel" and i + 1 < len(args):
            max_parallel = int(args[i + 1])
            i += 2
        else:
            stdout.write(f"unknown argument: {args[i]}\n")
            return 2

    if items_raw is None:
        stdout.write("error: --items is required\n")
        return 2
    if prompt is None:
        stdout.write("error: --prompt is required\n")
        return 2

    try:
        items = _json.loads(items_raw)
        if not isinstance(items, list):
            items = [item.strip() for item in items_raw.split(",") if item.strip()]
    except (_json.JSONDecodeError, ValueError):
        items = [item.strip() for item in items_raw.split(",") if item.strip()]

    if not items:
        stdout.write("error: --items produced an empty list\n")
        return 2

    results = fan_out_dispatch(
        items,
        prompt_template=prompt,
        tier=tier,
        max_parallel=max_parallel,
    )

    for result in results:
        stdout.write(_json.dumps(result.to_json()) + "\n")

    summary = aggregate_fan_out_results(results)
    stdout.write(_json.dumps(summary) + "\n")
    return 0 if summary["failed"] == 0 else 1


def _debate_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow debate "topic" [--personas N] [--rounds N]`.

    Runs a structured multi-perspective debate on the given topic using
    default personas (Pragmatist, Skeptic, Innovator, Operator).
    """

    import json as _json

    from runtime.debate_workflow import DebateConfig, default_personas, run_debate
    from storage.postgres.connection import ensure_postgres_available

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow debate <topic> [--personas N] [--rounds N] [--tier mid]\n"
            "       workflow debate <topic> [--custom-personas name:perspective ...]\n"
            "\n"
            "Default personas: Pragmatist, Skeptic, Innovator, Operator\n"
            "Example: workflow debate 'Should we rewrite the auth system?'\n"
        )
        return 2

    topic = args[0] if args else None
    if not topic:
        stdout.write("error: topic is required\n")
        return 2

    num_personas = 4
    num_rounds = 1
    tier = "mid"
    custom_personas_raw: list[str] = []

    i = 1
    while i < len(args):
        if args[i] == "--personas" and i + 1 < len(args):
            try:
                num_personas = int(args[i + 1])
            except ValueError:
                stdout.write(f"error: --personas must be an integer, got {args[i + 1]}\n")
                return 2
            i += 2
        elif args[i] == "--rounds" and i + 1 < len(args):
            try:
                num_rounds = int(args[i + 1])
            except ValueError:
                stdout.write(f"error: --rounds must be an integer, got {args[i + 1]}\n")
                return 2
            i += 2
        elif args[i] == "--tier" and i + 1 < len(args):
            tier = args[i + 1]
            i += 2
        elif args[i] == "--custom-personas" and i + 1 < len(args):
            custom_personas_raw.append(args[i + 1])
            i += 2
        else:
            stdout.write(f"unknown argument: {args[i]}\n")
            return 2

    if custom_personas_raw:
        from runtime.debate_workflow import PersonaDefinition

        personas = []
        for raw in custom_personas_raw:
            if ":" not in raw:
                stdout.write(f"error: custom persona format is 'Name:perspective', got {raw}\n")
                return 2
            name, perspective = raw.split(":", 1)
            personas.append(PersonaDefinition(name=name.strip(), perspective=perspective.strip()))
    else:
        all_personas = default_personas()
        personas = all_personas[:num_personas]

    config = DebateConfig(
        topic=topic,
        personas=personas,
        rounds=num_rounds,
        tier=tier,
    )

    metrics_conn = None
    try:
        metrics_conn = ensure_postgres_available(
            env={
                "WORKFLOW_DATABASE_URL": os.environ["WORKFLOW_DATABASE_URL"]
            }
        )
    except Exception:
        metrics_conn = None

    try:
        result = run_debate(config, metrics_conn=metrics_conn)
    except Exception as exc:
        stdout.write(f"error: debate failed: {exc}\n")
        return 1

    output = {
        "status": result.status,
        "topic": result.topic,
        "personas": list(result.persona_responses.keys()),
        "persona_responses": result.persona_responses,
        "synthesis": result.synthesis,
    }

    stdout.write(_json.dumps(output, indent=2) + "\n")
    return 0 if result.status == "succeeded" else 1


def _runs_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow runs [run_id]` — query workflow runs from Postgres."""

    import json as _json

    if args and args[0] in {"-h", "--help"}:
        stdout.write("usage: workflow runs [<run_id>]\n")
        return 2

    try:
        from runtime.persistent_evidence import query_recent_runs, query_run_detail
    except ImportError as exc:
        stdout.write(f"error: persistent evidence module not available: {exc}\n")
        return 1

    if args:
        run_id = args[0]
        try:
            detail = query_run_detail(run_id)
        except Exception as exc:
            stdout.write(f"error: failed to query run: {exc}\n")
            return 1
        if detail is None:
            stdout.write(f"run not found: {run_id}\n")
            return 1
        stdout.write(_json.dumps(detail, indent=2) + "\n")
        return 0

    try:
        runs = query_recent_runs(limit=20)
    except Exception as exc:
        stdout.write(f"error: failed to query runs: {exc}\n")
        return 1

    if not runs:
        stdout.write("no workflow runs found\n")
        return 0

    stdout.write(_json.dumps(runs, indent=2) + "\n")
    return 0


def _cancel_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow cancel <run_id>` — cancel an in-flight workflow."""
    from contextlib import redirect_stdout
    from io import StringIO
    from types import SimpleNamespace

    from surfaces.cli import workflow_cli

    if not args or args[0] in {"-h", "--help"}:
        stdout.write("usage: workflow cancel <run_id>\n")
        return 2

    run_id = args[0]
    buffer = StringIO()
    with redirect_stdout(buffer):
        exit_code = workflow_cli.cmd_cancel(SimpleNamespace(run_id=run_id))
    stdout.write(buffer.getvalue())
    return exit_code


def _active_command(*, stdout: TextIO) -> int:
    """Handle `workflow active` — list currently running workflows."""

    import json as _json

    from runtime.run_control import get_run_control

    run_control = get_run_control()
    active_ids = run_control.active_run_ids()

    stdout.write(
        _json.dumps(
            {
                "active_runs": active_ids,
                "count": len(active_ids),
            },
            indent=2,
        )
        + "\n"
    )
    return 0


def _queue_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow queue <submit|stats|list|worker|cancel> [args]`.

    Subcommands
    -----------
    submit <spec.json> [--priority N] [--max-attempts N]
        Submit a workflow spec file to the job queue.

    stats
        Print queue statistics (counts per status) as JSON.

    list [--status pending] [--limit 50]
        List queued jobs, optionally filtered by status.

    worker [--max-concurrent 4] [--poll-interval 2.0] [--capabilities a,b]
        Start the workflow worker that polls and executes jobs.

    cancel <job_id>
        Cancel a pending or claimed job.
    """

    import json as _json
    import os

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow queue <submit|stats|list|worker|cancel> [args]\n"
            "\n"
            "  submit <spec.json> [--priority N] [--max-attempts N]\n"
            "  stats\n"
            "  list [--status pending] [--limit 50]\n"
            "  worker [--max-concurrent 4] [--poll-interval 2.0] [--capabilities a,b]\n"
            "  cancel <job_id>\n"
        )
        return 2

    subcommand = args[0]
    sub_args = args[1:]

    if subcommand == "submit":
        if not sub_args or sub_args[0] in {"-h", "--help"}:
            stdout.write(
                "usage: workflow queue submit <spec.json> [--priority N] [--max-attempts N]\n"
            )
            return 2

        from storage.postgres.connection import SyncPostgresConnection
        from runtime.workflow_spec import WorkflowSpec
        from runtime.workflow.unified import submit_workflow_inline

        spec_path = sub_args[0]
        priority = 100
        max_attempts = 3

        i = 1
        while i < len(sub_args):
            if sub_args[i] == "--priority" and i + 1 < len(sub_args):
                try:
                    priority = int(sub_args[i + 1])
                except ValueError:
                    stdout.write(
                        f"error: --priority must be an integer, got: {sub_args[i + 1]}\n"
                    )
                    return 2
                i += 2
            elif sub_args[i] == "--max-attempts" and i + 1 < len(sub_args):
                try:
                    max_attempts = int(sub_args[i + 1])
                except ValueError:
                    stdout.write(
                        f"error: --max-attempts must be an integer, got: {sub_args[i + 1]}\n"
                    )
                    return 2
                i += 2
            else:
                stdout.write(f"error: unknown argument: {sub_args[i]}\n")
                return 2

        try:
            spec = WorkflowSpec.load(spec_path)
            spec_dict = _json.loads(_json.dumps(spec._raw))
            for job in spec_dict.get("jobs", []):
                job["max_attempts"] = max_attempts
            conn = _workflow_runtime_conn()
            result = submit_workflow_inline(conn, spec_dict)
        except Exception as exc:
            stdout.write(f"error: failed to submit workflow: {exc}\n")
            return 1

        stdout.write(
            _json.dumps(
                {
                    "run_id": result["run_id"],
                    "status": result["status"],
                    "total_jobs": result.get("total_jobs", 0),
                    "priority": priority,
                    "max_attempts": max_attempts,
                    "note": "priority is accepted for compatibility but scheduling is workflow-runtime driven",
                },
                indent=2,
            )
            + "\n"
        )
        return 0

    if subcommand == "stats":
        try:
            conn = _workflow_runtime_conn()
            rows = conn.execute(
                """SELECT status, COUNT(*) AS count
                   FROM workflow_jobs
                   GROUP BY status
                   ORDER BY status"""
            )
            stats = {str(row["status"]): int(row["count"]) for row in (rows or [])}
            stats["total"] = sum(stats.values())
        except Exception as exc:
            stdout.write(f"error: failed to fetch workflow job stats: {exc}\n")
            return 1
        stdout.write(_json.dumps(stats, indent=2) + "\n")
        return 0

    if subcommand == "list":
        status_filter: str | None = None
        limit = 50

        i = 0
        while i < len(sub_args):
            if sub_args[i] == "--status" and i + 1 < len(sub_args):
                status_filter = sub_args[i + 1]
                i += 2
            elif sub_args[i] == "--limit" and i + 1 < len(sub_args):
                try:
                    limit = int(sub_args[i + 1])
                except ValueError:
                    stdout.write(
                        f"error: --limit must be an integer, got: {sub_args[i + 1]}\n"
                    )
                    return 2
                i += 2
            else:
                stdout.write(f"error: unknown argument: {sub_args[i]}\n")
                return 2

        try:
            conn = _workflow_runtime_conn()
            params: list[object] = []
            where = ""
            if status_filter:
                params.append(status_filter)
                where = f"WHERE j.status = ${len(params)}"
            params.append(limit)
            rows = conn.execute(
                f"""SELECT j.id, j.run_id, j.label, j.status, j.agent_slug, j.resolved_agent,
                           j.attempt, j.max_attempts,
                           COALESCE(wr.request_envelope->>'name', wr.workflow_id) AS workflow_name,
                           j.created_at, j.ready_at, j.claimed_at, j.started_at, j.finished_at
                    FROM workflow_jobs j
                    LEFT JOIN workflow_runs wr ON wr.run_id = j.run_id
                    {where}
                    ORDER BY j.created_at DESC
                    LIMIT ${len(params)}""",
                *params,
            )
        except Exception as exc:
            stdout.write(f"error: failed to list workflow jobs: {exc}\n")
            return 1

        rows = [
            {
                "id": int(row["id"]),
                "run_id": row["run_id"],
                "workflow_name": row.get("workflow_name"),
                "label": row["label"],
                "status": row["status"],
                "agent_slug": row.get("agent_slug"),
                "resolved_agent": row.get("resolved_agent"),
                "attempt": int(row.get("attempt") or 0),
                "max_attempts": int(row.get("max_attempts") or 0),
                "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
                "ready_at": row["ready_at"].isoformat() if row.get("ready_at") else None,
                "claimed_at": row["claimed_at"].isoformat() if row.get("claimed_at") else None,
                "started_at": row["started_at"].isoformat() if row.get("started_at") else None,
                "finished_at": row["finished_at"].isoformat() if row.get("finished_at") else None,
            }
            for row in (rows or [])
        ]
        stdout.write(_json.dumps(rows, indent=2) + "\n")
        return 0

    if subcommand == "worker":
        max_concurrent = 4
        poll_interval = 2.0
        capabilities: list[str] | None = None

        i = 0
        while i < len(sub_args):
            if sub_args[i] == "--max-concurrent" and i + 1 < len(sub_args):
                try:
                    max_concurrent = int(sub_args[i + 1])
                except ValueError:
                    stdout.write(
                        f"error: --max-concurrent must be an integer, got: {sub_args[i + 1]}\n"
                    )
                    return 2
                i += 2
            elif sub_args[i] == "--poll-interval" and i + 1 < len(sub_args):
                try:
                    poll_interval = float(sub_args[i + 1])
                except ValueError:
                    stdout.write(
                        f"error: --poll-interval must be a float, got: {sub_args[i + 1]}\n"
                    )
                    return 2
                i += 2
            elif sub_args[i] == "--capabilities" and i + 1 < len(sub_args):
                capabilities = [capability.strip() for capability in sub_args[i + 1].split(",") if capability.strip()]
                i += 2
            else:
                stdout.write(f"error: unknown argument: {sub_args[i]}\n")
                return 2

        from runtime.workflow.unified import run_worker_loop

        conn = _workflow_runtime_conn()
        worker_id = f"workflow-worker-{os.getpid()}"
        stdout.write(
            _json.dumps(
                {
                    "worker_id": worker_id,
                    "max_concurrent": max_concurrent,
                    "poll_interval_s": poll_interval,
                    "capabilities": capabilities,
                    "status": "starting",
                    "note": "capabilities are accepted for compatibility but worker admission is workflow-runtime driven",
                },
                indent=2,
            )
            + "\n"
        )
        stdout.flush()

        try:
            run_worker_loop(
                conn,
                os.getcwd(),
                poll_interval=poll_interval,
                worker_id=worker_id,
                max_local_concurrent=max_concurrent,
            )
        except KeyboardInterrupt:
            pass

        stdout.write(_json.dumps({"worker_id": worker_id, "status": "stopped"}) + "\n")
        return 0

    if subcommand == "cancel":
        if not sub_args or sub_args[0] in {"-h", "--help"}:
            stdout.write("usage: workflow queue cancel <job_id>\n")
            return 2

        job_id = sub_args[0]

        try:
            from runtime.workflow.unified import _recompute_workflow_run_state

            conn = _workflow_runtime_conn()
            rows = conn.execute(
                """UPDATE workflow_jobs
                   SET status = 'cancelled', finished_at = NOW()
                   WHERE id = $1::bigint
                     AND status IN ('pending', 'ready', 'claimed', 'running')
                   RETURNING run_id""",
                job_id,
            )
        except Exception as exc:
            stdout.write(f"error: failed to cancel job: {exc}\n")
            return 1

        if rows:
            _recompute_workflow_run_state(conn, str(rows[0]["run_id"]))
            stdout.write(_json.dumps({"job_id": job_id, "status": "cancelled"}, indent=2) + "\n")
            return 0

        stdout.write(
            _json.dumps(
                {
                    "job_id": job_id,
                    "status": "not_cancelled",
                    "message": "Job not found or already in a terminal state",
                },
                indent=2,
            )
            + "\n"
        )
        return 1

    stdout.write(f"unknown queue subcommand: {subcommand}\n")
    return 2
