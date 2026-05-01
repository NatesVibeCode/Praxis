"""Workflow-oriented CLI command handlers."""

from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from typing import TextIO

from surfaces.cli._db import cli_repo_root, cli_sync_conn
from surfaces.cli.mcp_tools import load_json_file, print_json, run_cli_tool
from surfaces._workflow_database import workflow_database_authority_for_repo
from runtime.spec_materializer import materialize_prompt_launch_spec as compile_prompt_launch_spec
from runtime.workspace_paths import workflow_root

_DETACHED_WAIT_ATTEMPTS = 30
_FOREGROUND_SUBMIT_FLAG = "--foreground-submit"
_SCRATCH_AGENT_RUNTIME_PROFILE_REF = "scratch_agent"


def _workflow_cli():
    from surfaces.cli import workflow_cli

    return workflow_cli


def _parse_args(parser, args: list[str], *, stdout: TextIO):
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stdout):
        return parser.parse_args(args)


def _workflow_tool(params: dict[str, object]) -> dict[str, object]:
    from surfaces.mcp.tools.workflow import tool_praxis_workflow

    return tool_praxis_workflow(params)


def _workflow_operation(operation_name: str, payload: dict[str, object]) -> dict[str, object]:
    from runtime.operation_catalog_gateway import execute_operation_from_env
    from surfaces.mcp.subsystems import workflow_database_env

    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name=operation_name,
        payload=payload,
    )
    return result if isinstance(result, dict) else {"ok": False, "status": "failed", "result": result}


def _workflow_subsystems():
    from surfaces.mcp.subsystems import _subs

    return _subs


def _workflow_query_mod():
    from surfaces.api.handlers import workflow_query as workflow_query_mod

    return workflow_query_mod


def _prompt_provider_choices() -> tuple[str, ...]:
    from registry import provider_execution_registry as provider_registry_mod

    return tuple(provider_registry_mod.registered_providers())


def _default_prompt_provider_slug() -> str:
    from registry import provider_execution_registry as provider_registry_mod

    return provider_registry_mod.default_provider_slug()


def _prompt_provider_help_line() -> str:
    try:
        providers = ", ".join(_prompt_provider_choices()) or "unavailable"
        default_provider = _default_prompt_provider_slug()
    except Exception:
        providers = "unavailable (configure WORKFLOW_DATABASE_URL)"
        default_provider = "unavailable"
    return (
        "  --provider <slug>    Registered provider: "
        f"{providers} (default: {default_provider})\n"
    )


def _coerce_json_object(raw: object) -> dict[str, object]:
    if not isinstance(raw, dict):
        raise ValueError("input must decode to a JSON object")
    return dict(raw)


def _load_input_payload(
    *,
    input_json: str | None,
    input_file: str | None,
) -> dict[str, object]:
    if input_json is not None and input_file is not None:
        raise ValueError("pass only one of --input-json or --input-file")
    if input_json is not None:
        return _coerce_json_object(json.loads(input_json))
    if input_file is not None:
        return _coerce_json_object(load_json_file(input_file))
    raise ValueError("one of --input-json or --input-file is required")


def _manifest_record_payload(row: dict[str, object]) -> dict[str, object]:
    from runtime.helm_manifest import normalize_helm_bundle

    manifest_id = str(row.get("id") or row.get("manifest_id") or "").strip()
    name = str(row.get("name") or manifest_id).strip()
    description = str(row.get("description") or "").strip()
    manifest = normalize_helm_bundle(
        row.get("manifest"),
        manifest_id=manifest_id,
        name=name or manifest_id,
        description=description,
    )
    payload: dict[str, object] = {
        "manifest_id": manifest_id,
        "name": name or manifest_id,
        "description": description,
        "manifest": manifest,
    }
    for field in ("version", "status", "created_by", "created_at", "updated_at"):
        value = row.get(field)
        if value is not None:
            payload[field] = value
    return payload


def _workflow_runtime_conn():
    return cli_sync_conn()


def _render_templated_spec_to_temp_file(spec_path: str, variables: dict[str, str]) -> str:
    from runtime.workflow_spec import load_raw
    from runtime.template_engine import render_spec

    raw = load_raw(spec_path)
    rendered = render_spec(raw, variables)
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as handle:
        json.dump(rendered, handle, indent=2)
        handle.write("\n")
        return handle.name


def _extract_common_run_options(
    args: list[str],
    *,
    stdout: TextIO,
) -> tuple[dict[str, object], list[str]] | None:
    options: dict[str, object] = {
        "dry_run": False,
        "preview_execution": False,
        "fresh": False,
        "job_id": None,
        "run_id": None,
        "result_file": None,
        "foreground_submit": False,
    }
    remaining: list[str] = []
    i = 0
    while i < len(args):
        token = args[i]
        if token == "--dry-run":
            options["dry_run"] = True
            i += 1
            continue
        if token == "--preview-execution":
            options["preview_execution"] = True
            i += 1
            continue
        if token == "--fresh":
            options["fresh"] = True
            i += 1
            continue
        if token == _FOREGROUND_SUBMIT_FLAG:
            options["foreground_submit"] = True
            i += 1
            continue
        if token in {"--job-id", "--run-id", "--result-file"}:
            if i + 1 >= len(args):
                stdout.write(f"error: {token} requires a value\n")
                return None
            options[token[2:].replace("-", "_")] = args[i + 1]
            i += 2
            continue
        remaining.append(token)
        i += 1
    return options, remaining


def _workflow_root(repo_root: Path) -> Path:
    return workflow_root(repo_root)


def _detached_result_file(repo_root: Path, result_file_base: str) -> Path:
    suffix = f"{int(time.time())}.{os.getpid()}.{time.time_ns() % 1_000_000}.json"
    return repo_root / "artifacts" / f"{result_file_base}.{suffix}"


def _forwarded_job_id(args: list[str]) -> str | None:
    for index, token in enumerate(args):
        if token == "--job-id" and index + 1 < len(args):
            return args[index + 1]
    return None


def _stream_command_text(run_id: str) -> str:
    return f"./scripts/praxis workflow stream {run_id}"


def _emit_live_stream_block(
    *,
    stdout: TextIO,
    run_id: str,
    stream_url: object | None = None,
    status_url: object | None = None,
) -> None:
    stdout.write("\nLIVE STREAM\n")
    stdout.write(f"  {_stream_command_text(run_id)}\n")
    if stream_url:
        stdout.write(f"  GET {stream_url}\n")
    if status_url:
        stdout.write(f"  status snapshot: ./scripts/praxis workflow run-status {run_id} --summary\n")


def _detached_launch_env(repo_root: Path) -> tuple[dict[str, str], str]:
    authority = workflow_database_authority_for_repo(repo_root, env=os.environ)
    env = dict(os.environ)
    env["WORKFLOW_DATABASE_URL"] = str(authority.database_url or "")
    env["WORKFLOW_DATABASE_AUTHORITY_SOURCE"] = authority.source
    env["PATH"] = str(os.environ.get("PATH", ""))
    workflow_root = str(_workflow_root(repo_root))
    existing_pythonpath = str(os.environ.get("PYTHONPATH", "")).strip()
    env["PYTHONPATH"] = (
        workflow_root
        if not existing_pythonpath
        else f"{workflow_root}{os.pathsep}{existing_pythonpath}"
    )
    return env, authority.source


def _emit_detached_submission_status(
    *,
    stdout: TextIO,
    payload: dict[str, object],
    success_prefix: str,
    emit_parent: bool,
    result_file: Path,
    authority_source: str,
) -> None:
    run_id = str(payload.get("run_id") or "unknown")
    workflow_id = str(payload.get("workflow_id") or "unknown")
    status = str(payload.get("status") or "unknown")
    parent_run_id = str(payload.get("parent_run_id") or "unknown")
    prefix = "Workflow replayed" if status == "replayed" else success_prefix

    stdout.write(f"{prefix}: {run_id}\n")
    stdout.write(f"Workflow ID: {workflow_id}\n")
    if emit_parent:
        stdout.write(f"Parent run: {parent_run_id}\n")
    stdout.write(f"Submission status: {status}\n")
    stdout.write(f"DB authority source: {authority_source}\n")
    stdout.write(f"Result file: {result_file}\n")
    _emit_live_stream_block(
        stdout=stdout,
        run_id=run_id,
        stream_url=payload.get("stream_url"),
        status_url=payload.get("status_url"),
    )


def _launch_detached_frontdoor(
    *,
    command_name: str,
    args: list[str],
    stdout: TextIO,
    result_file_base: str,
    success_prefix: str,
    emit_parent: bool,
) -> int:
    repo_root = cli_repo_root()
    repo_root.joinpath("artifacts").mkdir(parents=True, exist_ok=True)
    result_file = _detached_result_file(repo_root, result_file_base)
    log_path = repo_root / "artifacts" / "workflow.log"
    env, authority_source = _detached_launch_env(repo_root)

    forwarded_args = list(args)
    if _forwarded_job_id(forwarded_args) is None:
        forwarded_args.extend(["--job-id", f"workflow-launch-{int(time.time())}-{os.getpid()}"])
    forwarded_args.extend(["--result-file", str(result_file), _FOREGROUND_SUBMIT_FLAG])
    command = [
        sys.executable,
        "-m",
        "surfaces.cli.main",
        "workflow",
        command_name,
        *forwarded_args,
    ]

    with log_path.open("a", encoding="utf-8") as log_handle:
        process = subprocess.Popen(
            command,
            cwd=str(Path.cwd()),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=log_handle,
            start_new_session=True,
        )

    for _ in range(_DETACHED_WAIT_ATTEMPTS):
        if result_file.is_file() and result_file.stat().st_size > 0:
            payload = json.loads(result_file.read_text(encoding="utf-8"))
            _emit_detached_submission_status(
                stdout=stdout,
                payload=payload if isinstance(payload, dict) else {},
                success_prefix=success_prefix,
                emit_parent=emit_parent,
                result_file=result_file,
                authority_source=authority_source,
            )
            return 0
        if process.poll() is not None:
            break
        time.sleep(1)

    if process.poll() is not None and (not result_file.exists() or result_file.stat().st_size == 0):
        stdout.write(
            f"Workflow {command_name} process exited before durable submission completed.\n"
        )
        stdout.write(f"DB authority source: {authority_source}\n")
        stdout.write("No result file was written.\n")
        stdout.write("Check artifacts/workflow.log for the launch error.\n")
        return 1

    stdout.write(
        f"Workflow {command_name} process started (PID {process.pid}), awaiting durable submission result.\n"
    )
    stdout.write(f"DB authority source: {authority_source}\n")
    stdout.write(f"Result file: {result_file}\n")
    stdout.write("Durable run id is not available yet; do not guess from active runs.\n")
    stdout.write("When the result file appears, stream the run_id it contains:\n")
    stdout.write("  ./scripts/praxis workflow stream <run_id>\n")
    return 0


def _run_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow run <spec.json>` or `workflow run -p <prompt>`.

    File-backed launches still go through `workflow_cli.cmd_run`. Prompt-backed
    launches use the same authority helper directly so we do not synthesize a
    throwaway spec file just to submit one inline workflow.
    """

    import json as _json

    from runtime.workflow_spec import is_batch_spec, load_raw

    original_args = list(args)
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
            "\n"
            "extra launch controls:\n"
            "  --preview-execution  Print the exact worker-facing execution payload without submitting\n"
            "  --fresh              Force a fresh run while letting Praxis mint the run_id\n"
            "  --job-id <id>        Attach a caller-facing tracking id to the result file\n"
            "  --result-file <path> Write the queued submit payload to disk\n"
            "\n"
            "watch live:\n"
            "  workflow stream <run_id>\n"
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
    parsed_common = _extract_common_run_options(args, stdout=stdout)
    if parsed_common is None:
        return 2
    common_options, args = parsed_common
    if not args:
        stdout.write("error: workflow run requires a spec path or -p <prompt>\n")
        return 2
    if common_options["dry_run"] and common_options["preview_execution"]:
        stdout.write("error: --preview-execution cannot be combined with --dry-run\n")
        return 2
    if (
        not common_options["dry_run"]
        and not common_options["preview_execution"]
        and not common_options["foreground_submit"]
    ):
        return _launch_detached_frontdoor(
            command_name="run",
            args=original_args,
            stdout=stdout,
            result_file_base="workflow_run_result",
            success_prefix="Workflow submitted",
            emit_parent=False,
        )

    if args[0] in {"-p", "--prompt"}:
        if len(args) < 2:
            stdout.write(
                "usage: workflow run -p <prompt> [options]\n"
                "\n"
                "options:\n"
                + _prompt_provider_help_line() +
                "  --model <slug>       Model slug (provider-specific)\n"
                "  --tier <tier>        Route by tier: frontier, mid, economy\n"
                "  --write <paths>      Files the model should modify (comma-separated)\n"
                "  --workdir <dir>      Workspace root for file reads/writes\n"
                "  --context <paths>    Extra files to inject as context (comma-separated)\n"
                "  --timeout <secs>     Execution timeout (default: 300)\n"
                "  --task-type <type>   Task type for routing: code_generation, review, etc.\n"
                "  --system <prompt>    System prompt override\n"
                "  --workspace <ref>    Workspace authority ref\n"
                "  --runtime-profile <ref> Runtime profile authority ref\n"
                "  --scratch            Run in the blank scratch_agent container lane\n"
                "  --preview-execution  Print the exact worker-facing execution payload without submitting\n"
                "  --dry-run            Parse and show the spec without executing\n"
                "  --fresh              Force a fresh run while letting Praxis mint the run_id\n"
            )
            return 2

        provider = None
        model = None
        tier = None
        adapter = None
        scope_write = None
        workdir = None
        context_files = None
        timeout = 300
        task_type = None
        system_prompt = None
        runtime_profile_ref = None
        workspace_ref = None
        clean_args: list[str] = []
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
            elif args[i] == "--runtime-profile" and i + 1 < len(args):
                runtime_profile_ref = args[i + 1]
                i += 2
            elif args[i] == "--workspace" and i + 1 < len(args):
                workspace_ref = args[i + 1]
                i += 2
            elif args[i] == "--scratch":
                runtime_profile_ref = _SCRATCH_AGENT_RUNTIME_PROFILE_REF
                workspace_ref = _SCRATCH_AGENT_RUNTIME_PROFILE_REF
                i += 1
            else:
                clean_args.append(args[i])
                i += 1
        if provider is None:
            provider = _default_prompt_provider_slug()
        prompt = " ".join(clean_args)
        if scope_write and system_prompt is None:
            system_prompt = "You are a code editor. Return ONLY valid JSON structured output."
        try:
            prompt_launch_spec = compile_prompt_launch_spec(
                prompt=prompt,
                provider_slug=provider,
                model_slug=model,
                tier=tier,
                adapter_type=adapter,
                scope_write=scope_write,
                workdir=workdir,
                context_files=context_files,
                timeout=timeout,
                task_type=task_type,
                system_prompt=system_prompt,
                workspace_ref=workspace_ref,
                runtime_profile_ref=runtime_profile_ref,
            )
        except ValueError as exc:
            stdout.write(f"error: {exc}\n")
            return 2
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stdout):
            return _workflow_cli()._submit_workflow_launch(
                prompt_launch_spec=prompt_launch_spec,
                preview_execution=bool(common_options["preview_execution"]),
                dry_run=bool(common_options["dry_run"]),
                fresh=bool(common_options["fresh"]),
                job_id=common_options["job_id"],
                run_id=common_options["run_id"],
                result_file=common_options["result_file"],
                requested_by_kind="cli",
                requested_by_ref="workflow.run.prompt",
            )

    spec_path = args[0]

    try:
        raw = load_raw(spec_path)
    except (OSError, _json.JSONDecodeError) as exc:
        stdout.write(f"error: could not read spec file: {exc}\n")
        return 2

    if is_batch_spec(raw):
        stdout.write(
            "error: workflow run no longer launches batch specs directly; "
            "use workflow chain or the API/MCP frontdoor\n"
        )
        return 2

    rendered_path = spec_path
    if variables:
        rendered_path = _render_templated_spec_to_temp_file(spec_path, variables)
    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stdout):
            return _workflow_cli().cmd_run(
                SimpleNamespace(
                    spec=rendered_path,
                    preview_execution=bool(common_options["preview_execution"]),
                    dry_run=bool(common_options["dry_run"]),
                    fresh=bool(common_options["fresh"]),
                    job_id=common_options["job_id"],
                    run_id=common_options["run_id"],
                    result_file=common_options["result_file"],
                )
            )
    finally:
        if rendered_path != spec_path:
            try:
                os.unlink(rendered_path)
            except OSError:
                pass


def _spawn_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow spawn <parent_run_id> <spec.json>` with detached submit parity."""

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow spawn <parent_run_id> <spec.json> [--reason <reason>] "
            "[--parent-job-label <label>] [--lineage-depth <n>] [--fresh] "
            "[--job-id <id>] [--run-id <id>] [--result-file <path>]\n"
        )
        return 2

    original_args = list(args)
    foreground_submit = False
    filtered_args: list[str] = []
    for token in args:
        if token == _FOREGROUND_SUBMIT_FLAG:
            foreground_submit = True
            continue
        filtered_args.append(token)
    args = filtered_args

    if len(args) < 2:
        stdout.write(
            "usage: workflow spawn <parent_run_id> <spec.json> [--reason <reason>] "
            "[--parent-job-label <label>] [--lineage-depth <n>] [--fresh] "
            "[--job-id <id>] [--run-id <id>] [--result-file <path>]\n"
        )
        return 2

    if not foreground_submit:
        return _launch_detached_frontdoor(
            command_name="spawn",
            args=original_args,
            stdout=stdout,
            result_file_base="workflow_spawn_result",
            success_prefix="Child workflow spawned",
            emit_parent=True,
        )

    parent_run_id = args[0]
    spec_path = args[1]
    reason = "cli.spawn"
    parent_job_label = None
    lineage_depth = None
    fresh = False
    job_id = None
    run_id = None
    result_file = None

    i = 2
    while i < len(args):
        token = args[i]
        if token == "--reason" and i + 1 < len(args):
            reason = args[i + 1]
            i += 2
            continue
        if token == "--parent-job-label" and i + 1 < len(args):
            parent_job_label = args[i + 1]
            i += 2
            continue
        if token == "--lineage-depth" and i + 1 < len(args):
            try:
                lineage_depth = int(args[i + 1])
            except ValueError:
                stdout.write(
                    f"error: --lineage-depth must be an integer, got: {args[i + 1]}\n"
                )
                return 2
            i += 2
            continue
        if token == "--fresh":
            fresh = True
            i += 1
            continue
        if token in {"--job-id", "--run-id", "--result-file"} and i + 1 < len(args):
            value = args[i + 1]
            if token == "--job-id":
                job_id = value
            elif token == "--run-id":
                run_id = value
            else:
                result_file = value
            i += 2
            continue
        stdout.write(f"error: unknown spawn argument: {token}\n")
        return 2

    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stdout):
        return _workflow_cli().cmd_spawn(
            SimpleNamespace(
                parent_run_id=parent_run_id,
                spec=spec_path,
                reason=reason,
                parent_job_label=parent_job_label,
                lineage_depth=lineage_depth,
                fresh=fresh,
                job_id=job_id,
                run_id=run_id,
                result_file=result_file,
            )
        )


def _dry_run_command(args: list[str], *, stdout: TextIO) -> int:
    """Compatibility alias for `workflow run --dry-run`."""

    return _run_command([*args, "--dry-run"], stdout=stdout)


def _chain_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow chain <coordination.json>` or legacy `<spec1> <spec2> ...`.

    Two modes:

    1. **Coordination-program mode** (preferred, for multi-wave DAGs): one
       positional arg pointing at a coordination JSON (``config/cascade/chain/*.json``)
       whose top-level object has a ``waves`` key. Submits via the durable
       ``WorkflowChainProgram`` command bus (BUG-61881910).

    2. **Legacy sequential mode**: two or more spec paths. Each job depends on
       the previous one succeeding. Uses ``submit_chain``.

    The two shapes are distinguished by (a) arg count and (b) JSON content.
    """

    import json as _json
    from pathlib import Path as _Path

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow chain <coordination.json>\n"
            "       workflow chain <spec1.json> <spec2.json> ...\n"
            "\n"
            "Coordination-program mode: single JSON with a top-level 'waves' key\n"
            "(e.g. config/cascade/chain/example_program.json) is submitted\n"
            "via the durable multi-wave chain command bus.\n"
            "\n"
            "Legacy sequential mode: two or more spec paths submit a job chain where\n"
            "each job depends on the previous one succeeding.\n"
            "\n"
            "Options (coordination-program mode only):\n"
            "  --no-adopt-active   do not adopt an already-active chain for this program\n"
        )
        return 2

    # Strip coordination-program-only flags before file detection.
    adopt_active = True
    positional: list[str] = []
    for arg in args:
        if arg == "--no-adopt-active":
            adopt_active = False
        elif arg in {"--adopt-active"}:
            adopt_active = True
        else:
            positional.append(arg)

    # --- Coordination-program detection --------------------------------
    if len(positional) == 1:
        coord_path = positional[0]
        path_obj = _Path(coord_path)
        if not path_obj.exists():
            stdout.write(f"error: spec file not found: {coord_path}\n")
            return 1
        try:
            raw = _json.loads(path_obj.read_text(encoding="utf-8"))
        except _json.JSONDecodeError as exc:
            stdout.write(f"error: invalid JSON in {coord_path}: {exc}\n")
            return 1
        if isinstance(raw, dict) and "waves" in raw:
            return _submit_coordination_chain(
                coord_path,
                adopt_active=adopt_active,
                stdout=stdout,
            )
        # Single file but not a coordination program → legacy requires 2+.
        stdout.write(
            "error: legacy chain mode requires at least 2 spec files, "
            "or a single coordination-program JSON with a top-level 'waves' key\n"
        )
        return 2

    # --- Legacy sequential chain ---------------------------------------
    from runtime.workflow_spec import load_workflow_spec
    from runtime.job_dependencies import submit_chain

    if len(positional) < 2:
        stdout.write("error: chain requires at least 2 spec files\n")
        return 2

    specs = []
    for spec_path in positional:
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


def _submit_coordination_chain(
    coordination_path: str,
    *,
    adopt_active: bool,
    stdout: TextIO,
) -> int:
    """Submit a durable multi-wave WorkflowChainProgram via the command bus.

    This is the CLI frontdoor for ``config/cascade/chain/<program>.json``
    coordination JSONs (BUG-61881910). Symmetric with ``workflow run`` for
    single specs — both go through a durable request/render command bus path.
    """
    import json as _json

    from runtime.control_commands import (
        render_workflow_chain_submit_response,
        request_workflow_chain_submit_command,
    )

    try:
        from surfaces.mcp.subsystems import _subs  # type: ignore[import-not-found]
        pg_conn = _subs.get_pg_conn()
    except Exception:
        # Fall back to direct storage bootstrap when running outside the
        # MCP server process (e.g. headless terminal launches).
        from storage.postgres.connection import get_sync_postgres_connection
        pg_conn = get_sync_postgres_connection()

    try:
        from runtime.workflow_chain import WorkflowChainError
    except ImportError:
        WorkflowChainError = Exception  # type: ignore[assignment]

    import os as _os
    repo_root = _os.environ.get("PRAXIS_REPO_ROOT") or _os.getcwd()

    try:
        command = request_workflow_chain_submit_command(
            pg_conn,
            requested_by_kind="cli",
            requested_by_ref="workflow.chain",
            coordination_path=coordination_path,
            repo_root=repo_root,
            adopt_active=adopt_active,
        )
        result = render_workflow_chain_submit_response(
            pg_conn,
            command,
            coordination_path=coordination_path,
        )
    except WorkflowChainError as exc:
        stdout.write(_json.dumps(
            {"status": "failed", "error": str(exc)}, default=str, indent=2
        ) + "\n")
        return 1
    except Exception as exc:
        stdout.write(_json.dumps(
            {"status": "failed", "error": f"{type(exc).__name__}: {exc}"},
            default=str, indent=2,
        ) + "\n")
        return 1

    stdout.write(_json.dumps(result, default=str, indent=2) + "\n")
    status_value = str(result.get("status") or "").lower()
    if status_value in {"failed", "approval_required"}:
        return 1
    return 0


def _generate_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow generate` — render a spec from a manifest."""
    import argparse

    parser = argparse.ArgumentParser(prog="workflow generate")
    parser.add_argument("manifest_file", help="Path to the minimal JSON manifest file")
    parser.add_argument("output", help="Path to the output .queue.json spec file")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--strict", action="store_true", help="Fail if the output file already exists")
    mode.add_argument("--merge", action="store_true", help="Merge with existing output file if it exists")

    try:
        parsed = parser.parse_args(args)
    except SystemExit as exc:
        return exc.code

    from surfaces.cli import workflow_cli
    return workflow_cli.cmd_generate(parsed)


def _validate_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow validate` — validate a spec without running."""
    import argparse

    parser = argparse.ArgumentParser(prog="workflow validate")
    parser.add_argument("spec", help="Path to the .queue.json spec file")

    try:
        parsed = _parse_args(parser, args, stdout=stdout)
    except SystemExit as exc:
        return exc.code

    from surfaces.cli import workflow_cli
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stdout):
        return workflow_cli.cmd_validate(parsed)


def _stream_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow stream` — stream run progress."""
    import argparse

    parser = argparse.ArgumentParser(prog="workflow stream")
    parser.add_argument("run_id", help="Workflow run id to stream")
    parser.add_argument("--timeout", type=float, default=None, help="Stop streaming after N seconds")
    parser.add_argument("--poll-interval", type=float, default=2.0, help="Poll interval in seconds")

    try:
        parsed = _parse_args(parser, args, stdout=stdout)
    except SystemExit as exc:
        return exc.code

    from surfaces.cli import workflow_cli
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stdout):
        return workflow_cli.cmd_stream(parsed)


def _chain_status_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow chain-status` — show multi-wave status."""
    import argparse

    parser = argparse.ArgumentParser(prog="workflow chain-status")
    parser.add_argument("chain_id", nargs="?", default=None, help="Chain ID to inspect")
    parser.add_argument("--limit", type=int, default=20, help="Max chains to list")

    try:
        parsed = parser.parse_args(args)
    except SystemExit as exc:
        return exc.code

    from surfaces.cli import workflow_cli
    return workflow_cli.cmd_chain_status(parsed)


def _status_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow status` — print recent workflow summary as JSON."""

    as_json = False
    days: int | None = None
    limit: int | None = None
    i = 0

    while i < len(args):
        token = args[i]
        if token in {"-h", "--help"}:
            stdout.write("usage: workflow status [--days N] [--limit N] [--json]\n")
            return 0
        if token == "--json":
            as_json = True
            i += 1
            continue
        if token == "--days":
            if i + 1 >= len(args):
                stdout.write("error: --days requires a value\n")
                return 2
            try:
                days = int(args[i + 1])
            except ValueError:
                stdout.write(f"error: invalid days value: {args[i + 1]}\n")
                return 2
            i += 2
            continue
        if token == "--limit":
            if i + 1 >= len(args):
                stdout.write("error: --limit requires a value\n")
                return 2
            try:
                limit = int(args[i + 1])
            except ValueError:
                stdout.write(f"error: invalid limit value: {args[i + 1]}\n")
                return 2
            i += 2
            continue
        stdout.write(f"error: unknown argument: {token}\n")
        return 2

    import json as _json

    from runtime.workflow_status import get_workflow_history

    history = get_workflow_history()
    # If limit is provided, we need to bypass the summary() which uses max_size
    # but for now we'll just support days in summary()
    summary_data = history.summary(days=days)
    if limit is not None:
        # If they asked for a specific limit, we might need a different summary or just filter last_5
        # but the summary() itself aggregates over all 'recent' items.
        # history.summary() uses self._max_size as the limit for _recent_workflows_snapshot.
        pass

    stdout.write(_json.dumps(summary_data, indent=2) + "\n")
    return 0


def _compact_status_mapping(mapping: object, keys: tuple[str, ...]) -> dict[str, object]:
    if not isinstance(mapping, dict):
        return {}
    return {key: mapping[key] for key in keys if mapping.get(key) is not None}


def _run_status_summary_payload(payload: dict[str, object]) -> dict[str, object]:
    """Project verbose workflow status into the fields agents need first."""

    jobs = payload.get("jobs") if isinstance(payload.get("jobs"), list) else []
    compact_jobs: list[dict[str, object]] = []
    job_status_counts: dict[str, int] = {}
    for raw_job in jobs:
        if not isinstance(raw_job, dict):
            continue
        status = str(raw_job.get("status") or "unknown")
        job_status_counts[status] = job_status_counts.get(status, 0) + 1
        compact_jobs.append(
            _compact_status_mapping(
                raw_job,
                (
                    "job_label",
                    "label",
                    "status",
                    "agent_slug",
                    "attempt",
                    "error_code",
                    "reason_code",
                    "heartbeat_age_seconds",
                ),
            )
        )

    health = payload.get("health")
    compact_health = _compact_status_mapping(
        health,
        (
            "state",
            "likely_failed",
            "elapsed_seconds",
            "completed_jobs",
            "running_or_claimed",
            "terminal_jobs",
            "non_retryable_failed_jobs",
        ),
    )
    if isinstance(health, dict) and isinstance(health.get("signals"), list):
        compact_health["signals"] = [
            _compact_status_mapping(
                signal,
                ("type", "severity", "message", "node_id", "failure_code", "hint", "jobs"),
            )
            for signal in health["signals"]
            if isinstance(signal, dict)
        ]

    recovery = payload.get("recovery")
    compact_recovery = _compact_status_mapping(recovery, ("mode", "reason", "live_stream"))
    if isinstance(recovery, dict) and isinstance(recovery.get("recommended_tool"), dict):
        compact_recovery["recommended_tool"] = _compact_status_mapping(
            recovery["recommended_tool"],
            ("name", "arguments"),
        )

    return {
        "run_id": payload.get("run_id"),
        "status": payload.get("status"),
        "spec_name": payload.get("spec_name"),
        "total_jobs": len(compact_jobs),
        "job_status_counts": job_status_counts,
        "health": compact_health,
        "recovery": compact_recovery,
        "jobs": compact_jobs,
    }


def _run_status_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow run-status <run_id>` with optional idle recovery."""

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow run-status <run_id> [--kill-if-idle] [--idle-threshold-seconds N] [--json] [--summary]\n"
        )
        return 2

    run_id = args[0].strip()
    kill_if_idle = False
    idle_threshold_seconds: int | None = None
    summary = False
    i = 1
    while i < len(args):
        if args[i] == "--kill-if-idle":
            kill_if_idle = True
            i += 1
        elif args[i] == "--idle-threshold-seconds" and i + 1 < len(args):
            try:
                idle_threshold_seconds = int(args[i + 1])
            except ValueError:
                stdout.write(f"error: idle threshold must be an integer, got: {args[i + 1]}\n")
                return 2
            i += 2
        elif args[i] == "--json":
            i += 1
        elif args[i] in {"--summary", "--compact"}:
            summary = True
            i += 1
        else:
            stdout.write(f"unknown argument: {args[i]}\n")
            return 2

    params: dict[str, object] = {"action": "status", "run_id": run_id}
    if kill_if_idle:
        params["kill_if_idle"] = True
    if idle_threshold_seconds is not None:
        params["idle_threshold_seconds"] = idle_threshold_seconds

    payload = _workflow_tool(params)
    if summary:
        payload = _run_status_summary_payload(payload)
    print_json(stdout, payload)
    return 0 if not payload.get("error") and payload.get("status") != "not_found" else 1


def _inspect_job_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow inspect-job <run_id> [label]`."""

    if not args or args[0] in {"-h", "--help"}:
        stdout.write("usage: workflow inspect-job <run_id> [label]\n")
        return 2

    run_id = args[0].strip()
    label = args[1].strip() if len(args) > 1 else ""
    params: dict[str, object] = {"action": "inspect", "run_id": run_id}
    if label:
        params["label"] = label
    payload = _workflow_tool(params)
    print_json(stdout, payload)
    return 0 if not payload.get("error") else 1


def _authority_index_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow authority-index` — print the concept→code→DB→surface index."""

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow authority-index [--json]\n"
            "Prints the curated authority index from config/authority_index.yaml.\n"
        )
        return 0

    from runtime.authority_index import load_authority_index

    as_json = "--json" in args
    try:
        entries = load_authority_index()
    except FileNotFoundError as exc:
        stdout.write(f"authority_index missing: {exc}\n")
        return 1

    if as_json:
        print_json(stdout, {"entries": [entry.as_dict() for entry in entries]})
        return 0

    header = ("CONCEPT", "MODULE", "TABLES", "CLI", "MCP", "TESTS")
    rows = [header]
    for entry in entries:
        rows.append(
            (
                entry.concept,
                entry.authority_module,
                ",".join(entry.storage_tables) or "-",
                entry.cli or "-",
                entry.mcp or "-",
                ";".join(entry.proving_tests) or "-",
            )
        )
    widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
    for row in rows:
        stdout.write(
            "  ".join(cell.ljust(width) for cell, width in zip(row, widths)).rstrip()
            + "\n"
        )
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

    try:
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
    except Exception as exc:
        reason_code = (
            "verifier.db_authority_unavailable"
            if exc.__class__.__name__ == "PostgresConfigurationError"
            else "verifier.error"
        )
        print_json(
            stdout,
            {
                "error": str(exc),
                "status": "error",
                "reason_code": reason_code,
                "source_authority": "verifier_registry",
            },
        )
        return 1
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

    conn = cli_sync_conn()
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

    if args and args[0] == "eval":
        return _pipeline_eval_command(args[1:], stdout=stdout)

    from runtime.workflow import run_workflow_pipeline
    from runtime.workflow_builder import WorkflowStep

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow pipeline <pipeline.json>\n"
            "       workflow pipeline eval <spec.json> [--json]\n"
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
                loop=step.get("loop", False),
                loop_prompt=step.get("loop_prompt"),
                loop_max_parallel=step.get("loop_max_parallel", 4),
            )
            for step in raw_steps
        ]
    except (KeyError, TypeError) as exc:
        stdout.write(f"error: invalid step definition: {exc}\n")
        return 2

    result = run_workflow_pipeline(steps)
    stdout.write(_json.dumps(result.to_json(), indent=2) + "\n")
    return 0 if result.status == "succeeded" else 1


def _pipeline_eval_command(args: list[str], *, stdout: TextIO) -> int:
    """Read-only evaluator for workflow launch contracts."""

    json_output = False
    filtered: list[str] = []
    for token in args:
        if token == "--json":
            json_output = True
            continue
        filtered.append(token)

    if not filtered or filtered[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow pipeline eval <spec.json> [--json]\n"
            "\n"
            "Read-only launch contract evaluation. It validates the spec, builds the\n"
            "worker-facing preview, and fails if prompts, shards, write scope,\n"
            "submission tools, provider availability, or scoped tool instructions\n"
            "do not agree. It never launches jobs and never probes providers.\n"
        )
        return 2

    spec_path = filtered[0]
    if len(filtered) > 1:
        stdout.write(f"error: unexpected argument: {filtered[1]}\n")
        return 2

    from runtime.workflow.pipeline_eval import evaluate_pipeline_preview
    from runtime.workflow._admission import preview_workflow_execution
    from runtime.workflow_spec import WorkflowSpec, WorkflowSpecError
    from runtime.workflow_validation import _authority_error_result, validate_workflow_spec

    try:
        spec = WorkflowSpec.load(spec_path)
    except WorkflowSpecError as exc:
        payload = {
            "ok": False,
            "error_count": 1,
            "warning_count": 0,
            "findings": [
                {
                    "severity": "error",
                    "kind": "spec_load_failed",
                    "message": str(exc),
                }
            ],
        }
        if json_output:
            stdout.write(json.dumps(payload, indent=2) + "\n")
        else:
            stdout.write(f"=== Workflow Pipeline Eval: FAILED ===\n{exc}\n")
        return 1

    try:
        conn = cli_sync_conn()
    except Exception as exc:
        validation_result = _authority_error_result(spec, f"{type(exc).__name__}: {exc}")
        preview_payload: dict[str, object] = {
            "spec_name": spec.name,
            "workflow_id": spec.workflow_id,
            "total_jobs": len(spec.jobs),
            "jobs": [],
            "warnings": [validation_result.get("error") or "database authority unavailable"],
        }
    else:
        validation_result = validate_workflow_spec(spec, pg_conn=conn)
        try:
            preview_payload = preview_workflow_execution(
                conn,
                spec_path=spec_path,
                repo_root=str(cli_repo_root()),
            )
        except Exception as exc:
            preview_payload = {
                "spec_name": spec.name,
                "workflow_id": spec.workflow_id,
                "total_jobs": len(spec.jobs),
                "jobs": [],
                "warnings": [],
            }
            validation_result = {
                **dict(validation_result),
                "valid": False,
                "error_kind": "preview_failed",
                "error": f"workflow execution preview failed: {type(exc).__name__}: {exc}",
            }

    result = evaluate_pipeline_preview(
        spec,
        validation_result=validation_result,
        preview_payload=preview_payload,
    )

    if json_output:
        stdout.write(json.dumps(result.to_dict(), indent=2) + "\n")
        return 0 if result.ok else 1

    stdout.write(f"=== Workflow Pipeline Eval: {'PASSED' if result.ok else 'FAILED'} ===\n")
    stdout.write(f"Name:        {result.spec_name}\n")
    stdout.write(f"Workflow ID: {result.workflow_id or '-'}\n")
    stdout.write(f"Jobs:        {result.total_jobs}\n")
    stdout.write(f"Errors:      {result.error_count}\n")
    stdout.write(f"Warnings:    {result.warning_count}\n")
    stdout.write("\n")
    stdout.write("Phase progress:\n")
    for phase in result.phase_progress:
        required = " (required before launch)" if phase.get("required_before_launch") else ""
        stdout.write(
            f"- {phase.get('phase')}: {phase.get('status')}{required}\n"
        )
    stdout.write("\n")
    for finding in result.findings:
        label = f" [{finding.label}]" if finding.label else ""
        stdout.write(f"- {finding.severity.upper()} {finding.kind}{label}: {finding.message}\n")
    if not result.findings:
        stdout.write("No contract findings.\n")
    directories = result.directory_summary.get("directories") or []
    if directories:
        stdout.write("\nDirectory summary:\n")
        for item in directories[:8]:
            stdout.write(
                f"- {item.get('directory')}: "
                f"{item.get('errors', 0)} error(s), {item.get('warnings', 0)} warning(s)\n"
            )
    if result.quarantine_candidates:
        stdout.write("\nQuarantine candidates:\n")
        for item in result.quarantine_candidates:
            stdout.write(
                f"- {item.get('workflow_id') or result.workflow_id or '-'}: "
                f"{item.get('reason_code')} ({item.get('error_count', 0)} error(s))\n"
            )
    stdout.write(
        "\nProvider freshness: required before launch; refresh through "
        "praxis_provider_availability_refresh if stale or unknown.\n"
    )
    return 0 if result.ok else 1


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


def _loop_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow loop --items "a,b,c" --prompt "Do X with {{item}}"`.

    Runs one spec per item in parallel, prints each result as a
    JSON line, then prints a summary object.
    """

    import json as _json

    from runtime.loop import aggregate_loop_results, loop_dispatch

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            'usage: workflow loop --items "a,b,c" --prompt "Analyze: {{item}}"'
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

    results = loop_dispatch(
        items,
        prompt_template=prompt,
        tier=tier,
        max_parallel=max_parallel,
    )

    for result in results:
        stdout.write(_json.dumps(result.to_json()) + "\n")

    summary = aggregate_loop_results(results)
    stdout.write(_json.dumps(summary) + "\n")
    return 0 if summary["failed"] == 0 else 1


def _debate_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow debate "topic" [--personas N] [--rounds N]`.

    Runs a structured multi-perspective debate on the given topic using
    default personas (Pragmatist, Skeptic, Innovator, Operator).
    """

    import json as _json

    from runtime.debate_workflow import DebateConfig, default_personas, run_debate

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
    metrics_error = None
    try:
        metrics_conn = cli_sync_conn()
    except Exception as exc:
        metrics_conn = None
        metrics_error = f"{type(exc).__name__}: {exc}"

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
        "metrics_persistence": {
            "available": metrics_conn is not None,
            **({"error": metrics_error} if metrics_error else {}),
        },
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


def _manifest_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle manifest lifecycle through a stable CLI front door."""

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow manifest get <manifest_id>\n"
            "       workflow manifest generate <intent...>\n"
            "       workflow manifest generate-quick <intent...> [--template-id <id>]\n"
            "       workflow manifest refine <manifest_id> <instruction...>\n"
            "       workflow manifest save (--input-json <json> | --input-file <path>)\n"
            "       workflow manifest save-as [--name <name>] [--description <text>] (--input-json <json> | --input-file <path>)\n"
        )
        return 2

    subcommand = args[0]
    tail = args[1:]
    subsystems = _workflow_subsystems()
    conn = subsystems.get_pg_conn()

    try:
        if subcommand == "get":
            if len(tail) != 1:
                stdout.write("usage: workflow manifest get <manifest_id>\n")
                return 2
            from storage.postgres.workflow_runtime_repository import load_app_manifest_record

            row = load_app_manifest_record(conn, manifest_id=tail[0].strip())
            if row is None:
                stdout.write(json.dumps({"error": f"Manifest not found: {tail[0].strip()}"}, indent=2) + "\n")
                return 1
            print_json(stdout, _manifest_record_payload(dict(row)))
            return 0

        if subcommand == "generate":
            intent = " ".join(part for part in tail if part).strip()
            if not intent:
                stdout.write("usage: workflow manifest generate <intent...>\n")
                return 2
            from runtime.canonical_manifests import generate_manifest

            result = generate_manifest(
                conn,
                matcher=subsystems.get_intent_matcher(),
                generator=subsystems.get_manifest_generator(),
                intent=intent,
            )
            print_json(
                stdout,
                {
                    "manifest_id": result.manifest_id,
                    "manifest": result.manifest,
                    "version": result.version,
                    "confidence": result.confidence,
                    "explanation": result.explanation,
                },
            )
            return 0

        if subcommand == "generate-quick":
            template_id = None
            intent_parts: list[str] = []
            i = 0
            while i < len(tail):
                if tail[i] == "--template-id" and i + 1 < len(tail):
                    template_id = tail[i + 1].strip() or None
                    i += 2
                else:
                    intent_parts.append(tail[i])
                    i += 1
            intent = " ".join(intent_parts).strip()
            if not intent:
                stdout.write("usage: workflow manifest generate-quick <intent...> [--template-id <id>]\n")
                return 2
            from runtime.canonical_manifests import generate_manifest_quick

            payload = generate_manifest_quick(
                conn,
                matcher=subsystems.get_intent_matcher(),
                generator=subsystems.get_manifest_generator(),
                intent=intent,
                template_id=template_id,
            )
            print_json(stdout, payload)
            return 0

        if subcommand == "refine":
            if len(tail) < 2:
                stdout.write("usage: workflow manifest refine <manifest_id> <instruction...>\n")
                return 2
            manifest_id = tail[0].strip()
            instruction = " ".join(tail[1:]).strip()
            from runtime.canonical_manifests import refine_manifest

            result = refine_manifest(
                conn,
                generator=subsystems.get_manifest_generator(),
                manifest_id=manifest_id,
                instruction=instruction,
            )
            print_json(
                stdout,
                {
                    "manifest_id": result.manifest_id,
                    "manifest": result.manifest,
                    "version": result.version,
                    "confidence": result.confidence,
                    "explanation": result.explanation,
                },
            )
            return 0

        if subcommand == "save":
            input_json = None
            input_file = None
            i = 0
            while i < len(tail):
                if tail[i] == "--input-json" and i + 1 < len(tail):
                    input_json = tail[i + 1]
                    i += 2
                elif tail[i] == "--input-file" and i + 1 < len(tail):
                    input_file = tail[i + 1]
                    i += 2
                else:
                    stdout.write(f"unknown argument: {tail[i]}\n")
                    return 2
            payload = _load_input_payload(input_json=input_json, input_file=input_file)
            from runtime.canonical_manifests import save_manifest
            from surfaces.api.handlers.workflow_run import _extract_manifest_save_payload

            manifest_id, name, description, manifest = _extract_manifest_save_payload(payload)
            saved = save_manifest(
                conn,
                manifest_id=manifest_id,
                name=name,
                description=description,
                manifest=manifest,
            )
            print_json(stdout, _manifest_record_payload(saved))
            return 0

        if subcommand == "save-as":
            input_json = None
            input_file = None
            name = None
            description = ""
            i = 0
            while i < len(tail):
                if tail[i] == "--input-json" and i + 1 < len(tail):
                    input_json = tail[i + 1]
                    i += 2
                elif tail[i] == "--input-file" and i + 1 < len(tail):
                    input_file = tail[i + 1]
                    i += 2
                elif tail[i] == "--name" and i + 1 < len(tail):
                    name = tail[i + 1].strip()
                    i += 2
                elif tail[i] == "--description" and i + 1 < len(tail):
                    description = tail[i + 1]
                    i += 2
                else:
                    stdout.write(f"unknown argument: {tail[i]}\n")
                    return 2
            payload = _load_input_payload(input_json=input_json, input_file=input_file)
            from runtime.canonical_manifests import save_manifest_as

            body_name = str(payload.get("name") or "").strip()
            body_description = str(payload.get("description") or "").strip()
            manifest = payload.get("manifest") if isinstance(payload.get("manifest"), dict) else payload
            saved = save_manifest_as(
                conn,
                name=name or body_name,
                description=description or body_description,
                manifest=manifest,
            )
            print_json(stdout, _manifest_record_payload(saved))
            return 0
    except ValueError as exc:
        stdout.write(f"error: {exc}\n")
        return 2
    except Exception as exc:
        print_json(stdout, {"error": str(exc)})
        return 1

    stdout.write(f"unknown manifest subcommand: {subcommand}\n")
    return 2


def _triggers_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle workflow trigger list/create/update through the CLI."""

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow triggers list\n"
            "       workflow triggers create (--input-json <json> | --input-file <path>)\n"
            "       workflow triggers update <trigger_id> (--input-json <json> | --input-file <path>)\n"
        )
        return 2

    subcommand = args[0]
    tail = args[1:]
    query_mod = _workflow_query_mod()
    conn = _workflow_subsystems().get_pg_conn()

    try:
        if subcommand == "list":
            rows = conn.execute(
                """SELECT t.*, w.name AS workflow_name
                   FROM public.workflow_triggers t
                   JOIN public.workflows w ON w.id = t.workflow_id
                   ORDER BY t.created_at DESC"""
            )
            payload = {
                "triggers": [query_mod._trigger_to_dict(dict(row)) for row in (rows or [])],
                "count": len(rows or []),
            }
            print_json(stdout, payload)
            return 0

        input_json = None
        input_file = None
        trigger_id = None
        i = 0
        while i < len(tail):
            if tail[i] == "--input-json" and i + 1 < len(tail):
                input_json = tail[i + 1]
                i += 2
            elif tail[i] == "--input-file" and i + 1 < len(tail):
                input_file = tail[i + 1]
                i += 2
            elif subcommand == "update" and trigger_id is None:
                trigger_id = tail[i].strip()
                i += 1
            else:
                stdout.write(f"unknown argument: {tail[i]}\n")
                return 2

        payload = _load_input_payload(input_json=input_json, input_file=input_file)

        if subcommand == "create":
            error = query_mod._validate_trigger_body(
                payload,
                require_workflow_id=True,
                require_event_type=True,
            )
            if error:
                stdout.write(f"error: {error}\n")
                return 2
            from runtime.canonical_workflows import save_workflow_trigger

            row = save_workflow_trigger(conn, body=payload)
            print_json(stdout, {"trigger": query_mod._trigger_to_dict(dict(row))})
            return 0

        if subcommand == "update":
            if not trigger_id:
                stdout.write("usage: workflow triggers update <trigger_id> (--input-json <json> | --input-file <path>)\n")
                return 2
            error = query_mod._validate_trigger_body(
                payload,
                require_workflow_id=False,
                require_event_type=False,
            )
            if error:
                stdout.write(f"error: {error}\n")
                return 2
            from runtime.canonical_workflows import update_workflow_trigger

            row = update_workflow_trigger(
                conn,
                trigger_id=trigger_id,
                body=payload,
            )
            print_json(stdout, {"trigger": query_mod._trigger_to_dict(dict(row))})
            return 0
    except ValueError as exc:
        stdout.write(f"error: {exc}\n")
        return 2
    except Exception as exc:
        print_json(stdout, {"error": str(exc)})
        return 1

    stdout.write(f"unknown triggers subcommand: {subcommand}\n")
    return 2


def _deprecated_workflow_records_alias_command(
    alias: str,
    *,
    stdout: TextIO,
) -> int:
    stdout.write(
        f"error: workflow {alias} is deprecated; use workflow records list|get|create|update|rename instead\n"
    )
    return 2


def _records_help_text() -> str:
    return (
        "usage: workflow records list [--never-run] [--limit N] [--include-definition] [--json]\n"
        "       workflow records get <workflow_id> [--include-definition] [--json]\n"
        "       workflow records create (--input-json <json> | --input-file <path>) [--json]\n"
        "       workflow records update <workflow_id> (--input-json <json> | --input-file <path>) [--json]\n"
        "       workflow records rename <workflow_id> --to <new_workflow_id> [--name <display_name>] [--json]\n"
    )


def _records_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle workflow record reads and writes through the CLI."""

    if not args:
        stdout.write(_records_help_text())
        return 2
    if args[0] in {"-h", "--help"}:
        stdout.write(_records_help_text())
        return 0

    subcommand = args[0]
    if any(token in {"-h", "--help"} for token in args[1:]):
        stdout.write(_records_help_text())
        return 0
    tail = args[1:]
    query_mod = _workflow_query_mod()

    try:
        input_json = None
        input_file = None
        workflow_id = None
        new_workflow_id = None
        rename_name = None
        include_definition = False
        never_run = False
        limit = 100
        i = 0
        while i < len(tail):
            if tail[i] == "--input-json" and i + 1 < len(tail):
                input_json = tail[i + 1]
                i += 2
            elif tail[i] == "--input-file" and i + 1 < len(tail):
                input_file = tail[i + 1]
                i += 2
            elif tail[i] == "--to" and i + 1 < len(tail):
                new_workflow_id = tail[i + 1]
                i += 2
            elif tail[i] == "--name" and i + 1 < len(tail):
                rename_name = tail[i + 1]
                i += 2
            elif tail[i] == "--include-definition":
                include_definition = True
                i += 1
            elif tail[i] == "--json":
                i += 1
            elif tail[i] == "--never-run":
                never_run = True
                i += 1
            elif tail[i] == "--limit" and i + 1 < len(tail):
                limit = int(tail[i + 1])
                i += 2
            elif subcommand == "get" and workflow_id is None:
                workflow_id = tail[i].strip()
                i += 1
            elif subcommand == "update" and workflow_id is None:
                workflow_id = tail[i].strip()
                i += 1
            elif subcommand == "rename" and workflow_id is None:
                workflow_id = tail[i].strip()
                i += 1
            else:
                stdout.write(f"unknown argument: {tail[i]}\n")
                return 2

        if subcommand == "list":
            if input_json is not None or input_file is not None or workflow_id or new_workflow_id or rename_name:
                stdout.write(
                    "usage: workflow records list [--never-run] [--limit N] "
                    "[--include-definition] [--json]\n"
                )
                return 2
            from storage.postgres.workflow_runtime_repository import list_workflow_records

            conn = _workflow_subsystems().get_pg_conn()
            rows = list_workflow_records(
                conn,
                never_run=never_run,
                limit=limit,
            )
            workflows = [
                query_mod._workflow_to_dict(dict(row), include_definition=include_definition)
                for row in rows
            ]
            print_json(
                stdout,
                {
                    "workflows": workflows,
                    "count": len(workflows),
                    "filters": {
                        "never_run": never_run,
                        "limit": max(0, min(int(limit), 500)),
                        "include_definition": include_definition,
                    },
                    "source_authority": "public.workflows",
                },
            )
            return 0

        if subcommand == "get":
            if input_json is not None or input_file is not None or not workflow_id:
                stdout.write("usage: workflow records get <workflow_id> [--include-definition] [--json]\n")
                return 2
            from storage.postgres.workflow_runtime_repository import load_workflow_record

            conn = _workflow_subsystems().get_pg_conn()
            row = load_workflow_record(conn, workflow_id=workflow_id)
            if row is None:
                print_json(
                    stdout,
                    {
                        "error": f"workflow not found: {workflow_id}",
                        "status": "not_found",
                        "reason_code": "workflow_records.not_found",
                        "workflow_id": workflow_id,
                    },
                )
                return 1
            print_json(
                stdout,
                {"workflow": query_mod._workflow_to_dict(dict(row), include_definition=include_definition)},
            )
            return 0

        if subcommand == "rename":
            if input_json is not None or input_file is not None:
                stdout.write("error: rename does not accept input-json or input-file\n")
                return 2
            if not workflow_id or not new_workflow_id:
                stdout.write(
                    "usage: workflow records rename <workflow_id> --to <new_workflow_id> "
                    "[--name <display_name>] [--json]\n"
                )
                return 2
            from runtime.canonical_workflows import rename_workflow

            conn = _workflow_subsystems().get_pg_conn()
            row = rename_workflow(
                conn,
                workflow_id=workflow_id,
                new_workflow_id=new_workflow_id,
                name=rename_name,
                operator_surface="workflow records",
            )
            print_json(stdout, {"workflow": query_mod._workflow_to_dict(dict(row), include_definition=True)})
            return 0

        payload = _load_input_payload(input_json=input_json, input_file=input_file)

        if subcommand == "create":
            error = query_mod._validate_workflow_body(
                payload,
                require_name=True,
                require_definition=True,
            )
            if error:
                stdout.write(f"error: {error}\n")
                return 2
            from runtime.canonical_workflows import save_workflow

            conn = _workflow_subsystems().get_pg_conn()
            row = save_workflow(conn, workflow_id=None, body=payload)
            print_json(stdout, {"workflow": query_mod._workflow_to_dict(dict(row), include_definition=True)})
            return 0

        if subcommand == "update":
            if not workflow_id:
                stdout.write(
                    "usage: workflow records update <workflow_id> "
                    "(--input-json <json> | --input-file <path>) [--json]\n"
                )
                return 2
            error = query_mod._validate_workflow_body(
                payload,
                require_name=False,
                require_definition=False,
            )
            if error:
                stdout.write(f"error: {error}\n")
                return 2
            if not payload:
                stdout.write("error: No workflow fields provided for update\n")
                return 2
            from runtime.canonical_workflows import save_workflow

            conn = _workflow_subsystems().get_pg_conn()
            row = save_workflow(conn, workflow_id=workflow_id, body=payload)
            print_json(stdout, {"workflow": query_mod._workflow_to_dict(dict(row), include_definition=True)})
            return 0
    except ValueError as exc:
        stdout.write(f"error: {exc}\n")
        return 2
    except Exception as exc:
        reason_code = (
            "workflow_records.db_authority_unavailable"
            if exc.__class__.__name__ == "PostgresConfigurationError"
            else "workflow_records.error"
        )
        print_json(
            stdout,
            {
                "error": str(exc),
                "status": "error",
                "reason_code": reason_code,
                "source_authority": "public.workflows",
            },
        )
        return 1

    stdout.write(f"unknown records subcommand: {subcommand}\n")
    return 2


def _retry_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow retry` — retry a failed job."""
    import argparse

    parser = argparse.ArgumentParser(prog="workflow retry")
    parser.add_argument("run_id", help="Workflow run id")
    parser.add_argument("label", help="Job label to retry")
    parser.add_argument(
        "--previous-failure",
        required=True,
        help="Receipt-backed failure being retried.",
    )
    parser.add_argument(
        "--retry-delta",
        required=True,
        help="What is materially different about this attempt.",
    )

    try:
        parsed = _parse_args(parser, args, stdout=stdout)
    except SystemExit as exc:
        return exc.code

    from surfaces.cli import workflow_cli
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stdout):
        return workflow_cli.cmd_retry(parsed)


def _cancel_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow cancel` — cancel a run."""
    import argparse

    parser = argparse.ArgumentParser(prog="workflow cancel")
    parser.add_argument("run_id", help="Workflow run id to cancel")

    try:
        parsed = _parse_args(parser, args, stdout=stdout)
    except SystemExit as exc:
        return exc.code

    from surfaces.cli import workflow_cli
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stdout):
        return workflow_cli.cmd_cancel(parsed)


def _repair_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow repair` — repair run sync state and inspect repair queue."""
    import argparse

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow repair <run_id>|queue|claim|release|complete|summary [options]\n"
            "\n"
            "  <run_id>       Repair post-run sync state for one workflow run\n"
            "  queue          List durable repair queue items\n"
            "  claim          Claim the next queued repair item\n"
            "  release        Return a claimed repair item to queued\n"
            "  complete       Mark a repair item terminal\n"
            "  summary        Count repair items by scope/status\n"
        )
        return 0 if args and args[0] in {"-h", "--help"} else 2

    if args and args[0] in {"queue", "list"}:
        parser = argparse.ArgumentParser(prog=f"workflow repair {args[0]}")
        parser.add_argument("--status", default="queued", help="Queue status filter")
        parser.add_argument("--scope", choices=("solution", "workflow", "job"), help="Repair scope filter")
        parser.add_argument("--run-id", help="Workflow run id filter")
        parser.add_argument("--solution-id", help="Solution id filter")
        parser.add_argument("--limit", type=int, default=50, help="Max rows to return")
        try:
            parsed = _parse_args(parser, args[1:], stdout=stdout)
        except SystemExit as exc:
            return exc.code
        try:
            payload = _workflow_operation(
                "workflow_repair_queue.status",
                {
                    "action": "list",
                    "queue_status": parsed.status,
                    "repair_scope": parsed.scope,
                    "run_id": parsed.run_id,
                    "solution_id": parsed.solution_id,
                    "limit": parsed.limit,
                },
            )
        except Exception as exc:
            stdout.write(json.dumps({"status": "failed", "error": str(exc)}, indent=2, default=str) + "\n")
            return 1
        stdout.write(json.dumps(payload, indent=2, default=str) + "\n")
        return 0

    if args and args[0] == "claim":
        parser = argparse.ArgumentParser(prog="workflow repair claim")
        parser.add_argument("--scope", choices=("solution", "workflow", "job"), help="Repair scope filter")
        parser.add_argument("--claimed-by", default=f"cli.workflow.repair:{os.getpid()}", help="Repair worker id")
        parser.add_argument("--ttl-minutes", type=int, default=30, help="Claim lease duration")
        try:
            parsed = _parse_args(parser, args[1:], stdout=stdout)
        except SystemExit as exc:
            return exc.code
        try:
            payload = _workflow_operation(
                "workflow_repair_queue.command",
                {
                    "action": "claim",
                    "claimed_by": parsed.claimed_by,
                    "repair_scope": parsed.scope,
                    "claim_ttl_minutes": parsed.ttl_minutes,
                },
            )
        except Exception as exc:
            stdout.write(json.dumps({"status": "failed", "error": str(exc)}, indent=2, default=str) + "\n")
            return 1
        stdout.write(json.dumps(payload, indent=2, default=str) + "\n")
        return 0 if payload.get("status") in {"claimed", "empty"} else 1

    if args and args[0] == "complete":
        parser = argparse.ArgumentParser(prog="workflow repair complete")
        parser.add_argument("repair_id", help="Repair queue id")
        parser.add_argument(
            "--status",
            choices=("completed", "failed", "cancelled", "superseded"),
            default="completed",
            help="Terminal queue status",
        )
        parser.add_argument("--result-ref", help="Optional result or evidence ref")
        parser.add_argument("--note", help="Optional repair note")
        try:
            parsed = _parse_args(parser, args[1:], stdout=stdout)
        except SystemExit as exc:
            return exc.code
        try:
            payload = _workflow_operation(
                "workflow_repair_queue.command",
                {
                    "action": "complete",
                    "repair_id": parsed.repair_id,
                    "queue_status": parsed.status,
                    "result_ref": parsed.result_ref,
                    "repair_note": parsed.note,
                },
            )
        except Exception as exc:
            stdout.write(json.dumps({"status": "failed", "error": str(exc)}, indent=2, default=str) + "\n")
            return 1
        stdout.write(json.dumps(payload, indent=2, default=str) + "\n")
        return 0 if payload.get("status") == "updated" else 1

    if args and args[0] == "release":
        parser = argparse.ArgumentParser(prog="workflow repair release")
        parser.add_argument("repair_id", help="Repair queue id")
        parser.add_argument("--note", help="Optional release note")
        try:
            parsed = _parse_args(parser, args[1:], stdout=stdout)
        except SystemExit as exc:
            return exc.code
        try:
            payload = _workflow_operation(
                "workflow_repair_queue.command",
                {
                    "action": "release",
                    "repair_id": parsed.repair_id,
                    "repair_note": parsed.note,
                },
            )
        except Exception as exc:
            stdout.write(json.dumps({"status": "failed", "error": str(exc)}, indent=2, default=str) + "\n")
            return 1
        stdout.write(json.dumps(payload, indent=2, default=str) + "\n")
        return 0 if payload.get("status") == "released" else 1

    if args and args[0] == "summary":
        parser = argparse.ArgumentParser(prog="workflow repair summary")
        try:
            _parse_args(parser, args[1:], stdout=stdout)
        except SystemExit as exc:
            return exc.code
        try:
            payload = _workflow_operation(
                "workflow_repair_queue.status",
                {"action": "summary"},
            )
        except Exception as exc:
            stdout.write(json.dumps({"status": "failed", "error": str(exc)}, indent=2, default=str) + "\n")
            return 1
        stdout.write(json.dumps(payload, indent=2, default=str) + "\n")
        return 0

    parser = argparse.ArgumentParser(prog="workflow repair")
    parser.add_argument("run_id", help="Workflow run id to repair")

    try:
        parsed = _parse_args(parser, args, stdout=stdout)
    except SystemExit as exc:
        return exc.code

    from surfaces.cli import workflow_cli
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stdout):
        return workflow_cli.cmd_repair(parsed)


def _work_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow work <claim|acknowledge>` for worker subscription state."""

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow work <claim|acknowledge> [options]\n"
            "\n"
            "  claim         Read claimable worker work for a subscription/run pair\n"
            "  acknowledge   Commit a worker batch acknowledgement checkpoint\n"
            "\n"
            "  claim options:\n"
            "    --subscription-id <id>        Durable subscription id\n"
            "    --run-id <run_id>             Workflow run id\n"
            "    --last-acked-evidence-seq N    Optional last acknowledged evidence seq\n"
            "    --limit N                     Max facts to read (default: 100)\n"
            "\n"
            "  acknowledge options:\n"
            "    --work-json <json>             Serialized claim payload from workflow work claim\n"
            "    --work-file <path>             Read the serialized claim payload from a file\n"
            "    --through-evidence-seq N       Optional explicit ack watermark\n"
            "    --yes                         Required to commit the acknowledgement\n"
        )
        return 2

    subcommand = args[0]
    tail = args[1:]

    if subcommand == "claim":
        subscription_id = ""
        run_id = ""
        last_acked_evidence_seq = None
        limit = 100
        i = 0
        while i < len(tail):
            if tail[i] == "--subscription-id" and i + 1 < len(tail):
                subscription_id = tail[i + 1]
                i += 2
            elif tail[i] == "--run-id" and i + 1 < len(tail):
                run_id = tail[i + 1]
                i += 2
            elif tail[i] == "--last-acked-evidence-seq" and i + 1 < len(tail):
                try:
                    last_acked_evidence_seq = int(tail[i + 1])
                except ValueError:
                    stdout.write(
                        f"error: --last-acked-evidence-seq must be an integer, got: {tail[i + 1]}\n"
                    )
                    return 2
                i += 2
            elif tail[i] == "--limit" and i + 1 < len(tail):
                try:
                    limit = int(tail[i + 1])
                except ValueError:
                    stdout.write(f"error: --limit must be an integer, got: {tail[i + 1]}\n")
                    return 2
                i += 2
            else:
                stdout.write(f"error: unknown argument: {tail[i]}\n")
                return 2

        if not subscription_id or not run_id:
            stdout.write(
                "usage: workflow work claim --subscription-id <id> --run-id <run_id> [--last-acked-evidence-seq N] [--limit N]\n"
            )
            return 2

        exit_code, payload = run_cli_tool(
            "praxis_workflow",
            {
                "action": "claim",
                "subscription_id": subscription_id,
                "run_id": run_id,
                "last_acked_evidence_seq": last_acked_evidence_seq,
                "limit": limit,
            },
        )
        print_json(stdout, payload)
        return exit_code

    if subcommand in {"ack", "acknowledge"}:
        work_json: str | None = None
        work_file: str | None = None
        through_evidence_seq = None
        yes = False
        i = 0
        while i < len(tail):
            if tail[i] == "--work-json" and i + 1 < len(tail):
                work_json = tail[i + 1]
                i += 2
            elif tail[i] == "--work-file" and i + 1 < len(tail):
                work_file = tail[i + 1]
                i += 2
            elif tail[i] == "--through-evidence-seq" and i + 1 < len(tail):
                try:
                    through_evidence_seq = int(tail[i + 1])
                except ValueError:
                    stdout.write(
                        f"error: --through-evidence-seq must be an integer, got: {tail[i + 1]}\n"
                    )
                    return 2
                i += 2
            elif tail[i] == "--yes":
                yes = True
                i += 1
            else:
                stdout.write(f"error: unknown argument: {tail[i]}\n")
                return 2

        if not yes:
            stdout.write("error: --yes is required to acknowledge worker work\n")
            return 2

        if work_json is not None and work_file is not None:
            stdout.write("error: pass only one of --work-json or --work-file\n")
            return 2
        if work_json is not None:
            work_payload = json.loads(work_json)
        elif work_file is not None:
            work_payload = load_json_file(work_file)
        else:
            stdout.write(
                "usage: workflow work acknowledge --work-json <json> | --work-file <path> [--through-evidence-seq N] --yes\n"
            )
            return 2

        exit_code, payload = run_cli_tool(
            "praxis_workflow",
            {
                "action": "acknowledge",
                "work": work_payload,
                "through_evidence_seq": through_evidence_seq,
            },
        )
        print_json(stdout, payload)
        return exit_code

    stdout.write(f"unknown work subcommand: {subcommand}\n")
    return 2


def _parse_positive_cli_int(value: str, *, field_name: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a positive integer, got: {value}") from exc
    if parsed <= 0:
        raise ValueError(f"{field_name} must be a positive integer, got: {value}")
    return parsed


def _parse_runtime_truth_args(
    args: list[str],
    *,
    include_manifest_audit_limit: bool,
) -> dict[str, object]:
    params: dict[str, object] = {}
    i = 0
    while i < len(args):
        token = args[i]
        if token == "--json":
            i += 1
            continue
        if token == "--run-id" and i + 1 < len(args):
            params["run_id"] = args[i + 1]
            i += 2
            continue
        if token == "--since-minutes" and i + 1 < len(args):
            params["since_minutes"] = _parse_positive_cli_int(
                args[i + 1],
                field_name="since_minutes",
            )
            i += 2
            continue
        if token == "--heartbeat-fresh-seconds" and i + 1 < len(args):
            params["heartbeat_fresh_seconds"] = _parse_positive_cli_int(
                args[i + 1],
                field_name="heartbeat_fresh_seconds",
            )
            i += 2
            continue
        if include_manifest_audit_limit and token == "--manifest-audit-limit" and i + 1 < len(args):
            params["manifest_audit_limit"] = _parse_positive_cli_int(
                args[i + 1],
                field_name="manifest_audit_limit",
            )
            i += 2
            continue
        raise ValueError(f"unknown argument: {token}")
    return params


def _runtime_truth_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow runtime-truth`."""

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow runtime-truth [--run-id ID] [--since-minutes N] "
            "[--heartbeat-fresh-seconds N] [--manifest-audit-limit N] [--json]\n"
        )
        return 0
    try:
        params = _parse_runtime_truth_args(args, include_manifest_audit_limit=True)
    except ValueError as exc:
        stdout.write(f"error: {exc}\n")
        return 2
    exit_code, payload = run_cli_tool("praxis_runtime_truth_snapshot", params)
    print_json(stdout, payload)
    return exit_code


def _firecheck_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow firecheck`."""

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow firecheck [--run-id ID] [--since-minutes N] "
            "[--heartbeat-fresh-seconds N] [--json]\n"
        )
        return 0
    try:
        params = _parse_runtime_truth_args(args, include_manifest_audit_limit=False)
    except ValueError as exc:
        stdout.write(f"error: {exc}\n")
        return 2
    exit_code, payload = run_cli_tool("praxis_firecheck", params)
    print_json(stdout, payload)
    if exit_code != 0:
        return exit_code
    if isinstance(payload, dict) and payload.get("can_fire") is False:
        return 1
    if isinstance(payload, dict) and payload.get("ok") is False:
        return 1
    return 0


def _remediation_plan_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow remediation-plan`."""

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow remediation-plan [--failure-type TYPE] [--failure-code CODE] "
            "[--stderr TEXT] [--run-id ID] [--json]\n"
        )
        return 0
    params: dict[str, object] = {}
    i = 0
    while i < len(args):
        token = args[i]
        if token == "--json":
            i += 1
            continue
        if token in {"--failure-type", "--failure-code", "--stderr", "--run-id"} and i + 1 < len(args):
            params[token[2:].replace("-", "_")] = args[i + 1]
            i += 2
            continue
        stdout.write(f"error: unknown argument: {token}\n")
        return 2
    exit_code, payload = run_cli_tool("praxis_remediation_plan", params)
    print_json(stdout, payload)
    return exit_code


def _remediation_apply_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow remediation-apply`."""

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow remediation-apply [--failure-type TYPE] [--failure-code CODE] "
            "[--blocker-code CODE] [--stderr TEXT] [--run-id ID] [--provider SLUG] "
            "[--stale-after-seconds N] [--dry-run|--apply --yes] [--json]\n"
        )
        return 0
    params: dict[str, object] = {"dry_run": True, "confirm": False}
    i = 0
    while i < len(args):
        token = args[i]
        if token == "--json":
            i += 1
            continue
        if token == "--dry-run":
            params["dry_run"] = True
            i += 1
            continue
        if token == "--apply":
            params["dry_run"] = False
            i += 1
            continue
        if token == "--yes":
            params["confirm"] = True
            i += 1
            continue
        if token in {
            "--failure-type",
            "--failure-code",
            "--blocker-code",
            "--stderr",
            "--run-id",
        } and i + 1 < len(args):
            params[token[2:].replace("-", "_")] = args[i + 1]
            i += 2
            continue
        if token == "--provider" and i + 1 < len(args):
            params["provider_slug"] = args[i + 1]
            i += 2
            continue
        if token == "--stale-after-seconds" and i + 1 < len(args):
            try:
                params["stale_after_seconds"] = _parse_positive_cli_int(
                    args[i + 1],
                    field_name="stale_after_seconds",
                )
            except ValueError as exc:
                stdout.write(f"error: {exc}\n")
                return 2
            i += 2
            continue
        stdout.write(f"error: unknown argument: {token}\n")
        return 2
    if params.get("dry_run") is False and params.get("confirm") is not True:
        stdout.write("error: --yes is required with --apply\n")
        return 2
    exit_code, payload = run_cli_tool("praxis_remediation_apply", params)
    print_json(stdout, payload)
    return exit_code


def _active_command(*, stdout: TextIO) -> int:
    """Handle `workflow active` — list currently running workflows from DB authority."""

    import json as _json

    exit_code, snapshot = run_cli_tool(
        "praxis_status_snapshot",
        {
            "since_hours": 24,
        },
    )
    if exit_code != 0:
        print_json(stdout, snapshot)
        return exit_code

    runs = snapshot.get("in_flight_workflows")
    if not isinstance(runs, list):
        runs = []
    active_ids = [
        str(run.get("run_id"))
        for run in runs
        if isinstance(run, dict) and str(run.get("run_id") or "").strip()
    ]
    payload = {
        "active_runs": active_ids,
        "count": len(active_ids),
        "runs": runs,
        "queue": {
            "depth": snapshot.get("queue_depth"),
            "status": snapshot.get("queue_depth_status"),
            "pending": snapshot.get("queue_depth_pending"),
            "ready": snapshot.get("queue_depth_ready"),
            "claimed": snapshot.get("queue_depth_claimed"),
            "running": snapshot.get("queue_depth_running"),
            "total": snapshot.get("queue_depth_total"),
        },
        "metrics": {
            "since_hours": snapshot.get("since_hours"),
            "pass_rate": snapshot.get("pass_rate"),
            "adjusted_pass_rate": snapshot.get("adjusted_pass_rate"),
            "observability_state": snapshot.get("observability_state"),
        },
        "source": "praxis_status_snapshot",
    }

    stdout.write(_json.dumps(payload, indent=2, default=str) + "\n")
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

    worker [--max-concurrent N] [--poll-interval 2.0] [--capabilities a,b]
        Start the workflow worker that polls and executes jobs.

    cancel <job_id>
        Cancel a pending or claimed job.
    """

    import json as _json

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow queue <submit|stats|list|worker|cancel> [args]\n"
            "\n"
            "  submit <spec.json> [--priority N] [--max-attempts N]\n"
            "  stats\n"
            "  list [--status pending] [--limit 50]\n"
            "  worker [--max-concurrent N] [--poll-interval 2.0] [--capabilities a,b]\n"
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

        from runtime.control_commands import (
            render_workflow_submit_response,
            request_workflow_submit_command,
        )
        from runtime.workflow_spec import WorkflowSpec

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
            result = render_workflow_submit_response(
                request_workflow_submit_command(
                    conn,
                    requested_by_kind="cli",
                    requested_by_ref="workflow.queue.submit",
                    inline_spec=spec_dict,
                ),
                spec_name=str(spec_dict.get("name") or getattr(spec, "name", "inline")),
                total_jobs=len(spec_dict.get("jobs", [])),
            )
        except Exception as exc:
            stdout.write(f"error: failed to submit workflow: {exc}\n")
            return 1

        if result.get("status") != "queued":
            stdout.write(_json.dumps(result, indent=2) + "\n")
            return 1

        stdout.write(
            _json.dumps(
                {
                    "run_id": result["run_id"],
                    "status": result["status"],
                    "total_jobs": result.get("total_jobs", 0),
                    "command_id": result.get("command_id"),
                    "command_status": result.get("command_status"),
                    "result_ref": result.get("result_ref"),
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
        max_concurrent: int | None = None
        poll_interval = 2.0
        capabilities: list[str] | None = None

        i = 0
        while i < len(sub_args):
            if sub_args[i] in {"--max-concurrent", "--concurrency"} and i + 1 < len(sub_args):
                try:
                    max_concurrent = int(sub_args[i + 1])
                except ValueError:
                    stdout.write(
                        f"error: {sub_args[i]} must be an integer, got: {sub_args[i + 1]}\n"
                    )
                    return 2
                if max_concurrent < 1:
                    stdout.write(
                        f"error: {sub_args[i]} must be a positive integer, got: {sub_args[i + 1]}\n"
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

        from runtime.workflow._worker_loop import resolve_worker_concurrency, run_worker_loop

        try:
            concurrency_decision = (
                {
                    "max_concurrent": max_concurrent,
                    "source": "cli",
                    "cpu_count": None,
                    "available_memory_bytes": None,
                    "memory_slot_bytes": None,
                }
                if max_concurrent is not None
                else resolve_worker_concurrency()
            )
        except ValueError as exc:
            stdout.write(f"error: {exc}\n")
            return 2

        conn = _workflow_runtime_conn()
        worker_id = f"workflow-worker-{os.getpid()}"
        stdout.write(
            _json.dumps(
                {
                    "worker_id": worker_id,
                    "max_concurrent": concurrency_decision["max_concurrent"],
                    "concurrency_source": concurrency_decision["source"],
                    "cpu_count": concurrency_decision["cpu_count"],
                    "available_memory_bytes": concurrency_decision["available_memory_bytes"],
                    "memory_slot_bytes": concurrency_decision["memory_slot_bytes"],
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
            from runtime.control_commands import (
                ControlCommandType,
                ControlIntent,
                execute_control_intent,
                render_control_command_response,
            )

            conn = _workflow_runtime_conn()
            rows = conn.execute(
                """SELECT run_id
                   FROM workflow_jobs
                   WHERE id = $1::bigint
                   LIMIT 1""",
                job_id,
            )
            if not rows:
                stdout.write(f"error: workflow job not found: {job_id}\n")
                return 1
            run_id = str(rows[0]["run_id"])
            command = execute_control_intent(
                conn,
                ControlIntent(
                    command_type=ControlCommandType.WORKFLOW_CANCEL,
                    requested_by_kind="cli",
                    requested_by_ref="workflow.queue.cancel",
                    idempotency_key=f"workflow.cancel.cli.queue.{job_id}",
                    payload={"run_id": run_id, "include_running": True},
                ),
                approved_by="cli.workflow.queue.cancel",
            )
            result = render_control_command_response(
                conn,
                command,
                action="cancel",
                run_id=run_id,
                job_id=job_id,
            )
        except Exception as exc:
            stdout.write(f"error: failed to cancel job: {exc}\n")
            return 1

        if result.get("status") == "cancelled":
            stdout.write(_json.dumps(result, indent=2) + "\n")
            return 0

        stdout.write(_json.dumps(result, indent=2) + "\n")
        return 1

    stdout.write(f"unknown queue subcommand: {subcommand}\n")
    return 2
