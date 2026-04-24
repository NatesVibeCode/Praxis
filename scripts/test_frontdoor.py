#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_ROOT = REPO_ROOT / "Code&DBs" / "Workflow"
DEFAULT_DB_URL = os.environ.get("WORKFLOW_DATABASE_URL", "")
CACHE_DIR = REPO_ROOT / ".cache" / "dag-test"
FOCUS_FILE = CACHE_DIR / "focused_suite.json"
DEFAULT_SUITE = "workflow_first_slice"
USAGE = (
    "usage: ./scripts/test.sh "
    "suite list|suite focus|plan|check-affected|validate|selftest|moon-style-lint"
)

SUITE_DEFINITIONS: dict[str, dict[str, Any]] = {
    "workflow_first_slice": {
        "description": "First runnable slice covering workflow front-door validation and the vertical-slice contract checks.",
        "paths": [
            "Code&DBs/Workflow/tests/contracts/test_workflow_intake_contract.py",
            "Code&DBs/Workflow/tests/contracts/test_inspect_replay_contracts.py",
            "Code&DBs/Workflow/tests/contracts/test_deterministic_task_adapter_contract.py",
            "Code&DBs/Workflow/tests/contracts/test_evidence_contracts.py",
            "Code&DBs/Workflow/tests/integration/test_first_slice_end_to_end.py",
            "Code&DBs/Workflow/tests/integration/test_workflow_intake_slice.py",
            "Code&DBs/Workflow/tests/integration/test_inspect_and_replay_slice.py",
            "Code&DBs/Workflow/tests/integration/test_cli_inspect_replay.py",
            "Code&DBs/Workflow/tests/integration/test_native_frontdoor.py",
            "Code&DBs/Workflow/tests/integration/test_repo_local_primary_operator_entrypoint.py",
            "Code&DBs/Workflow/tests/unit/test_workflow_cli_validate.py",
            "Code&DBs/Workflow/tests/unit/test_workflow_cli_run.py",
        ],
    },
    "unit": {
        "description": "Fast module, policy, and validator tests.",
        "paths": ["Code&DBs/Workflow/tests/unit"],
    },
    "integration": {
        "description": "End-to-end workflow, persistence, replay, and surface interaction tests.",
        "paths": ["Code&DBs/Workflow/tests/integration"],
    },
    "contracts": {
        "description": "Contract tests that prove the boundary envelopes and replay rules.",
        "paths": ["Code&DBs/Workflow/tests/contracts"],
    },
    "all": {
        "description": "The whole workflow test tree.",
        "paths": ["Code&DBs/Workflow/tests"],
    },
}

PATH_KEYS = {
    "registry_paths",
    "read_scope",
    "write_scope",
    "reused_queue_refs",
    "read",
    "write",
}


def _command_text(argv: list[str]) -> str:
    parts = ["./scripts/test.sh", *argv]
    return " ".join(shlex.quote(part) for part in parts)


def _workflow_command_text(argv: list[str]) -> str:
    parts = ["./scripts/workflow.sh", *argv]
    return " ".join(shlex.quote(part) for part in parts)


def _pytest_command_text(command: list[str]) -> str:
    return (
        ". ./scripts/_workflow_env.sh && workflow_load_repo_env && PYTHONPATH="
        + shlex.quote(str(WORKFLOW_ROOT))
        + " "
        + " ".join(shlex.quote(part) for part in command)
    )


def _suite_command_text(suite_name: str, *, collect_only: bool = False) -> str:
    paths = _suite_paths(suite_name)
    return _pytest_command_text(_pytest_command(paths, collect_only=collect_only))


def _pytest_command(paths: list[str], *, collect_only: bool = False) -> list[str]:
    command = [sys.executable, "-m", "pytest", "--noconftest", "-q"]
    if collect_only:
        command.append("--collect-only")
    command.extend(paths)
    return command


def _subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        str(WORKFLOW_ROOT)
        if not existing
        else f"{WORKFLOW_ROOT}{os.pathsep}{existing}"
    )
    if DEFAULT_DB_URL:
        env.setdefault("WORKFLOW_DATABASE_URL", DEFAULT_DB_URL)
    env["PATH"] = env.get("PATH", "")
    return env


def _load_focus() -> str | None:
    try:
        payload = json.loads(FOCUS_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        return None
    focus = payload.get("suite") if isinstance(payload, dict) else None
    return focus if isinstance(focus, str) and focus in SUITE_DEFINITIONS else None


def _store_focus(suite_name: str) -> dict[str, Any]:
    previous = _load_focus()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    FOCUS_FILE.write_text(
        json.dumps({"suite": suite_name, "updated_at": time.time()}, indent=2),
        encoding="utf-8",
    )
    return {"current_focus": suite_name, "previous_focus": previous}


def _suite_paths(suite_name: str) -> list[str]:
    spec = SUITE_DEFINITIONS[suite_name]
    return [str(path) for path in spec["paths"]]


def _suite_list_result() -> dict[str, Any]:
    focus = _load_focus() or DEFAULT_SUITE
    suites = []
    for name, spec in SUITE_DEFINITIONS.items():
        suites.append(
            {
                "name": name,
                "description": spec["description"],
                "paths": _suite_paths(name),
                "focused": name == focus,
                "pytest_command": _suite_command_text(name),
            }
        )
    return {"current_focus": focus, "suites": suites}


def _queue_data(queue_file: str) -> tuple[dict[str, Any], Path]:
    queue_path = Path(queue_file)
    try:
        payload = json.loads(queue_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"queue file not found: {queue_path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"queue file is not valid JSON: {queue_path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"queue file must contain a JSON object: {queue_path}")
    return payload, queue_path


def _collect_repo_paths(node: Any, collected: set[str]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            if key in PATH_KEYS and isinstance(value, list):
                for item in value:
                    if isinstance(item, str):
                        collected.add(item)
                continue
            _collect_repo_paths(value, collected)
    elif isinstance(node, list):
        for item in node:
            _collect_repo_paths(item, collected)


def _queue_paths(payload: dict[str, Any]) -> list[str]:
    collected: set[str] = set()
    _collect_repo_paths(payload, collected)
    return sorted(collected)


def _suite_names_for_path(path: str) -> set[str]:
    suites: set[str] = set()
    if path == "Code&DBs/Workflow/runtime/debate_workflow.py":
        suites.add("integration")
    if path == "Code&DBs/Workflow/runtime/triggers.py":
        suites.add("integration")
    if path == "Code&DBs/Workflow/surfaces/mcp/tools/workflow.py":
        suites.add("integration")
    if "Code&DBs/Workflow/tests/unit/" in path:
        suites.add("unit")
    if "Code&DBs/Workflow/tests/integration/" in path:
        suites.add("integration")
    if "Code&DBs/Workflow/tests/contracts/" in path:
        suites.add("contracts")
    if "Code&DBs/Workflow/memory/" in path:
        suites.add("unit")
    if "Code&DBs/Workflow/registry/agent_config.py" == path:
        suites.add("unit")
    if path.startswith("Code&DBs/Workflow/runtime/") and "/workflow/" not in path:
        suites.add("unit")
    if any(
        token in path
        for token in (
            "Code&DBs/Workflow/surfaces/cli/workflow_cli.py",
            "Code&DBs/Workflow/surfaces/cli/workflow_runner.py",
            "Code&DBs/Workflow/runtime/workflow/",
            "Code&DBs/Workflow/runtime/compiler.py",
            "Code&DBs/Workflow/runtime/verification.py",
            "Code&DBs/Workflow/runtime/integrations/",
            "Code&DBs/Workflow/runtime/task_assembler.py",
            "scripts/workflow.sh",
            "scripts/test.sh",
            "artifacts/workflow/authority_cleanup/",
            "artifacts/workflow/db_only_phase/",
        )
    ):
        suites.add("workflow_first_slice")
    return suites


def _path_covers(parent: str, child: str) -> bool:
    parent = parent.rstrip("/")
    child = child.rstrip("/")
    return child == parent or child.startswith(parent + "/")


def _append_suite_path(selected_paths: list[str], candidate: str) -> None:
    if any(_path_covers(existing, candidate) for existing in selected_paths):
        return
    selected_paths[:] = [
        existing for existing in selected_paths if not _path_covers(candidate, existing)
    ]
    selected_paths.append(candidate)


def _queue_analysis(queue_file: str) -> dict[str, Any]:
    payload, queue_path = _queue_data(queue_file)
    queue_paths = _queue_paths(payload)
    focus = _load_focus() or DEFAULT_SUITE
    affected_suites: set[str] = set()
    for path in queue_paths:
        affected_suites.update(_suite_names_for_path(path))
    selected_suites = sorted({focus, *affected_suites})
    selected_paths: list[str] = []
    for suite_name in selected_suites:
        for test_path in _suite_paths(suite_name):
            if test_path not in selected_paths:
                _append_suite_path(selected_paths, test_path)
    unclassified_paths = [
        path for path in queue_paths if not _suite_names_for_path(path)
    ]
    recommended_command = _pytest_command(selected_paths)
    return {
        "queue_file": str(queue_path),
        "queue_name": payload.get("name"),
        "workflow_id": payload.get("workflow_id"),
        "current_focus": focus,
        "queue_paths": queue_paths,
        "affected_suites": sorted(affected_suites),
        "selected_suites": selected_suites,
        "selected_test_paths": selected_paths,
        "unclassified_paths": unclassified_paths,
        "recommended_command": _pytest_command_text(recommended_command),
    }


def _run_command(command: list[str]) -> dict[str, Any]:
    started = time.perf_counter()
    proc = subprocess.run(
        command,
        cwd=str(REPO_ROOT),
        env=_subprocess_env(),
        capture_output=True,
        text=True,
    )
    return {
        "duration_s": round(time.perf_counter() - started, 3),
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def _suite_list_payload() -> dict[str, Any]:
    return {"ok": True, "results": _suite_list_result(), "errors": [], "warnings": []}


def _help_payload() -> dict[str, Any]:
    return {
        "ok": True,
        "results": {
            "usage": USAGE,
            "commands": [
                "suite list",
                "suite focus [name]",
                "plan <queue-file>",
                "check-affected <queue-file>",
                "validate <queue-file>",
                "selftest",
                "moon-style-lint",
            ],
        },
        "errors": [],
        "warnings": [],
    }


def _suite_focus_payload(args: list[str]) -> dict[str, Any]:
    if not args:
        current = _load_focus() or DEFAULT_SUITE
        return {
            "ok": True,
            "results": {"current_focus": current, "available_suites": sorted(SUITE_DEFINITIONS)},
            "errors": [],
            "warnings": [],
        }
    suite_name = args[0]
    if suite_name not in SUITE_DEFINITIONS:
        return {
            "ok": False,
            "results": {"requested_suite": suite_name},
            "errors": [f"unknown suite: {suite_name}"],
            "warnings": [f"known suites: {', '.join(sorted(SUITE_DEFINITIONS))}"],
        }
    focus_result = _store_focus(suite_name)
    return {
        "ok": True,
        "results": {
            **focus_result,
            "suite": {
                "name": suite_name,
                "description": SUITE_DEFINITIONS[suite_name]["description"],
                "pytest_command": _suite_command_text(suite_name),
            },
        },
        "errors": [],
        "warnings": [],
    }


def _plan_payload(args: list[str]) -> dict[str, Any]:
    if len(args) != 1:
        return {
            "ok": False,
            "results": {},
            "errors": ["plan requires exactly one queue file"],
            "warnings": [],
        }
    analysis = _queue_analysis(args[0])
    return {"ok": True, "results": analysis, "errors": [], "warnings": []}


def _check_affected_payload(args: list[str]) -> dict[str, Any]:
    if len(args) != 1:
        return {
            "ok": False,
            "results": {},
            "errors": ["check-affected requires exactly one queue file"],
            "warnings": [],
        }
    analysis = _queue_analysis(args[0])
    if not analysis["affected_suites"]:
        return {
            "ok": False,
            "results": analysis,
            "errors": ["no known test suites matched the queue file paths"],
            "warnings": [],
        }
    return {
        "ok": True,
        "results": analysis,
        "errors": [],
        "warnings": [],
    }


def _validate_payload(args: list[str]) -> dict[str, Any]:
    if len(args) != 1:
        return {
            "ok": False,
            "results": {},
            "errors": ["validate requires exactly one queue file"],
            "warnings": [],
        }
    queue_file = args[0]
    command = [str(REPO_ROOT / "scripts" / "workflow.sh"), "validate", queue_file]
    run = _run_command(command)
    ok = run["returncode"] == 0
    errors: list[str] = []
    warnings: list[str] = []
    if ok:
        pass
    elif "Spec Validation: PASSED" in run["stdout"] and "Agent resolution check failed" in run["stdout"]:
        ok = False
        errors.append(
            "workflow validation failed: agent resolution was blocked by the sandbox permission surface"
        )
        warnings.append(
            "workflow spec validated, but agent resolution was blocked by the sandbox permission surface"
        )
    elif "agent authority unavailable:" in run["stdout"] and "AUTHORITY ERROR" in run["stdout"]:
        ok = False
        errors.append(
            "workflow validation failed: agent resolution was blocked by the sandbox permission surface"
        )
        warnings.append(
            "workflow spec parsed, but agent resolution was blocked by the sandbox permission surface"
        )
    else:
        errors.append(f"workflow validation failed with exit code {run['returncode']}")
        if run["stderr"]:
            errors.append(run["stderr"].strip())
        elif run["stdout"]:
            errors.append(run["stdout"].strip())
    return {
        "ok": ok,
        "results": {
            "queue_file": str(Path(queue_file)),
            "workflow_command": _workflow_command_text(["validate", queue_file]),
            "workflow_run": run,
        },
        "errors": errors,
        "warnings": warnings,
    }


def _selftest_payload() -> dict[str, Any]:
    suite_result = _suite_list_result()
    pytest_targets = [
        "Code&DBs/Workflow/tests/unit/test_workflow_cli_validate.py",
        "Code&DBs/Workflow/tests/unit/test_workflow_cli_run.py",
    ]
    command = _pytest_command(pytest_targets, collect_only=True)
    run = _run_command(command)
    warnings: list[str] = []
    errors: list[str] = []
    ok = True
    if run["returncode"] == 4:
        warnings.append("pytest collection exited with code 4; surfaced as a non-fatal warning")
    elif run["returncode"] != 0:
        ok = False
        errors.append(f"pytest collection failed with exit code {run['returncode']}")
        if run["stderr"]:
            errors.append(run["stderr"].strip())
        elif run["stdout"]:
            errors.append(run["stdout"].strip())
    return {
        "ok": ok,
        "results": {
            "suite_registry": suite_result,
            "pytest_collection": {
                "command": "PYTHONPATH="
                + shlex.quote(str(WORKFLOW_ROOT))
                + " "
                + " ".join(shlex.quote(part) for part in command),
                "run": run,
            },
        },
        "errors": errors,
        "warnings": warnings,
    }


def _moon_style_lint_payload(args: list[str]) -> dict[str, Any]:
    if args:
        return {
            "ok": False,
            "results": {},
            "errors": ["moon-style-lint does not accept positional arguments"],
            "warnings": [],
        }
    command = [sys.executable, str(REPO_ROOT / "scripts" / "moon_style_lint.py")]
    run = _run_command(command)
    try:
        details = json.loads(run["stdout"] or "{}")
    except json.JSONDecodeError:
        return {
            "ok": False,
            "results": {"raw": run["stdout"]},
            "errors": ["moon-style-lint output was not valid JSON"],
            "warnings": [run["stderr"].strip()] if run["stderr"] else [],
        }

    ok = run["returncode"] == 0 and bool(details.get("ok", False))
    return {
        "ok": ok,
        "results": details.get("results", {}),
        "errors": list(details.get("errors", [])),
        "warnings": list(details.get("warnings", [])),
    }


def _dispatch(argv: list[str]) -> dict[str, Any]:
    if not argv:
        return {
            "ok": False,
            "results": {},
            "errors": [USAGE],
            "warnings": [],
        }

    command = argv[0]
    tail = argv[1:]

    if command in {"help", "-h", "--help"}:
        return _help_payload()

    if command == "suite":
        if not tail:
            return {
                "ok": False,
                "results": {},
                "errors": ["suite requires list or focus"],
                "warnings": [],
            }
        suite_command = tail[0]
        suite_args = tail[1:]
        if suite_command == "list":
            return _suite_list_payload()
        if suite_command == "focus":
            return _suite_focus_payload(suite_args)
        return {
            "ok": False,
            "results": {},
            "errors": [f"unknown suite subcommand: {suite_command}"],
            "warnings": [],
        }

    if command == "plan":
        return _plan_payload(tail)
    if command == "check-affected":
        return _check_affected_payload(tail)
    if command == "validate":
        return _validate_payload(tail)
    if command == "selftest":
        if tail:
            return {
                "ok": False,
                "results": {},
                "errors": ["selftest does not accept positional arguments"],
                "warnings": [],
            }
        return _selftest_payload()
    if command == "moon-style-lint":
        return _moon_style_lint_payload(tail)

    return {
        "ok": False,
        "results": {},
        "errors": [f"unknown test front door command: {command}"],
        "warnings": [],
    }


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    started = time.perf_counter()
    envelope: dict[str, Any]
    try:
        result = _dispatch(args)
        envelope = {
            "ok": bool(result.get("ok")),
            "command": _command_text(args),
            "duration_s": round(time.perf_counter() - started, 3),
            "results": result.get("results", {}),
            "errors": list(result.get("errors", [])),
            "warnings": list(result.get("warnings", [])),
        }
    except Exception as exc:  # pragma: no cover - defensive shell-front-door guard
        envelope = {
            "ok": False,
            "command": _command_text(args),
            "duration_s": round(time.perf_counter() - started, 3),
            "results": {},
            "errors": [f"{exc.__class__.__name__}: {exc}"],
            "warnings": [],
        }
    print(json.dumps(envelope, indent=2))
    return 0 if envelope["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
