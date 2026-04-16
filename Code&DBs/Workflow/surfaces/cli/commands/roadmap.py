"""CQRS-native roadmap CLI frontdoor commands."""

from __future__ import annotations

import json
from typing import Any, TextIO

from surfaces.cli.mcp_tools import print_json, run_cli_tool


def _usage() -> str:
    return (
        "usage: workflow roadmap "
        "<view|status|scoreboard|graph|write|closeout> [args]"
    )


def _help_text() -> str:
    return "\n".join(
        [
            "usage: workflow roadmap <command> [args]",
            "",
            "CQRS roadmap surface:",
            "  Query:",
            "    workflow roadmap view [--root <roadmap_item_id>] [--semantic-neighbor-limit <n>]",
            "      Read roadmap items from the CQRS read model (no run id required).",
            "    workflow roadmap status --run-id <run_id>",
            "    workflow roadmap scoreboard --run-id <run_id>",
            "    workflow roadmap graph --run-id <run_id>",
            "      Run-scoped operator views (require a workflow run id).",
            "  Command:",
            "    workflow roadmap write <preview|validate|commit> --title <title> --intent-brief <brief> [options]",
            "    workflow roadmap closeout <preview|commit> [--bug-id <id>]... [--roadmap-item-id <id>]...",
            "",
            "Tip: `view` is read-only; write and closeout mutate state only when action is commit.",
        ]
    )


def _parse_json_flag(flag: str, value: str) -> dict[str, object]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{flag} must be valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{flag} must decode to a JSON object")
    return parsed


def _roadmap_view_command(args: list[str], *, stdout: TextIO) -> int:
    root_roadmap_item_id: str | None = None
    semantic_neighbor_limit: int | None = None
    index = 0
    while index < len(args):
        flag = args[index]
        if flag == "--root":
            if index + 1 >= len(args):
                stdout.write("missing value for --root\n")
                return 2
            root_roadmap_item_id = args[index + 1]
            index += 2
            continue
        if flag == "--semantic-neighbor-limit":
            if index + 1 >= len(args):
                stdout.write("missing value for --semantic-neighbor-limit\n")
                return 2
            semantic_neighbor_limit = int(args[index + 1])
            index += 2
            continue
        stdout.write(f"unknown argument: {flag}\n")
        return 2

    params: dict[str, object] = {}
    if root_roadmap_item_id:
        params["root_roadmap_item_id"] = root_roadmap_item_id
    if semantic_neighbor_limit is not None:
        params["semantic_neighbor_limit"] = semantic_neighbor_limit

    exit_code, payload = run_cli_tool("praxis_operator_roadmap_view", params)
    print_json(stdout, payload)
    return exit_code


def _roadmap_operator_view_command(
    args: list[str],
    *,
    view: str,
    stdout: TextIO,
) -> int:
    run_id: str | None = None
    index = 0
    while index < len(args):
        flag = args[index]
        if flag == "--run-id":
            if index + 1 >= len(args):
                stdout.write("missing value for --run-id\n")
                return 2
            run_id = args[index + 1]
            index += 2
            continue
        stdout.write(f"unknown argument: {flag}\n")
        return 2

    if not run_id:
        stdout.write(
            f"usage: workflow roadmap {view} --run-id <run_id>\n"
            "note: this command is run-scoped.\n"
            "for roadmap table/read-model visibility, use:\n"
            "  workflow roadmap view\n"
        )
        return 2

    exit_code, payload = run_cli_tool(
        "praxis_operator_view",
        {"view": view, "run_id": run_id},
    )
    print_json(stdout, payload)
    return exit_code


def _roadmap_write_command(args: list[str], *, stdout: TextIO) -> int:
    if not args:
        stdout.write(
            "usage: workflow roadmap write <preview|validate|commit> "
            "--title <title> --intent-brief <brief> [options]\n"
        )
        return 2
    action = args[0].strip().lower()
    if action not in {"preview", "validate", "commit"}:
        stdout.write("usage: workflow roadmap write <preview|validate|commit> --title <title> --intent-brief <brief>\n")
        return 2

    params: dict[str, Any] = {"action": action}
    depends_on: list[str] = []
    registry_paths: list[str] = []
    index = 1
    while index < len(args):
        flag = args[index]
        if flag == "--phase-ready":
            params["phase_ready"] = True
            index += 1
            continue
        if flag == "--not-phase-ready":
            params["phase_ready"] = False
            index += 1
            continue
        if index + 1 >= len(args):
            stdout.write(f"missing value for {flag}\n")
            return 2
        value = args[index + 1]
        if flag == "--title":
            params["title"] = value
        elif flag == "--intent-brief":
            params["intent_brief"] = value
        elif flag == "--template":
            params["template"] = value
        elif flag == "--priority":
            params["priority"] = value
        elif flag == "--parent":
            params["parent_roadmap_item_id"] = value
        elif flag == "--slug":
            params["slug"] = value
        elif flag == "--depends-on":
            depends_on.append(value)
        elif flag == "--source-bug":
            params["source_bug_id"] = value
        elif flag == "--registry-path":
            registry_paths.append(value)
        elif flag == "--decision-ref":
            params["decision_ref"] = value
        elif flag == "--item-kind":
            params["item_kind"] = value
        elif flag == "--tier":
            params["tier"] = value
        elif flag == "--approval-tag":
            params["approval_tag"] = value
        elif flag == "--reference-doc":
            params["reference_doc"] = value
        elif flag == "--outcome-gate":
            params["outcome_gate"] = value
        elif flag == "--acceptance-criteria-json":
            params["acceptance_criteria"] = _parse_json_flag(flag, value)
        else:
            stdout.write(f"unknown argument: {flag}\n")
            return 2
        index += 2

    if depends_on:
        params["depends_on"] = depends_on
    if registry_paths:
        params["registry_paths"] = registry_paths

    if not str(params.get("title") or "").strip() or not str(params.get("intent_brief") or "").strip():
        stdout.write(
            "usage: workflow roadmap write <preview|validate|commit> "
            "--title <title> --intent-brief <brief> [options]\n"
        )
        return 2

    exit_code, payload = run_cli_tool("praxis_operator_write", params)
    print_json(stdout, payload)
    return exit_code


def _roadmap_closeout_command(args: list[str], *, stdout: TextIO) -> int:
    if not args:
        stdout.write(
            "usage: workflow roadmap closeout <preview|commit> "
            "[--bug-id <id>]... [--roadmap-item-id <id>]...\n"
        )
        return 2
    action = args[0].strip().lower()
    if action not in {"preview", "commit"}:
        stdout.write(
            "usage: workflow roadmap closeout <preview|commit> "
            "[--bug-id <id>]... [--roadmap-item-id <id>]...\n"
        )
        return 2

    bug_ids: list[str] = []
    roadmap_item_ids: list[str] = []
    index = 1
    while index < len(args):
        flag = args[index]
        if index + 1 >= len(args):
            stdout.write(f"missing value for {flag}\n")
            return 2
        value = args[index + 1]
        if flag == "--bug-id":
            bug_ids.append(value)
        elif flag == "--roadmap-item-id":
            roadmap_item_ids.append(value)
        else:
            stdout.write(f"unknown argument: {flag}\n")
            return 2
        index += 2

    params: dict[str, object] = {"action": action}
    if bug_ids:
        params["bug_ids"] = bug_ids
    if roadmap_item_ids:
        params["roadmap_item_ids"] = roadmap_item_ids

    exit_code, payload = run_cli_tool("praxis_operator_closeout", params)
    print_json(stdout, payload)
    return exit_code


def _roadmap_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help", "help"}:
        stdout.write(_help_text() + "\n")
        return 0

    subcommand = args[0]
    tail = args[1:]
    if subcommand == "view":
        return _roadmap_view_command(tail, stdout=stdout)
    if subcommand in {"status", "scoreboard", "graph"}:
        return _roadmap_operator_view_command(tail, view=subcommand, stdout=stdout)
    if subcommand == "write":
        return _roadmap_write_command(tail, stdout=stdout)
    if subcommand == "closeout":
        return _roadmap_closeout_command(tail, stdout=stdout)

    stdout.write(f"unknown roadmap subcommand: {subcommand}\n")
    stdout.write(f"{_usage()}\n")
    return 2
