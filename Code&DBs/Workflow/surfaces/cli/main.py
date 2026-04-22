"""Command-line entrypoint stubs for the workflow frontdoor.

The CLI is a parser and renderer. It does not own runtime truth.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib
import json
import os
from difflib import SequenceMatcher
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, TextIO

if TYPE_CHECKING:
    from observability.read_models import (
        GraphLineageReadModel,
        GraphTopologyReadModel,
        InspectionReadModel,
        ReplayReadModel,
    )

__all__ = [
    "GraphLineageCommand",
    "GraphTopologyCommand",
    "InspectCommand",
    "ReplayCommand",
    "_github_command",
    "_parse_pr_spec",
    "main",
]


@dataclass(frozen=True, slots=True)
class InspectCommand:
    """CLI intent for an inspect run request."""

    run_id: str


@dataclass(frozen=True, slots=True)
class ReplayCommand:
    """CLI intent for a replay run request."""

    run_id: str


@dataclass(frozen=True, slots=True)
class GraphTopologyCommand:
    """CLI intent for a graph topology read request."""

    run_id: str


@dataclass(frozen=True, slots=True)
class GraphLineageCommand:
    """CLI intent for a graph lineage read request."""

    run_id: str


_COMMAND_INDEX_ENTRIES: list[dict[str, str]] = [
    {"command": "workflow commands", "description": "Show this command index"},
    {"command": "workflow help commands", "description": "Show this command index via root help"},
    {"command": "workflow routes", "description": "Alias for workflow API route discovery"},
    {"command": "workflow help routes", "description": "Same route discovery help from the root help system"},
    {"command": "workflow mcp [list|search|describe|call|help]", "description": "Alias for workflow tools discovery"},
    {"command": "workflow help mcp", "description": "Same tool discovery help from the root help system"},
    {"command": "workflow run <spec.json>", "description": "Submit a workflow spec"},
    {
        "command": "workflow preview <spec.json>",
        "description": "Render the exact execution payload without submitting",
    },
    {
        "command": "workflow spawn <parent_run_id> <spec.json>",
        "description": "Spawn a child workflow with explicit parent lineage",
    },
    {"command": "workflow validate <spec.json>", "description": "Validate a spec without running"},
    {"command": "workflow records <create|update|rename>", "description": "Persist canonical workflow records"},
    {"command": "workflow status [--since-hours N]", "description": "Show recent workflow status"},
    {"command": "workflow run-status <run_id>", "description": "Inspect one run and explain idle recovery options"},
    {"command": "workflow active", "description": "Show active workflow runs"},
    {"command": "workflow stream <run_id>", "description": "Stream one workflow run"},
    {"command": "workflow retry <run_id> <label>", "description": "Retry one failed job"},
    {"command": "workflow cancel <run_id>", "description": "Cancel a workflow run"},
    {"command": "workflow repair <run_id>", "description": "Repair post-run sync state"},
    {"command": "workflow work <claim|acknowledge>", "description": "Claim or acknowledge worker work"},
    {"command": "workflow tools [list|search|describe|call|help]", "description": "Discover and call catalog-backed MCP tools"},
    {"command": "workflow integrations", "description": "Scoped route discovery for /api/integrations"},
    {"command": "workflow api integrations", "description": "Scoped route discovery for /api/integrations"},
    {"command": "workflow api data-dictionary", "description": "Scoped route discovery for /api/data-dictionary"},
    {"command": "workflow api help [routes|integrations|data-dictionary]", "description": "Show API route discovery help"},
    {"command": "workflow integration [list|describe|health|test|call|create|secret|reload|help]", "description": "Integration management via the catalog-backed MCP tool"},
    {"command": "workflow dictionary <list|describe|set-override|clear-override|reproject>", "description": "Unified data dictionary authority"},
    {"command": "workflow authority-memory refresh", "description": "Refresh authority FK projection into memory_edges"},
    {"command": "workflow maintenance <backfill-failure-categories|help>", "description": "Backfill failure classification fields from canonical receipts"},
    {"command": "workflow data <action>", "description": "Deterministic data cleanup, validation, and workflow launch"},
    {"command": "workflow authority-index", "description": "Print the concept→code→DB→surface index"},
    {
        "command": "workflow research [list|<topic>] [--workers N] [--agent SLUG] [--threshold N] [--json]",
        "description": "Launch or inspect the parallel research workflow frontdoor",
    },
    {"command": "workflow decompose <objective...>", "description": "Plan one objective into sprint-sized work"},
    {
        "command": "workflow schema|registry|object-type|object-field|object|catalog|files|reload|reconcile",
        "description": "Direct database, file, and registry authority frontdoors",
    },
    {"command": "workflow handoff <latest|lineage|status|history>", "description": "CQRS handoff inspection surface"},
    {
        "command": "workflow query|recall|discover|research|architecture|artifacts|bugs|costs|leaderboard|trust|fitness|trends|scope|risk|reviews|receipts",
        "description": "Derived search, analysis, and bug-tracker surfaces",
    },
    {
        "command": "workflow inspect|replay|graph-topology|graph-lineage|topology|lineage",
        "description": "Derived observability views",
    },
    {
        "command": "workflow health|health-map|metrics|events|cache|circuits|slots|params|config|notifications|dashboard|api [routes|--host|--port]|routes|supervisor|capabilities|work",
        "description": "Operator and platform surfaces",
    },
    {
        "command": "workflow heartbeat [--scope <scope>] [--pretty]",
        "description": "Run the daily external-health heartbeat",
    },
    {"command": "workflow native-operator instance|health|db-health|bootstrap|db-bootstrap|smoke|inspect|status|graph-topology|graph-lineage|cockpit|route-disable|roadmap-write|work-item-closeout|roadmap-tree|provider-onboard|native-primary-cutover-gate", "description": "Repo-local operator surface"},
    {"command": "workflow roadmap view|status|scoreboard|graph|write|closeout", "description": "CQRS-native roadmap query/command frontdoor"},
    {"command": "workflow compile|github", "description": "Build and repository automation"},
]

_COMMAND_INDEX_TIPS: list[str] = [
    "run `workflow help <command>` or `workflow <command> --help` for command-specific usage",
    "run `workflow commands --json` for machine-readable discovery",
    "run `workflow api help` or `workflow help api` for HTTP route discovery",
    "run `workflow integrations` for the integration API route scope",
    "run `workflow api integrations` or `workflow api data-dictionary` for scoped route discovery",
    "run `workflow integration` for integration management",
    "run `workflow help tools` or `workflow mcp` for catalog-backed tool discovery",
]


class InspectReplayService(Protocol):
    """Thin runtime-facing contract for inspect and replay surfaces."""

    def inspect_run(self, *, run_id: str) -> InspectionReadModel:
        """Return the derived inspection view for one run."""

    def replay_run(self, *, run_id: str) -> ReplayReadModel:
        """Return the derived replay view for one run."""


class GraphSurfaceService(Protocol):
    """Thin runtime-facing contract for graph-derived read surfaces."""

    def graph_topology_run(self, *, run_id: str) -> GraphTopologyReadModel:
        """Return the derived graph topology view for one run."""

    def graph_lineage_run(self, *, run_id: str) -> GraphLineageReadModel:
        """Return the derived graph lineage view for one run."""


class ArgsCommandHandler(Protocol):
    """CLI handler protocol for commands that accept trailing args."""

    def __call__(self, args: list[str], *, stdout: TextIO) -> int:
        """Execute the command with argv tail and stdout sink."""


class StdoutCommandHandler(Protocol):
    """CLI handler protocol for commands with no positional args."""

    def __call__(self, *, stdout: TextIO) -> int:
        """Execute the command with stdout sink."""


class _PostgresObservabilityService:
    """Default repo-local observability composition for CLI frontdoor commands."""

    def __init__(self, *, env: Mapping[str, str] | None) -> None:
        self._env = env

    def _evidence_reader(self):
        from storage.postgres import PostgresEvidenceReader

        return PostgresEvidenceReader(env=self._env)

    def inspect_run(self, *, run_id: str) -> InspectionReadModel:
        from runtime.execution.orchestrator import RuntimeOrchestrator

        return RuntimeOrchestrator(
            evidence_reader=self._evidence_reader(),
        ).inspect_run(run_id=run_id)

    def replay_run(self, *, run_id: str) -> ReplayReadModel:
        from runtime.execution.orchestrator import RuntimeOrchestrator

        return RuntimeOrchestrator(
            evidence_reader=self._evidence_reader(),
        ).replay_run(run_id=run_id)

    def graph_topology_run(self, *, run_id: str) -> GraphTopologyReadModel:
        from observability import graph_topology_run
        from surfaces.api import frontdoor

        frontdoor.status(run_id=run_id, env=self._env)
        reader = self._evidence_reader()
        return graph_topology_run(
            run_id=run_id,
            canonical_evidence=reader.evidence_timeline(run_id),
        )

    def graph_lineage_run(self, *, run_id: str) -> GraphLineageReadModel:
        from observability import graph_lineage_run
        from surfaces.api import frontdoor

        frontdoor.status(run_id=run_id, env=self._env)
        reader = self._evidence_reader()
        inspection = self.inspect_run(run_id=run_id)
        return graph_lineage_run(
            run_id=run_id,
            canonical_evidence=reader.evidence_timeline(run_id),
            operator_frame_source=inspection.operator_frame_source,
            operator_frames=inspection.operator_frames,
        )


def _build_default_observability_service(
    *,
    env: Mapping[str, str] | None,
) -> _PostgresObservabilityService:
    """Bind repo-local Postgres-backed read models for inspect/replay/graph commands."""

    return _PostgresObservabilityService(env=env)


os.environ.setdefault("PRAXIS_DISABLE_STARTUP_WIRING", "1")


def _command_attr(module_name: str, command_name: str):
    module = importlib.import_module(module_name, __package__)
    return getattr(module, command_name)


def _lazy_args_module_command(module_name: str, command_name: str) -> ArgsCommandHandler:
    def _handler(args: list[str], *, stdout: TextIO) -> int:
        return _command_attr(module_name, command_name)(args, stdout=stdout)

    return _handler


def _lazy_stdout_module_command(module_name: str, command_name: str) -> StdoutCommandHandler:
    def _handler(*, stdout: TextIO) -> int:
        return _command_attr(module_name, command_name)(stdout=stdout)

    return _handler


def _compile_command(args: list[str], *, stdout: TextIO) -> int:
    return _command_attr(".commands.admin", "_compile_command")(args, stdout=stdout)


def _github_command(args: list[str], *, stdout: TextIO) -> int:
    return _command_attr(".commands.admin", "_github_command")(args, stdout=stdout)


def _parse_pr_spec(spec_str: str) -> tuple[str, str, int]:
    return _command_attr(".commands.admin", "_parse_pr_spec")(spec_str)


def _workflow_command_handler(command_name: str):
    workflow_commands = importlib.import_module(".commands.workflow", __package__)
    return getattr(workflow_commands, command_name)


def _lazy_workflow_args_command(command_name: str) -> ArgsCommandHandler:
    def _handler(args: list[str], *, stdout: TextIO) -> int:
        return _workflow_command_handler(command_name)(args, stdout=stdout)

    return _handler


def _lazy_workflow_stdout_command(command_name: str) -> StdoutCommandHandler:
    def _handler(*, stdout: TextIO) -> int:
        return _workflow_command_handler(command_name)(stdout=stdout)

    return _handler


def _run_legacy_compat_command(
    args: list[str],
    *,
    stdout: TextIO,
    prog: str,
    configure_parser: Callable[[argparse.ArgumentParser], None],
    runner: Callable[[argparse.Namespace], int],
) -> int:
    parser = argparse.ArgumentParser(prog=prog)
    configure_parser(parser)
    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stdout):
            parsed = parser.parse_args(args)
            return runner(parsed)
    except SystemExit as exc:
        return int(exc.code)


def _generate_command(args: list[str], *, stdout: TextIO) -> int:
    from . import workflow_cli as legacy_workflow_cli

    def _configure(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("manifest_file", help="Path to the minimal JSON manifest file")
        parser.add_argument("output", help="Path to the output .queue.json spec file")
        mode = parser.add_mutually_exclusive_group()
        mode.add_argument("--strict", action="store_true", help="Fail if the output file already exists")
        mode.add_argument("--merge", action="store_true", help="Merge with existing output file if it exists")

    return _run_legacy_compat_command(
        args,
        stdout=stdout,
        prog="workflow generate",
        configure_parser=_configure,
        runner=legacy_workflow_cli.cmd_generate,
    )


def _validate_command(args: list[str], *, stdout: TextIO) -> int:
    from . import workflow_cli as legacy_workflow_cli

    def _configure(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("spec", help="Path to .queue.json spec file")
    return _run_legacy_compat_command(
        args,
        stdout=stdout,
        prog="workflow validate",
        configure_parser=_configure,
        runner=legacy_workflow_cli.cmd_validate,
    )


def _stream_command(args: list[str], *, stdout: TextIO) -> int:
    from . import workflow_cli as legacy_workflow_cli

    def _configure(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("run_id", help="Workflow run id to stream")
        parser.add_argument("--timeout", type=float, default=None, help="Stop streaming after N seconds")
        parser.add_argument("--poll-interval", type=float, default=2.0, help="Poll interval in seconds")

    return _run_legacy_compat_command(
        args,
        stdout=stdout,
        prog="workflow stream",
        configure_parser=_configure,
        runner=legacy_workflow_cli.cmd_stream,
    )


def _chain_status_command(args: list[str], *, stdout: TextIO) -> int:
    from . import workflow_cli as legacy_workflow_cli

    def _configure(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("chain_id", nargs="?", help="Optional workflow chain id")
        parser.add_argument("--limit", type=int, default=20, help="How many recent chains to list")

    return _run_legacy_compat_command(
        args,
        stdout=stdout,
        prog="workflow chain-status",
        configure_parser=_configure,
        runner=legacy_workflow_cli.cmd_chain_status,
    )


def _preview_workflow_cli_command(args: list[str], *, stdout: TextIO) -> int:
    forwarded_args = list(args)
    if "--preview-execution" not in forwarded_args:
        forwarded_args.append("--preview-execution")
    return _lazy_workflow_args_command("_run_command")(forwarded_args, stdout=stdout)


_ARG_COMMANDS: dict[str, ArgsCommandHandler] | None = None


def _workflow_arg_commands() -> dict[str, ArgsCommandHandler]:
    global _ARG_COMMANDS
    if _ARG_COMMANDS is not None:
        return _ARG_COMMANDS

    _ARG_COMMANDS = {
        "run": _lazy_workflow_args_command("_run_command"),
        "preview": _preview_workflow_cli_command,
        "spawn": _lazy_workflow_args_command("_spawn_command"),
        "dry-run": _lazy_workflow_args_command("_dry_run_command"),
        "chain": _lazy_workflow_args_command("_chain_command"),
        "query": _lazy_args_module_command(".commands.query", "_query_command"),
        "research": _lazy_args_module_command(".commands.query", "_research_command"),
        "decompose": _lazy_args_module_command(".commands.query", "_decompose_command"),
        "data": _lazy_args_module_command(".commands.data", "_data_command"),
        "files": _lazy_args_module_command(".commands.files", "_files_command"),
        "handoff": _lazy_args_module_command(".commands.handoff", "_handoff_command"),
        "schema": _lazy_args_module_command(".commands.authority", "_schema_command"),
        "registry": _lazy_args_module_command(".commands.authority", "_registry_command"),
        "object-type": _lazy_args_module_command(".commands.authority", "_object_type_command"),
        "object-field": _lazy_args_module_command(".commands.authority", "_object_field_command"),
        "object": _lazy_args_module_command(".commands.authority", "_object_command"),
        "catalog": _lazy_args_module_command(".commands.authority", "_catalog_command"),
        "reload": _lazy_args_module_command(".commands.authority", "_reload_command"),
        "reconcile": _lazy_args_module_command(".commands.authority", "_reconcile_command"),
        "architecture": _lazy_args_module_command(".commands.query", "_architecture_command"),
        "bugs": _lazy_args_module_command(".commands.query", "_bugs_command"),
        "recall": _lazy_args_module_command(".commands.query", "_recall_command"),
        "discover": _lazy_args_module_command(".commands.query", "_discover_command"),
        "artifacts": _lazy_args_module_command(".commands.query", "_artifacts_command"),
        "health": _lazy_args_module_command(".commands.operate", "_health_command"),
        "heartbeat": _lazy_args_module_command(".commands.heartbeat", "_heartbeat_command"),
        "receipts": _lazy_args_module_command(".commands.query", "_receipts_command"),
        "diagnose": _lazy_workflow_args_command("_diagnose_command"),
        "inspect-job": _lazy_workflow_args_command("_inspect_job_command"),
        "authority-index": _lazy_workflow_args_command("_authority_index_command"),
        "leaderboard": _lazy_args_module_command(".commands.query", "_leaderboard_command"),
        "manifest": _lazy_workflow_args_command("_manifest_command"),
        "trust": _lazy_args_module_command(".commands.query", "_trust_command"),
        "fitness": _lazy_args_module_command(".commands.query", "_fitness_command"),
        "trends": _lazy_args_module_command(".commands.query", "_trends_command"),
        "verify": _lazy_workflow_args_command("_verify_command"),
        "verify-platform": _lazy_workflow_args_command("_verify_platform_command"),
        "pipeline": _lazy_workflow_args_command("_pipeline_command"),
        "proof": _lazy_workflow_args_command("_proof_command"),
        "heal": _lazy_workflow_args_command("_heal_command"),
        "run-status": _lazy_workflow_args_command("_run_status_command"),
        "scheduler": _lazy_workflow_args_command("_scheduler_command"),
        "loop": _lazy_workflow_args_command("_loop_command"),
        "debate": _lazy_workflow_args_command("_debate_command"),
        "runs": _lazy_workflow_args_command("_runs_command"),
        "retry": _lazy_workflow_args_command("_retry_command"),
        "cancel": _lazy_workflow_args_command("_cancel_command"),
        "circuits": _lazy_args_module_command(".commands.operate", "_circuits_command"),
        "params": _lazy_args_module_command(".commands.operate", "_params_command"),
        "notifications": _lazy_args_module_command(".commands.operate", "_notifications_command"),
        "config": _lazy_args_module_command(".commands.operate", "_config_command"),
        "dashboard": _lazy_args_module_command(".commands.operate", "_dashboard_command"),
        "queue": _lazy_workflow_args_command("_queue_command"),
        "capabilities": _lazy_args_module_command(".commands.operate", "_capabilities_command"),
        "scope": _lazy_args_module_command(".commands.query", "_scope_command"),
        "risk": _lazy_args_module_command(".commands.query", "_risk_command"),
        "events": _lazy_args_module_command(".commands.operate", "_events_command"),
        "cache": _lazy_args_module_command(".commands.operate", "_cache_command"),
        "health-map": _lazy_args_module_command(".commands.operate", "_health_map_command"),
        "reviews": _lazy_args_module_command(".commands.query", "_reviews_command"),
        "compile": _compile_command,
        "metrics": _lazy_args_module_command(".commands.operate", "_metrics_command"),
        "github": _github_command,
        "api": _lazy_args_module_command(".commands.operate", "_api_command"),
        "routes": lambda args, *, stdout: _command_attr(".commands.operate", "_api_command")(
            ["routes", *args],
            stdout=stdout,
        ),
        "integrations": lambda args, *, stdout: _command_attr(".commands.operate", "_api_command")(
            ["integrations", *args],
            stdout=stdout,
        ),
        "integration": _lazy_args_module_command(".commands.operate", "_integrations_command"),
        "maintenance": _lazy_args_module_command(".commands.maintenance", "_maintenance_command"),
        "supervisor": _lazy_args_module_command(".commands.operate", "_supervisor_command"),
        "tools": _lazy_args_module_command(".commands.tools", "_tools_command"),
        "dictionary": _lazy_args_module_command(".commands.authority", "_data_dictionary_command"),
        "authority-memory": _lazy_args_module_command(".commands.authority", "_authority_memory_command"),
        "generate": _generate_command,
        "validate": _validate_command,
        "stream": _stream_command,
        "chain-status": _chain_status_command,
        "triggers": _lazy_workflow_args_command("_triggers_command"),
        "records": _lazy_workflow_args_command("_records_command"),
        "repair": _lazy_workflow_args_command("_repair_command"),
        "work": _lazy_workflow_args_command("_work_command"),
        "roadmap": _lazy_args_module_command(".commands.roadmap", "_roadmap_command"),
        "commands": lambda args, *, stdout: _commands_index_command(args, stdout=stdout),
    }
    return _ARG_COMMANDS

_STDOUT_COMMANDS: dict[str, StdoutCommandHandler] = {
    "status": _lazy_workflow_stdout_command("_status_command"),
    "costs": _lazy_stdout_module_command(".commands.query", "_costs_command"),
    "slots": _lazy_stdout_module_command(".commands.operate", "_slots_command"),
    "active": _lazy_workflow_stdout_command("_active_command"),
}


def _usage() -> str:
    return "usage: workflow <command> [args]"


def _normalize_command_text(value: str) -> str:
    return " ".join(str(value or "").lower().split())


def _command_suggestions(topic: str, candidates: Sequence[str], *, limit: int = 3) -> list[str]:
    normalized_topic = _normalize_command_text(topic)
    if not normalized_topic:
        return []

    ranked: list[tuple[int, int, float, int, str]] = []
    for candidate in sorted({candidate for candidate in candidates if candidate}):
        normalized_candidate = _normalize_command_text(candidate)
        if not normalized_candidate or normalized_candidate == normalized_topic:
            continue
        prefix_score = 0 if normalized_candidate.startswith(normalized_topic) else 1
        contains_score = 0 if normalized_topic in normalized_candidate else 1
        similarity = SequenceMatcher(None, normalized_topic, normalized_candidate).ratio()
        length_penalty = abs(len(normalized_candidate) - len(normalized_topic))
        ranked.append((prefix_score, contains_score, -similarity, length_penalty, candidate))

    ranked.sort()
    return [candidate for *_ignored, candidate in ranked[:limit]]


def _help_topic_candidates() -> list[str]:
    return sorted(
        {
            "commands",
            "help",
            "index",
            "native-operator",
            *(
                command
                for command in _known_root_commands()
                if command not in {"defs", "workflows"}
            ),
        }
    )


def _native_operator_help_text() -> str:
    from . import native_operator

    return native_operator._help_text()


def _api_help_text() -> str:
    from io import StringIO

    from .commands.operate import _api_command

    buffer = StringIO()
    _api_command(["--help"], stdout=buffer)
    return buffer.getvalue().rstrip()


def _mcp_help_text() -> str:
    tools_quickstart_text = _command_attr(".commands.tools", "_tools_quickstart_text")
    return "\n".join(
        [
            "usage: workflow mcp [list|search|describe|call|help]",
            "",
            "Alias for workflow tools discovery.",
            "",
            tools_quickstart_text(),
        ]
    )


def _commands_index_text() -> str:
    lines = [
        "usage: workflow commands",
        "",
        "Command index:",
    ]
    for entry in _COMMAND_INDEX_ENTRIES:
        lines.append(f"  {entry['command']:<45} {entry['description']}")
    lines.extend(["", *[f"Tip: {tip}." for tip in _COMMAND_INDEX_TIPS]])
    return "\n".join(lines)


def _commands_index_payload() -> dict[str, object]:
    return {
        "usage": "workflow commands",
        "entries": list(_COMMAND_INDEX_ENTRIES),
        "tips": list(_COMMAND_INDEX_TIPS),
    }


def _commands_index_command(args: list[str], *, stdout: TextIO) -> int:
    as_json = False
    for arg in args:
        if arg == "--json":
            as_json = True
        elif arg in {"-h", "--help", "help"}:
            stdout.write(_commands_index_text() + "\n")
            return 0
        else:
            stdout.write(f"unknown argument: {arg}\n")
            return 2

    if as_json:
        stdout.write(json.dumps(_commands_index_payload(), indent=2) + "\n")
        return 0

    stdout.write(_commands_index_text() + "\n")
    return 0


def _help_text() -> str:
    return "\n".join(
        [
            "usage: workflow <command> [args]",
            "",
            "Most used:",
            "  workflow run <spec.json>",
            "  workflow preview <spec.json>",
            "  workflow validate <spec.json>",
            "  workflow run-status <run_id>",
            "  workflow mcp [list|search|describe|call|help]",
            "  workflow help mcp",
            "  workflow routes",
            "  workflow api help",
            "  workflow integrations",
            "  workflow integration list",
            "  workflow integration help",
            "  workflow help routes",
            "  workflow tools list",
            "  workflow tools search <topic> [--exact] [--surface <surface>] [--tier <tier>] [--risk <risk>]",
            "  workflow api routes",
            "  workflow api help",
            "  workflow dictionary list",
            "  workflow authority-memory refresh",
            "  workflow help tools",
            "  workflow help api",
            "  workflow commands --json",
            "  workflow query <question>",
            "  workflow research 'API auth drift'",
            "  workflow decompose 'build real-time notifications'",
            "  workflow data profile artifacts/data/users.csv",
            "  workflow authority-index",
            "  workflow maintenance backfill-failure-categories --yes",
            "  workflow files list --scope instance",
            "  workflow handoff latest --artifact-kind packet_lineage --revision-ref <ref>",
            "  workflow schema status",
            "  workflow object list --type-id ticket",
            "  workflow work claim --subscription-id <id> --run-id <run_id>",
            "  workflow inspect <run_id>",
            "  workflow replay <run_id>",
            "  workflow heartbeat [--scope <scope>] [--pretty]",
            "  workflow routes",
            "  workflow native-operator instance",
            "  workflow roadmap view",
            "",
            "Command groups:",
            "  workflow tools [list|search|describe|call|help]",
            "  workflow dictionary <list|describe|set-override|clear-override|reproject>",
            "  workflow authority-memory refresh",
            "  workflow maintenance <backfill-failure-categories|help>",
            "  workflow integration [list|describe|health|test|call|create|secret|reload|help]",
            "  workflow data <action>",
            "  workflow authority-index",
            "  workflow research [list|<topic>] [--workers N] [--agent SLUG] [--threshold N] [--json]",
            "  workflow decompose <objective...>",
            "  workflow files <list|get|content|upload|delete>",
            "  workflow handoff <latest|lineage|status|history>",
            "  workflow schema|registry|object-type|object-field|object|catalog|files|reload|reconcile",
            "  workflow query|recall|discover|research|architecture|artifacts|bugs|costs|leaderboard|trust|fitness|trends|scope|risk|reviews|receipts",
            "  workflow run|preview|run-status|status|active|scheduler|loop|debate|runs|manifest|triggers|retry|cancel|repair|heal|verify|verify-platform|pipeline|proof|queue|diagnose|inspect-job|heartbeat",
            "  workflow inspect|replay|graph-topology|graph-lineage|topology|lineage",
            "  workflow health|health-map|metrics|events|cache|circuits|slots|params|config|notifications|dashboard|api [routes|integrations|data-dictionary|--host|--port]|routes|supervisor|capabilities|work",
            "  workflow integrations",
            "                                                  Scoped route discovery for /api/integrations",
            "  workflow integration",
            "                                                  Integration management via the catalog-backed MCP tool",
            "  workflow maintenance",
            "                                                  Backfill failure classification fields from canonical receipts",
            "  workflow heartbeat [--scope <scope>] [--pretty]",
            "                                                  Run the daily external-health heartbeat",
            "  workflow native-operator instance|health|db-health|bootstrap|db-bootstrap|smoke|inspect|status|graph-topology|graph-lineage|cockpit|route-disable|roadmap-write|work-item-closeout|roadmap-tree|provider-onboard|native-primary-cutover-gate",
            "  workflow roadmap view|status|scoreboard|graph|write|closeout",
            "  workflow compile|github",
            "",
            "Tip: run `workflow commands` or `workflow help commands` for the full command index.",
            "Tip: run `workflow commands --json` when you want machine-readable discovery.",
            "Tip: run `workflow api help` or `workflow help api` for HTTP route discovery.",
            "Tip: run `workflow help run-status` for per-run status and idle recovery.",
            "Tip: run `workflow integrations` or `workflow api integrations` for the integration route scope.",
            "Tip: run `workflow integration` for integration management.",
            "Tip: run `workflow maintenance backfill-failure-categories --yes` to repair failure classification projections.",
            "Tip: run `workflow api data-dictionary` for the data dictionary route scope.",
            "Tip: run `workflow help tools`, `workflow help mcp`, or `workflow mcp` for catalog-backed tool discovery.",
            "Tip: run `workflow help <command>` or `workflow <command> --help` for command-specific usage.",
        ]
    )


def _help_topic_text(topic: str, *, stdout: TextIO) -> int:
    topic = topic.strip()
    if not topic:
        stdout.write(_help_text() + "\n")
        return 0
    if topic == "help":
        stdout.write(_help_text() + "\n")
        return 0
    if topic == "mcp":
        stdout.write(_mcp_help_text() + "\n")
        return 0
    if topic in {"commands", "index"}:
        stdout.write(_commands_index_text() + "\n")
        return 0
    if topic == "native-operator":
        stdout.write(_native_operator_help_text() + "\n")
        return 0
    if topic == "api":
        stdout.write(_api_help_text() + "\n")
        return 0
    if topic == "routes":
        stdout.write(_api_help_text() + "\n")
        return 0
    if topic == "tools":
        _workflow_arg_commands()["tools"](["--help"], stdout=stdout)
        return 0

    if topic in {"inspect", "replay", "graph-topology", "topology", "graph-lineage", "lineage"}:
        usage = {
            "inspect": "usage: workflow inspect <run_id>",
            "replay": "usage: workflow replay <run_id>",
            "graph-topology": "usage: workflow graph-topology <run_id>",
            "topology": "usage: workflow topology <run_id>",
            "graph-lineage": "usage: workflow graph-lineage <run_id>",
            "lineage": "usage: workflow lineage <run_id>",
        }[topic]
        stdout.write(usage + "\n")
        return 0

    if topic in _STDOUT_COMMANDS:
        usage = {
            "status": "usage: workflow status [--since-hours N]",
            "active": "usage: workflow active",
            "costs": "usage: workflow costs",
            "slots": "usage: workflow slots",
        }.get(topic)
        if usage is not None:
            stdout.write(usage + "\n")
            return 0

    if topic in _workflow_arg_commands():
        _workflow_arg_commands()[topic](["--help"], stdout=stdout)
        return 0

    stdout.write(f"unknown help topic: {topic}\n")
    suggestions = _command_suggestions(topic, _help_topic_candidates())
    if suggestions:
        stdout.write("did you mean:\n")
        for suggestion in suggestions:
            stdout.write(f"  workflow help {suggestion}\n")
    stdout.write(
        "try `workflow help commands`, `workflow help native-operator`, "
        "`workflow help api`, `workflow help query`, or `workflow help run`.\n"
    )
    return 2


def _known_root_commands() -> set[str]:
    return {
        "mcp",
        "native-operator",
        "inspect",
        "replay",
        "graph-topology",
        "topology",
        "graph-lineage",
        "lineage",
        *(_workflow_arg_commands().keys()),
        *(_STDOUT_COMMANDS.keys()),
    }


def _normalize_namespace_tokens(argv: Sequence[str]) -> list[str]:
    args = list(argv)
    if args and args[0] == "workflow":
        return args[1:]
    return args


def _parse(
    argv: Sequence[str],
) -> InspectCommand | ReplayCommand | GraphTopologyCommand | GraphLineageCommand:
    args = list(argv)
    if len(args) != 2:
        raise ValueError(_usage())
    command_name, run_id = args
    if command_name == "inspect":
        return InspectCommand(run_id=run_id)
    if command_name == "replay":
        return ReplayCommand(run_id=run_id)
    if command_name in {"graph-topology", "topology"}:
        return GraphTopologyCommand(run_id=run_id)
    if command_name in {"graph-lineage", "lineage"}:
        return GraphLineageCommand(run_id=run_id)
    raise ValueError(_usage())


def _has_callable(service: object, name: str) -> bool:
    return callable(getattr(service, name, None))


def _resolve_graph_service(
    service: object | None,
    graph_service: GraphSurfaceService | None,
    observability_service: GraphSurfaceService | None,
) -> object | None:
    return graph_service or observability_service or service


def _dispatch(
    command: InspectCommand | ReplayCommand | GraphTopologyCommand | GraphLineageCommand,
    *,
    service: object,
) -> str:
    if isinstance(command, InspectCommand):
        if not _has_callable(service, "inspect_run"):
            raise RuntimeError("cli frontdoor requires an inspect service")
        from .render import render_inspection

        return render_inspection(service.inspect_run(run_id=command.run_id))
    if isinstance(command, ReplayCommand):
        if not _has_callable(service, "replay_run"):
            raise RuntimeError("cli frontdoor requires a replay service")
        from .render import render_replay

        return render_replay(service.replay_run(run_id=command.run_id))
    if isinstance(command, GraphTopologyCommand):
        if not _has_callable(service, "graph_topology_run"):
            raise RuntimeError("cli frontdoor requires a graph topology service")
        from .render import render_graph_topology

        return render_graph_topology(service.graph_topology_run(run_id=command.run_id))
    if not _has_callable(service, "graph_lineage_run"):
        raise RuntimeError("cli frontdoor requires a graph lineage service")
    from .render import render_graph_lineage

    return render_graph_lineage(service.graph_lineage_run(run_id=command.run_id))


def main(
    argv: Sequence[str] | None = None,
    *,
    inspect_replay_service: InspectReplayService | None = None,
    runtime_orchestrator: InspectReplayService | None = None,
    graph_service: GraphSurfaceService | None = None,
    observability_service: GraphSurfaceService | None = None,
    env: Mapping[str, str] | None = None,
    stdout: TextIO | None = None,
) -> int:
    """Parse argv and route into application services.

    The skeleton intentionally stops short of truth ownership.
    """

    stdout = sys.stdout if stdout is None else stdout
    args = _normalize_namespace_tokens(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in {"-h", "--help", "help"}:
        if len(args) >= 2 and args[0] == "help":
            return _help_topic_text(args[1], stdout=stdout)
        stdout.write(_help_text() + "\n")
        return 0
    if args and args[0] == "native-operator":
        from . import native_operator

        return native_operator.main(args[1:], env=env, stdout=stdout)
    if args and args[0] == "mcp":
        return _workflow_arg_commands()["tools"](args[1:], stdout=stdout)

    if args[0] not in _known_root_commands():
        if args[0] == "defs":
            stdout.write(
                "workflow defs has been removed; use workflow records create|update|rename instead\n"
            )
            stdout.write(f"{_usage()}\n")
            return 2
        stdout.write(f"unknown command: {args[0]}\n")
        suggestions = _command_suggestions(args[0], _help_topic_candidates())
        if suggestions:
            stdout.write("did you mean:\n")
            for suggestion in suggestions:
                stdout.write(f"  workflow {suggestion}\n")
        stdout.write(
            "run `workflow commands` or `workflow help <command>` to see command-specific usage; "
            "try `workflow help api` for HTTP route discovery.\n"
        )
        stdout.write(f"{_usage()}\n")
        return 2

    if args:
        command_name = args[0]
        arg_commands = _workflow_arg_commands()
        if command_name in arg_commands:
            return arg_commands[command_name](args[1:], stdout=stdout)
        if command_name in _STDOUT_COMMANDS:
            return _STDOUT_COMMANDS[command_name](stdout=stdout)

    try:
        command = _parse(args)
    except ValueError as exc:
        stdout.write(f"{exc}\n")
        return 2

    service = inspect_replay_service or runtime_orchestrator
    if isinstance(command, (GraphTopologyCommand, GraphLineageCommand)):
        service = _resolve_graph_service(service, graph_service, observability_service)
    if service is None:
        service = _build_default_observability_service(env=env)
    if service is None:
        if isinstance(command, (GraphTopologyCommand, GraphLineageCommand)):
            raise RuntimeError("cli frontdoor requires a graph service")
        raise RuntimeError("cli frontdoor requires an inspect/replay service")

    rendered = _dispatch(command, service=service)
    stdout.write(f"{rendered}\n")
    return 0


# Keep `from surfaces.cli import main` bound to the callable entrypoint even
# after Python attaches the `surfaces.cli.main` submodule onto the package.
if __package__:
    package_module = sys.modules.get(__package__)
    if package_module is not None:
        setattr(package_module, "main", main)


if __name__ == "__main__":
    raise SystemExit(main())
