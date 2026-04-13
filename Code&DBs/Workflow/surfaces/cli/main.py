"""Command-line entrypoint stubs for the workflow frontdoor.

The CLI is a parser and renderer. It does not own runtime truth.
"""

from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol, TextIO

from observability.read_models import (
    GraphLineageReadModel,
    GraphTopologyReadModel,
    InspectionReadModel,
    ReplayReadModel,
)

from . import native_operator
from .commands.admin import _compile_command, _github_command, _parse_pr_spec
from .commands.workflow import (
    _active_command,
    _cancel_command,
    _chain_command,
    _debate_command,
    _diagnose_command,
    _fan_out_command,
    _heal_command,
    _pipeline_command,
    _proof_command,
    _queue_command,
    _run_command,
    _runs_command,
    _scheduler_command,
    _status_command,
    _verify_command,
    _verify_platform_command,
)
from .commands.operate import (
    _api_command,
    _cache_command,
    _capabilities_command,
    _circuits_command,
    _config_command,
    _dashboard_command,
    _events_command,
    _health_map_command,
    _metrics_command,
    _notifications_command,
    _params_command,
    _slots_command,
    _supervisor_command,
)
from .commands.query import (
    _costs_command,
    _fitness_command,
    _leaderboard_command,
    _receipts_command,
    _reviews_command,
    _risk_command,
    _scope_command,
    _trends_command,
    _trust_command,
)
from .render import (
    render_graph_lineage,
    render_graph_topology,
    render_inspection,
    render_replay,
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


_ARG_COMMANDS: dict[str, ArgsCommandHandler] = {
    "run": _run_command,
    "chain": _chain_command,
    "receipts": _receipts_command,
    "diagnose": _diagnose_command,
    "leaderboard": _leaderboard_command,
    "trust": _trust_command,
    "fitness": _fitness_command,
    "trends": _trends_command,
    "verify": _verify_command,
    "verify-platform": _verify_platform_command,
    "pipeline": _pipeline_command,
    "proof": _proof_command,
    "heal": _heal_command,
    "scheduler": _scheduler_command,
    "fan-out": _fan_out_command,
    "debate": _debate_command,
    "runs": _runs_command,
    "cancel": _cancel_command,
    "params": _params_command,
    "notifications": _notifications_command,
    "config": _config_command,
    "dashboard": _dashboard_command,
    "queue": _queue_command,
    "capabilities": _capabilities_command,
    "scope": _scope_command,
    "risk": _risk_command,
    "events": _events_command,
    "cache": _cache_command,
    "health-map": _health_map_command,
    "reviews": _reviews_command,
    "compile": _compile_command,
    "metrics": _metrics_command,
    "github": _github_command,
    "api": _api_command,
    "supervisor": _supervisor_command,
}

_STDOUT_COMMANDS: dict[str, StdoutCommandHandler] = {
    "status": _status_command,
    "costs": _costs_command,
    "circuits": _circuits_command,
    "slots": _slots_command,
    "active": _active_command,
}


def _usage() -> str:
    return "usage: workflow <capabilities|config|dashboard|run|pipeline|fan-out|proof|heal|queue|runs|scheduler|circuits|slots|params|cancel|active|notifications|status|costs|receipts|diagnose|verify|verify-platform|leaderboard|trust|fitness|scope|inspect|replay|graph-topology|graph-lineage> <run_id|spec.json|-p prompt>"


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
        return render_inspection(service.inspect_run(run_id=command.run_id))
    if isinstance(command, ReplayCommand):
        if not _has_callable(service, "replay_run"):
            raise RuntimeError("cli frontdoor requires a replay service")
        return render_replay(service.replay_run(run_id=command.run_id))
    if isinstance(command, GraphTopologyCommand):
        if not _has_callable(service, "graph_topology_run"):
            raise RuntimeError("cli frontdoor requires a graph topology service")
        return render_graph_topology(service.graph_topology_run(run_id=command.run_id))
    if not _has_callable(service, "graph_lineage_run"):
        raise RuntimeError("cli frontdoor requires a graph lineage service")
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
    args = list(sys.argv[1:] if argv is None else argv)
    if args and args[0] == "native-operator":
        return native_operator.main(args[1:], env=env, stdout=stdout)

    if args:
        command_name = args[0]
        if command_name in _ARG_COMMANDS:
            return _ARG_COMMANDS[command_name](args[1:], stdout=stdout)
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
