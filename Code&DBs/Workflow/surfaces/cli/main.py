"""Command-line entrypoint stubs for the workflow frontdoor.

The CLI is a parser and renderer. It does not own runtime truth.
"""

from __future__ import annotations

import contextlib
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

from .commands.admin import _compile_command, _github_command, _parse_pr_spec
from .commands.workflow import (
    _active_command,
    _cancel_command,
    _chain_command,
    _debate_command,
    _diagnose_command,
    _fan_out_command,
    _heal_command,
    _inspect_job_command,
    _manifest_command,
    _pipeline_command,
    _proof_command,
    _queue_command,
    _repair_command,
    _retry_command,
    _run_status_command,
    _run_command,
    _runs_command,
    _scheduler_command,
    _status_command,
    _triggers_command,
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
    _health_command,
    _health_map_command,
    _metrics_command,
    _notifications_command,
    _params_command,
    _slots_command,
    _supervisor_command,
)
from .commands.query import (
    _architecture_command,
    _artifacts_command,
    _bugs_command,
    _costs_command,
    _discover_command,
    _fitness_command,
    _leaderboard_command,
    _query_command,
    _recall_command,
    _receipts_command,
    _reviews_command,
    _risk_command,
    _scope_command,
    _trends_command,
    _trust_command,
)
from .commands.tools import _tools_command
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


def _delegate_legacy_workflow_cli(
    command_name: str,
    args: list[str],
    *,
    stdout: TextIO,
) -> int:
    from . import workflow_cli as legacy_workflow_cli

    original_argv = sys.argv
    try:
        sys.argv = ["workflow_cli", command_name, *args]
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stdout):
            return legacy_workflow_cli.main()
    finally:
        sys.argv = original_argv


_ARG_COMMANDS: dict[str, ArgsCommandHandler] = {
    "run": _run_command,
    "chain": _chain_command,
    "query": _query_command,
    "architecture": _architecture_command,
    "bugs": _bugs_command,
    "recall": _recall_command,
    "discover": _discover_command,
    "artifacts": _artifacts_command,
    "health": _health_command,
    "receipts": _receipts_command,
    "diagnose": _diagnose_command,
    "inspect-job": _inspect_job_command,
    "leaderboard": _leaderboard_command,
    "manifest": _manifest_command,
    "trust": _trust_command,
    "fitness": _fitness_command,
    "trends": _trends_command,
    "verify": _verify_command,
    "verify-platform": _verify_platform_command,
    "pipeline": _pipeline_command,
    "proof": _proof_command,
    "heal": _heal_command,
    "run-status": _run_status_command,
    "scheduler": _scheduler_command,
    "fan-out": _fan_out_command,
    "debate": _debate_command,
    "runs": _runs_command,
    "retry": _retry_command,
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
    "tools": _tools_command,
    "generate": lambda args, *, stdout: _delegate_legacy_workflow_cli("generate", args, stdout=stdout),
    "validate": lambda args, *, stdout: _delegate_legacy_workflow_cli("validate", args, stdout=stdout),
    "stream": lambda args, *, stdout: _delegate_legacy_workflow_cli("stream", args, stdout=stdout),
    "chain-status": lambda args, *, stdout: _delegate_legacy_workflow_cli("chain-status", args, stdout=stdout),
    "triggers": _triggers_command,
    "repair": _repair_command,
}

_STDOUT_COMMANDS: dict[str, StdoutCommandHandler] = {
    "status": _status_command,
    "costs": _costs_command,
    "circuits": _circuits_command,
    "slots": _slots_command,
    "active": _active_command,
}


def _usage() -> str:
    return "usage: workflow <tools|query|architecture|bugs|recall|discover|artifacts|health|run|run-status|status|costs|receipts|leaderboard|trust|fitness|scope|risk|reviews|diagnose|inspect-job|heal|verify|debate|runs|manifest|triggers|retry|repair|cancel|active|circuits|slots|inspect|replay|graph-topology|graph-lineage|...> <args>"


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
        from . import native_operator

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
