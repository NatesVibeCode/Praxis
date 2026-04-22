"""Praxis root CLI with explicit namespaces."""

from __future__ import annotations
import sys
from difflib import SequenceMatcher
from typing import TextIO


def _workflow_main() -> callable:
    from surfaces.cli.main import main as workflow_main

    return workflow_main


def _db_command(argv: list[str], *, stdout: TextIO) -> int:
    from surfaces.cli.commands.praxis_authoring import _db_command as command

    return command(argv, stdout=stdout)


def _registry_command_passthrough(argv: list[str], *, stdout: TextIO) -> int:
    from surfaces.cli.commands.praxis_authoring import _registry_command_passthrough as command

    return command(argv, stdout=stdout)


def _object_type_command_passthrough(argv: list[str], *, stdout: TextIO) -> int:
    from surfaces.cli.commands.praxis_authoring import _object_type_command_passthrough as command

    return command(argv, stdout=stdout)


def _object_command_passthrough(argv: list[str], *, stdout: TextIO) -> int:
    from surfaces.cli.commands.praxis_authoring import _object_command_passthrough as command

    return command(argv, stdout=stdout)


def _catalog_command_passthrough(argv: list[str], *, stdout: TextIO) -> int:
    from surfaces.cli.commands.praxis_authoring import _catalog_command_passthrough as command

    return command(argv, stdout=stdout)


def _data_command(argv: list[str], *, stdout: TextIO) -> int:
    from surfaces.cli.commands.praxis_authoring import _data_command as command

    return command(argv, stdout=stdout)


def _dataset_command(argv: list[str], *, stdout: TextIO) -> int:
    from surfaces.cli.commands.dataset import _dataset_command as command

    return command(argv, stdout=stdout)


def _page_command(argv: list[str], *, stdout: TextIO) -> int:
    from surfaces.cli.commands.praxis_authoring import _page_command as command

    return command(argv, stdout=stdout)


def _hierarchy_command(argv: list[str], *, stdout: TextIO) -> int:
    from surfaces.cli.commands.praxis_authoring import _hierarchy_command as command

    return command(argv, stdout=stdout)


def _reload_command(argv: list[str], *, stdout: TextIO) -> int:
    from surfaces.cli.commands.authority import _reload_command as command

    return command(argv, stdout=stdout)


def _reconcile_command(argv: list[str], *, stdout: TextIO) -> int:
    from surfaces.cli.commands.authority import _reconcile_command as command

    return command(argv, stdout=stdout)


def _launcher_command(argv: list[str], *, stdout: TextIO) -> int:
    from runtime.launcher_authority import launcher_cli

    return launcher_cli(argv, stdout=stdout)


def _usage() -> str:
    return "usage: praxis <namespace> [args]"


def _help_text() -> str:
    return "\n".join(
        [
            "usage: praxis <namespace> [args]",
            "",
            "Canonical operator surface:",
            "  praxis workflow <command>                     Execution, query, tool, and operator authority",
            "",
            "Direct authorities:",
            "  praxis launcher <configure|resolve|doctor>    Host-local launcher resolution",
            "  praxis db <status|plan|apply|describe>        Canonical schema authority",
            "  praxis registry <action>                      Manifest registry authority",
            "  praxis object-type <action>                   Object-type authority",
            "  praxis objects <action>                       Object record authority",
            "  praxis catalog <action>                       Surface catalog authority",
            "  praxis dataset <action>                       Dataset refinery (curated training data)",
            "  praxis reload                                 Runtime reload authority",
            "  praxis reconcile                              Data reconcile authority",
            "",
            "Authoring scaffolds:",
            "  praxis db primitive|table|view scaffold ...   Generate SQL scaffolds for runtime storage",
            "  praxis data shape plan ...                    Plan canonical cross-source record shapes",
            "  praxis object-type scaffold ...               Generate or apply object-type specs",
            "  praxis hierarchy scaffold ...                 Generate hierarchy field and view plans",
            "  praxis page scaffold ...                      Generate or apply starter app manifests",
            "",
            "Examples:",
            "  praxis workflow query \"what tools exist for manifests\"",
            "  praxis db status",
            "  praxis db primitive scaffold customer",
            "  praxis data shape plan --spec-file customer_360.json",
            "  praxis page scaffold \"customer health dashboard\" --apply --yes",
        ]
    )


def _normalize(value: str) -> str:
    return " ".join(value.lower().split())


def _command_suggestions(topic: str, candidates: list[str], *, limit: int = 3) -> list[str]:
    normalized_topic = _normalize(topic)
    if not normalized_topic:
        return []
    ranked: list[tuple[int, int, float, str]] = []
    for candidate in candidates:
        normalized_candidate = _normalize(candidate)
        if not normalized_candidate or normalized_candidate == normalized_topic:
            continue
        ranked.append(
            (
                0 if normalized_candidate.startswith(normalized_topic) else 1,
                0 if normalized_topic in normalized_candidate else 1,
                -SequenceMatcher(None, normalized_topic, normalized_candidate).ratio(),
                candidate,
            )
        )
    ranked.sort()
    return [candidate for *_ignored, candidate in ranked[:limit]]


def _normalize_argv(argv: list[str]) -> list[str]:
    if argv and argv[0] == "praxis":
        return argv[1:]
    return argv


def main(argv: list[str] | None = None, *, stdout: TextIO | None = None) -> int:
    stdout = sys.stdout if stdout is None else stdout
    args = _normalize_argv(list(sys.argv[1:] if argv is None else argv))
    if not args or args[0] in {"-h", "--help", "help"}:
        stdout.write(_help_text() + "\n")
        return 0

    namespace = args[0]
    tail = args[1:]

    if namespace == "workflow":
        return _workflow_main()(["workflow", *tail], stdout=stdout)
    if namespace == "launcher":
        return _launcher_command(tail, stdout=stdout)
    if namespace == "db":
        return _db_command(tail, stdout=stdout)
    if namespace == "registry":
        return _registry_command_passthrough(tail, stdout=stdout)
    if namespace == "object-type":
        return _object_type_command_passthrough(tail, stdout=stdout)
    if namespace in {"objects", "object"}:
        return _object_command_passthrough(tail, stdout=stdout)
    if namespace == "catalog":
        return _catalog_command_passthrough(tail, stdout=stdout)
    if namespace == "data":
        return _data_command(tail, stdout=stdout)
    if namespace == "dataset":
        return _dataset_command(tail, stdout=stdout)
    if namespace == "page":
        return _page_command(tail, stdout=stdout)
    if namespace == "hierarchy":
        return _hierarchy_command(tail, stdout=stdout)
    if namespace == "reload":
        return _reload_command([], stdout=stdout) if not tail else _reload_command(tail, stdout=stdout)
    if namespace == "reconcile":
        return _reconcile_command(tail, stdout=stdout)

    stdout.write(f"unknown namespace: {namespace}\n")
    suggestions = _command_suggestions(
        namespace,
        [
            "workflow",
            "launcher",
            "db",
            "registry",
            "object-type",
            "objects",
            "catalog",
            "data",
            "dataset",
            "page",
            "hierarchy",
            "reload",
            "reconcile",
        ],
    )
    if suggestions:
        stdout.write("did you mean:\n")
        for suggestion in suggestions:
            stdout.write(f"  praxis {suggestion}\n")
    stdout.write(_usage() + "\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
