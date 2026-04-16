from __future__ import annotations

from .registry import registry, CapabilityRoute
from .bus import CommandBus

_BOOTSTRAPPED = False


def bootstrap_registry() -> None:
    """Explicitly load CQRS command/query modules into the live registry."""

    global _BOOTSTRAPPED
    if _BOOTSTRAPPED:
        return

    from .commands import suggest_next
    from .commands import workflow_build
    from .queries import roadmap_tree
    from .queries import data_dictionary

    # Keep imports live for module-level registration side effects.
    del suggest_next, workflow_build, roadmap_tree, data_dictionary
    _BOOTSTRAPPED = True

__all__ = ["registry", "CommandBus", "CapabilityRoute", "bootstrap_registry"]
