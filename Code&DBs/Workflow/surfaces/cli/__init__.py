"""CLI frontdoor for workflow operations.

The CLI parses operator intent and renders derived views.
It does not create lifecycle truth or evidence truth.
"""

from importlib import import_module
import sys
from types import ModuleType

__all__ = [
    "GraphLineageCommand",
    "GraphTopologyCommand",
    "InspectCommand",
    "ReplayCommand",
    "main",
    "render_graph_lineage",
    "render_graph_topology",
    "render_inspection",
    "render_replay",
]


def main(*args, **kwargs):
    """Return the callable CLI entrypoint instead of the sibling module object."""

    from .main import main as _main

    return _main(*args, **kwargs)


class _CliPackageModule(ModuleType):
    """Keep `surfaces.cli.main` resolving to the callable entrypoint."""

    def __getattribute__(self, name: str):
        if name == "main":
            value = ModuleType.__getattribute__(self, "__dict__").get(name)
            if isinstance(value, ModuleType):
                bound_main = getattr(value, "main", None)
                if callable(bound_main):
                    return bound_main
        return ModuleType.__getattribute__(self, name)


def __getattr__(name: str):
    if name in {
        "GraphLineageCommand",
        "GraphTopologyCommand",
        "InspectCommand",
        "ReplayCommand",
    }:
        module = import_module(".main", __name__)
        return getattr(module, name)
    if name in {
        "render_graph_lineage",
        "render_graph_topology",
        "render_inspection",
        "render_replay",
    }:
        module = import_module(".render", __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


sys.modules[__name__].__class__ = _CliPackageModule
