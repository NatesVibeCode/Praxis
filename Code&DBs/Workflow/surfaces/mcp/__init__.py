"""MCP workflow server package."""


def start_server(*args, **kwargs):
    from .server import main

    return main(*args, **kwargs)

__all__ = ["start_server"]
