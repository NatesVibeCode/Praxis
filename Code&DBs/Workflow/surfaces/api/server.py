"""API server entry point for the Praxis Engine REST surface.

The server reads the declared runtime dependency contract before it launches
the REST app. Packaging reads the same contract from
``requirements.runtime.txt``.

Usage:
    python -m surfaces.api.server --host 127.0.0.1 --port 8420
    python -m surfaces.api.server --host 127.0.0.1 --port 8420 --reload
    # or via the CLI:
    workflow api

For the full product front door, use ``./scripts/praxis launch``.
"""

from __future__ import annotations

import argparse
import os
import sys

from runtime.dependency_contract import (
    format_dependency_truth_report,
    require_runtime_dependencies,
)
from runtime.primitive_contracts import resolve_runtime_http_endpoints
from surfaces.api.handlers._subsystems import workflow_database_env


def _env_flag(name: str, default: bool = False) -> bool:
    """Parse a boolean environment flag."""

    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    normalized = raw_value.strip().lower()
    return normalized not in {"", "0", "false", "no", "off"}


def _prime_workflow_database_env() -> None:
    """Fail fast if the API surface cannot resolve an explicit DB authority."""

    workflow_database_env()


def _runtime_http_endpoints(*, host: str, port: int) -> dict[str, str]:
    """Project the client-facing HTTP authority for this API process."""

    client_host = host.strip()
    if client_host in {"0.0.0.0", "::", "[::]"}:
        client_host = "localhost"
    return resolve_runtime_http_endpoints(
        workflow_env={"PRAXIS_API_BASE_URL": f"http://{client_host}:{port}"},
        native_instance={},
    )


def start_server(
    host: str = "127.0.0.1",
    port: int = 8420,
    *,
    reload: bool = False,
    reload_dirs: tuple[str, ...] | None = None,
) -> None:
    """Start the Praxis Engine REST API server.

    Args:
        host: Bind address (default ``"127.0.0.1"`` — loopback only).
            Container / prod deployments must pass ``--host 0.0.0.0`` or set
            ``PRAXIS_API_HOST=0.0.0.0`` so traffic from outside the container
            reaches the server.
        port: TCP port to listen on (default ``8420``).
        reload: Enable uvicorn auto-reload for local development.
        reload_dirs: Explicit directories to watch when reload is enabled.
    """
    _prime_workflow_database_env()
    try:
        report = require_runtime_dependencies(scope="api_server")
    except RuntimeError as exc:
        import logging
        logging.getLogger(__name__).warning("Dependency check failed (non-fatal): %s", exc)
        report = {"manifest_path": "unavailable"}
    try:
        import uvicorn
    except ImportError:
        raise RuntimeError("uvicorn is required: pip install uvicorn")
    app_target: str | object
    uvicorn_kwargs: dict[str, object] = {
        "host": host,
        "port": port,
        "log_level": "info",
    }
    if reload:
        watch_dirs = tuple(dict.fromkeys(reload_dirs or (os.getcwd(),)))
        app_target = "surfaces.api.rest:app"
        uvicorn_kwargs["reload"] = True
        uvicorn_kwargs["reload_dirs"] = list(watch_dirs)
    else:
        from surfaces.api.rest import app

        app_target = app

    endpoints = _runtime_http_endpoints(host=host, port=port)
    api_base_url = str(endpoints["api_base_url"]).rstrip("/")
    print(f"Starting Praxis Engine API on {api_base_url}")
    print(f"  Bind:    http://{host}:{port}")
    print(f"Dependency contract: {report['manifest_path']}")
    print(f"  Reload: {'enabled' if reload else 'disabled'}")
    print(f"  Docs:    {endpoints['api_docs_url']}")
    print(f"  Health:  {api_base_url}/api/health")
    print(f"  Dashboard: {api_base_url}/api/dashboard")
    print("Press Ctrl+C to stop.\n")

    uvicorn.run(app_target, **uvicorn_kwargs)


def main(argv: list[str] | None = None) -> int:
    """Parse CLI args and start the API server."""

    parser = argparse.ArgumentParser(description="Praxis Engine API server")
    parser.add_argument(
        "--host",
        default=os.environ.get("PRAXIS_API_HOST", "127.0.0.1"),
        help=(
            "Bind address (default: 127.0.0.1). "
            "Use 0.0.0.0 or set PRAXIS_API_HOST=0.0.0.0 for container/LAN access."
        ),
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("PRAXIS_API_PORT", "8420")),
        help="TCP port (default: 8420)",
    )
    parser.set_defaults(reload=_env_flag("PRAXIS_API_RELOAD", False))
    parser.add_argument(
        "--reload",
        dest="reload",
        action="store_true",
        help="Enable auto-reload for local development",
    )
    parser.add_argument(
        "--no-reload",
        dest="reload",
        action="store_false",
        help="Disable auto-reload",
    )
    parser.add_argument(
        "--reload-dir",
        dest="reload_dirs",
        action="append",
        default=None,
        help="Directory to watch for changes when reload is enabled; may be passed multiple times",
    )
    args = parser.parse_args(argv)
    start_server(
        host=args.host,
        port=args.port,
        reload=args.reload,
        reload_dirs=tuple(args.reload_dirs or ()),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
