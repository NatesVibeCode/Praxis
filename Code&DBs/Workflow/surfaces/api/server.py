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
from surfaces.api.handlers._subsystems import workflow_database_env


def _env_flag(name: str, default: bool = False) -> bool:
    """Parse a boolean environment flag."""

    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    normalized = raw_value.strip().lower()
    return normalized not in {"", "0", "false", "no", "off"}


def _prime_workflow_database_env() -> None:
    """Publish the normalized workflow DB authority for process-wide consumers."""

    try:
        env = workflow_database_env()
    except Exception:
        return
    database_url = str(env.get("WORKFLOW_DATABASE_URL") or "").strip()
    if database_url:
        os.environ["WORKFLOW_DATABASE_URL"] = database_url


def start_server(
    host: str = "0.0.0.0",
    port: int = 8420,
    *,
    reload: bool = False,
    reload_dirs: tuple[str, ...] | None = None,
    ) -> None:
    """Start the Praxis Engine REST API server.

    Args:
        host: Bind address (default ``"0.0.0.0"`` — all interfaces).
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

    print(f"Starting Praxis Engine API on http://{host}:{port}")
    print(f"Dependency contract: {report['manifest_path']}")
    print(f"  Reload: {'enabled' if reload else 'disabled'}")
    print(f"  Docs:    http://localhost:{port}/docs")
    print(f"  Health:  http://localhost:{port}/api/health")
    print(f"  Dashboard: http://localhost:{port}/api/dashboard")
    print("Press Ctrl+C to stop.\n")

    uvicorn.run(app_target, **uvicorn_kwargs)


def main(argv: list[str] | None = None) -> int:
    """Parse CLI args and start the API server."""

    parser = argparse.ArgumentParser(description="Praxis Engine API server")
    parser.add_argument(
        "--host",
        default=os.environ.get("PRAXIS_API_HOST", "0.0.0.0"),
        help="Bind address (default: 0.0.0.0)",
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
