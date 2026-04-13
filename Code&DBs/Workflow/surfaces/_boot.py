"""Explicit boot sequence for surface subsystems.

Replaces the implicit side-effects that _BaseSubsystems.__init__ used to
trigger (sys.path mutation, DB pool creation, schema bootstrap, registry
sync).  Call ``boot()`` once at surface startup.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any


def ensure_workflow_on_path(workflow_root: Path) -> None:
    """Add the workflow root to sys.path if not already present."""
    root_str = str(workflow_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)


def create_pg_conn(
    *,
    workflow_root: Path | None = None,
    env: dict[str, str] | None = None,
) -> Any:
    """Create a shared Postgres connection, bootstrap schema, sync registries."""
    if workflow_root is not None:
        ensure_workflow_on_path(workflow_root)

    if env is None:
        database_url = os.environ.get("WORKFLOW_DATABASE_URL", "").strip()
        if not database_url:
            raise RuntimeError("WORKFLOW_DATABASE_URL must be set")
        env = {
            "WORKFLOW_DATABASE_URL": database_url,
            "PATH": os.environ.get("PATH", ""),
        }

    from storage.postgres import ensure_postgres_available

    conn = ensure_postgres_available(env=env)
    return conn


def sync_registries(conn: Any) -> tuple[list[str], list[str]]:
    """Run registry syncs that used to happen as side effects in get_pg_conn.

    Returns (succeeded, skipped) lists for observability.
    """
    succeeded: list[str] = []
    skipped: list[str] = []

    try:
        from registry.integration_registry_sync import sync_integration_registry
        sync_integration_registry(conn)
        succeeded.append("integration_registry")
    except Exception:
        skipped.append("integration_registry")

    try:
        from registry.reference_catalog_sync import sync_reference_catalog
        sync_reference_catalog(conn)
        succeeded.append("reference_catalog")
    except Exception:
        skipped.append("reference_catalog")

    return succeeded, skipped


__all__ = ["create_pg_conn", "ensure_workflow_on_path", "sync_registries"]
