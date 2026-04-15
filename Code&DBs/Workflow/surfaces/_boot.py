"""Explicit boot sequence for surface subsystems.

Replaces the implicit side-effects that _BaseSubsystems.__init__ used to
trigger (sys.path mutation, DB pool creation, schema bootstrap, registry
sync).  Call ``boot()`` once at surface startup.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from ._workflow_database import workflow_database_env_for_repo


def ensure_workflow_on_path(workflow_root: Path) -> None:
    """Add the workflow root to sys.path if not already present."""
    root_str = str(workflow_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)


def create_pg_conn(
    *,
    repo_root: Path | None = None,
    workflow_root: Path | None = None,
    env: dict[str, str] | None = None,
) -> Any:
    """Create a shared Postgres connection, bootstrap schema, sync registries."""
    if workflow_root is not None:
        ensure_workflow_on_path(workflow_root)

    if env is None:
        resolved_repo_root = repo_root
        if resolved_repo_root is None:
            if workflow_root is None:
                raise RuntimeError("create_pg_conn requires repo_root or workflow_root when env is omitted")
            resolved_repo_root = workflow_root.parents[2]
        env = workflow_database_env_for_repo(resolved_repo_root)

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
        from runtime.capability_catalog import sync_capability_catalog

        sync_capability_catalog(conn)
        succeeded.append("capability_catalog")
    except Exception:
        skipped.append("capability_catalog")

    try:
        from registry.native_runtime_profile_sync import sync_native_runtime_profile_authority

        sync_native_runtime_profile_authority(conn)
        succeeded.append("native_runtime_profile_authority")
    except Exception:
        skipped.append("native_runtime_profile_authority")

    try:
        from runtime.reference_catalog_seeder import seed_reference_catalog

        seed_reference_catalog(conn)
        succeeded.append("reference_catalog")
    except Exception:
        skipped.append("reference_catalog")

    return succeeded, skipped


__all__ = ["create_pg_conn", "ensure_workflow_on_path", "sync_registries"]
