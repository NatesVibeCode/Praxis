"""Explicit boot sequence for surface subsystems.

Replaces the implicit side-effects that _BaseSubsystems.__init__ used to
trigger (sys.path mutation, DB pool creation, schema bootstrap, registry
sync).  Call ``boot()`` once at surface startup.
"""
from __future__ import annotations

import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ._workflow_database import workflow_database_env_for_repo


@dataclass(frozen=True, slots=True)
class _ExplicitWorkflowDatabaseStatus:
    database_url: str
    database_reachable: bool
    schema_bootstrapped: bool
    missing_schema_objects: tuple[str, ...]
    compile_artifact_authority_ready: bool
    compile_index_authority_ready: bool
    execution_packet_authority_ready: bool
    repo_snapshot_authority_ready: bool
    verification_registry_ready: bool
    verifier_authority_ready: bool
    healer_authority_ready: bool
    data_dir: str = ""
    log_file: str = ""
    pid: int | None = None
    port: int | None = None
    process_running: bool = True

    def to_json(self) -> dict[str, Any]:
        return {
            "data_dir": self.data_dir,
            "log_file": self.log_file,
            "database_url": self.database_url,
            "pid": self.pid,
            "port": self.port,
            "process_running": self.process_running,
            "database_reachable": self.database_reachable,
            "schema_bootstrapped": self.schema_bootstrapped,
            "missing_schema_objects": list(self.missing_schema_objects),
            "compile_artifact_authority_ready": self.compile_artifact_authority_ready,
            "compile_index_authority_ready": self.compile_index_authority_ready,
            "execution_packet_authority_ready": self.execution_packet_authority_ready,
            "repo_snapshot_authority_ready": self.repo_snapshot_authority_ready,
            "verification_registry_ready": self.verification_registry_ready,
            "verifier_authority_ready": self.verifier_authority_ready,
            "healer_authority_ready": self.healer_authority_ready,
        }


def ensure_workflow_on_path(workflow_root: Path) -> None:
    """Add the workflow root to sys.path if not already present."""
    root_str = str(workflow_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)


def resolve_surface_env(
    *,
    repo_root: Path | None = None,
    workflow_root: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    if workflow_root is not None:
        ensure_workflow_on_path(workflow_root)

    if env is not None:
        return dict(env)

    resolved_repo_root = repo_root
    if resolved_repo_root is None:
        if workflow_root is None:
            raise RuntimeError("create_pg_conn requires repo_root or workflow_root when env is omitted")
        resolved_repo_root = workflow_root.parents[2]
    return workflow_database_env_for_repo(resolved_repo_root)


def create_pg_conn(
    *,
    repo_root: Path | None = None,
    workflow_root: Path | None = None,
    env: dict[str, str] | None = None,
) -> Any:
    """Create a shared Postgres connection without hidden startup side effects."""
    resolved_env = resolve_surface_env(
        repo_root=repo_root,
        workflow_root=workflow_root,
        env=env,
    )

    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

    return SyncPostgresConnection(get_workflow_pool(env=resolved_env))


def bootstrap_pg_conn(
    *,
    repo_root: Path | None = None,
    workflow_root: Path | None = None,
    env: dict[str, str] | None = None,
) -> Any:
    """Create a shared Postgres connection and explicitly bootstrap schema."""
    resolved_env = resolve_surface_env(
        repo_root=repo_root,
        workflow_root=workflow_root,
        env=env,
    )

    from storage.postgres import ensure_postgres_available

    return ensure_postgres_available(env=resolved_env)


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
        from runtime.integrations.connector_registrar import sync_built_connectors

        sync_built_connectors(conn)
        succeeded.append("connector_registry")
    except Exception:
        skipped.append("connector_registry")

    try:
        from runtime.reference_catalog_seeder import seed_reference_catalog

        seed_reference_catalog(conn)
        succeeded.append("reference_catalog")
    except Exception:
        skipped.append("reference_catalog")

    return succeeded, skipped


def workflow_database_status(
    *,
    repo_root: Path | None = None,
    workflow_root: Path | None = None,
    env: Mapping[str, str] | None = None,
    bootstrap: bool = False,
) -> Any:
    """Resolve env once and delegate to the canonical local Postgres helper."""

    resolved_env = resolve_surface_env(
        repo_root=repo_root,
        workflow_root=workflow_root,
        env=env,
    )

    from storage.dev_postgres import DevPostgresError, local_postgres_bootstrap, local_postgres_health

    def _explicit_authority_status(*, bootstrap_requested: bool) -> _ExplicitWorkflowDatabaseStatus:
        from storage.migrations import workflow_compile_authority_readiness_requirements
        from storage.postgres import inspect_workflow_schema
        from storage.postgres.connection import (
            _run_sync,
            get_workflow_pool,
            resolve_workflow_database_url,
        )
        from storage.postgres.fresh_install_seed import seed_fresh_install_authority_async
        from storage.postgres.schema import bootstrap_workflow_schema

        pool = get_workflow_pool(env=resolved_env)

        async def _inspect():
            async with pool.acquire() as conn:
                if bootstrap_requested:
                    await bootstrap_workflow_schema(conn)
                    await seed_fresh_install_authority_async(conn)
                return await inspect_workflow_schema(conn)

        readiness = _run_sync(_inspect())
        missing_set = set(readiness.missing_relations)
        readiness_requirements = dict(workflow_compile_authority_readiness_requirements())
        return _ExplicitWorkflowDatabaseStatus(
            database_url=resolve_workflow_database_url(env=resolved_env),
            database_reachable=True,
            schema_bootstrapped=readiness.is_bootstrapped,
            missing_schema_objects=tuple(readiness.missing_relations),
            compile_artifact_authority_ready=all(
                table not in missing_set
                for table in readiness_requirements["compile_artifact_authority_ready"]
            ),
            compile_index_authority_ready=all(
                table not in missing_set
                for table in readiness_requirements["compile_index_authority_ready"]
            ),
            execution_packet_authority_ready=all(
                table not in missing_set
                for table in readiness_requirements["execution_packet_authority_ready"]
            ),
            repo_snapshot_authority_ready=all(
                table not in missing_set
                for table in readiness_requirements["repo_snapshot_authority_ready"]
            ),
            verification_registry_ready=all(
                table not in missing_set
                for table in readiness_requirements["verification_registry_ready"]
            ),
            verifier_authority_ready=all(
                table not in missing_set
                for table in readiness_requirements["verifier_authority_ready"]
            ),
            healer_authority_ready=all(
                table not in missing_set
                for table in readiness_requirements["healer_authority_ready"]
            ),
        )

    try:
        if bootstrap:
            return local_postgres_bootstrap(env=resolved_env)
        return local_postgres_health(env=resolved_env)
    except DevPostgresError as exc:
        if exc.reason_code != "dev_postgres.disabled":
            raise
        return _explicit_authority_status(bootstrap_requested=bootstrap)


def workflow_database_status_payload(
    *,
    repo_root: Path | None = None,
    workflow_root: Path | None = None,
    env: Mapping[str, str] | None = None,
    bootstrap: bool = False,
) -> dict[str, Any]:
    """Return the JSON-safe status payload for the canonical database authority."""

    return workflow_database_status(
        repo_root=repo_root,
        workflow_root=workflow_root,
        env=env,
        bootstrap=bootstrap,
    ).to_json()


__all__ = [
    "bootstrap_pg_conn",
    "create_pg_conn",
    "ensure_workflow_on_path",
    "resolve_surface_env",
    "sync_registries",
    "workflow_database_status",
    "workflow_database_status_payload",
]
