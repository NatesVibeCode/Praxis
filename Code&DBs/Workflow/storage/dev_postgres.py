"""Repo-local developer helpers for the checked-in Postgres cluster.

This module is intentionally narrow:

- it only works with the explicit workflow Postgres URL
- it only targets the repo-local Postgres data directory
- it does not invent a fallback database or a second authority
- it exposes one explicit read-only health path and one explicit schema bootstrap path
- it keeps process control separate from schema bootstrap

The helpers are for local developer ergonomics, not runtime authority.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from shutil import which
from typing import Any, Mapping
from urllib.parse import parse_qs, unquote, urlsplit

import asyncpg

from .postgres import (
    PostgresConfigurationError,
    PostgresStorageError,
    bootstrap_workflow_schema,
    inspect_workflow_schema,
    resolve_workflow_database_url,
)

PRAXIS_LOCAL_POSTGRES_DATA_DIR_ENV = "PRAXIS_LOCAL_POSTGRES_DATA_DIR"
_DEFAULT_DATA_DIR = (
    Path(__file__).resolve().parents[2] / "Databases" / "postgres-dev" / "data"
)
_DEFAULT_LOG_FILE = (
    Path(__file__).resolve().parents[2] / "Databases" / "postgres-dev" / "log" / "postgres.log"
)
_CONNECTION_TIMEOUT_S = 5.0
_DEFAULT_POSTGRES_PORT = 5432
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})


class DevPostgresError(PostgresStorageError):
    """Raised when repo-local developer Postgres helpers fail safely."""


def _native_postgres_disabled() -> DevPostgresError:
    return DevPostgresError(
        "dev_postgres.disabled",
        "Native repo-local Postgres helpers are disabled; use Docker or Cloudflare runtime authority only.",
    )


@dataclass(frozen=True, slots=True)
class DevPostgresConfig:
    """Explicit repo-local settings for the developer Postgres cluster."""

    data_dir: Path
    log_file: Path
    database_url: str
    pg_ctl: str
    cluster_port: int


@dataclass(frozen=True, slots=True)
class DevPostgresStatus:
    """Boring status snapshot for the repo-local developer Postgres cluster."""

    data_dir: str
    log_file: str
    database_url: str
    pid: int | None
    port: int | None
    process_running: bool
    database_reachable: bool
    schema_bootstrapped: bool
    missing_schema_objects: tuple[str, ...] = ()
    compile_artifact_authority_ready: bool = False
    compile_index_authority_ready: bool = False
    execution_packet_authority_ready: bool = False
    repo_snapshot_authority_ready: bool = False
    verification_registry_ready: bool = False
    verifier_authority_ready: bool = False
    healer_authority_ready: bool = False

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


def _fail_config(
    reason_code: str,
    message: str,
    *,
    details: Mapping[str, Any] | None = None,
) -> PostgresConfigurationError:
    return PostgresConfigurationError(reason_code, message, details=details)


def _require_existing_directory(path: Path, *, field_name: str) -> Path:
    if not path.exists():
        raise _fail_config(
            "dev_postgres.config_missing",
            f"{field_name} must point at an existing directory",
            details={"field": field_name, "path": str(path)},
        )
    if not path.is_dir():
        raise _fail_config(
            "dev_postgres.config_invalid",
            f"{field_name} must point at a directory",
            details={"field": field_name, "path": str(path)},
        )
    return path.resolve()


def resolve_local_postgres_data_dir(
    env: Mapping[str, str] | None = None,
) -> Path:
    """Resolve the explicit repo-local PGDATA path."""

    source = env if env is not None else os.environ
    raw_value = source.get(PRAXIS_LOCAL_POSTGRES_DATA_DIR_ENV)
    if raw_value is None:
        return _require_existing_directory(
            _DEFAULT_DATA_DIR,
            field_name="repo-local postgres data directory",
        )
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise _fail_config(
            "dev_postgres.config_invalid",
            f"{PRAXIS_LOCAL_POSTGRES_DATA_DIR_ENV} must be a non-empty directory path",
            details={
                "environment_variable": PRAXIS_LOCAL_POSTGRES_DATA_DIR_ENV,
                "value_type": type(raw_value).__name__,
            },
        )
    return _require_existing_directory(
        Path(raw_value).expanduser(),
        field_name=PRAXIS_LOCAL_POSTGRES_DATA_DIR_ENV,
    )


def _resolve_pg_ctl() -> str:
    pg_ctl = which("pg_ctl")
    if pg_ctl is None:
        raise _fail_config(
            "dev_postgres.binary_missing",
            "pg_ctl must be available on PATH for local Postgres helpers",
        )
    return pg_ctl


def _read_cluster_port_from_postmaster_opts(data_dir: Path) -> int:
    opts_file = data_dir / "postmaster.opts"
    try:
        raw_value = opts_file.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise _fail_config(
            "dev_postgres.identity_missing",
            "repo-local Postgres data directory must expose postmaster.opts for cluster binding",
            details={"data_dir": str(data_dir), "path": str(opts_file)},
        ) from exc
    if not raw_value:
        raise _fail_config(
            "dev_postgres.identity_missing",
            "repo-local Postgres cluster port could not be derived from postmaster.opts",
            details={"data_dir": str(data_dir), "path": str(opts_file)},
        )

    argv = shlex.split(raw_value)
    for index, arg in enumerate(argv):
        if arg == "-p" and index + 1 < len(argv):
            try:
                return int(argv[index + 1])
            except ValueError as exc:
                raise _fail_config(
                    "dev_postgres.identity_invalid",
                    "repo-local Postgres cluster port in postmaster.opts must be an integer",
                    details={
                        "data_dir": str(data_dir),
                        "path": str(opts_file),
                        "value": argv[index + 1],
                    },
                ) from exc
        if arg.startswith("-p") and arg != "-p":
            try:
                return int(arg[2:])
            except ValueError as exc:
                raise _fail_config(
                    "dev_postgres.identity_invalid",
                    "repo-local Postgres cluster port in postmaster.opts must be an integer",
                    details={"data_dir": str(data_dir), "path": str(opts_file), "value": arg},
                ) from exc

    _, pid_port = _read_postmaster_pid(data_dir)
    if pid_port is not None:
        return pid_port

    # PostgreSQL defaults to 5432 when no explicit -p override is present.
    return _DEFAULT_POSTGRES_PORT


def _resolve_cluster_port(data_dir: Path) -> int:
    return _read_cluster_port_from_postmaster_opts(data_dir)


@dataclass(frozen=True, slots=True)
class _DatabaseUrlTarget:
    host: str | None
    port: int
    database_name: str | None
    is_unix_socket: bool


def _parse_database_url_target(database_url: str) -> _DatabaseUrlTarget:
    parsed = urlsplit(database_url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    query_host = query.get("host", [None])[-1]
    query_port = query.get("port", [None])[-1]

    host = query_host if query_host is not None else parsed.hostname
    if isinstance(host, str):
        host = host.strip() or None

    port = parsed.port
    if query_port not in (None, ""):
        try:
            port = int(query_port)
        except ValueError as exc:
            raise _fail_config(
                "dev_postgres.config_invalid",
                "WORKFLOW_DATABASE_URL query-string port must be an integer",
                details={"database_url": database_url, "value": query_port},
            ) from exc
    if port is None:
        port = _DEFAULT_POSTGRES_PORT

    database_name = unquote(parsed.path.lstrip("/")) or None
    return _DatabaseUrlTarget(
        host=host,
        port=port,
        database_name=database_name,
        is_unix_socket=bool(host and host.startswith("/")),
    )


def _assert_database_url_targets_managed_cluster(config: DevPostgresConfig) -> None:
    target = _parse_database_url_target(config.database_url)
    if target.port != config.cluster_port:
        raise _fail_config(
            "dev_postgres.identity_mismatch",
            "WORKFLOW_DATABASE_URL must target the managed repo-local Postgres cluster port",
            details={
                "database_url": config.database_url,
                "data_dir": str(config.data_dir),
                "expected_port": config.cluster_port,
                "actual_port": target.port,
                "actual_host": target.host,
            },
        )

    if target.is_unix_socket:
        return

    if target.host is None or target.host.lower() not in _LOOPBACK_HOSTS:
        raise _fail_config(
            "dev_postgres.identity_mismatch",
            "WORKFLOW_DATABASE_URL must target the managed repo-local Postgres cluster on a loopback host or local socket",
            details={
                "database_url": config.database_url,
                "data_dir": str(config.data_dir),
                "expected_port": config.cluster_port,
                "actual_host": target.host,
                "actual_port": target.port,
            },
        )


def resolve_local_postgres_config(
    env: Mapping[str, str] | None = None,
) -> DevPostgresConfig:
    """Resolve the explicit config required for local developer control."""

    data_dir = resolve_local_postgres_data_dir(env=env)
    database_url = resolve_workflow_database_url(env=env)
    log_file = _DEFAULT_LOG_FILE
    config = DevPostgresConfig(
        data_dir=data_dir,
        log_file=log_file,
        database_url=database_url,
        pg_ctl=_resolve_pg_ctl(),
        cluster_port=_resolve_cluster_port(data_dir),
    )
    _assert_database_url_targets_managed_cluster(config)
    return config


def _run_coroutine(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    try:
        coro.close()
    except Exception:
        pass
    raise DevPostgresError(
        "dev_postgres.event_loop_active",
        "sync local Postgres helpers require a non-async call boundary",
    )


def _pg_ctl_status(config: DevPostgresConfig) -> tuple[bool, str]:
    completed = subprocess.run(
        [config.pg_ctl, "-D", str(config.data_dir), "status"],
        check=False,
        capture_output=True,
        text=True,
    )
    output = (completed.stdout or completed.stderr or "").strip()
    return completed.returncode == 0, output


def _read_postmaster_pid(data_dir: Path) -> tuple[int | None, int | None]:
    pid_file = data_dir / "postmaster.pid"
    if not pid_file.exists():
        return None, None
    try:
        lines = pid_file.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None, None
    pid = None
    port = None
    if lines:
        try:
            pid = int(lines[0].strip())
        except ValueError:
            pid = None
    if len(lines) >= 4:
        try:
            port = int(lines[3].strip())
        except ValueError:
            port = None
    return pid, port


async def _probe_database(config: DevPostgresConfig) -> tuple[bool, bool, tuple[str, ...]]:
    try:
        conn = await asyncpg.connect(config.database_url, timeout=_CONNECTION_TIMEOUT_S)
    except (OSError, asyncpg.PostgresError):
        return False, False, ()
    try:
        identity_row = await conn.fetchrow(
            """
            SELECT
                current_setting('data_directory') AS data_directory,
                current_setting('port') AS port
            """
        )
        if identity_row is None:
            raise _fail_config(
                "dev_postgres.identity_mismatch",
                "WORKFLOW_DATABASE_URL must resolve to the managed repo-local Postgres cluster",
                details={"database_url": config.database_url, "data_dir": str(config.data_dir)},
            )
        actual_data_dir = Path(str(identity_row["data_directory"])).resolve()
        actual_port = int(identity_row["port"])
        if actual_data_dir != config.data_dir or actual_port != config.cluster_port:
            raise _fail_config(
                "dev_postgres.identity_mismatch",
                "WORKFLOW_DATABASE_URL points at a different Postgres cluster than the managed repo-local data directory",
                details={
                    "database_url": config.database_url,
                    "expected_data_dir": str(config.data_dir),
                    "actual_data_dir": str(actual_data_dir),
                    "expected_port": config.cluster_port,
                    "actual_port": actual_port,
                },
            )

        readiness = await inspect_workflow_schema(conn)
        return True, readiness.is_bootstrapped, readiness.missing_relations
    except PostgresConfigurationError:
        raise
    except (OSError, ValueError):
        raise _fail_config(
            "dev_postgres.identity_invalid",
            "failed to verify the managed repo-local Postgres cluster identity",
            details={"database_url": config.database_url, "data_dir": str(config.data_dir)},
        )
    except asyncpg.PostgresError:
        return True, False, ()
    finally:
        await conn.close()


def _collect_local_postgres_health(config: DevPostgresConfig) -> DevPostgresStatus:
    process_running, _ = _pg_ctl_status(config)
    pid, port = _read_postmaster_pid(config.data_dir) if process_running else (None, None)
    database_reachable, schema_bootstrapped, missing_schema_objects = (
        _run_coroutine(_probe_database(config))
        if process_running
        else (False, False, ())
    )
    missing_set = set(missing_schema_objects)
    return DevPostgresStatus(
        data_dir=str(config.data_dir),
        log_file=str(config.log_file),
        database_url=config.database_url,
        pid=pid,
        port=port,
        process_running=process_running,
        database_reachable=database_reachable,
        schema_bootstrapped=schema_bootstrapped,
        missing_schema_objects=tuple(missing_schema_objects),
        compile_artifact_authority_ready=all(
            name not in missing_set
            for name in ("compile_artifacts", "capability_catalog", "verify_refs")
        ),
        compile_index_authority_ready="compile_index_snapshots" not in missing_set,
        execution_packet_authority_ready="execution_packets" not in missing_set,
        repo_snapshot_authority_ready="repo_snapshots" not in missing_set,
        verification_registry_ready="verification_registry" not in missing_set,
        verifier_authority_ready=all(
            name not in missing_set
            for name in ("verifier_registry", "verification_runs")
        ),
        healer_authority_ready=all(
            name not in missing_set
            for name in ("healer_registry", "verifier_healer_bindings", "healing_runs")
        ),
    )


async def _connect_verified_database(config: DevPostgresConfig) -> asyncpg.Connection:
    try:
        conn = await asyncpg.connect(
            config.database_url,
            timeout=_CONNECTION_TIMEOUT_S,
        )
    except (OSError, asyncpg.PostgresError) as exc:
        raise DevPostgresError(
            "dev_postgres.bootstrap_failed",
            "failed to connect to the local Postgres database for bootstrap",
            details={
                "database_url": config.database_url,
                "data_dir": str(config.data_dir),
            },
        ) from exc

    try:
        row = await conn.fetchrow(
            """
            SELECT
                current_setting('data_directory') AS data_directory,
                current_setting('port') AS port
            """
        )
        if row is None:
            raise _fail_config(
                "dev_postgres.identity_mismatch",
                "WORKFLOW_DATABASE_URL must resolve to the managed repo-local Postgres cluster",
                details={"database_url": config.database_url, "data_dir": str(config.data_dir)},
            )
        actual_data_dir = Path(str(row["data_directory"])).resolve()
        actual_port = int(row["port"])
    except PostgresConfigurationError:
        await conn.close()
        raise
    except (OSError, ValueError, asyncpg.PostgresError) as exc:
        await conn.close()
        raise DevPostgresError(
            "dev_postgres.bootstrap_failed",
            "failed to verify the local Postgres cluster identity before bootstrap",
            details={
                "database_url": config.database_url,
                "data_dir": str(config.data_dir),
            },
        ) from exc

    if actual_data_dir != config.data_dir or actual_port != config.cluster_port:
        await conn.close()
        raise _fail_config(
            "dev_postgres.identity_mismatch",
            "WORKFLOW_DATABASE_URL points at a different Postgres cluster than the managed repo-local data directory",
            details={
                "database_url": config.database_url,
                "expected_data_dir": str(config.data_dir),
                "actual_data_dir": str(actual_data_dir),
                "expected_port": config.cluster_port,
                "actual_port": actual_port,
            },
        )

    return conn


def local_postgres_status(
    env: Mapping[str, str] | None = None,
) -> DevPostgresStatus:
    """Backward-compatible alias for the explicit health surface."""

    return local_postgres_health(env=env)


def local_postgres_health(
    env: Mapping[str, str] | None = None,
) -> DevPostgresStatus:
    """Return the explicit read-only health snapshot for the repo-local cluster."""
    raise _native_postgres_disabled()


def local_postgres_up(
    env: Mapping[str, str] | None = None,
) -> DevPostgresStatus:
    """Start the repo-local cluster if it is not already running."""
    raise _native_postgres_disabled()


def local_postgres_down(
    env: Mapping[str, str] | None = None,
) -> DevPostgresStatus:
    """Stop the repo-local cluster if it is running."""
    raise _native_postgres_disabled()


def local_postgres_bootstrap(
    env: Mapping[str, str] | None = None,
) -> DevPostgresStatus:
    """Apply the canonical workflow schema to the explicit local database."""
    raise _native_postgres_disabled()


def local_postgres_restart(
    env: Mapping[str, str] | None = None,
) -> DevPostgresStatus:
    """Restart the local cluster without mutating schema."""
    raise _native_postgres_disabled()


def _print_status(status: DevPostgresStatus) -> None:
    json.dump(status.to_json(), sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Repo-local Postgres developer helper")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("up", "down", "health", "status", "bootstrap", "restart"):
        subparsers.add_parser(command)

    args = parser.parse_args(argv)
    if args.command == "up":
        _print_status(local_postgres_up())
    elif args.command == "down":
        _print_status(local_postgres_down())
    elif args.command in {"health", "status"}:
        _print_status(local_postgres_health())
    elif args.command == "bootstrap":
        _print_status(local_postgres_bootstrap())
    elif args.command == "restart":
        _print_status(local_postgres_restart())
    else:  # pragma: no cover - argparse guarantees the command.
        raise AssertionError(f"unsupported command: {args.command}")
    return 0


if __name__ == "__main__":  # pragma: no cover - manual developer entrypoint
    raise SystemExit(main())


__all__ = [
    "PRAXIS_LOCAL_POSTGRES_DATA_DIR_ENV",
    "DevPostgresConfig",
    "DevPostgresError",
    "DevPostgresStatus",
    "local_postgres_bootstrap",
    "local_postgres_down",
    "local_postgres_health",
    "local_postgres_restart",
    "local_postgres_status",
    "local_postgres_up",
    "main",
    "resolve_local_postgres_config",
    "resolve_local_postgres_data_dir",
]
