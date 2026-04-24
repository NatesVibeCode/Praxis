"""Platform gate probes: Homebrew, Python 3.14, Postgres role, pgvector."""

from __future__ import annotations

import shutil
import subprocess
from collections.abc import Mapping
from pathlib import Path

from .graph import (
    GateProbe,
    GateResult,
    ONBOARDING_GRAPH,
    gate_result,
)


_HOMEBREW = GateProbe(
    gate_ref="platform.homebrew",
    domain="platform",
    title="Homebrew installed",
    purpose=(
        "Bootstrap prereqs on macOS (python@3.14, postgresql@16, node) are "
        "installed with brew; the fresh-clone error messages assume brew is on PATH."
    ),
    platforms=("darwin",),
    ok_cache_ttl_s=3600,
)


_PYTHON_3_14 = GateProbe(
    gate_ref="platform.python3_14",
    domain="platform",
    title="Python 3.14 on PATH",
    purpose=(
        "Praxis native-operator wrappers (scripts/native-operator-common.sh) are "
        "pinned to Python 3.14; ./scripts/bootstrap and the workflow worker both "
        "require this exact version."
    ),
    ok_cache_ttl_s=3600,
)


_PSQL = GateProbe(
    gate_ref="platform.psql",
    domain="platform",
    title="psql client on PATH",
    purpose=(
        "Migrations and probe-layer queries shell out to psql; the client binary "
        "must be available before Postgres-side gates can be evaluated."
    ),
    ok_cache_ttl_s=3600,
)


_POSTGRES_ROLE = GateProbe(
    gate_ref="platform.postgres_role",
    domain="platform",
    title="Postgres connection role has CREATE + SUPERUSER",
    purpose=(
        "scripts/bootstrap creates the praxis database and enables pgvector; "
        "both require CREATEDB and ideally SUPERUSER on the connecting role."
    ),
    depends_on=("platform.psql",),
    ok_cache_ttl_s=300,
)


_PGVECTOR = GateProbe(
    gate_ref="platform.pgvector",
    domain="platform",
    title="pgvector extension available",
    purpose=(
        "Semantic compile, discover, and recall all use pgvector-backed "
        "embeddings; the extension must be installed for the target database."
    ),
    depends_on=("platform.psql",),
    ok_cache_ttl_s=300,
)


def probe_homebrew(env: Mapping[str, str], repo_root: Path) -> GateResult:
    brew_path = shutil.which("brew")
    if brew_path is None:
        return gate_result(
            _HOMEBREW,
            status="missing",
            observed_state={"brew_on_path": False},
            remediation_hint=(
                'Install Homebrew: /bin/bash -c "$(curl -fsSL '
                'https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"'
            ),
            remediation_doc_url="https://brew.sh",
        )
    return gate_result(
        _HOMEBREW,
        status="ok",
        observed_state={"brew_on_path": True, "brew_path": brew_path},
    )


def probe_python_3_14(env: Mapping[str, str], repo_root: Path) -> GateResult:
    path = shutil.which("python3.14")
    if path is None:
        return gate_result(
            _PYTHON_3_14,
            status="missing",
            observed_state={"python3_14_on_path": False},
            remediation_hint=(
                "Install Python 3.14: brew install python@3.14 (macOS) or "
                "sudo apt install python3.14 python3.14-venv (Linux with deadsnakes PPA)"
            ),
        )
    try:
        completed = subprocess.run(
            [path, "-c", "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}')"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
        return gate_result(
            _PYTHON_3_14,
            status="blocked",
            observed_state={"python3_14_on_path": True, "python3_14_path": path, "error": str(exc)},
            remediation_hint=f"python3.14 is on PATH at {path} but does not execute; reinstall via brew or apt",
        )
    version = (completed.stdout or "").strip()
    if not version.startswith("3.14"):
        return gate_result(
            _PYTHON_3_14,
            status="blocked",
            observed_state={"python3_14_path": path, "reported_version": version},
            remediation_hint=f"python3.14 at {path} reports version {version}; reinstall python@3.14",
        )
    return gate_result(
        _PYTHON_3_14,
        status="ok",
        observed_state={"python3_14_path": path, "version": version},
    )


def probe_psql(env: Mapping[str, str], repo_root: Path) -> GateResult:
    path = shutil.which("psql")
    if path is None:
        return gate_result(
            _PSQL,
            status="missing",
            observed_state={"psql_on_path": False},
            remediation_hint=(
                "Install Postgres 16+ (includes psql): brew install postgresql@16 "
                "(macOS) or sudo apt install postgresql-client-16 (Linux)"
            ),
        )
    return gate_result(
        _PSQL,
        status="ok",
        observed_state={"psql_on_path": True, "psql_path": path},
    )


def _resolve_database_url(env: Mapping[str, str]) -> str | None:
    url = (env.get("WORKFLOW_DATABASE_URL") or "").strip()
    return url or None


def probe_postgres_role(env: Mapping[str, str], repo_root: Path) -> GateResult:
    database_url = _resolve_database_url(env)
    if database_url is None:
        return gate_result(
            _POSTGRES_ROLE,
            status="unknown",
            observed_state={"database_url_set": False},
            remediation_hint=(
                "Set WORKFLOW_DATABASE_URL (form: postgresql://user@host:5432/praxis) "
                "before probing Postgres role privileges"
            ),
        )
    try:
        completed = subprocess.run(
            [
                "psql",
                database_url,
                "-Atc",
                "SELECT rolcreatedb, rolsuper FROM pg_roles WHERE rolname = current_user",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        return gate_result(
            _POSTGRES_ROLE,
            status="blocked",
            observed_state={"psql_error": stderr or str(exc)},
            remediation_hint=(
                "Postgres rejected the role lookup. On Linux: "
                'sudo -u postgres psql -c "CREATE USER $USER SUPERUSER". '
                "On macOS: ensure brew services postgresql@16 is running and your shell user is a role."
            ),
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        return gate_result(
            _POSTGRES_ROLE,
            status="blocked",
            observed_state={"error": str(exc), "error_type": type(exc).__name__},
            remediation_hint="Postgres did not respond within 10s; check pg_isready and that the DSN is reachable",
        )
    raw = (completed.stdout or "").strip()
    if not raw:
        return gate_result(
            _POSTGRES_ROLE,
            status="missing",
            observed_state={"role_row_found": False},
            remediation_hint=(
                "Current shell user is not a Postgres role. Create one: "
                'sudo -u postgres psql -c "CREATE USER $USER SUPERUSER"'
            ),
        )
    parts = raw.split("|")
    rolcreatedb = parts[0].strip().lower() == "t"
    rolsuper = parts[1].strip().lower() == "t" if len(parts) > 1 else False
    if not rolcreatedb:
        return gate_result(
            _POSTGRES_ROLE,
            status="blocked",
            observed_state={"rolcreatedb": False, "rolsuper": rolsuper},
            remediation_hint=(
                'Grant CREATEDB: sudo -u postgres psql -c "ALTER USER $USER CREATEDB"'
            ),
        )
    return gate_result(
        _POSTGRES_ROLE,
        status="ok",
        observed_state={"rolcreatedb": True, "rolsuper": rolsuper},
    )


def probe_pgvector(env: Mapping[str, str], repo_root: Path) -> GateResult:
    database_url = _resolve_database_url(env)
    if database_url is None:
        return gate_result(
            _PGVECTOR,
            status="unknown",
            observed_state={"database_url_set": False},
            remediation_hint="Set WORKFLOW_DATABASE_URL before probing pgvector availability",
        )
    try:
        completed = subprocess.run(
            [
                "psql",
                database_url,
                "-Atc",
                "SELECT 1 FROM pg_available_extensions WHERE name = 'vector'",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        return gate_result(
            _PGVECTOR,
            status="blocked",
            observed_state={"psql_error": stderr or str(exc)},
            remediation_hint="pgvector availability check failed; ensure Postgres is reachable and role has catalog access",
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        return gate_result(
            _PGVECTOR,
            status="blocked",
            observed_state={"error": str(exc), "error_type": type(exc).__name__},
            remediation_hint="pgvector probe timed out or could not run psql",
        )
    has_extension = (completed.stdout or "").strip() == "1"
    if not has_extension:
        return gate_result(
            _PGVECTOR,
            status="missing",
            observed_state={"extension_available": False},
            remediation_hint=(
                "Install pgvector: brew install pgvector/brew/pgvector (macOS) or "
                "sudo apt install postgresql-16-pgvector (Linux). After install, "
                "run ./scripts/bootstrap which issues CREATE EXTENSION IF NOT EXISTS vector."
            ),
        )
    return gate_result(
        _PGVECTOR,
        status="ok",
        observed_state={"extension_available": True},
    )


def register(graph=ONBOARDING_GRAPH) -> None:
    graph.register(_HOMEBREW, probe_homebrew)
    graph.register(_PYTHON_3_14, probe_python_3_14)
    graph.register(_PSQL, probe_psql)
    graph.register(_POSTGRES_ROLE, probe_postgres_role)
    graph.register(_PGVECTOR, probe_pgvector)
