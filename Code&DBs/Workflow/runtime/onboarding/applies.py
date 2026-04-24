"""Gate apply handlers: mutations that move a gate from ``missing`` to ``ok``.

Each handler performs one minimal mutation and re-probes to produce a fresh
``GateResult``. All handlers are idempotent — re-running a successful apply
must be a no-op, not an error.

Handlers live here rather than inside each probes_* module so the mutation
authority is one surface. Probes read; apply handlers write.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .graph import GateApply, GateResult, ONBOARDING_GRAPH, gate_result


def _now() -> datetime:
    return datetime.now(timezone.utc)


# --- MCP: write ~/.claude/.mcp.json praxis entry ----------------------------


def _mcp_json_path(env: Mapping[str, str]) -> Path:
    claude_home = env.get("CLAUDE_HOME") or str(Path.home() / ".claude")
    return Path(claude_home) / ".mcp.json"


def _workflow_root(repo_root: Path) -> Path:
    return repo_root / "Code&DBs" / "Workflow"


def apply_claude_code_mcp(
    env: Mapping[str, str],
    repo_root: Path,
    *,
    database_url: str | None = None,
) -> GateResult:
    """Write or update the praxis entry in ~/.claude/.mcp.json."""
    from . import probes_mcp

    probe = ONBOARDING_GRAPH.probe("mcp.claude_code")
    mcp_path = _mcp_json_path(env)
    workflow_root = _workflow_root(repo_root)

    resolved_db_url = (
        database_url
        or env.get("WORKFLOW_DATABASE_URL")
        or ""
    ).strip()
    if not resolved_db_url:
        return gate_result(
            probe,
            status="blocked",
            observed_state={"mcp_json_path": str(mcp_path), "database_url_resolved": False},
            remediation_hint=(
                "Cannot write .mcp.json: WORKFLOW_DATABASE_URL is not set. "
                "Set it in your shell or .env before running apply."
            ),
        )

    mcp_path.parent.mkdir(parents=True, exist_ok=True)
    if mcp_path.exists():
        try:
            raw = mcp_path.read_text(encoding="utf-8")
            config = json.loads(raw) if raw.strip() else {}
            if not isinstance(config, dict):
                config = {}
        except json.JSONDecodeError:
            return gate_result(
                probe,
                status="blocked",
                observed_state={"mcp_json_path": str(mcp_path), "parse_error": True},
                remediation_hint=(
                    f"{mcp_path} already exists and is not valid JSON. Back it up and "
                    "re-run apply, or fix the file by hand."
                ),
            )
    else:
        config = {}

    servers = config.setdefault("mcpServers", {})
    servers["praxis"] = {
        "command": "python",
        "args": ["-m", "surfaces.mcp.server"],
        "cwd": str(workflow_root),
        "env": {"WORKFLOW_DATABASE_URL": resolved_db_url},
    }

    tmp_path = mcp_path.with_suffix(".json.tmp")
    tmp_path.write_text(
        json.dumps(config, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    os.replace(tmp_path, mcp_path)

    # Re-probe to confirm the write succeeded.
    result = probes_mcp.probe_claude_code_mcp(env, repo_root)
    return result


_APPLY_CLAUDE_CODE_MCP = GateApply(
    apply_ref="apply.mcp.claude_code.write",
    gate_ref="mcp.claude_code",
    description=(
        "Write or update the praxis entry in ~/.claude/.mcp.json so Claude Code "
        "can reach the praxis_* MCP tools."
    ),
    handler=apply_claude_code_mcp,
    mutates=("filesystem:~/.claude/.mcp.json",),
    requires_approval=True,
)


# --- Runtime: .env file -----------------------------------------------------


def apply_env_file_write(
    env: Mapping[str, str],
    repo_root: Path,
    *,
    database_url: str | None = None,
    api_port: str | None = None,
) -> GateResult:
    """Write repo .env with WORKFLOW_DATABASE_URL + WORKFLOW_DATABASE_TRUSTED."""
    from . import probes_runtime

    probe = ONBOARDING_GRAPH.probe("runtime.env_file")
    env_path = repo_root / ".env"

    if env_path.exists():
        # Idempotent: if the file already declares WORKFLOW_DATABASE_URL, leave it.
        body = env_path.read_text(encoding="utf-8", errors="replace")
        if any(
            line.strip().startswith("WORKFLOW_DATABASE_URL=") and line.strip() != "WORKFLOW_DATABASE_URL="
            for line in body.splitlines()
        ):
            return probes_runtime.probe_env_file(env, repo_root)

    resolved_db_url = (database_url or env.get("WORKFLOW_DATABASE_URL") or "").strip()
    if not resolved_db_url:
        return gate_result(
            probe,
            status="blocked",
            observed_state={"env_path": str(env_path), "database_url_resolved": False},
            remediation_hint=(
                "Cannot write .env: pass database_url=... or set WORKFLOW_DATABASE_URL "
                "in the environment."
            ),
        )

    port = (api_port or env.get("PRAXIS_API_PORT") or "8420").strip()
    lines = [
        "# Created by apply.runtime.env_file.write.",
        f"WORKFLOW_DATABASE_URL={resolved_db_url}",
        "WORKFLOW_DATABASE_TRUSTED=true",
        f"PRAXIS_API_PORT={port}",
    ]
    tmp_path = env_path.with_suffix(".env.tmp")
    tmp_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.chmod(tmp_path, 0o600)
    os.replace(tmp_path, env_path)

    return probes_runtime.probe_env_file(env, repo_root)


_APPLY_ENV_FILE = GateApply(
    apply_ref="apply.runtime.env_file.write",
    gate_ref="runtime.env_file",
    description="Write repo-local .env with WORKFLOW_DATABASE_URL.",
    handler=apply_env_file_write,
    mutates=("filesystem:$REPO_ROOT/.env",),
    requires_approval=True,
)


# --- Platform: create the workflow database --------------------------------


def apply_workflow_database_create(
    env: Mapping[str, str],
    repo_root: Path,
) -> GateResult:
    """Idempotent CREATE DATABASE praxis via the maintenance URL."""
    from . import probes_platform

    probe = ONBOARDING_GRAPH.probe("platform.workflow_database")
    database_url = (env.get("WORKFLOW_DATABASE_URL") or "").strip()
    if not database_url:
        return gate_result(
            probe,
            status="blocked",
            observed_state={"database_url_set": False},
            remediation_hint="Set WORKFLOW_DATABASE_URL before applying workflow_database.create",
        )

    database_name = probes_platform._parse_database_name(database_url)
    maintenance_url = probes_platform._maintenance_url(database_url)

    try:
        check = subprocess.run(
            [
                "psql",
                maintenance_url,
                "-v",
                f"db_name={database_name}",
                "-Atc",
                "SELECT 1 FROM pg_database WHERE datname = :'db_name'",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if (check.stdout or "").strip() == "1":
            # Already exists; idempotent no-op.
            return probes_platform.probe_workflow_database(env, repo_root)

        subprocess.run(
            [
                "psql",
                maintenance_url,
                "-v",
                "ON_ERROR_STOP=1",
                "-v",
                f"db_name={database_name}",
                "-c",
                'CREATE DATABASE :"db_name"',
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        return gate_result(
            probe,
            status="blocked",
            observed_state={
                "maintenance_url": maintenance_url,
                "database_name": database_name,
                "psql_error": stderr or str(exc),
            },
            remediation_hint=(
                f"CREATE DATABASE {database_name} failed. The connecting role may "
                'lack CREATEDB: sudo -u postgres psql -c "ALTER USER $USER CREATEDB"'
            ),
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        return gate_result(
            probe,
            status="blocked",
            observed_state={"error": str(exc), "error_type": type(exc).__name__},
            remediation_hint="CREATE DATABASE timed out against the maintenance URL",
        )

    return probes_platform.probe_workflow_database(env, repo_root)


_APPLY_WORKFLOW_DATABASE = GateApply(
    apply_ref="apply.platform.workflow_database.create",
    gate_ref="platform.workflow_database",
    description="Run CREATE DATABASE praxis if the target database is missing.",
    handler=apply_workflow_database_create,
    mutates=("postgres:CREATE DATABASE",),
    requires_approval=True,
)


# --- Platform: enable pgvector extension -----------------------------------


def apply_pgvector_enable(
    env: Mapping[str, str],
    repo_root: Path,
) -> GateResult:
    """Idempotent CREATE EXTENSION IF NOT EXISTS vector on the target database."""
    from . import probes_platform

    probe = ONBOARDING_GRAPH.probe("platform.pgvector_installed")
    database_url = (env.get("WORKFLOW_DATABASE_URL") or "").strip()
    if not database_url:
        return gate_result(
            probe,
            status="blocked",
            observed_state={"database_url_set": False},
            remediation_hint="Set WORKFLOW_DATABASE_URL before applying pgvector_installed.enable",
        )

    try:
        subprocess.run(
            [
                "psql",
                database_url,
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "CREATE EXTENSION IF NOT EXISTS vector",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        return gate_result(
            probe,
            status="blocked",
            observed_state={"psql_error": stderr or str(exc)},
            remediation_hint=(
                "CREATE EXTENSION vector failed. Ensure pgvector is installed "
                "on the server (brew install pgvector/brew/pgvector on macOS, "
                "sudo apt install postgresql-16-pgvector on Linux)."
            ),
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        return gate_result(
            probe,
            status="blocked",
            observed_state={"error": str(exc), "error_type": type(exc).__name__},
            remediation_hint="CREATE EXTENSION timed out",
        )

    return probes_platform.probe_pgvector_installed(env, repo_root)


_APPLY_PGVECTOR_ENABLE = GateApply(
    apply_ref="apply.platform.pgvector_installed.enable",
    gate_ref="platform.pgvector_installed",
    description="Run CREATE EXTENSION IF NOT EXISTS vector on the target database.",
    handler=apply_pgvector_enable,
    mutates=("postgres:CREATE EXTENSION vector",),
    requires_approval=True,
)


# --- Providers: emit the exact command the operator must run ---------------
# Provider credentials live in macOS Keychain (darwin) or env vars (linux).
# Apply does not secretly type values on the user's behalf — it returns a
# result whose remediation_hint contains the precise command to paste.


def _provider_apply_factory(provider_slug: str, env_var: str, human_name: str):
    def _apply(env: Mapping[str, str], repo_root: Path) -> GateResult:
        from . import probes_provider

        probe_ref = f"provider.{provider_slug}"
        probe = ONBOARDING_GRAPH.probe(probe_ref)
        probe_fn = {
            "openai": probes_provider.probe_openai,
            "google": probes_provider.probe_google,
            "openrouter": probes_provider.probe_openrouter,
            "deepseek": probes_provider.probe_deepseek,
        }[provider_slug]

        # Re-probe: if the credential is already resolvable, return ok.
        current = probe_fn(env, repo_root)
        if current.status == "ok":
            return current

        # Otherwise emit a remediation hint. Apply does not type keys for users.
        if sys.platform == "darwin":
            hint = (
                f"Run this command with your {human_name} API key filled in, then "
                "re-run apply to verify:\n"
                f'  security add-generic-password -U -a "praxis" -s "{env_var}" -w "<your-key>"'
            )
        else:
            hint = (
                f"Export {env_var} in your shell rc with your {human_name} API key, "
                "then re-run apply:\n"
                f'  echo \'export {env_var}="<your-key>"\' >> ~/.bashrc && source ~/.bashrc'
            )
        return gate_result(
            probe,
            status="missing",
            observed_state={
                "env_var": env_var,
                "provider_slug": provider_slug,
                "apply_emits_command_only": True,
            },
            remediation_hint=hint,
        )

    return _apply


_PROVIDER_APPLIES = {
    "openai": ("OPENAI_API_KEY", "OpenAI"),
    "google": ("GEMINI_API_KEY", "Google (Gemini)"),
    "openrouter": ("OPENROUTER_API_KEY", "OpenRouter"),
    "deepseek": ("DEEPSEEK_API_KEY", "DeepSeek"),
}


def register(graph=ONBOARDING_GRAPH) -> None:
    graph.register_apply(_APPLY_CLAUDE_CODE_MCP)
    graph.register_apply(_APPLY_ENV_FILE)
    graph.register_apply(_APPLY_WORKFLOW_DATABASE)
    graph.register_apply(_APPLY_PGVECTOR_ENABLE)
    for provider_slug, (env_var, human_name) in _PROVIDER_APPLIES.items():
        apply_entry = GateApply(
            apply_ref=f"apply.provider.{provider_slug}.remediate",
            gate_ref=f"provider.{provider_slug}",
            description=(
                f"Verify that the {human_name} credential resolves; if not, emit "
                "the exact command the operator must run."
            ),
            handler=_provider_apply_factory(provider_slug, env_var, human_name),
            mutates=(),
            requires_approval=False,
        )
        graph.register_apply(apply_entry)
