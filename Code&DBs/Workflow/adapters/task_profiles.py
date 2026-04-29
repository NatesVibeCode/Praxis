"""Task type → tool profile mapping for configurable LLM adapter behavior.

Profiles and keyword routing rules are authoritative in Postgres
(task_type_profiles, task_type_keyword_rules). Runtime code should never
invent task-profile or keyword-routing authority outside those tables.

Each TaskProfile specifies:
  - allowed_tools: which tools the model can use
  - default_tier: routing tier when tier is not explicitly set
  - file_attach: whether to attach image/file resources
  - system_prompt_hint: task-specific instruction appended to system prompt
  - default_scope_read/write: inferred file scope when not explicit in spec
  - default_authoring_contract/acceptance_contract: fallback contracts
"""

from __future__ import annotations

import asyncio
from runtime.async_bridge import run_sync_safe
import concurrent.futures
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from runtime.workspace_paths import repo_root as workspace_repo_root
from storage.postgres.connection import resolve_workflow_database_url
from storage.postgres.validators import PostgresConfigurationError

_DATABASE_URL_ENV = "WORKFLOW_DATABASE_URL"


class TaskProfileAuthorityError(RuntimeError):
    """Raised when DB-backed task profile authority is unavailable."""


@dataclass(frozen=True)
class TaskProfile:
    """Configuration profile for a task type."""

    task_type: str
    allowed_tools: tuple[str, ...]
    default_tier: str  # "frontier", "mid", "economy"
    file_attach: bool
    system_prompt_hint: str
    default_scope_read: tuple[str, ...] = ()
    default_scope_write: tuple[str, ...] = ()
    default_authoring_contract: dict[str, Any] | None = None
    default_acceptance_contract: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# DB-backed loader
# ---------------------------------------------------------------------------

_PROFILES_DB_LOADED = False
_DB_TASK_PROFILES: dict[str, TaskProfile] | None = None
_DB_TASK_TYPE_KEYWORDS: list[tuple[tuple[str, ...], str, tuple[str, ...], tuple[str, ...]]] | None = None


def _run_async(coro: object) -> object:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is not None and loop.is_running():
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result(timeout=10)
    return run_sync_safe(coro)


def _read_repo_env_file(path: Path) -> dict[str, str]:
    try:
        content = path.read_text(encoding="utf-8")
    except OSError:
        return {}

    parsed: dict[str, str] = {}
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        if key and value:
            parsed[key] = value
    return parsed


def _task_profile_repo_root() -> Path:
    return workspace_repo_root()


def _resolve_task_profile_database_url(*, required: bool) -> str | None:
    if _DATABASE_URL_ENV in os.environ:
        raw_env_value = str(os.environ.get(_DATABASE_URL_ENV, "")).strip()
        if not raw_env_value:
            if required:
                raise TaskProfileAuthorityError(
                    "task_profiles requires explicit WORKFLOW_DATABASE_URL Postgres authority"
                )
            return None
        try:
            return resolve_workflow_database_url()
        except PostgresConfigurationError as exc:
            raise TaskProfileAuthorityError(str(exc)) from exc

    repo_env = _read_repo_env_file(_task_profile_repo_root() / ".env")
    if _DATABASE_URL_ENV in repo_env:
        try:
            return resolve_workflow_database_url(repo_env)
        except PostgresConfigurationError as exc:
            raise TaskProfileAuthorityError(str(exc)) from exc

    if required:
        raise TaskProfileAuthorityError(
            "task_profiles requires explicit WORKFLOW_DATABASE_URL Postgres authority"
        )
    return None


def _load_profiles_from_db(*, required: bool) -> None:
    global _PROFILES_DB_LOADED, _DB_TASK_PROFILES, _DB_TASK_TYPE_KEYWORDS
    if _PROFILES_DB_LOADED:
        return

    try:
        import asyncpg
        db_url = _resolve_task_profile_database_url(required=required)
        if db_url is None:
            return

        async def _fetch() -> tuple[list, list]:
            conn = await asyncpg.connect(db_url)
            try:
                profiles = await conn.fetch(
                    "SELECT task_type, allowed_tools, default_tier, file_attach, system_prompt_hint,"
                    "       default_scope_read, default_scope_write,"
                    "       default_authoring_contract, default_acceptance_contract "
                    "FROM task_type_profiles WHERE status = 'active' ORDER BY task_type"
                )
                keywords = await conn.fetch(
                    "SELECT keywords, task_type, sort_order, context_code_clues, context_creative_clues "
                    "FROM task_type_keyword_rules ORDER BY sort_order"
                )
                return list(profiles), list(keywords)
            finally:
                await conn.close()

        profile_rows, keyword_rows = _run_async(_fetch())  # type: ignore[misc]

        loaded_profiles: dict[str, TaskProfile] = {}
        if profile_rows:
            for row in profile_rows:
                tools = row["allowed_tools"]
                if isinstance(tools, str):
                    tools = json.loads(tools)
                task_type = str(row["task_type"])
                scope_read_raw = row.get("default_scope_read")
                if isinstance(scope_read_raw, str):
                    scope_read_raw = json.loads(scope_read_raw)
                scope_write_raw = row.get("default_scope_write")
                if isinstance(scope_write_raw, str):
                    scope_write_raw = json.loads(scope_write_raw)
                auth_contract_raw = row.get("default_authoring_contract")
                if isinstance(auth_contract_raw, str):
                    auth_contract_raw = json.loads(auth_contract_raw)
                acc_contract_raw = row.get("default_acceptance_contract")
                if isinstance(acc_contract_raw, str):
                    acc_contract_raw = json.loads(acc_contract_raw)
                loaded_profiles[task_type] = TaskProfile(
                    task_type=task_type,
                    allowed_tools=tuple(str(t) for t in (tools or [])),
                    default_tier=str(row["default_tier"]),
                    file_attach=bool(row["file_attach"]),
                    system_prompt_hint=str(row["system_prompt_hint"] or ""),
                    default_scope_read=tuple(str(p) for p in (scope_read_raw or [])),
                    default_scope_write=tuple(str(p) for p in (scope_write_raw or [])),
                    default_authoring_contract=auth_contract_raw if isinstance(auth_contract_raw, dict) else None,
                    default_acceptance_contract=acc_contract_raw if isinstance(acc_contract_raw, dict) else None,
                )

        loaded_keywords: list[tuple[tuple[str, ...], str, tuple[str, ...], tuple[str, ...]]] = []
        if keyword_rows:
            for row in keyword_rows:
                kws = tuple(str(k) for k in (row["keywords"] or []))
                if not kws:
                    continue
                code_clues = tuple(str(c) for c in (row["context_code_clues"] or []))
                creative_clues = tuple(str(c) for c in (row["context_creative_clues"] or []))
                loaded_keywords.append((kws, str(row["task_type"]), code_clues, creative_clues))

        _DB_TASK_PROFILES = loaded_profiles
        _DB_TASK_TYPE_KEYWORDS = loaded_keywords
    except TaskProfileAuthorityError:
        raise
    except ImportError as exc:
        if required:
            raise TaskProfileAuthorityError("task_profiles requires asyncpg for DB-backed authority") from exc
    except Exception as exc:
        if required:
            raise TaskProfileAuthorityError(
                f"task_profiles failed to load DB authority: {type(exc).__name__}: {exc}"
            ) from exc
    finally:
        _PROFILES_DB_LOADED = True


def reload_profiles_from_db() -> None:
    """Force a fresh DB read on the next lookup."""
    global _PROFILES_DB_LOADED, _DB_TASK_PROFILES, _DB_TASK_TYPE_KEYWORDS
    _PROFILES_DB_LOADED = False
    _DB_TASK_PROFILES = None
    _DB_TASK_TYPE_KEYWORDS = None


def try_resolve_profile(task_type: str) -> TaskProfile | None:
    """Preview/bootstrap-only profile lookup.

    Execution-bound callers must use ``resolve_profile`` so missing DB-backed
    task-profile authority cannot silently collapse into generic behavior.
    """
    _load_profiles_from_db(required=False)
    if _DB_TASK_PROFILES is None:
        return None
    if task_type in _DB_TASK_PROFILES:
        return _DB_TASK_PROFILES[task_type]
    return _DB_TASK_PROFILES.get("general")


# ---------------------------------------------------------------------------
# Resolution and inference
# ---------------------------------------------------------------------------

def resolve_profile(task_type: str) -> TaskProfile:
    """Look up a task profile by type.

    Returns the profile for task_type, or the "general" profile if
    task_type is unknown.
    """
    _load_profiles_from_db(required=True)
    if not _DB_TASK_PROFILES:
        raise TaskProfileAuthorityError("task_profiles DB authority returned no active profiles")
    if task_type in _DB_TASK_PROFILES:
        return _DB_TASK_PROFILES[task_type]
    general = _DB_TASK_PROFILES.get("general")
    if general is not None:
        return general
    raise TaskProfileAuthorityError(
        f"task_profiles has no active profile for {task_type!r} and no 'general' profile"
    )


def infer_task_type(
    prompt: str,
    *,
    label: str | None = None,
    require_authority: bool = False,
) -> str:
    """Infer task type from prompt text and optional label.

    Uses keyword matching against the task_type_keyword_rules registry. Falls
    back to "general" when authority is present but no keywords match.
    """
    _load_profiles_from_db(required=require_authority)
    if require_authority and _DB_TASK_TYPE_KEYWORDS is None:
        raise TaskProfileAuthorityError(
            "task_profiles keyword authority unavailable for execution-bound inference"
        )
    combined = " ".join(filter(None, [label or "", prompt])).lower()
    keyword_rules = _DB_TASK_TYPE_KEYWORDS or []

    for keywords, task_type, code_clues, creative_clues in keyword_rules:
        for kw in keywords:
            if kw in combined:
                if code_clues or creative_clues:
                    is_code = any(c in combined for c in code_clues)
                    is_creative = any(c in combined for c in creative_clues)
                    if is_code and not is_creative:
                        return "code_generation"
                    if is_creative and not is_code:
                        return "creative"
                    return task_type
                return task_type

    return "general"


def merge_allowed_tools(
    profile_tools: tuple[str, ...],
    explicit_tools: list[str] | None,
) -> list[str]:
    """Merge profile's allowed_tools with any explicit allowed_tools.

    Takes the union of both sets, preserving uniqueness.
    """
    merged = list(profile_tools)
    if explicit_tools:
        for tool in explicit_tools:
            if tool not in merged:
                merged.append(tool)
    return merged
