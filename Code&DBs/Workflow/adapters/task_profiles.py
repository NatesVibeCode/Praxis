"""Task type → tool profile mapping for configurable LLM adapter behavior.

Profiles and keyword routing rules are authoritative in Postgres
(task_type_profiles, task_type_keyword_rules). The in-module dicts are
cold-start fallbacks used only when the DB is unavailable.

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
import concurrent.futures
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

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
# Task profiles — fallback seeds (authoritative copy lives in Postgres)
# ---------------------------------------------------------------------------

_SEED_TASK_PROFILES: dict[str, TaskProfile] = {
    "research": TaskProfile(
        task_type="research",
        allowed_tools=("WebSearch", "WebFetch", "Read"),
        default_tier="mid",
        file_attach=False,
        system_prompt_hint="Search for information and cite sources.",
        default_scope_read=(),
        default_scope_write=("artifacts/",),
    ),
    "code_generation": TaskProfile(
        task_type="code_generation",
        allowed_tools=("Read", "Edit", "Write", "Bash"),
        default_tier="mid",
        file_attach=False,
        system_prompt_hint="Write clean, tested code.",
        default_scope_read=("src/", "lib/", "tests/"),
        default_scope_write=("src/", "tests/"),
    ),
    "code_edit": TaskProfile(
        task_type="code_edit",
        allowed_tools=("Read", "Edit", "Bash"),
        default_tier="mid",
        file_attach=False,
        system_prompt_hint="Make targeted edits only.",
        default_scope_read=("src/", "lib/"),
        default_scope_write=("src/",),
    ),
    "code_review": TaskProfile(
        task_type="code_review",
        allowed_tools=("Read", "Grep", "Glob"),
        default_tier="mid",
        file_attach=False,
        system_prompt_hint="Review code for issues. Be specific.",
        default_scope_read=("src/", "lib/", "tests/"),
        default_scope_write=(),
    ),
    "analysis": TaskProfile(
        task_type="analysis",
        allowed_tools=("Read",),
        default_tier="economy",
        file_attach=False,
        system_prompt_hint="Analyze data. Output structured results.",
        default_scope_read=("src/", "artifacts/"),
        default_scope_write=("artifacts/",),
    ),
    "creative": TaskProfile(
        task_type="creative",
        allowed_tools=(),
        default_tier="mid",
        file_attach=False,
        system_prompt_hint="Write with voice and personality.",
        default_scope_read=(),
        default_scope_write=("artifacts/",),
    ),
    "debug": TaskProfile(
        task_type="debug",
        allowed_tools=("Read", "Bash", "Grep", "Glob"),
        default_tier="mid",
        file_attach=False,
        system_prompt_hint="Find the root cause. Be systematic.",
        default_scope_read=("src/", "lib/", "tests/", "logs/"),
        default_scope_write=("src/",),
    ),
    "extraction": TaskProfile(
        task_type="extraction",
        allowed_tools=("Read",),
        default_tier="economy",
        file_attach=False,
        system_prompt_hint="Extract structured data. Output JSON.",
        default_scope_read=(),
        default_scope_write=("artifacts/",),
    ),
    "ocr": TaskProfile(
        task_type="ocr",
        allowed_tools=("Read",),
        default_tier="mid",
        file_attach=True,
        system_prompt_hint="Read and transcribe the image content.",
        default_scope_read=(),
        default_scope_write=("artifacts/",),
    ),
    "debate": TaskProfile(
        task_type="debate",
        allowed_tools=(),
        default_tier="frontier",
        file_attach=False,
        system_prompt_hint="Take a strong position. Be specific. No hedging.",
        default_scope_read=(),
        default_scope_write=(),
    ),
    "brainstorm": TaskProfile(
        task_type="brainstorm",
        allowed_tools=(),
        default_tier="mid",
        file_attach=False,
        system_prompt_hint="Generate ideas. Be creative and concrete.",
        default_scope_read=(),
        default_scope_write=("artifacts/",),
    ),
    "architecture": TaskProfile(
        task_type="architecture",
        allowed_tools=("Read", "Grep", "Glob"),
        default_tier="frontier",
        file_attach=False,
        system_prompt_hint="Design systems with clear contracts and tradeoffs.",
        default_scope_read=("src/", "docs/"),
        default_scope_write=("docs/", "artifacts/"),
    ),
    "review": TaskProfile(
        task_type="review",
        allowed_tools=("Read", "Grep", "Glob"),
        default_tier="mid",
        file_attach=False,
        system_prompt_hint="Review thoroughly. Score on dimensions, not pass/fail.",
        default_scope_read=("src/", "lib/", "tests/"),
        default_scope_write=(),
    ),
    "general": TaskProfile(
        task_type="general",
        allowed_tools=(),
        default_tier="mid",
        file_attach=False,
        system_prompt_hint="",
        default_scope_read=(),
        default_scope_write=(),
    ),
}


# ---------------------------------------------------------------------------
# Keyword routing rules — fallback seeds
# Each entry: (keywords, task_type, code_clues, creative_clues)
# code_clues/creative_clues are used when the keyword is ambiguous.
# ---------------------------------------------------------------------------

_SEED_TASK_TYPE_KEYWORDS: list[tuple[tuple[str, ...], str, tuple[str, ...], tuple[str, ...]]] = [
    (("debate", "argue", "position", "perspective", "crossfire"), "debate",          (), ()),
    (("brainstorm", "ideate", "explore", "possibilities"),        "brainstorm",      (), ()),
    (("architect", "design", "system design", "tradeoff"),        "architecture",    (), ()),
    (("debug", "diagnose", "trace", "troubleshoot"),              "debug",           (), ()),
    (("research", "discover", "search", "find", "gather"),        "research",        (), ()),
    (("review", "audit", "check", "lint", "inspect"),             "code_review",     (), ()),
    (("edit", "fix", "rename", "format", "refactor"),             "code_edit",       (), ()),
    (("build", "create", "implement", "generate"),                "code_generation", (), ()),
    (("extract", "parse", "scrape", "pull"),                      "extraction",      (), ()),
    (("score", "evaluate", "analyze", "analyse", "rank", "assess"), "analysis",      (), ()),
    (("draft", "email", "outreach", "compose", "copywrite"),      "creative",        (), ()),
    (("ocr", "image", "scan", "transcribe"),                      "ocr",             (), ()),
    # "write" is ambiguous — context clues resolve code vs. creative
    (("write",), "code_generation",
     ("function", "class", "module", "test", "script", "code"),
     ("email", "message", "outreach", "blog", "post")),
]


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
    return asyncio.run(coro)


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
    return Path(__file__).resolve().parents[3]


def _resolve_task_profile_database_url(*, required: bool) -> str | None:
    if _DATABASE_URL_ENV in os.environ:
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

        loaded_profiles = dict(_SEED_TASK_PROFILES)
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

        loaded_keywords = list(_SEED_TASK_TYPE_KEYWORDS)
        if keyword_rows:
            loaded_keywords = []
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


def seed_profile(task_type: str) -> TaskProfile:
    """Return the explicit non-authoritative seed profile for authoring defaults."""
    return _SEED_TASK_PROFILES.get(task_type, _SEED_TASK_PROFILES["general"])


def try_resolve_profile(task_type: str) -> TaskProfile | None:
    """Attempt DB-backed profile resolution without requiring authority."""
    _load_profiles_from_db(required=False)
    if _DB_TASK_PROFILES is None:
        return None
    return _DB_TASK_PROFILES.get(task_type, _DB_TASK_PROFILES["general"])


# ---------------------------------------------------------------------------
# Resolution and inference
# ---------------------------------------------------------------------------

def resolve_profile(task_type: str) -> TaskProfile:
    """Look up a task profile by type.

    Returns the profile for task_type, or the "general" profile if
    task_type is unknown.
    """
    _load_profiles_from_db(required=True)
    assert _DB_TASK_PROFILES is not None
    return _DB_TASK_PROFILES.get(task_type, _DB_TASK_PROFILES["general"])


def infer_task_type(prompt: str, *, label: str | None = None) -> str:
    """Infer task type from prompt text and optional label.

    Uses keyword matching against the task_type_keyword_rules registry.
    Falls back to "general" when no keywords match.
    """
    combined = " ".join(filter(None, [label or "", prompt])).lower()

    for keywords, task_type, code_clues, creative_clues in _SEED_TASK_TYPE_KEYWORDS:
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
