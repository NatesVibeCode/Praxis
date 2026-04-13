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
from typing import Any

_DATABASE_URL_ENV = "WORKFLOW_DATABASE_URL"


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

TASK_PROFILES: dict[str, TaskProfile] = {
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

_TASK_TYPE_KEYWORDS: list[tuple[tuple[str, ...], str, tuple[str, ...], tuple[str, ...]]] = [
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


def _run_async(coro: object) -> object:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is not None and loop.is_running():
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result(timeout=10)
    return asyncio.run(coro)


def _load_profiles_from_db() -> None:
    global _PROFILES_DB_LOADED
    if _PROFILES_DB_LOADED:
        return

    try:
        import asyncpg

        db_url = os.environ.get(_DATABASE_URL_ENV, "").strip()
        if not db_url.startswith(("postgresql://", "postgres://")):
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

        if profile_rows:
            TASK_PROFILES.clear()
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
                TASK_PROFILES[task_type] = TaskProfile(
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

        if keyword_rows:
            _TASK_TYPE_KEYWORDS.clear()
            for row in keyword_rows:
                kws = tuple(str(k) for k in (row["keywords"] or []))
                if not kws:
                    continue
                code_clues = tuple(str(c) for c in (row["context_code_clues"] or []))
                creative_clues = tuple(str(c) for c in (row["context_creative_clues"] or []))
                _TASK_TYPE_KEYWORDS.append((kws, str(row["task_type"]), code_clues, creative_clues))

    except Exception:
        pass
    finally:
        _PROFILES_DB_LOADED = True


def reload_profiles_from_db() -> None:
    """Force a fresh DB read on the next lookup."""
    global _PROFILES_DB_LOADED
    _PROFILES_DB_LOADED = False
    _load_profiles_from_db()


# ---------------------------------------------------------------------------
# Resolution and inference
# ---------------------------------------------------------------------------

def resolve_profile(task_type: str) -> TaskProfile:
    """Look up a task profile by type.

    Returns the profile for task_type, or the "general" profile if
    task_type is unknown.
    """
    _load_profiles_from_db()
    return TASK_PROFILES.get(task_type, TASK_PROFILES["general"])


def infer_task_type(prompt: str, *, label: str | None = None) -> str:
    """Infer task type from prompt text and optional label.

    Uses keyword matching against the task_type_keyword_rules registry.
    Falls back to "general" when no keywords match.
    """
    _load_profiles_from_db()
    combined = " ".join(filter(None, [label or "", prompt])).lower()

    for keywords, task_type, code_clues, creative_clues in _TASK_TYPE_KEYWORDS:
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
