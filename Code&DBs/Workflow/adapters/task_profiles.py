"""Task type → tool profile mapping for configurable LLM adapter behavior.

Profiles and keyword routing rules are authoritative in Postgres
(task_type_profiles, task_type_keyword_rules). The in-module dicts are
cold-start fallbacks used only when the DB is unavailable.

Each TaskProfile specifies:
  - allowed_tools: which tools the model can use
  - default_tier: routing tier when tier is not explicitly set
  - file_attach: whether to attach image/file resources
  - system_prompt_hint: task-specific instruction appended to system prompt
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import os
from dataclasses import dataclass

_DATABASE_URL_ENV = "WORKFLOW_DATABASE_URL"


@dataclass(frozen=True)
class TaskProfile:
    """Configuration profile for a task type."""

    task_type: str
    allowed_tools: tuple[str, ...]
    default_tier: str  # "frontier", "mid", "economy"
    file_attach: bool
    system_prompt_hint: str


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
    ),
    "code_generation": TaskProfile(
        task_type="code_generation",
        allowed_tools=("Read", "Edit", "Write", "Bash"),
        default_tier="mid",
        file_attach=False,
        system_prompt_hint="Write clean, tested code.",
    ),
    "code_edit": TaskProfile(
        task_type="code_edit",
        allowed_tools=("Read", "Edit", "Bash"),
        default_tier="mid",
        file_attach=False,
        system_prompt_hint="Make targeted edits only.",
    ),
    "code_review": TaskProfile(
        task_type="code_review",
        allowed_tools=("Read", "Grep", "Glob"),
        default_tier="mid",
        file_attach=False,
        system_prompt_hint="Review code for issues. Be specific.",
    ),
    "analysis": TaskProfile(
        task_type="analysis",
        allowed_tools=("Read",),
        default_tier="economy",
        file_attach=False,
        system_prompt_hint="Analyze data. Output structured results.",
    ),
    "creative": TaskProfile(
        task_type="creative",
        allowed_tools=(),
        default_tier="mid",
        file_attach=False,
        system_prompt_hint="Write with voice and personality.",
    ),
    "debug": TaskProfile(
        task_type="debug",
        allowed_tools=("Read", "Bash", "Grep", "Glob"),
        default_tier="mid",
        file_attach=False,
        system_prompt_hint="Find the root cause. Be systematic.",
    ),
    "extraction": TaskProfile(
        task_type="extraction",
        allowed_tools=("Read",),
        default_tier="economy",
        file_attach=False,
        system_prompt_hint="Extract structured data. Output JSON.",
    ),
    "ocr": TaskProfile(
        task_type="ocr",
        allowed_tools=("Read",),
        default_tier="mid",
        file_attach=True,
        system_prompt_hint="Read and transcribe the image content.",
    ),
    "debate": TaskProfile(
        task_type="debate",
        allowed_tools=(),
        default_tier="frontier",
        file_attach=False,
        system_prompt_hint="Take a strong position. Be specific. No hedging.",
    ),
    "brainstorm": TaskProfile(
        task_type="brainstorm",
        allowed_tools=(),
        default_tier="mid",
        file_attach=False,
        system_prompt_hint="Generate ideas. Be creative and concrete.",
    ),
    "architecture": TaskProfile(
        task_type="architecture",
        allowed_tools=("Read", "Grep", "Glob"),
        default_tier="frontier",
        file_attach=False,
        system_prompt_hint="Design systems with clear contracts and tradeoffs.",
    ),
    "review": TaskProfile(
        task_type="review",
        allowed_tools=("Read", "Grep", "Glob"),
        default_tier="mid",
        file_attach=False,
        system_prompt_hint="Review thoroughly. Score on dimensions, not pass/fail.",
    ),
    "general": TaskProfile(
        task_type="general",
        allowed_tools=(),
        default_tier="mid",
        file_attach=False,
        system_prompt_hint="",
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
                    "SELECT task_type, allowed_tools, default_tier, file_attach, system_prompt_hint "
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
                TASK_PROFILES[task_type] = TaskProfile(
                    task_type=task_type,
                    allowed_tools=tuple(str(t) for t in (tools or [])),
                    default_tier=str(row["default_tier"]),
                    file_attach=bool(row["file_attach"]),
                    system_prompt_hint=str(row["system_prompt_hint"] or ""),
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
