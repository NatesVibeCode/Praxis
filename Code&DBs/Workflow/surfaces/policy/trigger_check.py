"""Trigger matching against the operator-decision trigger registry.

Reads `policy/operator-decision-triggers.json` (repo-rooted, harness-neutral)
and matches a proposed agent action against the declared triggers for each
standing order. Returns the matched (decision, condition) pairs so the
caller can:

  - Inject the matching standing orders' titles + rationales into the agent's
    next-message context (additionalContext / surface-specific equivalent)
  - Emit a FrictionEvent into the friction_ledger for the audit trail
  - Optionally escalate to a typed gap or operator notification when the
    same agent has tripped the same trigger N times in a session

This module is *advisory*. It does not block. Hard blocks are navigation
puzzles for the agent, not lessons (per /praxis-debate fork round 2).
Enforcement-with-rejection lives at the data layer (Packet 4 — policy
authority) where it belongs: BEFORE INSERT/UPDATE triggers on authority tables
backed by `policy_definitions` rows that are FK-bound to operator_decisions.

The trigger registry itself lives at `policy/operator-decision-triggers.json`
at the repo root. It is hand-authored alongside operator_decisions writes
because `operator_decisions.scope_clamp` is preserved verbatim by migration
264 (`scope_clamp_preserved_verbatim`) and cannot be repurposed for
machine-readable matchers. The registry is the sidecar projection.
"""

from __future__ import annotations

import fnmatch
import json
import logging
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Iterable

logger = logging.getLogger(__name__)


def _resolve_repo_root_from_module_path() -> str:
    """Walk up from this file to find the repo root.

    This module lives at <repo>/Code&DBs/Workflow/surfaces/policy/trigger_check.py
    so the repo root is four parents up.
    """
    here = os.path.abspath(__file__)
    return os.path.normpath(os.path.join(os.path.dirname(here), "..", "..", "..", ".."))


def _registry_path() -> str:
    explicit = os.environ.get("PRAXIS_TRIGGER_REGISTRY")
    if explicit:
        return explicit
    base = (
        os.environ.get("PRAXIS_HOST_WORKSPACE_ROOT")
        or os.environ.get("CLAUDE_PROJECT_DIR")
        or _resolve_repo_root_from_module_path()
    )
    return os.path.join(base, "policy", "operator-decision-triggers.json")


@dataclass(frozen=True)
class TriggerMatch:
    """One matched (decision, condition) pair.

    `decision` is the registry entry as parsed JSON.
    `condition` is the specific match clause inside the decision's `match`
    array that fired.
    """
    decision: dict[str, Any]
    condition: dict[str, Any]

    @property
    def decision_key(self) -> str:
        return str(self.decision.get("decision_key") or "")

    @property
    def title(self) -> str:
        return str(self.decision.get("title") or "")

    @property
    def advisory_only(self) -> bool:
        if "advisory_only" in self.condition:
            return bool(self.condition.get("advisory_only"))
        return self.provenance != "explicit"

    @property
    def provenance(self) -> str:
        """Decision provenance: 'explicit' (operator unequivocally said so)
        or 'inferred' (model guessed during conversation parsing). Default
        'inferred' when the registry entry omits the field — registries
        authored before migration 302 are treated as inferred until
        promoted.
        """
        raw = str(self.decision.get("decision_provenance") or "").strip().lower()
        return "explicit" if raw == "explicit" else "inferred"

    @property
    def why(self) -> str:
        """Deeper motivation for the decision, separate from rationale.
        Operator-authored field added in migration 302; nullable. Render
        layers should surface this when present so consumers can drill
        without a separate fetch.
        """
        return str(self.decision.get("why") or self.decision.get("decision_why") or "").strip()

    def trigger_repr(self) -> str:
        return str(
            self.condition.get("regex")
            or self.condition.get("file_glob")
            or self.condition.get("string_match")
            or "(matched)"
        )


@lru_cache(maxsize=4)
def _load_registry_cached(path: str, mtime_ns: int) -> tuple[dict[str, Any], ...]:
    """Load and cache the trigger registry, keyed by (path, mtime).

    The mtime in the cache key forces a reload when operators edit the file.
    Returns a tuple of decision dicts (immutable) so the cache stays valid.
    """
    try:
        with open(path, "r", encoding="utf-8") as fp:
            registry = json.load(fp)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("trigger_check: failed to load registry at %s: %s", path, exc)
        return ()
    triggers = registry.get("triggers")
    if not isinstance(triggers, list):
        return ()
    return tuple(triggers)


def load_registry(path: str | None = None) -> tuple[dict[str, Any], ...]:
    """Public entry: load (and cache) the trigger registry."""
    resolved = path or _registry_path()
    try:
        mtime_ns = os.stat(resolved).st_mtime_ns
    except OSError:
        return ()
    return _load_registry_cached(resolved, mtime_ns)


# Per-harness tool-name aliases. Each harness names its built-in shell /
# edit / write / read tools differently. The registry uses the Claude Code
# names as the canonical key (Bash/Edit/Write/Read/MultiEdit) because that
# was the first harness wired. Other harnesses' hooks pass their native
# tool name and we normalize before matching, so the registry stays
# harness-neutral and only one set of trigger names exists.
#
# Gemini CLI:    run_shell_command, replace, write_file, read_file, MultiEdit
# Codex CLI:     local_shell, apply_patch, ... (see .codex/hooks/)
# Cursor:        no hooks today (rules-based) — N/A
_TOOL_NAME_ALIASES: dict[str, str] = {
    # Gemini CLI native names → registry canonical names.
    "run_shell_command": "Bash",
    "ShellTool": "Bash",
    "replace": "Edit",
    "write_file": "Write",
    "read_file": "Read",
    # Codex CLI native names → registry canonical names.
    "local_shell": "Bash",
    "shell": "Bash",
    "apply_patch": "Edit",
}


_TOOL_NAME_HARNESSES: dict[str, str] = {
    # Claude Code uses canonical tool names in hook payloads.
    "Bash": "claude_code",
    "Edit": "claude_code",
    "MultiEdit": "claude_code",
    "Write": "claude_code",
    "Read": "claude_code",
    # Gemini CLI native names.
    "run_shell_command": "gemini_cli",
    "ShellTool": "gemini_cli",
    "replace": "gemini_cli",
    "write_file": "gemini_cli",
    "read_file": "gemini_cli",
    # Codex CLI native names.
    "local_shell": "codex_cli",
    "shell": "codex_cli",
    "apply_patch": "codex_cli",
}


def _normalize_tool_name(tool_name: str) -> str:
    return _TOOL_NAME_ALIASES.get(tool_name, tool_name)


def _infer_harness(tool_name: str) -> str:
    return _TOOL_NAME_HARNESSES.get(tool_name, "")


def _extract_match_target(
    tool_name: str,
    tool_input: dict[str, Any],
) -> tuple[str | None, str | None, str]:
    """Return (regex_target, file_path, content_target) for matching.

    Matches the harness-side hook's logic so a Bash command, an Edit, a
    Write, or a Read is evaluated identically whether the call comes from
    a PreToolUse hook or from inside `surfaces.mcp.invocation.invoke_tool`.

    Tool name shape examples:
      - "Bash" — agent-harness Bash tool
      - "Edit" / "MultiEdit" / "Write" / "Read" — agent file tools
      - "run_shell_command" / "replace" / "write_file" / "read_file" — Gemini CLI
      - "local_shell" / "apply_patch" — Codex CLI
      - "praxis_compose_and_launch" — Praxis MCP tool name (rare match here;
        usually triggers fire on agent-harness tools that touch sensitive
        files or run shell commands. Praxis tool calls already record their
        own gateway receipts.)
    """
    raw_tool_name = tool_name
    if raw_tool_name == "apply_patch":
        patch_text = _first_text(
            tool_input,
            ("patch", "input", "content", "cmd", "command", "text"),
        )
        return patch_text or None, "\n".join(_patch_file_paths(patch_text)), patch_text

    tool_name = _normalize_tool_name(tool_name)
    if tool_name == "Bash":
        cmd = str(tool_input.get("command") or "")
        return cmd, None, cmd
    if tool_name in ("Edit", "MultiEdit"):
        path = str(tool_input.get("file_path") or "")
        chunks: list[str] = [
            str(tool_input.get("old_string") or ""),
            str(tool_input.get("new_string") or ""),
        ]
        for edit in tool_input.get("edits") or []:
            chunks.append(str(edit.get("old_string") or ""))
            chunks.append(str(edit.get("new_string") or ""))
        return None, path, "\n".join(chunks)
    if tool_name == "Write":
        path = str(tool_input.get("file_path") or "")
        content = str(tool_input.get("content") or "")
        return None, path, content
    if tool_name == "Read":
        path = str(tool_input.get("file_path") or "")
        return None, path, ""
    # Catch-all for Praxis MCP tool dispatch and other surfaces. The tool
    # input is treated as the regex target (for tool-name regex triggers)
    # and as the content target (for string_match against payload JSON).
    try:
        content = json.dumps(tool_input, default=str)
    except Exception:
        content = ""
    return tool_name, None, content


def _first_text(tool_input: dict[str, Any], keys: Iterable[str]) -> str:
    for key in keys:
        value = tool_input.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _patch_file_paths(patch_text: str) -> tuple[str, ...]:
    paths: list[str] = []
    for line in patch_text.splitlines():
        match = re.match(r"^\*\*\* (?:Add|Update|Delete) File: (.+)$", line)
        if not match:
            continue
        path = match.group(1).strip()
        if path and path not in paths:
            paths.append(path)
    return tuple(paths)


def _match_glob(pattern: str, file_path: str) -> bool:
    """Glob match supporting **/ — fnmatch doesn't natively understand **."""
    if not file_path:
        return False
    if "\n" in file_path:
        return any(_match_glob(pattern, part) for part in file_path.splitlines())
    if fnmatch.fnmatch(file_path, pattern):
        return True
    collapsed = pattern.replace("**/", "*").replace("/**", "*")
    if fnmatch.fnmatch(file_path, collapsed):
        return True
    base = os.path.basename(file_path)
    if fnmatch.fnmatch(base, pattern) or fnmatch.fnmatch(base, collapsed):
        return True
    stripped = pattern.lstrip("*").lstrip("/")
    if stripped and len(stripped) >= 4 and stripped in file_path:
        return True
    return False


def _match_one(
    condition: dict[str, Any],
    tool_name: str,
    harness: str,
    regex_target: str | None,
    file_path: str | None,
    content_target: str,
) -> bool:
    cond_harness = str(condition.get("harness") or "").strip()
    if cond_harness and cond_harness != harness:
        return False

    cond_tool = str(condition.get("tool") or "").strip()
    if cond_tool and cond_tool != tool_name:
        return False

    rgx = condition.get("regex")
    if rgx:
        if regex_target is None:
            return False
        try:
            if not re.search(rgx, regex_target):
                return False
        except re.error:
            logger.warning("trigger_check: bad regex in registry: %r", rgx)
            return False

    glob = condition.get("file_glob")
    if glob and not _match_glob(glob, file_path or ""):
        return False

    string_match = condition.get("string_match")
    if string_match:
        if not content_target:
            return False
        try:
            if not re.search(string_match, content_target):
                return False
        except re.error:
            logger.warning("trigger_check: bad string_match regex: %r", string_match)
            return False

    return True


# Per-process session-scope cooldown for advisory triggers. Once an
# (decision_key, file_path) pair fires inside the current process, suppress
# subsequent advisory matches for the same pair — the agent has already
# seen the surface; firing again on the very next edit is noise. EXPLICIT
# triggers (advisory_only=False) always fire because they're operator-
# binding signals, not advisory ones.
#
# Process scope = session scope: each fresh PreToolUse hook spawn is its
# own subprocess, so this cache only suppresses within a single MCP-tier
# invocation chain. For the per-harness hook (Bash/Edit/Write) lane, the
# hook is a fresh subprocess each time and won't dedupe — we accept that
# cost in v1; v2 ships a marker file under PRAXIS_SESSION_COOLDOWN_DIR
# (env-overridable) so per-harness invocations dedupe across the session.
# See BUG-3E9820C4.
_ADVISORY_FIRED: set[tuple[str, str]] = set()


def _cooldown_marker_dir() -> str | None:
    """Directory the per-harness hook can write fired-pair markers to so
    consecutive subprocess invocations dedupe. Operator opts in via env.
    Returns None when not configured — cooldown stays in-process only."""
    return os.environ.get("PRAXIS_SESSION_COOLDOWN_DIR")


def _cooldown_key(decision_key: str, file_path: str | None, regex_target: str | None) -> str:
    # The marker key needs to be stable across consecutive edits to the
    # same target. For Edit/Write, the file_path is the natural key. For
    # Bash, multiple commands in a session that match the same trigger
    # are usually the same intent (e.g. running docker restart twice in
    # a row), so the regex_target works. Falling back to bare key still
    # keeps purely-semantic triggers from re-firing.
    target = (file_path or regex_target or "").strip()
    return f"{decision_key}::{target}"


def _cooldown_seen(key: str) -> bool:
    if key in _ADVISORY_FIRED:
        return True
    marker_dir = _cooldown_marker_dir()
    if marker_dir:
        try:
            marker_path = os.path.join(marker_dir, key.replace("/", "_").replace(":", "_"))
            if os.path.exists(marker_path):
                _ADVISORY_FIRED.add(key)  # populate in-process cache too
                return True
        except OSError:
            pass
    return False


def _cooldown_record(key: str) -> None:
    _ADVISORY_FIRED.add(key)
    marker_dir = _cooldown_marker_dir()
    if marker_dir:
        try:
            os.makedirs(marker_dir, exist_ok=True)
            marker_path = os.path.join(marker_dir, key.replace("/", "_").replace(":", "_"))
            with open(marker_path, "w") as fp:
                fp.write("fired")
        except OSError:
            pass


def check(
    tool_name: str,
    tool_input: dict[str, Any] | None,
    *,
    registry: Iterable[dict[str, Any]] | None = None,
) -> list[TriggerMatch]:
    """Match a proposed action against the trigger registry.

    Returns a list of TriggerMatch objects, one per matching (decision,
    condition) pair. Empty list = no matches. Always non-throwing — caller
    may receive an empty list if the registry is missing/corrupt.

    Advisory triggers (advisory_only=True) deduplicate on (decision_key,
    file_path-or-regex-target) per-session via the cooldown helpers above
    so consecutive edits to the same file don't surface the same advisory
    repeatedly. Explicit triggers (advisory_only=False) always fire —
    operator-binding signals are never silenced.
    """
    if not tool_name:
        return []
    raw_tool_name = tool_name
    tool_name = _normalize_tool_name(tool_name)
    harness = _infer_harness(raw_tool_name)
    decisions = registry if registry is not None else load_registry()
    if not decisions:
        return []
    if tool_input is None:
        tool_input = {}
    if not isinstance(tool_input, dict):
        return []

    regex_target, file_path, content_target = _extract_match_target(raw_tool_name, tool_input)

    matches: list[TriggerMatch] = []
    for decision in decisions:
        for condition in decision.get("match") or []:
            if _match_one(condition, tool_name, harness, regex_target, file_path, content_target):
                # Cooldown: skip advisory matches we've already surfaced
                # in this session for the same (decision_key, target) pair.
                # Explicit (non-advisory) matches always fire. Use
                # TriggerMatch.advisory_only so old registry rows without a
                # provenance flag default to advisory instead of noisy.
                match = TriggerMatch(decision=decision, condition=condition)
                decision_key = str(decision.get("decision_key") or "")
                if match.advisory_only and decision_key:
                    cd_key = _cooldown_key(decision_key, file_path, regex_target)
                    if _cooldown_seen(cd_key):
                        # Already surfaced this session — skip the duplicate.
                        break
                    _cooldown_record(cd_key)
                matches.append(match)
                break  # one match per decision is enough
    return matches


def render_additional_context(matches: list[TriggerMatch], tool_name: str) -> str:
    """Render matches as a concise context string for harness injection.

    Verbosity costs tokens AND dilutes signal. Title + decision_key + a
    one-line trigger summary is enough; full rationale lives in
    operator_decisions and is reachable via praxis_orient.
    """
    if not matches:
        return ""
    lines = ["⚠ STANDING ORDER MATCH — pause and consider:"]
    for match in matches:
        # Provenance + advisory badges drive how seriously the agent should
        # weight the surface. Explicit decisions are operator-authored and
        # binding. Inferred decisions are model-derived and advisory by
        # default — they should be checked against the operator before
        # being treated as load-bearing. The rationale field lives in
        # operator_decisions and is fetched-on-demand to keep this surface
        # cheap on tokens.
        badges: list[str] = []
        if match.provenance == "explicit":
            badges.append("EXPLICIT")
        else:
            badges.append("inferred")
        if match.advisory_only:
            badges.append("advisory")
        badge_str = f" [{' / '.join(badges)}]" if badges else ""
        lines.append("")
        lines.append(f"• {match.title}{badge_str}")
        lines.append(f"  decision_key: {match.decision_key}")
        why = match.why or str(match.decision.get("$note") or "").strip()
        if why:
            lines.append(f"  why: {why.split(chr(10))[0][:240]}")
        lines.append(f"  triggered by: {match.trigger_repr()}")
    lines.append("")
    lines.append(
        f"Action proposed: {tool_name} (continuing). EXPLICIT matches are "
        "operator-binding — pivot if you're about to violate. Inferred "
        "matches are advisory; check before treating as load-bearing. "
        "Drill: praxis_operator_decisions(action='get', decision_key='<key>') "
        "for full rationale + why + scope_clamp."
    )
    return "\n".join(lines)


__all__ = [
    "TriggerMatch",
    "check",
    "load_registry",
    "render_additional_context",
]
