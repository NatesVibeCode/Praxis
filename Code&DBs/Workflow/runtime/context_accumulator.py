"""Context accumulation across workflow pipeline nodes.

Collects output summaries from completed nodes and renders them as
a context section that subsequent nodes can consume.  This gives
downstream steps visibility into what prior steps produced —
e.g. node_2 sees "Prior Step Results" from node_0 and node_1.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_CHARS_PER_TOKEN = float(os.environ.get("PRAXIS_CHARS_PER_TOKEN", "4.0"))
_DEFAULT_CONTEXT_TOKENS = int(os.environ.get("PRAXIS_DEFAULT_CONTEXT_TOKENS", "8000"))
_CONTEXT_PREVIEW_CHARS = int(os.environ.get("PRAXIS_CONTEXT_PREVIEW_CHARS", "2000"))
_KEEP_RECENT_FULL = int(os.environ.get("PRAXIS_CONTEXT_KEEP_RECENT", "2"))
_COMPRESS_CHARS = int(os.environ.get("PRAXIS_CONTEXT_COMPRESS_CHARS", "120"))


def _estimate_tokens(text: str) -> int:
    """Rough token estimate based on configurable chars per token ratio.

    Configured via PRAXIS_CHARS_PER_TOKEN environment variable (default: 4.0).
    """
    return max(1, int(len(text) / _CHARS_PER_TOKEN)) if text else 0


def _extract_preview(outputs: dict[str, Any], *, max_chars: int) -> str:
    """Extract the most useful preview string from a node's outputs.

    Prefers the ``completion`` key (LLM text output), falls back to a
    compact repr of the full outputs dict, truncated to *max_chars*.
    """
    completion = outputs.get("completion")
    if isinstance(completion, str) and completion.strip():
        text = completion.strip()
    else:
        # Compact representation of outputs for non-LLM nodes
        pairs: list[str] = []
        for key, value in sorted(outputs.items()):
            val_str = str(value)
            if len(val_str) > 200:
                val_str = val_str[:200] + "..."
            pairs.append(f"{key}: {val_str}")
        text = "\n".join(pairs) if pairs else "(no outputs)"

    if len(text) > max_chars:
        text = text[:max_chars] + "\n... [truncated]"
    return text


@dataclass(frozen=True, slots=True)
class ContextWindowPolicy:
    """Explicit policy for accumulated-context compression and eviction."""

    max_context_tokens: int
    keep_recent_full: int
    preview_chars: int
    compressed_chars: int


@dataclass(frozen=True, slots=True)
class ContextWindowDecision:
    """Observable result of enforcing a context-window policy."""

    max_context_tokens: int
    total_tokens_est: int
    compressed_entry_ids: tuple[str, ...]
    evicted_entry_ids: tuple[str, ...]
    retained_entry_ids: tuple[str, ...]
    budget_remaining: int
    is_over_budget: bool


@dataclass(frozen=True, slots=True)
class _ContextEntry:
    """One completed node's context record."""

    node_id: str
    role: str  # display_name / task_name
    summary: str  # "succeeded" / "failed"
    output_preview: str
    semantic_summary: str
    state: str = "full"


@dataclass(frozen=True, slots=True)
class AccumulatedContext:
    """Immutable snapshot of accumulated pipeline context."""

    entries: tuple[_ContextEntry, ...]
    total_tokens_est: int
    window_decision: ContextWindowDecision


def _semantic_summary(outputs: dict[str, Any], *, fallback: str) -> str:
    """Extract the best deterministic semantic summary from node outputs."""
    for key in ("semantic_summary", "summary", "result_summary"):
        value = outputs.get(key)
        if isinstance(value, str) and value.strip():
            return " ".join(value.split())

    parsed = outputs.get("parsed_output")
    if isinstance(parsed, dict):
        for key in ("semantic_summary", "summary", "result_summary", "rationale"):
            value = parsed.get(key)
            if isinstance(value, str) and value.strip():
                return " ".join(value.split())
        decisions = parsed.get("decisions")
        if isinstance(decisions, list) and decisions:
            joined = "; ".join(str(item).strip() for item in decisions[:3] if item)
            if joined:
                return f"Decisions: {joined}"

    first_line = fallback.split("\n", 1)[0].strip() if fallback else ""
    return first_line or "(no output)"


def _compress_entry(entry: _ContextEntry, *, max_chars: int) -> _ContextEntry:
    """Compress an entry to a deterministic semantic summary.

    Configured via PRAXIS_CONTEXT_COMPRESS_CHARS environment variable (default: 120).
    """
    compressed_preview = entry.semantic_summary or entry.output_preview
    if len(compressed_preview) > max_chars:
        compressed_preview = compressed_preview[:max_chars] + "..."
    return _ContextEntry(
        node_id=entry.node_id,
        role=entry.role,
        summary=entry.summary,
        output_preview=compressed_preview,
        semantic_summary=entry.semantic_summary,
        state="compressed",
    )


class ContextAccumulator:
    """Collects results from completed pipeline nodes and renders
    them as a context section for injection into subsequent nodes.

    When *max_context_tokens* is set (configured via PRAXIS_DEFAULT_CONTEXT_TOKENS,
    default 8000), older entries are automatically compressed to single-line
    summaries so the total rendered context stays within budget. The most recent
    N entries (configured via PRAXIS_CONTEXT_KEEP_RECENT, default 2) are always kept
    at full length.
    """

    def __init__(
        self,
        *,
        max_context_tokens: int | None = None,
        keep_recent_full: int | None = None,
        max_preview_chars: int | None = None,
        max_compressed_chars: int | None = None,
    ) -> None:
        self._entries: list[_ContextEntry] = []
        self._evicted_entry_ids: list[str] = []
        self._policy = ContextWindowPolicy(
            max_context_tokens=(
                max_context_tokens
                if max_context_tokens is not None
                else _DEFAULT_CONTEXT_TOKENS
            ),
            keep_recent_full=(
                keep_recent_full
                if keep_recent_full is not None
                else _KEEP_RECENT_FULL
            ),
            preview_chars=(
                max_preview_chars
                if max_preview_chars is not None
                else _CONTEXT_PREVIEW_CHARS
            ),
            compressed_chars=(
                max_compressed_chars
                if max_compressed_chars is not None
                else _COMPRESS_CHARS
            ),
        )

    def add_node_result(
        self,
        node_id: str,
        node_name: str,
        status: str,
        outputs: dict[str, Any],
        *,
        max_preview_chars: int | None = None,
    ) -> None:
        """Record one completed node's result for downstream context.

        If the new entry would push total tokens over the budget, older
        entries (all except the most recent N, configured via PRAXIS_CONTEXT_KEEP_RECENT)
        are compressed to single-line summaries.

        Parameters
        ----------
        max_preview_chars:
            Maximum chars for preview extraction. If None, uses PRAXIS_CONTEXT_PREVIEW_CHARS
            environment variable (default 2000).
        """
        if max_preview_chars is None:
            max_preview_chars = self._policy.preview_chars
        preview = _extract_preview(outputs, max_chars=max_preview_chars)
        entry = _ContextEntry(
            node_id=node_id,
            role=node_name,
            summary=status,
            output_preview=preview,
            semantic_summary=_semantic_summary(outputs, fallback=preview),
        )
        self._entries.append(entry)
        self._enforce_budget()

    def _enforce_budget(self) -> None:
        """Compress then evict older entries if total tokens exceed the budget.

        Strategy: keep the most recent N entries (PRAXIS_CONTEXT_KEEP_RECENT, default 2)
        at full length, compress everything older to semantic summaries, then evict
        oldest compressed entries if the compressed set still exceeds the budget.
        """
        if self._policy.max_context_tokens <= 0:
            return
        if self.token_estimate() <= self._policy.max_context_tokens:
            return

        # Number of entries to keep at full fidelity (the tail)
        keep_full = max(0, self._policy.keep_recent_full)

        # Compress entries from oldest forward until we're under budget
        # or we've compressed everything except the last `keep_full`.
        compress_limit = max(0, len(self._entries) - keep_full)
        for i in range(compress_limit):
            compressed = _compress_entry(
                self._entries[i],
                max_chars=self._policy.compressed_chars,
            )
            if self._entries[i] != compressed:
                self._entries[i] = compressed
                # Re-check after each compression
                if self.token_estimate() <= self._policy.max_context_tokens:
                    return

        if self.token_estimate() <= self._policy.max_context_tokens:
            return

        # Evict oldest compressed entries first. Recent full-fidelity entries
        # remain protected, even if their own size keeps the packet over budget.
        next_entries: list[_ContextEntry] = []
        protected_tail_start = max(0, len(self._entries) - keep_full)
        for index, entry in enumerate(self._entries):
            is_protected_recent = index >= protected_tail_start
            candidate_entries = next_entries + [entry] + self._entries[index + 1 :]
            if (
                not is_protected_recent
                and entry.state == "compressed"
                and self._token_estimate_for(candidate_entries)
                > self._policy.max_context_tokens
            ):
                self._evicted_entry_ids.append(entry.node_id)
                continue
            next_entries.append(entry)
        self._entries = next_entries

    def is_over_budget(self) -> bool:
        """Return True if the current token estimate exceeds the budget."""
        return self.token_estimate() > self._policy.max_context_tokens

    def budget_remaining(self) -> int:
        """Return how many tokens remain before hitting the budget.

        Returns 0 (not negative) when already over budget.
        """
        return max(0, self._policy.max_context_tokens - self.token_estimate())

    def render_context_section(self) -> dict[str, Any]:
        """Return a context section dict suitable for injection into
        a node's ``input_payload["context_sections"]`` list.

        Format::

            {"name": "prior_results", "content": "## Prior Step Results\\n\\n..."}
        """
        if not self._entries:
            return {"name": "prior_results", "content": ""}

        parts: list[str] = ["## Prior Step Results"]
        for entry in self._entries:
            parts.append(f"\n### Step: {entry.role} ({entry.summary})")
            parts.append(entry.output_preview)

        content = "\n".join(parts)
        decision = self.window_decision()
        return {
            "name": "prior_results",
            "content": content,
            "metadata": {
                "context_window": {
                    "max_context_tokens": decision.max_context_tokens,
                    "token_estimate": decision.total_tokens_est,
                    "compressed_entry_ids": list(decision.compressed_entry_ids),
                    "evicted_entry_ids": list(decision.evicted_entry_ids),
                    "budget_remaining": decision.budget_remaining,
                    "is_over_budget": decision.is_over_budget,
                }
            },
        }

    def token_estimate(self) -> int:
        """Estimate total tokens across all accumulated entries."""
        return self._token_estimate_for(self._entries)

    def _token_estimate_for(self, entries: list[_ContextEntry]) -> int:
        if not entries:
            return 0
        parts: list[str] = ["## Prior Step Results"]
        for entry in entries:
            parts.append(f"\n### Step: {entry.role} ({entry.summary})")
            parts.append(entry.output_preview)
        return _estimate_tokens("\n".join(parts))

    def window_decision(self) -> ContextWindowDecision:
        """Return the latest observable context-window enforcement decision."""
        total_tokens = self.token_estimate()
        return ContextWindowDecision(
            max_context_tokens=self._policy.max_context_tokens,
            total_tokens_est=total_tokens,
            compressed_entry_ids=tuple(
                entry.node_id for entry in self._entries if entry.state == "compressed"
            ),
            evicted_entry_ids=tuple(dict.fromkeys(self._evicted_entry_ids)),
            retained_entry_ids=tuple(entry.node_id for entry in self._entries),
            budget_remaining=max(0, self._policy.max_context_tokens - total_tokens),
            is_over_budget=total_tokens > self._policy.max_context_tokens,
        )

    def snapshot(self) -> AccumulatedContext:
        """Return an immutable snapshot of current state."""
        return AccumulatedContext(
            entries=tuple(self._entries),
            total_tokens_est=self.token_estimate(),
            window_decision=self.window_decision(),
        )

    def __len__(self) -> int:
        return len(self._entries)

    def __bool__(self) -> bool:
        return len(self._entries) > 0


__all__ = [
    "AccumulatedContext",
    "ContextAccumulator",
    "ContextWindowDecision",
    "ContextWindowPolicy",
]
