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
class _ContextEntry:
    """One completed node's context record."""

    node_id: str
    role: str  # display_name / task_name
    summary: str  # "succeeded" / "failed"
    output_preview: str


@dataclass(frozen=True, slots=True)
class AccumulatedContext:
    """Immutable snapshot of accumulated pipeline context."""

    entries: tuple[_ContextEntry, ...]
    total_tokens_est: int


def _compress_entry(entry: _ContextEntry) -> _ContextEntry:
    """Compress an entry to a single-line summary (status + first line of output).

    Configured via PRAXIS_CONTEXT_COMPRESS_CHARS environment variable (default: 120).
    """
    first_line = ""
    if entry.output_preview:
        first_line = entry.output_preview.split("\n", 1)[0].strip()
        if len(first_line) > _COMPRESS_CHARS:
            first_line = first_line[:_COMPRESS_CHARS] + "..."
    compressed_preview = f"{first_line}" if first_line else "(no output)"
    return _ContextEntry(
        node_id=entry.node_id,
        role=entry.role,
        summary=entry.summary,
        output_preview=compressed_preview,
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

    def __init__(self, *, max_context_tokens: int | None = None) -> None:
        self._entries: list[_ContextEntry] = []
        self._max_context_tokens = max_context_tokens if max_context_tokens is not None else _DEFAULT_CONTEXT_TOKENS

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
            max_preview_chars = _CONTEXT_PREVIEW_CHARS
        preview = _extract_preview(outputs, max_chars=max_preview_chars)
        entry = _ContextEntry(
            node_id=node_id,
            role=node_name,
            summary=status,
            output_preview=preview,
        )
        self._entries.append(entry)
        self._enforce_budget()

    def _enforce_budget(self) -> None:
        """Compress older entries if total tokens exceed the budget.

        Strategy: keep the most recent N entries (PRAXIS_CONTEXT_KEEP_RECENT, default 2)
        at full length, compress everything older to single-line summaries.
        """
        if self._max_context_tokens <= 0:
            return
        if self.token_estimate() <= self._max_context_tokens:
            return

        # Number of entries to keep at full fidelity (the tail)
        keep_full = _KEEP_RECENT_FULL

        # Compress entries from oldest forward until we're under budget
        # or we've compressed everything except the last `keep_full`.
        compress_limit = max(0, len(self._entries) - keep_full)
        for i in range(compress_limit):
            if self._entries[i] != _compress_entry(self._entries[i]):
                self._entries[i] = _compress_entry(self._entries[i])
                # Re-check after each compression
                if self.token_estimate() <= self._max_context_tokens:
                    return

    def is_over_budget(self) -> bool:
        """Return True if the current token estimate exceeds the budget."""
        return self.token_estimate() > self._max_context_tokens

    def budget_remaining(self) -> int:
        """Return how many tokens remain before hitting the budget.

        Returns 0 (not negative) when already over budget.
        """
        return max(0, self._max_context_tokens - self.token_estimate())

    def render_context_section(self) -> dict[str, str]:
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
        return {"name": "prior_results", "content": content}

    def token_estimate(self) -> int:
        """Estimate total tokens across all accumulated entries."""
        section = self.render_context_section()
        return _estimate_tokens(section.get("content", ""))

    def snapshot(self) -> AccumulatedContext:
        """Return an immutable snapshot of current state."""
        return AccumulatedContext(
            entries=tuple(self._entries),
            total_tokens_est=self.token_estimate(),
        )

    def __len__(self) -> int:
        return len(self._entries)

    def __bool__(self) -> bool:
        return len(self._entries) > 0


__all__ = [
    "AccumulatedContext",
    "ContextAccumulator",
]
