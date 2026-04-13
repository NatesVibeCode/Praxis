"""Prompt renderer for dispatch.

Compiles a WorkflowSpec (with optional system prompt and context sections)
into structured messages suitable for LLM adapters.  This bridges the gap
between raw prompt strings and the compiled prompt format that real dispatch
needs — system instructions, context, scope constraints, and task description
assembled from the context bundle and routing decision.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .workflow import WorkflowSpec


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_CHARS_PER_TOKEN = float(os.environ.get("PRAXIS_CHARS_PER_TOKEN", "4.0"))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _estimate_tokens(text: str) -> int:
    """Rough token estimate based on configurable chars per token ratio.

    Configured via PRAXIS_CHARS_PER_TOKEN environment variable (default: 4.0).
    """
    return max(1, int(len(text) / _CHARS_PER_TOKEN)) if text else 0


# ---------------------------------------------------------------------------
# Rendered prompt
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class RenderedPrompt:
    """Immutable compiled prompt ready for adapter consumption."""

    system_message: str
    user_message: str
    context_sections: tuple[dict[str, str], ...]
    total_tokens_est: int
    rendered_at: datetime


# ---------------------------------------------------------------------------
# Renderer
# ---------------------------------------------------------------------------

def render_prompt(
    spec: WorkflowSpec,
    *,
    context_sections: list[dict[str, str]] | None = None,
) -> RenderedPrompt:
    """Compile a WorkflowSpec into a RenderedPrompt.

    Parameters
    ----------
    spec:
        The workflow spec containing prompt, provider/model info, and
        optional system_prompt / context_sections.
    context_sections:
        Override context sections.  When *None*, falls back to
        ``spec.context_sections``.
    """

    # --- system message ---------------------------------------------------
    system_parts: list[str] = []

    system_parts.append(
        f"Provider: {spec.provider_slug} | "
        f"Adapter: {spec.adapter_type}"
        + (f" | Model: {spec.model_slug}" if spec.model_slug else "")
    )

    if spec.system_prompt:
        system_parts.append(spec.system_prompt)

    system_message = "\n\n".join(system_parts)

    # --- resolve context sections -----------------------------------------
    sections = context_sections if context_sections is not None else spec.context_sections
    frozen_sections: tuple[dict[str, str], ...] = ()
    if sections:
        frozen_sections = tuple(
            {"name": s["name"], "content": s["content"]} for s in sections
        )

    # --- user message -----------------------------------------------------
    user_parts: list[str] = [spec.prompt]

    for section in frozen_sections:
        user_parts.append(f"\n\n--- {section['name']} ---\n{section['content']}")

    user_message = "".join(user_parts)

    # --- token estimate ---------------------------------------------------
    total_tokens_est = _estimate_tokens(system_message) + _estimate_tokens(user_message)

    return RenderedPrompt(
        system_message=system_message,
        user_message=user_message,
        context_sections=frozen_sections,
        total_tokens_est=total_tokens_est,
        rendered_at=_utc_now(),
    )


def render_prompt_as_messages(
    rendered: RenderedPrompt,
) -> tuple[dict[str, str], ...]:
    """Convert a RenderedPrompt into the messages format adapters expect.

    Returns a tuple of ``{"role": ..., "content": ...}`` dicts.
    """

    messages: list[dict[str, str]] = []

    if rendered.system_message:
        messages.append({"role": "system", "content": rendered.system_message})

    messages.append({"role": "user", "content": rendered.user_message})

    return tuple(messages)
