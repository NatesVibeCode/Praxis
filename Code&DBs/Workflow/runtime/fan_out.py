"""Fan-out / map primitive for dynamic parallel dispatch.

Takes a list of items (or extracts one from an upstream completion)
and dispatches independent specs in parallel — one per item.  This is
the "for each lead in the list, do X" pattern.

The fan-out is dispatch-level: it creates multiple independent
dispatches via ``run_workflow_parallel()``.  It does not modify the
workflow graph at runtime.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from .workflow import WorkflowSpec, WorkflowResult, run_workflow_parallel


# ---------------------------------------------------------------------------
# Item serialisation
# ---------------------------------------------------------------------------

def _serialize_item(item: Any) -> str:
    """Render an item for prompt injection.

    Dicts and lists are JSON-encoded; everything else is ``str()``.
    """
    if isinstance(item, (dict, list)):
        return json.dumps(item, ensure_ascii=False)
    return str(item)


# ---------------------------------------------------------------------------
# List extraction from upstream completions
# ---------------------------------------------------------------------------

_JSON_ARRAY_RE = re.compile(r"\[.*\]", re.DOTALL)


def extract_items_from_completion(text: str) -> list[Any]:
    """Best-effort extraction of a list from an LLM completion.

    Strategy (in order):
    1. If the entire text is a JSON array, use it.
    2. If the text contains a fenced code block with a JSON array, use
       the first one found.
    3. If a JSON array appears anywhere in the text, use the first one.
    4. Fall back to non-empty stripped lines.
    """
    stripped = text.strip()

    # 1. Whole text is a JSON array
    try:
        parsed = json.loads(stripped)
        if isinstance(parsed, list):
            return parsed
    except (json.JSONDecodeError, ValueError):
        pass

    # 2. Fenced code block
    fenced = re.findall(r"```(?:json)?\s*\n(.*?)```", text, re.DOTALL)
    for block in fenced:
        try:
            parsed = json.loads(block.strip())
            if isinstance(parsed, list):
                return parsed
        except (json.JSONDecodeError, ValueError):
            continue

    # 3. Embedded JSON array
    match = _JSON_ARRAY_RE.search(text)
    if match:
        try:
            parsed = json.loads(match.group(0))
            if isinstance(parsed, list):
                return parsed
        except (json.JSONDecodeError, ValueError):
            pass

    # 4. Line-separated fallback
    lines = [line.strip() for line in stripped.splitlines() if line.strip()]
    return lines


# ---------------------------------------------------------------------------
# Core fan-out
# ---------------------------------------------------------------------------

def fan_out_dispatch(
    items: list[Any],
    *,
    prompt_template: str,
    tier: str = "mid",
    max_parallel: int | None = None,
    label_prefix: str = "fan",
    **spec_kwargs: Any,
) -> list[WorkflowResult]:
    """Dispatch one spec per item, all in parallel.

    Parameters
    ----------
    items:
        The list to fan over.  Each element is serialised and
        substituted into *prompt_template* at ``{{item}}``.
    prompt_template:
        Prompt string containing ``{{item}}`` as a placeholder.
    tier:
        Routing tier forwarded to every ``WorkflowSpec``.
    max_parallel:
        Cap on concurrent dispatches (forwarded to
        ``run_workflow_parallel``).
    label_prefix:
        Prefix for the auto-generated ``label`` on each spec.
    **spec_kwargs:
        Extra keyword arguments forwarded verbatim to every
        ``WorkflowSpec`` (e.g. ``max_tokens``, ``timeout``).
    """
    if not items:
        return []

    specs: list[WorkflowSpec] = []
    for i, item in enumerate(items):
        rendered = prompt_template.replace("{{item}}", _serialize_item(item))
        specs.append(
            WorkflowSpec(
                prompt=rendered,
                tier=tier,
                label=f"{label_prefix}_{i}",
                **spec_kwargs,
            )
        )

    return run_workflow_parallel(specs, max_workers=max_parallel)


def fan_out_from_completion(
    upstream_completion: str,
    *,
    prompt_template: str,
    tier: str = "mid",
    max_parallel: int | None = None,
    **spec_kwargs: Any,
) -> list[WorkflowResult]:
    """Extract a list from *upstream_completion* and fan out.

    The extraction tries JSON array first, then line-separated items.
    See :func:`extract_items_from_completion` for details.
    """
    items = extract_items_from_completion(upstream_completion)
    if not items:
        return []
    return fan_out_dispatch(
        items,
        prompt_template=prompt_template,
        tier=tier,
        max_parallel=max_parallel,
        **spec_kwargs,
    )


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def aggregate_fan_out_results(
    results: list[WorkflowResult],
) -> dict[str, Any]:
    """Combine fan-out results into a summary dict.

    Returns
    -------
    dict with keys:
        total       – number of dispatches
        succeeded   – count with status ``"succeeded"``
        failed      – count with status != ``"succeeded"``
        completions – list of completion strings (None for failures)
        outputs     – list of output dicts
        results     – list of full ``to_json()`` dicts
    """
    succeeded = sum(1 for r in results if r.status == "succeeded")
    return {
        "kind": "fan_out_summary",
        "total": len(results),
        "succeeded": succeeded,
        "failed": len(results) - succeeded,
        "completions": [r.completion for r in results],
        "outputs": [dict(r.outputs) for r in results],
        "results": [r.to_json() for r in results],
    }


__all__ = [
    "aggregate_fan_out_results",
    "extract_items_from_completion",
    "fan_out_dispatch",
    "fan_out_from_completion",
]
