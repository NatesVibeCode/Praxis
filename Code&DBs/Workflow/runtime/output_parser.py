"""Structured output parsing for LLM completions.

Extracts JSON, lists, and structured data from raw LLM text so that
downstream pipeline nodes can consume typed data instead of parsing
prose themselves.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class StructuredOutput:
    """Result of attempting to parse structured data from a completion."""

    raw_text: str
    parsed: dict | list | None
    parse_success: bool
    format: str  # "json", "markdown_json", "embedded_json", "text"


def parse_json_from_completion(completion: str) -> dict | list | None:
    """Try to extract JSON from an LLM completion string.

    Handles three patterns in order:
      1. Pure JSON — the entire string is valid JSON.
      2. Markdown code block — ```json ... ``` or ``` ... ```.
      3. Embedded JSON — find the first ``{`` or ``[`` and try to
         parse from there to the matching close bracket.

    Returns the parsed object (dict or list), or None if no valid
    JSON could be extracted.  Never raises.
    """
    if not isinstance(completion, str) or not completion.strip():
        return None

    text = completion.strip()

    # 1. Pure JSON
    result = _try_parse_json(text)
    if result is not None:
        return result

    # 2. Markdown code block: ```json\n...\n``` or ```\n...\n```
    result = _try_markdown_json(text)
    if result is not None:
        return result

    # 3. Embedded JSON — scan for first { or [
    result = _try_embedded_json(text)
    if result is not None:
        return result

    return None


def parse_structured_output(
    completion: str,
    *,
    schema: dict | None = None,
) -> StructuredOutput:
    """Parse structured output from a completion with optional schema validation.

    Parameters
    ----------
    completion:
        Raw LLM completion text.
    schema:
        If provided, a dict whose keys are the required top-level keys
        in the parsed output.  Only applies when parsed result is a dict.
        This is a simple key-existence check, not full JSON Schema.

    Returns a ``StructuredOutput`` with ``parse_success=True`` only when
    valid JSON was extracted *and* schema validation (if requested) passed.
    """
    if not isinstance(completion, str):
        return StructuredOutput(
            raw_text=str(completion),
            parsed=None,
            parse_success=False,
            format="text",
        )

    text = completion.strip()

    # Try each extraction strategy and track which one succeeded
    parsed = _try_parse_json(text)
    if parsed is not None:
        fmt = "json"
    else:
        parsed = _try_markdown_json(text)
        if parsed is not None:
            fmt = "markdown_json"
        else:
            parsed = _try_embedded_json(text)
            if parsed is not None:
                fmt = "embedded_json"
            else:
                return StructuredOutput(
                    raw_text=completion,
                    parsed=None,
                    parse_success=False,
                    format="text",
                )

    # Schema validation — check required keys exist
    if schema is not None and isinstance(parsed, dict):
        required_keys = set(schema.keys())
        missing = required_keys - set(parsed.keys())
        if missing:
            return StructuredOutput(
                raw_text=completion,
                parsed=parsed,
                parse_success=False,
                format=fmt,
            )

    return StructuredOutput(
        raw_text=completion,
        parsed=parsed,
        parse_success=True,
        format=fmt,
    )


def extract_list_items(completion: str) -> list[str]:
    """Extract list items from numbered lists, bullet lists, or newline-separated lines.

    Recognizes:
      - Numbered lists:  ``1. item``, ``2) item``, ``1: item``
      - Bullet lists:    ``- item``, ``* item``, ``+ item``
      - Newline-separated non-empty lines (fallback when no list markers found)

    Returns an empty list if no list structure is found or input is empty.
    """
    if not isinstance(completion, str) or not completion.strip():
        return []

    lines = completion.strip().splitlines()

    # Try numbered list: "1. item", "2) item", "1: item"
    numbered = _extract_numbered(lines)
    if numbered:
        return numbered

    # Try bullet list: "- item", "* item", "+ item"
    bulleted = _extract_bulleted(lines)
    if bulleted:
        return bulleted

    # Fallback: non-empty lines (only if there are at least 2)
    non_empty = [line.strip() for line in lines if line.strip()]
    if len(non_empty) >= 2:
        return non_empty

    return []


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_MARKDOWN_JSON_RE = re.compile(
    r"```(?:json|JSON)?\s*\n(.*?)\n\s*```",
    re.DOTALL,
)

_NUMBERED_RE = re.compile(r"^\s*\d+[.):\-]\s+(.+)$")
_BULLET_RE = re.compile(r"^\s*[-*+]\s+(.+)$")


def _try_parse_json(text: str) -> dict | list | None:
    """Attempt to parse the entire text as JSON."""
    try:
        result = json.loads(text)
        if isinstance(result, (dict, list)):
            return result
    except (json.JSONDecodeError, ValueError):
        pass
    return None


def _try_markdown_json(text: str) -> dict | list | None:
    """Extract JSON from markdown code blocks."""
    match = _MARKDOWN_JSON_RE.search(text)
    if match:
        return _try_parse_json(match.group(1).strip())
    return None


def _try_embedded_json(text: str) -> dict | list | None:
    """Find the first { or [ and try to parse a JSON value starting there."""
    # Find earliest opening bracket
    brace_idx = text.find("{")
    bracket_idx = text.find("[")

    candidates: list[int] = []
    if brace_idx != -1:
        candidates.append(brace_idx)
    if bracket_idx != -1:
        candidates.append(bracket_idx)

    if not candidates:
        return None

    # Try from each candidate position, earliest first
    for start in sorted(candidates):
        opener = text[start]
        closer = "}" if opener == "{" else "]"

        # Walk from the end of the string backward to find the last matching closer
        end = text.rfind(closer)
        if end <= start:
            continue

        candidate = text[start : end + 1]
        result = _try_parse_json(candidate)
        if result is not None:
            return result

    return None


def _extract_numbered(lines: list[str]) -> list[str]:
    """Extract items from numbered list lines."""
    items: list[str] = []
    for line in lines:
        m = _NUMBERED_RE.match(line)
        if m:
            items.append(m.group(1).strip())
    # Only return if we found at least 2 numbered items
    return items if len(items) >= 2 else []


def _extract_bulleted(lines: list[str]) -> list[str]:
    """Extract items from bullet list lines."""
    items: list[str] = []
    for line in lines:
        m = _BULLET_RE.match(line)
        if m:
            items.append(m.group(1).strip())
    # Only return if we found at least 2 bulleted items
    return items if len(items) >= 2 else []


__all__ = [
    "StructuredOutput",
    "extract_list_items",
    "parse_json_from_completion",
    "parse_structured_output",
]
