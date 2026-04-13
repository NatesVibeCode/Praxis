"""Structured output parsing for model responses.

Models produce structured output via stdout — either JSON with code blocks
or fenced markdown code blocks. This module parses both formats into a
canonical StructuredOutput that the graph uses to decide file writes.

The model never touches the filesystem. It produces code as text.
The graph reads this output and writes files after review/promotion gates.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class CodeBlock:
    """A single code artifact produced by a model.

    Attributes
    ----------
    file_path:
        Relative path where this code should be written (e.g. "runtime/domain.py").
    content:
        The full file content as text.
    language:
        Language identifier (e.g. "python", "json", "sql").
    action:
        What the graph should do: "create", "replace", or "patch".
    """

    file_path: str
    content: str
    language: str = "python"
    action: str = "replace"  # "create" | "replace" | "patch"


@dataclass(frozen=True, slots=True)
class StructuredOutput:
    """Canonical structured output from a model dispatch.

    This is the contract between the adapter (which captures model stdout)
    and the graph (which decides what to write to disk).
    """

    code_blocks: tuple[CodeBlock, ...]
    explanation: str
    raw_text: str
    metadata: dict[str, Any] = field(default_factory=dict)
    parse_strategy: str = "unknown"

    @property
    def has_code(self) -> bool:
        return len(self.code_blocks) > 0

    @property
    def file_paths(self) -> tuple[str, ...]:
        return tuple(cb.file_path for cb in self.code_blocks)


# ---------------------------------------------------------------------------
# JSON extraction (handles markdown fences, leading text, etc.)
# ---------------------------------------------------------------------------

_FENCED_JSON_RE = re.compile(r"```(?:json)?\s*\n(.*?)```", re.DOTALL)


def _extract_json_text(text: str) -> str | None:
    """Extract a JSON value from text that may contain markdown fences or prose.

    Strategies (tried in order):
      1. Fenced code block: ```json ... ``` or ``` ... ```
      2. Bracket scan: first '{' or '[' through matching last '}' or ']'
    """
    raw = text.strip()
    if not raw:
        return None

    # Strategy 1 — fenced code block
    m = _FENCED_JSON_RE.search(raw)
    if m:
        return m.group(1).strip()

    # Strategy 2 — outermost brackets
    first_brace = raw.find("{")
    first_bracket = raw.find("[")
    openers = [i for i in (first_brace, first_bracket) if i >= 0]
    if not openers:
        return None
    start = min(openers)
    closer = "}" if raw[start] == "{" else "]"
    end = raw.rfind(closer)
    if end <= start:
        return None
    return raw[start : end + 1]


# ---------------------------------------------------------------------------
# JSON output parsing
# ---------------------------------------------------------------------------

def _parse_json_output(text: str) -> StructuredOutput | None:
    """Try to parse model output as JSON structured output.

    Handles two formats:

    1. Direct structured output::

        {"code_blocks": [...], "explanation": "..."}

    2. Claude --output-format json envelope::

        {"type": "result", "result": "{\"code_blocks\": [...], ...}"}

    """
    try:
        data = json.loads(text.strip())
    except (json.JSONDecodeError, ValueError):
        return None

    if not isinstance(data, dict):
        return None

    # Unwrap provider JSON envelopes if present.
    # Claude: {"type": "result", "result": "<json string or dict>"}
    # Codex:  {"message": "<text>", ...} or {"response": "<text>"}
    # Gemini: {"result": "<text>", ...} or {"response": {...}}
    for key in ("result", "response", "message", "content", "output", "structured_output"):
        if key in data and "code_blocks" not in data:
            inner = data[key]
            if isinstance(inner, str):
                # Try direct JSON parse first
                try:
                    inner_data = json.loads(inner)
                    if isinstance(inner_data, dict) and "code_blocks" in inner_data:
                        data = inner_data
                        break
                except (json.JSONDecodeError, ValueError):
                    pass
                # Try extracting JSON from markdown fences or prose
                extracted = _extract_json_text(inner)
                if extracted:
                    try:
                        inner_data = json.loads(extracted)
                        if isinstance(inner_data, dict) and "code_blocks" in inner_data:
                            data = inner_data
                            break
                    except (json.JSONDecodeError, ValueError):
                        pass
            elif isinstance(inner, dict):
                if "code_blocks" in inner:
                    data = inner
                    break

    blocks_raw = data.get("code_blocks")
    if not isinstance(blocks_raw, list):
        return None

    blocks: list[CodeBlock] = []
    for b in blocks_raw:
        if not isinstance(b, dict):
            continue
        fp = b.get("file_path", "")
        content = b.get("content", "")
        if not fp or not content:
            continue
        blocks.append(CodeBlock(
            file_path=str(fp),
            content=str(content),
            language=str(b.get("language", "python")),
            action=str(b.get("action", "replace")),
        ))

    return StructuredOutput(
        code_blocks=tuple(blocks),
        explanation=str(data.get("explanation", "")),
        raw_text=text,
        metadata={k: v for k, v in data.items() if k not in ("code_blocks", "explanation")},
        parse_strategy="json",
    )


# ---------------------------------------------------------------------------
# Fenced code block parsing
# ---------------------------------------------------------------------------

# Matches: ```language:filepath or ```language filepath
# Also matches: ```language\n// filepath: path/to/file
_FENCE_PATTERN = re.compile(
    r"```(\w+)(?::|\s+)([\w/._-]+)\s*\n(.*?)```",
    re.DOTALL,
)

# Matches: FILE: path/to/file\n```language\n...\n```
_FILE_HEADER_PATTERN = re.compile(
    r"(?:FILE|File|file):\s*([\w/._-]+)\s*\n```(\w*)\n(.*?)```",
    re.DOTALL,
)

# Matches generic fenced blocks: ```language\n...\n```
_GENERIC_FENCE = re.compile(
    r"```(\w*)\n(.*?)```",
    re.DOTALL,
)


def _parse_fenced_output(text: str, *, default_path: str = "") -> StructuredOutput | None:
    """Parse model output containing fenced code blocks.

    Supports several patterns:
      1. ```python:runtime/domain.py ... ```
      2. FILE: runtime/domain.py\\n```python\\n...\\n```
      3. Generic ```python\\n...\\n``` (uses default_path)
    """
    blocks: list[CodeBlock] = []

    # Try pattern 1: ```lang:path
    for m in _FENCE_PATTERN.finditer(text):
        lang, path, content = m.group(1), m.group(2), m.group(3)
        blocks.append(CodeBlock(
            file_path=path.strip(),
            content=content.rstrip(),
            language=lang.strip(),
            action="replace",
        ))

    # Try pattern 2: FILE: path\n```lang
    if not blocks:
        for m in _FILE_HEADER_PATTERN.finditer(text):
            path, lang, content = m.group(1), m.group(2), m.group(3)
            blocks.append(CodeBlock(
                file_path=path.strip(),
                content=content.rstrip(),
                language=lang.strip() or "python",
                action="replace",
            ))

    # Try pattern 3: generic fenced blocks
    if not blocks and default_path:
        for m in _GENERIC_FENCE.finditer(text):
            lang, content = m.group(1), m.group(2)
            blocks.append(CodeBlock(
                file_path=default_path,
                content=content.rstrip(),
                language=lang.strip() or "python",
                action="replace",
            ))

    if not blocks:
        return None

    # Extract explanation (text outside code fences)
    explanation = _GENERIC_FENCE.sub("", text).strip()
    explanation = _FILE_HEADER_PATTERN.sub("", explanation).strip()

    return StructuredOutput(
        code_blocks=tuple(blocks),
        explanation=explanation,
        raw_text=text,
        parse_strategy="fenced",
    )


# ---------------------------------------------------------------------------
# NDJSON stream parsing
# ---------------------------------------------------------------------------

def _parse_ndjson_stream(text: str) -> StructuredOutput | None:
    """Parse NDJSON stream output (one JSON object per line).

    Handles multiple provider formats:

    Claude (--output-format stream-json)::
        {"type": "content_block_delta", "delta": {"type": "text_delta", "text": "..."}}
        {"type": "result", "result": "..."}

    Codex (exec --json)::
        {"type": "item.completed", "item": {"type": "agent_message", "text": "..."}}
        {"type": "turn.completed", "usage": {...}}
    """
    lines = text.strip().splitlines()
    content_parts: list[str] = []
    last_result: dict[str, Any] | None = None

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue

        if not isinstance(obj, dict):
            continue

        msg_type = obj.get("type", "")

        # --- Claude formats ---
        if msg_type == "result":
            last_result = obj
        elif msg_type == "assistant":
            content = obj.get("message", {}).get("content", "")
            if content:
                content_parts.append(str(content))
        elif msg_type == "content_block_delta":
            delta = obj.get("delta", {})
            if delta.get("type") == "text_delta":
                content_parts.append(str(delta.get("text", "")))

        # --- Codex formats ---
        elif msg_type == "item.completed":
            item = obj.get("item", {})
            item_text = item.get("text", "")
            if item_text:
                content_parts.append(str(item_text))

    if not content_parts and not last_result:
        return None

    # Use result text if available, otherwise join content parts
    full_text = ""
    if last_result:
        full_text = str(last_result.get("result", ""))
    if not full_text:
        full_text = "".join(content_parts)

    if not full_text:
        return None

    # Try to parse the assembled text as JSON or fenced blocks
    parsed = _parse_json_output(full_text)
    if parsed and parsed.has_code:
        return StructuredOutput(
            code_blocks=parsed.code_blocks,
            explanation=parsed.explanation,
            raw_text=text,
            metadata=parsed.metadata,
            parse_strategy="ndjson+json",
        )

    # Try extracting JSON from prose-prefixed text (Codex pattern)
    extracted = _extract_json_text(full_text)
    if extracted and extracted != full_text:
        parsed = _parse_json_output(extracted)
        if parsed and parsed.has_code:
            return StructuredOutput(
                code_blocks=parsed.code_blocks,
                explanation=parsed.explanation,
                raw_text=text,
                metadata=parsed.metadata,
                parse_strategy="ndjson+extracted_json",
            )

    parsed = _parse_fenced_output(full_text)
    if parsed:
        return StructuredOutput(
            code_blocks=parsed.code_blocks,
            explanation=parsed.explanation,
            raw_text=text,
            parse_strategy="ndjson+fenced",
        )

    # No structured content — return as explanation-only
    return StructuredOutput(
        code_blocks=(),
        explanation=full_text,
        raw_text=text,
        parse_strategy="ndjson+text",
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def parse_model_output(
    text: str,
    *,
    default_file_path: str = "",
) -> StructuredOutput:
    """Parse raw model stdout into StructuredOutput.

    Tries strategies in order:
      1. JSON structured output
      2. NDJSON stream (Claude stream-json format)
      3. Fenced code blocks with file paths
      4. Fallback: raw text as explanation with no code blocks

    Parameters
    ----------
    text:
        Raw stdout from the model process.
    default_file_path:
        If fenced blocks don't specify a path, use this as the target file.
    """
    if not text or not text.strip():
        return StructuredOutput(
            code_blocks=(),
            explanation="",
            raw_text=text or "",
            parse_strategy="empty",
        )

    stripped = text.strip()

    # 1. Try JSON (direct or inside provider envelope)
    result = _parse_json_output(stripped)
    if result and result.has_code:
        return result

    # 1b. Try extracting JSON from markdown fences or surrounding prose
    extracted = _extract_json_text(stripped)
    if extracted and extracted != stripped:
        result = _parse_json_output(extracted)
        if result and result.has_code:
            return result

    # 2. Try NDJSON stream
    if "\n" in stripped and stripped.startswith("{"):
        result = _parse_ndjson_stream(stripped)
        if result:
            return result

    # 3. Try fenced code blocks
    result = _parse_fenced_output(stripped, default_path=default_file_path)
    if result:
        return result

    # 4. Fallback — raw text, no code
    return StructuredOutput(
        code_blocks=(),
        explanation=stripped,
        raw_text=text,
        parse_strategy="raw_text",
    )
