"""Context accumulation and file reference extraction for workflow execution."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace

from contracts.domain import WorkflowNodeContract

from ..context_accumulator import ContextAccumulator


def extract_file_refs(outputs: dict) -> list[str]:
    """Extract file references from parsed output or raw completion text.

    This helper supports the dynamic scope resolution system. When a pipeline
    step produces output containing file paths (e.g., a research or discovery
    step identifies which files need modification), this function extracts
    those references so the next step can resolve their dependencies.

    The function checks:

      1. parsed_output (dict): Looks for these keys:
         - "files", "paths", "write_scope", "targets", "modules", "file_paths"
         Each key's value can be a list of strings or a single string.

      2. completion (str): Scans the raw LLM text for:
         - Python file patterns: path/to/file.py, ./file.py, file.py
         - Path patterns: word/word, word/word/word (2-5 segments max)

    Args:
        outputs: The node's output dictionary. Expected keys include:
          - "parsed_output": dict with structured data from JSON extraction
          - "completion": raw LLM text response
          - Any other output keys (unused by this function)

    Returns:
        Sorted, deduplicated list of file paths found.
        Empty list if no references are discovered.

    Examples:
        >>> outputs = {
        ...     "completion": "Modified files: src/main.py, lib/utils.py",
        ...     "parsed_output": {"files": ["tests/test_main.py"]},
        ... }
        >>> extract_file_refs(outputs)
        ['lib/utils.py', 'src/main.py', 'tests/test_main.py']

        >>> outputs = {
        ...     "parsed_output": {
        ...         "write_scope": ["runtime/dispatch.py", "runtime/execute.py"],
        ...     }
        ... }
        >>> extract_file_refs(outputs)
        ['runtime/dispatch.py', 'runtime/execute.py']
    """
    import re

    file_refs: set[str] = set()

    # Check for parsed_output (from JSON extraction)
    parsed = outputs.get("parsed_output")
    if isinstance(parsed, dict):
        # Look for common scope-related keys
        for key in ("files", "paths", "write_scope", "targets", "modules", "file_paths"):
            val = parsed.get(key)
            if isinstance(val, list):
                for item in val:
                    if isinstance(item, str):
                        file_refs.add(item.strip())
            elif isinstance(val, str):
                file_refs.add(val.strip())

    # Also scan completion text for file patterns
    completion_text = outputs.get("completion")
    if isinstance(completion_text, str) and completion_text.strip():
        # Match Python files: path/to/file.py, ./file.py, file.py
        for match in re.finditer(r"[\w./\-]+\.py\b", completion_text):
            file_refs.add(match.group(0))

        # Match path patterns: word/word, word/word/word (at least 2 segments)
        for match in re.finditer(r"[\w\-]+/[\w./\-]+(?:/[\w./\-]+)*", completion_text):
            candidate = match.group(0)
            # Filter out URLs and common false positives
            if not candidate.startswith("http") and candidate.count("/") <= 5:
                file_refs.add(candidate)

    # Remove empty strings and deduplicate
    return sorted(ref for ref in file_refs if ref)


def inject_accumulated_context(
    node: WorkflowNodeContract,
    accumulator: ContextAccumulator,
) -> WorkflowNodeContract:
    """Return a copy of *node* with accumulated context injected into its inputs.

    Adds the rendered prior-results section to:
      - ``input_payload.context_sections`` (list of dicts) so that the
        prompt renderer picks it up when the dispatch layer uses it.
      - The ``prompt`` string directly (appended) so that adapters that
        read the raw prompt through the workflow CLI backend also see it.

    If the accumulator is empty, returns the original node unchanged.
    """
    if not accumulator:
        return node

    section = accumulator.render_context_section()
    if not section.get("content"):
        return node

    updated_inputs = dict(node.inputs)

    # --- inject into input_payload if it exists as a nested dict ---
    inner_payload = updated_inputs.get("input_payload")
    if isinstance(inner_payload, Mapping):
        inner_payload = dict(inner_payload)
        # Append to context_sections list
        existing_sections = list(inner_payload.get("context_sections") or [])
        existing_sections.append(section)
        inner_payload["context_sections"] = existing_sections
        # Also append to prompt string for adapters that read prompt directly
        prompt = inner_payload.get("prompt")
        if isinstance(prompt, str):
            inner_payload["prompt"] = (
                prompt + "\n\n--- " + section["name"] + " ---\n" + section["content"]
            )
        updated_inputs["input_payload"] = inner_payload
    else:
        # Flat inputs layout — inject context_sections and update prompt
        existing_sections = list(updated_inputs.get("context_sections") or [])
        existing_sections.append(section)
        updated_inputs["context_sections"] = existing_sections
        prompt = updated_inputs.get("prompt")
        if isinstance(prompt, str):
            updated_inputs["prompt"] = (
                prompt + "\n\n--- " + section["name"] + " ---\n" + section["content"]
            )

    return replace(node, inputs=updated_inputs)


__all__ = [
    "extract_file_refs",
    "inject_accumulated_context",
]
