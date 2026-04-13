"""Template variable interpolation for workflow specs.

Pure-Python {{variable}} interpolation — no Jinja2 dependency.

Supports:
  - Simple variables:       {{company_name}}
  - Nested access:          {{leads.count}}
  - Default values:         {{key|default:fallback text}}
  - Recursive spec render:  render_spec() walks all string values in a dict
"""

from __future__ import annotations

import re
from typing import Any


# ---------------------------------------------------------------------------
# Error
# ---------------------------------------------------------------------------

class TemplateRenderError(RuntimeError):
    """Raised when a template variable cannot be resolved."""

    def __init__(self, variable_name: str, reason_code: str, message: str | None = None):
        self.variable_name = variable_name
        self.reason_code = reason_code
        super().__init__(message or f"unresolved template variable: {variable_name}")


# ---------------------------------------------------------------------------
# Variable resolution
# ---------------------------------------------------------------------------

# Matches {{key}}, {{key.sub}}, {{key|default:value}}
_VAR_RE = re.compile(r"\{\{(.+?)\}\}")

# Splits the default filter: key|default:fallback
_DEFAULT_RE = re.compile(r"^(.+?)\|default:(.*)$")


def _resolve(key: str, variables: dict[str, Any]) -> Any:
    """Walk dotted path through nested dicts/objects.

    ``_resolve("leads.count", {"leads": {"count": 42}})`` -> ``42``

    Raises KeyError if any segment is missing.
    """
    parts = key.split(".")
    current: Any = variables
    for part in parts:
        if isinstance(current, dict):
            current = current[part]
        elif hasattr(current, part):
            current = getattr(current, part)
        else:
            raise KeyError(part)
    return current


def _replace_match(match: re.Match[str], variables: dict[str, Any]) -> str:
    """Replacement function for a single {{...}} match."""
    raw = match.group(1).strip()

    # Check for |default: filter
    default_match = _DEFAULT_RE.match(raw)
    if default_match:
        key = default_match.group(1).strip()
        fallback = default_match.group(2)
        try:
            return str(_resolve(key, variables))
        except (KeyError, TypeError, AttributeError):
            return fallback

    # No default — must resolve or raise
    try:
        return str(_resolve(raw, variables))
    except (KeyError, TypeError, AttributeError):
        raise TemplateRenderError(
            variable_name=raw,
            reason_code="unresolved_variable",
            message=f"template variable '{raw}' not found in provided variables",
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def render_template(template: str, variables: dict[str, Any]) -> str:
    """Interpolate ``{{variable}}`` placeholders in *template*.

    - Replaces ``{{key}}`` with ``str(variables[key])``.
    - Supports nested access: ``{{leads.count}}``.
    - Supports ``{{key|default:fallback}}`` for optional variables.
    - Raises ``TemplateRenderError`` for unresolved variables with no default.
    """
    if "{{" not in template:
        return template  # fast path

    def _replacer(m: re.Match[str]) -> str:
        return _replace_match(m, variables)

    return _VAR_RE.sub(_replacer, template)


def render_spec(spec_dict: dict[str, Any], variables: dict[str, Any]) -> dict[str, Any]:
    """Recursively render all string values in *spec_dict* through the template engine.

    Walks the full dict tree.  Handles:
      - ``prompt``, ``system_prompt``, ``label`` (top-level strings)
      - ``context_sections[*].content`` (list of dicts)
      - Any other nested string values

    Returns a new dict — the original is not mutated.
    """
    if not variables:
        return spec_dict
    return _render_value(spec_dict, variables)


def _render_value(value: Any, variables: dict[str, Any]) -> Any:
    """Recursively render template variables in an arbitrary value."""
    if isinstance(value, str):
        return render_template(value, variables)
    if isinstance(value, dict):
        return {k: _render_value(v, variables) for k, v in value.items()}
    if isinstance(value, list):
        return [_render_value(item, variables) for item in value]
    return value
