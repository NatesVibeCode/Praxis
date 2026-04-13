"""Compiler sublayer: LLM compilation.

Handles LLM calls, response parsing, context building, output guarding,
and refinement summary construction.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

from runtime.definition_compile_kernel import split_sentences as _kernel_split_sentences

logger = logging.getLogger(__name__)

_DEFAULT_REFINE_AGENT_ROUTE = "auto/medium"
_LOW_VALUE_DUPLICATE_WORD_RE = re.compile(
    r"\b(?P<word>a|an|and|or|the|of|to|in|on|for|with|by|from)\b(?:\s+\b(?P=word)\b)+",
    re.IGNORECASE,
)
_ACRONYM_TOKEN_RE = re.compile(r"\b[A-Z]{2,8}\b")
_WORD_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9_-]*")


def derive_title(prose: str, compiled_prose: str) -> str:
    source = compiled_prose or prose
    if not source:
        return "Untitled operating model"
    first_line = source.splitlines()[0].strip()
    candidate = re.split(r"[.!?]", first_line, maxsplit=1)[0].strip()
    candidate = candidate[:120].strip(" -:;,.")
    return candidate or "Untitled operating model"


def compiler_llm_timeout_seconds() -> float:
    raw = os.environ.get("WORKFLOW_COMPILE_LLM_TIMEOUT_S", "").strip()
    if not raw:
        return 12.0
    try:
        value = float(raw)
    except ValueError:
        return 12.0
    return max(1.0, value)


def compiler_llm_enabled() -> bool:
    raw = os.environ.get("WORKFLOW_COMPILER_ENABLE_LLM", "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def compiler_refine_agent_route() -> str:
    raw = os.environ.get("WORKFLOW_REFINE_AGENT_ROUTE", "").strip()
    return raw or _DEFAULT_REFINE_AGENT_ROUTE


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _as_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_as_text(item) for item in value if _as_text(item)]


def build_llm_context(
    *,
    catalog: list[dict[str, Any]],
    integrations: list[dict[str, Any]],
    object_types: list[dict[str, Any]],
    matched_refs: list[dict[str, Any]],
    composition: dict[str, Any],
    capabilities: list[dict[str, Any]],
    route_hints: tuple[tuple[str, str], ...] = (),
    route_hints_cache: tuple[tuple[str, str], ...] = (),
) -> str:
    integration_lines: list[str] = []
    for integration in integrations[:20]:
        for capability in integration.get("capabilities", [])[:8]:
            description = capability.get("description") or integration.get("description") or "No description"
            integration_lines.append(
                f"- @{integration['id']}/{capability['action']}: {description}"
            )

    object_lines: list[str] = []
    for object_type in object_types[:20]:
        if not object_type.get("fields"):
            object_lines.append(
                f"- #{object_type['type_id']}: {object_type.get('description') or object_type.get('name') or 'No description'}"
            )
            continue
        for field in object_type["fields"][:10]:
            description = field.get("description") or field.get("type") or "No description"
            object_lines.append(
                f"- #{object_type['type_id']}/{field['name']}: {description}"
            )

    semantic_lines = [
        f"- {match['name']} [{match['category']}] rank={match['rank']:.3f}: {match['description'] or 'No description'}"
        for match in matched_refs[:15]
    ]
    binding_lines = [
        f"- {binding['source_id']} -> {binding['target_id']}: {binding['rationale']}"
        for binding in composition.get("bindings", [])[:10]
    ]
    catalog_lines = [
        f"- {entry['slug']}: {entry.get('description') or entry.get('display_name') or ''}".rstrip()
        for entry in catalog[:20]
    ]
    capability_lines = [
        f"- {capability['slug']}: {capability.get('summary') or capability.get('description') or capability.get('title') or 'No description'}"
        for capability in capabilities[:12]
    ]

    effective_route_hints = route_hints or route_hints_cache
    available_routes = ", ".join(
        dict.fromkeys(route for _, route in effective_route_hints)
    ) or "auto/build"

    sections = [
        "Available integrations:",
        "\n".join(integration_lines) if integration_lines else "(none)",
        "",
        "Available data objects:",
        "\n".join(object_lines) if object_lines else "(none)",
        "",
        f"Available agent routes: {available_routes}",
        "",
        "Reference catalog samples:",
        "\n".join(catalog_lines) if catalog_lines else "(none)",
        "",
        "Available research and execution toolchains:",
        "\n".join(capability_lines) if capability_lines else "(none)",
        "",
        "Semantic matches from IntentMatcher:",
        "\n".join(semantic_lines) if semantic_lines else "(none)",
    ]

    if binding_lines:
        sections.extend(
            [
                "",
                "Suggested composition bindings:",
                "\n".join(binding_lines),
            ]
        )

    return "\n".join(sections)


def call_llm_compile(prose: str, context: str, *, conn: Any = None, hydrate_env: Any = None, get_connection: Any = None) -> dict[str, Any]:
    """Dispatch compilation as a job through the queue system."""
    if hydrate_env is not None:
        hydrate_env()
    import time

    shrunk_context = context[:3000] if len(context) > 3000 else context

    prompt = f"""TASK: Compile this operating model description into structured prose with executable references.

{shrunk_context}

RULES:
- Replace vague system references with @integration/action (e.g., @webhook/post, @notifications/send)
- Replace vague data references with #type/field (e.g., #contact/email, #bug/severity)
- Mark dynamic values as {{variable: option1|option2}} (e.g., {{priority: P1|P2|P3}})
- Name agents with descriptive hyphenated names ending in -agent (e.g., triage-agent, quality-reviewer)
- Keep prose natural and readable — not code
- Add Authority and SLA lines if the description implies them
- If the workflow depends on specific research or execution methods, select capability slugs from the toolchain list into a capabilities array

INPUT:
{prose}

OUTPUT (JSON only, no markdown fences, no other text):
{{"title":"short title","prose":"the compiled prose with @/#/{{}} references","authority":"","sla":{{}},"capabilities":["research/local-knowledge"]}}"""

    if conn is None and get_connection is not None:
        conn = get_connection()

    route_slug = compiler_refine_agent_route()

    from runtime.workflow.unified import submit_workflow_inline

    spec = {
        "name": f"compile_{int(time.time())}",
        "phase": "build",
        "outcome_goal": "Compile operating model prose",
        "jobs": [{
            "label": "compile",
            "agent": route_slug,
            "prompt": prompt,
        }],
    }

    result = submit_workflow_inline(conn, spec)
    run_id = result["run_id"]
    logger.info("Compilation submitted as workflow %s", run_id)

    timeout_seconds = compiler_llm_timeout_seconds()
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        rows = conn.execute(
            "SELECT status, stdout_preview FROM workflow_jobs WHERE run_id = $1 AND label = 'compile'",
            run_id,
        )
        if rows and rows[0]["status"] in ("succeeded", "failed", "dead_letter"):
            raw = rows[0].get("stdout_preview", "") or ""
            if rows[0]["status"] != "succeeded":
                raise RuntimeError(f"Compile job failed: {raw[:200]}")
            logger.debug("Compile job output (%d chars): %.300s", len(raw), raw)
            return parse_compile_response(raw, prose)
        time.sleep(2)

    raise RuntimeError(f"Compile job {run_id} timed out after {timeout_seconds:.1f}s")


def parse_compile_response(raw: str, original_prose: str) -> dict[str, Any]:
    """Parse LLM CLI response into a compile result dict."""

    def _make_result(d: dict) -> dict[str, Any]:
        return {
            "title": _as_text(d.get("title")) or derive_title(original_prose, original_prose),
            "prose": _as_text(d.get("prose")) or original_prose,
            "authority": _as_text(d.get("authority")) or "",
            "sla": d.get("sla") if isinstance(d.get("sla"), dict) else {},
            "capabilities": _as_string_list(d.get("capabilities")),
        }

    text = (raw or "").strip()
    if not text:
        raise ValueError("Empty LLM response")

    # 1. Try direct JSON parse
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            for envelope_key in ("result", "response", "text", "output", "content"):
                val = parsed.get(envelope_key)
                if isinstance(val, str) and len(val) > 20:
                    text = val
                    break
                elif isinstance(val, dict) and "prose" in val:
                    return _make_result(val)
            else:
                if "prose" in parsed:
                    return _make_result(parsed)
                if "title" in parsed:
                    return _make_result(parsed)
    except (json.JSONDecodeError, TypeError):
        pass

    # 2. Strip code fences
    stripped = strip_code_fences(text)

    # 3. Extract JSON object from text
    for candidate in (stripped, text):
        extracted = extract_json_object(candidate)
        if extracted:
            try:
                parsed = json.loads(extracted)
                if isinstance(parsed, dict):
                    return _make_result(parsed)
            except (json.JSONDecodeError, TypeError):
                pass

    raise ValueError("Could not parse JSON response")


def guard_llm_compiled_output(source_prose: str, compiled: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
    candidate = _as_text(compiled.get("prose")) or source_prose
    sanitized_candidate = collapse_duplicate_low_value_words(candidate)
    guarded = dict(compiled)
    guarded["prose"] = sanitized_candidate or source_prose

    missing_critical_tokens = sorted(
        token for token in critical_source_tokens(source_prose) if token not in token_set(guarded["prose"])
    )
    if missing_critical_tokens:
        guarded["prose"] = source_prose
        return (
            guarded,
            "unsafe_source_token_loss:" + ",".join(missing_critical_tokens),
        )

    source_steps = _kernel_split_sentences(source_prose)
    candidate_steps = _kernel_split_sentences(guarded["prose"])
    if len(source_steps) > 1 and len(candidate_steps) < len(source_steps):
        guarded["prose"] = source_prose
        return (
            guarded,
            f"unsafe_step_collapse:{len(source_steps)}->{len(candidate_steps)}",
        )

    return guarded, None


def build_refinement_summary(
    *,
    source_prose: str,
    compiled: dict[str, Any],
    llm_requested: bool,
    llm_succeeded: bool,
    llm_error: str | None,
    llm_guard_reason: str | None = None,
) -> dict[str, Any]:
    compiled_prose = _as_text(compiled.get("prose")) or source_prose
    authority = _as_text(compiled.get("authority"))
    sla = compiled.get("sla") if isinstance(compiled.get("sla"), dict) else {}
    compiled_capabilities = _as_string_list(compiled.get("capabilities"))
    materially_changed = (
        compiled_prose.strip() != source_prose.strip()
        or bool(authority)
        or bool(sla)
        or bool(compiled_capabilities)
    )

    if not llm_requested:
        return {
            "requested": False,
            "applied": False,
            "used_llm": False,
            "status": "deterministic",
            "message": "Compile returned the deterministic definition artifact.",
            "reason": "llm_not_requested",
        }

    if llm_guard_reason:
        return {
            "requested": True,
            "applied": False,
            "used_llm": True,
            "status": "fallback",
            "message": "Refine produced a risky prose rewrite, so compile kept the original wording.",
            "reason": llm_guard_reason,
        }

    if llm_succeeded and materially_changed:
        return {
            "requested": True,
            "applied": True,
            "used_llm": True,
            "status": "refined",
            "message": "Refine improved the definition articulation and rebuilt the definition from source prose.",
            "reason": None,
        }

    if llm_succeeded:
        return {
            "requested": True,
            "applied": False,
            "used_llm": True,
            "status": "unchanged",
            "message": "Refine completed, but the resulting definition was materially unchanged.",
            "reason": None,
        }

    return {
        "requested": True,
        "applied": False,
        "used_llm": False,
        "status": "fallback",
        "message": "Refine was requested, but compile kept the deterministic definition artifact.",
        "reason": llm_error or "llm_unavailable",
    }


def collapse_duplicate_low_value_words(text: str) -> str:
    collapsed = re.sub(r"\s+", " ", _as_text(text)).strip()
    if not collapsed:
        return ""
    return _LOW_VALUE_DUPLICATE_WORD_RE.sub(lambda match: match.group("word"), collapsed)


def token_set(text: str) -> set[str]:
    return {token.lower() for token in _WORD_TOKEN_RE.findall(_as_text(text))}


def critical_source_tokens(text: str) -> set[str]:
    return {token.lower() for token in _ACRONYM_TOKEN_RE.findall(_as_text(text))}


def strip_code_fences(text: str) -> str:
    stripped = text.strip()
    if not stripped.startswith("```"):
        return stripped
    lines = stripped.splitlines()
    if len(lines) >= 2 and lines[-1].strip() == "```":
        return "\n".join(lines[1:-1]).strip()
    return stripped


def extract_json_object(text: str) -> str:
    start = text.find("{")
    if start < 0:
        return ""
    depth = 0
    in_string = False
    escape = False
    for index in range(start, len(text)):
        char = text[index]
        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    return ""


def parse_json_object(raw: str) -> Any:
    text = (raw or "").strip()
    candidates = [text]
    stripped = strip_code_fences(text)
    if stripped and stripped not in candidates:
        candidates.append(stripped)
    extracted = extract_json_object(stripped or text)
    if extracted and extracted not in candidates:
        candidates.append(extracted)

    for candidate in candidates:
        try:
            return json.loads(candidate)
        except (json.JSONDecodeError, TypeError):
            continue
    raise ValueError("Could not parse JSON response")
