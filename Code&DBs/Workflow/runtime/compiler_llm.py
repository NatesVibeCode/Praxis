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

# Auto-route key. The router's resolve_failover_chain accepts either an
# auto/X key (which maps to task_type=X) or an explicit provider/model
# slug. A bare task_type like "build" is rejected as Invalid agent slug.
# Earlier failure mode that hid here: openrouter llm_task admission was
# briefly degraded so the resolved chain had no llm_task adapter, raising
# "task_type_routing has no llm_task-backed primary for 'auto/build'" and
# falling through to the deterministic path. compile_runs trace now records
# this with llm_skip_reason set to the exact RuntimeError.
_APP_COMPILE_TASK_ROUTE = "auto/build"
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
    """Compile prose via direct HTTP call to the task_type_routing primary.

    This is the Praxis app's "Describe it" compile path. It resolves the
    primary llm_task route for `auto/build` from `task_type_routing`
    authority (not the workflow runtime_profile) and calls that provider's
    HTTP endpoint directly. Keeping resolution in the DB means a single
    `task_type_routing` row flip retargets the compile engine without
    editing code — and confines paid providers to this app surface instead
    of leaking into background workflow jobs.
    """
    del conn, get_connection  # unused — compile does not submit a workflow job
    if hydrate_env is not None:
        hydrate_env()

    from adapters.keychain import resolve_secret
    from adapters.llm_client import LLMRequest, call_llm
    from registry.provider_execution_registry import (
        resolve_api_endpoint,
        resolve_api_key_env_vars,
        resolve_api_protocol_family,
    )

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

    provider_slug, model_slug = _resolve_app_compile_route()
    endpoint = resolve_api_endpoint(provider_slug, model_slug)
    if not endpoint:
        raise RuntimeError(
            f"no registered endpoint for {provider_slug}/{model_slug}"
        )
    protocol_family = resolve_api_protocol_family(provider_slug)
    if not protocol_family:
        raise RuntimeError(
            f"no registered protocol_family for {provider_slug}"
        )

    env = dict(os.environ)
    api_key: str | None = None
    for env_var in resolve_api_key_env_vars(provider_slug):
        candidate = resolve_secret(env_var, env=env)
        if candidate and candidate.strip():
            api_key = candidate.strip()
            break
    if not api_key:
        raise RuntimeError(
            f"no API key available for {provider_slug} (tried Keychain + env)"
        )

    request = LLMRequest(
        endpoint_uri=str(endpoint),
        api_key=api_key,
        provider_slug=provider_slug,
        model_slug=model_slug,
        messages=({"role": "user", "content": prompt},),
        protocol_family=str(protocol_family),
        timeout_seconds=int(compiler_llm_timeout_seconds()),
    )
    response = call_llm(request)
    logger.debug("Compile response (%d chars): %.300s", len(response.content), response.content)
    parsed = parse_compile_response(response.content, prose)
    # Surface provider + model on the parsed payload so compile_run_trace can
    # record which lane fired without a second resolution pass.
    if isinstance(parsed, dict):
        parsed.setdefault("provider_slug", provider_slug)
        parsed.setdefault("model_slug", model_slug)
    return parsed


def _resolve_app_compile_route() -> tuple[str, str]:
    """Pick the primary API-backed route for the compile task_type.

    Uses `TaskTypeRouter.resolve_failover_chain(_APP_COMPILE_TASK_ROUTE)`
    with no runtime_profile_ref, so resolution pulls straight from
    `task_type_routing` (where explicit operator rows like migration 175
    pin the app's preferred engine at rank=1). The first `llm_task`
    adapter in the chain wins — CLI-backed routes are skipped because
    compile must hit HTTP, not spawn a subprocess.
    """
    import importlib

    router_mod = importlib.import_module(f"{__package__}.task_type_router")
    TaskTypeRouter = router_mod.TaskTypeRouter

    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

    pool = get_workflow_pool()
    pg = SyncPostgresConnection(pool)
    router = TaskTypeRouter(pg)
    chain = router.resolve_failover_chain(_APP_COMPILE_TASK_ROUTE)
    if not isinstance(chain, list) or not chain:
        raise RuntimeError(
            f"task_type_routing returned no decisions for {_APP_COMPILE_TASK_ROUTE!r}"
        )
    for decision in chain:
        adapter_type = str(getattr(decision, "adapter_type", "") or "").strip().lower()
        if adapter_type == "llm_task":
            return str(decision.provider_slug), str(decision.model_slug)
    raise RuntimeError(
        f"task_type_routing has no llm_task-backed primary for {_APP_COMPILE_TASK_ROUTE!r}"
    )


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
