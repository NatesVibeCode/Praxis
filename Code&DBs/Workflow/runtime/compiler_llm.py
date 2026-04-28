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

_APP_COMPILE_TASK_ROUTE = "auto/compile"
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
    ) or _APP_COMPILE_TASK_ROUTE

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
    """Compile prose via three independently-routed sub-task LLM calls.

    Stages, each routed by its own task_type row in `task_type_routing`:
      1. compile_synthesize — parse prose into skeleton (title, placeholders,
         agent roles, variable hints).
      2. compile_pill_match — resolve placeholders against the catalog into
         concrete @integration/action and #type/field bindings.
      3. compile_author     — weave bound refs into the final compiled prose
         with authority/SLA/capabilities.

    Each stage's primary model is picked by the routing authority — flipping
    a `task_type_routing` row retargets that stage without editing code.
    """
    del conn, get_connection  # unused — compile does not submit a workflow job
    if hydrate_env is not None:
        hydrate_env()

    shrunk_context = context[:3000] if len(context) > 3000 else context

    # Stage 1: synthesize — extract title + skeleton with placeholders
    synthesize_raw = _call_compile_sub_task(
        task_type="compile_synthesize",
        prompt=_compile_synthesize_prompt(prose),
    )
    skeleton = _parse_synthesize_response(synthesize_raw, prose)

    # Stage 2: pill_match — resolve placeholders against the catalog
    pill_match_raw = _call_compile_sub_task(
        task_type="compile_pill_match",
        prompt=_compile_pill_match_prompt(skeleton, shrunk_context),
    )
    bindings = _parse_pill_match_response(pill_match_raw)

    # Stage 3: author — weave bindings into the final compiled prose
    author_raw = _call_compile_sub_task(
        task_type="compile_author",
        prompt=_compile_author_prompt(prose, skeleton, bindings, shrunk_context),
    )
    return parse_compile_response(author_raw, prose)


def _call_voting_sub_task(
    *,
    task_type: str,
    prompt: str,
    parser: Any,
    min_votes: int = 3,
    max_votes: int = 5,
    tiebreaker_provider: str | None = None,
    tiebreaker_model: str | None = None,
    early_stop_unanimous: bool = True,
) -> dict[str, Any]:
    """Adaptive voting dispatcher for classification-shaped sub-tasks.

    Resolves top-K voter candidates via `resolve_top_k_voters(task_type)`, runs
    parallel calls, parses each via `parser(raw)` → comparable answer dict, then:

      Round 1 (min_votes calls in parallel):
        - All agree → return that answer (early stop)
        - Clear majority (≥⌈min_votes/2⌉ agree) → return majority
        - Otherwise → expand to Round 2

      Round 2 (additional calls, up to max_votes total):
        - Clear majority across all votes → return majority
        - Still split → escalate to tiebreaker

      Round 3 (single tiebreaker call):
        - Use the supplied tiebreaker_provider/model, OR the rank-1 (highest-
          score) candidate from the resolver as escalation
        - Return its answer with provenance

    The parser must return a hashable-key answer (e.g. a tuple or frozen dict)
    so votes can be tallied. Returns {"answer": <parser output>, "votes": [...],
    "decision_path": "unanimous"|"majority_round1"|"majority_round2"|"tiebreaker"}.
    """
    voters = resolve_top_k_voters(task_type, k=max_votes)
    if not voters:
        raise RuntimeError(
            f"resolve_top_k_voters returned no candidates for {task_type!r}"
        )

    initial_voters = voters[:min_votes]
    extra_voters = voters[min_votes:max_votes]

    def _vote(voter: dict[str, Any]) -> dict[str, Any]:
        try:
            raw = _call_specific_route(
                provider_slug=voter["provider_slug"],
                model_slug=voter["model_slug"],
                prompt=prompt,
                temperature=voter.get("temperature"),
                max_tokens=voter.get("max_tokens"),
            )
            answer = parser(raw)
            return {
                "voter": f"{voter['provider_slug']}/{voter['model_slug']}",
                "answer": answer,
                "raw": raw,
                "ok": True,
            }
        except Exception as exc:
            return {
                "voter": f"{voter['provider_slug']}/{voter['model_slug']}",
                "answer": None,
                "raw": None,
                "ok": False,
                "error": f"{type(exc).__name__}: {str(exc)[:200]}",
            }

    import concurrent.futures

    votes: list[dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(min_votes, len(initial_voters))) as pool:
        futures = [pool.submit(_vote, v) for v in initial_voters]
        for fut in futures:
            votes.append(fut.result())

    def _tally(items: list[dict[str, Any]]) -> tuple[Any, int, int]:
        """Return (winning_answer, vote_count, total_ok). Skips failed votes."""
        ok_votes = [v for v in items if v.get("ok") and v.get("answer") is not None]
        if not ok_votes:
            return None, 0, 0
        counts: dict[Any, int] = {}
        for v in ok_votes:
            key = _hashable(v["answer"])
            counts[key] = counts.get(key, 0) + 1
        winner_key, winner_count = max(counts.items(), key=lambda kv: kv[1])
        for v in ok_votes:
            if _hashable(v["answer"]) == winner_key:
                return v["answer"], winner_count, len(ok_votes)
        return None, 0, len(ok_votes)

    answer, count, total_ok = _tally(votes)

    if early_stop_unanimous and total_ok > 0 and count == total_ok and count >= min_votes:
        return {"answer": answer, "votes": votes, "decision_path": "unanimous", "vote_count": count, "total_ok": total_ok}

    majority_threshold = (min_votes // 2) + 1
    if count >= majority_threshold and (count / max(total_ok, 1)) > 0.5:
        return {"answer": answer, "votes": votes, "decision_path": "majority_round1", "vote_count": count, "total_ok": total_ok}

    if extra_voters:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(extra_voters)) as pool:
            extra_futures = [pool.submit(_vote, v) for v in extra_voters]
            for fut in extra_futures:
                votes.append(fut.result())
        answer, count, total_ok = _tally(votes)
        majority_threshold = (max_votes // 2) + 1
        if count >= majority_threshold and (count / max(total_ok, 1)) > 0.5:
            return {"answer": answer, "votes": votes, "decision_path": "majority_round2", "vote_count": count, "total_ok": total_ok}

    if tiebreaker_provider and tiebreaker_model:
        tiebreaker = {
            "provider_slug": tiebreaker_provider,
            "model_slug": tiebreaker_model,
            "temperature": 0.0,
            "max_tokens": 4096,
        }
    else:
        tiebreaker = voters[0]
    tiebreak_vote = _vote(tiebreaker)
    votes.append(tiebreak_vote)
    if tiebreak_vote.get("ok"):
        return {"answer": tiebreak_vote["answer"], "votes": votes, "decision_path": "tiebreaker",
                "vote_count": 1, "total_ok": total_ok + 1}

    if answer is not None:
        return {"answer": answer, "votes": votes, "decision_path": "tiebreaker_fallback",
                "vote_count": count, "total_ok": total_ok}
    raise RuntimeError(
        f"voting for {task_type!r} produced no usable answer across {len(votes)} votes"
    )


def _hashable(value: Any) -> Any:
    """Convert nested dict/list values to hashable equivalents for vote tallying."""
    if isinstance(value, dict):
        return tuple(sorted((k, _hashable(v)) for k, v in value.items()))
    if isinstance(value, list):
        return tuple(_hashable(item) for item in value)
    return value


def _call_specific_route(
    *,
    provider_slug: str,
    model_slug: str,
    prompt: str,
    temperature: float | None = None,
    max_tokens: int | None = None,
) -> str:
    """Single LLM call against a named provider/model. Used by voting helper."""
    from adapters.keychain import resolve_secret
    from adapters.llm_client import LLMRequest, call_llm
    from registry.provider_execution_registry import (
        resolve_api_endpoint,
        resolve_api_key_env_vars,
        resolve_api_protocol_family,
    )

    endpoint = resolve_api_endpoint(provider_slug, model_slug)
    if not endpoint:
        raise RuntimeError(f"no registered endpoint for {provider_slug}/{model_slug}")
    protocol_family = resolve_api_protocol_family(provider_slug)
    if not protocol_family:
        raise RuntimeError(f"no registered protocol_family for {provider_slug}")

    env = dict(os.environ)
    api_key: str | None = None
    for env_var in resolve_api_key_env_vars(provider_slug):
        candidate = resolve_secret(env_var, env=env)
        if candidate and candidate.strip():
            api_key = candidate.strip()
            break
    if not api_key:
        raise RuntimeError(f"no API key available for {provider_slug}")

    request = LLMRequest(
        endpoint_uri=str(endpoint),
        api_key=api_key,
        provider_slug=provider_slug,
        model_slug=model_slug,
        messages=({"role": "user", "content": prompt},),
        max_tokens=max_tokens,
        temperature=temperature if temperature is not None else 0.0,
        protocol_family=str(protocol_family),
        timeout_seconds=int(compiler_llm_timeout_seconds()),
    )
    response = call_llm(request)
    return response.content


def _call_compile_sub_task(*, task_type: str, prompt: str) -> str:
    """Resolve routes for a compile sub-task and run failover loop. Returns raw response content."""
    from adapters.keychain import resolve_secret
    from adapters.llm_client import LLMRequest, call_llm
    from registry.provider_execution_registry import (
        resolve_api_endpoint,
        resolve_api_key_env_vars,
        resolve_api_protocol_family,
    )

    configs = resolve_matrix_gated_route_configs(task_type)
    if not configs:
        raise RuntimeError(
            f"task_type_routing returned no llm_task routes for {task_type!r}"
        )

    last_error: Exception | None = None
    for cfg in configs:
        provider_slug = cfg["provider_slug"]
        model_slug = cfg["model_slug"]
        try:
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
                max_tokens=cfg.get("max_tokens"),
                temperature=cfg["temperature"] if cfg.get("temperature") is not None else 0.0,
                protocol_family=str(protocol_family),
                timeout_seconds=int(compiler_llm_timeout_seconds()),
            )
            response = call_llm(request)
            logger.debug(
                "Compile sub-task %s response (%d chars): %.200s",
                task_type, len(response.content), response.content,
            )
            return response.content
        except Exception as exc:
            last_error = exc
            logger.warning(
                "Compile sub-task %s route %s/%s failed; trying next route if available: %s",
                task_type, provider_slug, model_slug, exc,
            )
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"all routes for compile sub-task {task_type!r} failed")


def _compile_synthesize_prompt(prose: str) -> str:
    return f"""TASK: Read this operating-model prose and produce a structured skeleton.

Extract the title, the rough prose with placeholders for unresolved references, the agent role names, and any variable patterns. Do NOT try to resolve references against a catalog yet — that's a later step.

PLACEHOLDER NOTATION:
- System/integration references (vague): mark as @?short-role-name (e.g., @?notifier, @?contact-store)
- Data type references (vague): mark as #?short-type-name (e.g., #?contact, #?ticket)
- Variable patterns: capture in variable_hints (e.g., {{"label":"priority","options":["P1","P2","P3"]}})

INPUT:
{prose}

OUTPUT (JSON only, no markdown fences, no other text):
{{"title":"short title","prose_skeleton":"prose with @?placeholder and #?placeholder markers","agent_roles":["role-name"],"variable_hints":[{{"label":"priority","options":["P1","P2","P3"]}}]}}"""


def _compile_pill_match_prompt(skeleton: dict[str, Any], shrunk_context: str) -> str:
    skeleton_json = json.dumps({
        "prose_skeleton": skeleton.get("prose_skeleton", ""),
        "agent_roles": skeleton.get("agent_roles", []),
    }, ensure_ascii=False)
    return f"""TASK: Resolve the @? and #? placeholders in this skeleton against the catalog.

For each placeholder, pick the best concrete catalog entry. Use null for placeholders with no good match.

CATALOG:
{shrunk_context}

SKELETON:
{skeleton_json}

OUTPUT (JSON only, no markdown fences, no other text):
{{"bindings":[{{"placeholder":"@?notifier","resolved":"@notifications/send","confidence":0.9}},{{"placeholder":"#?contact","resolved":"#contact/email","confidence":0.8}}]}}"""


def _compile_author_prompt(prose: str, skeleton: dict[str, Any], bindings: dict[str, Any], shrunk_context: str) -> str:
    skeleton_json = json.dumps(skeleton, ensure_ascii=False)
    bindings_json = json.dumps(bindings, ensure_ascii=False)
    return f"""TASK: Compose the final operating model from the original prose, the skeleton, and the resolved bindings.

{shrunk_context}

RULES:
- Replace each @?placeholder and #?placeholder with its resolved binding from BINDINGS, or rephrase if null
- Mark dynamic values as {{variable: option1|option2}} from variable_hints (e.g., {{priority: P1|P2|P3}})
- Name agents with descriptive hyphenated names ending in -agent (e.g., triage-agent, quality-reviewer)
- Keep prose natural and readable — not code
- Add Authority and SLA lines if the prose implies them
- If the workflow depends on specific research or execution methods, select capability slugs from the toolchain list into a capabilities array

ORIGINAL PROSE:
{prose}

SKELETON:
{skeleton_json}

BINDINGS:
{bindings_json}

OUTPUT (JSON only, no markdown fences, no other text):
{{"title":"short title","prose":"the compiled prose with @/#/{{}} references","authority":"","sla":{{}},"capabilities":["research/local-knowledge"]}}"""


def _parse_synthesize_response(raw: str, original_prose: str) -> dict[str, Any]:
    """Parse synthesize stage. Returns dict with title, prose_skeleton, agent_roles, variable_hints.

    Falls back to a degenerate skeleton built from the original prose if parsing fails,
    so the pipeline can still proceed to author with the original text.
    """
    try:
        parsed = parse_json_object(raw)
    except ValueError:
        return {
            "title": derive_title(original_prose, original_prose),
            "prose_skeleton": original_prose,
            "agent_roles": [],
            "variable_hints": [],
        }
    if not isinstance(parsed, dict):
        return {
            "title": derive_title(original_prose, original_prose),
            "prose_skeleton": original_prose,
            "agent_roles": [],
            "variable_hints": [],
        }
    return {
        "title": _as_text(parsed.get("title")) or derive_title(original_prose, original_prose),
        "prose_skeleton": _as_text(parsed.get("prose_skeleton")) or original_prose,
        "agent_roles": _as_string_list(parsed.get("agent_roles")),
        "variable_hints": parsed.get("variable_hints") if isinstance(parsed.get("variable_hints"), list) else [],
    }


def _parse_pill_match_response(raw: str) -> dict[str, Any]:
    """Parse pill_match stage. Returns dict with bindings list. Empty list on parse failure."""
    try:
        parsed = parse_json_object(raw)
    except ValueError:
        return {"bindings": []}
    if not isinstance(parsed, dict):
        return {"bindings": []}
    bindings = parsed.get("bindings") if isinstance(parsed.get("bindings"), list) else []
    return {"bindings": bindings}


def _resolve_app_compile_route() -> tuple[str, str]:
    """Pick the primary API-backed route for the compile task_type."""
    routes = _resolve_app_compile_routes()
    if not routes:
        raise RuntimeError(
            f"task_type_routing returned no decisions for {_APP_COMPILE_TASK_ROUTE!r}"
        )
    return routes[0]


def resolve_matrix_gated_routes(
    task_type: str,
    *,
    transport_type: str = "API",
    adapter_type: str = "llm_task",
) -> list[tuple[str, str]]:
    """Return matrix-gated routes for a task_type, ordered by task_type_routing rank.

    Reads `effective_private_provider_job_catalog` (the ON-only view of the
    private_model_access_control_matrix) and joins task_type_routing for rank
    ordering. The matrix is the ON/OFF authority; route_source is lineage only
    and is not consulted here.
    """
    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool
    from registry.native_runtime_profile_sync import default_native_runtime_profile_ref

    pool = get_workflow_pool()
    pg = SyncPostgresConnection(pool)
    runtime_profile_ref = (
        os.environ.get("PRAXIS_RUNTIME_PROFILE_REF", "").strip()
        or default_native_runtime_profile_ref(pg)
    )
    try:
        rows = pg.fetch(
            """
            SELECT catalog.provider_slug, catalog.model_slug
              FROM effective_private_provider_job_catalog AS catalog
              JOIN task_type_routing AS route
                ON route.task_type = catalog.job_type
               AND route.provider_slug = catalog.provider_slug
               AND route.model_slug = catalog.model_slug
               AND route.transport_type = catalog.transport_type
               AND route.sub_task_type = '*'
             WHERE catalog.runtime_profile_ref = $1
               AND catalog.job_type = $2
               AND catalog.transport_type = $3
               AND catalog.adapter_type = $4
               AND route.permitted IS TRUE
             ORDER BY route.rank ASC, route.updated_at DESC, catalog.provider_slug, catalog.model_slug
            """,
            runtime_profile_ref,
            task_type,
            transport_type,
            adapter_type,
        )
    except Exception as exc:
        raise RuntimeError(
            f"effective provider job catalog could not resolve {task_type} routes "
            f"for runtime_profile_ref={runtime_profile_ref!r}: {exc}"
        ) from exc
    return [
        (str(row["provider_slug"]), str(row["model_slug"]))
        for row in rows or []
        if str(row.get("provider_slug") or "").strip()
        and str(row.get("model_slug") or "").strip()
    ]


def resolve_top_k_voters(
    task_type: str,
    *,
    k: int = 3,
    transport_type: str = "API",
    adapter_type: str = "llm_task",
    diverse_providers: bool = True,
    exclude_providers: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    """Pick top-K voter candidates for a voting-shaped sub-task, scored from data.

    Reads matrix-gated routes (same admission gate as resolve_matrix_gated_routes),
    joins each route to its provider_model_candidates row for capability tags +
    task affinities + benchmark_profile, then to task_type_route_profiles for
    affinity_labels + benchmark_metric_weights. Each candidate gets a score:

        affinity_score    = (primary match × 3) + (secondary × 2) + (specialized × 1) - (avoid × 5)
        benchmark_score   = sum(benchmark_profile[metric] × benchmark_metric_weights[metric])
        combined          = affinity_score × 100 + benchmark_score

    Affinity dominates while benchmark_profile is empty (current state); benchmark
    refines ordering automatically once Artificial Analysis sync populates it.

    `diverse_providers=True` enforces uncorrelated errors by walking sorted
    candidates and skipping a provider once it's already represented (relaxes
    the constraint if the pool is too small to fill K).
    """
    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool
    from registry.native_runtime_profile_sync import default_native_runtime_profile_ref

    pool = get_workflow_pool()
    pg = SyncPostgresConnection(pool)
    runtime_profile_ref = (
        os.environ.get("PRAXIS_RUNTIME_PROFILE_REF", "").strip()
        or default_native_runtime_profile_ref(pg)
    )

    profile_row = pg.fetchrow(
        """
        SELECT affinity_labels, benchmark_metric_weights
          FROM task_type_route_profiles
         WHERE task_type = $1
        """,
        task_type,
    )
    affinity_labels = (profile_row or {}).get("affinity_labels") if profile_row else None
    benchmark_metric_weights = (profile_row or {}).get("benchmark_metric_weights") if profile_row else None
    if isinstance(affinity_labels, str):
        try:
            affinity_labels = json.loads(affinity_labels)
        except (json.JSONDecodeError, TypeError):
            affinity_labels = None
    if isinstance(benchmark_metric_weights, str):
        try:
            benchmark_metric_weights = json.loads(benchmark_metric_weights)
        except (json.JSONDecodeError, TypeError):
            benchmark_metric_weights = None
    affinity_labels = affinity_labels if isinstance(affinity_labels, dict) else {}
    benchmark_metric_weights = benchmark_metric_weights if isinstance(benchmark_metric_weights, dict) else {}

    primary = set(_as_string_list(affinity_labels.get("primary")))
    secondary = set(_as_string_list(affinity_labels.get("secondary")))
    specialized = set(_as_string_list(affinity_labels.get("specialized")))
    avoid = set(_as_string_list(affinity_labels.get("avoid")))

    rows = pg.fetch(
        """
        SELECT catalog.provider_slug,
               catalog.model_slug,
               route.temperature,
               route.max_tokens,
               cand.capability_tags,
               cand.task_affinities,
               cand.benchmark_profile,
               cand.priority
          FROM effective_private_provider_job_catalog AS catalog
          JOIN task_type_routing AS route
            ON route.task_type = catalog.job_type
           AND route.provider_slug = catalog.provider_slug
           AND route.model_slug = catalog.model_slug
           AND route.transport_type = catalog.transport_type
           AND route.sub_task_type = '*'
          LEFT JOIN provider_model_candidates AS cand
            ON cand.provider_slug = catalog.provider_slug
           AND cand.model_slug = catalog.model_slug
           AND cand.status = 'active'
         WHERE catalog.runtime_profile_ref = $1
           AND catalog.job_type = $2
           AND catalog.transport_type = $3
           AND catalog.adapter_type = $4
           AND route.permitted IS TRUE
        """,
        runtime_profile_ref,
        task_type,
        transport_type,
        adapter_type,
    )

    scored: list[dict[str, Any]] = []
    for row in rows or []:
        provider = str(row.get("provider_slug") or "").strip()
        model = str(row.get("model_slug") or "").strip()
        if not provider or not model or provider in exclude_providers:
            continue

        capability_tags = row.get("capability_tags") or []
        task_affinities = row.get("task_affinities") or {}
        benchmark_profile = row.get("benchmark_profile") or {}
        if isinstance(capability_tags, str):
            try:
                capability_tags = json.loads(capability_tags)
            except (json.JSONDecodeError, TypeError):
                capability_tags = []
        if isinstance(task_affinities, str):
            try:
                task_affinities = json.loads(task_affinities)
            except (json.JSONDecodeError, TypeError):
                task_affinities = {}
        if isinstance(benchmark_profile, str):
            try:
                benchmark_profile = json.loads(benchmark_profile)
            except (json.JSONDecodeError, TypeError):
                benchmark_profile = {}

        candidate_tags: set[str] = set()
        if isinstance(capability_tags, list):
            candidate_tags.update(str(t).strip() for t in capability_tags if t)
        if isinstance(task_affinities, dict):
            for bucket in ("primary", "secondary", "specialized"):
                items = task_affinities.get(bucket)
                if isinstance(items, list):
                    candidate_tags.update(str(t).strip() for t in items if t)

        candidate_avoid: set[str] = set()
        if isinstance(task_affinities, dict):
            avoid_list = task_affinities.get("avoid")
            if isinstance(avoid_list, list):
                candidate_avoid.update(str(t).strip() for t in avoid_list if t)

        affinity_score = (
            3 * len(primary & candidate_tags)
            + 2 * len(secondary & candidate_tags)
            + 1 * len(specialized & candidate_tags)
            - 5 * len(avoid & candidate_tags)
            - 5 * len(primary & candidate_avoid)
        )

        benchmark_score = 0.0
        if isinstance(benchmark_profile, dict):
            for metric, weight in benchmark_metric_weights.items():
                metric_value = benchmark_profile.get(metric)
                if isinstance(metric_value, (int, float)):
                    benchmark_score += float(metric_value) * float(weight)

        priority = row.get("priority")
        priority_score = -float(priority) if isinstance(priority, (int, float)) else 0.0

        combined_score = affinity_score * 100.0 + benchmark_score + priority_score * 0.01
        temperature = row.get("temperature")
        max_tokens = row.get("max_tokens")

        scored.append({
            "provider_slug": provider,
            "model_slug": model,
            "score": combined_score,
            "score_breakdown": {
                "affinity": float(affinity_score),
                "benchmark": float(benchmark_score),
                "priority": priority_score,
            },
            "temperature": float(temperature) if temperature is not None else None,
            "max_tokens": int(max_tokens) if max_tokens is not None else None,
        })

    scored.sort(key=lambda c: c["score"], reverse=True)

    if not diverse_providers or len(scored) <= k:
        return scored[:k]

    selected: list[dict[str, Any]] = []
    used_providers: set[str] = set()
    leftovers: list[dict[str, Any]] = []
    for cand in scored:
        if len(selected) >= k:
            break
        if cand["provider_slug"] in used_providers:
            leftovers.append(cand)
            continue
        selected.append(cand)
        used_providers.add(cand["provider_slug"])
    if len(selected) < k:
        selected.extend(leftovers[: k - len(selected)])
    return selected


def resolve_matrix_gated_route_configs(
    task_type: str,
    *,
    transport_type: str = "API",
    adapter_type: str = "llm_task",
) -> list[dict[str, Any]]:
    """Sibling of :func:`resolve_matrix_gated_routes` that returns the full
    routing-row config (provider_slug, model_slug, temperature, max_tokens)
    instead of just (provider, model) tuples. Migration 276 added the
    temperature + max_tokens columns; call sites that want per-row LLM
    knobs use this resolver instead of the tuple version. NULL columns
    surface as ``None`` and the call site is expected to fall back to its
    own default.
    """
    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool
    from registry.native_runtime_profile_sync import default_native_runtime_profile_ref

    pool = get_workflow_pool()
    pg = SyncPostgresConnection(pool)
    runtime_profile_ref = (
        os.environ.get("PRAXIS_RUNTIME_PROFILE_REF", "").strip()
        or default_native_runtime_profile_ref(pg)
    )
    try:
        rows = pg.fetch(
            """
            SELECT catalog.provider_slug,
                   catalog.model_slug,
                   route.temperature,
                   route.max_tokens
              FROM effective_private_provider_job_catalog AS catalog
              JOIN task_type_routing AS route
                ON route.task_type = catalog.job_type
               AND route.provider_slug = catalog.provider_slug
               AND route.model_slug = catalog.model_slug
               AND route.transport_type = catalog.transport_type
               AND route.sub_task_type = '*'
             WHERE catalog.runtime_profile_ref = $1
               AND catalog.job_type = $2
               AND catalog.transport_type = $3
               AND catalog.adapter_type = $4
               AND route.permitted IS TRUE
             ORDER BY route.rank ASC, route.updated_at DESC,
                      catalog.provider_slug, catalog.model_slug
            """,
            runtime_profile_ref,
            task_type,
            transport_type,
            adapter_type,
        )
    except Exception as exc:
        raise RuntimeError(
            f"effective provider job catalog could not resolve {task_type} route "
            f"configs for runtime_profile_ref={runtime_profile_ref!r}: {exc}"
        ) from exc
    out: list[dict[str, Any]] = []
    for row in rows or []:
        provider = str(row["provider_slug"] if "provider_slug" in row else row.get("provider_slug") or "").strip()
        model = str(row["model_slug"] if "model_slug" in row else row.get("model_slug") or "").strip()
        if not provider or not model:
            continue
        temperature = row["temperature"] if "temperature" in row else row.get("temperature")
        max_tokens = row["max_tokens"] if "max_tokens" in row else row.get("max_tokens")
        out.append({
            "provider_slug": provider,
            "model_slug": model,
            "temperature": float(temperature) if temperature is not None else None,
            "max_tokens": int(max_tokens) if max_tokens is not None else None,
        })
    return out


def _resolve_provider_for_model(model_slug: str) -> str | None:
    """Find the provider that hosts ``model_slug`` by querying
    ``provider_model_candidates``. Used by experiment override resolution
    when the operator pins ``model_slug`` without ``provider_slug`` —
    lets a leg name e.g. ``deepseek-ai/DeepSeek-V3`` even if that model
    isn't in the task_type_routing rows for the work being done.

    Returns the provider_slug from the highest-priority active candidate
    matching the model, or ``None`` when no candidate is registered.
    """
    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

    if not model_slug or not isinstance(model_slug, str):
        return None
    pool = get_workflow_pool()
    pg = SyncPostgresConnection(pool)
    try:
        row = pg.fetchrow(
            """
            SELECT provider_slug
              FROM provider_model_candidates
             WHERE model_slug = $1 AND status = 'active'
             ORDER BY priority ASC, created_at DESC
             LIMIT 1
            """,
            model_slug.strip(),
        )
    except Exception as exc:
        # Best-effort lookup. The caller falls back to route-table
        # narrowing or a loud error when this returns None.
        return None
    if row is None:
        return None
    candidate = row["provider_slug"] if "provider_slug" in row else row.get("provider_slug")
    return str(candidate).strip() if candidate else None


def resolve_task_type_config(task_type: str) -> dict[str, Any] | None:
    """Return the rank-1 row's config for ``task_type`` as a flat dict, or
    None when no permitted route exists.

    Used by the compose_experiment runner: when an experiment leg names a
    ``base_task_type``, we look up the row's resolved config and the
    experiment leg may layer its own deltas on top. The compose call then
    receives a fully-resolved knob set.

    Shape: ``{provider_slug, model_slug, temperature, max_tokens}``. Any
    of the values may be ``None`` when the row does not specify them
    (caller falls back to its own default).
    """
    configs = resolve_matrix_gated_route_configs(task_type)
    return configs[0] if configs else None


def _resolve_app_compile_routes() -> list[tuple[str, str]]:
    """Return matrix-gated API routes for the compile task_type."""
    routes = resolve_matrix_gated_routes("compile")
    if routes:
        return routes
    from registry.native_runtime_profile_sync import default_native_runtime_profile_ref
    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

    pool = get_workflow_pool()
    pg = SyncPostgresConnection(pool)
    runtime_profile_ref = (
        os.environ.get("PRAXIS_RUNTIME_PROFILE_REF", "").strip()
        or default_native_runtime_profile_ref(pg)
    )
    raise RuntimeError(
        "effective provider job catalog returned no runnable API compile "
        f"routes for runtime_profile_ref={runtime_profile_ref!r}"
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
