"""Catalog and capability helpers for the operating model compiler."""

from __future__ import annotations

import json
import re
from typing import Any

from runtime.object_schema import list_compiled_object_types
from runtime.integrations.display_names import (
    base_integration_name,
    display_name_for_integration,
)


def load_reference_catalog(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT slug, ref_type, display_name, resolved_id, resolved_table, description
          FROM reference_catalog
         ORDER BY ref_type, slug
        """
    )
    catalog: list[dict[str, Any]] = []
    for row in rows or []:
        item = dict(row)
        ref_type = _slugify(item.get("ref_type"))
        slug = _normalize_reference_slug(ref_type, _as_text(item.get("slug")))
        if not ref_type or not slug:
            continue
        catalog.append(
            {
                "slug": slug,
                "ref_type": ref_type,
                "display_name": _as_text(item.get("display_name")),
                "resolved_id": _as_text(item.get("resolved_id")),
                "resolved_table": _as_text(item.get("resolved_table")),
                "description": _as_text(item.get("description")),
            }
        )
    return catalog


def load_integrations(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, name, provider, capabilities, auth_status, description
          FROM integration_registry
         WHERE auth_status = 'connected'
         ORDER BY name
        """
    )
    integrations: list[dict[str, Any]] = []
    for row in rows or []:
        item = dict(row)
        capabilities = _as_json(item.get("capabilities"), default=[])
        normalized_caps: list[dict[str, Any]] = []
        for capability in capabilities if isinstance(capabilities, list) else []:
            if isinstance(capability, str):
                action = _slugify(capability)
                description = ""
                inputs: list[Any] = []
                required_args: list[Any] = []
            elif isinstance(capability, dict):
                action = _slugify(capability.get("action"))
                description = _as_text(capability.get("description"))
                inputs = capability.get("inputs") if isinstance(capability.get("inputs"), list) else []
                required_args = (
                    capability.get("requiredArgs")
                    if isinstance(capability.get("requiredArgs"), list)
                    else []
                )
            else:
                continue
            if not action:
                continue
            normalized_caps.append(
                {
                    "action": action,
                    "description": description,
                    "inputs": inputs,
                    "requiredArgs": required_args,
                }
            )
        integrations.append(
            {
                "id": _as_text(item.get("id")),
                "name": base_integration_name(item),
                "display_name": display_name_for_integration(item),
                "provider": _as_text(item.get("provider")),
                "auth_status": _as_text(item.get("auth_status")),
                "description": _as_text(item.get("description")),
                "capabilities": normalized_caps,
            }
        )
    return integrations


def load_object_types(conn) -> list[dict[str, Any]]:
    return list_compiled_object_types(conn, limit=1000)


def flatten_match_result(match_result) -> list[dict[str, Any]]:
    matched_refs: list[dict[str, Any]] = []
    for category in ("ui_components", "calculations", "workflows"):
        for item in getattr(match_result, category, ()) or ():
            matched_refs.append(
                {
                    "name": item.name,
                    "description": item.description,
                    "category": item.category,
                    "rank": float(item.rank),
                    "metadata": dict(item.metadata or {}),
                }
            )
    return matched_refs


def composition_to_dict(plan) -> dict[str, Any]:
    if plan is None:
        return {}
    return {
        "components": list(getattr(plan, "components", ()) or ()),
        "calculations": list(getattr(plan, "calculations", ()) or ()),
        "workflows": list(getattr(plan, "workflows", ()) or ()),
        "bindings": [
            {
                "source_id": binding.source_id,
                "source_type": binding.source_type,
                "target_id": binding.target_id,
                "target_type": binding.target_type,
                "rationale": binding.rationale,
            }
            for binding in getattr(plan, "bindings", ()) or ()
        ],
        "layout_suggestion": _as_text(getattr(plan, "layout_suggestion", "")),
        "confidence": float(getattr(plan, "confidence", 0.0) or 0.0),
    }


def build_capability_catalog(integrations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    catalog: list[dict[str, Any]] = [
        {
            "id": "cap-research-local-knowledge",
            "slug": "research/local-knowledge",
            "kind": "memory",
            "title": "Local knowledge recall",
            "summary": "Search prior findings and saved research before going outbound.",
            "description": "Uses praxis_research and the local research runtime to search existing findings and compile briefs before new work starts.",
            "route": "praxis_research",
            "engines": ["praxis_research", "memory.research_runtime"],
            "signals": ["research", "findings", "knowledge", "brief", "prior", "existing", "history", "recall"],
            "reference_slugs": [],
        },
        {
            "id": "cap-research-fan-out",
            "slug": "research/fan-out",
            "kind": "fanout",
            "title": "Parallel research fan-out (API burst)",
            "summary": "Burst N parallel fast-model workers over a research prompt.",
            "description": "Count-based SLM burst via runtime fan_out dispatch. API providers only; CLI adapters are rejected because they break under concurrency bursts.",
            "route": "workflow.fanout",
            "engines": ["fan_out_dispatch", "gpt-5.4-mini"],
            "signals": ["parallel", "fan out", "burst", "fast model", "workers", "broad", "sweep"],
            "reference_slugs": [],
        },
        {
            "id": "cap-research-loop",
            "slug": "research/loop",
            "kind": "loop",
            "title": "Research loop (item-based)",
            "summary": "Run a research step over each item in a list.",
            "description": "Item-based parallel map via runtime loop dispatch. Any provider is allowed; one spec per item with templated prompt substitution.",
            "route": "workflow.loop",
            "engines": ["loop_dispatch"],
            "signals": ["for each", "loop", "iterate", "per item", "per lead", "per url", "map"],
            "reference_slugs": [],
        },
        {
            "id": "cap-research-external-catalog",
            "slug": "research/external-catalog",
            "kind": "catalog_route",
            "title": "Internet docs research",
            "summary": "Use the effective provider catalog for outbound web and official-docs research when local context is not enough.",
            "description": "Uses the catalog-selected research lane exposed by the runtime planner and executor for outbound internet scans, official API docs lookup, and source-backed research passes.",
            "route": "auto/research",
            "engines": ["effective-provider-catalog"],
            "signals": [
                "gemini",
                "cli",
                "scan",
                "external",
                "web",
                "search",
                "browse",
                "online",
                "internet",
                "brave",
                "api docs",
                "official docs",
                "documentation",
                "source urls",
            ],
            "reference_slugs": [],
        },
    ]

    for integration in integrations:
        integration_id = _slugify(integration.get("id") or integration.get("name"))
        integration_name = _as_text(integration.get("display_name")) or display_name_for_integration(integration)
        if not integration_id:
            continue
        for capability in integration.get("capabilities", []):
            action = _slugify(capability.get("action")) if isinstance(capability, dict) else _slugify(capability)
            description = (
                _as_text(capability.get("description")) if isinstance(capability, dict) else ""
            ) or _as_text(integration.get("description"))
            if not action or not looks_like_research_action(action, description):
                continue
            catalog.append(
                {
                    "id": f"cap-{integration_id}-{action}",
                    "slug": f"tool/{integration_id}/{action}",
                    "kind": "integration",
                    "title": f"{integration_name} {action.replace('-', ' ').replace('_', ' ')}",
                    "summary": description or f"Use {integration_name} for {action.replace('-', ' ')}.",
                    "description": description or f"Connected integration capability @{integration_id}/{action}.",
                    "route": f"@{integration_id}/{action}",
                    "engines": [integration_name],
                    "signals": [
                        integration_id,
                        integration_name.lower(),
                        action.replace("-", " "),
                        description.lower(),
                    ],
                    "reference_slugs": [f"@{integration_id}/{action}", f"@{integration_id}"],
                }
            )

    return catalog


def looks_like_research_action(action: str, description: str) -> bool:
    haystack = f"{action} {description}".lower()
    return any(
        token in haystack
        for token in (
            "search",
            "query",
            "intel",
            "research",
            "review opportunities",
            "review-opportunities",
            "find",
            "receipt",
        )
    )


def select_capabilities(
    *,
    original_prose: str,
    materialized_prose: str,
    materialized_capability_slugs: list[str],
    references: list[dict[str, Any]],
    jobs: list[dict[str, Any]],
    catalog: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    catalog_index = {
        _as_text(item.get("capability_slug") or item.get("slug")): item
        for item in catalog
        if _as_text(item.get("capability_slug") or item.get("slug"))
    }
    selected_slugs: list[str] = []

    for slug in materialized_capability_slugs:
        normalized = normalize_capability_slug(slug)
        if normalized in catalog_index and normalized not in selected_slugs:
            selected_slugs.append(normalized)

    if not selected_slugs:
        for slug in infer_capability_slugs(original_prose, materialized_prose, references, jobs, catalog_index):
            if slug in catalog_index and slug not in selected_slugs:
                selected_slugs.append(slug)

    selected: list[dict[str, Any]] = []
    for index, slug in enumerate(selected_slugs, start=1):
        capability = dict(catalog_index[slug])
        capability["id"] = capability.get("id") or f"cap-{index:03d}"
        capability["step_indexes"] = infer_capability_step_indexes(capability, jobs)
        capability["rationale"] = infer_capability_rationale(capability, materialized_prose, original_prose)
        selected.append(capability)
    return selected


def infer_capability_slugs(
    original_prose: str,
    materialized_prose: str,
    references: list[dict[str, Any]],
    jobs: list[dict[str, Any]],
    catalog: dict[str, dict[str, Any]],
) -> list[str]:
    selected: list[str] = []
    haystack = "\n".join(
        [
            original_prose,
            materialized_prose,
            *[_as_text(job.get("label")) for job in jobs],
            *[_as_text(job.get("prompt")) for job in jobs],
            *[_as_text(reference.get("slug")) for reference in references],
        ]
    ).lower()

    has_external_research = any(
        token in haystack
        for token in (
            "external",
            "web",
            "online",
            "internet",
            "browse",
            "scan",
            "brave",
            "browser",
            "api docs",
            "official docs",
            "documentation",
            "source urls",
            "source urls used",
        )
    )
    has_local_recall = any(
        token in haystack
        for token in (
            "prior",
            "existing",
            "history",
            "recall",
            "previous findings",
            "saved research",
            "already know",
            "knowledge graph",
            "local knowledge",
        )
    )

    if has_local_recall or (
        not has_external_research
        and any(token in haystack for token in ("research", "analyze", "investigate", "learn", "brief", "find out"))
    ):
        selected.append("research/local-knowledge")

    if any(token in haystack for token in ("fan out", "burst", "broad sweep", "multiple sources", "cross-check", "parallel", "compare")):
        selected.append("research/fan-out")
    elif "research" in haystack and len(jobs) > 1:
        selected.append("research/fan-out")
    if any(token in haystack for token in ("for each", "per item", "per lead", "per url", "iterate", "loop over", "map over")):
        selected.append("research/loop")

    if has_external_research:
        selected.append("research/external-catalog")

    reference_slugs = {_as_text(reference.get("slug")).lower() for reference in references}
    for slug, capability in catalog.items():
        if _as_text(capability.get("capability_kind") or capability.get("kind")) != "integration":
            continue
        route = _as_text(capability.get("route")).lower()
        if route and route in reference_slugs:
            selected.append(slug)
            continue
        if any(signal and signal in haystack for signal in capability.get("signals", [])):
            selected.append(slug)

    deduped: list[str] = []
    for slug in selected:
        if slug in catalog and slug not in deduped:
            deduped.append(slug)
    return deduped


def infer_capability_step_indexes(capability: dict[str, Any], jobs: list[dict[str, Any]]) -> list[int]:
    if not jobs:
        return []

    route = _as_text(capability.get("route")).lower()
    signals = [signal.lower() for signal in capability.get("signals", []) if _as_text(signal)]
    matched: list[int] = []

    for index, job in enumerate(jobs):
        haystack = "\n".join(
            [
                _as_text(job.get("label")),
                _as_text(job.get("name")),
                _as_text(job.get("title")),
                _as_text(job.get("prompt")),
                _as_text(job.get("agent")),
            ]
        ).lower()
        if route and route in haystack:
            matched.append(index)
            continue
        if any(signal and signal in haystack for signal in signals):
            matched.append(index)

    if matched:
        return matched

    kind = _as_text(capability.get("capability_kind") or capability.get("kind"))
    if kind in {"memory", "cli"}:
        return [0]
    if kind in {"loop", "fanout"}:
        return list(range(len(jobs)))
    return []


def infer_capability_rationale(
    capability: dict[str, Any],
    materialized_prose: str,
    original_prose: str,
) -> str:
    signals = [signal.lower() for signal in capability.get("signals", []) if _as_text(signal)]
    for sentence, _start, _end in _split_sentences(materialized_prose):
        lowered = sentence.lower()
        if any(signal and signal in lowered for signal in signals):
            return sentence

    kind = _as_text(capability.get("kind"))
    if kind == "memory":
        return "This workflow benefits from checking prior findings before new work starts."
    if kind == "fanout":
        return "This workflow benefits from bursting N parallel API workers rather than relying on a single pass."
    if kind == "loop":
        return "This workflow benefits from iterating the same step over each item in a list."
    if kind == "cli":
        return "This workflow may need a broader external scan than the local graph alone can provide."
    if kind == "integration":
        return "This connected search/intelligence surface is relevant to the workflow's research path."
    return (_as_text(capability.get("summary")) or original_prose[:160]).strip()


def normalize_capability_slug(value: str) -> str:
    text = _as_text(value).lower()
    if not text:
        return ""
    return text.strip()


def _normalize_reference_slug(ref_type: str, slug: str) -> str:
    raw = _as_text(slug)
    if not raw:
        return ""
    if ref_type == "integration":
        raw = raw if raw.startswith("@") else f"@{raw}"
        prefix, _, suffix = raw[1:].partition("/")
        prefix = _slugify(prefix)
        suffix = _slugify(suffix)
        return f"@{prefix}/{suffix}" if suffix else f"@{prefix}"
    if ref_type == "object":
        raw = raw if raw.startswith("#") else f"#{raw}"
        prefix, _, suffix = raw[1:].partition("/")
        prefix = _slugify(prefix)
        if suffix.startswith("{") and suffix.endswith("}"):
            inner = _slugify(suffix[1:-1])
            suffix = f"{{{inner}}}" if inner else ""
        else:
            suffix = _slugify(suffix)
        return f"#{prefix}/{suffix}" if suffix else f"#{prefix}"
    if ref_type == "variable":
        inner = _slugify(raw.strip("{} ").split(":", 1)[0])
        return f"{{{inner}}}" if inner else ""
    if ref_type == "agent":
        return _slugify(raw)
    return raw


def _split_sentences(text: str) -> list[tuple[str, int, int]]:
    sentences: list[tuple[str, int, int]] = []
    start = 0
    for match in re.finditer(r"[.!?\n]+", text):
        end = match.end()
        sentence = text[start:end].strip()
        if sentence:
            sentence_start = text.find(sentence, start, end)
            sentences.append((sentence, sentence_start, sentence_start + len(sentence)))
        start = end
    if start < len(text):
        sentence = text[start:].strip()
        if sentence:
            sentence_start = text.find(sentence, start)
            sentences.append((sentence, sentence_start, sentence_start + len(sentence)))
    return sentences


def _slugify(value: Any) -> str:
    text = _as_text(value).lower()
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9/_-]+", "-", text)
    text = re.sub(r"-{2,}", "-", text)
    return text.strip("-/")


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _as_json(value: Any, *, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return default
    return default
