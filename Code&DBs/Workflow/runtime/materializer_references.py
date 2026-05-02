"""Compiler sublayer: reference extraction and resolution.

Handles extracting @integration, #object, {variable}, and agent references
from compiled prose via regex patterns, resolving them against the catalog,
inferring agent routes, and generating provisional jobs.
"""

from __future__ import annotations

import re
import uuid
from typing import Any


# ---------------------------------------------------------------------------
# Reference regex patterns
# ---------------------------------------------------------------------------

INTEGRATION_PATTERN = re.compile(
    r"@[a-z0-9][a-z0-9_.-]*(?:/[a-z0-9][a-z0-9{}_.:-]*)?",
    re.IGNORECASE,
)
OBJECT_PATTERN = re.compile(
    r"#[a-z0-9][a-z0-9_.-]*(?:/(?:[a-z0-9][a-z0-9_.:-]*|\{[a-z0-9_-]+\}))?",
    re.IGNORECASE,
)
VARIABLE_PATTERN = re.compile(
    r"\{([a-z0-9_-]+)(?:\s*:\s*([^{}\n]+))?\}",
    re.IGNORECASE,
)
AGENT_PATTERN = re.compile(
    r"\b[a-z0-9]+(?:-[a-z0-9]+)*-agent\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Utility helpers (local copies to avoid circular imports)
# ---------------------------------------------------------------------------

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


def _slugify(value: Any) -> str:
    text = _as_text(value).lower()
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9/_-]+", "-", text)
    text = re.sub(r"-{2,}", "-", text)
    return text.strip("-/")


# ---------------------------------------------------------------------------
# Reference extraction
# ---------------------------------------------------------------------------

def extract_references(text: str) -> list[dict[str, Any]]:
    references: list[dict[str, Any]] = []
    occupied: list[tuple[int, int]] = []

    def add_reference(ref_type: str, span: tuple[int, int], raw: str, config: dict[str, Any] | None = None) -> None:
        start, end = span
        if _overlaps(span, occupied):
            return
        slug = normalize_reference_slug(ref_type, raw)
        if not slug:
            return
        payload = {
            "id": f"ref-{len(references) + 1:03d}",
            "type": ref_type,
            "slug": slug,
            "span": [start, end],
            "raw": raw,
            "config": dict(config or {}),
            "resolved": False,
            "resolved_to": None,
            "display_name": None,
            "description": None,
        }
        references.append(payload)
        occupied.append(span)

    for match in INTEGRATION_PATTERN.finditer(text):
        add_reference("integration", match.span(), match.group(0))
    for match in OBJECT_PATTERN.finditer(text):
        add_reference("object", match.span(), match.group(0))
    for match in VARIABLE_PATTERN.finditer(text):
        name = _slugify(match.group(1))
        options = [
            part.strip()
            for part in _as_text(match.group(2)).split("|")
            if part.strip()
        ]
        add_reference(
            "variable",
            match.span(),
            match.group(0),
            {"name": name, "options": options},
        )
    for match in AGENT_PATTERN.finditer(text):
        add_reference("agent", match.span(), match.group(0))

    references.sort(key=lambda item: item["span"][0])
    for index, reference in enumerate(references, start=1):
        reference["id"] = f"ref-{index:03d}"
    return references


# ---------------------------------------------------------------------------
# Reference resolution
# ---------------------------------------------------------------------------

def resolve_references(
    references: list[dict[str, Any]],
    catalog: list[dict[str, Any]],
    *,
    route_hints: tuple[tuple[str, str], ...] = (),
    route_hints_cache: tuple[tuple[str, str], ...] = (),
) -> tuple[list[dict[str, Any]], list[str]]:
    lookup = {
        (entry["ref_type"], entry["slug"]): entry
        for entry in catalog
        if entry.get("ref_type") and entry.get("slug")
    }
    unresolved: list[str] = []

    for reference in references:
        ref_type = reference["type"]
        slug = normalize_reference_slug(ref_type, reference["slug"])

        if ref_type == "variable":
            reference["slug"] = slug
            reference["resolved"] = True
            reference["resolved_to"] = slug
            continue

        entry = lookup.get((ref_type, slug))
        if entry is None and ref_type in {"integration", "object"}:
            entry = lookup.get((ref_type, base_reference_slug(ref_type, slug)))
        if entry is None and ref_type == "agent":
            agent_base = agent_base_slug(slug)
            entry = lookup.get((ref_type, slug)) or lookup.get((ref_type, agent_base))

        if entry is not None:
            reference["slug"] = slug
            reference["resolved"] = True
            reference["resolved_to"] = _resolved_to(ref_type, slug, entry)
            reference["display_name"] = entry.get("display_name")
            reference["description"] = entry.get("description")
            reference["config"] = merge_reference_config(
                reference,
                entry,
                route_hints=route_hints,
                route_hints_cache=route_hints_cache,
            )
            continue

        if ref_type == "agent":
            route = infer_agent_route(slug, reference, route_hints=route_hints, route_hints_cache=route_hints_cache)
            reference["slug"] = slug
            reference["resolved"] = True
            reference["resolved_to"] = f"task_type_routing:{route}"
            reference["display_name"] = display_name_for_agent(slug)
            reference["description"] = f"Compiled agent routed to {route}"
            reference["config"] = {
                **dict(reference.get("config") or {}),
                "route": route,
            }
            continue

        reference["slug"] = slug
        unresolved.append(slug)

    return references, sorted(set(unresolved))


# ---------------------------------------------------------------------------
# Job generation
# ---------------------------------------------------------------------------

def resolve_agent_from_registry(
    conn: Any,
    agent_ref: str | None,
    slug: str | None,
    reference: dict[str, Any],
    route_hints: tuple[tuple[str, str], ...] = (),
    route_hints_cache: tuple[tuple[str, str], ...] = (),
) -> dict[str, Any]:
    from runtime.agent_context import (
        build_agent_prompt_envelope,
        load_standing_orders,
    )
    import json

    agent_ref_str = _as_text(agent_ref)
    if agent_ref_str and not agent_ref_str.startswith("agent."):
        legacy_to_builtin = {
            "auto/build": "agent.builtin.build",
            "auto/review": "agent.builtin.review",
            "auto/research": "agent.builtin.research",
            "auto/reasoning": "agent.builtin.reasoning",
        }
        if agent_ref_str in legacy_to_builtin:
            agent_ref_str = legacy_to_builtin[agent_ref_str]

    agent_def = None
    if agent_ref_str and conn is not None:
        try:
            rows = conn.execute(
                """SELECT
                       agent_principal_ref,
                       title,
                       status,
                       system_prompt_template,
                       write_envelope,
                       capability_refs,
                       integration_refs,
                       standing_order_keys,
                       allowed_tools,
                       network_policy
                   FROM agent_registry
                   WHERE agent_principal_ref = $1
                   LIMIT 1""",
                agent_ref_str,
            )
            if rows:
                agent_def = dict(rows[0])
        except Exception:
            pass

    if agent_def:
        def _normalise_jsonb_list(value: Any) -> tuple[str, ...]:
            if value is None: return ()
            if isinstance(value, str):
                try: value = json.loads(value)
                except Exception: return ()
            if not isinstance(value, (list, tuple)): return ()
            return tuple(str(item) for item in value if isinstance(item, (str, int, float)))

        standing_keys = _normalise_jsonb_list(agent_def.get("standing_order_keys"))
        standing_orders = load_standing_orders(conn, standing_keys) if standing_keys and conn else []
        
        system_prompt = build_agent_prompt_envelope(
            agent=agent_def,
            base_instruction=agent_def.get("system_prompt_template") or f"You are {slug}. Execute the responsibilities assigned in this operating model.",
            standing_orders=standing_orders,
        )
        return {
            "route": agent_def["agent_principal_ref"],
            "agent_principal_ref": agent_def["agent_principal_ref"],
            "system_prompt": system_prompt,
            "write_envelope": list(_normalise_jsonb_list(agent_def.get("write_envelope"))),
            "allowed_tools": list(_normalise_jsonb_list(agent_def.get("allowed_tools"))),
            "network_policy": agent_def.get("network_policy") or "praxis_only",
        }

    inferred_route = infer_agent_route(
        slug,
        reference,
        route_hints=route_hints,
        route_hints_cache=route_hints_cache,
    )
    return {
        "route": inferred_route,
        "system_prompt": f"You are {slug}. Execute the responsibilities assigned in this operating model." if slug else "Execute the responsibilities assigned in this operating model.",
    }


def generate_jobs(
    materialized_prose: str,
    references: list[dict[str, Any]],
    *,
    conn: Any = None,
    route_hints: tuple[tuple[str, str], ...] = (),
    route_hints_cache: tuple[tuple[str, str], ...] = (),
) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    previous_label: str | None = None

    ordered = sorted(
        (reference for reference in references if reference.get("type") == "agent" and reference.get("resolved")),
        key=lambda item: item.get("span", [10**9])[0],
    )
    for index, reference in enumerate(ordered, start=1):
        slug = _as_text(reference.get("slug"))
        route = _as_text((reference.get("config") or {}).get("route")) or infer_agent_route(
            slug,
            reference,
            route_hints=route_hints,
            route_hints_cache=route_hints_cache,
        )
        label = unique_job_label(slug or f"job-{index}", existing=jobs)
        sentence = sentence_for_span(materialized_prose, reference.get("span"))
        prompt = "\n\n".join(
            [
                f"## Operating Model\n{materialized_prose}",
                f"## Responsible Agent\n{slug}",
                f"## Focus\n{sentence or materialized_prose}",
            ]
        )
        job = {
            "label": label,
            "agent": route,
            "prompt": prompt,
            "agent_name": slug,
            "system_prompt": f"You are {slug}. Execute the responsibilities assigned in this operating model.",
        }
        if previous_label:
            job["depends_on"] = [previous_label]
        jobs.append(job)
        previous_label = label

    if jobs:
        return jobs

    return [
        {
            "label": "execute",
            "agent": "auto/build",
            "prompt": f"## Operating Model\n{materialized_prose}",
        }
    ]


# ---------------------------------------------------------------------------
# Slug normalization helpers
# ---------------------------------------------------------------------------

def normalize_reference_slug(ref_type: str, slug: str) -> str:
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


def base_reference_slug(ref_type: str, slug: str) -> str:
    if ref_type not in {"integration", "object"}:
        return slug
    prefix = "@" if ref_type == "integration" else "#"
    return prefix + slug[1:].split("/", 1)[0]


def agent_base_slug(slug: str) -> str:
    normalized = _slugify(slug)
    if normalized.endswith("-agent"):
        return normalized[: -len("-agent")]
    return normalized


def infer_agent_route(
    slug: str,
    reference: dict[str, Any] | None = None,
    *,
    route_hints: tuple[tuple[str, str], ...] = (),
    route_hints_cache: tuple[tuple[str, str], ...] = (),
) -> str:
    haystack = slug.lower()
    if reference:
        haystack = " ".join(
            [
                haystack,
                _as_text(reference.get("description")),
                _as_text((reference.get("config") or {}).get("description")),
            ]
        ).lower()
    effective_route_hints = route_hints or route_hints_cache
    for hint, route in effective_route_hints:
        if hint in haystack:
            return route
    if any(token in haystack for token in ("review", "validate", "verify", "audit", "judge", "critic")):
        return "auto/review"
    if any(token in haystack for token in ("research", "investigate", "analyze", "search", "brief")):
        return "auto/research"
    if any(token in haystack for token in ("reason", "synth", "reconcile", "deduce")):
        return "auto/reasoning"
    return "auto/build"


def display_name_for_agent(slug: str) -> str:
    return _as_text(slug).replace("-", " ").title()


def merge_reference_config(
    reference: dict[str, Any],
    entry: dict[str, Any],
    *,
    route_hints: tuple[tuple[str, str], ...] = (),
    route_hints_cache: tuple[tuple[str, str], ...] = (),
) -> dict[str, Any]:
    config = dict(reference.get("config") or {})
    config.setdefault("resolved_id", entry.get("resolved_id"))
    config.setdefault("resolved_table", entry.get("resolved_table"))
    config.setdefault("description", entry.get("description"))

    slug = _as_text(reference.get("slug"))
    if reference.get("type") == "integration":
        integration_id, _, action = slug[1:].partition("/")
        config.setdefault("integration_id", integration_id)
        if action:
            config.setdefault("action", action)
    elif reference.get("type") == "object":
        type_id = slug[1:].split("/", 1)[0]
        config.setdefault("type_id", type_id)
        if "/" in slug:
            config.setdefault("field_name", slug.split("/", 1)[1])
    elif reference.get("type") == "agent":
        config.setdefault("route", infer_agent_route(slug, reference, route_hints=route_hints, route_hints_cache=route_hints_cache))
    return config


def _resolved_to(ref_type: str, slug: str, entry: dict[str, Any]) -> str:
    resolved_table = _as_text(entry.get("resolved_table"))
    resolved_id = _as_text(entry.get("resolved_id"))
    if ref_type == "integration":
        _, _, action = slug[1:].partition("/")
        if action:
            return f"{resolved_table}:{resolved_id}/{action}"
    return f"{resolved_table}:{resolved_id}" if resolved_id else f"{resolved_table}:{slug}"


# ---------------------------------------------------------------------------
# Span / job helpers
# ---------------------------------------------------------------------------

def sentence_for_span(text: str, span: Any) -> str:
    if not isinstance(span, list) or len(span) != 2:
        return text.strip()
    start = max(0, int(span[0]))
    end = min(len(text), int(span[1]))
    left = max(text.rfind(".", 0, start), text.rfind("\n", 0, start))
    right_candidates = [index for index in (text.find(".", end), text.find("\n", end)) if index != -1]
    right = min(right_candidates) if right_candidates else len(text)
    fragment = text[left + 1 : right].strip()
    return fragment or text.strip()


def unique_job_label(slug: str, *, existing: list[dict[str, Any]]) -> str:
    base = _slugify(slug.replace("-agent", "")) or "job"
    used = {job.get("label") for job in existing}
    if base not in used:
        return base
    index = 2
    while f"{base}-{index}" in used:
        index += 1
    return f"{base}-{index}"


def _overlaps(span: tuple[int, int], occupied: list[tuple[int, int]]) -> bool:
    start, end = span
    for existing_start, existing_end in occupied:
        if start < existing_end and end > existing_start:
            return True
    return False


def workflow_id_for_title(title: str) -> str:
    slug = _slugify(title).replace("/", "-")
    if slug:
        return slug[:40]
    return f"workflow-{uuid.uuid4().hex[:8]}"
