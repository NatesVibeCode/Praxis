"""Shared compile kernel for graph-backed operating-model definitions."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

_INLINE_NUMBERED_ITEM_RE = re.compile(r"(?<![A-Za-z0-9])(?P<number>\d{1,4})[.):-]\s+")
_SEQUENTIAL_CLAUSE_RE = re.compile(
    r"(?i)\b(?:i|we|you|they)\s+(?:will\s+)?need\s+to\b|\bthen\s+(?:we\s+need\s+to|that\s+needs?\s+to)\b"
)


def build_definition(
    *,
    source_prose: str,
    compiled_prose: str,
    references: list[dict[str, Any]],
    capabilities: list[dict[str, Any]],
    authority: str,
    sla: dict[str, Any],
) -> dict[str, Any]:
    narrative_blocks = build_narrative_blocks(compiled_prose, references, capabilities)
    trigger_intent = build_trigger_intent(compiled_prose, references, narrative_blocks)
    draft_flow = build_draft_flow(narrative_blocks)
    normalized_capabilities = attach_capability_flow_indexes(capabilities, draft_flow)
    definition_graph = build_definition_graph(
        source_prose=source_prose,
        compiled_prose=compiled_prose,
        references=references,
        capabilities=normalized_capabilities,
        authority=authority,
        sla=sla,
        narrative_blocks=narrative_blocks,
        trigger_intent=trigger_intent,
        draft_flow=draft_flow,
    )
    definition = {
        "type": "operating_model",
        "definition_graph": definition_graph,
        "source_prose": compiled_source_prose(definition_graph, fallback=source_prose),
        "compiled_prose": compiled_prose_projection(definition_graph, fallback=compiled_prose),
        "narrative_blocks": narrative_blocks_projection(definition_graph),
        "references": references_projection(definition_graph, fallback=references),
        "capabilities": capabilities_projection(definition_graph, fallback=normalized_capabilities),
        "authority": authority,
        "sla": sla,
        "trigger_intent": trigger_intent_projection(definition_graph),
        "draft_flow": draft_flow_projection(definition_graph),
    }
    definition["definition_revision"] = definition_revision(definition)
    return definition


def materialize_definition(definition: dict[str, Any]) -> dict[str, Any]:
    materialized = json.loads(json.dumps(definition, default=str)) if isinstance(definition, dict) else {}
    source_prose = as_text(materialized.get("source_prose"))
    compiled_prose = as_text(materialized.get("compiled_prose"))
    references = materialized.get("references") if isinstance(materialized.get("references"), list) else []
    capabilities = materialized.get("capabilities") if isinstance(materialized.get("capabilities"), list) else []
    trigger_intent = materialized.get("trigger_intent") if isinstance(materialized.get("trigger_intent"), list) else []
    narrative_blocks = (
        materialized.get("narrative_blocks") if isinstance(materialized.get("narrative_blocks"), list) else []
    )
    draft_flow = materialized.get("draft_flow") if isinstance(materialized.get("draft_flow"), list) else []
    authority = as_text(materialized.get("authority"))
    sla = materialized.get("sla") if isinstance(materialized.get("sla"), dict) else {}

    definition_graph = materialized.get("definition_graph")
    if not isinstance(definition_graph, dict):
        effective_narrative_blocks = narrative_blocks or build_narrative_blocks(compiled_prose, references, capabilities)
        effective_trigger_intent = trigger_intent or build_trigger_intent(
            compiled_prose,
            references,
            effective_narrative_blocks,
        )
        effective_draft_flow = draft_flow or build_draft_flow(effective_narrative_blocks)
        definition_graph = build_definition_graph(
            source_prose=source_prose,
            compiled_prose=compiled_prose,
            references=references,
            capabilities=capabilities,
            authority=authority,
            sla=sla,
            narrative_blocks=effective_narrative_blocks,
            trigger_intent=effective_trigger_intent,
            draft_flow=effective_draft_flow,
        )
    materialized["definition_graph"] = definition_graph
    materialized["source_prose"] = compiled_source_prose(definition_graph, fallback=source_prose)
    materialized["compiled_prose"] = compiled_prose_projection(definition_graph, fallback=compiled_prose)
    materialized["references"] = references_projection(definition_graph, fallback=references)
    materialized["narrative_blocks"] = narrative_blocks_projection(definition_graph, fallback=narrative_blocks)
    materialized["capabilities"] = capabilities_projection(definition_graph, fallback=capabilities)
    materialized["trigger_intent"] = trigger_intent_projection(definition_graph, fallback=trigger_intent)
    materialized["draft_flow"] = draft_flow_projection(definition_graph, fallback=draft_flow)
    materialized.setdefault("authority", authority)
    materialized.setdefault("sla", sla)
    materialized.setdefault("type", "operating_model")
    materialized.setdefault("definition_revision", definition_revision({k: v for k, v in materialized.items() if k != "definition_revision"}))
    return materialized


def build_definition_graph(
    *,
    source_prose: str,
    compiled_prose: str,
    references: list[dict[str, Any]],
    capabilities: list[dict[str, Any]],
    authority: str,
    sla: dict[str, Any],
    narrative_blocks: list[dict[str, Any]],
    trigger_intent: list[dict[str, Any]],
    draft_flow: list[dict[str, Any]],
) -> dict[str, Any]:
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []

    for index, reference in enumerate(references, start=1):
        slug = as_text(reference.get("slug"))
        if not slug:
            continue
        nodes.append(
            {
                "id": f"reference:{slug}",
                "kind": "reference",
                "order": index,
                "payload": json.loads(json.dumps(reference, default=str)),
            }
        )

    for index, capability in enumerate(capabilities, start=1):
        slug = as_text(capability.get("slug"))
        if not slug:
            continue
        nodes.append(
            {
                "id": f"capability:{slug}",
                "kind": "capability",
                "order": index,
                "payload": json.loads(json.dumps(capability, default=str)),
            }
        )

    for block in narrative_blocks:
        block_id = as_text(block.get("id"))
        if not block_id:
            continue
        nodes.append(
            {
                "id": block_id,
                "kind": "narrative_block",
                "order": int(block.get("order") or 0),
                "payload": json.loads(json.dumps(block, default=str)),
            }
        )
        for slug in as_string_list(block.get("reference_slugs")):
            edges.append({"from": block_id, "to": f"reference:{slug}", "kind": "mentions_reference"})
        for slug in as_string_list(block.get("capability_slugs")):
            edges.append({"from": block_id, "to": f"capability:{slug}", "kind": "uses_capability"})

    for trigger in trigger_intent:
        trigger_id = as_text(trigger.get("id"))
        if not trigger_id:
            continue
        nodes.append(
            {
                "id": trigger_id,
                "kind": "trigger",
                "order": len(nodes) + 1,
                "payload": json.loads(json.dumps(trigger, default=str)),
            }
        )
        source_ref = as_text(trigger.get("source_ref"))
        if source_ref:
            edges.append({"from": trigger_id, "to": f"reference:{source_ref}", "kind": "source_reference"})
        for block_id in as_string_list(trigger.get("source_block_ids")):
            edges.append({"from": trigger_id, "to": block_id, "kind": "source_block"})

    for step in draft_flow:
        step_id = as_text(step.get("id"))
        if not step_id:
            continue
        nodes.append(
            {
                "id": step_id,
                "kind": "draft_step",
                "order": int(step.get("order") or 0),
                "payload": json.loads(json.dumps(step, default=str)),
            }
        )
        for block_id in as_string_list(step.get("source_block_ids")):
            edges.append({"from": step_id, "to": block_id, "kind": "derived_from_block"})
        for dependency in as_string_list(step.get("depends_on")):
            edges.append({"from": step_id, "to": dependency, "kind": "depends_on"})
        for slug in as_string_list(step.get("reference_slugs")):
            edges.append({"from": step_id, "to": f"reference:{slug}", "kind": "uses_reference"})
        for slug in as_string_list(step.get("capability_slugs")):
            edges.append({"from": step_id, "to": f"capability:{slug}", "kind": "uses_capability"})

    nodes.sort(key=lambda item: (item.get("kind") or "", int(item.get("order") or 0), item.get("id") or ""))
    edges.sort(key=lambda item: (item.get("kind") or "", item.get("from") or "", item.get("to") or ""))
    return {
        "version": 1,
        "metadata": {
            "source_prose": source_prose,
            "compiled_prose": compiled_prose,
            "authority": authority,
            "sla": json.loads(json.dumps(sla, default=str)),
        },
        "nodes": nodes,
        "edges": edges,
    }


def compiled_source_prose(definition_graph: dict[str, Any], *, fallback: str = "") -> str:
    metadata = definition_graph.get("metadata") if isinstance(definition_graph, dict) else {}
    if isinstance(metadata, dict):
        value = as_text(metadata.get("source_prose"))
        if value:
            return value
    return fallback


def compiled_prose_projection(definition_graph: dict[str, Any], *, fallback: str = "") -> str:
    metadata = definition_graph.get("metadata") if isinstance(definition_graph, dict) else {}
    if isinstance(metadata, dict):
        value = as_text(metadata.get("compiled_prose"))
        if value:
            return value
    fallback_blocks = narrative_blocks_projection(definition_graph)
    if fallback_blocks:
        text = " ".join(as_text(block.get("text")) for block in fallback_blocks if as_text(block.get("text")))
        if text:
            return text
    return fallback


def references_projection(
    definition_graph: dict[str, Any],
    *,
    fallback: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    projected = _node_payloads(definition_graph, "reference")
    return projected or list(fallback or [])


def capabilities_projection(
    definition_graph: dict[str, Any],
    *,
    fallback: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    projected = _node_payloads(definition_graph, "capability")
    return projected or list(fallback or [])


def narrative_blocks_projection(
    definition_graph: dict[str, Any],
    *,
    fallback: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    projected = _node_payloads(definition_graph, "narrative_block")
    return projected or list(fallback or [])


def trigger_intent_projection(
    definition_graph: dict[str, Any],
    *,
    fallback: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    projected = _node_payloads(definition_graph, "trigger")
    return projected or list(fallback or [])


def draft_flow_projection(
    definition_graph: dict[str, Any],
    *,
    fallback: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    projected = _node_payloads(definition_graph, "draft_step")
    return projected or list(fallback or [])


def detect_triggers(compiled_prose: str, references: list[dict[str, Any]]) -> list[dict[str, Any]]:
    triggers: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    sentences = split_sentences(compiled_prose)
    integration_refs = [ref for ref in references if ref.get("type") == "integration"]
    object_refs = [ref for ref in references if ref.get("type") == "object"]

    for sentence, start, end in sentences:
        lower = sentence.lower()
        trigger: dict[str, Any] | None = None

        if any(token in lower for token in ("on schedule", "every hour", "every day", "daily", "hourly", "weekly", "nightly")):
            trigger = {
                "event_type": "schedule",
                "cron_expression": cron_for_sentence(lower),
                "filter": {"text": sentence},
            }
        elif "when " in lower or lower.startswith("on "):
            source_ref = first_reference_in_window(integration_refs, start, end)
            object_ref = first_reference_in_window(object_refs, start, end)
            if source_ref is not None:
                trigger = {
                    "event_type": "integration.event",
                    "source_ref": source_ref["slug"],
                    "filter": {"text": sentence},
                }
            elif object_ref is not None and any(word in lower for word in ("created", "updated", "deleted", "changes", "happens")):
                trigger = {
                    "event_type": "object.event",
                    "source_ref": object_ref["slug"],
                    "filter": {"text": sentence},
                }

        if trigger is None:
            continue

        key = (
            as_text(trigger.get("event_type")),
            as_text(trigger.get("source_ref")),
            as_text(trigger.get("cron_expression")),
        )
        if key in seen:
            continue
        seen.add(key)
        triggers.append(trigger)

    return triggers


def build_narrative_blocks(
    compiled_prose: str,
    references: list[dict[str, Any]],
    capabilities: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    sentences = split_sentences(compiled_prose)
    if not sentences and compiled_prose.strip():
        stripped = compiled_prose.strip()
        start = compiled_prose.find(stripped)
        sentences = [(stripped, max(start, 0), max(start, 0) + len(stripped))]

    for index, (sentence, start, end) in enumerate(sentences, start=1):
        reference_slugs = reference_slugs_for_window(references, start, end)
        capability_slugs = [
            as_text(capability.get("slug"))
            for capability in capabilities
            if capability_matches_block(capability, sentence, reference_slugs)
        ]
        blocks.append(
            {
                "id": f"block-{index:03d}",
                "title": titleize_fragment(sentence, fallback=f"Block {index}"),
                "summary": sentence,
                "text": sentence,
                "span": [start, end],
                "order": index,
                "reference_slugs": reference_slugs,
                "capability_slugs": [slug for slug in capability_slugs if slug],
            }
        )
    return blocks


def build_trigger_intent(
    compiled_prose: str,
    references: list[dict[str, Any]],
    narrative_blocks: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    trigger_intent: list[dict[str, Any]] = []
    for index, trigger in enumerate(detect_triggers(compiled_prose, references), start=1):
        filter_dict = trigger.get("filter") if isinstance(trigger.get("filter"), dict) else {}
        summary = as_text(filter_dict.get("text")) or trigger_summary(trigger)
        source_ref = as_text(trigger.get("source_ref"))
        source_block_ids = block_ids_for_trigger(summary, source_ref, narrative_blocks)
        reference_slugs = [source_ref] if source_ref else []
        trigger_payload = {
            "id": f"trigger-{index:03d}",
            "title": titleize_fragment(summary, fallback=f"Trigger {index}"),
            "summary": summary,
            "event_type": as_text(trigger.get("event_type")) or "manual",
            "filter": filter_dict,
            "source_block_ids": source_block_ids,
            "reference_slugs": reference_slugs,
        }
        if source_ref:
            trigger_payload["source_ref"] = source_ref
        cron_expression = as_text(trigger.get("cron_expression"))
        if cron_expression:
            trigger_payload["cron_expression"] = cron_expression
        trigger_intent.append(trigger_payload)
    return trigger_intent


def build_draft_flow(narrative_blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    draft_flow: list[dict[str, Any]] = []
    previous_id: str | None = None
    for index, block in enumerate(narrative_blocks, start=1):
        step_id = f"step-{index:03d}"
        draft_flow.append(
            {
                "id": step_id,
                "title": titleize_fragment(block.get("title"), fallback=f"Step {index}"),
                "summary": as_text(block.get("summary")) or as_text(block.get("text")),
                "source_block_ids": [as_text(block.get("id"))] if as_text(block.get("id")) else [],
                "reference_slugs": as_string_list(block.get("reference_slugs")),
                "capability_slugs": as_string_list(block.get("capability_slugs")),
                "depends_on": [previous_id] if previous_id else [],
                "order": index,
            }
        )
        previous_id = step_id
    return draft_flow


def attach_capability_flow_indexes(
    capabilities: list[dict[str, Any]],
    draft_flow: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for capability in capabilities:
        capability_copy = dict(capability)
        matching_orders = [
            int(step.get("order"))
            for step in draft_flow
            if as_text(capability.get("slug")) in as_string_list(step.get("capability_slugs"))
            or bool(set(as_string_list(capability.get("reference_slugs"))) & set(as_string_list(step.get("reference_slugs"))))
        ]
        capability_copy["step_indexes"] = matching_orders
        normalized.append(capability_copy)
    return normalized


def reference_slugs_for_window(references: list[dict[str, Any]], start: int, end: int) -> list[str]:
    slugs: list[str] = []
    for reference in references:
        span = reference.get("span")
        if not isinstance(span, list) or len(span) != 2:
            continue
        ref_start = int(span[0])
        ref_end = int(span[1])
        if ref_start < end and ref_end > start:
            slug = as_text(reference.get("slug"))
            if slug and slug not in slugs:
                slugs.append(slug)
    return slugs


def capability_matches_block(capability: dict[str, Any], sentence: str, reference_slugs: list[str]) -> bool:
    block_refs = {slug.lower() for slug in reference_slugs}
    capability_refs = {slug.lower() for slug in as_string_list(capability.get("reference_slugs")) if slug}
    if capability_refs & block_refs:
        return True

    haystack = sentence.lower()
    route = as_text(capability.get("route")).lower()
    if route and route in haystack:
        return True

    for signal in capability.get("signals", []) if isinstance(capability.get("signals"), list) else []:
        token = as_text(signal).lower()
        if token and token in haystack:
            return True
    return False


def block_ids_for_trigger(summary: str, source_ref: str, narrative_blocks: list[dict[str, Any]]) -> list[str]:
    block_ids: list[str] = []
    lowered_summary = summary.lower()
    for block in narrative_blocks:
        block_id = as_text(block.get("id"))
        if not block_id:
            continue
        if source_ref and source_ref in as_string_list(block.get("reference_slugs")):
            block_ids.append(block_id)
            continue
        text = as_text(block.get("text")).lower()
        if lowered_summary and lowered_summary == text:
            block_ids.append(block_id)
    return block_ids


def trigger_summary(trigger: dict[str, Any]) -> str:
    event_type = as_text(trigger.get("event_type")) or "manual"
    source_ref = as_text(trigger.get("source_ref"))
    if source_ref:
        return f"{event_type} from {source_ref}"
    return event_type


def definition_revision(definition: dict[str, Any]) -> str:
    payload = json.dumps(definition, sort_keys=True, separators=(",", ":"), default=str)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    return f"def_{digest}"


def titleize_fragment(value: Any, *, fallback: str) -> str:
    text = as_text(value)
    if not text:
        return fallback
    collapsed = re.sub(r"\s+", " ", text).strip()
    return collapsed.strip(" -:;,.") or fallback


def split_sentences(text: str) -> list[tuple[str, int, int]]:
    enumerated = split_inline_numbered_items(text)
    if enumerated:
        return enumerated

    sequenced = split_sequential_clauses(text)
    if sequenced:
        return sequenced

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


def split_sequential_clauses(text: str) -> list[tuple[str, int, int]]:
    matches = list(_SEQUENTIAL_CLAUSE_RE.finditer(text))
    if len(matches) < 2:
        return []

    starts = [0, *[match.start() for match in matches]]
    clauses: list[tuple[str, int, int]] = []
    seen_bounds: set[tuple[int, int]] = set()

    for index, raw_start in enumerate(starts):
        raw_end = starts[index + 1] if index + 1 < len(starts) else len(text)
        fragment = text[raw_start:raw_end]
        item = fragment.strip(" \t\r\n,;")
        if not item:
            continue
        item_start = text.find(item, raw_start, raw_end)
        if item_start < 0:
            item_start = raw_start
        bounds = (item_start, item_start + len(item))
        if bounds in seen_bounds:
            continue
        seen_bounds.add(bounds)
        clauses.append((item, *bounds))

    return clauses if len(clauses) >= 2 else []


def split_inline_numbered_items(text: str) -> list[tuple[str, int, int]]:
    matches = list(_INLINE_NUMBERED_ITEM_RE.finditer(text))
    if len(matches) < 2:
        return []

    items: list[tuple[str, int, int]] = []
    for index, match in enumerate(matches):
        content_start = match.end()
        content_end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        raw_item = text[content_start:content_end]
        item = raw_item.strip(" \t\r\n,;")
        if not item:
            continue
        item_start = text.find(item, content_start, content_end)
        if item_start < 0:
            item_start = content_start
        items.append((item, item_start, item_start + len(item)))

    return items if len(items) >= 2 else []


def first_reference_in_window(references: list[dict[str, Any]], start: int, end: int) -> dict[str, Any] | None:
    for reference in references:
        span = reference.get("span")
        if not isinstance(span, list) or len(span) != 2:
            continue
        if int(span[0]) >= start and int(span[1]) <= end:
            return reference
    return None


def cron_for_sentence(lowered_sentence: str) -> str:
    if "hourly" in lowered_sentence or "every hour" in lowered_sentence:
        return "0 * * * *"
    if "weekly" in lowered_sentence:
        return "0 9 * * 1"
    if "nightly" in lowered_sentence:
        return "0 2 * * *"
    return "0 9 * * *"


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def as_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [as_text(item) for item in value if as_text(item)]


def _node_payloads(definition_graph: dict[str, Any], kind: str) -> list[dict[str, Any]]:
    nodes = definition_graph.get("nodes") if isinstance(definition_graph, dict) else []
    projected: list[tuple[int, str, dict[str, Any]]] = []
    for node in nodes if isinstance(nodes, list) else []:
        if not isinstance(node, dict) or as_text(node.get("kind")) != kind:
            continue
        payload = node.get("payload") if isinstance(node.get("payload"), dict) else {}
        projected.append((int(node.get("order") or payload.get("order") or 0), as_text(node.get("id")), payload))
    projected.sort(key=lambda item: (item[0], item[1]))
    return [json.loads(json.dumps(item[2], default=str)) for item in projected]


__all__ = [
    "attach_capability_flow_indexes",
    "build_definition",
    "build_definition_graph",
    "build_draft_flow",
    "build_narrative_blocks",
    "build_trigger_intent",
    "capabilities_projection",
    "compiled_prose_projection",
    "definition_revision",
    "detect_triggers",
    "draft_flow_projection",
    "materialize_definition",
    "narrative_blocks_projection",
    "references_projection",
    "trigger_intent_projection",
]
