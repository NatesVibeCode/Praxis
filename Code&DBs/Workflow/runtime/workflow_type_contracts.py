"""Typed workflow graph helpers.

This is the first narrow contract layer for graph-authoring legality: a node
or catalog item declares the state types it consumes and produces, and Moon can
ask which next actions are actually possible from the accumulated graph state.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
import re
from typing import Any


_CAMEL_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_NON_TYPE_CHARS_RE = re.compile(r"[^a-z0-9_]+")

_TRIGGER_ROUTES = frozenset({"trigger", "trigger/schedule", "trigger/webhook"})

_ROUTE_CONTRACTS: tuple[tuple[tuple[str, ...], dict[str, tuple[str, ...]]], ...] = (
    (
        ("trigger/webhook",),
        {
            "consumes": (),
            "consumes_any": (),
            "produces": ("input_text", "trigger_event", "webhook_payload"),
        },
    ),
    (
        ("trigger/schedule",),
        {
            "consumes": (),
            "consumes_any": (),
            "produces": ("input_text", "trigger_event", "schedule_tick"),
        },
    ),
    (
        ("trigger",),
        {
            "consumes": (),
            "consumes_any": (),
            "produces": ("input_text", "trigger_event"),
        },
    ),
    (
        ("research", "search", "gather", "docs"),
        {
            "consumes": (),
            "consumes_any": ("input_text", "validated_input"),
            "produces": ("research_findings", "evidence_pack"),
        },
    ),
    (
        ("analyze", "analysis", "classify", "score", "triage", "categor"),
        {
            "consumes": (),
            "consumes_any": (
                "research_findings",
                "evidence_pack",
                "validated_input",
                "draft",
                "input_text",
            ),
            "produces": ("analysis_result",),
        },
    ),
    (
        ("draft", "write", "compose", "creative", "summarize", "summary"),
        {
            "consumes": (),
            "consumes_any": (
                "analysis_result",
                "research_findings",
                "evidence_pack",
                "validated_input",
                "summary",
                "input_text",
            ),
            "produces": ("draft", "summary"),
        },
    ),
    (
        ("review", "check", "audit"),
        {
            "consumes": (),
            "consumes_any": (
                "code_change",
                "diff",
                "draft",
                "analysis_result",
                "research_findings",
                "evidence_pack",
            ),
            "produces": ("review_result",),
        },
    ),
    (
        ("build", "implement", "develop", "code", "edit", "refactor", "stage", "execute"),
        {
            "consumes": (),
            "consumes_any": (
                "research_findings",
                "evidence_pack",
                "analysis_result",
                "draft",
                "architecture_plan",
                "validated_input",
                "input_text",
                "diagnosis",
                "review_result",
            ),
            "produces": ("code_change", "diff", "execution_receipt"),
        },
    ),
    (
        ("debug", "diagnose", "failure", "bug"),
        {
            "consumes": (),
            "consumes_any": ("failure", "error", "evidence_pack", "research_findings", "input_text"),
            "produces": ("diagnosis",),
        },
    ),
    (
        ("architecture", "architect", "design", "plan"),
        {
            "consumes": (),
            "consumes_any": ("requirements", "analysis_result", "research_findings", "input_text"),
            "produces": ("architecture_plan",),
        },
    ),
    (
        ("fanout", "fan-out", "loop", "foreach", "map"),
        {
            "consumes": (),
            "consumes_any": ("item_list", "input_text", "research_findings"),
            "produces": ("parallel_results",),
        },
    ),
    (
        ("notify", "notification", "send", "github", "issue", "webhook", "request", "invoke"),
        {
            "consumes": (),
            "consumes_any": (
                "draft",
                "summary",
                "analysis_result",
                "research_findings",
                "evidence_pack",
                "execution_receipt",
                "input_text",
            ),
            "produces": ("action_receipt", "notification_status"),
        },
    ),
)

_TITLE_OUTPUT_HINTS: tuple[tuple[tuple[str, ...], tuple[str, ...]], ...] = (
    (("validate", "check input"), ("validated_input",)),
    (("search", "research", "find", "gather", "docs"), ("research_findings", "evidence_pack")),
    (("analyze", "analysis", "classify", "score", "triage"), ("analysis_result",)),
    (("summarize", "summary"), ("summary",)),
    (("draft", "write", "compose"), ("draft",)),
    (("receipt", "record", "audit", "persist"), ("execution_receipt",)),
    (("notify", "send", "issue", "webhook"), ("action_receipt", "notification_status")),
)


def normalize_type_name(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = _CAMEL_BOUNDARY_RE.sub("_", text)
    text = text.replace("-", "_").replace(".", "_").replace("/", "_")
    text = _NON_TYPE_CHARS_RE.sub("_", text.lower())
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def _string_list(value: object) -> list[str]:
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, (list, tuple, set)):
        return []
    out: list[str] = []
    for item in value:
        normalized = normalize_type_name(item)
        if normalized and normalized not in out:
            out.append(normalized)
    return out


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _node_get(node: object, key: str, default: Any = None) -> Any:
    if isinstance(node, Mapping):
        return node.get(key, default)
    return getattr(node, key, default)


def _node_text(node: object, *keys: str) -> str:
    parts: list[str] = []
    for key in keys:
        value = _node_get(node, key)
        if value not in (None, ""):
            parts.append(str(value))
    return " ".join(parts).lower()


def _contract_from_mapping(value: object) -> dict[str, list[str]]:
    raw = _mapping(value)
    contract = _mapping(raw.get("type_contract") or raw.get("contract") or raw.get("metadata"))
    if "type_contract" in contract:
        contract = _mapping(contract.get("type_contract"))

    consumes = _string_list(
        raw.get("consumes")
        or raw.get("required_inputs")
        or raw.get("input_types")
        or contract.get("consumes")
        or contract.get("required_inputs")
        or contract.get("input_types")
    )
    consumes_any = _string_list(
        raw.get("consumes_any")
        or contract.get("consumes_any")
        or contract.get("optional_consumes")
    )
    produces = _string_list(
        raw.get("produces")
        or raw.get("outputs")
        or raw.get("output_types")
        or contract.get("produces")
        or contract.get("outputs")
        or contract.get("output_types")
    )
    return {
        "consumes": consumes,
        "consumes_any": consumes_any,
        "produces": produces,
    }


def _inferred_contract(value: Mapping[str, Any]) -> dict[str, list[str]]:
    searchable = " ".join(
        str(value.get(key) or "")
        for key in (
            "route",
            "capability_slug",
            "slug",
            "capability_kind",
            "kind",
            "title",
            "label",
            "summary",
            "description",
        )
    ).lower()
    for tokens, contract in _ROUTE_CONTRACTS:
        if any(token in searchable for token in tokens):
            return {
                "consumes": list(contract["consumes"]),
                "consumes_any": list(contract["consumes_any"]),
                "produces": list(contract["produces"]),
            }
    return {"consumes": (), "consumes_any": (), "produces": ("result",)}


def capability_type_contract(capability: Mapping[str, Any]) -> dict[str, list[str]]:
    declared = _contract_from_mapping(capability)
    inferred = _inferred_contract(capability)
    has_declared_inputs = bool(declared["consumes"] or declared["consumes_any"])
    return {
        "consumes": declared["consumes"] or ([] if has_declared_inputs else list(inferred["consumes"])),
        "consumes_any": declared["consumes_any"] or ([] if has_declared_inputs else list(inferred["consumes_any"])),
        "produces": declared["produces"] or list(inferred["produces"]),
    }


def route_type_contract(
    route: str,
    *,
    title: str | None = None,
    summary: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, list[str]]:
    """Return the typed contract for an agent route.

    Used by the surface compiler (compiler_output_builders.make_execution_phase
    and friends) to attach typed ``consumes`` / ``consumes_any`` / ``produces``
    to phases that previously only carried the route literal (``auto/build``,
    ``auto/review``, etc). Without this, downstream graph nodes inherit no
    type contract and Moon Composer cannot validate type-flow before commit
    (BUG-C6EE740C / BUG-5DD67C2A / BUG-99B9DC7E / BUG-2729F8B7).

    Resolution: ``extra`` overrides take precedence (declared contract from
    the catalog), otherwise the route + title + summary are searched against
    ``_ROUTE_CONTRACTS`` for inferred coverage. Falls back to
    ``produces=("result",)`` so unresolved routes still emit a typed shape.
    """
    payload: dict[str, Any] = {"route": route}
    if title:
        payload["title"] = title
    if summary:
        payload["summary"] = summary
    if extra:
        payload.update(dict(extra))
    return capability_type_contract(payload)


def node_produced_types(node: object) -> list[str]:
    declared = _contract_from_mapping(_mapping(node) if isinstance(node, Mapping) else {
        "outputs": _node_get(node, "outputs"),
        "produces": _node_get(node, "produces"),
        "expected_outputs": _node_get(node, "expected_outputs"),
        "type_contract": _node_get(node, "type_contract"),
    })
    outputs = list(declared["produces"])

    expected_outputs = _node_get(node, "expected_outputs")
    if isinstance(expected_outputs, Mapping):
        for key in expected_outputs:
            normalized = normalize_type_name(key)
            if normalized and normalized not in outputs:
                outputs.append(normalized)

    if outputs:
        return outputs

    route = str(_node_get(node, "route") or "").strip().lower()
    if route in _TRIGGER_ROUTES:
        return list(_inferred_contract({"route": route})["produces"])

    searchable = _node_text(node, "route", "title", "display_name", "summary", "prompt")
    for tokens, inferred_outputs in _TITLE_OUTPUT_HINTS:
        if any(token in searchable for token in tokens):
            return list(inferred_outputs)
    return []


def node_required_types(node: object) -> list[str]:
    declared = _contract_from_mapping(_mapping(node) if isinstance(node, Mapping) else {
        "required_inputs": _node_get(node, "required_inputs"),
        "inputs": _node_get(node, "inputs"),
        "consumes": _node_get(node, "consumes"),
        "type_contract": _node_get(node, "type_contract"),
    })
    required = list(declared["consumes"])
    inputs = _mapping(_node_get(node, "inputs"))
    for field_name in ("required_inputs", "consumes", "input_types"):
        for input_type in _string_list(inputs.get(field_name)):
            if input_type not in required:
                required.append(input_type)
    return required


def selected_accumulated_types(
    *,
    nodes: list[Mapping[str, Any]],
    edges: list[Mapping[str, Any]],
    selected_node_id: str | None,
) -> dict[str, Any]:
    node_by_id = {
        str(node.get("node_id") or node.get("id") or ""): node
        for node in nodes
        if str(node.get("node_id") or node.get("id") or "").strip()
    }
    if not selected_node_id or selected_node_id not in node_by_id:
        selected_node_id = next(reversed(node_by_id), None)

    parents: dict[str, set[str]] = {}
    for edge in edges:
        from_id = str(edge.get("from_node_id") or edge.get("from") or "").strip()
        to_id = str(edge.get("to_node_id") or edge.get("to") or "").strip()
        if from_id and to_id:
            parents.setdefault(to_id, set()).add(from_id)

    reachable: set[str] = set()
    if selected_node_id:
        queue: deque[str] = deque([selected_node_id])
        while queue:
            node_id = queue.popleft()
            if node_id in reachable:
                continue
            reachable.add(node_id)
            queue.extend(sorted(parents.get(node_id, ())))
    else:
        reachable.update(node_by_id)

    available: list[str] = []
    producers: dict[str, list[str]] = {}
    for node_id, node in node_by_id.items():
        if node_id not in reachable:
            continue
        produced = node_produced_types(node)
        if produced:
            producers[node_id] = produced
        for produced_type in produced:
            if produced_type not in available:
                available.append(produced_type)

    return {
        "selected_node_id": selected_node_id,
        "source_node_ids": sorted(reachable),
        "available_types": sorted(available),
        "producers": producers,
    }


def type_contract_satisfaction(
    available_types: list[str],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    available = set(_string_list(available_types))
    consumes = _string_list(contract.get("consumes"))
    consumes_any = _string_list(contract.get("consumes_any"))
    missing = sorted(item for item in consumes if item not in available)
    any_satisfied = not consumes_any or bool(available.intersection(consumes_any))
    legal = not missing and any_satisfied
    return {
        "legal": legal,
        "missing": missing,
        "missing_any_of": [] if any_satisfied else sorted(consumes_any),
        "satisfied": sorted((set(consumes) | set(consumes_any)).intersection(available)),
    }


def validate_workflow_request_type_flow(request: object) -> list[str]:
    nodes = list(_node_get(request, "nodes", ()) or ())
    edges = list(_node_get(request, "edges", ()) or ())
    node_by_id = {str(_node_get(node, "node_id") or ""): node for node in nodes}
    produced_by_node = {node_id: node_produced_types(node) for node_id, node in node_by_id.items()}
    parents: dict[str, set[str]] = {}
    for edge in edges:
        from_id = str(_node_get(edge, "from_node_id") or "").strip()
        to_id = str(_node_get(edge, "to_node_id") or "").strip()
        if from_id and to_id:
            parents.setdefault(to_id, set()).add(from_id)

    errors: list[str] = []
    ambient_inputs = {"input_text", "user_message", "system_message", "prompt"}
    for node_id, node in node_by_id.items():
        required = set(node_required_types(node))
        if not required:
            continue
        available = set(ambient_inputs)
        queue: deque[str] = deque(sorted(parents.get(node_id, ())))
        seen: set[str] = set()
        while queue:
            parent_id = queue.popleft()
            if parent_id in seen:
                continue
            seen.add(parent_id)
            available.update(produced_by_node.get(parent_id, ()))
            queue.extend(sorted(parents.get(parent_id, ())))
        missing = sorted(required - available)
        if missing:
            errors.append(
                f"workflow.type_flow.unsatisfied_inputs:{node_id}:"
                + ",".join(missing)
            )
    return errors


__all__ = [
    "capability_type_contract",
    "node_produced_types",
    "node_required_types",
    "normalize_type_name",
    "selected_accumulated_types",
    "type_contract_satisfaction",
    "validate_workflow_request_type_flow",
]
