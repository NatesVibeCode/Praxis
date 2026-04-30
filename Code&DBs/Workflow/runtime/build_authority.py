"""Build authority helpers for the graph-first operating-model workspace."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from typing import Any

from runtime.definition_compile_kernel import definition_revision, materialize_definition
from runtime.edge_release import normalize_edge_release, with_edge_release

_SLUG_RE = re.compile(r"[^a-z0-9]+")
_TRIGGER_MANUAL_ROUTE = "trigger"
_TRIGGER_SCHEDULE_ROUTE = "trigger/schedule"
_TRIGGER_WEBHOOK_ROUTE = "trigger/webhook"
_WEBHOOK_TRIGGER_EVENT_TYPE = "db.webhook_events.insert"
_BLOCKING_INPUT_REPAIR_ACTIONS = [
    "resolve_from_context",
    "add_producer_node",
    "bind_source_authority",
    "remove_requirement",
]

# Honors architecture-policy::compile::retrieval-is-the-filter-no-template-
# fallbacks (2026-04-25). When semantic retrieval over the operator prose
# finds zero capability matches in the catalog, the compiler emits a single
# honest typed gap rather than substituting a canned template.
_RETRIEVAL_NO_MATCH_REPAIR_ACTIONS = [
    "add_integration_reference_to_prose",
    "pick_capability_from_catalog",
    "narrow_workflow_scope",
]


def _json_clone(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def _as_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if value is None:
        return ""
    return str(value).strip()


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_as_text(item) for item in value if _as_text(item)]


def _slugify(value: str) -> str:
    lowered = _as_text(value).lower()
    if not lowered:
        return ""
    return _SLUG_RE.sub("-", lowered).strip("-")


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _stable_digest(prefix: str, payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return f"{prefix}_{hashlib.sha256(encoded.encode('utf-8')).hexdigest()[:16]}"


def _binding_target_key(value: Any) -> str:
    if not isinstance(value, dict):
        return _stable_digest("binding_target", value)
    target_ref = _as_text(value.get("target_ref"))
    if target_ref:
        return f"target_ref:{target_ref}"
    return _stable_digest("binding_target", _json_clone(value))


def _normalize_binding_targets(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, dict):
            continue
        cloned = _json_clone(item)
        key = _binding_target_key(cloned)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(cloned)
    return normalized


def _attachment_id_for(node_id: str, authority_kind: str, authority_ref: str, role: str) -> str:
    return _stable_digest(
        "attachment",
        {
            "node_id": node_id,
            "authority_kind": authority_kind,
            "authority_ref": authority_ref,
            "role": role,
        },
    )


def _snapshot_id_for(
    *,
    node_id: str | None,
    source_kind: str,
    source_locator: str,
    requested_shape: dict[str, Any] | None = None,
) -> str:
    return _stable_digest(
        "import",
        {
            "node_id": node_id,
            "source_kind": source_kind,
            "source_locator": source_locator,
            "requested_shape": requested_shape or {},
        },
    )


def _trigger_route_for_payload(trigger: dict[str, Any]) -> str:
    event_type = _as_text(trigger.get("event_type"))
    if event_type == "schedule":
        return _TRIGGER_SCHEDULE_ROUTE
    if event_type == _WEBHOOK_TRIGGER_EVENT_TYPE:
        return _TRIGGER_WEBHOOK_ROUTE
    return _TRIGGER_MANUAL_ROUTE


def _normalize_existing_binding(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    binding_id = _as_text(value.get("binding_id") or value.get("id"))
    if not binding_id:
        return None
    candidate_targets = value.get("candidate_targets")
    if not isinstance(candidate_targets, list):
        candidate_targets = []
    return {
        "binding_id": binding_id,
        "source_kind": _as_text(value.get("source_kind")) or "reference",
        "source_label": _as_text(value.get("source_label")),
        "source_span": list(value.get("source_span")) if isinstance(value.get("source_span"), list) else None,
        "source_node_ids": _string_list(value.get("source_node_ids")),
        "state": _as_text(value.get("state")) or "captured",
        "candidate_targets": _json_clone(candidate_targets),
        "accepted_target": _json_clone(value.get("accepted_target")) if isinstance(value.get("accepted_target"), dict) else None,
        "rationale": _as_text(value.get("rationale")),
        "created_at": _as_text(value.get("created_at")) or None,
        "updated_at": _as_text(value.get("updated_at")) or None,
        "freshness": _json_clone(value.get("freshness")) if isinstance(value.get("freshness"), dict) else None,
    }


def _normalize_import_snapshot(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    snapshot_id = _as_text(value.get("snapshot_id") or value.get("id"))
    if not snapshot_id:
        return None
    return {
        "snapshot_id": snapshot_id,
        "source_kind": _as_text(value.get("source_kind")) or "net_request",
        "source_locator": _as_text(value.get("source_locator")),
        "requested_shape": _json_clone(value.get("requested_shape")) if isinstance(value.get("requested_shape"), dict) else {},
        "payload": _json_clone(value.get("payload")) if isinstance(value.get("payload"), (dict, list)) else value.get("payload"),
        "freshness_ttl": int(value.get("freshness_ttl") or 3600),
        "captured_at": _as_text(value.get("captured_at")) or None,
        "stale_after_at": _as_text(value.get("stale_after_at")) or None,
        "approval_state": _as_text(value.get("approval_state")) or "staged",
        "admitted_targets": _normalize_binding_targets(value.get("admitted_targets")),
        "binding_id": _as_text(value.get("binding_id")) or None,
        "node_id": _as_text(value.get("node_id")) or None,
    }


def _normalize_attachment(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    attachment_id = _as_text(value.get("attachment_id") or value.get("id"))
    node_id = _as_text(value.get("node_id"))
    if not attachment_id or not node_id:
        return None
    return {
        "attachment_id": attachment_id,
        "node_id": node_id,
        "authority_kind": _as_text(value.get("authority_kind")) or "reference",
        "authority_ref": _as_text(value.get("authority_ref")),
        "role": _as_text(value.get("role")) or "input",
        "label": _as_text(value.get("label")),
        "promote_to_state": bool(value.get("promote_to_state")),
        "state_node_id": _as_text(value.get("state_node_id")) or None,
    }


def _normalize_existing_bindings(definition: dict[str, Any]) -> dict[str, dict[str, Any]]:
    bindings = definition.get("binding_ledger") if isinstance(definition.get("binding_ledger"), list) else []
    normalized: dict[str, dict[str, Any]] = {}
    for entry in bindings:
        normalized_entry = _normalize_existing_binding(entry)
        if normalized_entry is None:
            continue
        normalized[normalized_entry["binding_id"]] = normalized_entry
    return normalized


def _normalize_import_snapshots(definition: dict[str, Any]) -> list[dict[str, Any]]:
    snapshots = definition.get("import_snapshots") if isinstance(definition.get("import_snapshots"), list) else []
    normalized: list[dict[str, Any]] = []
    for entry in snapshots:
        snapshot = _normalize_import_snapshot(entry)
        if snapshot is not None:
            normalized.append(snapshot)
    return normalized


def _normalize_attachments(definition: dict[str, Any]) -> list[dict[str, Any]]:
    attachments = definition.get("authority_attachments") if isinstance(definition.get("authority_attachments"), list) else []
    normalized: list[dict[str, Any]] = []
    for entry in attachments:
        attachment = _normalize_attachment(entry)
        if attachment is not None:
            normalized.append(attachment)
    return normalized


def _compile_derived_binding_entries(definition: dict[str, Any]) -> list[dict[str, Any]]:
    references = definition.get("references") if isinstance(definition.get("references"), list) else []
    draft_flow = definition.get("draft_flow") if isinstance(definition.get("draft_flow"), list) else []
    entries: list[dict[str, Any]] = []
    for reference in references:
        if not isinstance(reference, dict):
            continue
        ref_id = _as_text(reference.get("id"))
        slug = _as_text(reference.get("slug"))
        if not ref_id or not slug:
            continue
        source_nodes = [
            _as_text(step.get("id"))
            for step in draft_flow
            if isinstance(step, dict) and slug in _string_list(step.get("reference_slugs"))
        ]
        resolved_to = _as_text(reference.get("resolved_to"))
        candidate_targets: list[dict[str, Any]] = []
        if resolved_to:
            candidate_targets.append(
                {
                    "target_ref": resolved_to,
                    "label": _as_text(reference.get("display_name")) or resolved_to,
                    "kind": _as_text(reference.get("type")) or "reference",
                }
            )
        else:
            default_target = slug
            if _as_text(reference.get("type")) == "object" and not slug.startswith("#"):
                default_target = f"#{slug}"
            candidate_targets.append(
                {
                    "target_ref": default_target,
                    "label": _as_text(reference.get("raw")) or _as_text(reference.get("display_name")) or slug,
                    "kind": _as_text(reference.get("type")) or "reference",
                }
            )
        entries.append(
            {
                "binding_id": f"binding:{ref_id}",
                "source_kind": "reference",
                "source_label": _as_text(reference.get("raw")) or slug,
                "source_span": list(reference.get("span")) if isinstance(reference.get("span"), list) else None,
                "source_node_ids": source_nodes,
                "state": "suggested" if resolved_to else "captured",
                "candidate_targets": candidate_targets,
                "accepted_target": None,
                "rationale": (
                    "Matched against current compile authority; explicit approval is still required before planning can run cleanly."
                    if resolved_to
                    else "Needs an accepted authority target before planning can run cleanly."
                ),
                "created_at": None,
                "updated_at": None,
                "freshness": None,
            }
        )
    return entries


def _snapshot_freshness(snapshot: dict[str, Any]) -> dict[str, Any] | None:
    stale_after = _as_text(snapshot.get("stale_after_at"))
    captured_at = _as_text(snapshot.get("captured_at"))
    if not stale_after and not captured_at:
        return None
    freshness = {
        "captured_at": captured_at or None,
        "stale_after_at": stale_after or None,
        "state": "fresh",
    }
    if stale_after:
        try:
            cutoff = datetime.fromisoformat(stale_after.replace("Z", "+00:00"))
            freshness["state"] = "stale" if datetime.now(timezone.utc) >= cutoff else "fresh"
        except ValueError:
            freshness["state"] = "unknown"
    return freshness


def _merge_binding_ledger(
    definition: dict[str, Any],
    import_snapshots: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    existing = _normalize_existing_bindings(definition)
    merged: dict[str, dict[str, Any]] = {}
    for entry in _compile_derived_binding_entries(definition):
        binding_id = entry["binding_id"]
        current = existing.get(binding_id)
        if current is None:
            merged[binding_id] = entry
            continue
        merged[binding_id] = {
            **entry,
            **{
                "state": current.get("state") or entry["state"],
                "candidate_targets": _json_clone(current.get("candidate_targets") or entry["candidate_targets"]),
                "accepted_target": _json_clone(current.get("accepted_target")) if isinstance(current.get("accepted_target"), dict) else entry["accepted_target"],
                "rationale": current.get("rationale") or entry["rationale"],
                "created_at": current.get("created_at"),
                "updated_at": current.get("updated_at"),
                "freshness": _json_clone(current.get("freshness")) if isinstance(current.get("freshness"), dict) else entry.get("freshness"),
                "source_node_ids": _string_list(current.get("source_node_ids")) or entry["source_node_ids"],
            },
        }
    for binding_id, entry in existing.items():
        merged.setdefault(binding_id, _json_clone(entry))

    for snapshot in import_snapshots:
        binding_id = _as_text(snapshot.get("binding_id")) or f"binding:import:{snapshot['snapshot_id']}"
        freshness = _snapshot_freshness(snapshot)
        requested_shape = snapshot.get("requested_shape") if isinstance(snapshot.get("requested_shape"), dict) else {}
        suggested_label = _as_text(requested_shape.get("label")) or _as_text(requested_shape.get("name")) or _as_text(snapshot.get("source_locator")) or snapshot["snapshot_id"]
        current_entry = merged.get(binding_id, {})
        candidate_targets = current_entry.get("candidate_targets") or []
        if not candidate_targets:
            candidate_targets = [
                {
                    "target_ref": _as_text(requested_shape.get("target_ref")) or f"#{_slugify(suggested_label) or snapshot['snapshot_id']}",
                    "label": suggested_label,
                    "kind": _as_text(requested_shape.get("kind")) or "type",
                }
            ]
        current_state = _as_text(current_entry.get("state"))
        state = "stale" if freshness and freshness.get("state") == "stale" else (
            current_state if current_state in {"accepted", "rejected"} else "suggested"
        )
        accepted_target = None
        if state == "accepted" and isinstance(current_entry.get("accepted_target"), dict):
            accepted_target = _json_clone(current_entry.get("accepted_target"))
        merged[binding_id] = {
            "binding_id": binding_id,
            "source_kind": "import_request",
            "source_label": suggested_label,
            "source_span": None,
            "source_node_ids": [snapshot["node_id"]] if _as_text(snapshot.get("node_id")) else [],
            "state": state,
            "candidate_targets": _json_clone(candidate_targets),
            "accepted_target": accepted_target,
            "rationale": (
                "Backed by an admitted import snapshot; explicit binding approval is still required."
                if snapshot.get("approval_state") == "admitted" and state != "accepted"
                else (
                    _as_text(current_entry.get("rationale")) or "Accepted in build workspace."
                    if state == "accepted"
                    else "Suggested from staged external evidence."
                )
            ),
            "created_at": current_entry.get("created_at") or snapshot.get("captured_at"),
            "updated_at": current_entry.get("updated_at") or snapshot.get("captured_at"),
            "freshness": freshness,
        }
        snapshot["binding_id"] = binding_id

    ordered = sorted(merged.values(), key=lambda item: item["binding_id"])
    return [_json_clone(entry) for entry in ordered]


def _definition_graph_capability_nodes(definition: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract capability nodes from the kernel-built definition_graph.

    definition_graph is the authoritative semantic-retrieval projection
    (built by runtime/definition_compile_kernel.build_definition_graph).
    Capability nodes carry the prose-grounded catalog matches; the build
    projection reads them directly instead of keyword-filtering a template.
    """
    definition_graph = definition.get("definition_graph") if isinstance(definition, dict) else None
    if not isinstance(definition_graph, dict):
        return []
    nodes = definition_graph.get("nodes")
    if not isinstance(nodes, list):
        return []
    return [node for node in nodes if isinstance(node, dict) and _as_text(node.get("kind")) == "capability"]


def _collect_retrieval_gaps(definition: dict[str, Any]) -> list[dict[str, Any]]:
    """When semantic retrieval finds no capability matches, emit one honest
    typed gap instead of a canned template.

    Honors architecture-policy::compile::retrieval-is-the-filter-no-template-
    fallbacks (2026-04-25). Empty retrieval is a signal: the operator must
    add an @integration reference, pick a capability from the catalog, or
    narrow the scope. Fabricating a scaffold hides that signal.
    """
    definition_graph = definition.get("definition_graph") if isinstance(definition, dict) else None
    if not isinstance(definition_graph, dict):
        # No definition_graph at all — this is a pre-migration bundle; stay
        # silent (preserves backwards compatibility with legacy definitions).
        return []
    compile_provenance = (
        definition.get("compile_provenance")
        if isinstance(definition.get("compile_provenance"), dict)
        else {}
    )
    semantic_retrieval = (
        compile_provenance.get("semantic_retrieval")
        if isinstance(compile_provenance.get("semantic_retrieval"), dict)
        else {}
    )
    if _as_text(semantic_retrieval.get("mode")) != "semantic":
        # A materialized graph is not evidence that semantic retrieval ran.
        # Saved drafts, imported build records, and legacy definitions may get
        # a definition_graph during projection. Only compiler output with an
        # explicit semantic retrieval attempt can honestly report no matches.
        return []
    capability_nodes = _definition_graph_capability_nodes(definition)
    if capability_nodes:
        return []

    ordered_steps = sorted(
        [step for step in definition.get("draft_flow", []) if isinstance(step, dict)],
        key=lambda step: int(step.get("order") or 0),
    )
    default_node_id = _as_text(ordered_steps[0].get("id")) if ordered_steps else None
    if not default_node_id:
        default_node_id = "workflow:root"

    typed_gap = {
        "gap_kind": "retrieval_match",
        "missing_type": "capability_match",
        "reason_code": "retrieval.no_match",
        "legal_repair_actions": _RETRIEVAL_NO_MATCH_REPAIR_ACTIONS,
        "context": {
            "node_id": default_node_id,
            "requirement_ref": "definition_graph.capability_nodes",
        },
    }
    return [
        {
            "issue_id": "issue:typed-gap:retrieval.no_match",
            "kind": "typed_gap",
            "node_id": default_node_id,
            "binding_id": None,
            "label": "No catalog match",
            "summary": (
                "Semantic retrieval found zero capability matches for this prose. "
                "The compiler will not fabricate a scaffold — add an @integration "
                "or @action reference, pick a capability from the catalog, or "
                "narrow the scope so retrieval can ground the workflow."
            ),
            "severity": "blocking",
            "gate_rule": typed_gap,
            "typed_gap": typed_gap,
        }
    ]


def _collect_issues(
    definition: dict[str, Any],
    binding_ledger: list[dict[str, Any]],
    import_snapshots: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    execution_setup = definition.get("execution_setup") if isinstance(definition.get("execution_setup"), dict) else {}
    constraints = execution_setup.get("constraints") if isinstance(execution_setup.get("constraints"), dict) else {}
    blocking_inputs = _string_list(constraints.get("blocking_inputs"))
    ordered_steps = sorted(
        [step for step in definition.get("draft_flow", []) if isinstance(step, dict)],
        key=lambda step: int(step.get("order") or 0),
    )
    default_node_id = _as_text(ordered_steps[0].get("id")) if ordered_steps else None

    # Emit a typed-gap issue when semantic retrieval returned zero
    # capabilities for the operator prose. One honest signal, no template.
    issues.extend(_collect_retrieval_gaps(definition))

    for binding in binding_ledger:
        state = _as_text(binding.get("state"))
        if state not in {"captured", "suggested", "stale"}:
            continue
        source_node_ids = _string_list(binding.get("source_node_ids"))
        target_node_id = source_node_ids[0] if source_node_ids else default_node_id
        if not target_node_id:
            continue
        severity = "blocking" if state in {"captured", "suggested", "stale"} else "warning"
        issues.append(
            {
                "issue_id": f"issue:{binding['binding_id']}",
                "kind": "binding_gate",
                "node_id": target_node_id,
                "binding_id": binding["binding_id"],
                "label": f"Resolve {binding['source_label'] or binding['binding_id']}",
                "summary": binding.get("rationale") or "This authority binding needs a decision before the node is fully grounded.",
                "severity": severity,
                "gate_rule": {
                    "binding_state": state,
                    "candidate_count": len(binding.get("candidate_targets") if isinstance(binding.get("candidate_targets"), list) else []),
                },
            }
        )

    for index, item in enumerate(blocking_inputs, start=1):
        if not default_node_id:
            continue
        typed_gap = {
            "gap_kind": "workflow_input",
            "missing_type": "workflow_input",
            "reason_code": "workflow.blocking_input.missing",
            "legal_repair_actions": _BLOCKING_INPUT_REPAIR_ACTIONS,
            "context": {
                "input_label": item,
                "node_id": default_node_id,
                "requirement_ref": f"execution_setup.constraints.blocking_inputs[{index - 1}]",
            },
        }
        issues.append(
            {
                "issue_id": f"issue:typed-gap:blocking-input:{index}",
                "kind": "typed_gap",
                "node_id": default_node_id,
                "binding_id": None,
                "label": "Resolve typed input gap",
                "summary": (
                    f"Workflow input gap '{item}' needs a source authority, producer node, "
                    "or narrowed requirement before execution."
                ),
                "severity": "blocking",
                "gate_rule": typed_gap,
                "typed_gap": typed_gap,
            }
        )

    phases = execution_setup.get("phases") if isinstance(execution_setup.get("phases"), list) else []
    if phases:
        phase_by_step_id = {
            _as_text(phase.get("step_id")): phase
            for phase in phases
            if isinstance(phase, dict) and _as_text(phase.get("step_id"))
        }
        for step in ordered_steps:
            step_id = _as_text(step.get("id"))
            if not step_id:
                continue
            phase = phase_by_step_id.get(step_id)
            route = _as_text(phase.get("agent_route") or phase.get("resolved_agent_slug")) if phase else ""
            if not route:
                issues.append(
                    {
                        "issue_id": f"issue:missing-route:{step_id}",
                        "kind": "missing_route",
                        "node_id": step_id,
                        "binding_id": None,
                        "label": "Choose how this step runs",
                        "summary": "This step has no executable route yet, so the workflow cannot be hardened or run.",
                        "severity": "blocking",
                        "gate_rule": {"required_field": "execution_setup.phases.agent_route"},
                    }
                )
                continue
            if route == "@workflow/invoke":
                integration_args = phase.get("integration_args") if isinstance(phase.get("integration_args"), dict) else {}
                target_workflow_id = (
                    _as_text(integration_args.get("target_workflow_id"))
                    or _as_text(integration_args.get("workflow_id"))
                )
                if not target_workflow_id:
                    issues.append(
                        {
                            "issue_id": f"issue:missing-workflow-target:{step_id}",
                            "kind": "missing_workflow_target",
                            "node_id": step_id,
                            "binding_id": None,
                            "label": "Choose a workflow to invoke",
                            "summary": "This step is set to invoke another workflow, but no target workflow is selected.",
                            "severity": "blocking",
                            "gate_rule": {
                                "route": route,
                                "required_field": "integration_args.target_workflow_id",
                            },
                        }
                    )
                    continue

    for snapshot in import_snapshots:
        freshness = _snapshot_freshness(snapshot)
        if not freshness or freshness.get("state") != "stale" or not _as_text(snapshot.get("node_id")):
            continue
        issues.append(
            {
                "issue_id": f"issue:stale-import:{snapshot['snapshot_id']}",
                "kind": "stale_import",
                "node_id": _as_text(snapshot.get("node_id")),
                "binding_id": _as_text(snapshot.get("binding_id")) or None,
                "label": f"Refresh {_as_text(snapshot.get('source_locator')) or snapshot['snapshot_id']}",
                "summary": "Imported evidence is stale and should be refreshed or re-admitted before relying on it.",
                "severity": "blocking",
                "gate_rule": {"snapshot_id": snapshot["snapshot_id"], "freshness": freshness},
            }
        )

    issues.sort(key=lambda item: (item.get("node_id") or "", item.get("severity") or "", item.get("issue_id") or ""))
    return [_json_clone(item) for item in issues]


def _build_state_node(
    *,
    node_id: str,
    title: str,
    summary: str,
    source_node_ids: list[str],
) -> dict[str, Any]:
    return {
        "node_id": node_id,
        "kind": "state",
        "title": title,
        "summary": summary,
        "route": "",
        "prompt": "",
        "required_inputs": [],
        "outputs": [],
        "persistence_targets": [],
        "handoff_target": None,
        "source_block_ids": [],
        "binding_ids": [],
        "status": "ready",
        "source_node_ids": source_node_ids,
    }


def _build_graph(
    definition: dict[str, Any],
    *,
    binding_ledger: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    attachments: list[dict[str, Any]],
) -> dict[str, Any]:
    execution_setup = definition.get("execution_setup") if isinstance(definition.get("execution_setup"), dict) else {}
    phase_by_step_id = {
        _as_text(phase.get("step_id")): phase
        for phase in execution_setup.get("phases", [])
        if isinstance(phase, dict) and _as_text(phase.get("step_id"))
    }
    # Edge gate rules stored by the build_graph subpath handler
    edge_gate_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    for eg in (execution_setup.get("edge_gates") or []):
        if isinstance(eg, dict):
            from_id = _as_text(eg.get("from_node_id"))
            to_id = _as_text(eg.get("to_node_id"))
            if from_id and to_id:
                edge_gate_by_pair[(from_id, to_id)] = eg
    ordered_steps = sorted(
        [step for step in definition.get("draft_flow", []) if isinstance(step, dict)],
        key=lambda step: int(step.get("order") or 0),
    )
    ordered_triggers = [
        trigger
        for trigger in definition.get("trigger_intent", [])
        if isinstance(trigger, dict)
    ]
    bindings_by_node: dict[str, list[str]] = {}
    for binding in binding_ledger:
        for node_id in _string_list(binding.get("source_node_ids")):
            bindings_by_node.setdefault(node_id, []).append(_as_text(binding.get("binding_id")))
    issues_by_node: dict[str, list[dict[str, Any]]] = {}
    for issue in issues:
        node_id = _as_text(issue.get("node_id"))
        if node_id:
            issues_by_node.setdefault(node_id, []).append(issue)
    attachments_by_node: dict[str, list[dict[str, Any]]] = {}
    for attachment in attachments:
        attachments_by_node.setdefault(attachment["node_id"], []).append(attachment)

    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    state_nodes: dict[str, dict[str, Any]] = {}
    state_edges: set[tuple[str, str, str]] = set()

    first_step_id = _as_text(ordered_steps[0].get("id")) if ordered_steps else ""
    explicit_trigger_targets_by_node_id: dict[str, list[str]] = {}
    for step in ordered_steps:
        step_id = _as_text(step.get("id"))
        if not step_id:
            continue
        for dependency in _string_list(step.get("depends_on")):
            explicit_trigger_targets_by_node_id.setdefault(dependency, []).append(step_id)
    for index, trigger in enumerate(ordered_triggers, start=1):
        node_id = _as_text(trigger.get("source_node_id")) or f"trigger-node-{index:03d}"
        route = _trigger_route_for_payload(trigger)
        nodes.append(
            {
                "node_id": node_id,
                "kind": "step",
                "title": _as_text(trigger.get("title")) or f"Trigger {index}",
                "summary": _as_text(trigger.get("summary")) or _as_text(trigger.get("event_type")) or "Trigger",
                "route": route,
                "integration_args": {},
                "trigger": {
                    "event_type": _as_text(trigger.get("event_type")),
                    "cron_expression": _as_text(trigger.get("cron_expression")),
                    "source_ref": _as_text(trigger.get("source_ref")),
                    "filter": _json_clone(trigger.get("filter")) if isinstance(trigger.get("filter"), dict) else {},
                },
                "prompt": "",
                "required_inputs": [],
                "outputs": [],
                "persistence_targets": [],
                "handoff_target": None,
                "source_block_ids": _string_list(trigger.get("source_block_ids")),
                "binding_ids": [],
                "status": "ready",
                "issue_ids": [],
            }
        )
        explicit_targets = explicit_trigger_targets_by_node_id.get(node_id, [])
        if first_step_id and not explicit_targets:
            edges.append(
                {
                    "edge_id": f"edge:{node_id}:{first_step_id}:trigger",
                    "kind": "sequence",
                    "from_node_id": node_id,
                    "to_node_id": first_step_id,
                    "branch_reason": "trigger",
                    "position_index": len(edges),
                }
            )

    for step in ordered_steps:
        step_id = _as_text(step.get("id"))
        phase = phase_by_step_id.get(step_id, {})
        step_issues = issues_by_node.get(step_id, [])
        node_status = "blocked" if any(_as_text(issue.get("severity")) == "blocking" for issue in step_issues) else "ready"
        persistence_targets = _string_list(phase.get("persistence_targets"))
        nodes.append(
            {
                "node_id": step_id,
                "kind": "step",
                "title": _as_text(step.get("title")) or f"Step {int(step.get('order') or 0) or 1}",
                "summary": _as_text(step.get("summary")) or _as_text(step.get("title")),
                "route": _as_text(phase.get("agent_route") or phase.get("resolved_agent_slug")),
                "integration_args": _json_clone(phase.get("integration_args")) if isinstance(phase.get("integration_args"), dict) else {},
                "prompt": _as_text(phase.get("system_prompt")),
                "required_inputs": _string_list(phase.get("required_inputs")),
                "outputs": _string_list(phase.get("outputs")),
                "persistence_targets": persistence_targets,
                "handoff_target": _as_text(phase.get("handoff_target")) or None,
                "task_type": _as_text(phase.get("task_type")) or None,
                "agent": _as_text(phase.get("agent")) or None,
                "capabilities": _string_list(phase.get("capabilities")),
                "write_scope": _string_list(phase.get("write_scope")),
                "agent_tool_plan": (
                    _json_clone(phase.get("agent_tool_plan"))
                    if isinstance(phase.get("agent_tool_plan"), dict)
                    else {}
                ),
                "completion_contract": (
                    _json_clone(phase.get("completion_contract"))
                    if isinstance(phase.get("completion_contract"), dict)
                    else {}
                ),
                "source_block_ids": _string_list(step.get("source_block_ids")),
                "binding_ids": sorted(bindings_by_node.get(step_id, [])),
                "status": node_status,
                "issue_ids": [_as_text(issue.get("issue_id")) for issue in step_issues if _as_text(issue.get("issue_id"))],
            }
        )
        for target in persistence_targets:
            state_node_id = f"state:persistence:{_slugify(target) or _stable_digest('state', target)}"
            if state_node_id not in state_nodes:
                state_nodes[state_node_id] = _build_state_node(
                    node_id=state_node_id,
                    title=target,
                    summary="Durable workflow state or persisted knowledge target.",
                    source_node_ids=[step_id],
                )
            state_edges.add((f"edge:{step_id}:{state_node_id}:persist", step_id, state_node_id))

        for attachment in attachments_by_node.get(step_id, []):
            if not attachment.get("promote_to_state"):
                continue
            state_node_id = attachment.get("state_node_id") or f"state:attachment:{_slugify(_as_text(attachment.get('authority_ref')) or attachment['attachment_id'])}"
            state_nodes[state_node_id] = _build_state_node(
                node_id=state_node_id,
                title=attachment.get("label") or _as_text(attachment.get("authority_ref")) or "Attached State",
                summary=f"Promoted authority attachment ({attachment.get('role') or 'state'}) visible on the workflow graph.",
                source_node_ids=[step_id],
            )
            if _as_text(attachment.get("role")) in {"state_dependency", "input"}:
                state_edges.add((f"edge:{state_node_id}:{step_id}:state", state_node_id, step_id))
            else:
                state_edges.add((f"edge:{step_id}:{state_node_id}:state", step_id, state_node_id))

    for step in ordered_steps:
        step_id = _as_text(step.get("id"))
        gate_nodes = []
        for issue in issues_by_node.get(step_id, []):
            gate_node_id = f"gate:{_slugify(_as_text(issue.get('issue_id')))}"
            gate_nodes.append(gate_node_id)
            nodes.append(
                {
                    "node_id": gate_node_id,
                    "kind": "gate",
                    "title": _as_text(issue.get("label")) or "Gate",
                    "summary": _as_text(issue.get("summary")) or "Authority gate.",
                    "route": "",
                    "integration_args": {},
                    "prompt": "",
                    "required_inputs": [],
                    "outputs": [],
                    "persistence_targets": [],
                    "handoff_target": step_id,
                    "source_block_ids": [],
                    "binding_ids": [_as_text(issue.get("binding_id"))] if _as_text(issue.get("binding_id")) else [],
                    "status": "blocked" if _as_text(issue.get("severity")) == "blocking" else "warning",
                    "gate_rule": _json_clone(issue.get("gate_rule")) if isinstance(issue.get("gate_rule"), dict) else {},
                }
            )
            edges.append(
                with_edge_release(
                    {
                        "edge_id": f"edge:{gate_node_id}:{step_id}",
                        "kind": "authority_gate",
                        "from_node_id": gate_node_id,
                        "to_node_id": step_id,
                        "position_index": len(edges),
                    }
                )
            )
        dependencies = _string_list(step.get("depends_on"))
        dependency_targets = gate_nodes or [step_id]
        for dependency in dependencies:
            for target in dependency_targets:
                eg = edge_gate_by_pair.get((dependency, target))
                edge_entry: dict[str, Any] = {
                    "edge_id": f"edge:{dependency}:{target}",
                    "kind": "proceeds_to",
                    "from_node_id": dependency,
                    "to_node_id": target,
                    "position_index": len(edges),
                }
                if eg:
                    edges.append(
                        with_edge_release(
                            edge_entry,
                            normalize_edge_release(eg),
                        )
                    )
                else:
                    edges.append(with_edge_release(edge_entry))

    for edge_id, from_node_id, to_node_id in sorted(state_edges):
        edges.append(
            with_edge_release(
                {
                    "edge_id": edge_id,
                    "kind": "state_informs" if from_node_id.startswith("state:") and not to_node_id.startswith("state:") else "proceeds_to",
                    "from_node_id": from_node_id,
                    "to_node_id": to_node_id,
                    "position_index": len(edges),
                }
            )
        )

    nodes.extend(state_nodes.values())
    unique_nodes: dict[str, dict[str, Any]] = {}
    for node in nodes:
        unique_nodes[_as_text(node.get("node_id"))] = node
    ordered_nodes = [unique_nodes[key] for key in sorted(unique_nodes)]
    ordered_edges = sorted(
        edges,
        key=lambda item: (_as_text(item.get("from_node_id")), _as_text(item.get("to_node_id")), _as_text(item.get("edge_id"))),
    )
    graph_payload = {
        "schema_version": 1,
        "definition_revision": _as_text(definition.get("definition_revision")),
        "nodes": ordered_nodes,
        "edges": ordered_edges,
    }
    return {
        "graph_id": _stable_digest("build_graph", graph_payload),
        "definition_revision": _as_text(definition.get("definition_revision")),
        "compiler_revision": _as_text((definition.get("compile_provenance") or {}).get("surface_revision")) if isinstance(definition.get("compile_provenance"), dict) else "",
        "schema_version": 1,
        "nodes": ordered_nodes,
        "edges": ordered_edges,
        "issues": issues,
        "projection_status": None,
    }


def build_authority_bundle(
    definition: dict[str, Any],
    *,
    compiled_spec: dict[str, Any] | None = None,
) -> dict[str, Any]:
    materialized = materialize_definition(definition if isinstance(definition, dict) else {})
    import_snapshots = _normalize_import_snapshots(materialized)
    attachments = _normalize_attachments(materialized)
    binding_ledger = _merge_binding_ledger(materialized, import_snapshots)
    issues = _collect_issues(materialized, binding_ledger, import_snapshots)
    has_blocking_issue = any(_as_text(issue.get("severity")) == "blocking" for issue in issues)
    projection_status = {
        "state": "blocked" if has_blocking_issue else "ready",
        "blocking_issue_ids": [
            _as_text(issue.get("issue_id"))
            for issue in issues
            if _as_text(issue.get("severity")) == "blocking" and _as_text(issue.get("issue_id"))
        ],
        "issue_count": len(issues),
        "compiled_spec_available": isinstance(compiled_spec, dict),
    }
    build_graph = _build_graph(
        materialized,
        binding_ledger=binding_ledger,
        issues=issues,
        attachments=attachments,
    )
    build_graph["projection_status"] = projection_status
    bundle = {
        "build_authority_version": 1,
        "build_graph": build_graph,
        "binding_ledger": binding_ledger,
        "import_snapshots": import_snapshots,
        "authority_attachments": attachments,
        "build_issues": issues,
        "projection_status": projection_status,
    }
    if isinstance(compiled_spec, dict):
        bundle["compiled_spec_projection"] = {
            "version": 1,
            "graph_id": build_graph["graph_id"],
            "definition_revision": _as_text(materialized.get("definition_revision")),
            "compiled_spec": _json_clone(compiled_spec),
        }
    else:
        bundle["compiled_spec_projection"] = None
    return bundle


def apply_authority_bundle(
    definition: dict[str, Any],
    *,
    compiled_spec: dict[str, Any] | None = None,
) -> dict[str, Any]:
    materialized = materialize_definition(definition if isinstance(definition, dict) else {})
    bundle = build_authority_bundle(materialized, compiled_spec=compiled_spec)
    for key, value in bundle.items():
        if key == "compiled_spec_projection":
            continue
        materialized[key] = value
    return materialized


def _find_materialized_attachment(materialized: dict[str, Any], attachment_id: str) -> dict[str, Any] | None:
    attachments = materialized.get("authority_attachments") if isinstance(materialized.get("authority_attachments"), list) else []
    for entry in attachments:
        attachment = _normalize_attachment(entry)
        if attachment is not None and _as_text(attachment.get("attachment_id")) == attachment_id:
            return attachment
    return None


def _find_materialized_binding(materialized: dict[str, Any], binding_id: str) -> dict[str, Any] | None:
    bindings = materialized.get("binding_ledger") if isinstance(materialized.get("binding_ledger"), list) else []
    for entry in bindings:
        binding = _normalize_existing_binding(entry)
        if binding is not None and _as_text(binding.get("binding_id")) == binding_id:
            return binding
    return None


def _find_materialized_snapshot(materialized: dict[str, Any], snapshot_id: str) -> dict[str, Any] | None:
    snapshots = materialized.get("import_snapshots") if isinstance(materialized.get("import_snapshots"), list) else []
    for entry in snapshots:
        snapshot = _normalize_import_snapshot(entry)
        if snapshot is not None and _as_text(snapshot.get("snapshot_id")) == snapshot_id:
            return snapshot
    return None


def build_mutation_undo_receipt(
    definition: dict[str, Any],
    *,
    workflow_id: str,
    subpath: str,
    body: dict[str, Any],
) -> dict[str, Any] | None:
    materialized = apply_authority_bundle(definition)
    steps: list[dict[str, Any]] = []

    if subpath == "attachments":
        node_id = _as_text(body.get("node_id"))
        authority_kind = _as_text(body.get("authority_kind"))
        authority_ref = _as_text(body.get("authority_ref"))
        role = _as_text(body.get("role")) or "input"
        if node_id and authority_kind and authority_ref:
            attachment_id = _attachment_id_for(node_id, authority_kind, authority_ref, role)
            steps.append(
                {
                    "subpath": f"attachments/{attachment_id}/restore",
                    "body": {
                        "attachment": _json_clone(_find_materialized_attachment(materialized, attachment_id)),
                    },
                }
            )
    elif subpath.startswith("bindings/") and (
        subpath.endswith("/accept") or subpath.endswith("/reject") or subpath.endswith("/replace")
    ):
        binding_id = subpath[len("bindings/") :].split("/", 1)[0].strip("/")
        if binding_id:
            steps.append(
                {
                    "subpath": f"bindings/{binding_id}/restore",
                    "body": {
                        "binding": _json_clone(_find_materialized_binding(materialized, binding_id)),
                    },
                }
            )
    elif subpath == "imports":
        source_locator = _as_text(body.get("source_locator"))
        if source_locator:
            snapshot_id = _snapshot_id_for(
                node_id=_as_text(body.get("node_id")) or None,
                source_kind=_as_text(body.get("source_kind")) or "net",
                source_locator=source_locator,
                requested_shape=body.get("requested_shape") if isinstance(body.get("requested_shape"), dict) else None,
            )
            steps.append(
                {
                    "subpath": f"imports/{snapshot_id}/restore",
                    "body": {
                        "snapshot": _json_clone(_find_materialized_snapshot(materialized, snapshot_id)),
                    },
                }
            )
    elif subpath.startswith("imports/") and subpath.endswith("/admit"):
        snapshot_id = subpath[len("imports/") : -len("/admit")].strip("/")
        if snapshot_id:
            steps.append(
                {
                    "subpath": f"imports/{snapshot_id}/restore",
                    "body": {
                        "snapshot": _json_clone(_find_materialized_snapshot(materialized, snapshot_id)),
                    },
                }
            )
    elif subpath == "materialize-here":
        node_id = _as_text(body.get("node_id"))
        admitted_target = body.get("admitted_target") if isinstance(body.get("admitted_target"), dict) else {}
        authority_kind = _as_text(body.get("authority_kind"))
        authority_ref = _as_text(body.get("authority_ref")) or _as_text(admitted_target.get("target_ref"))
        role = _as_text(body.get("role")) or "input"
        if node_id and authority_kind and authority_ref:
            attachment_id = _attachment_id_for(node_id, authority_kind, authority_ref, role)
            steps.append(
                {
                    "subpath": f"attachments/{attachment_id}/restore",
                    "body": {
                        "attachment": _json_clone(_find_materialized_attachment(materialized, attachment_id)),
                    },
                }
            )
        snapshot_id = _as_text(body.get("snapshot_id"))
        source_locator = _as_text(body.get("source_locator"))
        if not snapshot_id and source_locator:
            snapshot_id = _snapshot_id_for(
                node_id=node_id or None,
                source_kind=_as_text(body.get("source_kind")) or "net",
                source_locator=source_locator,
                requested_shape=body.get("requested_shape") if isinstance(body.get("requested_shape"), dict) else None,
            )
        if snapshot_id:
            steps.append(
                {
                    "subpath": f"imports/{snapshot_id}/restore",
                    "body": {
                        "snapshot": _json_clone(_find_materialized_snapshot(materialized, snapshot_id)),
                    },
                }
            )
    elif subpath == "build_graph":
        graph = materialized.get("build_graph") if isinstance(materialized.get("build_graph"), dict) else {}
        steps.append(
            {
                "subpath": "build_graph",
                "body": {
                    "nodes": _json_clone(graph.get("nodes")) if isinstance(graph.get("nodes"), list) else [],
                    "edges": _json_clone(graph.get("edges")) if isinstance(graph.get("edges"), list) else [],
                },
            }
        )

    if not steps:
        return None
    return {
        "workflow_id": workflow_id,
        "steps": steps,
    }


def recompute_definition_revision(definition: dict[str, Any]) -> dict[str, Any]:
    cloned = _json_clone(definition if isinstance(definition, dict) else {})
    cloned["definition_revision"] = definition_revision({k: v for k, v in cloned.items() if k != "definition_revision"})
    return cloned


def upsert_binding(
    definition: dict[str, Any],
    *,
    binding_id: str,
    state: str,
    accepted_target: dict[str, Any] | None = None,
    candidate_targets: list[dict[str, Any]] | None = None,
    rationale: str | None = None,
) -> dict[str, Any]:
    materialized = apply_authority_bundle(definition)
    bindings = materialized.get("binding_ledger") if isinstance(materialized.get("binding_ledger"), list) else []
    updated_at = _iso_now()
    for entry in bindings:
        if not isinstance(entry, dict) or _as_text(entry.get("binding_id")) != binding_id:
            continue
        entry["state"] = state
        if candidate_targets is not None:
            entry["candidate_targets"] = _json_clone(candidate_targets)
        if accepted_target is not None:
            entry["accepted_target"] = _json_clone(accepted_target)
        if rationale is not None:
            entry["rationale"] = rationale
        entry["updated_at"] = updated_at
        break
    else:
        bindings.append(
            {
                "binding_id": binding_id,
                "source_kind": "reference",
                "source_label": binding_id,
                "source_span": None,
                "source_node_ids": [],
                "state": state,
                "candidate_targets": _json_clone(candidate_targets or []),
                "accepted_target": _json_clone(accepted_target) if isinstance(accepted_target, dict) else None,
                "rationale": rationale or "",
                "created_at": updated_at,
                "updated_at": updated_at,
                "freshness": None,
            }
        )
    materialized["binding_ledger"] = bindings
    return recompute_definition_revision(materialized)


def restore_binding(
    definition: dict[str, Any],
    *,
    binding_id: str,
    binding: dict[str, Any] | None,
) -> dict[str, Any]:
    materialized = apply_authority_bundle(definition)
    bindings = materialized.get("binding_ledger") if isinstance(materialized.get("binding_ledger"), list) else []
    restored = _normalize_existing_binding(binding) if isinstance(binding, dict) else None
    next_bindings: list[dict[str, Any]] = []
    inserted = False
    for entry in bindings:
        if not isinstance(entry, dict):
            continue
        if _as_text(entry.get("binding_id")) != binding_id:
            next_bindings.append(entry)
            continue
        if restored is not None and not inserted:
            next_bindings.append(restored)
            inserted = True
    if restored is not None and not inserted:
        next_bindings.append(restored)
    materialized["binding_ledger"] = next_bindings
    return recompute_definition_revision(materialized)


def attach_authority(
    definition: dict[str, Any],
    *,
    node_id: str,
    authority_kind: str,
    authority_ref: str,
    role: str,
    label: str | None = None,
    promote_to_state: bool = False,
) -> dict[str, Any]:
    materialized = apply_authority_bundle(definition)
    attachments = materialized.get("authority_attachments") if isinstance(materialized.get("authority_attachments"), list) else []
    for entry in attachments:
        if not isinstance(entry, dict):
            continue
        if (
            _as_text(entry.get("node_id")) == node_id
            and _as_text(entry.get("authority_kind")) == authority_kind
            and _as_text(entry.get("authority_ref")) == authority_ref
            and _as_text(entry.get("role")) == role
        ):
            entry["label"] = label or _as_text(entry.get("label"))
            entry["promote_to_state"] = bool(promote_to_state)
            materialized["authority_attachments"] = attachments
            return recompute_definition_revision(materialized)
    attachments.append(
        {
            "attachment_id": _attachment_id_for(node_id, authority_kind, authority_ref, role),
            "node_id": node_id,
            "authority_kind": authority_kind,
            "authority_ref": authority_ref,
            "role": role,
            "label": label or authority_ref,
            "promote_to_state": bool(promote_to_state),
            "state_node_id": None,
        }
    )
    materialized["authority_attachments"] = attachments
    return recompute_definition_revision(materialized)


def restore_attachment(
    definition: dict[str, Any],
    *,
    attachment_id: str,
    attachment: dict[str, Any] | None,
) -> dict[str, Any]:
    materialized = apply_authority_bundle(definition)
    attachments = materialized.get("authority_attachments") if isinstance(materialized.get("authority_attachments"), list) else []
    restored = _normalize_attachment(attachment) if isinstance(attachment, dict) else None
    next_attachments: list[dict[str, Any]] = []
    inserted = False
    for entry in attachments:
        if not isinstance(entry, dict):
            continue
        if _as_text(entry.get("attachment_id")) != attachment_id:
            next_attachments.append(entry)
            continue
        if restored is not None and not inserted:
            next_attachments.append(restored)
            inserted = True
    if restored is not None and not inserted:
        next_attachments.append(restored)
    materialized["authority_attachments"] = next_attachments
    return recompute_definition_revision(materialized)


def stage_import_snapshot(
    definition: dict[str, Any],
    *,
    node_id: str | None,
    source_kind: str,
    source_locator: str,
    requested_shape: dict[str, Any] | None = None,
    payload: Any = None,
    freshness_ttl: int = 3600,
) -> dict[str, Any]:
    materialized = apply_authority_bundle(definition)
    snapshots = materialized.get("import_snapshots") if isinstance(materialized.get("import_snapshots"), list) else []
    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    snapshot_id = _snapshot_id_for(
        node_id=node_id,
        source_kind=source_kind,
        source_locator=source_locator,
        requested_shape=requested_shape if isinstance(requested_shape, dict) else None,
    )
    snapshot_entry = {
        "snapshot_id": snapshot_id,
        "source_kind": source_kind,
        "source_locator": source_locator,
        "requested_shape": _json_clone(requested_shape or {}),
        "payload": _json_clone(payload),
        "freshness_ttl": max(60, int(freshness_ttl or 3600)),
        "captured_at": captured_at.isoformat(),
        "stale_after_at": (captured_at + timedelta(seconds=max(60, int(freshness_ttl or 3600)))).isoformat(),
        "approval_state": "staged",
        "admitted_targets": [],
        "binding_id": f"binding:import:{snapshot_id}",
        "node_id": node_id,
    }
    replaced = False
    for index, existing in enumerate(snapshots):
        if not isinstance(existing, dict) or _as_text(existing.get("snapshot_id")) != snapshot_id:
            continue
        snapshots[index] = snapshot_entry
        replaced = True
        break
    if not replaced:
        snapshots.append(snapshot_entry)
    materialized["import_snapshots"] = snapshots
    return recompute_definition_revision(materialized)


def admit_import_snapshot(
    definition: dict[str, Any],
    *,
    snapshot_id: str,
    admitted_target: dict[str, Any],
) -> dict[str, Any]:
    materialized = apply_authority_bundle(definition)
    snapshots = materialized.get("import_snapshots") if isinstance(materialized.get("import_snapshots"), list) else []
    for snapshot in snapshots:
        if not isinstance(snapshot, dict) or _as_text(snapshot.get("snapshot_id")) != snapshot_id:
            continue
        snapshot["approval_state"] = "admitted"
        normalized_target = _json_clone(admitted_target)
        admitted_targets = _normalize_binding_targets(snapshot.get("admitted_targets"))
        target_key = _binding_target_key(normalized_target)
        for index, existing in enumerate(admitted_targets):
            if _binding_target_key(existing) != target_key:
                continue
            admitted_targets[index] = normalized_target
            break
        else:
            admitted_targets.append(normalized_target)
        snapshot["admitted_targets"] = admitted_targets
        break
    materialized["import_snapshots"] = snapshots
    return recompute_definition_revision(materialized)


def restore_import_snapshot(
    definition: dict[str, Any],
    *,
    snapshot_id: str,
    snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    materialized = apply_authority_bundle(definition)
    snapshots = materialized.get("import_snapshots") if isinstance(materialized.get("import_snapshots"), list) else []
    restored = _normalize_import_snapshot(snapshot) if isinstance(snapshot, dict) else None
    next_snapshots: list[dict[str, Any]] = []
    inserted = False
    for entry in snapshots:
        if not isinstance(entry, dict):
            continue
        if _as_text(entry.get("snapshot_id")) != snapshot_id:
            next_snapshots.append(entry)
            continue
        if restored is not None and not inserted:
            next_snapshots.append(restored)
            inserted = True
    if restored is not None and not inserted:
        next_snapshots.append(restored)
    materialized["import_snapshots"] = next_snapshots
    return recompute_definition_revision(materialized)
