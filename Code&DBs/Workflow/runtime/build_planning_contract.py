"""Explicit planning artifacts over workflow build authority state.

This module projects the current build workspace into the first shared planning
artifacts for the operating-model path:

- CandidateResolutionManifest: deterministic proposal layer
- ReviewablePlan: explicit approval/review layer

The source of truth stays where it already belongs:
- definition/build state in the workflow build record
- review provenance in workflow_build_review_decisions

This module is intentionally a projector. It does not introduce a second
planner stack and it does not mutate workflow state.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from runtime.build_authority import build_authority_bundle
from runtime.build_review_decisions import (
    effective_workflow_build_review_state,
    scrub_review_state_for_persistence,
)
from runtime.definition_compile_kernel import materialize_definition
from storage.postgres.workflow_build_planning_repository import (
    list_active_capability_bundle_definitions,
    list_active_workflow_shape_family_definitions,
    load_capability_bundle_definitions,
    load_default_workflow_build_review_policy,
    load_workflow_build_review_session,
    replace_workflow_build_candidate_manifest,
    upsert_workflow_build_execution_manifest,
    upsert_workflow_build_intent,
    upsert_workflow_build_review_session,
)


def _json_clone(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if value is None:
        return ""
    return str(value).strip()


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_text(item) for item in value if _text(item)]


def _stable_id(prefix: str, payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return f"{prefix}_{hashlib.sha256(encoded.encode('utf-8')).hexdigest()[:16]}"


def _can_use_planning_repo(conn: Any | None) -> bool:
    return conn is not None and hasattr(conn, "fetchrow") and hasattr(conn, "fetch") and hasattr(conn, "execute")


def _tokenize_text(*values: Any) -> set[str]:
    tokens: set[str] = set()
    for value in values:
        text = _text(value).lower()
        if not text:
            continue
        tokens.update(token for token in re.findall(r"[a-z0-9_]+", text) if token)
    return tokens


def _source_mode_for_definition(definition: dict[str, Any]) -> str:
    explicit = _text(definition.get("source_mode"))
    if explicit:
        return explicit
    if _text(definition.get("source_prose")) or _text(definition.get("compiled_prose")):
        return "prose"
    if definition.get("draft_flow"):
        return "saved_draft"
    return "api"


def _goal_from_definition(definition: dict[str, Any]) -> str:
    for field in ("goal", "compiled_prose", "source_prose", "title", "name"):
        text = _text(definition.get(field))
        if text:
            return text
    draft_flow = definition.get("draft_flow") if isinstance(definition.get("draft_flow"), list) else []
    if draft_flow:
        step = draft_flow[0] if isinstance(draft_flow[0], dict) else {}
        return _text(step.get("title")) or "Workflow build intent"
    return "Workflow build intent"


def _desired_outcome_from_definition(definition: dict[str, Any]) -> str:
    for field in ("desired_outcome", "compiled_prose", "source_prose", "authority"):
        text = _text(definition.get(field))
        if text:
            return text
    return _goal_from_definition(definition)


def _constraints_from_definition(definition: dict[str, Any]) -> list[str]:
    if isinstance(definition.get("constraints"), list):
        values = _string_list(definition.get("constraints"))
        if values:
            return values
    authority_text = _text(definition.get("authority"))
    return [authority_text] if authority_text else []


def _success_criteria_from_definition(definition: dict[str, Any]) -> list[str]:
    if isinstance(definition.get("success_criteria"), list):
        values = _string_list(definition.get("success_criteria"))
        if values:
            return values
    criteria: list[str] = []
    if isinstance(definition.get("draft_flow"), list) and definition.get("draft_flow"):
        criteria.append(f"{len(definition.get('draft_flow') or [])} draft steps materialized")
    if isinstance(definition.get("build_graph"), dict):
        criteria.append("Build graph projected cleanly")
    return criteria


def _referenced_entities_from_definition(definition: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for reference in definition.get("references", []) if isinstance(definition.get("references"), list) else []:
        if not isinstance(reference, dict):
            continue
        for field in ("slug", "raw", "resolved_to"):
            text = _text(reference.get(field))
            if text:
                values.append(text)
                break
    for binding in definition.get("binding_ledger", []) if isinstance(definition.get("binding_ledger"), list) else []:
        if not isinstance(binding, dict):
            continue
        text = _text(binding.get("source_label")) or _text(binding.get("binding_id"))
        if text:
            values.append(text)
    return list(dict.fromkeys(values))


def _uncertainty_markers_from_definition(definition: dict[str, Any]) -> list[str]:
    markers: list[str] = []
    for reference in definition.get("references", []) if isinstance(definition.get("references"), list) else []:
        if not isinstance(reference, dict):
            continue
        slug = _text(reference.get("slug"))
        if slug and (reference.get("resolved") is False or not _text(reference.get("resolved_to"))):
            markers.append(slug)
    return list(dict.fromkeys(markers))


def _candidate_approval_state(
    *,
    candidate_ref: str,
    decision: dict[str, Any] | None,
) -> str:
    if not isinstance(decision, dict):
        return "proposed"
    decision_name = _text(decision.get("decision")).lower()
    approved_ref = _text(decision.get("candidate_ref"))
    if decision_name == "approve":
        if approved_ref and approved_ref == candidate_ref:
            return "approved"
        if approved_ref:
            return "superseded"
        return "proposed"
    if decision_name == "reject":
        if approved_ref and approved_ref == candidate_ref:
            return "rejected"
        return "proposed"
    return "proposed"


def _slot_approval_state(decision: dict[str, Any] | None) -> str:
    if not isinstance(decision, dict):
        return "unapproved"
    decision_name = _text(decision.get("decision")).lower()
    if decision_name == "approve":
        return "approved"
    if decision_name == "reject":
        return "rejected"
    if decision_name == "defer":
        return "deferred"
    return "unapproved"


def _slot_candidate_resolution_state(
    *,
    binding: dict[str, Any],
    blocking_issue_ids: list[str],
) -> str:
    state = _text(binding.get("state")).lower()
    candidate_targets = (
        binding.get("candidate_targets")
        if isinstance(binding.get("candidate_targets"), list)
        else []
    )
    if blocking_issue_ids and state == "stale":
        return "blocked"
    if state == "captured" and not candidate_targets:
        return "unresolved"
    if state in {"suggested", "accepted", "rejected", "stale"} or candidate_targets:
        return "candidate_set"
    return "unresolved"


def _review_provenance(record: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(record, dict):
        return None
    return {
        "review_decision_id": _text(record.get("review_decision_id")) or None,
        "decision": _text(record.get("decision")) or None,
        "actor_type": _text(record.get("actor_type")) or None,
        "actor_ref": _text(record.get("actor_ref")) or None,
        "approval_mode": _text(record.get("approval_mode")) or None,
        "rationale": _text(record.get("rationale")) or None,
        "decided_at": _text(record.get("decided_at")) or None,
        "source_subpath": _text(record.get("source_subpath")) or None,
    }


def _workflow_shape_review_ref(build_graph: dict[str, Any]) -> str | None:
    definition_revision = _text(build_graph.get("definition_revision"))
    if definition_revision:
        return f"workflow_shape:{definition_revision}"
    graph_id = _text(build_graph.get("graph_id"))
    return graph_id or None


def build_intent_brief(
    *,
    definition: dict[str, Any],
    workflow_id: str | None = None,
    conn: Any | None = None,
) -> dict[str, Any]:
    materialized = materialize_definition(definition if isinstance(definition, dict) else {})
    definition_revision = _text(materialized.get("definition_revision")) or None
    goal = _goal_from_definition(materialized)
    desired_outcome = _desired_outcome_from_definition(materialized)
    payload = {
        "workflow_id": workflow_id,
        "definition_revision": definition_revision,
        "source_mode": _source_mode_for_definition(materialized),
        "goal": goal,
        "desired_outcome": desired_outcome,
        "constraints": _constraints_from_definition(materialized),
        "success_criteria": _success_criteria_from_definition(materialized),
        "referenced_entities": _referenced_entities_from_definition(materialized),
        "uncertainty_markers": _uncertainty_markers_from_definition(materialized),
        "bootstrap_state": {
            "has_source_prose": bool(_text(materialized.get("source_prose"))),
            "has_compiled_prose": bool(_text(materialized.get("compiled_prose"))),
            "draft_step_count": len(materialized.get("draft_flow") or [])
            if isinstance(materialized.get("draft_flow"), list)
            else 0,
            "build_graph_present": isinstance(materialized.get("build_graph"), dict),
        },
    }
    intent_ref = None
    if _can_use_planning_repo(conn) and workflow_id and definition_revision:
        try:
            stored = upsert_workflow_build_intent(
                conn,
                workflow_id=workflow_id,
                definition_revision=definition_revision,
                source_mode=payload["source_mode"],
                goal=payload["goal"],
                desired_outcome=payload["desired_outcome"],
                constraints=payload["constraints"],
                success_criteria=payload["success_criteria"],
                referenced_entities=payload["referenced_entities"],
                uncertainty_markers=payload["uncertainty_markers"],
                bootstrap_state=payload["bootstrap_state"],
            )
            intent_ref = _text(stored.get("intent_ref")) or None
        except Exception:
            intent_ref = None
    if not intent_ref and _can_use_planning_repo(conn) and workflow_id and definition_revision:
        try:
            stored = load_workflow_build_intent(
                conn,
                workflow_id=workflow_id,
                definition_revision=definition_revision,
            )
            intent_ref = _text((stored or {}).get("intent_ref")) or None
        except Exception:
            intent_ref = None
    return {
        "intent_version": 1,
        "intent_ref": intent_ref or _stable_id("intent", payload),
        **payload,
    }


def _load_bundle_defs(conn: Any | None) -> list[dict[str, Any]]:
    if not _can_use_planning_repo(conn):
        return []
    try:
        return list_active_capability_bundle_definitions(conn)
    except Exception:
        return []


def _load_shape_family_defs(conn: Any | None) -> list[dict[str, Any]]:
    if not _can_use_planning_repo(conn):
        return []
    try:
        return list_active_workflow_shape_family_definitions(conn)
    except Exception:
        return []


def _best_shape_family_ref(
    *,
    intent_brief: dict[str, Any],
    shape_family_defs: list[dict[str, Any]],
) -> str | None:
    if not shape_family_defs:
        return None
    tokens = _tokenize_text(
        intent_brief.get("goal"),
        intent_brief.get("desired_outcome"),
        *(intent_brief.get("constraints") or []),
        *(intent_brief.get("referenced_entities") or []),
    )
    best_ref: str | None = None
    best_score = -1
    for shape in shape_family_defs:
        shape_ref = _text(shape.get("shape_family_ref"))
        if not shape_ref:
            continue
        shape_tokens = _tokenize_text(
            shape_ref,
            shape.get("name"),
            (shape.get("shape_template_json") or {}).get("summary") if isinstance(shape.get("shape_template_json"), dict) else None,
            *(shape.get("default_bundle_affinities_json") or []),
        )
        score = len(tokens & shape_tokens)
        if score > best_score:
            best_score = score
            best_ref = shape_ref
    return best_ref


def _build_capability_bundle_slots(
    *,
    intent_brief: dict[str, Any],
    latest_by_target: dict[tuple[str, str], dict[str, Any]],
    bundle_defs: list[dict[str, Any]],
    preferred_shape_family_ref: str | None,
) -> list[dict[str, Any]]:
    if not bundle_defs:
        return []
    tokens = _tokenize_text(
        intent_brief.get("goal"),
        intent_brief.get("desired_outcome"),
        *(intent_brief.get("constraints") or []),
        *(intent_brief.get("success_criteria") or []),
        *(intent_brief.get("referenced_entities") or []),
    )
    grouped: dict[str, list[dict[str, Any]]] = {}
    for bundle in bundle_defs:
        bundle_ref = _text(bundle.get("bundle_ref"))
        family = _text(bundle.get("family")) or "general"
        if not bundle_ref:
            continue
        tag_tokens = _tokenize_text(*(bundle.get("intent_tags_json") or []))
        affinity_refs = _string_list(bundle.get("workflow_shape_affinities_json"))
        overlap = len(tokens & tag_tokens)
        affinity_bonus = 2 if preferred_shape_family_ref and preferred_shape_family_ref in affinity_refs else 0
        fit_score = float(overlap + affinity_bonus)
        if fit_score <= 0 and tag_tokens:
            continue
        latest_decision = latest_by_target.get(("capability_bundle", bundle_ref))
        grouped.setdefault(family, []).append(
            {
                "candidate_ref": bundle_ref,
                "rank_hint": fit_score,
                "label": bundle_ref,
                "kind": "capability_bundle",
                "family": family,
                "fit_score": fit_score,
                "confidence": min(1.0, 0.35 + (fit_score * 0.15)),
                "candidate_approval_state": _candidate_approval_state(
                    candidate_ref=bundle_ref,
                    decision=latest_decision,
                ),
                "payload": {
                    "bundle_ref": bundle_ref,
                    "family": family,
                    "intent_tags": _json_clone(bundle.get("intent_tags_json") or []),
                    "workflow_shape_affinities": _json_clone(bundle.get("workflow_shape_affinities_json") or []),
                },
                "review_provenance": _review_provenance(latest_decision),
            }
        )
    slots: list[dict[str, Any]] = []
    for family, raw_candidates in grouped.items():
        ordered = sorted(
            raw_candidates,
            key=lambda item: (-float(item.get("fit_score") or 0.0), item["candidate_ref"]),
        )
        latest_decision = None
        for item in ordered:
            record = latest_by_target.get(("capability_bundle", item["candidate_ref"]))
            if isinstance(record, dict) and _text(record.get("decision")).lower() == "approve":
                latest_decision = record
                break
        candidates = [
            {
                "candidate_ref": item["candidate_ref"],
                "rank": index,
                "label": item["label"],
                "kind": item["kind"],
                "family": item["family"],
                "fit_score": item["fit_score"],
                "confidence": item["confidence"],
                "candidate_approval_state": item["candidate_approval_state"],
                "payload": item["payload"],
                "review_provenance": item["review_provenance"],
            }
            for index, item in enumerate(ordered, start=1)
        ]
        slots.append(
            {
                "slot_ref": f"capability_bundle:{family}",
                "kind": "capability_bundle",
                "required": True,
                "family": family,
                "candidate_resolution_state": "candidate_set" if candidates else "unresolved",
                "approval_state": _slot_approval_state(latest_decision),
                "top_ranked_ref": candidates[0]["candidate_ref"] if candidates else None,
                "approved_ref": _text(latest_decision.get("candidate_ref")) or _text(latest_decision.get("target_ref")) or None
                if isinstance(latest_decision, dict) and _text(latest_decision.get("decision")).lower() == "approve"
                else None,
                "candidate_count": len(candidates),
                "candidates": candidates,
                "review_provenance": _review_provenance(latest_decision),
                "rationale": "Capability bundles are ranked from the registry and remain proposals until explicitly approved.",
            }
        )
    return sorted(slots, key=lambda item: item["slot_ref"])


def _build_binding_slots(
    *,
    binding_ledger: list[dict[str, Any]],
    issue_ids_by_binding: dict[str, list[str]],
    latest_by_target: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    slots: list[dict[str, Any]] = []
    for binding in binding_ledger:
        if not isinstance(binding, dict):
            continue
        binding_id = _text(binding.get("binding_id"))
        if not binding_id:
            continue
        latest_decision = latest_by_target.get(("binding", binding_id))
        candidate_targets = (
            binding.get("candidate_targets")
            if isinstance(binding.get("candidate_targets"), list)
            else []
        )
        candidates: list[dict[str, Any]] = []
        for index, target in enumerate(candidate_targets, start=1):
            if not isinstance(target, dict):
                continue
            candidate_ref = _text(target.get("target_ref")) or f"{binding_id}:candidate:{index}"
            candidates.append(
                {
                    "candidate_ref": candidate_ref,
                    "rank": index,
                    "label": _text(target.get("label")) or candidate_ref,
                    "kind": _text(target.get("kind")) or "reference",
                    "candidate_approval_state": _candidate_approval_state(
                        candidate_ref=candidate_ref,
                        decision=latest_decision,
                    ),
                    "payload": _json_clone(target),
                }
            )
        slots.append(
            {
                "slot_ref": binding_id,
                "kind": _text(binding.get("source_kind")) or "reference",
                "required": True,
                "source_label": _text(binding.get("source_label")) or binding_id,
                "candidate_resolution_state": _slot_candidate_resolution_state(
                    binding=binding,
                    blocking_issue_ids=issue_ids_by_binding.get(binding_id, []),
                ),
                "approval_state": _slot_approval_state(latest_decision),
                "top_ranked_ref": candidates[0]["candidate_ref"] if candidates else None,
                "approved_ref": _text(latest_decision.get("candidate_ref")) or None
                if isinstance(latest_decision, dict) and _text(latest_decision.get("decision")).lower() == "approve"
                else None,
                "blocking_issue_ids": issue_ids_by_binding.get(binding_id, []),
                "candidate_count": len(candidates),
                "candidates": candidates,
                "freshness": _json_clone(binding.get("freshness"))
                if isinstance(binding.get("freshness"), dict)
                else None,
                "review_provenance": _review_provenance(latest_decision),
                "rationale": _text(binding.get("rationale")) or None,
                "source_node_ids": _string_list(binding.get("source_node_ids")),
            }
        )
    return slots


def _build_import_evidence(
    *,
    import_snapshots: list[dict[str, Any]],
    latest_by_target: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    for snapshot in import_snapshots:
        if not isinstance(snapshot, dict):
            continue
        snapshot_id = _text(snapshot.get("snapshot_id"))
        if not snapshot_id:
            continue
        latest_decision = latest_by_target.get(("import_snapshot", snapshot_id))
        evidence.append(
            {
                "snapshot_ref": snapshot_id,
                "binding_ref": _text(snapshot.get("binding_id")) or None,
                "source_kind": _text(snapshot.get("source_kind")) or "net_request",
                "source_locator": _text(snapshot.get("source_locator")) or None,
                "candidate_resolution_state": (
                    "blocked"
                    if _text(snapshot.get("approval_state")) == "stale"
                    else "candidate_set"
                ),
                "approval_state": _slot_approval_state(latest_decision),
                "top_ranked_ref": (
                    _text((snapshot.get("admitted_targets") or [{}])[0].get("target_ref"))
                    if isinstance(snapshot.get("admitted_targets"), list) and snapshot.get("admitted_targets")
                    else None
                ),
                "captured_at": _text(snapshot.get("captured_at")) or None,
                "stale_after_at": _text(snapshot.get("stale_after_at")) or None,
                "review_provenance": _review_provenance(latest_decision),
                "requested_shape": _json_clone(snapshot.get("requested_shape"))
                if isinstance(snapshot.get("requested_shape"), dict)
                else {},
            }
        )
    return evidence


def _build_workflow_shape_candidates(
    *,
    build_graph: dict[str, Any] | None,
    latest_by_target: dict[tuple[str, str], dict[str, Any]],
    preferred_shape_family_ref: str | None = None,
) -> list[dict[str, Any]]:
    if not isinstance(build_graph, dict):
        return []
    graph_id = _text(build_graph.get("graph_id"))
    review_ref = _workflow_shape_review_ref(build_graph)
    if not graph_id or not review_ref:
        return []
    latest_decision = latest_by_target.get(("workflow_shape", review_ref))
    return [
        {
            "candidate_ref": review_ref,
            "kind": "build_graph",
            "approval_state": _slot_approval_state(latest_decision),
            "review_provenance": _review_provenance(latest_decision),
            "shape_family_ref": preferred_shape_family_ref,
            "summary": {
                "graph_id": graph_id,
                "node_count": len(build_graph.get("nodes") or []),
                "edge_count": len(build_graph.get("edges") or []),
                "projection_state": _text((build_graph.get("projection_status") or {}).get("state"))
                or None,
            },
        }
    ]


def _effective_review_state(
    conn: Any | None,
    *,
    workflow_id: str | None,
    definition_revision: str | None,
) -> dict[str, Any]:
    if conn is None or not workflow_id or not definition_revision:
        return {
            "review_group_ref": None,
            "latest_records": [],
            "latest_by_target": {},
            "approval_records": [],
            "approved_binding_refs": [],
            "approved_import_snapshot_refs": [],
            "approved_bundle_refs": [],
            "approved_workflow_shape_ref": None,
            "proposal_requests": [],
            "widening_ops": [],
        }
    return effective_workflow_build_review_state(
        conn,
        workflow_id=workflow_id,
        definition_revision=definition_revision,
    )


def _persist_candidate_manifest(
    conn: Any | None,
    *,
    workflow_id: str | None,
    definition_revision: str | None,
    intent_brief: dict[str, Any],
    manifest_payload: dict[str, Any],
) -> None:
    if not _can_use_planning_repo(conn) or not workflow_id or not definition_revision:
        return
    try:
        intent_ref = _text(intent_brief.get("intent_ref")) or None
        manifest_ref = _text(manifest_payload.get("manifest_ref")) or None
        manifest_revision = _text(manifest_payload.get("manifest_revision")) or None
        review_group_ref = _text(manifest_payload.get("review_group_ref")) or None
        if not intent_ref or not manifest_ref or not manifest_revision or not review_group_ref:
            return
        slots: list[dict[str, Any]] = []
        candidates: list[dict[str, Any]] = []
        for slot in manifest_payload.get("binding_slots", []):
            if not isinstance(slot, dict):
                continue
            slots.append(
                {
                    "slot_ref": slot["slot_ref"],
                    "slot_kind": "binding",
                    "required": bool(slot.get("required", True)),
                    "candidate_resolution_state": slot.get("candidate_resolution_state") or "unresolved",
                    "approval_state": slot.get("approval_state") or "unapproved",
                    "source_binding_ref": slot["slot_ref"],
                    "source_evidence_ref": None,
                    "top_ranked_ref": slot.get("top_ranked_ref"),
                    "approved_ref": slot.get("approved_ref"),
                    "resolution_rationale": slot.get("rationale"),
                    "slot_metadata": {
                        "source_label": slot.get("source_label"),
                        "source_node_ids": slot.get("source_node_ids") or [],
                        "blocking_issue_ids": slot.get("blocking_issue_ids") or [],
                        "freshness": slot.get("freshness"),
                    },
                }
            )
            for candidate in slot.get("candidates", []):
                if not isinstance(candidate, dict):
                    continue
                candidates.append(
                    {
                        "slot_ref": slot["slot_ref"],
                        "candidate_ref": candidate["candidate_ref"],
                        "target_kind": candidate.get("kind") or "reference",
                        "target_ref": candidate.get("candidate_ref"),
                        "rank": candidate.get("rank") or 1,
                        "fit_score": candidate.get("fit_score"),
                        "confidence": candidate.get("confidence"),
                        "source_def_ref": None,
                        "payload": candidate.get("payload") or {},
                        "candidate_approval_state": candidate.get("candidate_approval_state") or "proposed",
                        "candidate_rationale": slot.get("rationale"),
                    }
                )
        for evidence in manifest_payload.get("import_evidence", []):
            if not isinstance(evidence, dict):
                continue
            slot_ref = _text(evidence.get("snapshot_ref"))
            if not slot_ref:
                continue
            slots.append(
                {
                    "slot_ref": slot_ref,
                    "slot_kind": "import_snapshot",
                    "required": False,
                    "candidate_resolution_state": evidence.get("candidate_resolution_state") or "candidate_set",
                    "approval_state": evidence.get("approval_state") or "unapproved",
                    "source_binding_ref": evidence.get("binding_ref"),
                    "source_evidence_ref": slot_ref,
                    "top_ranked_ref": evidence.get("top_ranked_ref"),
                    "approved_ref": None,
                    "resolution_rationale": "Import snapshots remain evidence and candidates until explicitly reviewed.",
                    "slot_metadata": {
                        "source_kind": evidence.get("source_kind"),
                        "source_locator": evidence.get("source_locator"),
                        "requested_shape": evidence.get("requested_shape") or {},
                    },
                }
            )
            if evidence.get("top_ranked_ref"):
                candidates.append(
                    {
                        "slot_ref": slot_ref,
                        "candidate_ref": evidence["top_ranked_ref"],
                        "target_kind": "import_snapshot",
                        "target_ref": evidence["top_ranked_ref"],
                        "rank": 1,
                        "fit_score": None,
                        "confidence": None,
                        "source_def_ref": None,
                        "payload": evidence.get("requested_shape") or {},
                        "candidate_approval_state": "approved"
                        if evidence.get("approval_state") == "approved"
                        else "proposed",
                        "candidate_rationale": "Import evidence candidate",
                    }
                )
        for slot in manifest_payload.get("capability_bundle_candidates", []):
            if not isinstance(slot, dict):
                continue
            slots.append(
                {
                    "slot_ref": slot["slot_ref"],
                    "slot_kind": "capability_bundle",
                    "required": bool(slot.get("required", True)),
                    "candidate_resolution_state": slot.get("candidate_resolution_state") or "candidate_set",
                    "approval_state": slot.get("approval_state") or "unapproved",
                    "source_binding_ref": None,
                    "source_evidence_ref": None,
                    "top_ranked_ref": slot.get("top_ranked_ref"),
                    "approved_ref": slot.get("approved_ref"),
                    "resolution_rationale": slot.get("rationale"),
                    "slot_metadata": {
                        "family": slot.get("family"),
                    },
                }
            )
            for candidate in slot.get("candidates", []):
                if not isinstance(candidate, dict):
                    continue
                candidates.append(
                    {
                        "slot_ref": slot["slot_ref"],
                        "candidate_ref": candidate["candidate_ref"],
                        "target_kind": "capability_bundle",
                        "target_ref": candidate["candidate_ref"],
                        "rank": candidate.get("rank") or 1,
                        "fit_score": candidate.get("fit_score"),
                        "confidence": candidate.get("confidence"),
                        "source_def_ref": candidate["candidate_ref"],
                        "payload": candidate.get("payload") or {},
                        "candidate_approval_state": candidate.get("candidate_approval_state") or "proposed",
                        "candidate_rationale": slot.get("rationale"),
                    }
                )
        workflow_shape_slot_ref = "workflow_shape"
        if manifest_payload.get("workflow_shape_candidates"):
            slots.append(
                {
                    "slot_ref": workflow_shape_slot_ref,
                    "slot_kind": "workflow_shape",
                    "required": True,
                    "candidate_resolution_state": "candidate_set",
                    "approval_state": "approved"
                    if any(
                        isinstance(candidate, dict) and candidate.get("approval_state") == "approved"
                        for candidate in manifest_payload.get("workflow_shape_candidates", [])
                    )
                    else "unapproved",
                    "source_binding_ref": None,
                    "source_evidence_ref": None,
                    "top_ranked_ref": (
                        manifest_payload["workflow_shape_candidates"][0].get("candidate_ref")
                        if isinstance(manifest_payload["workflow_shape_candidates"][0], dict)
                        else None
                    ),
                    "approved_ref": next(
                        (
                            candidate.get("candidate_ref")
                            for candidate in manifest_payload.get("workflow_shape_candidates", [])
                            if isinstance(candidate, dict) and candidate.get("approval_state") == "approved"
                        ),
                        None,
                    ),
                    "resolution_rationale": "Workflow shape candidates remain proposals until explicitly reviewed.",
                    "slot_metadata": {
                        "shape_family_ref": next(
                            (
                                candidate.get("shape_family_ref")
                                for candidate in manifest_payload.get("workflow_shape_candidates", [])
                                if isinstance(candidate, dict) and candidate.get("shape_family_ref")
                            ),
                            None,
                        ),
                    },
                }
            )
            for index, candidate in enumerate(manifest_payload.get("workflow_shape_candidates", []), start=1):
                if not isinstance(candidate, dict):
                    continue
                candidates.append(
                    {
                        "slot_ref": workflow_shape_slot_ref,
                        "candidate_ref": candidate["candidate_ref"],
                        "target_kind": "workflow_shape",
                        "target_ref": candidate["candidate_ref"],
                        "rank": index,
                        "fit_score": None,
                        "confidence": None,
                        "source_def_ref": candidate.get("shape_family_ref"),
                        "payload": candidate.get("summary") or {},
                        "candidate_approval_state": "approved"
                        if candidate.get("approval_state") == "approved"
                        else "proposed",
                        "candidate_rationale": "Current workflow shape candidate",
                    }
                )
        replace_workflow_build_candidate_manifest(
            conn,
            manifest_ref=manifest_ref,
            workflow_id=workflow_id,
            definition_revision=definition_revision,
            manifest_revision=manifest_revision,
            intent_ref=intent_ref,
            review_group_ref=review_group_ref,
            execution_readiness=_text(manifest_payload.get("execution_readiness")) or "blocked",
            projection_status=manifest_payload.get("projection_status") or {},
            blocking_issues=manifest_payload.get("blocking_issues") or [],
            required_confirmations=manifest_payload.get("required_confirmations") or [],
            slots=slots,
            candidates=candidates,
        )
        policy = load_default_workflow_build_review_policy(conn)
        if isinstance(policy, dict):
            upsert_workflow_build_review_session(
                conn,
                review_group_ref=review_group_ref,
                workflow_id=workflow_id,
                definition_revision=definition_revision,
                manifest_ref=manifest_ref,
                review_policy_ref=_text(policy.get("review_policy_ref")) or "review_policy:workflow_build/default",
                status=_text(manifest_payload.get("execution_readiness")) or "review_required",
            )
    except Exception:
        return


def build_candidate_resolution_manifest(
    *,
    definition: dict[str, Any],
    workflow_id: str | None = None,
    conn: Any | None = None,
    compiled_spec: dict[str, Any] | None = None,
) -> dict[str, Any]:
    materialized = materialize_definition(definition if isinstance(definition, dict) else {})
    scrubbed_definition = scrub_review_state_for_persistence(materialized)
    authority_bundle = build_authority_bundle(scrubbed_definition, compiled_spec=compiled_spec)
    definition_revision = _text(materialized.get("definition_revision")) or None
    intent_brief = build_intent_brief(
        definition=materialized,
        workflow_id=workflow_id,
        conn=conn,
    )
    effective_review_state = _effective_review_state(
        conn,
        workflow_id=workflow_id,
        definition_revision=definition_revision,
    )
    latest_by_target = effective_review_state["latest_by_target"]
    review_group_ref = (
        effective_review_state["review_group_ref"]
        or (
            f"workflow_build:{workflow_id}:{definition_revision}"
            if workflow_id and definition_revision
            else None
        )
    )
    issue_ids_by_binding: dict[str, list[str]] = {}
    for issue in authority_bundle.get("build_issues", []):
        if not isinstance(issue, dict):
            continue
        binding_id = _text(issue.get("binding_id"))
        issue_id = _text(issue.get("issue_id"))
        if binding_id and issue_id:
            issue_ids_by_binding.setdefault(binding_id, []).append(issue_id)

    binding_slots = _build_binding_slots(
        binding_ledger=authority_bundle.get("binding_ledger") or [],
        issue_ids_by_binding=issue_ids_by_binding,
        latest_by_target=latest_by_target,
    )
    shape_family_defs = _load_shape_family_defs(conn)
    preferred_shape_family_ref = _best_shape_family_ref(
        intent_brief=intent_brief,
        shape_family_defs=shape_family_defs,
    )
    workflow_shape_candidates = _build_workflow_shape_candidates(
        build_graph=authority_bundle.get("build_graph")
        if isinstance(authority_bundle.get("build_graph"), dict)
        else None,
        latest_by_target=latest_by_target,
        preferred_shape_family_ref=preferred_shape_family_ref,
    )
    capability_bundle_candidates = _build_capability_bundle_slots(
        intent_brief=intent_brief,
        latest_by_target=latest_by_target,
        bundle_defs=_load_bundle_defs(conn),
        preferred_shape_family_ref=preferred_shape_family_ref,
    )
    open_required_slots = [
        slot
        for slot in binding_slots
        if slot.get("required") and slot.get("approval_state") != "approved"
    ]
    open_required_bundle_slots = [
        slot
        for slot in capability_bundle_candidates
        if slot.get("required") and slot.get("approval_state") != "approved"
    ]
    workflow_shape_review_required = any(
        isinstance(candidate, dict) and candidate.get("approval_state") != "approved"
        for candidate in workflow_shape_candidates
    )
    blocking_issues = [
        issue
        for issue in authority_bundle.get("build_issues", [])
        if isinstance(issue, dict) and _text(issue.get("severity")) == "blocking"
    ]
    hard_blocking_issues = [
        issue
        for issue in blocking_issues
        if _text(issue.get("kind")) not in {"binding_gate"}
    ]
    execution_readiness = (
        "blocked"
        if hard_blocking_issues
        else "review_required"
        if open_required_slots or open_required_bundle_slots or workflow_shape_review_required
        else "ready"
    )
    manifest_payload = {
        "workflow_id": workflow_id,
        "definition_revision": definition_revision,
        "intent_ref": intent_brief.get("intent_ref"),
        "binding_slots": binding_slots,
        "import_evidence": _build_import_evidence(
            import_snapshots=authority_bundle.get("import_snapshots") or [],
            latest_by_target=latest_by_target,
        ),
        "workflow_shape_candidates": workflow_shape_candidates,
        "capability_bundle_candidates": capability_bundle_candidates,
        "blocking_issues": _json_clone(hard_blocking_issues),
        "review_gates": _json_clone(
            [
                issue
                for issue in blocking_issues
                if _text(issue.get("kind")) == "binding_gate"
            ]
        ),
        "required_confirmations": [
            {
                "slot_ref": slot["slot_ref"],
                "reason": "Explicit approval is required before execution can proceed.",
            }
            for slot in open_required_slots
        ]
        + [
            {
                "slot_ref": slot["slot_ref"],
                "reason": "Capability bundle approval is required before hardening can proceed.",
            }
            for slot in open_required_bundle_slots
        ]
        + (
            [
                {
                    "slot_ref": "workflow_shape",
                    "reason": "Workflow shape requires explicit approval before hardening can proceed.",
                }
            ]
            if workflow_shape_review_required
            else []
        ),
        "overall_confidence": None,
        "execution_readiness": execution_readiness,
        "rationale": (
            "Deterministic candidate resolution produced proposals only; "
            "explicit review approval is still required before hardening."
        ),
        "projection_status": _json_clone(authority_bundle.get("projection_status") or {}),
        "review_group_ref": review_group_ref,
    }
    manifest_revision = _stable_id("candidate_manifest_revision", manifest_payload)
    manifest = {
        "manifest_version": 1,
        "manifest_id": _stable_id("candidate_manifest", manifest_payload),
        "manifest_ref": (
            f"candidate_manifest:{workflow_id}:{definition_revision}:{manifest_revision}"
            if workflow_id and definition_revision
            else _stable_id("candidate_manifest_ref", manifest_payload)
        ),
        "manifest_revision": manifest_revision,
        **manifest_payload,
    }
    _persist_candidate_manifest(
        conn,
        workflow_id=workflow_id,
        definition_revision=definition_revision,
        intent_brief=intent_brief,
        manifest_payload=manifest,
    )
    return manifest


def build_reviewable_plan(
    *,
    definition: dict[str, Any],
    workflow_id: str | None = None,
    conn: Any | None = None,
    compiled_spec: dict[str, Any] | None = None,
    candidate_manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    materialized = materialize_definition(definition if isinstance(definition, dict) else {})
    definition_revision = _text(materialized.get("definition_revision")) or None
    effective_review_state = _effective_review_state(
        conn,
        workflow_id=workflow_id,
        definition_revision=definition_revision,
    )
    latest_records = effective_review_state["latest_records"]
    manifest = (
        candidate_manifest
        if isinstance(candidate_manifest, dict)
        else build_candidate_resolution_manifest(
            definition=materialized,
            workflow_id=workflow_id,
            conn=conn,
            compiled_spec=compiled_spec,
        )
    )
    approval_records: list[dict[str, Any]] = []
    approved_binding_refs: list[dict[str, str]] = []
    approved_bundle_refs: list[str] = []
    approved_workflow_shape_ref: str | None = None
    deferred_slot_refs: list[str] = []
    proposal_requests: list[dict[str, Any]] = []
    widening_ops: list[dict[str, Any]] = []

    for record in latest_records:
        if not isinstance(record, dict):
            continue
        decision = _text(record.get("decision")).lower()
        target_kind = _text(record.get("target_kind"))
        target_ref = _text(record.get("target_ref"))
        approval_records.append(
            {
                "review_decision_id": _text(record.get("review_decision_id")) or None,
                "target_kind": target_kind,
                "target_ref": target_ref,
                "slot_ref": _text(record.get("slot_ref")) or None,
                "decision": decision,
                "candidate_ref": _text(record.get("candidate_ref")) or None,
                "candidate_payload": _json_clone(record.get("candidate_payload")),
                "approved_by": _text(record.get("actor_ref")) or None,
                "approved_at": _text(record.get("decided_at")) or None,
                "approval_mode": _text(record.get("approval_mode")) or None,
                "authority_scope": _text(record.get("authority_scope")) or None,
                "review_group_ref": _text(record.get("review_group_ref")) or None,
                "supersedes_decision_ref": _text(record.get("supersedes_decision_ref")) or None,
                "review_actor": {
                    "actor_type": _text(record.get("actor_type")) or None,
                    "actor_ref": _text(record.get("actor_ref")) or None,
                },
                "rationale": _text(record.get("rationale")) or None,
            }
        )
        if decision == "approve":
            if target_kind == "binding":
                approved_binding_refs.append(
                    {
                        "slot_ref": target_ref,
                        "candidate_ref": _text(record.get("candidate_ref")) or target_ref,
                    }
                )
            elif target_kind == "capability_bundle":
                approved_bundle_refs.append(target_ref)
            elif target_kind == "workflow_shape":
                approved_workflow_shape_ref = target_ref
        elif decision == "defer":
            deferred_slot_refs.append(target_ref)
        elif decision == "widen":
            widening_ops.append(
                {
                    "target_kind": target_kind,
                    "target_ref": target_ref,
                    "requested_by": {
                        "actor_type": _text(record.get("actor_type")) or None,
                        "actor_ref": _text(record.get("actor_ref")) or None,
                    },
                    "slot_ref": _text(record.get("slot_ref")) or None,
                    "authority_scope": _text(record.get("authority_scope")) or None,
                    "review_group_ref": _text(record.get("review_group_ref")) or None,
                    "supersedes_decision_ref": _text(record.get("supersedes_decision_ref")) or None,
                    "requested_at": _text(record.get("decided_at")) or None,
                    "operation": _json_clone(record.get("candidate_payload")),
                    "rationale": _text(record.get("rationale")) or None,
                }
            )
        elif decision == "proposal_request":
            proposal_requests.append(
                {
                    "target_kind": target_kind,
                    "target_ref": target_ref,
                    "candidate_ref": _text(record.get("candidate_ref")) or None,
                    "proposal_payload": _json_clone(record.get("candidate_payload")),
                    "requested_by": {
                        "actor_type": _text(record.get("actor_type")) or None,
                        "actor_ref": _text(record.get("actor_ref")) or None,
                    },
                    "slot_ref": _text(record.get("slot_ref")) or None,
                    "authority_scope": _text(record.get("authority_scope")) or None,
                    "review_group_ref": _text(record.get("review_group_ref")) or None,
                    "supersedes_decision_ref": _text(record.get("supersedes_decision_ref")) or None,
                    "requested_at": _text(record.get("decided_at")) or None,
                    "rationale": _text(record.get("rationale")) or None,
                }
            )

    status = "accepted"
    if proposal_requests:
        status = "needs_proposals"
    elif widening_ops:
        status = "needs_widening"
    elif manifest.get("execution_readiness") == "blocked":
        status = "blocked"
    elif manifest.get("execution_readiness") != "ready":
        status = "needs_review"
    elif deferred_slot_refs:
        status = "accepted_with_deferred_noncritical_slots"

    review_payload = {
        "workflow_id": workflow_id,
        "definition_revision": definition_revision,
        "manifest_id": manifest.get("manifest_id"),
        "manifest_ref": manifest.get("manifest_ref"),
        "approved_binding_refs": approved_binding_refs,
        "approved_bundle_refs": approved_bundle_refs,
        "approved_workflow_shape_ref": approved_workflow_shape_ref,
        "proposal_requests": proposal_requests,
        "widening_ops": widening_ops,
        "deferred_slot_refs": sorted(set(deferred_slot_refs)),
        "approval_records": approval_records,
        "status": status,
        "review_group_ref": effective_review_state["review_group_ref"],
        "review_policy_ref": None,
        "required_unapproved_slots": [
            slot["slot_ref"]
            for slot in manifest.get("binding_slots", [])
            if isinstance(slot, dict)
            and slot.get("required")
            and slot.get("approval_state") != "approved"
        ],
        "required_unapproved_bundle_slots": [
            slot["slot_ref"]
            for slot in manifest.get("capability_bundle_candidates", [])
            if isinstance(slot, dict)
            and slot.get("required")
            and slot.get("approval_state") != "approved"
        ],
    }
    if _can_use_planning_repo(conn) and workflow_id and definition_revision:
        try:
            session = load_workflow_build_review_session(
                conn,
                workflow_id=workflow_id,
                definition_revision=definition_revision,
                review_group_ref=_text(review_payload.get("review_group_ref")) or None,
            )
            if isinstance(session, dict):
                review_payload["review_policy_ref"] = _text(session.get("review_policy_ref")) or None
        except Exception:
            review_payload["review_policy_ref"] = None
    return {
        "review_version": 1,
        "review_id": _stable_id("review_plan", review_payload),
        **review_payload,
    }


def build_execution_manifest(
    *,
    definition: dict[str, Any],
    workflow_id: str | None = None,
    conn: Any | None = None,
    compiled_spec: dict[str, Any] | None = None,
    candidate_manifest: dict[str, Any] | None = None,
    reviewable_plan: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    materialized = materialize_definition(definition if isinstance(definition, dict) else {})
    definition_revision = _text(materialized.get("definition_revision")) or None
    manifest = (
        candidate_manifest
        if isinstance(candidate_manifest, dict)
        else build_candidate_resolution_manifest(
            definition=materialized,
            workflow_id=workflow_id,
            conn=conn,
            compiled_spec=compiled_spec,
        )
    )
    review = (
        reviewable_plan
        if isinstance(reviewable_plan, dict)
        else build_reviewable_plan(
            definition=materialized,
            workflow_id=workflow_id,
            conn=conn,
            compiled_spec=compiled_spec,
            candidate_manifest=manifest,
        )
    )
    if manifest.get("execution_readiness") != "ready":
        return None
    if review.get("proposal_requests") or review.get("widening_ops"):
        return None
    approved_bundle_refs = _string_list(review.get("approved_bundle_refs"))
    if not approved_bundle_refs:
        return None
    approved_bindings = [
        dict(item)
        for item in review.get("approved_binding_refs", [])
        if isinstance(item, dict)
    ]
    if not approved_bindings:
        return None
    bundle_defs = []
    if _can_use_planning_repo(conn):
        try:
            bundle_defs = load_capability_bundle_definitions(conn, bundle_refs=approved_bundle_refs)
        except Exception:
            bundle_defs = []
    mcp_tools: list[str] = []
    adapter_tools: list[str] = []
    verify_refs: list[str] = []
    review_policy_refs: list[str] = []
    submission_policies: list[dict[str, Any]] = []
    review_templates: list[dict[str, Any]] = []
    for bundle in bundle_defs:
        mcp_tools.extend(_string_list(bundle.get("allowed_mcp_tools_json")))
        adapter_tools.extend(_string_list(bundle.get("allowed_adapter_tools_json")))
        verify_refs.extend(_string_list((bundle.get("verification_policy_template_json") or {}).get("verify_refs")))
        review_policy_ref = _text((bundle.get("review_policy_template_json") or {}).get("review_policy_ref"))
        if review_policy_ref:
            review_policy_refs.append(review_policy_ref)
        if isinstance(bundle.get("submission_policy_template_json"), dict):
            submission_policies.append(_json_clone(bundle.get("submission_policy_template_json")))
        if isinstance(bundle.get("review_policy_template_json"), dict):
            review_templates.append(_json_clone(bundle.get("review_policy_template_json")))
    execution_payload = {
        "workflow_id": workflow_id,
        "definition_revision": definition_revision,
        "manifest_ref": manifest.get("manifest_ref"),
        "review_group_ref": review.get("review_group_ref"),
        "approved_bundle_refs": approved_bundle_refs,
        "resolved_bindings": [
            {
                "slot_ref": item.get("slot_ref"),
                "candidate_ref": item.get("candidate_ref"),
            }
            for item in approved_bindings
        ],
        "tool_allowlist": {
            "mcp_tools": list(dict.fromkeys(mcp_tools)),
            "adapter_tools": list(dict.fromkeys(adapter_tools)),
        },
        "verify_refs": list(dict.fromkeys(verify_refs)),
        "policy_gates": {
            "review_policy_ref": review.get("review_policy_ref"),
            "bundle_review_policy_refs": list(dict.fromkeys(review_policy_refs)),
            "submission_policies": submission_policies,
            "review_templates": review_templates,
        },
    }
    execution_manifest_ref = (
        f"execution_manifest:{workflow_id}:{definition_revision}:{_text(manifest.get('manifest_revision'))}"
        if workflow_id and definition_revision
        else _stable_id("execution_manifest", execution_payload)
    )
    hardening_report = {
        "status": "compiled" if isinstance(compiled_spec, dict) else "pending_compiled_spec",
        "manifest_ref": manifest.get("manifest_ref"),
        "review_group_ref": review.get("review_group_ref"),
        "approved_bundle_refs": approved_bundle_refs,
    }
    execution_manifest = {
        "execution_manifest_version": 1,
        "execution_manifest_ref": execution_manifest_ref,
        **execution_payload,
        "compiled_spec": _json_clone(compiled_spec) if isinstance(compiled_spec, dict) else {},
        "hardening_report": hardening_report,
    }
    if _can_use_planning_repo(conn) and workflow_id and definition_revision and isinstance(compiled_spec, dict):
        try:
            upsert_workflow_build_execution_manifest(
                conn,
                execution_manifest_ref=execution_manifest_ref,
                workflow_id=workflow_id,
                definition_revision=definition_revision,
                manifest_ref=_text(manifest.get("manifest_ref")) or manifest.get("manifest_id") or execution_manifest_ref,
                review_group_ref=_text(review.get("review_group_ref")) or f"workflow_build:{workflow_id}:{definition_revision}",
                compiled_spec=compiled_spec,
                resolved_bindings=execution_payload["resolved_bindings"],
                approved_bundle_refs=approved_bundle_refs,
                tool_allowlist=execution_payload["tool_allowlist"],
                verify_refs=execution_payload["verify_refs"],
                policy_gates=execution_payload["policy_gates"],
                hardening_report=hardening_report,
            )
        except Exception:
            pass
    return execution_manifest


__all__ = [
    "build_execution_manifest",
    "build_intent_brief",
    "build_candidate_resolution_manifest",
    "build_reviewable_plan",
]
