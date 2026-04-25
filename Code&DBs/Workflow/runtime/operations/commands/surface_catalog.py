"""Surface-catalog command handlers — typed write paths for experience templates.

All writes route through ``operation_catalog_registry`` +
``operation_catalog_gateway`` per the standing order
``architecture-policy::platform-architecture::conceptual-events-register-
through-operation-catalog-registry``. Admission follows
``architecture-policy::surface-catalog::type-lattice-and-risk-mitigation-
is-authority-reuse`` — slot types must exist in the lattice, emitted ops
must be registered, specialized templates must declare a fallback.

The handler writes the template as a ``memory_entities`` vertex plus its
typed ``memory_edges`` (``uses_shape``, ``consumes``, ``targets_template``,
``includes``) so it joins the same outcome graph as the seed templates.
``projection.surface.legal_templates`` will pick it up on the next read.
"""
from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel


class SurfaceTemplateSlot(BaseModel):
    """One positional slot on the template.

    ``slot_name`` is the shape's slot key (left, right, action_rail…).
    ``pill_type_ref`` is the memory_entities id of the pill_type the slot
    consumes. Optional slots without a consumes binding (e.g. action_rail
    that consumes an operation, not a pill) omit ``pill_type_ref``.
    """

    slot_name: str
    pill_type_ref: str | None = None
    ordinal: int = 0


class SurfaceTemplateRegisterCommand(BaseModel):
    """Input contract for ``surface.template.register``.

    ``template_ref`` must be a fresh id not yet present in memory_entities.
    ``shape_ref`` must exist as a layout_shape vertex.
    ``slot_consumes`` lists positional slots; every pill_type_ref must
    exist in the lattice. ``intent_ref`` is optional; when present the
    command also writes a ``targets_template`` edge weighted by
    ``intent_binding_weight``.
    """

    template_ref: str
    name: str
    summary: str
    shape_ref: str
    slot_consumes: list[SurfaceTemplateSlot] = []
    intent_ref: str | None = None
    intent_binding_weight: float = 1.0
    fallback_template_ref: str | None = None
    framework_ref: str | None = None
    render_hint: dict[str, Any] = {}
    emits_operation_refs: list[str] = []
    slot_order: list[str] = []


def _fetch_one(pg: Any, query: str, *args: Any) -> dict[str, Any] | None:
    row = pg.fetchrow(query, *args)
    return dict(row) if row else None


def _entity_kind(pg: Any, entity_id: str) -> str | None:
    row = _fetch_one(pg, "SELECT entity_type FROM memory_entities WHERE id = $1", entity_id)
    return row["entity_type"] if row else None


def _operation_name(pg: Any, operation_ref: str) -> str | None:
    row = _fetch_one(
        pg,
        "SELECT operation_name FROM operation_catalog_registry WHERE operation_ref = $1 OR operation_name = $1",
        operation_ref,
    )
    return row["operation_name"] if row else None


def _is_base_pill_type(pg: Any, pill_ref: str) -> bool:
    """A pill type is considered base when it has no ``subtype_of`` edge."""
    parent = _fetch_one(
        pg,
        "SELECT target_id FROM memory_edges WHERE source_id = $1 AND relation_type = 'subtype_of' LIMIT 1",
        pill_ref,
    )
    return parent is None


def _run_admission_gate(
    pg: Any,
    command: SurfaceTemplateRegisterCommand,
) -> list[dict[str, Any]]:
    """Enforce the 5-check policy. Returns a list of violations (empty = pass)."""
    violations: list[dict[str, Any]] = []

    # Check 0 — template_ref must be fresh (or entity_type matches)
    existing_kind = _entity_kind(pg, command.template_ref)
    if existing_kind and existing_kind != "experience_template":
        violations.append({
            "check": "template_ref_kind_mismatch",
            "detail": f"{command.template_ref} already exists as entity_type={existing_kind}, expected experience_template",
        })

    # Check A — shape_ref must exist as a layout_shape
    shape_kind = _entity_kind(pg, command.shape_ref)
    if shape_kind is None:
        violations.append({
            "check": "shape_ref_not_registered",
            "detail": f"shape_ref {command.shape_ref} is not in memory_entities",
            "repair_actions": [{"action": "register_layout_shape", "shape_ref": command.shape_ref}],
        })
    elif shape_kind != "layout_shape":
        violations.append({
            "check": "shape_ref_wrong_kind",
            "detail": f"{command.shape_ref} entity_type={shape_kind}, expected layout_shape",
        })

    # Check 1 — every consumes pill_type_ref exists and is a pill_type
    has_specialized_slot = False
    for slot in command.slot_consumes:
        if slot.pill_type_ref is None:
            continue
        kind = _entity_kind(pg, slot.pill_type_ref)
        if kind is None:
            violations.append({
                "check": "consumes_pill_type_not_registered",
                "slot": slot.slot_name,
                "detail": f"pill_type_ref {slot.pill_type_ref} is not in memory_entities",
                "repair_actions": [{"action": "register_pill_type", "pill_type_ref": slot.pill_type_ref}],
            })
            continue
        if kind != "pill_type":
            violations.append({
                "check": "consumes_wrong_entity_kind",
                "slot": slot.slot_name,
                "detail": f"{slot.pill_type_ref} entity_type={kind}, expected pill_type",
            })
            continue
        if not _is_base_pill_type(pg, slot.pill_type_ref):
            has_specialized_slot = True

    # Check 2 — every emits operation exists in operation_catalog_registry
    for op_ref in command.emits_operation_refs:
        if _operation_name(pg, op_ref) is None:
            violations.append({
                "check": "emits_operation_not_registered",
                "operation_ref": op_ref,
                "detail": f"operation_catalog_registry has no row for {op_ref}",
            })

    # Check 4 — ambiguous same-specificity bindings for the same intent
    # A strict tie-breaker policy is a follow-up; for now we reject any
    # proposed intent binding that would share weight=intent_binding_weight
    # with an existing peer on the same intent.
    if command.intent_ref:
        peers = pg.execute(
            """
            SELECT target_id as template_ref, weight
            FROM memory_edges
            WHERE source_id = $1 AND relation_type = 'targets_template'
            """,
            command.intent_ref,
        )
        for peer in peers:
            peer_template_ref = peer["template_ref"]
            if (
                peer_template_ref != command.template_ref
                and abs(float(peer["weight"] or 0.0) - command.intent_binding_weight) < 1e-9
            ):
                violations.append({
                    "check": "ambiguous_intent_binding",
                    "detail": (
                        f"intent {command.intent_ref} already targets {peer_template_ref} "
                        f"at weight {peer['weight']}; pick a distinct intent_binding_weight"
                    ),
                })
                break

    # Check 5 — specialized templates require explicit fallback OR a generic sibling
    if has_specialized_slot and not command.fallback_template_ref:
        if command.intent_ref:
            sibling = pg.fetchrow(
                """
                SELECT te.source_id AS template_ref
                FROM memory_edges te
                JOIN memory_entities t ON t.id = te.target_id
                WHERE te.relation_type = 'targets_template'
                  AND te.source_id = $1
                  AND te.target_id <> $2
                  AND t.entity_type = 'experience_template'
                LIMIT 1
                """,
                command.intent_ref,
                command.template_ref,
            )
            if sibling is None:
                violations.append({
                    "check": "specialized_template_requires_fallback",
                    "detail": (
                        "specialized template consumes subtypes past the base lattice; "
                        "declare fallback_template_ref OR register a generic sibling "
                        f"for intent {command.intent_ref} first"
                    ),
                })
        else:
            violations.append({
                "check": "specialized_template_requires_fallback",
                "detail": "specialized template without intent binding must declare fallback_template_ref",
            })

    # Check 5.1 — declared fallback_template_ref must actually exist
    if command.fallback_template_ref:
        kind = _entity_kind(pg, command.fallback_template_ref)
        if kind != "experience_template":
            violations.append({
                "check": "fallback_template_not_registered",
                "detail": f"fallback_template_ref {command.fallback_template_ref} is not registered as experience_template",
            })

    # Check 3 — field-access grants. Opt-in via render_hint.field_grants:
    # {slot_name: [pill_field_ref, ...]}. Each declared field must be reachable
    # from the slot's pill_type_ref via has_field, optionally inherited through
    # subtype_of edges. Templates that don't declare field_grants pass this
    # check silently (the success scope_note flags the deferred check), so the
    # debt resolves template-by-template as authors opt in.
    field_grants_violations = _check_field_access_grants(pg, command)
    violations.extend(field_grants_violations)

    return violations


def _check_field_access_grants(
    pg: Any,
    command: SurfaceTemplateRegisterCommand,
) -> list[dict[str, Any]]:
    """Verify every declared render_hint.field_grants entry is reachable from
    its slot's pill_type via has_field + subtype_of*.

    No declaration → no check (debt deferred for that template).
    Declaration present but missing slot → declared_for_unknown_slot.
    Declaration present, field unreachable → field_not_granted.
    """
    render_hint = command.render_hint or {}
    field_grants = render_hint.get("field_grants")
    if not isinstance(field_grants, dict) or not field_grants:
        return []

    slot_pill_types: dict[str, str] = {
        s.slot_name: s.pill_type_ref
        for s in command.slot_consumes
        if s.pill_type_ref
    }

    violations: list[dict[str, Any]] = []
    for slot_name, raw_fields in field_grants.items():
        if slot_name not in slot_pill_types:
            violations.append({
                "check": "field_grant_for_unknown_slot",
                "slot": slot_name,
                "detail": (
                    f"render_hint.field_grants references slot '{slot_name}' "
                    "but no slot_consumes entry exists with that slot_name"
                ),
            })
            continue
        if not isinstance(raw_fields, list):
            violations.append({
                "check": "field_grant_not_list",
                "slot": slot_name,
                "detail": f"render_hint.field_grants[{slot_name!r}] must be a list of pill_field refs",
            })
            continue

        slot_pill_type = slot_pill_types[slot_name]
        granted_fields = _granted_fields_for_pill_type(pg, slot_pill_type)
        for field_ref in raw_fields:
            if not isinstance(field_ref, str) or not field_ref.strip():
                continue
            if field_ref not in granted_fields:
                violations.append({
                    "check": "field_not_granted",
                    "slot": slot_name,
                    "field_ref": field_ref,
                    "pill_type_ref": slot_pill_type,
                    "detail": (
                        f"slot '{slot_name}' bound to pill_type {slot_pill_type} "
                        f"does not grant field {field_ref} via has_field + subtype_of*; "
                        "either declare a more specific pill_type for the slot, or "
                        "drop the field from render_hint.field_grants"
                    ),
                    "repair_actions": [
                        {"action": "register_has_field_edge", "from_pill_type": slot_pill_type, "field_ref": field_ref},
                        {"action": "tighten_slot_pill_type", "slot": slot_name, "current": slot_pill_type, "field_ref": field_ref},
                    ],
                })
    return violations


def _granted_fields_for_pill_type(pg: Any, pill_type_ref: str) -> set[str]:
    """All pill_field refs reachable from pill_type_ref via has_field, including
    inherited fields up the subtype_of chain. One recursive CTE per call.
    """
    rows = pg.execute(
        """
        WITH RECURSIVE ancestors AS (
            SELECT $1::text AS ancestor
            UNION ALL
            SELECT e.target_id
              FROM ancestors a
              JOIN memory_edges e
                ON e.source_id = a.ancestor
               AND e.relation_type = 'subtype_of'
        )
        SELECT DISTINCT e.target_id AS field_ref
          FROM memory_edges e
          JOIN ancestors a ON a.ancestor = e.source_id
         WHERE e.relation_type = 'has_field'
        """,
        pill_type_ref,
    )
    return {row["field_ref"] for row in rows}


def handle_surface_template_register(
    command: SurfaceTemplateRegisterCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Admission-gated write of an experience_template into the outcome graph.

    On success, returns the applied template_ref plus the list of edges
    written. On admission failure, returns {"ok": False, "violations": [...]}
    with non-200 semantics — the gateway surfaces this as the
    ``result_payload`` on the receipt and downstream observers see the
    typed gap. Nothing is written when admission fails.
    """
    pg = subsystems.get_pg_conn()

    violations = _run_admission_gate(pg, command)
    if violations:
        return {
            "ok": False,
            "admission_gate": "surface.template.register",
            "violations": violations,
            "template_ref": command.template_ref,
        }

    metadata = {
        "slot_order": list(command.slot_order) or [s.slot_name for s in command.slot_consumes],
        "slot_type_refs": {
            s.slot_name: s.pill_type_ref for s in command.slot_consumes if s.pill_type_ref
        },
        "render_hint": dict(command.render_hint),
        "registered_via": "operation.surface.template.register",
    }
    if command.fallback_template_ref:
        metadata["fallback_template_ref"] = command.fallback_template_ref
    if command.framework_ref:
        metadata["framework_ref"] = command.framework_ref

    # 1. Vertex
    pg.execute(
        """
        INSERT INTO memory_entities (
            id, entity_type, name, content, metadata, source, confidence, archived
        ) VALUES (
            $1, 'experience_template', $2, $3, $4::jsonb, 'surface.template.register', 1.0, FALSE
        )
        ON CONFLICT (id) DO UPDATE SET
            entity_type = EXCLUDED.entity_type,
            name = EXCLUDED.name,
            content = EXCLUDED.content,
            metadata = EXCLUDED.metadata,
            source = EXCLUDED.source,
            confidence = EXCLUDED.confidence,
            updated_at = now()
        """,
        command.template_ref,
        command.name,
        command.summary,
        json.dumps(metadata),
    )

    edges_written: list[dict[str, Any]] = []

    def _upsert_edge(source_id: str, target_id: str, relation_type: str, weight: float, meta: dict[str, Any] | None = None) -> None:
        pg.execute(
            """
            INSERT INTO memory_edges (source_id, target_id, relation_type, weight, metadata)
            VALUES ($1, $2, $3, $4, $5::jsonb)
            ON CONFLICT (source_id, target_id, relation_type) DO UPDATE SET
                weight = EXCLUDED.weight,
                metadata = EXCLUDED.metadata
            """,
            source_id,
            target_id,
            relation_type,
            weight,
            json.dumps(meta or {}),
        )
        edges_written.append({
            "source_id": source_id,
            "target_id": target_id,
            "relation_type": relation_type,
            "weight": weight,
        })

    # 2. uses_shape
    _upsert_edge(command.template_ref, command.shape_ref, "uses_shape", 1.0)

    # 3. consumes edges (disambiguate duplicate targets by relation_type suffix)
    used_targets: set[str] = set()
    for slot in command.slot_consumes:
        if slot.pill_type_ref is None:
            continue
        relation_type = "consumes"
        if slot.pill_type_ref in used_targets:
            relation_type = f"consumes:{slot.slot_name}"
        _upsert_edge(
            command.template_ref,
            slot.pill_type_ref,
            relation_type,
            1.0,
            {"slot": slot.slot_name, "ordinal": slot.ordinal},
        )
        used_targets.add(slot.pill_type_ref)

    # 4. emits edges (optional; for future legal_actions reasoning)
    for op_ref in command.emits_operation_refs:
        _upsert_edge(command.template_ref, op_ref, "emits", 1.0)

    # 5. intent targeting
    if command.intent_ref:
        _upsert_edge(
            command.intent_ref,
            command.template_ref,
            "targets_template",
            command.intent_binding_weight,
        )

    # 6. framework inclusion
    if command.framework_ref:
        _upsert_edge(command.framework_ref, command.template_ref, "includes", 1.0)

    field_grants_declared = bool(
        isinstance(command.render_hint, dict)
        and command.render_hint.get("field_grants")
    )
    result: dict[str, Any] = {
        "ok": True,
        "template_ref": command.template_ref,
        "edges_written": edges_written,
        "admission_gate": "surface.template.register",
        "field_grants_validated": field_grants_declared,
    }
    if not field_grants_declared:
        # Author hasn't opted into field-access verification yet. Note the
        # deferral so observers can audit the gap; the check itself is now
        # in place (see _check_field_access_grants) and runs the moment a
        # template declares render_hint.field_grants.
        result["scope_note"] = (
            "Template landed through operation_catalog_gateway; "
            "legal_templates projection picks it up on next read. "
            "render_hint.field_grants not declared on this template, so "
            "field-access admission ran in opt-in mode (no fields to check). "
            "Declare render_hint.field_grants to remove this note."
        )
    return result
