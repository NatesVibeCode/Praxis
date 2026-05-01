"""Experience-template legality + compilation reducer.

Implements ``projection.surface.legal_templates`` — the CQRS read-model that
takes an intent and a set of pill types and returns (a) ranked templates by
lattice-depth specificity × intent binding weight, and (b) a compiled
``PraxisSurfaceBundleV4`` for the winner. When no template is legal, the
projection returns a typed_gap with repair_actions instead of silently
degrading.

Anchored by:
  architecture-policy::surface-catalog::surface-composition-cqrs-direction
  architecture-policy::platform-architecture::legal-is-computable-not-permitted
  architecture-policy::surface-catalog::type-lattice-and-risk-mitigation-is-authority-reuse

Graph walk (all reads through memory_entities + memory_edges):
  intent --targets_template--> template        (ranked by edge.weight)
  template --consumes[:slot]--> slot_type
  pill --(subtype_of*)--> slot_type or equal    (admission)
  specificity[template] = Σ depth_in_lattice(slot_type) across slots
  rank[template] = specificity × binding_weight × usage_decay (decay=1.0 for wedge 1)
"""
from __future__ import annotations

import json
from typing import Any, Mapping


def _as_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


_TEMPLATE_ENTITY_TYPE = "experience_template"
_INTENT_ENTITY_TYPE = "intent"
_PILL_TYPE_ENTITY_TYPE = "pill_type"


def _load_ancestor_chains(pg: Any, refs: list[str]) -> dict[str, dict[str, int]]:
    """Recursive CTE: walk the subtype_of DAG once for a batch of starting refs.

    Returns ``{start: {ancestor: depth}}`` where each start carries depth=0
    on itself and an entry for every ancestor reachable through subtype_of
    edges, with the hop distance as the value. Replaces the per-call
    ``_lattice_depth`` + ``_is_subtype_of`` Python N+1 walks with a single
    query — measurable round-trip reduction (was O(K*S*D), now O(1)).

    Both lookups derive from the same chain map:
      lattice_depth(ref)         -> max(chains[ref].values())
      is_subtype(pill, slot_t)   -> slot_t in chains[pill]
    """
    deduped = sorted({r for r in refs if isinstance(r, str) and r})
    if not deduped:
        return {}
    rows = pg.execute(
        """
        WITH RECURSIVE ancestors AS (
            SELECT id AS start, id AS ancestor, 0 AS depth
              FROM unnest($1::text[]) AS s(id)
            UNION ALL
            SELECT a.start, e.target_id, a.depth + 1
              FROM ancestors a
              JOIN memory_edges e
                ON e.source_id = a.ancestor
               AND e.relation_type = 'subtype_of'
        )
        SELECT start, ancestor, depth
          FROM ancestors
        """,
        deduped,
    )
    chains: dict[str, dict[str, int]] = {ref: {} for ref in deduped}
    for row in rows:
        start = row["start"]
        ancestor = row["ancestor"]
        depth = int(row["depth"])
        existing = chains[start].get(ancestor)
        # In a DAG with multiple paths, keep the SHORTEST distance — matches
        # the per-step walk semantics of the previous _is_subtype_of helper.
        if existing is None or depth < existing:
            chains[start][ancestor] = depth
    return chains


def _lattice_depth_from_chain(chain: dict[str, int]) -> int:
    """Hops from the seed up to the root (deepest ancestor in its chain)."""
    return max(chain.values()) if chain else 0


def _is_subtype_from_chain(chain: dict[str, int], target_ref: str) -> bool:
    return target_ref in chain


def _template_slots(pg: Any, template_ref: str) -> list[dict[str, Any]]:
    """Return slots for a template, ordered by ordinal.

    memory_edges UNIQUE is (source_id, target_id, relation_type). Two slots
    pointing at the SAME pill_type must use distinct relation_types. We
    accept both ``consumes`` (primary slot) and ``consumes:<slot>``
    (disambiguator for identical target refs).
    """
    rows = pg.execute(
        """
        SELECT relation_type, target_id, metadata
        FROM memory_edges
        WHERE source_id = $1
          AND (relation_type = 'consumes' OR relation_type LIKE 'consumes:%')
        """,
        template_ref,
    )
    slots = []
    for row in rows:
        meta = _as_dict(row["metadata"])
        slots.append({
            "slot_name": meta.get("slot") or (row["relation_type"].split(":", 1)[1] if ":" in row["relation_type"] else "main"),
            "ordinal": int(meta.get("ordinal", 0)),
            "slot_type": row["target_id"],
            "relation_type": row["relation_type"],
        })
    slots.sort(key=lambda s: s["ordinal"])
    return slots


def _fetch_template(pg: Any, template_ref: str) -> dict[str, Any] | None:
    row = pg.fetchrow(
        "SELECT id, name, content, metadata FROM memory_entities WHERE id = $1 AND entity_type = $2",
        template_ref,
        _TEMPLATE_ENTITY_TYPE,
    )
    if row is None:
        return None
    return {
        "template_ref": row["id"],
        "name": row["name"],
        "summary": row["content"],
        "metadata": _as_dict(row["metadata"]),
    }


def _score_template(
    pg: Any,
    template: dict[str, Any],
    pill_refs: list[str],
    *,
    chains: dict[str, dict[str, int]],
) -> dict[str, Any] | None:
    """Admission + specificity scoring against a precomputed ancestor map.

    ``chains`` is the result of one batch ``_load_ancestor_chains`` call
    covering every pill_ref + slot_type relevant to this dispatch. Replaces
    per-slot N+1 walks with O(1) dict lookups.

    Returns None when any slot can't bind (template is illegal for these
    pills). Slot binding is positional by ordinal — pill at ordinal i must
    be a subtype of slot at ordinal i.
    """
    slots = _template_slots(pg, template["template_ref"])
    # Optional slots (e.g. action_rail) are declared in metadata.slot_order
    # and are not required to match a pill. For this wedge we only score
    # slots that have a memory_edges consumes edge — action_rail without
    # an edge is treated as ambient/optional.
    required_slots = [s for s in slots if s["slot_type"].startswith("pill_type.")]

    if len(pill_refs) < len(required_slots):
        return None  # not enough pills to fill required slots

    specificity = 0
    bound_slots: list[dict[str, Any]] = []
    for slot, pill_ref in zip(required_slots, pill_refs):
        pill_chain = chains.get(pill_ref) or {}
        if not _is_subtype_from_chain(pill_chain, slot["slot_type"]):
            return None  # illegal — pill doesn't satisfy slot type
        slot_chain = chains.get(slot["slot_type"]) or {}
        depth = _lattice_depth_from_chain(slot_chain)
        specificity += depth
        bound_slots.append({
            "slot_name": slot["slot_name"],
            "ordinal": slot["ordinal"],
            "slot_type": slot["slot_type"],
            "pill_ref": pill_ref,
            "lattice_depth": depth,
        })

    return {
        "template_ref": template["template_ref"],
        "specificity": specificity,
        "bound_slots": bound_slots,
    }


def _compile_bundle(
    template: dict[str, Any],
    bound_slots: list[dict[str, Any]],
    *,
    intent_ref: str | None = None,
    pill_refs: list[str] | None = None,
) -> dict[str, Any]:
    """Compile a PraxisSurfaceBundleV4 from a template + bound slots.

    The template's ``render_hint`` metadata names which module renders each
    slot. For the first wedge we default to the markdown module for data
    slots and emit an Approve/Reject button-row on the action_rail cells
    when the template declares ``render_hint.action_rail_module='button-row'``.
    Action-rail buttons POST through /api/surface/action carrying the
    typed context (action_ref, intent_ref, template_ref, pill_refs) so
    the click lands as an authority_operation_receipts row.
    """
    render_hint = template["metadata"].get("render_hint", {}) or {}
    slot_order = template["metadata"].get("slot_order") or [bs["slot_name"] for bs in bound_slots]
    title = template.get("name") or template["template_ref"]

    # Map our bound slots into the shape's quadrant footprint.
    # shape.split: left=A1..B2, right=A3..B4, action_rail=C1..C4 (all optional in this wedge)
    slot_to_anchor = {"left": "A1", "right": "A3", "action_rail": "C1"}
    slot_to_span = {"left": "2x2", "right": "2x2", "action_rail": "4x1"}

    quadrants: dict[str, dict[str, Any]] = {}
    for bs in bound_slots:
        anchor = slot_to_anchor.get(bs["slot_name"], "A1")
        span = slot_to_span.get(bs["slot_name"], "1x1")
        module_id = render_hint.get(f"{bs['slot_name']}_module", "markdown")
        quadrants[anchor] = {
            "module": module_id,
            "span": span,
            "config": _render_config_for_slot(bs, module_id),
        }

    # Synthesize action_rail quadrant when the template declares an action-rail
    # module. This slot is not backed by a ``consumes`` edge (it consumes an
    # operation, not a pill type) so the scorer skips it — the compiler
    # materializes the button-row here from render_hint + typed context.
    action_rail_module = render_hint.get("action_rail_module")
    if "action_rail" in slot_order and action_rail_module:
        anchor = slot_to_anchor.get("action_rail", "C1")
        span = slot_to_span.get("action_rail", "4x1")
        action_rail_actions = render_hint.get("action_rail_actions") or []
        quadrants[anchor] = {
            "module": action_rail_module,
            "span": span,
            "config": _render_config_for_action_rail(
                template_ref=template["template_ref"],
                intent_ref=intent_ref,
                pill_refs=pill_refs or [],
                declared_actions=action_rail_actions,
            ),
        }

    manifest = {
        "version": 2,
        "grid": "4x4",
        "title": title,
        "quadrants": quadrants,
    }

    bundle = {
        "version": 4,
        "kind": "praxis_surface_bundle",
        "title": title,
        "default_tab_id": "main",
        "tabs": [
            {"id": "main", "label": title, "surface_id": "main", "source_option_ids": []}
        ],
        "surfaces": {
            "main": {
                "id": "main",
                "title": title,
                "kind": "quadrant_manifest",
                "manifest": manifest,
            }
        },
        "source_options": {},
        "legacy": {"materialized_from_template": template["template_ref"]},
        "description": template.get("summary"),
    }
    return bundle


def _render_config_for_slot(bound_slot: dict[str, Any], module_id: str) -> dict[str, Any]:
    slot_name = bound_slot["slot_name"]
    pill_ref = bound_slot["pill_ref"]
    slot_type = bound_slot["slot_type"]
    if module_id == "markdown":
        return {
            "content": (
                f"### {slot_name.title()} · `{pill_ref}`\n\n"
                f"_Bound type:_ `{slot_type}` (lattice depth {bound_slot['lattice_depth']})\n\n"
                "Slot-type-aware renderer lands in a later packet; this is the "
                "wedge-1 placeholder proving binding + shape compilation."
            ),
        }
    return {}


def _render_config_for_action_rail(
    *,
    template_ref: str,
    intent_ref: str | None,
    pill_refs: list[str],
    declared_actions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Compile a ButtonRowModule config from the template's declared actions.

    The template owns its action set via ``render_hint.action_rail_actions``;
    each declared action carries ``{label, variant?, action_ref}``. The
    compiler weaves the typed dispatch context (intent_ref, template_ref,
    pill_refs, caller_ref) into each button's input. No domain string is
    parsed from intent_ref — the template controls the action_ref directly.

    Falls back to a generic Approve/Reject pair only when the template did
    not declare action_rail_actions, deriving action_ref from the intent's
    domain so legacy templates without explicit declarations still ship a
    meaningful default.
    """
    context = {
        "intent_ref": intent_ref,
        "template_ref": template_ref,
        "pill_refs": pill_refs,
    }
    actions: list[dict[str, Any]] = []
    declared = list(declared_actions or [])
    if declared:
        for raw in declared:
            if not isinstance(raw, dict):
                continue
            label = str(raw.get("label") or "").strip()
            action_ref = str(raw.get("action_ref") or "").strip()
            if not label or not action_ref:
                continue
            variant = raw.get("variant") if raw.get("variant") in ("primary", "danger", "default") else "default"
            actions.append({
                "label": label,
                "variant": variant,
                "operation": "surface.action.performed",
                "input": {
                    **context,
                    "action_ref": action_ref,
                    "caller_ref": "surface.compose.button_row",
                },
            })
    if not actions:
        # Legacy fallback for templates that haven't declared action_rail_actions
        # yet. Subsequent template registrations should always declare them.
        domain = (intent_ref or "intent.surface").split(".", 1)[-1] if intent_ref else "surface"
        actions = [
            {
                "label": "Approve",
                "variant": "primary",
                "operation": "surface.action.performed",
                "input": {**context, "action_ref": f"{domain}.approve", "caller_ref": "surface.compose.button_row"},
            },
            {
                "label": "Reject",
                "variant": "danger",
                "operation": "surface.action.performed",
                "input": {**context, "action_ref": f"{domain}.reject", "caller_ref": "surface.compose.button_row"},
            },
        ]
    return {"actions": actions}


def _typed_gap_for_no_match(intent_ref: str, pill_refs: list[str], candidates: list[str]) -> dict[str, Any]:
    return {
        "gap_kind": "no_legal_template_for_intent",
        "reason_code": "legal_templates_empty",
        "intent_ref": intent_ref,
        "pill_refs": pill_refs,
        "candidate_template_refs": candidates,
        "legal_repair_actions": [
            {
                "action": "register_template",
                "for_intent": intent_ref,
                "with_consumes_hint": pill_refs,
                "via": "surface.template.register (future operation_catalog_registry command)",
            },
            {
                "action": "bind_additional_pill",
                "hint": "Bind pills of types that an existing template for this intent consumes",
            },
        ],
    }


def legal_templates_reducer(
    subs: Any,
    *,
    source_ref: str,
    query_params: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    del source_ref
    params = dict(query_params or {})
    intent_refs = params.get("intent", [])
    pill_refs = params.get("pill", [])
    intent_ref = intent_refs[0] if intent_refs else None

    result: dict[str, Any] = {
        "intent_ref": intent_ref,
        "pill_refs": pill_refs,
        "ranked_templates": [],
        "winner": None,
        "materialized_bundle": None,
        "typed_gap": None,
    }

    if not intent_ref:
        result["typed_gap"] = {
            "gap_kind": "missing_intent_param",
            "reason_code": "intent_required",
            "legal_repair_actions": [
                {"action": "retry", "hint": "call with ?intent=<intent_ref>"},
            ],
        }
        return result

    pg = subs.get_pg_conn()

    # Intent must exist in the graph.
    intent_row = pg.fetchrow(
        "SELECT id FROM memory_entities WHERE id = $1 AND entity_type = $2",
        intent_ref,
        _INTENT_ENTITY_TYPE,
    )
    if intent_row is None:
        result["typed_gap"] = {
            "gap_kind": "intent_not_registered",
            "reason_code": "intent_not_found",
            "intent_ref": intent_ref,
            "legal_repair_actions": [
                {"action": "register_intent", "hint": f"add memory_entities row for {intent_ref}"},
            ],
        }
        return result

    # Pull the candidate templates (edges + weights).
    binding_rows = pg.execute(
        "SELECT target_id as template_ref, weight as binding_weight, metadata "
        "FROM memory_edges WHERE source_id = $1 AND relation_type = 'targets_template'",
        intent_ref,
    )
    candidates = list(binding_rows)

    if not candidates:
        result["typed_gap"] = _typed_gap_for_no_match(intent_ref, pill_refs, [])
        return result

    # Pull every candidate's consumes edges in one query so we know all slot
    # types up-front; then walk the subtype DAG once for {pills + slot_types}
    # via a recursive CTE — replaces per-slot N+1 walks with O(1) dict lookups.
    candidate_template_refs = [row["template_ref"] for row in candidates]
    consumes_rows = list(pg.execute(
        """
        SELECT source_id AS template_ref, target_id AS slot_type
          FROM memory_edges
         WHERE source_id = ANY($1::text[])
           AND (relation_type = 'consumes' OR relation_type LIKE 'consumes:%')
        """,
        candidate_template_refs,
    ))
    slot_types = {row["slot_type"] for row in consumes_rows if row["slot_type"].startswith("pill_type.")}
    chains = _load_ancestor_chains(pg, list({*pill_refs, *slot_types}))

    # Score each candidate.
    scored: list[dict[str, Any]] = []
    for row in candidates:
        template_ref = row["template_ref"]
        binding_weight = float(row["binding_weight"] or 1.0)
        template = _fetch_template(pg, template_ref)
        if template is None:
            continue
        scoring = _score_template(pg, template, pill_refs, chains=chains)
        if scoring is None:
            # Illegal candidate — tracked in the response for debuggability.
            scored.append({
                "template_ref": template_ref,
                "legal": False,
                "reason": "slot_type_mismatch_or_insufficient_pills",
                "binding_weight": binding_weight,
            })
            continue
        rank = (scoring["specificity"] + 1) * binding_weight  # +1 so a base-type template is still non-zero
        scored.append({
            "template_ref": template_ref,
            "legal": True,
            "specificity": scoring["specificity"],
            "binding_weight": binding_weight,
            "rank": rank,
            "bound_slots": scoring["bound_slots"],
            "template_name": template.get("name"),
            "template": template,
        })

    legal_scored = [s for s in scored if s.get("legal")]
    legal_scored.sort(key=lambda s: s["rank"], reverse=True)

    result["ranked_templates"] = [
        {k: v for k, v in s.items() if k != "template"}
        for s in scored
    ]

    if not legal_scored:
        result["typed_gap"] = _typed_gap_for_no_match(intent_ref, pill_refs, [s["template_ref"] for s in scored])
        return result

    winner = legal_scored[0]
    result["winner"] = winner["template_ref"]
    result["materialized_bundle"] = _compile_bundle(
        winner["template"],
        winner["bound_slots"],
        intent_ref=intent_ref,
        pill_refs=pill_refs,
    )
    return result
