"""praxis_agent_forge — preview the canonical CQRS path for adding or
evolving a durable agent principal.

Mirrors ``runtime.operations.queries.operator_composed.handle_query_operation_forge``
but for agents instead of operations. The wizard is preview-only: it
reads existing rows, validates inputs against authoritative tables
(operator_decisions for standing_order_keys, capability_catalog for
capability_refs, integration_registry for integration_refs), and
produces the exact ``agent_principal.register`` payload the caller
should send next, plus the canonical write order and reject paths.

Use case: before anyone hand-writes an agent_principals row or a
sibling agent runtime, ``praxis_agent_forge`` makes the canonical
path explicit and validates the inputs against live authority. This
is the agent equivalent of operator-decision standing order
``architecture-policy::agent-behavior::cqrs-wizard-before-cqrs-edits``
applied to agent registration.

Aligns with the planned A2A roadmap (
``roadmap_item.a2a.native.stateful.server.for.agentic.work.agent.registry.and.routing.control.plane``)
by treating the durable agent record as the authority object — the
forge is the canonical entry point regardless of whether the caller
later goes through ``agent_principal.register`` (current) or
``a2a_agent.register`` (when the A2A kernel lands).
"""

from __future__ import annotations

import json
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic input model
# ─────────────────────────────────────────────────────────────────────────────


class QueryAgentForge(BaseModel):
    """Preview the canonical agent registration path."""

    agent_principal_ref: str = Field(
        ...,
        min_length=1,
        description="Durable agent identity, e.g. 'agent.exec.nate'.",
    )
    title: str | None = Field(
        default=None,
        description="Human-readable title. Falls back to agent_principal_ref.",
    )
    mission: str | None = Field(
        default=None,
        description=(
            "Plain-prose description of what this agent is for — appears in "
            "the predicted prompt context the trust compiler will produce."
        ),
    )
    status: Literal["active", "paused", "killed"] = "active"
    max_in_flight_wakes: int = Field(default=1, ge=1, le=16)
    write_envelope: list[str] = Field(
        default_factory=list,
        description="Path globs the agent is allowed to write to.",
    )
    capability_refs: list[str] = Field(
        default_factory=list,
        description="capability_catalog entries this agent can fulfil.",
    )
    integration_refs: list[str] = Field(
        default_factory=list,
        description="integration_registry ids this agent may invoke.",
    )
    standing_order_keys: list[str] = Field(
        default_factory=list,
        description=(
            "operator_decisions decision_keys the agent must obey at every wake. "
            "The forge validates each key exists with status in ('decided','active')."
        ),
    )
    allowed_tools: list[str] = Field(
        default_factory=list,
        description=(
            "Tool name allowlist. Empty means 'unrestricted within capability_refs'."
        ),
    )
    network_policy: Literal["disabled", "provider_only", "praxis_only", "enabled"] = (
        "praxis_only"
    )
    default_conversation_id: str | None = None
    routing_policy: dict[str, Any] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    decision_ref: str | None = Field(
        default=None,
        description=(
            "operator_decision that anchors this agent's existence. "
            "Defaults to the business-agent-substrate authority decision."
        ),
    )

    @field_validator("agent_principal_ref")
    @classmethod
    def _strip_ref(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("agent_principal_ref must be non-empty")
        return stripped


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


_DEFAULT_DECISION_REF = (
    "architecture-policy::business-agent-substrate::"
    "delegated-workers-praxis-only-no-internet"
)


def _shell_json(payload: dict[str, Any]) -> str:
    return "'" + json.dumps(payload, sort_keys=True).replace("'", "'\\''") + "'"


def _verify_decision_keys(conn: Any, keys: list[str]) -> tuple[list[str], list[str]]:
    """Return (resolved_keys, unresolved_keys) for the given decision_keys."""
    if not keys:
        return [], []
    rows = conn.execute(
        """SELECT decision_key
           FROM operator_decisions
           WHERE decision_key = ANY($1::text[])
             AND decision_status IN ('active', 'decided')""",
        list(keys),
    )
    resolved = {str(r["decision_key"]) for r in (rows or [])}
    return (
        sorted(k for k in keys if k in resolved),
        sorted(k for k in keys if k not in resolved),
    )


def _verify_capability_refs(conn: Any, refs: list[str]) -> tuple[list[str], list[str]]:
    if not refs:
        return [], []
    try:
        rows = conn.execute(
            """SELECT capability_ref FROM capability_catalog
               WHERE capability_ref = ANY($1::text[])""",
            list(refs),
        )
    except Exception:
        # capability_catalog may not be present in every environment;
        # fail soft so the forge stays useful even mid-migration
        return list(refs), []
    resolved = {str(r["capability_ref"]) for r in (rows or [])}
    return (
        sorted(r for r in refs if r in resolved),
        sorted(r for r in refs if r not in resolved),
    )


def _verify_integration_refs(conn: Any, refs: list[str]) -> tuple[list[str], list[str]]:
    if not refs:
        return [], []
    try:
        rows = conn.execute(
            "SELECT id FROM integration_registry WHERE id = ANY($1::text[])",
            list(refs),
        )
    except Exception:
        return list(refs), []
    resolved = {str(r["id"]) for r in (rows or [])}
    return (
        sorted(r for r in refs if r in resolved),
        sorted(r for r in refs if r not in resolved),
    )


def _existing_agent(conn: Any, ref: str) -> dict[str, Any] | None:
    rows = conn.execute(
        """SELECT
               agent_principal_ref, title, status, max_in_flight_wakes,
               network_policy, default_conversation_id, decision_ref,
               jsonb_array_length(write_envelope)     AS write_envelope_size,
               jsonb_array_length(capability_refs)    AS capability_refs_size,
               jsonb_array_length(integration_refs)   AS integration_refs_size,
               jsonb_array_length(standing_order_keys) AS standing_order_keys_size,
               jsonb_array_length(allowed_tools)      AS allowed_tools_size,
               created_at, updated_at
           FROM agent_registry
           WHERE agent_principal_ref = $1
           LIMIT 1""",
        ref,
    )
    if not rows:
        return None
    row = dict(rows[0])
    if row.get("created_at"):
        row["created_at"] = row["created_at"].isoformat()
    if row.get("updated_at"):
        row["updated_at"] = row["updated_at"].isoformat()
    return row


# ─────────────────────────────────────────────────────────────────────────────
# Handler
# ─────────────────────────────────────────────────────────────────────────────


def handle_query_agent_forge(
    query: QueryAgentForge,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()

    existing = _existing_agent(conn, query.agent_principal_ref)
    title = (query.title or query.agent_principal_ref).strip()
    decision_ref = query.decision_ref or _DEFAULT_DECISION_REF

    standing_orders_resolved, standing_orders_unresolved = _verify_decision_keys(
        conn, list(query.standing_order_keys)
    )
    capabilities_resolved, capabilities_unresolved = _verify_capability_refs(
        conn, list(query.capability_refs)
    )
    integrations_resolved, integrations_unresolved = _verify_integration_refs(
        conn, list(query.integration_refs)
    )

    register_payload = {
        "agent_principal_ref": query.agent_principal_ref,
        "title": title,
        "status": query.status,
        "max_in_flight_wakes": int(query.max_in_flight_wakes),
        "write_envelope": list(query.write_envelope),
        "capability_refs": list(query.capability_refs),
        "integration_refs": list(query.integration_refs),
        "standing_order_keys": list(query.standing_order_keys),
        "allowed_tools": list(query.allowed_tools),
        "network_policy": query.network_policy,
        "default_conversation_id": query.default_conversation_id,
        "routing_policy": query.routing_policy,
        "metadata": dict(query.metadata or {}),
    }
    if query.mission:
        register_payload["metadata"]["mission"] = query.mission

    missing: list[str] = []
    if not query.write_envelope and query.status == "active":
        # Active agents must declare a write_envelope or be explicitly read-only;
        # an empty envelope on an active agent is the failure mode we caught last
        # cycle (sandbox would reject every write).
        missing.append("write_envelope")
    if not query.standing_order_keys:
        # An agent with no standing orders has no policy cage. Refuse the forge.
        missing.append("standing_order_keys")
    if standing_orders_unresolved:
        missing.append("standing_order_keys (unresolved)")
    if capabilities_unresolved:
        missing.append("capability_refs (unresolved)")
    if integrations_unresolved:
        missing.append("integration_refs (unresolved)")
    if (
        query.network_policy == "praxis_only"
        and not query.metadata.get("acknowledges_network_policy")
        and not query.mission
    ):
        # praxis_only is the cage; if the caller hasn't articulated a mission
        # we can't tell whether the cage matches what they actually need.
        missing.append("mission (or metadata.acknowledges_network_policy)")

    register_command = (
        "praxis workflow tools call praxis_agent_principal_register --input-json "
        f"{_shell_json(register_payload)} --yes"
    )

    fast_feedback_commands = [
        # Per the standing order architecture-policy::agent-behavior::
        # cqrs-wizard-before-cqrs-edits, every agent registration must be
        # preceded by this preview. These commands verify the registration
        # round-trips cleanly.
        'PYTHONPATH="Code&DBs/Workflow" .venv/bin/python -m pytest --noconftest -q '
        "Code&DBs/Workflow/tests/unit/test_agent_principals_ops.py "
        "Code&DBs/Workflow/tests/unit/test_agent_forge.py",
        f"praxis workflow tools call agent_principal.describe --input-json "
        f"'{{\"agent_principal_ref\":\"{query.agent_principal_ref}\"}}'",
        f"praxis workflow tools call agent_wake.list --input-json "
        f"'{{\"agent_principal_ref\":\"{query.agent_principal_ref}\",\"limit\":10}}'",
    ]

    return {
        "ok": True,
        "operation": "agent.query.forge",
        "view": "agent_forge",
        "authority": (
            "agent_principals + authority_object_registry + data_dictionary_objects"
        ),
        "state": "existing_agent" if existing else "new_agent",
        "existing_agent": existing,
        "register_payload": register_payload,
        "validation": {
            "standing_orders_resolved": standing_orders_resolved,
            "standing_orders_unresolved": standing_orders_unresolved,
            "capabilities_resolved": capabilities_resolved,
            "capabilities_unresolved": capabilities_unresolved,
            "integrations_resolved": integrations_resolved,
            "integrations_unresolved": integrations_unresolved,
        },
        "next_action_packet": {
            "intent": {
                "agent_principal_ref": query.agent_principal_ref,
                "title": title,
                "status": query.status,
                "network_policy": query.network_policy,
            },
            "write_order": [
                "Resolve any unresolved standing_order_keys / capability_refs / "
                "integration_refs before registering.",
                "Run the agent_forge preview against final inputs and confirm "
                "missing_inputs is empty.",
                "Register through agent_principal.register (idempotent on "
                "agent_principal_ref) — this is the canonical write path.",
                "Wire workflow_triggers rows for any cron / webhook wakes the "
                "agent should respond to (target_kind='agent_wake', "
                "target_ref=<agent_principal_ref>).",
                "Pin a default_conversation_id only when chat-trigger wakes are "
                "wanted; otherwise leave NULL.",
                "Run focused tests: test_agent_principals_ops + test_agent_forge.",
            ],
            "register_command": register_command,
            "fast_feedback_commands": fast_feedback_commands,
            "success_evidence": [
                "agent_principals row exists with the requested status and scope",
                "every standing_order_key resolves in operator_decisions",
                "agent_principal.registered event was emitted on register",
                "agent_principal.describe returns the full envelope including "
                "recent_wakes, recent_delegations, recent_tool_gaps lists",
            ],
        },
        "decision_ref": decision_ref,
        "missing_inputs": missing,
        "recommended_path": [
            "Use agent_forge to validate every new agent or scope change before "
            "calling agent_principal.register.",
            "Bind standing_order_keys to existing operator_decisions — never "
            "invent a key the operator hasn't filed.",
            "Default to network_policy='praxis_only' for delegated workers; "
            "elevate only with an explicit operator_decision that scopes the "
            "elevation.",
            "Keep write_envelope tight (one path glob is usually enough); the "
            "envelope is the cage, not the promise.",
            "Pin default_conversation_id only when you want chat-trigger wakes "
            "in /console; for cron-only or webhook-only agents leave it NULL.",
        ],
        "reject_paths": [
            "Do not seed an agent without standing_order_keys — an agent with "
            "no policy cage is not first-class.",
            "Do not bind standing_order_keys that don't exist in "
            "operator_decisions; the forge will mark them unresolved and the "
            "register call will succeed but the agent will load empty policy "
            "context at every wake.",
            "Do not declare capability_refs that aren't in capability_catalog — "
            "the trust compiler will not bind the agent to those capabilities.",
            "Do not seed multiple agents with the same default_conversation_id; "
            "each pinned conversation belongs to exactly one principal.",
            "Do not skip agent_forge before hand-writing a migration that "
            "registers an agent — drift between migration and agent_principal."
            "register's contract is the failure mode the wizard prevents.",
        ],
        "ok_to_register": existing is None and not missing,
    }


__all__ = ["QueryAgentForge", "handle_query_agent_forge"]
