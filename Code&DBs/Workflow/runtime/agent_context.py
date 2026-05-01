"""Trust-compiled context envelope for one agent_principal wake.

When a chat message, schedule firing, webhook, or delegation wakes an
agent_principal, runtime.triggers calls ``compile_agent_context`` to
produce the inline workflow spec the wake will run as. The envelope
narrows the agent's choice space at the moment of action:

  * standing orders (loaded from operator_decisions filtered to the
    agent's standing_order_keys) are inlined in the system prompt
  * the agent's write_envelope, allowed_tools, integration_refs, and
    network_policy come along on the inline_spec.metadata so the
    sandbox runtime + execution policy honor them
  * recent wakes + recent chat turns are summarised so the agent has
    short-term memory across triggers without a separate memory layer

This module owns no SQL of its own beyond simple SELECTs against
agent_principals, agent_wakes, operator_decisions, and chat_messages.
Mutations live in runtime.operations.commands.agent_principals.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any, Mapping, Sequence


@dataclass(frozen=True, slots=True)
class AgentContextEnvelope:
    """Output of compile_agent_context — feeds runtime.triggers.

    inline_spec is suitable for ``submit_workflow_command(inline_spec=...)``.
    """

    agent_principal_ref: str
    agent_status: str
    network_policy: str
    write_envelope: tuple[str, ...]
    allowed_tools: tuple[str, ...]
    integration_refs: tuple[str, ...]
    capability_refs: tuple[str, ...]
    standing_order_keys: tuple[str, ...]
    standing_orders_text: str
    inline_spec: Mapping[str, Any]
    payload_hash: str


def _normalise_jsonb_list(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return ()
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(str(item) for item in value if isinstance(item, (str, int, float)))


def _payload_hash(agent_ref: str, trigger_kind: str, payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        {"agent": agent_ref, "kind": trigger_kind, "payload": payload},
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def load_agent_principal(conn: Any, agent_principal_ref: str) -> Mapping[str, Any] | None:
    rows = conn.execute(
        """SELECT
               agent_principal_ref,
               title,
               status,
               max_in_flight_wakes,
               write_envelope,
               capability_refs,
               integration_refs,
               standing_order_keys,
               allowed_tools,
               network_policy,
               default_conversation_id,
               routing_policy,
               metadata
           FROM agent_registry
           WHERE agent_principal_ref = $1
           LIMIT 1""",
        agent_principal_ref,
    )
    if not rows:
        return None
    return dict(rows[0])


def load_standing_orders(
    conn: Any,
    decision_keys: Sequence[str],
) -> list[Mapping[str, Any]]:
    if not decision_keys:
        return []
    rows = conn.execute(
        """SELECT
               decision_key,
               decision_kind,
               decision_status,
               title,
               rationale,
               decision_scope_kind,
               decision_scope_ref
           FROM operator_decisions
           WHERE decision_key = ANY($1::text[])
             AND decision_status IN ('active', 'decided')
             AND effective_from <= now()
             AND (effective_to IS NULL OR effective_to > now())
           ORDER BY decision_key""",
        list(decision_keys),
    )
    return [dict(row) for row in (rows or [])]


def load_recent_wakes(
    conn: Any,
    agent_principal_ref: str,
    *,
    limit: int = 5,
) -> list[Mapping[str, Any]]:
    rows = conn.execute(
        """SELECT
               wake_id,
               trigger_kind,
               trigger_source_ref,
               status,
               run_id,
               received_at,
               completed_at,
               skip_reason
           FROM agent_wakes
           WHERE agent_principal_ref = $1
           ORDER BY received_at DESC
           LIMIT $2""",
        agent_principal_ref,
        int(limit),
    )
    return [dict(row) for row in (rows or [])]


def load_recent_chat_turns(
    conn: Any,
    conversation_id: str | None,
    *,
    limit: int = 10,
) -> list[Mapping[str, Any]]:
    if not conversation_id:
        return []
    rows = conn.execute(
        """SELECT id, role, content, created_at
           FROM chat_messages
           WHERE conversation_id = $1
           ORDER BY created_at DESC
           LIMIT $2""",
        conversation_id,
        int(limit),
    )
    return list(reversed([dict(row) for row in (rows or [])]))


def in_flight_wake_count(conn: Any, agent_principal_ref: str) -> int:
    rows = conn.execute(
        """SELECT COUNT(*)::int AS n
           FROM agent_wakes
           WHERE agent_principal_ref = $1
             AND status IN ('pending', 'dispatched')""",
        agent_principal_ref,
    )
    return int(rows[0]["n"]) if rows else 0


def _format_standing_orders(orders: Sequence[Mapping[str, Any]]) -> str:
    if not orders:
        return "(no standing orders bound to this principal)"
    lines: list[str] = []
    for order in orders:
        key = str(order.get("decision_key") or "").strip()
        title = str(order.get("title") or "").strip() or key
        rationale = str(order.get("rationale") or "").strip()
        block = f"• [{key}] {title}"
        if rationale:
            block += f"\n    {rationale}"
        lines.append(block)
    return "\n".join(lines)


def _format_recent_wakes(wakes: Sequence[Mapping[str, Any]]) -> str:
    if not wakes:
        return "(no prior wakes on record)"
    lines = []
    for wake in wakes:
        kind = str(wake.get("trigger_kind") or "?").strip()
        status = str(wake.get("status") or "?").strip()
        ts = wake.get("received_at")
        ts_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts or "")
        skip = wake.get("skip_reason")
        suffix = f" skip={skip}" if skip else ""
        lines.append(f"• {ts_str} kind={kind} status={status}{suffix}")
    return "\n".join(lines)


def _format_recent_chat(turns: Sequence[Mapping[str, Any]]) -> str:
    if not turns:
        return "(no prior chat on this conversation)"
    lines = []
    for turn in turns:
        role = str(turn.get("role") or "?").strip()
        content = str(turn.get("content") or "").strip()
        if len(content) > 400:
            content = content[:400] + "…"
        lines.append(f"[{role}] {content}")
    return "\n".join(lines)


def _build_system_prompt(
    *,
    agent: Mapping[str, Any],
    trigger_kind: str,
    trigger_source_ref: str | None,
    payload: Mapping[str, Any],
    standing_orders: Sequence[Mapping[str, Any]],
    recent_wakes: Sequence[Mapping[str, Any]],
    recent_chat: Sequence[Mapping[str, Any]],
) -> str:
    write_envelope = _normalise_jsonb_list(agent.get("write_envelope"))
    integration_refs = _normalise_jsonb_list(agent.get("integration_refs"))
    capability_refs = _normalise_jsonb_list(agent.get("capability_refs"))
    allowed_tools = _normalise_jsonb_list(agent.get("allowed_tools"))
    network_policy = str(agent.get("network_policy") or "praxis_only")

    payload_excerpt = json.dumps(payload, default=str, sort_keys=True)
    if len(payload_excerpt) > 1500:
        payload_excerpt = payload_excerpt[:1500] + "…"

    return (
        f"You are {agent['agent_principal_ref']} ({agent.get('title') or agent['agent_principal_ref']}).\n"
        f"You are a durable LLM principal in Praxis Engine. This is one bounded wake — "
        f"do the work the trigger asks for, write a closeout summary, and stop.\n"
        f"\n"
        f"# Trigger\n"
        f"kind: {trigger_kind}\n"
        f"source_ref: {trigger_source_ref or '(none)'}\n"
        f"payload (truncated):\n{payload_excerpt}\n"
        f"\n"
        f"# Scope cage (enforced by sandbox + gateway)\n"
        f"write_envelope: {list(write_envelope) or '(empty — read-only)'}\n"
        f"network_policy: {network_policy}  "
        f"# praxis_only = MCP bridge + admitted provider routes only, no open web\n"
        f"integration_refs: {list(integration_refs) or '(none — Praxis-internal only)'}\n"
        f"capability_refs: {list(capability_refs) or '(none)'}\n"
        f"allowed_tools: {list(allowed_tools) or '(unrestricted within capability_refs)'}\n"
        f"\n"
        f"# Standing orders bound to this principal\n"
        f"{_format_standing_orders(standing_orders)}\n"
        f"\n"
        f"# Recent wakes (your short-term memory)\n"
        f"{_format_recent_wakes(recent_wakes)}\n"
        f"\n"
        f"# Recent chat on the pinned conversation\n"
        f"{_format_recent_chat(recent_chat)}\n"
        f"\n"
        f"# How to close out\n"
        f"When the bounded action is complete, post one short summary turn back into "
        f"the pinned conversation (or, if no conversation is pinned, write a summary "
        f"artifact under your write_envelope). The wake row's closeout_receipt_id is "
        f"stamped automatically from your last gateway-dispatched receipt.\n"
        f"\n"
        f"# If you need a tool that doesn't exist\n"
        f"File an agent_tool_gap via the agent_tool_gap.file command. Do not improvise "
        f"around a missing capability — gaps are roadmap fuel.\n"
    )


def compile_agent_context(
    conn: Any,
    *,
    agent_principal_ref: str,
    trigger_kind: str,
    trigger_source_ref: str | None = None,
    payload: Mapping[str, Any] | None = None,
    history_limit: int = 5,
    chat_limit: int = 10,
) -> AgentContextEnvelope | None:
    """Compile the trust envelope for one wake. Returns None if the agent
    is missing or non-active (caller should record a skip)."""

    payload_dict = dict(payload or {})
    agent = load_agent_principal(conn, agent_principal_ref)
    if agent is None:
        return None

    status = str(agent.get("status") or "").strip()
    if status != "active":
        return AgentContextEnvelope(
            agent_principal_ref=agent_principal_ref,
            agent_status=status or "unknown",
            network_policy=str(agent.get("network_policy") or "praxis_only"),
            write_envelope=_normalise_jsonb_list(agent.get("write_envelope")),
            allowed_tools=_normalise_jsonb_list(agent.get("allowed_tools")),
            integration_refs=_normalise_jsonb_list(agent.get("integration_refs")),
            capability_refs=_normalise_jsonb_list(agent.get("capability_refs")),
            standing_order_keys=_normalise_jsonb_list(agent.get("standing_order_keys")),
            standing_orders_text="(skipped: agent status is not active)",
            inline_spec={},
            payload_hash=_payload_hash(agent_principal_ref, trigger_kind, payload_dict),
        )

    decision_keys = _normalise_jsonb_list(agent.get("standing_order_keys"))
    standing_orders = load_standing_orders(conn, decision_keys)
    recent_wakes = load_recent_wakes(conn, agent_principal_ref, limit=history_limit)
    recent_chat = load_recent_chat_turns(
        conn,
        agent.get("default_conversation_id"),
        limit=chat_limit,
    )

    system_prompt = _build_system_prompt(
        agent=agent,
        trigger_kind=trigger_kind,
        trigger_source_ref=trigger_source_ref,
        payload=payload_dict,
        standing_orders=standing_orders,
        recent_wakes=recent_wakes,
        recent_chat=recent_chat,
    )

    write_envelope = _normalise_jsonb_list(agent.get("write_envelope"))
    integration_refs = _normalise_jsonb_list(agent.get("integration_refs"))
    capability_refs = _normalise_jsonb_list(agent.get("capability_refs"))
    allowed_tools = _normalise_jsonb_list(agent.get("allowed_tools"))
    network_policy = str(agent.get("network_policy") or "praxis_only")

    inline_spec: dict[str, Any] = {
        "name": f"agent_wake::{agent_principal_ref}::{trigger_kind}",
        "metadata": {
            "agent_principal_ref": agent_principal_ref,
            "agent_title": agent.get("title"),
            "trigger_kind": trigger_kind,
            "trigger_source_ref": trigger_source_ref,
            "execution_bundle": {
                # Phase B cage: mcp_tool_names is what the workflow MCP
                # token minter consumes (runtime.workflow.mcp_bridge.
                # workflow_mcp_tool_names → mint_workflow_mcp_session_token).
                # Closes BUG-CE7D35D4 — admitted tools were declared in
                # access_policy.allowed_tools but the minter ignored that
                # list. Top-level mcp_tool_names is the executable cage;
                # access_policy.allowed_tools stays as the audit-readable
                # declaration.
                "mcp_tool_names": list(allowed_tools),
                "access_policy": {
                    "write_scope": list(write_envelope),
                    "network_policy": network_policy,
                    "integration_refs": list(integration_refs),
                    "capability_refs": list(capability_refs),
                    "allowed_tools": list(allowed_tools),
                }
            },
        },
        "jobs": [
            {
                "label": "agent_wake",
                "prompt": system_prompt,
                "task_type": "agent_wake",
                "max_iterations": 8,
                "metadata": {
                    "agent_principal_ref": agent_principal_ref,
                    "trigger_kind": trigger_kind,
                },
            }
        ],
    }

    standing_orders_text = _format_standing_orders(standing_orders)

    return AgentContextEnvelope(
        agent_principal_ref=agent_principal_ref,
        agent_status="active",
        network_policy=network_policy,
        write_envelope=write_envelope,
        allowed_tools=allowed_tools,
        integration_refs=integration_refs,
        capability_refs=capability_refs,
        standing_order_keys=decision_keys,
        standing_orders_text=standing_orders_text,
        inline_spec=inline_spec,
        payload_hash=_payload_hash(agent_principal_ref, trigger_kind, payload_dict),
    )


__all__ = [
    "AgentContextEnvelope",
    "compile_agent_context",
    "in_flight_wake_count",
    "load_agent_principal",
    "load_recent_chat_turns",
    "load_recent_wakes",
    "load_standing_orders",
]
