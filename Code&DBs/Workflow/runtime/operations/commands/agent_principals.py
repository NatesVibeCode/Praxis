"""CQRS commands for agent_principal authority.

Owns:
  agent_principal.register      upsert one agent_principals row
  agent_principal.update_status flip active|paused|killed
  agent_wake.request            insert pending wake row + emit
                                agent.wake.requested system event
  agent_tool_gap.file           file a gap row + emit agent.tool_gap.filed

Delegation lives in ``runtime.operations.commands.agent_delegate``.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic command models
# ─────────────────────────────────────────────────────────────────────────────


class RegisterAgentPrincipalCommand(BaseModel):
    """Upsert one agent_principals row."""

    agent_principal_ref: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)
    status: Literal["active", "paused", "killed"] = "active"
    max_in_flight_wakes: int = Field(default=1, ge=1, le=16)
    write_envelope: list[str] = Field(default_factory=list)
    capability_refs: list[str] = Field(default_factory=list)
    integration_refs: list[str] = Field(default_factory=list)
    standing_order_keys: list[str] = Field(default_factory=list)
    allowed_tools: list[str] = Field(default_factory=list)
    network_policy: Literal["disabled", "provider_only", "praxis_only", "enabled"] = (
        "praxis_only"
    )
    default_conversation_id: str | None = None
    routing_policy: dict[str, Any] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("agent_principal_ref", "title")
    @classmethod
    def _strip_required(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("must be non-empty")
        return stripped


class UpdateAgentPrincipalStatusCommand(BaseModel):
    """Flip an agent principal's status."""

    agent_principal_ref: str = Field(..., min_length=1)
    status: Literal["active", "paused", "killed"]
    reason: str | None = None


class RequestAgentWakeCommand(BaseModel):
    """Insert one pending agent_wakes row + emit agent.wake.requested."""

    agent_principal_ref: str = Field(..., min_length=1)
    trigger_kind: Literal["chat", "schedule", "webhook", "delegation", "manual"]
    trigger_source_ref: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class FileAgentToolGapCommand(BaseModel):
    """Worker files a tool gap when Praxis lacks a needed capability."""

    reporter_agent_ref: str = Field(..., min_length=1)
    missing_capability: str = Field(..., min_length=1)
    attempted_task: str = Field(..., min_length=1)
    blocked_action: str = Field(..., min_length=1)
    reporter_run_id: str | None = None
    reporter_delegation_id: str | None = None
    admitted_tools: list[str] = Field(default_factory=list)
    suggested_tool_contract: dict[str, Any] | None = None
    evidence: dict[str, Any] = Field(default_factory=dict)
    severity: Literal["low", "medium", "high", "blocking"] = "medium"


# ─────────────────────────────────────────────────────────────────────────────
# Handlers
# ─────────────────────────────────────────────────────────────────────────────


def _payload_hash(agent_ref: str, trigger_kind: str, payload: dict[str, Any]) -> str:
    canonical = json.dumps(
        {"agent": agent_ref, "kind": trigger_kind, "payload": payload},
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def handle_register_agent_principal(
    command: RegisterAgentPrincipalCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    rows = conn.execute(
        """INSERT INTO agent_principals (
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
           )
           VALUES (
               $1, $2, $3, $4,
               $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb, $9::jsonb,
               $10, $11, $12::jsonb, $13::jsonb
           )
           ON CONFLICT (agent_principal_ref) DO UPDATE SET
               title = EXCLUDED.title,
               status = EXCLUDED.status,
               max_in_flight_wakes = EXCLUDED.max_in_flight_wakes,
               write_envelope = EXCLUDED.write_envelope,
               capability_refs = EXCLUDED.capability_refs,
               integration_refs = EXCLUDED.integration_refs,
               standing_order_keys = EXCLUDED.standing_order_keys,
               allowed_tools = EXCLUDED.allowed_tools,
               network_policy = EXCLUDED.network_policy,
               default_conversation_id = EXCLUDED.default_conversation_id,
               routing_policy = EXCLUDED.routing_policy,
               metadata = EXCLUDED.metadata,
               updated_at = now()
           RETURNING agent_principal_ref, status, updated_at""",
        command.agent_principal_ref,
        command.title,
        command.status,
        int(command.max_in_flight_wakes),
        json.dumps(command.write_envelope),
        json.dumps(command.capability_refs),
        json.dumps(command.integration_refs),
        json.dumps(command.standing_order_keys),
        json.dumps(command.allowed_tools),
        command.network_policy,
        command.default_conversation_id,
        json.dumps(command.routing_policy) if command.routing_policy is not None else None,
        json.dumps(command.metadata),
    )
    if not rows:
        return {
            "ok": False,
            "operation": "agent_principal.register",
            "error_code": "agent_principal.upsert_returned_no_row",
        }
    row = dict(rows[0])
    try:
        from runtime.system_events import emit_system_event

        emit_system_event(
            conn,
            event_type="agent_principal.registered",
            source_id=row["agent_principal_ref"],
            source_type="agent_principal",
            payload={
                "agent_principal_ref": row["agent_principal_ref"],
                "status": row["status"],
                "title": command.title,
            },
        )
    except Exception:
        pass
    return {
        "ok": True,
        "operation": "agent_principal.register",
        "agent_principal_ref": row["agent_principal_ref"],
        "status": row["status"],
        "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None,
    }


def handle_update_agent_principal_status(
    command: UpdateAgentPrincipalStatusCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    rows = conn.execute(
        """UPDATE agent_principals
              SET status = $2, updated_at = now()
            WHERE agent_principal_ref = $1
        RETURNING agent_principal_ref, status, updated_at""",
        command.agent_principal_ref,
        command.status,
    )
    if not rows:
        return {
            "ok": False,
            "operation": "agent_principal.update_status",
            "error_code": "agent_principal.not_found",
            "agent_principal_ref": command.agent_principal_ref,
        }
    row = dict(rows[0])
    try:
        from runtime.system_events import emit_system_event

        emit_system_event(
            conn,
            event_type="agent_principal.status_updated",
            source_id=row["agent_principal_ref"],
            source_type="agent_principal",
            payload={
                "agent_principal_ref": row["agent_principal_ref"],
                "status": row["status"],
                "reason": command.reason,
            },
        )
    except Exception:
        pass
    return {
        "ok": True,
        "operation": "agent_principal.update_status",
        "agent_principal_ref": row["agent_principal_ref"],
        "status": row["status"],
        "reason": command.reason,
        "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None,
    }


def handle_request_agent_wake(
    command: RequestAgentWakeCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    payload_hash = _payload_hash(
        command.agent_principal_ref, command.trigger_kind, command.payload
    )

    principal_rows = conn.execute(
        "SELECT status FROM agent_principals WHERE agent_principal_ref = $1",
        command.agent_principal_ref,
    )
    if not principal_rows:
        return {
            "ok": False,
            "operation": "agent_wake.request",
            "error_code": "agent_principal.not_found",
            "agent_principal_ref": command.agent_principal_ref,
        }

    rows = conn.execute(
        """INSERT INTO agent_wakes (
               agent_principal_ref, trigger_kind, trigger_source_ref,
               payload, payload_hash, status
           )
           VALUES ($1, $2, $3, $4::jsonb, $5, 'pending')
           ON CONFLICT (agent_principal_ref, trigger_kind, payload_hash)
               WHERE payload_hash IS NOT NULL
               DO NOTHING
           RETURNING wake_id, received_at""",
        command.agent_principal_ref,
        command.trigger_kind,
        command.trigger_source_ref,
        json.dumps(command.payload, default=str),
        payload_hash,
    )
    if not rows:
        # Duplicate wake — dedup hit. Return idempotent success.
        existing = conn.execute(
            """SELECT wake_id, status, run_id
               FROM agent_wakes
               WHERE agent_principal_ref = $1
                 AND trigger_kind = $2
                 AND payload_hash = $3
               LIMIT 1""",
            command.agent_principal_ref,
            command.trigger_kind,
            payload_hash,
        )
        if existing:
            existing_row = dict(existing[0])
            return {
                "ok": True,
                "operation": "agent_wake.request",
                "wake_id": str(existing_row["wake_id"]),
                "duplicate": True,
                "status": existing_row.get("status"),
                "run_id": existing_row.get("run_id"),
            }
        return {
            "ok": False,
            "operation": "agent_wake.request",
            "error_code": "agent_wake.dedup_collision_no_row",
        }

    wake_id = str(rows[0]["wake_id"])
    try:
        from runtime.system_events import emit_system_event

        emit_system_event(
            conn,
            event_type="agent.wake.requested",
            source_id=str(wake_id),
            source_type="agent_wake",
            payload={
                "wake_id": wake_id,
                "agent_principal_ref": command.agent_principal_ref,
                "trigger_kind": command.trigger_kind,
                "trigger_source_ref": command.trigger_source_ref,
                "payload_hash": payload_hash,
            },
        )
    except Exception:
        # Falling through is OK — the agent_wakes row exists; the trigger
        # evaluator can also be triggered by the schedule.fired path.
        pass

    return {
        "ok": True,
        "operation": "agent_wake.request",
        "wake_id": wake_id,
        "agent_principal_ref": command.agent_principal_ref,
        "trigger_kind": command.trigger_kind,
        "duplicate": False,
        "payload_hash": payload_hash,
    }


def handle_file_agent_tool_gap(
    command: FileAgentToolGapCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    rows = conn.execute(
        """INSERT INTO agent_tool_gaps (
               reporter_agent_ref,
               reporter_run_id,
               reporter_delegation_id,
               missing_capability,
               attempted_task,
               admitted_tools,
               blocked_action,
               suggested_tool_contract,
               evidence,
               severity,
               status
           )
           VALUES ($1, $2, $3::uuid, $4, $5, $6::jsonb, $7, $8::jsonb, $9::jsonb, $10, 'open')
           RETURNING gap_id, created_at""",
        command.reporter_agent_ref,
        command.reporter_run_id,
        command.reporter_delegation_id,
        command.missing_capability,
        command.attempted_task,
        json.dumps(command.admitted_tools),
        command.blocked_action,
        json.dumps(command.suggested_tool_contract)
        if command.suggested_tool_contract is not None
        else None,
        json.dumps(command.evidence),
        command.severity,
    )
    if not rows:
        return {
            "ok": False,
            "operation": "agent_tool_gap.file",
            "error_code": "agent_tool_gap.insert_returned_no_row",
        }
    gap_id = str(rows[0]["gap_id"])
    if command.reporter_delegation_id:
        conn.execute(
            "UPDATE agent_delegations SET gap_count = gap_count + 1 WHERE delegation_id = $1::uuid",
            command.reporter_delegation_id,
        )

    try:
        from runtime.system_events import emit_system_event

        emit_system_event(
            conn,
            event_type="agent.tool_gap.filed",
            source_id=gap_id,
            source_type="agent_tool_gap",
            payload={
                "gap_id": gap_id,
                "reporter_agent_ref": command.reporter_agent_ref,
                "missing_capability": command.missing_capability,
                "severity": command.severity,
                "reporter_delegation_id": command.reporter_delegation_id,
            },
        )
    except Exception:
        pass

    return {
        "ok": True,
        "operation": "agent_tool_gap.file",
        "gap_id": gap_id,
        "reporter_agent_ref": command.reporter_agent_ref,
        "missing_capability": command.missing_capability,
        "severity": command.severity,
    }


__all__ = [
    "FileAgentToolGapCommand",
    "RegisterAgentPrincipalCommand",
    "RequestAgentWakeCommand",
    "UpdateAgentPrincipalStatusCommand",
    "handle_file_agent_tool_gap",
    "handle_register_agent_principal",
    "handle_request_agent_wake",
    "handle_update_agent_principal_status",
]
