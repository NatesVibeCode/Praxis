"""Trigger evaluator.

Processes system_events, matches against workflow_triggers, and fires workflows.
Notification wakes the consumer; checkpoints make the replay deterministic.
Pure Postgres — no LLM, no HTTP.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from storage.postgres import PostgresSubscriptionRepository

logger = logging.getLogger(__name__)

MAX_EVENTS_PER_CYCLE = 50
MAX_TRIGGER_DEPTH = 3
_TRIGGER_EVALUATOR_SUBSCRIPTION_ID = "trigger_evaluator"
_TRIGGER_EVALUATOR_CHECKPOINT_RUN_ID = "trigger_evaluator"
_TRIGGER_EVALUATOR_SUBSCRIPTION_NAME = "Workflow Trigger Evaluator"
_TRIGGER_EVALUATOR_CONSUMER_KIND = "system"
_TRIGGER_EVALUATOR_ENVELOPE_KIND = "system_event"
_TRIGGER_EVALUATOR_CURSOR_SCOPE = "global"


def _subscription_writes(conn: Any) -> PostgresSubscriptionRepository:
    return PostgresSubscriptionRepository(conn)


def evaluate_triggers(conn: Any) -> int:
    """Run workflow triggers and durable event subscriptions from checkpoints."""
    fired = _evaluate_workflow_triggers(conn)
    fired += evaluate_event_subscriptions(conn)
    return fired


def _evaluate_workflow_triggers(conn: Any) -> int:
    """Process workflow_triggers from durable subscription checkpoints."""
    return _evaluate_workflow_triggers_with_checkpoints(conn)


def _evaluate_workflow_triggers_with_checkpoints(conn: Any) -> int:
    """Process workflow_triggers from durable subscription checkpoints."""
    _ensure_trigger_evaluator_subscription(conn)
    checkpoint_rows = conn.execute(
        """SELECT last_evidence_seq
           FROM public.subscription_checkpoints
           WHERE subscription_id = $1
             AND run_id = $2
           ORDER BY checkpointed_at DESC
           LIMIT 1""",
        _TRIGGER_EVALUATOR_SUBSCRIPTION_ID,
        _TRIGGER_EVALUATOR_CHECKPOINT_RUN_ID,
    )
    last_checkpoint = checkpoint_rows[0]["last_evidence_seq"] if checkpoint_rows else 0
    if last_checkpoint is None:
        last_checkpoint = 0

    events = _load_workflow_events_from_checkpoint(conn, last_checkpoint)
    if not events:
        return 0

    fired = 0

    for event in events:
        fired += _evaluate_workflow_triggers_for_event(conn, event=event)

    _upsert_workflow_trigger_checkpoint(
        conn,
        last_checkpoint_event_id=events[-1]["id"],
    )

    return fired


def _load_workflow_events_from_checkpoint(conn: Any, last_checkpoint: int) -> list[dict[str, Any]]:
    args: list[Any] = [last_checkpoint, MAX_EVENTS_PER_CYCLE]
    return conn.execute(
        """SELECT id, event_type, source_id, source_type, payload, created_at
           FROM public.system_events
           WHERE id > $1
           ORDER BY id ASC
           LIMIT $2""",
        *args,
    )


def _ensure_trigger_evaluator_subscription(conn: Any) -> None:
    """Persist the trigger evaluator as a first-class durable subscriber."""
    _subscription_writes(conn).upsert_event_subscription(
        subscription_id=_TRIGGER_EVALUATOR_SUBSCRIPTION_ID,
        subscription_name=_TRIGGER_EVALUATOR_SUBSCRIPTION_NAME,
        consumer_kind=_TRIGGER_EVALUATOR_CONSUMER_KIND,
        envelope_kind=_TRIGGER_EVALUATOR_ENVELOPE_KIND,
        workflow_id=None,
        run_id=None,
        cursor_scope=_TRIGGER_EVALUATOR_CURSOR_SCOPE,
        status="active",
        delivery_policy={},
        filter_policy={},
    )


def _upsert_workflow_trigger_checkpoint(
    conn: Any,
    *,
    last_checkpoint_event_id: int,
) -> None:
    _upsert_subscription_checkpoint(
        conn,
        subscription_id=_TRIGGER_EVALUATOR_SUBSCRIPTION_ID,
        checkpoint_run_id=_TRIGGER_EVALUATOR_CHECKPOINT_RUN_ID,
        last_processed_id=last_checkpoint_event_id,
        metadata={
            "last_event_id": last_checkpoint_event_id,
            "processor": "runtime.triggers._evaluate_workflow_triggers",
        },
    )


def _evaluate_workflow_triggers_for_event(
    conn: Any,
    *,
    event: dict[str, Any],
) -> int:
    event_type = event["event_type"]
    payload = event.get("payload") or {}

    # Parse payload if string
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            payload = {}

    # 2. Find matching enabled triggers
    trigger_event_types = _workflow_trigger_event_types(event_type)
    if len(trigger_event_types) == 1:
        triggers = conn.execute(
            """SELECT t.id, t.workflow_id, t.filter,
                      t.trigger_type, t.integration_id, t.integration_action, t.integration_args,
                      t.target_kind, t.target_ref, t.target_args,
                      w.definition, w.materialized_spec, w.name as workflow_name
               FROM public.workflow_triggers t
               LEFT JOIN public.workflows w ON w.id = t.workflow_id
               WHERE t.event_type = $1 AND t.enabled = TRUE""",
            trigger_event_types[0],
        )
    else:
        triggers = conn.execute(
            """SELECT t.id, t.workflow_id, t.filter,
                      t.trigger_type, t.integration_id, t.integration_action, t.integration_args,
                      t.target_kind, t.target_ref, t.target_args,
                      w.definition, w.materialized_spec, w.name as workflow_name
               FROM public.workflow_triggers t
               LEFT JOIN public.workflows w ON w.id = t.workflow_id
               WHERE t.event_type = ANY($1::text[]) AND t.enabled = TRUE""",
            list(trigger_event_types),
        )

    fired = 0
    for trigger in (triggers or []):
        trigger_id = trigger["id"]
        trigger_filter = trigger.get("filter") or {}
        # Generic target_kind takes precedence over legacy trigger_type.
        # 'agent_wake' is the new third option alongside the legacy
        # 'workflow' and 'integration' trigger_type values. Closes
        # BUG-F28F5090 — agent_wake target was schema-only without
        # this dispatch branch.
        target_kind = str(trigger.get("target_kind") or "").strip().lower()
        trigger_type = str(trigger.get("trigger_type") or "workflow").strip().lower()
        if target_kind == "agent_wake":
            trigger_type = "agent_wake"

        # Parse filter if string
        if isinstance(trigger_filter, str):
            try:
                trigger_filter = json.loads(trigger_filter)
            except (json.JSONDecodeError, TypeError):
                trigger_filter = {}

        # Check filter match (flat key-value equality)
        if not _filter_matches(trigger_filter, payload):
            continue

        # Check trigger depth
        try:
            source_depth = int(payload.get("trigger_depth", 0))
        except (ValueError, TypeError):
            source_depth = 0
        if source_depth >= MAX_TRIGGER_DEPTH:
            logger.warning(
                "trigger.depth_exceeded: trigger_id=%s depth=%d max=%d -- suppressing execution",
                trigger_id,
                source_depth,
                MAX_TRIGGER_DEPTH,
            )
            _emit_depth_exceeded_event(
                conn,
                workflow_id=trigger.get("workflow_id") or "",
                trigger_id=trigger_id,
                event={
                    "id": event.get("id"),
                    "payload": {
                        "trigger_id": trigger_id,
                        "trigger_depth": source_depth,
                    },
                },
            )
            continue

        try:
            if trigger_type == "integration":
                did_fire = _fire_integration_trigger(conn, trigger=trigger, event=event, payload=payload)
            elif trigger_type == "agent_wake":
                did_fire = _fire_agent_wake_trigger(
                    conn,
                    trigger=trigger,
                    event=event,
                    event_type=event_type,
                    payload=payload,
                )
            else:
                did_fire = _fire_workflow_trigger(
                    conn,
                    trigger=trigger,
                    event=event,
                    event_type=event_type,
                    payload=payload,
                )
        except Exception:
            logger.exception("trigger.fire_failed: trigger_id=%s type=%s", trigger_id, trigger_type)
            continue

        if did_fire:
            _subscription_writes(conn).increment_workflow_trigger_fire_count(
                trigger_id=trigger_id,
            )
            fired += 1

    return fired


def _fire_workflow_trigger(
    conn: Any,
    *,
    trigger: dict[str, Any],
    event: dict[str, Any],
    event_type: str,
    payload: dict[str, Any],
) -> bool:
    """Fire a workflow from a matched trigger."""
    trigger_id = trigger["id"]
    workflow_id = trigger["workflow_id"]
    workflow_name = trigger.get("workflow_name", workflow_id)

    from runtime.operating_model_planner import current_compiled_spec

    materialized_spec = current_compiled_spec(trigger.get("definition"), trigger.get("materialized_spec"))
    if not materialized_spec:
        logger.warning(
            "Trigger %s matched but workflow %s has no current plan authority; skipping",
            trigger_id,
            workflow_id,
        )
        return False

    source_depth = payload.get("trigger_depth", 0)
    parent_run_id = payload.get("run_id") or event.get("source_id")

    from runtime.control_commands import submit_workflow_command

    spec_copy = json.loads(json.dumps(materialized_spec))
    if spec_copy.get("jobs"):
        event_context = (
            f"\n\n## Trigger Context\n"
            f"Event: {event_type}\n"
            f"Source: {event.get('source_type', 'unknown')}/{event.get('source_id', 'unknown')}\n"
            f"Payload: {json.dumps(payload, default=str)[:500]}"
        )
        spec_copy["jobs"][0]["prompt"] = spec_copy["jobs"][0].get("prompt", "") + event_context

    result = submit_workflow_command(
        conn,
        requested_by_kind="runtime",
        requested_by_ref=f"workflow_trigger.{trigger_id}",
        inline_spec=spec_copy,
        parent_run_id=parent_run_id,
        dispatch_reason=f"workflow_trigger.{event_type}",
        trigger_depth=source_depth + 1,
        spec_name=str(spec_copy.get("name") or workflow_name),
        total_jobs=len(spec_copy.get("jobs") or []),
    )
    if result.get("error") or not result.get("run_id"):
        raise RuntimeError(str(result.get("error") or result))

    logger.info(
        "Trigger %s fired workflow '%s' → run %s (depth=%d)",
        trigger_id,
        workflow_name,
        result["run_id"],
        source_depth + 1,
    )
    return True


def _fire_integration_trigger(
    conn: Any,
    *,
    trigger: dict[str, Any],
    event: dict[str, Any],
    payload: dict[str, Any],
) -> bool:
    """Fire an integration action from a matched trigger."""
    trigger_id = trigger["id"]
    integration_id = str(trigger.get("integration_id") or "").strip()
    integration_action = str(trigger.get("integration_action") or "").strip()
    integration_args = trigger.get("integration_args") or {}

    if not integration_id or not integration_action:
        logger.warning(
            "trigger.integration_missing_target: trigger_id=%s",
            trigger_id,
        )
        return False

    if isinstance(integration_args, str):
        try:
            integration_args = json.loads(integration_args)
        except (json.JSONDecodeError, TypeError):
            integration_args = {}

    merged_args = {**integration_args, "_trigger_event": payload}

    from runtime.integrations import (
        execute_integration,
        integration_result_error_code,
        integration_result_succeeded,
        integration_result_status,
    )

    result = execute_integration(integration_id, integration_action, merged_args, conn)
    result_status = integration_result_status(result)

    logger.info(
        "Trigger %s fired integration %s/%s → %s",
        trigger_id,
        integration_id,
        integration_action,
        result_status,
    )
    if integration_result_succeeded(result):
        return True
    logger.warning(
        "trigger.integration_not_fired: trigger_id=%s integration=%s/%s status=%s error=%s",
        trigger_id,
        integration_id,
        integration_action,
        result_status,
        integration_result_error_code(result),
    )
    return False


def _fire_agent_wake_trigger(
    conn: Any,
    *,
    trigger: dict[str, Any],
    event: dict[str, Any],
    event_type: str,
    payload: dict[str, Any],
) -> bool:
    """Dispatch an agent_wake from a matched trigger.

    Closes BUG-F28F5090 — the trigger evaluator had no dispatch branch for
    target_kind='agent_wake' rows; schedule.fired and webhook.received events
    fell through to the legacy workflow path and never produced
    agent_wakes rows.

    Reads target_ref as the agent_principal_ref. Reads target_args
    {trigger_kind, payload, ...} for the wake shape. Compiles the trust
    envelope (runtime.agent_context.compile_agent_context), refuses if
    the agent isn't active or the in-flight cap is hit, deduplicates by
    (principal, trigger_kind, payload_hash, trigger_event_id), inserts
    the agent_wakes row, and launches a workflow run with
    requested_by_kind='agent', requested_by_ref=<agent_principal_ref> so
    every receipt attributes back to the principal.
    """
    from runtime.agent_context import compile_agent_context, in_flight_wake_count
    from runtime.control_commands import submit_workflow_command

    trigger_id = trigger["id"]
    agent_ref = str(trigger.get("target_ref") or "").strip()
    if not agent_ref:
        logger.warning(
            "trigger.agent_wake_missing_target_ref: trigger_id=%s",
            trigger_id,
        )
        return False

    target_args = trigger.get("target_args") or {}
    if isinstance(target_args, str):
        try:
            target_args = json.loads(target_args)
        except (json.JSONDecodeError, TypeError):
            target_args = {}

    trigger_kind = str(target_args.get("trigger_kind") or "schedule").strip().lower()
    merged_payload = {
        **(target_args.get("payload") or {}),
        "_event": {
            "event_type": event_type,
            "event_id": event.get("id"),
            "source_id": event.get("source_id"),
            "source_type": event.get("source_type"),
        },
        "_trigger_payload": payload,
    }

    envelope = compile_agent_context(
        conn,
        agent_principal_ref=agent_ref,
        trigger_kind=trigger_kind,
        trigger_source_ref=str(trigger_id),
        payload=merged_payload,
    )
    if envelope is None:
        logger.warning(
            "trigger.agent_wake_unknown_principal: trigger_id=%s agent_ref=%s",
            trigger_id,
            agent_ref,
        )
        _record_skipped_wake(
            conn,
            agent_ref=agent_ref,
            trigger_kind=trigger_kind,
            trigger_source_ref=str(trigger_id),
            trigger_event_id=event.get("id"),
            payload=merged_payload,
            payload_hash=None,
            skip_reason="unknown_principal",
        )
        return False

    if envelope.agent_status != "active":
        _record_skipped_wake(
            conn,
            agent_ref=agent_ref,
            trigger_kind=trigger_kind,
            trigger_source_ref=str(trigger_id),
            trigger_event_id=event.get("id"),
            payload=merged_payload,
            payload_hash=envelope.payload_hash,
            skip_reason=f"status_{envelope.agent_status}",
        )
        logger.info(
            "trigger.agent_wake_skipped: trigger_id=%s agent_ref=%s status=%s",
            trigger_id,
            agent_ref,
            envelope.agent_status,
        )
        return False

    rows = conn.execute(
        "SELECT max_in_flight_wakes FROM agent_registry WHERE agent_principal_ref = $1",
        agent_ref,
    )
    cap = int(rows[0]["max_in_flight_wakes"]) if rows else 1
    if in_flight_wake_count(conn, agent_ref) >= cap:
        _record_skipped_wake(
            conn,
            agent_ref=agent_ref,
            trigger_kind=trigger_kind,
            trigger_source_ref=str(trigger_id),
            trigger_event_id=event.get("id"),
            payload=merged_payload,
            payload_hash=envelope.payload_hash,
            skip_reason="in_flight_cap",
        )
        logger.info(
            "trigger.agent_wake_in_flight_cap: trigger_id=%s agent_ref=%s cap=%d",
            trigger_id,
            agent_ref,
            cap,
        )
        return False

    # Dedup hardening: include trigger_event_id in the unique key. Two
    # distinct events (different event_id) with identical payloads must
    # produce two separate wake rows. The unique index in the schema
    # carries trigger_event_id as part of the key after migration.
    wake_rows = conn.execute(
        """INSERT INTO agent_wakes (
               agent_principal_ref, trigger_kind, trigger_source_ref,
               trigger_event_id, payload, payload_hash, status
           )
           VALUES ($1, $2, $3, $4, $5::jsonb, $6, 'pending')
           ON CONFLICT (agent_principal_ref, trigger_kind, payload_hash, (COALESCE(trigger_event_id, 0)))
               WHERE payload_hash IS NOT NULL
               DO NOTHING
           RETURNING wake_id""",
        agent_ref,
        trigger_kind,
        str(trigger_id),
        event.get("id"),
        json.dumps(merged_payload, default=str),
        envelope.payload_hash,
    )
    if not wake_rows:
        logger.info(
            "trigger.agent_wake_duplicate: trigger_id=%s agent_ref=%s payload_hash=%s",
            trigger_id,
            agent_ref,
            envelope.payload_hash,
        )
        return False
    wake_id = str(wake_rows[0]["wake_id"])

    source_depth = payload.get("trigger_depth", 0)
    if isinstance(source_depth, str):
        try:
            source_depth = int(source_depth)
        except ValueError:
            source_depth = 0

    result = submit_workflow_command(
        conn,
        requested_by_kind="agent",
        requested_by_ref=agent_ref,
        inline_spec=dict(envelope.inline_spec),
        dispatch_reason=f"agent_wake.{trigger_kind}",
        trigger_depth=int(source_depth) + 1,
        spec_name=str(envelope.inline_spec.get("name") or f"agent_wake::{agent_ref}"),
        total_jobs=len(envelope.inline_spec.get("jobs") or []),
    )
    if result.get("error") or not result.get("run_id"):
        conn.execute(
            "UPDATE agent_wakes SET status='failed', skip_reason=$1 WHERE wake_id=$2",
            f"submit_failed: {result.get('error') or 'no run_id'}",
            wake_id,
        )
        raise RuntimeError(str(result.get("error") or result))

    run_id = str(result["run_id"])
    conn.execute(
        """UPDATE agent_wakes
              SET status='dispatched', dispatched_at=now(), run_id=$1
            WHERE wake_id=$2""",
        run_id,
        wake_id,
    )
    try:
        from runtime.system_events import emit_system_event

        emit_system_event(
            conn,
            event_type="agent.wake.dispatched",
            source_id=str(wake_id),
            source_type="agent_wake",
            payload={
                "wake_id": wake_id,
                "agent_principal_ref": agent_ref,
                "run_id": run_id,
                "trigger_kind": trigger_kind,
            },
        )
    except Exception:
        logger.debug("agent.wake.dispatched emission unavailable", exc_info=True)

    logger.info(
        "Trigger %s fired agent_wake for %s → wake=%s run=%s",
        trigger_id,
        agent_ref,
        wake_id,
        run_id,
    )
    return True


def _record_skipped_wake(
    conn: Any,
    *,
    agent_ref: str,
    trigger_kind: str,
    trigger_source_ref: str,
    trigger_event_id: Any,
    payload: dict[str, Any],
    payload_hash: str | None,
    skip_reason: str,
) -> None:
    """Insert a skipped wake row so the audit trail captures suppressed triggers."""
    try:
        conn.execute(
            """INSERT INTO agent_wakes (
                   agent_principal_ref, trigger_kind, trigger_source_ref,
                   trigger_event_id, payload, payload_hash, status, skip_reason
               )
               VALUES ($1, $2, $3, $4, $5::jsonb, $6, 'skipped', $7)""",
            agent_ref,
            trigger_kind,
            trigger_source_ref,
            trigger_event_id,
            json.dumps(payload, default=str),
            payload_hash,
            skip_reason,
        )
    except Exception:
        logger.debug("trigger.skipped_wake_record_failed", exc_info=True)


def _workflow_trigger_event_types(event_type: str) -> tuple[str, ...]:
    normalized = (event_type or "").strip()
    if normalized == "schedule.fired":
        # `schedule` remains the compiler-facing alias; `schedule.fired` is the
        # runtime source. Treat them as one trigger surface so both paths work.
        return ("schedule.fired", "schedule")
    if normalized == "schedule":
        return ("schedule", "schedule.fired")
    return (normalized,)


def evaluate_event_subscriptions(conn: Any) -> int:
    """Process workflow-firing event subscriptions against system_events."""
    subscriptions = conn.execute(
        """SELECT s.subscription_id,
                  s.subscription_name,
                  s.workflow_id,
                  s.run_id,
                  s.consumer_kind,
                  s.envelope_kind,
                  s.cursor_scope,
                  s.filter_policy,
                  w.definition,
                  w.materialized_spec,
                  w.name AS workflow_name
           FROM public.event_subscriptions s
           LEFT JOIN public.workflows w ON w.id = s.workflow_id
           WHERE s.status = 'active'
             AND s.workflow_id IS NOT NULL
             AND COALESCE(s.envelope_kind, '') = 'system_event'
             AND NOT (
                 COALESCE(s.consumer_kind, '') = 'worker'
                 AND COALESCE(s.cursor_scope, '') = 'run'
             )
           ORDER BY s.created_at ASC
           LIMIT $1""",
        MAX_EVENTS_PER_CYCLE,
    )

    fired = 0

    for subscription in subscriptions or []:
        subscription_id = subscription["subscription_id"]
        checkpoint_run_id = _subscription_checkpoint_run_id(subscription)
        fired += _process_event_subscription(
            conn,
            subscription=subscription,
            checkpoint_run_id=checkpoint_run_id,
        )

    return fired


def _process_event_subscription(
    conn: Any,
    *,
    subscription: dict[str, Any],
    checkpoint_run_id: str,
) -> int:
    subscription_id = subscription["subscription_id"]
    workflow_id = subscription.get("workflow_id")
    workflow_name = subscription.get("workflow_name") or workflow_id or subscription_id
    envelope_kind = str(subscription.get("envelope_kind") or "system_event").strip()
    from runtime.operating_model_planner import current_compiled_spec

    materialized_spec = current_compiled_spec(subscription.get("definition"), subscription.get("materialized_spec"))
    filter_policy = _json_mapping(subscription.get("filter_policy"))

    if envelope_kind != "system_event":
        logger.warning(
            "Event subscription %s has unsupported envelope_kind=%s for trigger evaluator; skipping",
            subscription_id,
            envelope_kind,
        )
        return 0
    if not workflow_id:
        logger.warning(
            "Event subscription %s has no workflow_id; skipping",
            subscription_id,
        )
        return 0
    if not materialized_spec:
        has_authority_snapshot = bool(subscription.get("definition")) or bool(subscription.get("materialized_spec"))
        logger.warning(
            "Event subscription %s %s for workflow %s; skipping",
            subscription_id,
            "has stale plan authority"
            if has_authority_snapshot
            else "has no stored plan authority",
            workflow_id,
        )
        return 0

    checkpoint_rows = conn.execute(
        """SELECT checkpoint_id,
                  last_evidence_seq
           FROM public.subscription_checkpoints
           WHERE subscription_id = $1
             AND run_id = $2
           ORDER BY checkpointed_at DESC
           LIMIT 1""",
        subscription_id,
        checkpoint_run_id,
    )
    checkpoint = checkpoint_rows[0] if checkpoint_rows else None
    last_processed_id = checkpoint.get("last_evidence_seq") if checkpoint else None

    events = _load_subscription_events(
        conn,
        filter_policy=filter_policy,
        last_processed_id=last_processed_id,
    )
    if not events:
        return 0

    fired = 0
    max_processed_id = last_processed_id

    for event in events:
        try:
            _submit_workflow_for_event(
                conn,
                workflow_id=workflow_id,
                workflow_name=workflow_name,
                materialized_spec=materialized_spec,
                event=event,
                source_depth=_event_payload(event).get("trigger_depth", 0),
                context_title="Subscription Context",
                context_lines=[
                    f"Subscription: {subscription_id}",
                    f"Checkpoint Run: {checkpoint_run_id}",
                    f"Filter Policy: {json.dumps(filter_policy, default=str)[:500]}",
                ],
            )
        except RuntimeError as exc:
            logger.warning("Event subscription %s rejected event %s: %s", subscription_id, event["id"], exc)
            break
        except Exception as exc:
            logger.error("Event subscription %s failed on event %s: %s", subscription_id, event["id"], exc)
            break

        max_processed_id = event["id"]
        fired += 1

    if max_processed_id is not None and max_processed_id != last_processed_id:
        _upsert_subscription_checkpoint(
            conn,
            subscription_id=subscription_id,
            checkpoint_run_id=checkpoint_run_id,
            last_processed_id=max_processed_id,
            metadata={
                "last_event_id": max_processed_id,
                "processor": "runtime.triggers.evaluate_event_subscriptions",
            },
        )

    return fired


def _load_subscription_events(
    conn: Any,
    *,
    filter_policy: dict[str, Any],
    last_processed_id: int | None,
) -> list[dict[str, Any]]:
    match_mode, match_value = _event_type_match(filter_policy)
    source_id = filter_policy.get("source_id")
    source_type = filter_policy.get("source_type")

    clauses: list[str] = []
    args: list[Any] = []

    if last_processed_id is not None:
        args.append(last_processed_id)
        clauses.append(f"id > ${len(args)}")
    else:
        clauses.append("created_at >= now() - interval '24 hours'")

    if match_mode == "exact":
        args.append(match_value)
        clauses.append(f"event_type = ${len(args)}")
    elif match_mode == "like":
        args.append(match_value)
        clauses.append(f"event_type LIKE ${len(args)}")
    elif match_mode == "in":
        args.append(match_value)
        clauses.append(f"event_type = ANY(${len(args)})")

    if source_id:
        args.append(source_id)
        clauses.append(f"source_id = ${len(args)}")
    if source_type:
        args.append(source_type)
        clauses.append(f"source_type = ${len(args)}")

    args.append(MAX_EVENTS_PER_CYCLE)
    where_clause = " AND ".join(clauses) if clauses else "TRUE"
    query = (
        "SELECT id, event_type, source_id, source_type, payload, created_at "
        "FROM public.system_events "
        f"WHERE {where_clause} "
        "ORDER BY id ASC "
        f"LIMIT ${len(args)}"
    )

    rows = conn.execute(query, *args)
    payload_filter = _subscription_payload_filter(filter_policy)
    if not payload_filter:
        return list(rows or [])
    return [
        event
        for event in (rows or [])
        if _filter_matches(payload_filter, _event_payload(event))
    ]


def _event_type_match(filter_policy: dict[str, Any]) -> tuple[str | None, Any]:
    event_types = filter_policy.get("event_types")
    if isinstance(event_types, list):
        normalized = [str(event_type) for event_type in event_types if str(event_type).strip()]
        if normalized:
            return "in", normalized

    raw = (
        filter_policy.get("event_type_like")
        or filter_policy.get("event_type_pattern")
        or filter_policy.get("event_type")
    )
    if isinstance(raw, list):
        normalized = [str(event_type) for event_type in raw if str(event_type).strip()]
        if normalized:
            return "in", normalized
    if not isinstance(raw, str) or not raw.strip():
        return None, None
    value = raw.strip()
    if (
        "event_type_like" in filter_policy
        or "event_type_pattern" in filter_policy
        or "%" in value
        or "_" in value
    ):
        return "like", value
    return "exact", value


def _subscription_payload_filter(filter_policy: dict[str, Any]) -> dict[str, Any]:
    payload_filter = filter_policy.get("payload")
    if isinstance(payload_filter, dict):
        return payload_filter
    payload_filter = filter_policy.get("payload_filter")
    if isinstance(payload_filter, dict):
        return payload_filter
    return {}


def _subscription_checkpoint_run_id(subscription: dict[str, Any]) -> str:
    run_id = subscription.get("run_id")
    if isinstance(run_id, str) and run_id.strip():
        return run_id.strip()
    return f"subscription:{subscription['subscription_id']}"


def _upsert_subscription_checkpoint(
    conn: Any,
    *,
    subscription_id: str,
    checkpoint_run_id: str,
    last_processed_id: int,
    metadata: dict[str, Any],
) -> None:
    _subscription_writes(conn).upsert_subscription_checkpoint(
        subscription_id=subscription_id,
        run_id=checkpoint_run_id,
        last_evidence_seq=last_processed_id,
        last_authority_id=f"system_event:{last_processed_id}",
        checkpoint_status="committed",
        metadata=metadata,
    )


def _submit_workflow_for_event(
    conn: Any,
    *,
    workflow_id: str,
    workflow_name: str,
    materialized_spec: Any,
    event: dict[str, Any],
    source_depth: int,
    context_title: str,
    context_lines: list[str] | None = None,
) -> dict[str, Any]:
    if source_depth >= MAX_TRIGGER_DEPTH:
        logger.warning(
            "trigger.depth_exceeded: workflow_id=%s depth=%d max=%d -- suppressing execution",
            workflow_id,
            source_depth,
            MAX_TRIGGER_DEPTH,
        )
        _emit_depth_exceeded_event(conn, workflow_id=workflow_id, trigger_id=workflow_id, event=event)
        raise RuntimeError(
            f"Trigger depth {source_depth} exceeds maximum ({MAX_TRIGGER_DEPTH})"
        )

    spec_copy = _compiled_spec_dict(materialized_spec, workflow_id=workflow_id)
    payload = _event_payload(event)
    event_type = event["event_type"]
    parent_run_id = payload.get("run_id") or event.get("source_id")

    from runtime.control_commands import submit_workflow_command

    if spec_copy.get("jobs"):
        extra_lines = [
            f"Event: {event_type}",
            f"Source: {event.get('source_type', 'unknown')}/{event.get('source_id', 'unknown')}",
            f"Payload: {json.dumps(payload, default=str)[:500]}",
        ]
        extra_lines.extend(context_lines or [])
        event_context = f"\n\n## {context_title}\n" + "\n".join(extra_lines)
        spec_copy["jobs"][0]["prompt"] = spec_copy["jobs"][0].get("prompt", "") + event_context

    result = submit_workflow_command(
        conn,
        requested_by_kind="runtime",
        requested_by_ref=f"event_subscription.{workflow_id}",
        inline_spec=spec_copy,
        parent_run_id=parent_run_id,
        dispatch_reason=f"event_subscription.{event_type}",
        trigger_depth=source_depth + 1,
        spec_name=str(spec_copy.get("name") or workflow_name),
        total_jobs=len(spec_copy.get("jobs") or []),
    )
    if result.get("error") or not result.get("run_id"):
        raise RuntimeError(str(result.get("error") or result))
    logger.info(
        "Workflow '%s' triggered by %s → run %s (depth=%d)",
        workflow_name,
        event_type,
        result["run_id"],
        source_depth + 1,
    )
    return result


def _compiled_spec_dict(materialized_spec: Any, *, workflow_id: str) -> dict[str, Any]:
    if isinstance(materialized_spec, str):
        try:
            materialized_spec = json.loads(materialized_spec)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError(f"invalid materialized_spec for workflow {workflow_id}") from exc
    if not isinstance(materialized_spec, dict):
        raise ValueError(f"materialized_spec for workflow {workflow_id} must be a mapping")
    return json.loads(json.dumps(materialized_spec))


def _event_payload(event: dict[str, Any]) -> dict[str, Any]:
    return _json_mapping(event.get("payload"))


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return {}
    if isinstance(value, dict):
        return value
    return {}


def _emit_depth_exceeded_event(
    conn: Any,
    *,
    workflow_id: str,
    trigger_id: str,
    event: dict[str, Any],
) -> None:
    payload = _event_payload(event)
    _subscription_writes(conn).insert_system_event(
        event_type="trigger.depth_exceeded",
        source_id=str(trigger_id),
        source_type="workflow_trigger",
        payload={
            "trigger_id": trigger_id,
            "workflow_id": workflow_id,
            "depth": payload.get("trigger_depth", 0),
            "max_depth": MAX_TRIGGER_DEPTH,
        },
    )


def _filter_matches(filter_dict: dict, payload: dict) -> bool:
    """Match filter against payload. Supports flat key-value and condition trees."""
    if not filter_dict:
        return True
    if "op" in filter_dict and ("conditions" in filter_dict or "field" in filter_dict):
        from runtime.condition_evaluator import evaluate_condition_tree
        return evaluate_condition_tree(payload, filter_dict)
    for key, expected in filter_dict.items():
        actual = payload.get(key)
        if str(actual) != str(expected):
            return False
    return True
