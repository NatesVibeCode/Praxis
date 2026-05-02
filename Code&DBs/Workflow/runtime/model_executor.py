"""Operating Model Runtime — makes canvas cards executable via the event bus.

Cards are nodes. Edges define execution order. The dispatch worker polls for
ready cards and executes them through the existing agent CLI infrastructure.

Compatibility entry points:
  start_model_run()   — fail-closed guard for the retired model-card creation lane
  execute_card()      — picks up an existing ready run_node, dispatches based on card kind
  release_downstream() — releases downstream cards for existing compatibility runs
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any

from runtime.execution.records import (
    NODE_EXECUTION_RECEIPT_TYPE,
)
from runtime.run_node_receipts import write_run_node_receipt
from runtime.native_authority import default_native_runtime_profile_ref_required
from runtime.execution_transport import resolve_execution_transport
from storage.postgres.workflow_orchestration_repository import (
    PostgresRunNodeStateRepository,
)

logger = logging.getLogger(__name__)


class LegacyModelRuntimeRetiredError(RuntimeError):
    """Raised when a caller tries to create new runs through the retired model-card lane."""

# Edge kinds that require upstream success to fire
SUCCESS_EDGES = {
    'proceeds_to', 'mission_to_decision', 'decision_to_action',
    'action_to_state', 'authority_gate',
}
# Edge kinds that fire on upstream failure
FAILURE_EDGES = {'alternate_route', 'recovers_via'}
# Edge kinds that fire on any terminal state
ANY_TERMINAL_EDGES = {'escalates_to', 'state_informs'}


def start_model_run(conn, model: dict) -> dict:
    """Block new model-card runs from the retired side path."""
    del conn, model
    raise LegacyModelRuntimeRetiredError(
        "model-card run creation is retired; submit through the unified workflow front door"
    )


# ---------------------------------------------------------------------------
# execute_card: dispatch based on card kind
# ---------------------------------------------------------------------------

def execute_card(conn, run_node_row: dict, repo_root: str) -> dict:
    """Execute a single card. Returns {status, outputs, failure_code?}."""
    node_id = run_node_row['node_id']
    node_type = run_node_row['node_type']
    card = run_node_row['input_payload']
    if isinstance(card, str):
        card = json.loads(card)

    run_id = run_node_row['run_id']
    kind = card.get('kind', node_type.replace('card_', ''))

    logger.info("Executing card %s (kind=%s) in run %s", node_id, kind, run_id)

    if kind == 'mission':
        return _execute_mission(conn, run_id, card)
    elif kind == 'decision':
        return _execute_decision(conn, run_id, run_node_row, card)
    elif kind == 'action':
        return _execute_action(conn, run_id, card, repo_root)
    elif kind == 'state_knowledge':
        return _execute_state(conn, run_id, card)
    elif kind == 'step':
        return _execute_action(conn, run_id, card, repo_root)
    elif kind == 'subflow':
        return _execute_action(conn, run_id, card, repo_root)
    else:
        return {"status": "succeeded", "outputs": {}}


def _execute_mission(conn, run_id: str, card: dict) -> dict:
    """Mission cards succeed immediately — they define the goal, not an action."""
    return {
        "status": "succeeded",
        "outputs": {
            "goal": card.get('goal', ''),
            "criteria": card.get('successCriteria', []),
        },
    }


def _execute_decision(conn, run_id: str, run_node_row: dict, card: dict) -> dict:
    """Decision cards with autonomous authority auto-select recommended option.
    Human authority cards pause for approval."""
    authority = card.get('authority', 'autonomous')

    if authority in ('human_review', 'human_approve', 'human_execute'):
        return {
            "status": "awaiting_human",
            "outputs": {
                "reason": "Human approval required",
                "authority": authority,
            },
        }

    # Autonomous: pick recommended option or first
    options = card.get('options', [])
    chosen = next((o for o in options if o.get('recommended')), options[0] if options else None)
    return {
        "status": "succeeded",
        "outputs": {
            "decision": chosen.get('label', 'auto') if chosen else 'auto',
            "rationale": chosen.get('rationale', '') if chosen else '',
        },
    }


def _execute_action(conn, run_id: str, card: dict, repo_root: str) -> dict:
    """Execute an action card by dispatching to an agent CLI."""
    executor = card.get('executor', {})
    executor_kind = executor.get('kind', 'system')
    task = card.get('task', card.get('objective', ''))
    tools = [
        str(tool).strip()
        for tool in (card.get('toolPermissions', []) or [])
        if str(tool).strip()
    ]

    # Collect upstream outputs for context
    upstream_outputs = _collect_upstream_outputs(conn, run_id, card.get('id', ''))

    if executor_kind == 'human':
        # Human actions pause
        return {"status": "awaiting_human", "outputs": {"reason": "Manual action required"}}

    # App cards → routed prompt execution through task-route authority
    # Agent cards → direct unified execution primitives
    if executor_kind == 'app':
        prompt = _build_mcp_tool_prompt(card, upstream_outputs)
        try:
            import time as _time
            start = _time.monotonic()
            from runtime.task_assembler import TaskAssembler
            routed = TaskAssembler.call_routed_prompt(
                conn,
                prompt,
                task_type=str(card.get("task_type") or "chat"),
                purpose=f"app_card:{card.get('id', '') or executor.get('name', 'app')}",
            )
            duration = round(_time.monotonic() - start, 2)
            return {
                "status": "succeeded",
                "outputs": {
                    "stdout": routed.text[:4000],
                    "executed_by": executor.get('name', 'routed_app_prompt'),
                    "resolved_agent": f"{routed.provider_slug}/{routed.model_slug}",
                    "execution_transport": routed.transport_type.lower(),
                    "runtime_profile_ref": routed.runtime_profile_ref,
                    "candidate_ref": routed.candidate_ref,
                    "duration_seconds": duration,
                },
            }
        except Exception as exc:
            logger.error("Routed app card execution failed: %s", exc)
            raise

    # Agent cards → direct provider execution via unified runtime helpers
    prompt = _build_card_prompt(card, upstream_outputs)
    try:
        from registry.agent_config import AgentRegistry
        from runtime.workflow.unified import _build_platform_context, _execute_api, _execute_cli

        agent_slug = executor.get('detail')
        if not isinstance(agent_slug, str) or not agent_slug.strip():
            raise RuntimeError("Card executor.detail is required for agent cards")
        agent_slug = agent_slug.strip()
        resolved_agent_slug = agent_slug
        registry = AgentRegistry.load_from_postgres(conn)
        if isinstance(agent_slug, str) and agent_slug.startswith('auto/'):
            from runtime.task_type_router import TaskTypeRouter

            decision = TaskTypeRouter(conn).resolve(
                agent_slug,
                runtime_profile_ref=default_native_runtime_profile_ref_required(),
            )
            resolved_agent_slug = f"{decision.provider_slug}/{decision.model_slug}"

        agent_config = registry.get(resolved_agent_slug)

        if agent_config is None:
            raise RuntimeError(
                f"No agent available for task={task!r} requested_agent={agent_slug!r} resolved_agent={resolved_agent_slug!r}"
            )

        full_prompt = f"{prompt}\n\n{_build_platform_context(repo_root)}"
        started = time.monotonic()
        transport = resolve_execution_transport(agent_config)
        transport_kind = transport.transport_kind
        execution_bundle = {
            "run_id": run_id,
            "job_label": str(card.get("id") or task or "model_card"),
            "mcp_tool_names": tools,
        }

        if transport_kind in {"cli", "mcp"}:
            execution = _execute_cli(
                agent_config,
                full_prompt,
                repo_root,
                execution_bundle=execution_bundle,
            )
        elif transport_kind == "api":
            execution = _execute_api(agent_config, full_prompt, repo_root)
        else:
            raise NotImplementedError(f"Unsupported execution transport: {transport_kind}")
        duration_seconds = round(time.monotonic() - started, 2)
        status = execution.get("status")
        if not isinstance(status, str) or not status:
            raise RuntimeError("Execution transport did not return a status")

        return {
            "status": status,
            "outputs": {
                "stdout": execution.get("stdout", "")[:4000],
                "executed_by": executor.get('name', executor_kind),
                "resolved_agent": resolved_agent_slug,
                "execution_transport": transport_kind,
                "duration_seconds": duration_seconds,
            },
            "failure_code": execution.get("error_code", ""),
        }
    except Exception as exc:
        logger.error("Card execution failed: %s", exc, exc_info=True)
        raise


def _execute_state(conn, run_id: str, card: dict) -> dict:
    """State cards capture upstream outputs into variables."""
    upstream_outputs = _collect_upstream_outputs(conn, run_id, card.get('id', ''))

    variables = dict(card.get('variables', {}))
    # Merge upstream outputs into variables
    for key, value in upstream_outputs.items():
        if isinstance(value, str):
            variables[key] = value

    return {
        "status": "succeeded",
        "outputs": {"variables": variables, "upstream": upstream_outputs},
    }


def _collect_upstream_outputs(conn, run_id: str, card_id: str) -> dict:
    """Collect outputs from all completed upstream cards."""
    rows = conn.execute(
        """SELECT rn.node_id, rn.output_payload
           FROM run_nodes rn
           JOIN run_edges re ON re.run_id = rn.run_id AND re.from_node_id = rn.node_id
           WHERE re.run_id = $1 AND re.to_node_id = $2
             AND rn.current_state = 'succeeded'""",
        run_id, card_id,
    )
    outputs: dict[str, Any] = {}
    for row in rows:
        payload = row['output_payload']
        if isinstance(payload, str):
            payload = json.loads(payload)
        outputs[row['node_id']] = payload
    return outputs


def _build_mcp_tool_prompt(card: dict, upstream_outputs: dict) -> str:
    """Build a focused prompt for MCP tool execution.

    App executor cards need a specific, actionable prompt that tells the agent
    exactly which tool to call and what parameters to use.
    """
    executor = card.get('executor', {})
    tool_name = executor.get('name', '')
    task = card.get('task', card.get('objective', ''))
    tools = card.get('toolPermissions', [])

    parts = [
        f"Execute this task using the {tool_name} tool: {task}",
        "",
        "INSTRUCTIONS:",
        f"- Use the MCP tool '{tool_name}' to accomplish this task",
        "- Return the actual data/results, not a description of what you would do",
        "- Format the output as structured data (JSON if possible)",
    ]

    if tools:
        parts.append(f"- Allowed tool actions: {', '.join(tools)}")

    if upstream_outputs:
        parts.append("")
        parts.append("CONTEXT FROM PREVIOUS STEPS:")
        for node_id, outputs in upstream_outputs.items():
            parts.append(f"  {node_id}: {json.dumps(outputs, default=str)[:800]}")

    parts.append("")
    parts.append("Return the results directly. No explanation needed.")

    return "\n".join(parts)


def _build_card_prompt(card: dict, upstream_outputs: dict) -> str:
    """Build a prompt for agent execution from card + upstream context."""
    task = card.get('task', card.get('objective', ''))
    tools = card.get('toolPermissions', [])

    parts = [f"Task: {task}"]

    if tools:
        parts.append(f"Available tools: {', '.join(tools)}")

    if upstream_outputs:
        parts.append("Context from upstream cards:")
        for node_id, outputs in upstream_outputs.items():
            parts.append(f"  {node_id}: {json.dumps(outputs, default=str)[:500]}")

    parts.append("Return your result as a concise summary. Include any data retrieved.")

    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# release_downstream: edge dependency resolution
# ---------------------------------------------------------------------------

def release_downstream(conn, run_id: str, completed_card_id: str) -> list[str]:
    """After a card completes, check and release downstream cards.
    Returns list of newly released card IDs."""
    # Get the completed card's terminal state
    completed = conn.execute(
        "SELECT current_state FROM run_nodes WHERE run_id=$1 AND node_id=$2",
        run_id, completed_card_id,
    )
    if not completed:
        return []
    completed_state = completed[0]['current_state']

    # Find outbound edges
    outbound = conn.execute(
        """SELECT run_edge_id, edge_id, to_node_id, edge_type
           FROM run_edges WHERE run_id=$1 AND from_node_id=$2""",
        run_id, completed_card_id,
    )

    released: list[str] = []
    touched_downstream_nodes: set[str] = set()
    for edge in outbound:
        edge_type = edge['edge_type']
        should_fire = False

        if edge_type in SUCCESS_EDGES and completed_state == 'succeeded':
            should_fire = True
        elif edge_type in FAILURE_EDGES and completed_state == 'failed':
            should_fire = True
        elif edge_type in ANY_TERMINAL_EDGES and completed_state in ('succeeded', 'failed'):
            should_fire = True

        to_node = edge['to_node_id']
        touched_downstream_nodes.add(to_node)
        if should_fire:
            # Mark edge as released
            conn.execute(
                """UPDATE run_edges SET release_state='released', released_at=NOW()
                   WHERE run_edge_id=$1""",
                edge['run_edge_id'],
            )
        else:
            conn.execute(
                """UPDATE run_edges SET release_state='skipped', released_at=NOW()
                   WHERE run_edge_id=$1
                     AND release_state='pending'""",
                edge['run_edge_id'],
            )

    for to_node in sorted(touched_downstream_nodes):
        unreleased = conn.execute(
            """SELECT count(*) as cnt FROM run_edges
               WHERE run_id=$1 AND to_node_id=$2 AND release_state='pending'""",
            run_id, to_node,
        )

        if unreleased and unreleased[0]['cnt'] == 0:
            updated = conn.execute(
                """UPDATE run_nodes SET current_state='ready'
                   WHERE run_id=$1 AND node_id=$2 AND current_state='waiting'
                   RETURNING node_id""",
                run_id, to_node,
            )
            if updated:
                released.append(to_node)
                logger.info("Released card %s in run %s", to_node, run_id)

    # Finalize the run once every card has reached a terminal state.
    _finalize_run_if_terminal(conn, run_id)

    return released


def _finalize_run_if_terminal(conn, run_id: str) -> None:
    """If all cards are in terminal state, mark the run as complete."""
    non_terminal = conn.execute(
        """SELECT count(*) as cnt FROM run_nodes
           WHERE run_id=$1 AND current_state NOT IN ('succeeded', 'failed')""",
        run_id,
    )
    if non_terminal and non_terminal[0]['cnt'] == 0:
        # All done — check if any failed
        failed = conn.execute(
            """SELECT count(*) as cnt FROM run_nodes
               WHERE run_id=$1 AND current_state='failed'""",
            run_id,
        )
        terminal_state = 'failed' if (failed and failed[0]['cnt'] > 0) else 'succeeded'
        conn.execute(
            """
            UPDATE workflow_runs
            SET current_state=$2,
                finished_at=NOW(),
                terminal_reason_code = COALESCE(terminal_reason_code, 'model_executor.completed'),
                started_at = COALESCE(started_at, admitted_at, requested_at, NOW())
            WHERE run_id=$1
              AND current_state NOT IN ('succeeded', 'failed', 'dead_letter', 'cancelled')
            """,
            run_id, terminal_state,
        )
        logger.info("Model run %s completed: %s", run_id, terminal_state)


def _claim_awaiting_human_run_node_id(conn, run_id: str, card_id: str) -> str:
    run_node_id = conn.fetchval(
        """UPDATE run_nodes
           SET current_state='running',
               started_at = COALESCE(started_at, NOW())
           WHERE run_node_id = (
               SELECT run_node_id
               FROM run_nodes
               WHERE run_id=$1 AND node_id=$2 AND current_state='awaiting_human'
               ORDER BY attempt_number DESC
               LIMIT 1
           )
           RETURNING run_node_id""",
        run_id,
        card_id,
    )
    if not run_node_id:
        raise RuntimeError(
            f"No claimable awaiting_human run_node found for run_id={run_id!r} card_id={card_id!r}"
        )
    return str(run_node_id)


# ---------------------------------------------------------------------------
# approve_card: human approval via UI
# ---------------------------------------------------------------------------

def approve_card(conn, run_id: str, card_id: str, decision: str, notes: str = '') -> dict:
    """Handle human approval/rejection for a decision card."""
    outputs = {
        "decision": "approved" if decision == "approved" else "rejected",
        "notes": notes,
        "decided_by": "human",
    }
    state_repository = PostgresRunNodeStateRepository(conn)
    run_node_id = _claim_awaiting_human_run_node_id(conn, run_id, card_id)
    if decision == 'approved':
        receipt_id = write_run_node_receipt(
            conn,
            run_id=run_id,
            node_id=card_id,
            phase="terminal",
            receipt_type=NODE_EXECUTION_RECEIPT_TYPE,
            status="succeeded",
            outputs=outputs,
            agent_slug="human",
            executor_type="runtime.model_executor.approve_card",
        )
        updated = state_repository.mark_terminal_state(
            run_node_id=run_node_id,
            state="succeeded",
            output_payload=outputs,
            receipt_id=receipt_id,
            expected_current_state="running",
        )
        if not updated:
            raise RuntimeError(
                f"Approval conflict for run_id={run_id!r} card_id={card_id!r}"
            )
        released = release_downstream(conn, run_id, card_id)
        return {"status": "approved", "released_cards": released}
    else:
        receipt_id = write_run_node_receipt(
            conn,
            run_id=run_id,
            node_id=card_id,
            phase="terminal",
            receipt_type=NODE_EXECUTION_RECEIPT_TYPE,
            status="failed",
            outputs=outputs,
            failure_code="human_rejected",
            agent_slug="human",
            executor_type="runtime.model_executor.approve_card",
        )
        updated = state_repository.mark_terminal_state(
            run_node_id=run_node_id,
            state="failed",
            output_payload=outputs,
            failure_code="human_rejected",
            receipt_id=receipt_id,
            expected_current_state="running",
        )
        if not updated:
            raise RuntimeError(
                f"Approval conflict for run_id={run_id!r} card_id={card_id!r}"
            )
        released = release_downstream(conn, run_id, card_id)
        _finalize_run_if_terminal(conn, run_id)
        return {"status": "rejected", "released_cards": released}


def get_run_status(conn, run_id: str) -> dict:
    """Get canonical run status with legacy card state attached when present."""
    from runtime.workflow._status import get_run_status as get_canonical_run_status

    status = get_canonical_run_status(conn, run_id)
    if status is None:
        return {"error": "Run not found"}

    nodes = conn.execute(
        """SELECT node_id, node_type, current_state, started_at, finished_at,
                  output_payload, failure_code
           FROM run_nodes WHERE run_id=$1""",
        run_id,
    )

    cards: dict[str, dict] = {}
    for n in nodes:
        output = n['output_payload']
        if isinstance(output, str):
            output = json.loads(output)
        cards[n['node_id']] = {
            "status": n['current_state'],
            "node_type": n['node_type'],
            "started_at": str(n['started_at']) if n['started_at'] else None,
            "finished_at": str(n['finished_at']) if n['finished_at'] else None,
            "outputs": output,
            "failure_code": n['failure_code'],
        }

    result = dict(status)
    result["cards"] = cards
    if cards:
        result["model_runtime_status_projection"] = "run_nodes_compatibility"
    return result
