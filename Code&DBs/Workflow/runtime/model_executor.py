"""Operating Model Runtime — makes canvas cards executable via the event bus.

Cards are nodes. Edges define execution order. The dispatch worker polls for
ready cards and executes them through the existing agent CLI infrastructure.

Three entry points:
  start_model_run()   — OperatingModel → workflow_run + run_nodes + run_edges
  execute_card()      — picks up a ready run_node, dispatches based on card kind
  release_downstream() — after a card completes, releases downstream cards
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid
from datetime import datetime, timezone
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

# Edge kinds that require upstream success to fire
SUCCESS_EDGES = {
    'proceeds_to', 'mission_to_decision', 'decision_to_action',
    'action_to_state', 'authority_gate',
}
# Edge kinds that fire on upstream failure
FAILURE_EDGES = {'alternate_route', 'recovers_via'}
# Edge kinds that fire on any terminal state
ANY_TERMINAL_EDGES = {'escalates_to', 'state_informs'}


def _uid(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# start_model_run: OperatingModel → workflow_run + run_nodes + run_edges
# ---------------------------------------------------------------------------

def start_model_run(conn, model: dict) -> dict:
    """Create a workflow_run with run_nodes for each card and run_edges for each edge."""
    model_id = model.get('id', uuid.uuid4().hex[:12])
    model_name = model.get('name', 'Unnamed model')
    cards = model.get('cards', [])
    edges = model.get('edges', [])

    run_id = f"model_{uuid.uuid4().hex[:12]}"
    workflow_id = f"model:{model_id}"
    request_id = _uid('req')
    now = datetime.now(timezone.utc)

    # --- Authority chain (reuses dispatch pattern) ---
    def_id = f"model_def:{hashlib.sha256(workflow_id.encode()).hexdigest()[:10]}:v1"
    adm_id = f"model_adm:{run_id}"
    ctx_id = f"ctx_{uuid.uuid4().hex[:8]}"

    # 1. workflow_definitions
    conn.execute(
        """INSERT INTO workflow_definitions (
            workflow_definition_id, workflow_id, schema_version,
            definition_version, definition_hash, status,
            request_envelope, normalized_definition, created_at
        ) VALUES ($1, $2, 1, 1, $3, 'active', $4::jsonb, $5::jsonb, $6)
        ON CONFLICT (workflow_definition_id) DO NOTHING""",
        def_id, workflow_id, uuid.uuid4().hex[:16],
        json.dumps({"type": "model_run", "model_name": model_name}),
        json.dumps({"model": True}),
        now,
    )

    # 2. workflow_definition_nodes (one per card)
    for i, card in enumerate(cards):
        wdn_id = f"{def_id}:node:{card['id']}"
        conn.execute(
            """INSERT INTO workflow_definition_nodes (
                workflow_definition_node_id, workflow_definition_id,
                node_id, node_type, schema_version, adapter_type,
                display_name, inputs, expected_outputs, success_condition,
                failure_behavior, authority_requirements, execution_boundary,
                position_index
            ) VALUES ($1, $2, $3, $4, 1, $5, $6, $7::jsonb, $8::jsonb,
                      $9::jsonb, $10::jsonb, $11::jsonb, $12::jsonb, $13)
            ON CONFLICT (workflow_definition_node_id) DO NOTHING""",
            wdn_id, def_id,
            card['id'], f"card_{card['kind']}", 'card_executor',
            card.get('task', card.get('goal', card.get('name', card['id']))),
            json.dumps(card),  # full card as input spec
            json.dumps({}),
            json.dumps({}),
            json.dumps({}),
            json.dumps({'authority': card.get('authority', 'autonomous')}),
            json.dumps({}),
            i,
        )

    # 3. workflow_definition_edges
    for i, edge in enumerate(edges):
        wde_id = f"{def_id}:edge:{edge['id']}"
        conn.execute(
            """INSERT INTO workflow_definition_edges (
                workflow_definition_edge_id, workflow_definition_id,
                edge_id, edge_type, schema_version, from_node_id, to_node_id,
                release_condition, payload_mapping, position_index
            ) VALUES ($1, $2, $3, $4, 1, $5, $6, '{}'::jsonb, '{}'::jsonb, $7)
            ON CONFLICT (workflow_definition_edge_id) DO NOTHING""",
            wde_id, def_id,
            edge['id'], edge.get('kind', 'proceeds_to'),
            edge['from'], edge['to'], i,
        )

    # 4. admission_decisions
    conn.execute(
        """INSERT INTO admission_decisions (
            admission_decision_id, workflow_id, request_id,
            decision, reason_code, decided_at, decided_by,
            policy_snapshot_ref, validation_result_ref, authority_context_ref
        ) VALUES ($1, $2, $3, 'admit', 'model_auto', $4, 'model_executor',
                  'model:auto', 'model:auto', 'model:auto')
        ON CONFLICT (admission_decision_id) DO NOTHING""",
        adm_id, workflow_id, request_id, now,
    )

    # 5. workflow_runs
    conn.execute(
        """INSERT INTO workflow_runs (
            run_id, workflow_id, request_id, request_digest,
            authority_context_digest, workflow_definition_id,
            admitted_definition_hash, run_idempotency_key,
            schema_version, request_envelope, context_bundle_id,
            admission_decision_id, current_state, requested_at, admitted_at, started_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, $9::jsonb, $10, $11, 'running', $12, $12, $12)
        ON CONFLICT (run_id) DO NOTHING""",
        run_id, workflow_id, request_id,
        uuid.uuid4().hex[:16], uuid.uuid4().hex[:16],
        def_id, uuid.uuid4().hex[:16],
        f"model:{model_id}:{run_id}",
        json.dumps({
            "type": "model_run", "model_id": model_id,
            "model_name": model_name, "total_cards": len(cards),
        }),
        ctx_id, adm_id, now,
    )

    # 6. Build inbound edge map to determine root cards
    inbound: dict[str, list[str]] = {card['id']: [] for card in cards}
    for edge in edges:
        to_id = edge['to']
        if to_id in inbound:
            inbound[to_id].append(edge['from'])

    root_cards = [cid for cid, deps in inbound.items() if len(deps) == 0]

    # 7. run_nodes (one per card)
    for card in cards:
        is_root = card['id'] in root_cards
        wdn_id = f"{def_id}:node:{card['id']}"
        rn_id = _uid('rn')
        conn.execute(
            """INSERT INTO run_nodes (
                run_node_id, run_id, workflow_definition_node_id,
                node_id, node_type, attempt_number, current_state,
                adapter_type, context_bundle_id,
                input_payload, output_payload
            ) VALUES ($1, $2, $3, $4, $5, 1, $6, 'card_executor', $7,
                      $8::jsonb, '{}'::jsonb)
            ON CONFLICT (run_id, node_id, attempt_number) DO NOTHING""",
            rn_id, run_id, wdn_id,
            card['id'], f"card_{card['kind']}", 'ready' if is_root else 'waiting',
            ctx_id, json.dumps(card),
        )

    # 8. run_edges
    for edge in edges:
        re_id = _uid('re')
        wde_id = f"{def_id}:edge:{edge['id']}"

        # Look up run_node_ids for upstream/downstream
        upstream_rn = conn.execute(
            "SELECT run_node_id FROM run_nodes WHERE run_id=$1 AND node_id=$2",
            run_id, edge['from'],
        )
        downstream_rn = conn.execute(
            "SELECT run_node_id FROM run_nodes WHERE run_id=$1 AND node_id=$2",
            run_id, edge['to'],
        )

        conn.execute(
            """INSERT INTO run_edges (
                run_edge_id, run_id, workflow_definition_edge_id,
                edge_id, from_node_id, to_node_id, edge_type,
                release_state, payload_mapping_resolved,
                upstream_run_node_id, downstream_run_node_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', '{}'::jsonb, $8, $9)
            ON CONFLICT (run_id, edge_id) DO NOTHING""",
            re_id, run_id, wde_id,
            edge['id'], edge['from'], edge['to'], edge.get('kind', 'proceeds_to'),
            upstream_rn[0]['run_node_id'] if upstream_rn else None,
            downstream_rn[0]['run_node_id'] if downstream_rn else None,
        )

    logger.info("Model run started: %s (%d cards, %d edges, %d roots)",
                run_id, len(cards), len(edges), len(root_cards))

    return {
        "run_id": run_id,
        "workflow_id": workflow_id,
        "total_cards": len(cards),
        "ready_cards": root_cards,
    }


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

    # App cards → Haiku API (instant, parallel, no CLI overhead)
    # Agent cards → direct unified execution primitives
    if executor_kind == 'app':
        prompt = _build_mcp_tool_prompt(card, upstream_outputs)
        try:
            import time as _time
            start = _time.monotonic()
            from runtime.task_assembler import TaskAssembler
            raw = TaskAssembler._call_haiku(prompt)
            duration = round(_time.monotonic() - start, 2)
            if not raw:
                raise RuntimeError("Haiku API returned empty")
            return {
                "status": "succeeded",
                "outputs": {
                    "stdout": raw[:4000],
                    "executed_by": executor.get('name', 'haiku'),
                    "duration_seconds": duration,
                },
            }
        except Exception as exc:
            logger.error("Haiku card execution failed: %s", exc)
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
    for edge in outbound:
        edge_type = edge['edge_type']
        should_fire = False

        if edge_type in SUCCESS_EDGES and completed_state == 'succeeded':
            should_fire = True
        elif edge_type in FAILURE_EDGES and completed_state == 'failed':
            should_fire = True
        elif edge_type in ANY_TERMINAL_EDGES and completed_state in ('succeeded', 'failed'):
            should_fire = True

        if should_fire:
            # Mark edge as released
            conn.execute(
                """UPDATE run_edges SET release_state='released', released_at=NOW()
                   WHERE run_edge_id=$1""",
                edge['run_edge_id'],
            )

            # Check if ALL inbound edges to the downstream card are released
            to_node = edge['to_node_id']
            unreleased = conn.execute(
                """SELECT count(*) as cnt FROM run_edges
                   WHERE run_id=$1 AND to_node_id=$2 AND release_state='pending'""",
                run_id, to_node,
            )

            if unreleased and unreleased[0]['cnt'] == 0:
                # All deps satisfied — release the downstream card
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


def _latest_awaiting_human_run_node_id(conn, run_id: str, card_id: str) -> str:
    run_node_id = conn.fetchval(
        """SELECT run_node_id
           FROM run_nodes
           WHERE run_id=$1 AND node_id=$2 AND current_state='awaiting_human'
           ORDER BY attempt_number DESC
           LIMIT 1""",
        run_id,
        card_id,
    )
    if not run_node_id:
        raise RuntimeError(
            f"No awaiting_human run_node found for run_id={run_id!r} card_id={card_id!r}"
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
    run_node_id = _latest_awaiting_human_run_node_id(conn, run_id, card_id)
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
        state_repository.mark_terminal_state(
            run_node_id=run_node_id,
            state="succeeded",
            output_payload=outputs,
            receipt_id=receipt_id,
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
        state_repository.mark_terminal_state(
            run_node_id=run_node_id,
            state="failed",
            output_payload=outputs,
            failure_code="human_rejected",
            receipt_id=receipt_id,
        )
        released = release_downstream(conn, run_id, card_id)
        _finalize_run_if_terminal(conn, run_id)
        return {"status": "rejected", "released_cards": released}


def get_run_status(conn, run_id: str) -> dict:
    """Get full status of a model run with per-card states."""
    run = conn.execute(
        "SELECT run_id, workflow_id, current_state FROM workflow_runs WHERE run_id=$1",
        run_id,
    )
    if not run:
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

    return {
        "run_id": run_id,
        "status": run[0]['current_state'],
        "cards": cards,
    }
