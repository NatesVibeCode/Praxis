import type { BuildEdge, BuildNode, BuildPayload } from './types';

type DefinitionGate = {
  type: string;
  label?: string;
  required_approvers?: number;
  verify_command?: string;
  condition?: Record<string, unknown>;
  max_attempts?: number;
  fallback_job?: string;
};

const TRIGGER_MANUAL_ROUTE = 'trigger';
const TRIGGER_SCHEDULE_ROUTE = 'trigger/schedule';
const TRIGGER_WEBHOOK_ROUTE = 'trigger/webhook';
const WEBHOOK_TRIGGER_EVENT = 'db.webhook_events.insert';

function isTriggerRoute(route?: string): boolean {
  return route === TRIGGER_MANUAL_ROUTE || route === TRIGGER_SCHEDULE_ROUTE || route === TRIGGER_WEBHOOK_ROUTE;
}

function stringList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value
    .map(item => typeof item === 'string' ? item.trim() : '')
    .filter((item): item is string => item.length > 0);
}

function triggerIntentFromNode(
  node: BuildNode,
  index: number,
): Record<string, unknown> {
  const route = node.route || TRIGGER_MANUAL_ROUTE;
  const filter = node.trigger?.filter && typeof node.trigger.filter === 'object' && !Array.isArray(node.trigger.filter)
    ? { ...node.trigger.filter }
    : {};
  const sourceRef = typeof node.trigger?.source_ref === 'string' ? node.trigger.source_ref.trim() : '';
  const cronExpression = typeof node.trigger?.cron_expression === 'string' ? node.trigger.cron_expression.trim() : '';
  const base: Record<string, unknown> = {
    id: `trigger-${String(index + 1).padStart(3, '0')}`,
    title: node.title || `Trigger ${index + 1}`,
    summary: node.summary || node.title || route,
    source_node_id: node.node_id,
    source_block_ids: node.source_block_ids || [],
    reference_slugs: [],
    filter,
  };
  if (route === TRIGGER_SCHEDULE_ROUTE) {
    return { ...base, event_type: 'schedule', cron_expression: cronExpression || '@daily' };
  }
  if (route === TRIGGER_WEBHOOK_ROUTE) {
    return {
      ...base,
      event_type: (typeof node.trigger?.event_type === 'string' && node.trigger.event_type.trim()) || WEBHOOK_TRIGGER_EVENT,
      ...(sourceRef ? { source_ref: sourceRef } : {}),
    };
  }
  return {
    ...base,
    event_type: 'manual',
    ...(sourceRef ? { source_ref: sourceRef } : {}),
  };
}

function buildDefinitionGate(edge: BuildEdge): DefinitionGate | null {
  const gate = edge.gate;
  if (!gate?.family) return null;

  let definitionGate: DefinitionGate | null = null;
  switch (gate.family) {
    case 'approval':
      definitionGate = { type: 'approval', required_approvers: 1 };
      break;
    case 'human_review':
      definitionGate = { type: 'human_review' };
      break;
    case 'validation':
      definitionGate = { type: 'validation', verify_command: gate.config?.verify_command };
      break;
    case 'conditional':
      definitionGate = {
        type: 'conditional',
        condition: gate.config?.condition && typeof gate.config.condition === 'object' && !Array.isArray(gate.config.condition)
          ? { ...gate.config.condition as Record<string, unknown> }
          : undefined,
      };
      break;
    case 'retry':
      definitionGate = { type: 'retry', max_attempts: gate.config?.max_attempts || 3 };
      break;
    case 'after_failure':
      definitionGate = { type: 'on_failure', fallback_job: gate.config?.fallback };
      break;
    default:
      return null;
  }

  const label = edge.gateLabel || gate.label;
  if (label) definitionGate.label = label;
  return definitionGate;
}

export function buildGraphToDefinition(buildGraph: BuildPayload['build_graph']): Record<string, unknown> {
  const nodes = buildGraph?.nodes || [];
  const edges = buildGraph?.edges || [];
  const routeByNode = Object.fromEntries(nodes.map(node => [node.node_id, node.route || '']));
  const incoming: Record<string, string[]> = {};
  const gatesByTarget: Record<string, DefinitionGate[]> = {};
  const edge_gates: Array<Record<string, unknown>> = [];
  for (const e of edges) {
    if (e.kind === 'authority_gate') continue;
    if (isTriggerRoute(routeByNode[e.from_node_id]) || isTriggerRoute(routeByNode[e.to_node_id])) continue;
    if (e.to_node_id && e.from_node_id) {
      incoming[e.to_node_id] = incoming[e.to_node_id] || [];
      incoming[e.to_node_id].push(e.from_node_id);
    }
    const gate = buildDefinitionGate(e);
    if (gate && e.to_node_id) {
      gatesByTarget[e.to_node_id] = gatesByTarget[e.to_node_id] || [];
      gatesByTarget[e.to_node_id].push(gate);
      edge_gates.push({
        edge_id: e.edge_id,
        from_node_id: e.from_node_id,
        to_node_id: e.to_node_id,
        family: e.gate?.family,
        label: gate.label || e.gate?.label || '',
        branch_reason: e.branch_reason || undefined,
        ...(e.gate?.config && typeof e.gate.config === 'object' && !Array.isArray(e.gate.config)
          ? { config: { ...e.gate.config } }
          : {}),
        gate,
      });
    }
  }
  const triggerNodes = nodes.filter(n => (!n.kind || n.kind === 'step') && isTriggerRoute(n.route));
  const stepNodes = nodes.filter(n => (!n.kind || n.kind === 'step') && !isTriggerRoute(n.route));
  const trigger_intent = triggerNodes.map((node, index) => triggerIntentFromNode(node, index));
  const draft_flow = stepNodes.map((n, i) => {
    const gates = gatesByTarget[n.node_id] || [];
    return {
      id: n.node_id,
      order: i,
      title: n.title || `Step ${i + 1}`,
      summary: n.summary || n.title || '',
      depends_on: incoming[n.node_id] || [],
      source_block_ids: n.source_block_ids || [],
      ...(gates.length > 0 ? { gates } : {}),
    };
  });
  const phases = stepNodes
    .filter(n => n.route)
    .map(n => ({
      step_id: n.node_id,
      agent_route: n.route!,
      system_prompt: (n.prompt || '').trim(),
      required_inputs: stringList(n.required_inputs),
      outputs: stringList(n.outputs),
      persistence_targets: stringList(n.persistence_targets),
      handoff_target: typeof n.handoff_target === 'string' && n.handoff_target.trim()
        ? n.handoff_target.trim()
        : null,
    }));
  return {
    trigger_intent,
    draft_flow,
    execution_setup: {
      phases,
      ...(edge_gates.length > 0 ? { edge_gates } : {}),
    },
  };
}
