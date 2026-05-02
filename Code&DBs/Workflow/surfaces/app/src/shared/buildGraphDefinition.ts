import type { BuildEdge, BuildNode, BuildPayload } from './types';
import { baseConditionFromRelease, normalizeBuildEdgeRelease } from './edgeRelease';

type DefinitionGate = {
  type: string;
  label?: string;
  required_approvers?: number;
  verify_refs?: string[];
  condition?: Record<string, unknown>;
  max_attempts?: number;
  fallback_job?: string;
};

export interface ReleasePlanSource {
  definition?: Record<string, unknown>;
  buildGraph?: BuildPayload['build_graph'] | null;
  fingerprint: string;
  title: string;
}

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

function plainObject(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return { ...(value as Record<string, unknown>) };
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
  const release = normalizeBuildEdgeRelease(edge);
  if (!release.family || release.family === 'after_success') return null;

  let definitionGate: DefinitionGate | null = null;
  switch (release.family) {
    case 'approval':
      definitionGate = { type: 'approval', required_approvers: 1 };
      break;
    case 'human_review':
      definitionGate = { type: 'human_review' };
      break;
    case 'validation':
      definitionGate = { type: 'validation', verify_refs: stringList(release.config?.verify_refs) };
      break;
    case 'conditional':
      definitionGate = {
        type: 'conditional',
        condition: baseConditionFromRelease(release),
      };
      break;
    case 'retry':
      definitionGate = { type: 'retry', max_attempts: release.config?.max_attempts || 3 };
      break;
    case 'after_failure':
      definitionGate = { type: 'on_failure', fallback_job: release.config?.fallback };
      break;
    default:
      return null;
  }

  const label = edge.gateLabel || release.label;
  if (label) definitionGate.label = label;
  return definitionGate;
}

function stableSerialize(value: unknown): string {
  if (value === null || value === undefined) return 'null';
  if (typeof value === 'number' || typeof value === 'boolean') return JSON.stringify(value);
  if (typeof value === 'string') return JSON.stringify(value);
  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableSerialize(entry)).join(',')}]`;
  }
  if (typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, entry]) => entry !== undefined)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, entry]) => `${JSON.stringify(key)}:${stableSerialize(entry)}`);
    return `{${entries.join(',')}}`;
  }
  return JSON.stringify(String(value));
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
      const release = normalizeBuildEdgeRelease(e);
      gatesByTarget[e.to_node_id] = gatesByTarget[e.to_node_id] || [];
      gatesByTarget[e.to_node_id].push(gate);
      edge_gates.push({
        edge_id: e.edge_id,
        from_node_id: e.from_node_id,
        to_node_id: e.to_node_id,
        release: {
          family: release.family,
          edge_type: release.edge_type,
          release_condition: { ...release.release_condition },
          ...(release.label ? { label: release.label } : {}),
          ...(release.branch_reason ? { branch_reason: release.branch_reason } : {}),
          ...(release.state ? { state: release.state } : {}),
          ...(release.config ? { config: { ...release.config } } : {}),
        },
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
      task_type: typeof n.task_type === 'string' && n.task_type.trim() ? n.task_type.trim() : null,
      agent: typeof n.agent === 'string' && n.agent.trim() ? n.agent.trim() : null,
      required_inputs: stringList(n.required_inputs),
      outputs: stringList(n.outputs),
      persistence_targets: stringList(n.persistence_targets),
      capabilities: stringList(n.capabilities),
      write_scope: stringList(n.write_scope),
      handoff_target: typeof n.handoff_target === 'string' && n.handoff_target.trim()
        ? n.handoff_target.trim()
        : null,
      integration_args: plainObject(n.integration_args),
      agent_tool_plan: plainObject(n.agent_tool_plan),
      completion_contract: plainObject(n.completion_contract),
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

export function resolveReleasePlanSource(payload: BuildPayload | null): ReleasePlanSource | null {
  if (!payload) return null;
  const hasDefinition = payload.definition && Object.keys(payload.definition).length > 0;
  const definition = hasDefinition ? payload.definition : undefined;
  const buildGraph = payload.build_graph ?? null;
  const compiledSpecProjection = payload.materialized_spec_projection?.materialized_spec ?? null;
  if (!definition && !buildGraph) return null;
  const title = String(payload.workflow?.name || (definition as { title?: unknown })?.title || 'canvas-workflow');
  return {
    ...(definition ? { definition } : {}),
    ...(buildGraph ? { buildGraph } : {}),
    fingerprint: stableSerialize({ title, definition: definition || null, buildGraph, compiledSpecProjection }),
    title,
  };
}
