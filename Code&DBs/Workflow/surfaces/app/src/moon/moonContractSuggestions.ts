import type {
  AuthorityAttachment,
  BindingLedgerEntry,
  BindingTarget,
  BuildIssue,
  BuildNode,
  BuildPayload,
  CompiledSpec,
  ImportSnapshot,
} from '../shared/types';
import type { ObjectType } from '../shared/hooks/useObjectTypes';

export interface ContractFieldSuggestion {
  value: string;
  /** Short provenance, e.g. "Output · Fetch profile" — shown on hover / subtitle */
  detail?: string;
}

/** Dock slice for the selected node (same shape as `DockContent` in the presenter). */
export type DockPrimitiveSlice = {
  contextAttachments?: AuthorityAttachment[];
  connectBindings?: BindingLedgerEntry[];
  imports?: ImportSnapshot[];
};

/** Extra sources mined from the workflow payload (compiled plan, issues, …). */
export type PrimitiveContractExtras = {
  compiledSpec?: CompiledSpec | null;
  buildIssues?: BuildIssue[] | null;
};

function addBindingTargetTokens(
  add: (raw: string, detail?: string) => void,
  target: BindingTarget | null | undefined,
  detail: string,
) {
  if (!target) return;
  if (target.target_ref?.trim()) add(target.target_ref.trim(), detail);
  if (target.label?.trim() && target.label.trim() !== target.target_ref?.trim()) {
    add(target.label.trim(), detail);
  }
}

function importLocatorToken(locator: string): string | null {
  const t = locator.trim();
  if (!t) return null;
  const noQuery = t.split('?')[0];
  const parts = noQuery.split('/').filter(Boolean);
  const last = parts[parts.length - 1] || noQuery;
  const slug = last.replace(/[^a-zA-Z0-9._-]+/g, '_').replace(/^_+|_+$/g, '');
  if (!slug || slug.length > 72) return null;
  return slug;
}

function ingestIntegrationArgsTokens(
  add: (raw: string, detail?: string) => void,
  args: BuildNode['integration_args'] | undefined,
  nodeLabel: string,
) {
  if (!args || typeof args !== 'object' || Array.isArray(args)) return;
  const rec = args as Record<string, unknown>;
  for (const key of Object.keys(rec)) {
    if (key.trim()) add(key.trim(), `Integration · ${nodeLabel}`);
  }
  const headers = rec.headers;
  if (headers && typeof headers === 'object' && !Array.isArray(headers)) {
    for (const hk of Object.keys(headers as Record<string, unknown>)) {
      const k = hk.trim();
      if (k) add(k, `HTTP header · ${nodeLabel}`);
    }
  }
  const meta = rec.metadata;
  if (meta && typeof meta === 'object' && !Array.isArray(meta)) {
    for (const mk of Object.keys(meta as Record<string, unknown>)) {
      const k = mk.trim();
      if (k) add(`metadata.${k}`, `Notification metadata · ${nodeLabel}`);
    }
  }
}

function ingestTriggerFilterPaths(
  add: (raw: string, detail?: string) => void,
  filter: unknown,
  nodeLabel: string,
  prefix = '',
  depth = 0,
) {
  if (depth > 4 || !filter || typeof filter !== 'object' || Array.isArray(filter)) return;
  for (const key of Object.keys(filter as Record<string, unknown>)) {
    const k = key.trim();
    if (!k) continue;
    const path = prefix ? `${prefix}.${k}` : k;
    add(path, `Trigger filter · ${nodeLabel}`);
    const v = (filter as Record<string, unknown>)[key];
    if (v && typeof v === 'object' && !Array.isArray(v)) {
      ingestTriggerFilterPaths(add, v, nodeLabel, path, depth + 1);
    }
  }
}

function ingestGraphNodeDeepTokens(
  add: (raw: string, detail?: string) => void,
  nodes: BuildNode[] | undefined,
) {
  for (const n of nodes || []) {
    const label = (n.title || n.node_id || '').trim() || n.node_id;
    ingestIntegrationArgsTokens(add, n.integration_args, label);
    if (n.trigger?.filter) {
      ingestTriggerFilterPaths(add, n.trigger.filter, label);
    }
    const gr = n.gate_rule;
    if (gr && typeof gr === 'object' && !Array.isArray(gr)) {
      for (const gk of Object.keys(gr as Record<string, unknown>)) {
        const k = gk.trim();
        if (k) add(k, `Gate rule · ${label}`);
      }
    }
  }
}

function ingestCompiledSpecSuggestions(
  add: (raw: string, detail?: string) => void,
  compiled: CompiledSpec | null | undefined,
) {
  if (!compiled) return;
  for (const job of compiled.jobs || []) {
    if (job.label?.trim()) add(job.label.trim(), 'Compiled job');
    if (job.agent?.trim()) add(job.agent.trim(), 'Compiled job · agent');
  }
  for (const t of compiled.triggers || []) {
    if (t.event_type?.trim()) add(t.event_type.trim(), 'Compiled trigger · event');
    if (t.source_ref?.trim()) add(t.source_ref.trim(), 'Compiled trigger · ref');
  }
}

function ingestBuildIssueTokens(
  add: (raw: string, detail?: string) => void,
  issues: BuildIssue[] | null | undefined,
  currentNodeId: string | null | undefined,
) {
  const selfId = (currentNodeId || '').trim();
  if (!selfId) return;
  for (const issue of issues || []) {
    if (issue.node_id && issue.node_id.trim() !== selfId) continue;
    if (issue.label?.trim()) add(issue.label.trim(), 'Build issue');
    if (issue.binding_id?.trim()) add(issue.binding_id.trim(), 'Issue · binding');
  }
}

function ingestDockSuggestions(
  add: (raw: string, detail?: string) => void,
  dock: DockPrimitiveSlice | null | undefined,
) {
  if (!dock) return;

  for (const a of dock.contextAttachments || []) {
    const kind = (a.authority_kind || 'attachment').trim() || 'attachment';
    if (a.authority_ref?.trim()) add(a.authority_ref.trim(), `Authority · ${kind}`);
    if (a.label?.trim() && a.label.trim() !== a.authority_ref?.trim()) {
      add(a.label.trim(), `Attachment label · ${kind}`);
    }
  }

  for (const b of dock.connectBindings || []) {
    if (b.source_label?.trim()) add(b.source_label.trim(), 'Binding · source');
    for (const t of b.candidate_targets || []) {
      addBindingTargetTokens(add, t, 'Binding · candidate');
    }
    addBindingTargetTokens(add, b.accepted_target, 'Binding · accepted');
  }

  for (const imp of dock.imports || []) {
    const shape = imp.requested_shape || {};
    const slabel = typeof shape.label === 'string' ? shape.label.trim() : '';
    const starget = typeof shape.target_ref === 'string' ? shape.target_ref.trim().replace(/^#/, '') : '';
    if (slabel) add(slabel, 'Import · requested shape');
    if (starget) add(starget, 'Import · target ref');
    for (const t of imp.admitted_targets || []) {
      addBindingTargetTokens(add, t, 'Import · admitted');
    }
    const locTok = imp.source_locator ? importLocatorToken(imp.source_locator) : null;
    if (locTok) add(locTok, 'Import · source tail');
  }
}

/**
 * Merge suggestions from peer nodes on the build graph, registered object-type fields,
 * the selected node's dock slice, per-node integration/trigger/gate tokens, optional
 * compiled spec jobs/triggers, and build issues scoped to the current node.
 */
export function buildPrimitiveContractSuggestions(
  buildGraph: BuildPayload['build_graph'] | null | undefined,
  currentNodeId: string | null | undefined,
  objectTypes: ObjectType[],
  dock?: DockPrimitiveSlice | null,
  extras?: PrimitiveContractExtras | null,
): ContractFieldSuggestion[] {
  const seen = new Set<string>();
  const out: ContractFieldSuggestion[] = [];

  const add = (raw: string, detail?: string) => {
    const value = String(raw || '').trim();
    if (!value || seen.has(value)) return;
    seen.add(value);
    out.push(detail ? { value, detail } : { value });
  };

  for (const n of buildGraph?.nodes || []) {
    if (currentNodeId && n.node_id === currentNodeId) continue;
    const label = (n.title || n.node_id || '').trim() || n.node_id;
    for (const o of n.outputs || []) add(String(o), `Output · ${label}`);
    for (const r of n.required_inputs || []) add(String(r), `Input · ${label}`);
    for (const p of n.persistence_targets || []) add(String(p), `Persist · ${label}`);
  }

  for (const ot of objectTypes || []) {
    const otLabel = (ot.name || ot.type_id || '').trim() || ot.type_id;
    for (const pd of ot.fields || []) {
      const name = typeof (pd as { name?: string })?.name === 'string'
        ? (pd as { name: string }).name.trim()
        : '';
      if (name) add(name, `Field · ${otLabel}`);
    }
  }

  if (currentNodeId) {
    const self = (buildGraph?.nodes || []).find((n) => n.node_id === currentNodeId);
    if (self) {
      const label = (self.title || self.node_id || '').trim() || self.node_id;
      for (const o of self.outputs || []) add(String(o), `This step · output · ${label}`);
      for (const r of self.required_inputs || []) add(String(r), `This step · input · ${label}`);
      for (const p of self.persistence_targets || []) add(String(p), `This step · persist · ${label}`);
    }
  }

  ingestGraphNodeDeepTokens(add, buildGraph?.nodes);
  ingestDockSuggestions(add, dock ?? null);
  ingestCompiledSpecSuggestions(add, extras?.compiledSpec ?? null);
  ingestBuildIssueTokens(add, extras?.buildIssues ?? null, currentNodeId);

  return out;
}
