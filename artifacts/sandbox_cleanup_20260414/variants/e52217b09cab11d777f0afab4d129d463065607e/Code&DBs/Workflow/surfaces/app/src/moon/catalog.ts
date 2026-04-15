// Live catalog — fetched from backend registries with static fallbacks.
// Trigger and control families are platform primitives (always present).
// Gather, think, and act families are populated from live registries.

import type { GlyphType } from './moonBuildPresenter';
import type { DragDropKind } from './moonBuildReducer';

export interface CatalogItem {
  id: string;
  label: string;
  icon: GlyphType;
  family: 'trigger' | 'gather' | 'think' | 'act' | 'control';
  status: 'ready' | 'coming_soon';
  dropKind: DragDropKind;
  description?: string;
  /** The route/action value sent to refineDefinition when applied to a node */
  actionValue?: string;
  /** The gate family name sent when applied to an edge */
  gateFamily?: string;
  /** Where this item came from: static, capability, or integration */
  source?: 'static' | 'capability' | 'integration';
  /** For integration items: connection status */
  connectionStatus?: string;
}

// Static items — real engine primitives that exist independent of registries.
// Each item maps to a real engine capability with a real actionValue or gateFamily.
const STATIC_ITEMS: CatalogItem[] = [
  // ── Trigger ──────────────────────────────────────────────────────────
  { id: 'trigger-manual',    label: 'Manual',         icon: 'trigger',  family: 'trigger', status: 'ready', dropKind: 'node', actionValue: 'trigger',          description: 'User-initiated run' },
  { id: 'trigger-webhook',   label: 'Webhook',        icon: 'tool',     family: 'trigger', status: 'ready', dropKind: 'node', actionValue: 'trigger/webhook',  description: 'Inbound webhook with HMAC verification' },
  { id: 'trigger-schedule',  label: 'Schedule',       icon: 'trigger',  family: 'trigger', status: 'ready', dropKind: 'node', actionValue: 'trigger/schedule', description: 'Cron or interval trigger' },

  // ── Gather ───────────────────────────────────────────────────────────
  { id: 'gather-research',   label: 'Web Research',   icon: 'research', family: 'gather',  status: 'ready', dropKind: 'node', actionValue: 'auto/research',    description: 'Search and analyze web sources' },
  { id: 'gather-docs',       label: 'Docs',           icon: 'research', family: 'gather',  status: 'ready', dropKind: 'node', actionValue: 'auto/research',    description: 'Read and extract from documents' },

  // ── Think ────────────────────────────────────────────────────────────
  { id: 'think-classify',    label: 'Classify',       icon: 'classify', family: 'think',   status: 'ready', dropKind: 'node', actionValue: 'auto/classify',    description: 'Score, triage, or categorize' },
  { id: 'think-draft',       label: 'Draft',          icon: 'draft',    family: 'think',   status: 'ready', dropKind: 'node', actionValue: 'auto/draft',       description: 'Generate or compose content' },
  { id: 'think-fan-out',     label: 'Fan Out',        icon: 'classify', family: 'think',   status: 'ready', dropKind: 'node', actionValue: 'auto/fan-out',     description: 'Split into parallel sub-tasks and aggregate' },

  // ── Act ──────────────────────────────────────────────────────────────
  { id: 'act-notify',        label: 'Notify',         icon: 'notify',   family: 'act',     status: 'ready', dropKind: 'node', actionValue: '@notifications/send', description: 'Send notification (Slack, email, etc.)' },
  { id: 'act-webhook-out',   label: 'HTTP Request',   icon: 'tool',     family: 'act',     status: 'ready', dropKind: 'node', actionValue: '@webhook/post',       description: 'Call an external webhook or API' },
  { id: 'act-invoke',        label: 'Run Workflow',   icon: 'tool',     family: 'act',     status: 'ready', dropKind: 'node', actionValue: '@workflow/invoke',    description: 'Invoke another workflow as a sub-workflow' },

  // ── Control (edges) ──────────────────────────────────────────────────
  { id: 'ctrl-approval',     label: 'Approval',       icon: 'gate',     family: 'control', status: 'ready', dropKind: 'edge', gateFamily: 'approval',           description: 'Human approval gate' },
  { id: 'ctrl-review',       label: 'Human Review',   icon: 'review',   family: 'control', status: 'ready', dropKind: 'edge', gateFamily: 'human_review',       description: 'Manual review before proceeding' },
  { id: 'ctrl-validation',   label: 'Validation',     icon: 'gate',     family: 'control', status: 'ready', dropKind: 'edge', gateFamily: 'validation',         description: 'Automated check gate' },
  { id: 'ctrl-branch',       label: 'Branch',         icon: 'gate',     family: 'control', status: 'ready', dropKind: 'edge', gateFamily: 'conditional',        description: 'Conditional path (equals, in, not_equals, not_in)' },
  { id: 'ctrl-retry',        label: 'Retry',          icon: 'gate',     family: 'control', status: 'ready', dropKind: 'edge', gateFamily: 'retry',              description: 'Retry with backoff + provider failover chain' },
  { id: 'ctrl-on-failure',   label: 'On Failure',     icon: 'gate',     family: 'control', status: 'ready', dropKind: 'edge', gateFamily: 'after_failure',      description: 'Run only if upstream step failed' },
];

// Mutable catalog — starts with static items, updated by loadCatalog()
let CATALOG: CatalogItem[] = [...STATIC_ITEMS];
let _loaded = false;

export { CATALOG };

export type CatalogFamily = CatalogItem['family'];

export const FAMILY_LABELS: Record<CatalogFamily, string> = {
  trigger: 'Trigger',
  gather:  'Gather',
  think:   'Think',
  act:     'Act',
  control: 'Control',
};

export function catalogByFamily(family?: CatalogFamily): CatalogItem[] {
  if (!family) return CATALOG;
  return CATALOG.filter(c => c.family === family);
}

/** Fetch live catalog from backend and merge with static primitives. */
export async function loadCatalog(): Promise<CatalogItem[]> {
  if (_loaded) return CATALOG;
  try {
    const resp = await fetch('/api/catalog');
    if (!resp.ok) return CATALOG;
    const data = await resp.json();
    const items: CatalogItem[] = data.items || [];
    // Dedupe by id — backend items override static fallbacks
    const seen = new Set<string>();
    const merged: CatalogItem[] = [];
    for (const item of items) {
      if (!seen.has(item.id)) {
        seen.add(item.id);
        merged.push(item);
      }
    }
    // Add any static items not covered by backend (shouldn't happen, but safe)
    for (const item of STATIC_ITEMS) {
      if (!seen.has(item.id)) {
        seen.add(item.id);
        merged.push(item);
      }
    }
    CATALOG = merged;
    _loaded = true;
  } catch {
    // Silently fall back to static items
  }
  return CATALOG;
}

/** Get current catalog synchronously (may be static if fetch hasn't completed). */
export function getCatalog(): CatalogItem[] {
  return CATALOG;
}
