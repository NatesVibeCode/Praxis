export type PresetCategory =
  | 'overview'
  | 'engineering'
  | 'workflows'
  | 'models'
  | 'bugs'
  | 'costs'
  | 'building';

export interface ModulePreset {
  presetId: string;
  moduleId: string;
  span?: string;
  config: Record<string, unknown>;
  name: string;
  description: string;
  category: PresetCategory;
  icon?: string;
}

const CATEGORY_ORDER: PresetCategory[] = [
  'overview',
  'engineering',
  'workflows',
  'models',
  'bugs',
  'costs',
  'building',
];

export const CATEGORY_LABELS: Record<PresetCategory, string> = {
  overview: 'Platform Overview',
  engineering: 'Engineering Observability',
  workflows: 'Workflow Runs',
  models: 'Model Performance',
  bugs: 'Bug Tracking',
  costs: 'Cost & Spending',
  building: 'Building Tools',
};

const MODULE_PRESETS: ModulePreset[] = [
  // ── Overview ──────────────────────────────────────────────
  {
    presetId: 'pass-rate',
    moduleId: 'metric',
    span: '1x1',
    name: 'Pass Rate',
    description: 'Workflow pass rate (24h)',
    category: 'overview',
    icon: '✓',
    config: {
      endpoint: 'platform-overview',
      path: 'pass_rate',
      label: 'Pass Rate',
      format: 'percent',
    },
  },
  {
    presetId: 'open-bugs-count',
    moduleId: 'metric',
    span: '1x1',
    name: 'Open Bugs',
    description: 'Count of unresolved bugs',
    category: 'overview',
    icon: '!',
    config: {
      endpoint: 'platform-overview',
      path: 'open_bugs',
      label: 'Open Bugs',
      color: 'var(--danger)',
    },
  },
  {
    presetId: 'total-runs',
    moduleId: 'metric',
    span: '1x1',
    name: 'Total Runs',
    description: 'Workflow runs in last 24h',
    category: 'overview',
    icon: '#',
    config: {
      endpoint: 'platform-overview',
      path: 'total_workflow_runs',
      label: 'Total Runs',
    },
  },
  {
    presetId: 'platform-stats',
    moduleId: 'stat-row',
    span: '2x1',
    name: 'Platform Stats',
    description: 'Key metrics in one row',
    category: 'overview',
    icon: '═',
    config: {
      endpoint: 'platform-overview',
      stats: [
        { path: 'pass_rate', label: 'Pass Rate', format: 'percent' },
        { path: 'total_workflow_runs', label: 'Runs' },
        { path: 'open_bugs', label: 'Open Bugs', color: 'var(--danger)' },
        { path: 'total_bugs', label: 'Total Bugs' },
      ],
    },
  },
  {
    presetId: 'active-models',
    moduleId: 'status-grid',
    span: '2x2',
    name: 'Active Models',
    description: 'Model status grid',
    category: 'overview',
    icon: '▦',
    config: {
      endpoint: 'platform-overview',
      path: 'active_models',
      title: 'Active Models',
    },
  },
  {
    presetId: 'platform-kv',
    moduleId: 'key-value',
    span: '2x1',
    name: 'Platform Summary',
    description: 'All platform metrics as key-value pairs',
    category: 'overview',
    icon: '≡',
    config: {
      endpoint: 'platform-overview',
    },
  },

  // ── Engineering Observability ────────────────────────────
  {
    presetId: 'platform-observability-row',
    moduleId: 'stat-row',
    span: '2x1',
    name: 'Platform Pulse',
    description: 'Live probe and operator-state summary',
    category: 'engineering',
    icon: '≣',
    config: {
      endpoint: 'observability/platform',
      stats: [
        { path: 'summary.overall', label: 'Overall' },
        { path: 'summary.queue_depth', label: 'Queue' },
        { path: 'summary.failed_checks', label: 'Failed', color: 'var(--danger)' },
        { path: 'summary.write_conflicts', label: 'Conflicts', color: 'var(--danger)' },
      ],
    },
  },
  {
    presetId: 'platform-probes-grid',
    moduleId: 'status-grid',
    span: '2x2',
    name: 'Platform Probes',
    description: 'Probe-by-probe platform health',
    category: 'engineering',
    icon: '▦',
    config: {
      endpoint: 'observability/platform',
      path: 'checks',
      title: 'Platform Probes',
    },
  },
  {
    presetId: 'code-hotspots-table',
    moduleId: 'data-table',
    span: '2x2',
    name: 'Code Hotspots',
    description: 'Files where code risk, bugs, and regressions cluster',
    category: 'engineering',
    icon: '▤',
    config: {
      endpoint: 'observability/code-hotspots',
      path: 'files',
      columns: [
        { key: 'file_path', label: 'File' },
        { key: 'component', label: 'Component' },
        { key: 'hotspot_score', label: 'Score' },
        { key: 'open_bug_count', label: 'Open Bugs' },
        { key: 'regression_count', label: 'Regressions' },
      ],
    },
  },
  {
    presetId: 'hotspot-components-chart',
    moduleId: 'chart',
    span: '2x2',
    name: 'Hotspot Components',
    description: 'Which subsystems are concentrating pain',
    category: 'engineering',
    icon: '▥',
    config: {
      endpoint: 'observability/code-hotspots',
      path: 'components',
      type: 'bar',
      xKey: 'component',
      yKey: 'score',
      title: 'Hotspot Components',
    },
  },
  {
    presetId: 'bug-scoreboard-row',
    moduleId: 'stat-row',
    span: '2x1',
    name: 'Bug Scoreboard',
    description: 'Replay readiness, regressions, and bug pressure',
    category: 'engineering',
    icon: '═',
    config: {
      endpoint: 'observability/bug-scoreboard',
      stats: [
        { path: 'summary.total_bugs', label: 'Total Bugs' },
        { path: 'summary.open_bugs', label: 'Open Bugs', color: 'var(--danger)' },
        { path: 'summary.replay_ready_bugs', label: 'Replay Ready' },
        { path: 'summary.regression_bugs', label: 'Regressions', color: 'var(--danger)' },
      ],
    },
  },
  {
    presetId: 'recurring-bugs-table',
    moduleId: 'data-table',
    span: '2x2',
    name: 'Recurring Bugs',
    description: 'Open bugs with repeated impact and observability gaps',
    category: 'engineering',
    icon: '▤',
    config: {
      endpoint: 'observability/bug-scoreboard',
      path: 'top_recurring',
      columns: [
        { key: 'title', label: 'Bug' },
        { key: 'severity', label: 'Severity' },
        { key: 'recurrence_count', label: 'Recurrences' },
        { key: 'observability_state', label: 'Observability' },
      ],
    },
  },

  // ── Workflow Runs ─────────────────────────────────────────
  {
    presetId: 'recent-runs',
    moduleId: 'data-table',
    span: '2x2',
    name: 'Recent Runs',
    description: 'Latest workflow runs with status',
    category: 'workflows',
    icon: '▤',
    config: {
      endpoint: 'runs/recent',
      columns: [
        { key: 'spec_name', label: 'Workflow' },
        { key: 'status', label: 'Status' },
        { key: 'completed_jobs', label: 'Done' },
        { key: 'total_cost', label: 'Cost' },
      ],
    },
  },
  {
    presetId: 'recent-activity',
    moduleId: 'activity-feed',
    span: '2x2',
    name: 'Recent Activity',
    description: 'Live feed of workflow events',
    category: 'workflows',
    icon: '↕',
    config: {
      endpoint: 'platform-overview',
      path: 'recent_workflows',
      title: 'Recent Activity',
    },
  },
  {
    presetId: 'run-status-breakdown',
    moduleId: 'chart',
    span: '2x2',
    name: 'Run Status Breakdown',
    description: 'Pie chart of run outcomes',
    category: 'workflows',
    icon: '◔',
    config: {
      endpoint: 'runs/recent',
      type: 'pie',
      groupBy: 'status',
      title: 'Run Status',
    },
  },

  // ── Model Performance ─────────────────────────────────────
  {
    presetId: 'model-leaderboard',
    moduleId: 'data-table',
    span: '2x2',
    name: 'Model Leaderboard',
    description: 'Agent pass/fail rates ranked',
    category: 'models',
    icon: '▤',
    config: {
      endpoint: 'leaderboard',
      columns: [
        { key: 'model', label: 'Model' },
        { key: 'passed_tasks', label: 'Passed' },
        { key: 'failed_tasks', label: 'Failed' },
        { key: 'pass_rate', label: 'Pass Rate' },
      ],
    },
  },
  {
    presetId: 'model-pass-rates',
    moduleId: 'chart',
    span: '2x2',
    name: 'Model Pass Rates',
    description: 'Bar chart comparing model performance',
    category: 'models',
    icon: '▥',
    config: {
      endpoint: 'leaderboard',
      type: 'bar',
      xKey: 'model',
      yKey: 'pass_rate',
      title: 'Model Pass Rates',
    },
  },

  // ── Bug Tracking ──────────────────────────────────────────
  {
    presetId: 'open-bugs-table',
    moduleId: 'data-table',
    span: '2x2',
    name: 'Open Tickets',
    description: 'Actionable open tickets with replay state',
    category: 'bugs',
    icon: '▤',
    config: {
      endpoint: 'bugs?open_only=1&include_replay_state=1&limit=50',
      columns: [
        { key: 'bug_id', label: 'Ticket' },
        { key: 'title', label: 'Bug' },
        { key: 'severity', label: 'Severity' },
        { key: 'status', label: 'Status' },
        { key: 'replay_ready', label: 'Replay' },
      ],
    },
  },
  {
    presetId: 'open-ticket-workbench',
    moduleId: 'bug-card',
    span: '2x2',
    name: 'Ticket Workbench',
    description: 'Open ticket queue with severity, replay, and detail drill-in',
    category: 'bugs',
    icon: '▦',
    config: {
      endpoint: 'bugs?open_only=1&include_replay_state=1&limit=20',
      title: 'Open Tickets',
    },
  },
  {
    presetId: 'bug-severity-chart',
    moduleId: 'chart',
    span: '2x2',
    name: 'Bug Severity',
    description: 'Breakdown by severity level',
    category: 'bugs',
    icon: '◔',
    config: {
      endpoint: 'platform-overview',
      path: 'bug_severity',
      type: 'pie',
      xKey: 'code',
      yKey: 'count',
      title: 'Bugs by Severity',
    },
  },

  // ── Cost & Spending ───────────────────────────────────────
  {
    presetId: 'cost-summary',
    moduleId: 'key-value',
    span: '2x1',
    name: 'Cost Summary',
    description: 'Token spend breakdown',
    category: 'costs',
    icon: '$',
    config: {
      endpoint: 'costs',
    },
  },
  {
    presetId: 'cost-by-run',
    moduleId: 'data-table',
    span: '2x2',
    name: 'Cost by Run',
    description: 'Per-run cost breakdown',
    category: 'costs',
    icon: '▤',
    config: {
      endpoint: 'runs/recent',
      columns: [
        { key: 'spec_name', label: 'Workflow' },
        { key: 'total_cost', label: 'Cost ($)' },
        { key: 'status', label: 'Status' },
      ],
    },
  },

  // ── Building Tools ────────────────────────────────────────
  {
    presetId: 'workflow-builder',
    moduleId: 'workflow-builder',
    span: '2x2',
    name: 'Workflow Builder',
    description: 'Visual workflow editor',
    category: 'building',
    icon: '⚒',
    config: {},
  },
  {
    presetId: 'search',
    moduleId: 'search-panel',
    span: '2x2',
    name: 'Search',
    description: 'Search across registries',
    category: 'building',
    icon: '⌕',
    config: {},
  },
  {
    presetId: 'notes',
    moduleId: 'markdown',
    span: '2x2',
    name: 'Notes',
    description: 'Freeform markdown notes',
    category: 'building',
    icon: '¶',
    config: {
      content: '# Notes\n\nStart typing here...',
    },
  },
];

export function listPresets(): ModulePreset[] {
  return MODULE_PRESETS;
}

export function listPresetsByCategory(): { category: PresetCategory; label: string; presets: ModulePreset[] }[] {
  const grouped: Record<string, ModulePreset[]> = {};
  for (const p of MODULE_PRESETS) {
    (grouped[p.category] ??= []).push(p);
  }
  return CATEGORY_ORDER
    .filter(cat => grouped[cat]?.length)
    .map(cat => ({ category: cat, label: CATEGORY_LABELS[cat], presets: grouped[cat] }));
}

export function getPreset(presetId: string): ModulePreset | undefined {
  return MODULE_PRESETS.find(p => p.presetId === presetId);
}
