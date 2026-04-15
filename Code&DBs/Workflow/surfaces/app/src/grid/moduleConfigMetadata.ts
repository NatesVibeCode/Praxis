export const GRID_CHART_TYPES = ['bar', 'line', 'pie'] as const;
export const GRID_ACTION_VARIANTS = ['primary', 'secondary', 'danger', 'ghost'] as const;

export const GRID_TEXT_KEYS = new Set([
  'objectType',
  'title',
  'placeholder',
  'publishSelection',
  'subscribeSelection',
  'path',
  'label',
  'format',
  'color',
  'xKey',
  'yKey',
  'groupBy',
  'worldPath',
  'searchQuery',
  'content',
  'onSubmitEndpoint',
]);

export const GRID_CHART_TYPE_KEYS = new Set(['chartType', 'type']);

export const GRID_DATA_SOURCES = [
  { value: 'platform-overview', label: 'Platform Overview' },
  { value: 'observability/platform', label: 'Platform Observability' },
  { value: 'observability/code-hotspots', label: 'Code Hotspots' },
  { value: 'observability/bug-scoreboard', label: 'Bug Scoreboard' },
  { value: 'runs/recent', label: 'Recent Runs' },
  { value: 'leaderboard', label: 'Model Leaderboard' },
  { value: 'workflow-status', label: 'Workflow Status' },
  { value: 'costs', label: 'Cost Summary' },
  { value: 'bugs', label: 'Bug List' },
  { value: 'models', label: 'Available Models' },
] as const;

const FIELD_LABELS: Record<string, string> = {
  actions: 'Actions',
  chartType: 'Chart type',
  columns: 'Columns',
  content: 'Content',
  endpoint: 'Data source',
  label: 'Label',
  objectType: 'Object type',
  onSubmitEndpoint: 'Submit endpoint',
  path: 'Path',
  placeholder: 'Placeholder',
  publishSelection: 'Publish selection',
  refreshInterval: 'Refresh interval',
  subscribeSelection: 'Subscribe selection',
  title: 'Title',
  type: 'Chart type',
  worldPath: 'World path',
  xKey: 'X axis key',
  yKey: 'Y axis key',
};

export function gridFieldLabel(key: string): string {
  return FIELD_LABELS[key] || key.replace(/([a-z])([A-Z])/g, '$1 $2').replace(/^./, (char) => char.toUpperCase());
}

export function defaultConfigValueForKey(key: string): unknown {
  if (key === 'columns' || key === 'actions') return [];
  if (key === 'refreshInterval') return 0;
  if (key === 'chartType' || key === 'type') return 'bar';
  return '';
}
