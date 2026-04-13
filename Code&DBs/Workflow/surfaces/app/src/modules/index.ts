import React from 'react';
import { blockCatalog } from '../blocks/catalog';
import { registerModule, resolveModule, listModules } from './moduleRegistry';
import type { ModuleDefinition, QuadrantProps } from './types';

function lazyComponent(
  loader: () => Promise<{ default: React.ComponentType<QuadrantProps> }>,
): React.LazyExoticComponent<React.ComponentType<QuadrantProps>> {
  return React.lazy(loader);
}

function lazyDefault<TModule extends { default: React.ComponentType<QuadrantProps> }>(
  loader: () => Promise<TModule>,
): React.LazyExoticComponent<React.ComponentType<QuadrantProps>> {
  return lazyComponent(() =>
    loader().then((m) => ({ default: m.default as React.ComponentType<QuadrantProps> })),
  );
}

function lazyNamed<TModule extends Record<string, unknown>>(
  loader: () => Promise<TModule>,
  exportName: keyof TModule,
): React.LazyExoticComponent<React.ComponentType<QuadrantProps>> {
  return lazyComponent(() =>
    loader().then((m) => ({ default: m[exportName] as React.ComponentType<QuadrantProps> })),
  );
}

const componentLoaders: Record<string, React.LazyExoticComponent<React.ComponentType<QuadrantProps>>> = {
  metric: lazyDefault(() => import('./display/MetricModule')),
  'stat-row': lazyDefault(() => import('./display/StatRowModule')),
  chart: lazyDefault(() => import('./display/ChartModule')),
  'activity-feed': lazyDefault(() => import('./display/ActivityFeedModule')),
  'status-grid': lazyDefault(() => import('./display/StatusGridModule')),
  markdown: lazyDefault(() => import('./display/MarkdownModule')),
  'key-value': lazyDefault(() => import('./display/KeyValueModule')),
  'data-table': lazyDefault(() => import('./display/DataTableModule')),
  'text-input': lazyNamed(() => import('./input/TextInputModule'), 'TextInputModule'),
  'intent-box': lazyDefault(() => import('./input/IntentBoxModule')),
  'dropdown-select': lazyNamed(() => import('./input/DropdownSelectModule'), 'DropdownSelectModule'),
  'button-row': lazyNamed(() => import('./input/ButtonRowModule'), 'ButtonRowModule'),
  'file-drop': lazyNamed(() => import('./input/FileDropModule'), 'FileDropModule'),
  'workflow-form': lazyDefault(() => import('./tool/WorkflowFormModule')),
  'search-panel': lazyDefault(() => import('./tool/SearchPanelModule')),
  'workflow-builder': lazyDefault(() => import('./tool/WorkflowBuilderModule')),
  'object-type-browser': lazyDefault(() => import('./tool/ObjectTypeBrowserModule')),
  'schema-editor': lazyDefault(() => import('./tool/SchemaEditorModule')),
  'workflow-status': lazyDefault(() => import('./composite/WorkflowStatusModule')),
  'bug-card': lazyDefault(() => import('./composite/BugCardModule')),
  'model-card': lazyDefault(() => import('./composite/ModelCardModule')),
  'registry-browser': lazyDefault(() => import('./composite/RegistryBrowserModule')),
  'slot-layout': lazyDefault(() => import('./composite/SlotLayoutModule')),
};

for (const entry of blockCatalog) {
  const component = componentLoaders[entry.id];
  if (!component) continue;
  registerModule({
    id: entry.id,
    name: entry.name,
    type: entry.type,
    defaultSpan: entry.defaultSpan,
    component,
    description: entry.description,
  } satisfies ModuleDefinition);
}

export { registerModule, resolveModule, listModules };
