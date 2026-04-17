import React, { useMemo } from 'react';
import { MenuPanel, type MenuSection } from '../menu';
import { resolveModule } from '../modules/moduleRegistry';
import { GRID_CHART_TYPES, gridFieldLabel, gridSpanLabel } from './moduleConfigMetadata';
import type { UiActionTarget } from '../control/uiActionLedger';

interface ModuleActionMenuProps {
  open: boolean;
  anchorRect: DOMRect | null;
  quadrantId: string;
  moduleId: string;
  moduleType: string;
  span: string;
  availableSpans: string[];
  config: Record<string, unknown>;
  onClose: () => void;
  onOpenConfig: (focusKey?: string | null) => void;
  onUpdateConfig: (
    nextConfig: Record<string, unknown>,
    meta: {
      label: string;
      reason: string;
      outcome: string;
      target?: UiActionTarget | null;
      changeSummary?: string[];
    },
  ) => void;
  onUpdateSpan: (
    nextSpan: string,
    meta: {
      label: string;
      reason: string;
      outcome: string;
      target?: UiActionTarget | null;
      changeSummary?: string[];
    },
  ) => void;
  onRemoveModule: () => void;
}

function currentChartKey(moduleId: string, config: Record<string, unknown>): 'chartType' | 'type' | null {
  if ('chartType' in config) return 'chartType';
  if ('type' in config || moduleId === 'chart') return 'type';
  return null;
}

export function ModuleActionMenu({
  open,
  anchorRect,
  quadrantId,
  moduleId,
  moduleType,
  span,
  availableSpans,
  config,
  onClose,
  onOpenConfig,
  onUpdateConfig,
  onUpdateSpan,
  onRemoveModule,
}: ModuleActionMenuProps) {
  const moduleDef = resolveModule(moduleId);

  const sections = useMemo<MenuSection[]>(() => {
    const nextSections: MenuSection[] = [];
    const quickItems = [];
    const chartKey = currentChartKey(moduleId, config);
    const hasEndpoint = moduleType === 'display' || moduleType === 'tool' || 'endpoint' in config;

    if (moduleType === 'input' || 'label' in config) {
      quickItems.push({
        id: 'edit-label',
        label: 'Edit label',
        description: 'Jump straight to the label field.',
        keywords: ['label', 'caption', 'rename'],
        meta: gridFieldLabel('label'),
        onSelect: () => onOpenConfig('label'),
      });
    }

    if (moduleType === 'input' || 'placeholder' in config) {
      quickItems.push({
        id: 'edit-placeholder',
        label: 'Edit placeholder',
        description: 'Update empty-state guidance for the control.',
        keywords: ['placeholder', 'hint', 'prompt'],
        meta: gridFieldLabel('placeholder'),
        onSelect: () => onOpenConfig('placeholder'),
      });
    }

    if ('title' in config || moduleType === 'display' || moduleType === 'composite') {
      quickItems.push({
        id: 'edit-title',
        label: 'Edit title',
        description: 'Rename the visible module title.',
        keywords: ['title', 'heading', 'name'],
        meta: gridFieldLabel('title'),
        onSelect: () => onOpenConfig('title'),
      });
    }

    if (Array.isArray(config.columns)) {
      quickItems.push({
        id: 'edit-columns',
        label: 'Edit columns',
        description: 'Manage visible fields and sorting behavior.',
        keywords: ['columns', 'table', 'fields'],
        meta: gridFieldLabel('columns'),
        onSelect: () => onOpenConfig('columns'),
      });
    }

    if (Array.isArray(config.actions)) {
      quickItems.push({
        id: 'edit-actions',
        label: 'Edit actions',
        description: 'Tune the module button row and variants.',
        keywords: ['actions', 'buttons'],
        meta: gridFieldLabel('actions'),
        onSelect: () => onOpenConfig('actions'),
      });
    }

    if (hasEndpoint) {
      quickItems.push({
        id: 'edit-endpoint',
        label: 'Edit endpoint',
        description: 'Open the endpoint field with live route suggestions and raw path input.',
        keywords: ['endpoint', 'source', 'api', 'data'],
        meta: gridFieldLabel('endpoint'),
        onSelect: () => onOpenConfig('endpoint'),
      });
    }

    if (hasEndpoint || 'path' in config) {
      quickItems.push({
        id: 'edit-path',
        label: 'Edit path',
        description: 'Update the value path extracted from the endpoint response.',
        keywords: ['path', 'json', 'selection'],
        meta: gridFieldLabel('path'),
        onSelect: () => onOpenConfig('path'),
      });
    }

    quickItems.push({
      id: 'resize-module',
      label: 'Resize module',
      description: 'Open the size control in the manual settings panel.',
      keywords: ['resize', 'size', 'span', 'layout'],
      meta: gridSpanLabel(span),
      onSelect: () => onOpenConfig('span'),
    });

    quickItems.push({
      id: 'advanced-config',
      label: 'Advanced config',
      description: 'Open the full module config editor.',
      keywords: ['advanced', 'config', 'settings'],
      meta: 'All fields',
      onSelect: () => onOpenConfig(null),
    });

    nextSections.push({
      id: 'quick',
      title: 'Quick Edit',
      items: quickItems,
    });

    if (availableSpans.length > 0) {
      nextSections.push({
        id: 'size',
        title: 'Size',
        items: availableSpans.map((spanOption) => ({
          id: `span:${spanOption}`,
          label: gridSpanLabel(spanOption),
          description: spanOption === span
            ? 'Current module footprint.'
            : `Resize this module to ${gridSpanLabel(spanOption)}.`,
          keywords: ['size', 'span', 'layout', spanOption],
          selected: span === spanOption,
          onSelect: () => onUpdateSpan(
            spanOption,
            {
              label: 'Resize module',
              reason: `Resize quadrant ${quadrantId} to ${gridSpanLabel(spanOption)}.`,
              outcome: `The module now occupies ${gridSpanLabel(spanOption)} in the grid.`,
              target: {
                kind: 'quadrant',
                label: `${quadrantId} · ${moduleDef?.name ?? moduleId}`,
                id: quadrantId,
              },
              changeSummary: [`Size ${gridSpanLabel(spanOption)}`],
            },
          ),
        })),
      });
    }

    if (chartKey) {
      nextSections.push({
        id: 'chart-type',
        title: 'Chart Type',
        items: GRID_CHART_TYPES.map((chartType) => ({
          id: `${chartKey}:${chartType}`,
          label: chartType[0].toUpperCase() + chartType.slice(1),
          description: `Switch this module to a ${chartType} chart.`,
          keywords: ['chart', 'visualization', chartType],
          selected: String(config[chartKey] ?? 'bar') === chartType,
          onSelect: () => onUpdateConfig(
            { ...config, [chartKey]: chartType },
            {
              label: 'Change chart type',
              reason: `Update quadrant ${quadrantId} to use the ${chartType} chart variant.`,
              outcome: `The module now renders as a ${chartType} chart.`,
              target: {
                kind: 'quadrant',
                label: `${quadrantId} · ${moduleDef?.name ?? moduleId}`,
                id: quadrantId,
              },
              changeSummary: ['Chart type', chartType],
            },
          ),
        })),
      });
    }

    nextSections.push({
      id: 'danger',
      title: 'Danger',
      items: [
        {
          id: 'remove-module',
          label: 'Remove module',
          description: `Delete the module from quadrant ${quadrantId}.`,
          keywords: ['remove', 'delete', 'clear'],
          tone: 'danger',
          onSelect: onRemoveModule,
        },
      ],
    });

    return nextSections;
  }, [availableSpans, config, moduleId, moduleType, onOpenConfig, onRemoveModule, onUpdateConfig, onUpdateSpan, quadrantId, span]);

  return (
    <MenuPanel
      open={open}
      anchorRect={anchorRect}
      title={`${moduleDef?.name ?? moduleId} actions`}
      subtitle={`Quadrant ${quadrantId}`}
      searchPlaceholder="Search actions and settings…"
      sections={sections}
      onClose={onClose}
    />
  );
}
