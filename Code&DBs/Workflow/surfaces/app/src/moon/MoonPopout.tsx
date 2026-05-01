import React, { useMemo } from 'react';
import type { OrbitNode, DockContent } from './moonBuildPresenter';
import type { CatalogItem } from './catalog';
import { getCatalogSurfacePolicy, getCatalogTruth } from './actionTruth';
import { MoonGlyph } from './MoonGlyph';
import { MenuPanel, type MenuSection } from '../menu';

interface Props {
  node: OrbitNode;
  content: DockContent | null;
  anchorRect: DOMRect | null;
  onClose: () => void;
  onSelect: (nodeId: string, value: string) => void;
  catalog: CatalogItem[];
  onStartCatalogDrag?: (event: React.PointerEvent, item: CatalogItem) => void;
}

const NODE_FAMILIES = ['trigger', 'gather', 'think', 'act'] as const;

function questionFor(node: OrbitNode): string {
  if (!node.route) return 'What should this step do?';
  if (node.ringState === 'decided-incomplete') return 'What else does this step need?';
  return 'Change this step?';
}

export function MoonPopout({
  node,
  content: _content,
  anchorRect,
  onClose,
  onSelect,
  catalog,
  onStartCatalogDrag: _onStartCatalogDrag,
}: Props) {
  const nodeActions = useMemo(
    () => catalog
      .filter((item) => item.dropKind === 'node' && item.status === 'ready')
      .map((item) => ({
        item,
        truth: getCatalogTruth(item),
        policy: getCatalogSurfacePolicy(item),
      })),
    [catalog],
  );

  const sections = useMemo<MenuSection[]>(() => {
    const visibleRouteValues = new Set(
      nodeActions
        .filter(({ policy, item }) => policy.tier === 'primary' && item.actionValue)
        .map(({ item }) => item.actionValue as string),
    );

    const toMenuAction = (
      item: CatalogItem,
      detail: string,
      meta: string,
    ) => ({
      id: item.id,
      label: item.label,
      description: detail,
      keywords: [item.actionValue || '', item.family, meta, detail, item.source || '', item.connectionStatus || ''],
      selected: node.route === item.actionValue,
      meta,
      icon: <MoonGlyph type={item.icon} size={14} color={node.route === item.actionValue ? 'currentColor' : 'var(--text)'} />,
      onSelect: () => {
        if (item.actionValue) onSelect(node.id, item.actionValue);
      },
    });

    const primarySections = NODE_FAMILIES.map((family) => ({
      id: family,
      title: family[0].toUpperCase() + family.slice(1),
      items: nodeActions
        .filter(({ item, policy }) => item.family === family && policy.tier === 'primary')
        .map(({ item, truth, policy }) => toMenuAction(item, policy.detail, truth.badge)),
    })).filter((section) => section.items.length > 0);

    const legacyItems = nodeActions
      .filter(({ item, policy }) => (
        policy.tier === 'hidden'
        && item.actionValue === node.route
        && item.actionValue
        && !visibleRouteValues.has(item.actionValue)
      ))
      .map(({ item, policy }) => toMenuAction(item, policy.hardChoice || policy.detail, policy.badge));

    return [
      ...primarySections,
      ...(legacyItems.length > 0 ? [{ id: 'legacy', title: 'Hidden / legacy', items: legacyItems }] : []),
    ];
  }, [node.id, node.route, nodeActions, onSelect]);

  return (
    <MenuPanel
      open
      anchorRect={anchorRect}
      title={node.title}
      subtitle={questionFor(node)}
      searchPlaceholder="Search capabilities…"
      sections={sections}
      onClose={onClose}
      width={360}
    />
  );
}
