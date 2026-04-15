import { describe, expect, it } from 'vitest';

import type { CatalogItem } from './catalog';
import {
  isMoonSurfaceAuthorityItem,
  summarizeCatalogSurface,
  summarizeCatalogTruth,
} from './actionTruth';

describe('actionTruth', () => {
  it('summarizes only first-class Moon surface items in the dock counts', () => {
    const items: CatalogItem[] = [
      {
        id: 'trigger-manual',
        label: 'Manual',
        icon: 'trigger',
        family: 'trigger',
        status: 'ready',
        dropKind: 'node',
        actionValue: 'trigger',
        source: 'surface_registry',
      },
      {
        id: 'gather-docs',
        label: 'Docs',
        icon: 'research',
        family: 'gather',
        status: 'ready',
        dropKind: 'node',
        actionValue: 'auto/research',
        source: 'surface_registry',
      },
      {
        id: 'ctrl-branch',
        label: 'Branch',
        icon: 'gate',
        family: 'control',
        status: 'ready',
        dropKind: 'edge',
        gateFamily: 'conditional',
        source: 'surface_registry',
      },
      {
        id: 'ctrl-approval',
        label: 'Approval',
        icon: 'gate',
        family: 'control',
        status: 'ready',
        dropKind: 'edge',
        gateFamily: 'approval',
        source: 'surface_registry',
      },
      {
        id: 'ctrl-validation',
        label: 'Validation',
        icon: 'gate',
        family: 'control',
        status: 'ready',
        dropKind: 'edge',
        gateFamily: 'validation',
        source: 'surface_registry',
      },
      {
        id: 'integration-slack-send',
        label: 'Slack send',
        icon: 'notify',
        family: 'act',
        status: 'ready',
        dropKind: 'node',
        actionValue: '@slack/send',
        source: 'integration',
      },
    ];

    const moonSurfaceItems = items.filter(isMoonSurfaceAuthorityItem);
    const truthSummary = summarizeCatalogTruth(moonSurfaceItems);
    const surfaceSummary = summarizeCatalogSurface(moonSurfaceItems);

    expect(moonSurfaceItems.map((item) => item.id)).toEqual([
      'trigger-manual',
      'gather-docs',
      'ctrl-branch',
      'ctrl-approval',
      'ctrl-validation',
    ]);
    expect(truthSummary.nodeTotal).toBe(2);
    expect(truthSummary.nodeCounts.runtime).toBe(1);
    expect(truthSummary.nodeCounts.alias).toBe(1);
    expect(truthSummary.edgeTotal).toBe(3);
    expect(truthSummary.edgeCounts.runtime).toBe(3);
    expect(truthSummary.edgeCounts.persisted).toBe(0);
    expect(surfaceSummary.nodeCounts.primary).toBe(1);
    expect(surfaceSummary.nodeCounts.hidden).toBe(1);
    expect(surfaceSummary.edgeCounts.primary).toBe(3);
    expect(surfaceSummary.edgeCounts.advanced).toBe(0);
  });
});
