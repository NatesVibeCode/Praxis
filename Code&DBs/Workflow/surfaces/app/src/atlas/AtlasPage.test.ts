import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

import { describe, expect, it } from 'vitest';

import { buildElements, buildSemanticModel } from './AtlasPage';

function readAtlasCss() {
  const candidates = [
    join(process.cwd(), 'src/atlas/AtlasPage.css'),
    join(process.cwd(), 'surfaces/app/src/atlas/AtlasPage.css'),
    join(process.cwd(), 'Code&DBs/Workflow/surfaces/app/src/atlas/AtlasPage.css'),
  ];
  const cssPath = candidates.find((candidate) => existsSync(candidate));
  if (!cssPath) {
    throw new Error(`Could not locate AtlasPage.css from ${process.cwd()}`);
  }
  return readFileSync(cssPath, 'utf8');
}

function makePayload() {
  return {
    ok: true,
    nodes: [
      {
        data: {
          id: 'area::authority',
          label: 'Authority',
          area: 'authority',
          is_area: true,
          activity_score: 0.88,
        },
      },
      {
        data: {
          id: 'area::memory',
          label: 'Memory',
          area: 'memory',
          is_area: true,
          activity_score: 0.5,
        },
      },
      {
        data: {
          id: 'operator_decisions',
          label: 'operator_decisions',
          type: 'table',
          area: 'authority',
          activity_score: 0.92,
        },
      },
      {
        data: {
          id: 'memory_entities',
          label: 'memory_entities',
          type: 'table',
          area: 'memory',
          activity_score: 0.62,
        },
      },
    ],
    edges: [
      {
        data: {
          id: 'area::authority|depends_on|area::memory',
          source: 'area::authority',
          target: 'area::memory',
          label: 'depends_on',
          weight: 3,
          is_aggregate: true,
          activity_score: 0.9,
        },
      },
    ],
    areas: [
      { slug: 'authority', title: 'Authority', summary: 'Decision authority', color: '#f3efe6', member_count: 2 },
      { slug: 'memory', title: 'Memory', summary: 'Mutable memory', color: '#d8d2c5', member_count: 2 },
    ],
    metadata: {
      node_count: 4,
      edge_count: 1,
      aggregate_edge_count: 1,
      source_authority: 'Praxis.db',
      generated_at: '2026-04-24T12:00:00Z',
    },
    warnings: [],
  };
}

describe('AtlasPage', () => {
  it('builds an area-first overview without the old functional_area filter path', () => {
    const model = buildSemanticModel(makePayload() as never);
    const elements = buildElements(model, 'overview', null, null);

    expect(model.areaSignals.get('authority')?.authorityCount).toBe(1);
    expect(model.areaSignals.get('memory')?.memberCount).toBe(2);
    expect(elements.filter((element) => element.data?.node_kind === 'area')).toHaveLength(2);
    expect(elements.some((element) => element.data?.node_kind === 'object')).toBe(false);
    expect(elements.some((element) => element.classes === 'overview-dependency')).toBe(true);
  });

  it('keeps the narrow-width responsive layout contract in the stylesheet', () => {
    const atlasCss = readAtlasCss();

    expect(atlasCss).toContain('@media (max-width: 760px)');
    expect(atlasCss).toContain('.atlas-card {');
    expect(atlasCss).toContain('left: 10px !important;');
    expect(atlasCss).toContain('bottom: 48px;');
    expect(atlasCss).toContain('.atlas-semantic-strip {');
    expect(atlasCss).toContain('display: none;');
    expect(atlasCss).toContain('.atlas-area-hint {');
  });
});
