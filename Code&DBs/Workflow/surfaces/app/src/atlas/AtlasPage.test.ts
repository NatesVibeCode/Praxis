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

function readAtlasSource() {
  const candidates = [
    join(process.cwd(), 'src/atlas/AtlasPage.tsx'),
    join(process.cwd(), 'surfaces/app/src/atlas/AtlasPage.tsx'),
    join(process.cwd(), 'Code&DBs/Workflow/surfaces/app/src/atlas/AtlasPage.tsx'),
  ];
  const sourcePath = candidates.find((candidate) => existsSync(candidate));
  if (!sourcePath) {
    throw new Error(`Could not locate AtlasPage.tsx from ${process.cwd()}`);
  }
  return readFileSync(sourcePath, 'utf8');
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

  it('marks graph changes from the event stream instead of interval polling', () => {
    const model = buildSemanticModel(makePayload() as never);
    const elements = buildElements(model, 'overview', null, null, new Set(['authority']));
    const changed = elements.find((element) => element.data?.id === 'area::authority');
    const source = readAtlasSource();

    expect(changed?.classes).toContain('atlas-live-changed');
    expect(source).toContain("new window.EventSource(ATLAS_GRAPH_STREAM_PATH)");
    expect(source).not.toContain('ATLAS_POLL_MS');
    expect(source).not.toContain('window.setInterval(() => { refresh(true); }');
  });

  it('renders a provenance chain inside the focus card (B4.3)', () => {
    const source = readAtlasSource();
    const css = readAtlasCss();

    // The focus card invokes the provenance subcomponent
    expect(source).toContain('<AtlasProvenanceChain node={node} />');
    expect(source).toContain('function AtlasProvenanceChain');

    // Composes the canonical prx-chain primitive through the React adapter.
    expect(source).toContain('EventChain');
    expect(source).toContain('atlas-card__provenance');

    // Surfaces the canonical lineage fields the payload already carries
    expect(source).toContain('node.authority_source');
    expect(source).toContain('node.relation_source');
    expect(source).toContain('node.binding_revision');
    expect(source).toContain('node.decision_ref');

    // CSS hooks the chain inside the focus card
    expect(css).toContain('.atlas-card__provenance');
    expect(css).toContain('.atlas-card__provenance .prx-chain');
  });

  it('exposes three-view toggle with URL sync (constellation / contact / ledger)', () => {
    const source = readAtlasSource();

    expect(source).toContain("type AtlasView = 'constellation' | 'contact' | 'ledger'");
    expect(source).toContain('readViewFromUrl');
    expect(source).toContain('syncViewToUrl');

    expect(source).toContain("import { AtlasConstellation } from './AtlasConstellation'");
    expect(source).toContain("import { AtlasContactSheet } from './AtlasContactSheet'");
    expect(source).toContain("import { AtlasLedger } from './AtlasLedger'");
    expect(source).toContain('RadioPillGroup');

    expect(source).toContain('atlas-view-toggle');

    expect(source).toContain("view === 'constellation'");
    expect(source).toContain("view === 'contact'");
    expect(source).toContain("view === 'ledger'");
  });

  it('keeps the narrow-width responsive layout contract in the stylesheet', () => {
    const atlasCss = readAtlasCss();

    expect(atlasCss).toContain('@media (max-width: 760px)');
    expect(atlasCss).toContain('.atlas-card {');
    expect(atlasCss).toContain('left: 10px !important;');
    expect(atlasCss).toContain('bottom: 48px;');
    expect(atlasCss).toContain('.atlas-semantic-strip {');
    expect(atlasCss).toContain('display: none;');
    expect(atlasCss).toContain('.atlas-ledger {');
  });
});
