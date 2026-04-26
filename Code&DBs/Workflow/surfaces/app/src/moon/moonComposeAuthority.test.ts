import { describe, expect, it } from 'vitest';
import {
  buildAuthorityCompileProse,
  readUiExperiencePayload,
  summarizeComposeAuthority,
} from './moonComposeAuthority';

describe('moon compose authority', () => {
  it('reads operation-catalog envelopes and summarizes Atlas freshness', () => {
    const summary = summarizeComposeAuthority(
      {
        view: 'ui_experience_graph',
        payload: {
          source_authority: 'Praxis.db surface catalog plus app shell source registry',
          counts: { surface_controls_returned: 7 },
        },
      },
      {
        metadata: {
          freshness: {
            graph_freshness_state: 'fresh',
          },
        },
      },
    );

    expect(summary).toEqual({
      status: 'ready',
      buildControlCount: 7,
      atlasFreshness: 'fresh',
      sourceAuthority: 'Praxis.db surface catalog plus app shell source registry',
      warning: null,
    });
  });

  it('marks the snapshot degraded when only one authority surface responds', () => {
    const summary = summarizeComposeAuthority(
      { payload: { surface_controls: [{ id: 'trigger-manual' }] } },
      null,
    );

    expect(summary.status).toBe('degraded');
    expect(summary.buildControlCount).toBe(1);
    expect(summary.warning).toBe('Partial authority snapshot');
  });

  it('returns only the operator prose plus optional trigger annotation', () => {
    // BUG-C6EE740C: the previous behavior prepended 8 lines of system
    // context onto the user's prose, and those lines were getting compiled
    // INTO the workflow as prose-shaped nodes. Now the function returns
    // only what the operator typed, optionally annotated with the selected
    // trigger label. Authority/freshness/build-control diagnostics live in
    // the backend system prompt, not in the operator's input.
    const prose = buildAuthorityCompileProse({
      prose: 'Research leads and route good ones to sales.',
      triggerLabel: 'Manual',
      summary: {
        status: 'ready',
        buildControlCount: 4,
        atlasFreshness: 'fresh',
        sourceAuthority: 'Praxis.db surface catalog',
        warning: null,
      },
    });

    expect(prose).not.toContain('Build this as a Praxis workflow graph');
    expect(prose).not.toContain('Workflow authority:');
    expect(prose).not.toContain('Atlas freshness:');
    expect(prose).not.toContain('Available builder controls:');
    expect(prose).not.toContain('Operator request:');
    expect(prose).toBe('Research leads and route good ones to sales.\n\nSelected trigger: Manual');
  });

  it('returns only the operator prose when no trigger is selected', () => {
    const prose = buildAuthorityCompileProse({
      prose: 'Research leads and route good ones to sales.',
      summary: {
        status: 'ready',
        buildControlCount: 4,
        atlasFreshness: 'fresh',
        sourceAuthority: 'Praxis.db surface catalog',
        warning: null,
      },
    });

    expect(prose).toBe('Research leads and route good ones to sales.');
  });

  it('can read a direct payload when the gateway envelope is absent', () => {
    expect(readUiExperiencePayload({ source_authority: 'direct' })).toEqual({
      source_authority: 'direct',
    });
  });

  it('reads unified operate gateway envelopes', () => {
    expect(readUiExperiencePayload({
      ok: true,
      result: {
        view: 'ui_experience_graph',
        payload: { source_authority: 'operate gateway' },
      },
    })).toEqual({ source_authority: 'operate gateway' });
  });
});
