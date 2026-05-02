import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { CanvasSurfaceReviewPanel } from './CanvasSurfaceReviewPanel';

const reviewPanelMocks = vi.hoisted(() => ({
  fetchCatalogReviewDecisions: vi.fn(),
  postCatalogReviewDecision: vi.fn(),
}));

vi.mock('../shared/buildController', () => ({
  fetchCatalogReviewDecisions: reviewPanelMocks.fetchCatalogReviewDecisions,
  postCatalogReviewDecision: reviewPanelMocks.postCatalogReviewDecision,
}));

describe('CanvasSurfaceReviewPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    reviewPanelMocks.fetchCatalogReviewDecisions.mockResolvedValue({ review_decisions: [] });
    reviewPanelMocks.postCatalogReviewDecision.mockResolvedValue({
      review_decision: {
        review_decision_id: 'scrd_001',
        target_kind: 'source_policy',
        target_ref: 'capability',
        decision: 'approve',
      },
    });
  });

  it('records a DB-backed source policy review decision from the dock surface', async () => {
    const onCatalogReload = vi.fn(async () => undefined);

    render(
      <CanvasSurfaceReviewPanel
        catalogItems={[
          {
            id: 'ctrl-retry',
            label: 'Retry',
            icon: 'gate',
            family: 'control',
            status: 'ready',
            dropKind: 'edge',
            gateFamily: 'retry',
            source: 'surface_registry',
            truth: {
              category: 'runtime',
              badge: 'Executes',
              detail: 'Sets downstream retry policy.',
            },
            surfacePolicy: {
              tier: 'advanced',
              badge: 'Later',
              detail: 'Retry stays outside the primary gate surface.',
            },
          },
        ]}
        sourcePolicies={[
          {
            sourceKind: 'capability',
            truth: {
              category: 'runtime',
              badge: 'Runs on release',
              detail: 'Capability rows are backed by runtime lanes.',
            },
            surfacePolicy: {
              tier: 'hidden',
              badge: 'Hidden',
              detail: 'Capability rows stay off the main builder.',
            },
          },
        ]}
        onCatalogReload={onCatalogReload}
      />,
    );

    fireEvent.click(screen.getByRole('button', { name: /surface review/i }));
    fireEvent.click(screen.getByRole('button', { name: 'Dynamic lanes' }));
    fireEvent.click(screen.getByRole('button', { name: /Capability lanes/i }));
    fireEvent.change(screen.getByLabelText('Surface tier'), { target: { value: 'advanced' } });
    fireEvent.change(screen.getByLabelText('Surface badge'), { target: { value: 'Later' } });
    fireEvent.change(screen.getByLabelText('Surface detail'), { target: { value: 'Show capability rows in advanced review surfaces.' } });
    fireEvent.change(screen.getByLabelText('Rationale'), { target: { value: 'Operator approved advanced visibility.' } });
    fireEvent.click(screen.getByRole('button', { name: 'Approve override' }));

    await waitFor(() => {
      expect(reviewPanelMocks.postCatalogReviewDecision).toHaveBeenCalledWith({
        surface_name: 'canvas',
        target_kind: 'source_policy',
        target_ref: 'capability',
        decision: 'approve',
        rationale: 'Operator approved advanced visibility.',
        candidate_payload: {
          truth: {
            category: 'runtime',
            badge: 'Runs on release',
            detail: 'Capability rows are backed by runtime lanes.',
          },
          surfacePolicy: {
            tier: 'advanced',
            badge: 'Later',
            detail: 'Show capability rows in advanced review surfaces.',
          },
        },
      });
    });

    expect(onCatalogReload).toHaveBeenCalledTimes(1);
  });
});
