import '@testing-library/jest-dom';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';

import {
  ActionPreview,
  AgentPill,
  AuthorityBreadcrumb,
  ClaimCapsule,
  ClaimGrid,
  DagFlow,
  EmptyStateExplainer,
  EventChain,
  EvidenceReader,
  EvidenceStack,
  FreshnessStamp,
  LegalMovesRail,
  LegalReader,
  LinearFlow,
  NodeBand,
  PrxTable,
  ReceiptDiff,
  ScopeFence,
  StateTriplet,
  StatusRail,
  Timeline,
  VerifierGrid,
  VerifierSlot,
  WizardScaffold,
  WorkflowBar,
} from '../StructuralPrimitives';

describe('structural primitives render canonical prx-* shells', () => {
  test('AgentPill renders agent shell', () => {
    render(<AgentPill name="refund.agent" role="opus" tone="live" />);
    expect(screen.getByTestId('prx-agent-pill')).toHaveClass('prx-agent');
    expect(screen.getByTestId('prx-agent-pill').querySelector('.prx-led')).toHaveAttribute('data-tone', 'live');
  });

  test('StatusRail and Claim capsule render expected classes', () => {
    render(
      <div>
        <StatusRail items={[{ label: 'run', value: 'r_8af3' }, { label: 'latency', value: '142ms', tone: 'ok' }]} />
        <ClaimGrid>
          <ClaimCapsule trust="verified" title="Atlas graph fresh" rows={[{ key: 'proof', value: 'receipt' }]} />
        </ClaimGrid>
      </div>,
    );
    expect(screen.getByTestId('prx-status-rail')).toHaveClass('prx-status-rail');
    expect(screen.getByTestId('prx-claim-grid')).toHaveClass('prx-claim-grid');
    expect(screen.getByTestId('prx-claim-capsule')).toHaveAttribute('data-trust', 'verified');
  });

  test('StateTriplet and FreshnessStamp render their states', () => {
    render(
      <div>
        <StateTriplet current="FIX_PENDING_VERIFICATION" desired="FIXED" blockedBy="no verifier" next="register one" />
        <FreshnessStamp generated="12:04" sourceMax="12:03" projectionLag="41s" state="fresh" />
      </div>,
    );
    expect(screen.getByTestId('prx-state-triplet')).toHaveClass('prx-state-triplet');
    expect(screen.getByTestId('prx-freshness-stamp')).toHaveAttribute('data-state', 'fresh');
  });

  test('ActionPreview and EmptyStateExplainer render controls', () => {
    render(
      <div>
        <ActionPreview operation="workflow.run" rows={[{ key: 'will read', value: 'catalog' }]} />
        <EmptyStateExplainer title="No verifier attached" why="because this bug has no verifier_ref" actionLabel="register verifier" actionKeyHint="V" />
      </div>,
    );
    expect(screen.getByTestId('prx-action-preview')).toHaveClass('prx-action-preview');
    expect(screen.getByTestId('prx-empty-explainer')).toHaveClass('prx-empty-explainer');
  });

  test('AuthorityBreadcrumb and ScopeFence render authority path and boundary rows', () => {
    render(
      <div>
        <AuthorityBreadcrumb
          items={[
            { kind: 'db', cap: 'db', label: 'Praxis.db' },
            { kind: 'table', cap: 'table', label: 'operation_catalog_registry' },
            { kind: 'row', cap: 'operation', label: 'structured_documents.context_assemble', state: 'live' },
          ]}
        />
        <ScopeFence
          title="atlas.redesign.packet"
          zones={[
            {
              zone: 'inside',
              title: 'inside declared scope',
              rows: [{ scope: 'read', label: 'read', target: 'AtlasPage.tsx', note: 'semantic model' }],
            },
            {
              zone: 'outside',
              title: 'outside fence',
              rows: [{ scope: 'denied', label: 'deny', target: 'routeRegistry.ts', note: 'same component_ref' }],
            },
          ]}
        />
      </div>,
    );
    expect(screen.getByTestId('prx-authority-breadcrumb')).toHaveClass('prx-authority-breadcrumb');
    expect(screen.getByTestId('prx-scope-fence')).toHaveClass('prx-scope-fence');
    expect(screen.getByTestId('prx-scope-fence').querySelector('.scope-row')).toHaveAttribute('data-scope', 'read');
  });

  test('EvidenceStack, EvidenceReader, VerifierGrid, and VerifierSlot render proof structures', () => {
    render(
      <div>
        <EvidenceStack items={[{ kind: 'receipt', title: 'receipt r_8af3', meta: 'sealed' }]} />
        <EvidenceReader title="receipt r_8af3" body="gateway dispatch completed" />
        <VerifierGrid>
          <VerifierSlot state="passed" name="validates_fix" detail="Bug may move to FIXED." />
        </VerifierGrid>
      </div>,
    );
    expect(screen.getByTestId('prx-evidence-stack')).toHaveClass('prx-evidence-stack');
    expect(screen.getByTestId('prx-evidence-reader')).toHaveClass('prx-evidence-reader');
    expect(screen.getByTestId('prx-verifier-grid')).toHaveClass('prx-verifier-grid');
    expect(screen.getByTestId('prx-verifier-slot')).toHaveAttribute('data-state', 'passed');
  });

  test('EventChain renders authority event threads', () => {
    render(
      <EventChain
        items={[
          { label: 'plan.composed', value: '12:04:18', what: 'handler compose_plan_from_intent' },
          { label: 'typed_gap.created', value: '12:04:19', what: 'verifier missing', tone: 'warn' },
        ]}
      />,
    );
    expect(screen.getByTestId('prx-event-chain')).toHaveClass('prx-chain');
    expect(screen.getByText('typed_gap.created').closest('.ev')).toHaveAttribute('data-tone', 'warn');
  });

  test('LegalMovesRail and LegalReader render legal move surfaces', () => {
    render(
      <div>
        <LegalMovesRail
          context="atlas.pad.authority"
          selectedAction="inspect"
          items={[
            { action: 'inspect', glyph: '⌕', label: 'inspect', why: 'read area evidence' },
            { action: 'edit_api', glyph: '▨', label: 'edit API', why: 'out of scope', denied: true },
          ]}
        />
        <LegalReader title="inspect" body="Open the inline Ledger for this area." hint={<>event · <span>prx:legal-move</span></>} />
      </div>,
    );
    expect(screen.getByTestId('prx-legal-rail')).toHaveClass('prx-legal-rail');
    expect(screen.getByTestId('prx-legal-reader')).toHaveClass('prx-legal-reader');
    expect(screen.getByTestId('prx-legal-rail').querySelector('.move.selected')).toBeInTheDocument();
  });

  test('WorkflowBar, LinearFlow, and DagFlow render workflow structures', () => {
    render(
      <div>
        <WorkflowBar name="refund_batch_2026_04_30" meta="run_8af3" completed={4} total={7} />
        <LinearFlow
          nodes={[
            { state: 'ok', glyph: '›', name: 'compose_plan', ledTone: 'ok', summary: 'intent.text -> plan.composed', footerLeft: 'ok', footerRight: '142ms', edgeLabelAfter: 'plan.composed' },
            { state: 'cur', glyph: '›', name: 'verifier.run', ledTone: 'live', summary: 'workflow.run -> verification' },
          ]}
        />
        <DagFlow
          height={240}
          edges={[{ path: 'M 10 10 C 40 10, 40 40, 70 40', label: 'intent.text', labelX: 20, labelY: 8 }]}
          nodes={[
            { state: 'ok', glyph: '›', name: 'compose_plan', ledTone: 'ok', summary: 'command', left: 32, top: 24 },
            { state: 'placeholder', glyph: '·', name: 'step 3', summary: 'unplaced', left: 180, top: 24 },
          ]}
        />
      </div>,
    );
    expect(screen.getByTestId('prx-workflow-bar')).toHaveClass('prx-workflow-bar');
    expect(screen.getByTestId('prx-linear-flow')).toHaveAttribute('data-layout', 'linear');
    expect(screen.getByTestId('prx-dag-flow')).toHaveAttribute('data-layout', 'dag');
  });

  test('ReceiptDiff, NodeBand, and Timeline render inspection surfaces', () => {
    render(
      <div>
        <ReceiptDiff
          left={{ state: 'ok', title: 'receipt A', fields: [{ key: 'duration', value: '142ms' }] }}
          right={{ state: 'sealed', title: 'receipt B', fields: [{ key: 'duration', value: '198ms' }] }}
          delta="diverged"
          deltaState="diff"
        />
        <NodeBand
          receives={[<span key="a">payload</span>]}
          icon={<span>›</span>}
          name="Run Workflow"
          kind="command"
          produces={[<span key="b">receipt</span>]}
        />
        <Timeline
          ticks={['0', '10', '20']}
          rows={[{ actor: 'refund.agent', tone: 'live', blocks: [{ tone: 'ok', leftPct: 10, widthPct: 20, label: 'compose' }] }]}
        />
      </div>,
    );
    expect(screen.getByTestId('prx-receipt-diff')).toHaveClass('prx-receipt-diff');
    expect(screen.getByTestId('prx-node-band')).toHaveClass('prx-node-band');
    expect(screen.getByTestId('prx-timeline')).toHaveClass('prx-timeline');
  });

  test('PrxTable and WizardScaffold render catalog and wizard shells', () => {
    const onSort = vi.fn();
    const onRowClick = vi.fn();
    render(
      <div>
        <PrxTable
          columns={[
            { key: 'name', label: 'name', sortDirection: 'asc', onSort },
            { key: 'status', label: 'status', kind: 'stat' },
          ]}
          rows={[
            { id: 'search', name: 'praxis_search', status: { label: 'ok', tone: 'ok' as const } },
          ]}
          rowKey={(row) => row.id}
          selectedRowKey="search"
          onRowClick={onRowClick}
          getRowProps={(row) => ({ 'data-id': row.id })}
          filters={<span>kind=search</span>}
          meta="1 row"
        />
        <WizardScaffold
          steps={[
            { label: 'identity', state: 'done' },
            { label: 'review', state: 'active' },
          ]}
          form={<div>form</div>}
          preview={<pre>preview</pre>}
          footer={<button type="button">submit</button>}
        />
      </div>,
    );
    expect(screen.getByTestId('prx-table')).toHaveClass('prx-table');
    expect(screen.getByText('name')).toHaveClass('sort-asc');
    expect(screen.getByText('praxis_search').closest('tr')).toHaveClass('selected');
    expect(screen.getByText('praxis_search').closest('tr')).toHaveAttribute('data-id', 'search');
    fireEvent.click(screen.getByText('name'));
    expect(onSort).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByText('praxis_search'));
    expect(onRowClick).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId('prx-wizard')).toHaveClass('prx-wizard');
  });
});
