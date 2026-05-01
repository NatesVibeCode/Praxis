import '@testing-library/jest-dom';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';

import {
  Bargraph,
  Button,
  DiffBlock,
  GateBadge,
  Gauge,
  KbdCluster,
  LedDot,
  ListPanel,
  ManifestTree,
  MetricTile,
  PanelCard,
  RadioPillGroup,
  ReceiptCard,
  Runlog,
  SectionStrip,
  SourceChip,
  Sparkline,
  StatusRow,
  TableFilterInput,
  TokenChip,
} from '../DisplayPrimitives';

describe('display primitives render canonical prx-* shapes', () => {
  test('SectionStrip renders prx-section-strip', () => {
    render(<SectionStrip number={1} label="contract" />);
    expect(screen.getByTestId('prx-section-strip')).toHaveClass('prx-section-strip');
  });

  test('TokenChip renders prx-chip data attrs', () => {
    render(<TokenChip tone="locked" source="redacted">customer.email</TokenChip>);
    const chip = screen.getByTestId('prx-token-chip');
    expect(chip).toHaveClass('prx-chip');
    expect(chip).toHaveAttribute('data-tone', 'locked');
    expect(chip).toHaveAttribute('data-source', 'redacted');
  });

  test('GateBadge renders prx-gate state', () => {
    render(<GateBadge state="approved">approved</GateBadge>);
    const gate = screen.getByTestId('prx-gate-badge');
    expect(gate).toHaveClass('prx-gate');
    expect(gate).toHaveAttribute('data-state', 'approved');
    expect(gate.querySelector('.glyph')).toBeInTheDocument();
  });

  test('ManifestTree renders rows', () => {
    render(<ManifestTree rows={[{ glyph: '├─', label: 'read_scope', meta: 'read' }]} />);
    const tree = screen.getByTestId('prx-manifest-tree');
    expect(tree).toHaveClass('prx-tree');
    expect(tree.querySelector('.row .glyph')).toBeInTheDocument();
    expect(tree.querySelector('.row .meta')).toBeInTheDocument();
  });

  test('RadioPillGroup renders selectable radio pills', () => {
    const onChange = vi.fn();
    render(
      <RadioPillGroup
        ariaLabel="Atlas view"
        value="map"
        onChange={onChange}
        options={[
          { value: 'map', label: 'map' },
          { value: 'contact', label: 'contact' },
        ]}
      />,
    );
    expect(screen.getByTestId('prx-radio-pill-group')).toHaveClass('prx-radio-group');
    expect(screen.getByText('map')).toHaveAttribute('aria-checked', 'true');
    fireEvent.click(screen.getByText('contact'));
    expect(onChange).toHaveBeenCalledWith('contact', { value: 'contact', label: 'contact' });
  });

  test('TableFilterInput renders the canonical table filter shell', () => {
    render(<TableFilterInput placeholder="filter" value="" onChange={() => {}} />);
    const input = screen.getByTestId('prx-table-filter');
    expect(input).toHaveClass('prx-table-filter');
    expect(input).toHaveAttribute('autocomplete', 'off');
    expect(input).toHaveAttribute('spellcheck', 'false');
  });

  test('Runlog renders prx-runlog rows', () => {
    render(<Runlog rows={[{ ts: '12:04', actor: 'agent', what: 'read ledger', status: 'ok', tone: 'ok' }]} />);
    const runlog = screen.getByTestId('prx-runlog');
    expect(runlog).toHaveClass('prx-runlog');
    expect(runlog.querySelector('.row .stat')).toHaveAttribute('data-tone', 'ok');
  });

  test('DiffBlock renders prx-diff lines', () => {
    render(<DiffBlock lines={[{ mark: '+', text: 'verifier: pass' }]} />);
    const diff = screen.getByTestId('prx-diff-block');
    expect(diff).toHaveClass('prx-diff');
    expect(diff.querySelector('.line')).toHaveAttribute('data-mark', '+');
  });

  test('KbdCluster renders prx-kbd-cluster', () => {
    render(<KbdCluster keys={['cmd', 'K']} />);
    const cluster = screen.getByTestId('prx-kbd-cluster');
    expect(cluster).toHaveClass('prx-kbd-cluster');
    expect(cluster.querySelectorAll('.prx-kbd')).toHaveLength(2);
  });

  test('Gauge renders active ticks', () => {
    render(<Gauge filled={4} total={6} label="0.66 confidence" tone="warn" />);
    const gauge = screen.getByTestId('prx-gauge');
    expect(gauge).toHaveClass('prx-gauge');
    expect(gauge).toHaveAttribute('data-tone', 'warn');
    expect(gauge.querySelectorAll('.ticks .t.on')).toHaveLength(4);
  });

  test('ReceiptCard renders prx-receipt fields', () => {
    render(
      <ReceiptCard
        state="sealed"
        title="receipt r_8af3"
        fields={[{ key: 'runtime', value: '4.3s' }]}
        hash="sha256:abc"
        seal="sealed"
      />,
    );
    const receipt = screen.getByTestId('prx-receipt-card');
    expect(receipt).toHaveClass('prx-receipt');
    expect(receipt).toHaveAttribute('data-state', 'sealed');
    expect(receipt.querySelector('.ft .hash')).toBeInTheDocument();
  });

  test('Sparkline, Bargraph, and LedDot render canonical classes', () => {
    render(
      <div>
        <Sparkline values={[1, 4, 2, 5]} />
        <Bargraph bars="▁▃▅█" label="history" value="4 runs" tone="bad" />
        <LedDot tone="live" />
      </div>,
    );
    expect(screen.getByTestId('prx-sparkline')).toHaveClass('prx-spark');
    expect(screen.getByTestId('prx-bargraph')).toHaveClass('prx-bargraph');
    expect(screen.getByTestId('prx-led-dot')).toHaveClass('prx-led');
  });

  test('Button renders prx-button with tone/size attrs and forwards onClick', () => {
    const handler = jest.fn();
    render(
      <Button tone="primary" size="lg" active onClick={handler}>
        Compose workflow
      </Button>,
    );
    const btn = screen.getByTestId('prx-button');
    expect(btn).toHaveClass('prx-button');
    expect(btn).toHaveAttribute('data-tone', 'primary');
    expect(btn).toHaveAttribute('data-size', 'lg');
    expect(btn).toHaveAttribute('data-active', 'true');
    expect(btn).toHaveAttribute('type', 'button');
    fireEvent.click(btn);
    expect(handler).toHaveBeenCalledTimes(1);
  });

  test('Button without tone/size omits the data attrs', () => {
    render(<Button>plain</Button>);
    const btn = screen.getByTestId('prx-button');
    expect(btn).not.toHaveAttribute('data-tone');
    expect(btn).not.toHaveAttribute('data-size');
    expect(btn).not.toHaveAttribute('data-active');
  });

  test('PanelCard renders eyebrow / title / count / action / footer slots', () => {
    render(
      <PanelCard
        eyebrow="Materialize"
        title="Toolbelt Review"
        count={5}
        action={<Button size="sm">Add</Button>}
        footer={<span data-testid="frame-foot">view all</span>}
        tone="warn"
      >
        <span data-testid="frame-body">body</span>
      </PanelCard>,
    );
    const card = screen.getByTestId('prx-panel-card');
    expect(card).toHaveClass('prx-card');
    expect(card).toHaveAttribute('data-tone', 'warn');
    expect(card.querySelector('.prx-card__head .eyebrow')).toHaveTextContent('Materialize');
    expect(card.querySelector('.prx-card__head .title')).toHaveTextContent('Toolbelt Review');
    expect(card.querySelector('.prx-card__count')).toHaveTextContent('5');
    expect(card.querySelector('.prx-card__head-tail [data-testid="prx-button"]')).toBeInTheDocument();
    expect(screen.getByTestId('frame-body')).toBeInTheDocument();
    expect(screen.getByTestId('frame-foot')).toBeInTheDocument();
  });

  test('PanelCard hides head when no head props provided', () => {
    render(<PanelCard><span>just body</span></PanelCard>);
    const card = screen.getByTestId('prx-panel-card');
    expect(card.querySelector('.prx-card__head')).not.toBeInTheDocument();
  });

  test('MetricTile renders prx-tile with label/value/detail/action and tone', () => {
    const handler = jest.fn();
    render(
      <MetricTile
        label="Workflow Inventory"
        value="99 workflows"
        detail="1 live · 2 saved · 96 draft"
        action="Open builder →"
        tone="ok"
        onClick={handler}
      />,
    );
    const tile = screen.getByTestId('prx-tile');
    expect(tile).toHaveClass('prx-tile');
    expect(tile).toHaveAttribute('data-tone', 'ok');
    expect(tile.querySelector('.prx-tile__label')).toHaveTextContent('Workflow Inventory');
    expect(tile.querySelector('.prx-tile__value')).toHaveTextContent('99 workflows');
    expect(tile.querySelector('.prx-tile__detail')).toHaveTextContent('1 live · 2 saved · 96 draft');
    expect(tile.querySelector('.prx-tile__action')).toHaveTextContent('Open builder');
    fireEvent.click(tile);
    expect(handler).toHaveBeenCalledTimes(1);
  });

  test('ListPanel renders prx-list-panel with eyebrow / title / count / action / body — NO border', () => {
    render(
      <ListPanel
        eyebrow="Materialize"
        title="Toolbelt Review"
        count={5}
        action={<Button size="sm" tone="ghost">Add</Button>}
      >
        <span data-testid="lp-body">rows go here</span>
      </ListPanel>,
    );
    const panel = screen.getByTestId('prx-list-panel');
    expect(panel).toHaveClass('prx-list-panel');
    expect(panel.querySelector('.prx-list-panel__eyebrow')).toHaveTextContent('Materialize');
    expect(panel.querySelector('.prx-list-panel__title')).toHaveTextContent('Toolbelt Review');
    expect(panel.querySelector('.prx-list-panel__count')).toHaveTextContent('5');
    expect(panel.querySelector('.prx-list-panel__head-tail [data-testid="prx-button"]')).toBeInTheDocument();
    expect(screen.getByTestId('lp-body')).toBeInTheDocument();
  });

  test('StatusRow renders LedDot + title + detail + meta and is clickable when onClick provided', () => {
    const handler = jest.fn();
    render(
      <StatusRow
        tone="err"
        title="Workflow.Run.abc"
        detail="failed - no job receipts yet"
        meta="1h ago"
        onClick={handler}
      />,
    );
    const row = screen.getByTestId('prx-status-row');
    expect(row).toHaveClass('prx-status-row');
    expect(row.querySelector('[data-testid="prx-led-dot"]')).toHaveAttribute('data-tone', 'err');
    expect(row.querySelector('.prx-status-row__title')).toHaveTextContent('Workflow.Run.abc');
    expect(row.querySelector('.prx-status-row__detail')).toHaveTextContent('failed');
    expect(row.querySelector('.prx-status-row__meta')).toHaveTextContent('1h ago');
    fireEvent.click(row);
    expect(handler).toHaveBeenCalledTimes(1);
  });

  test('StatusRow renders as static div when no onClick', () => {
    render(<StatusRow tone="idle" title="static row" />);
    const row = screen.getByTestId('prx-status-row');
    expect(row.tagName).toBe('DIV');
    expect(row).toHaveClass('prx-status-row--static');
  });

  test('SourceChip renders prx-source-chip with tone/active attrs and fires onClick', () => {
    const handler = jest.fn();
    render(
      <SourceChip
        tone="ok"
        active
        label="workspace_records"
        subtitle="connected · 24 rows"
        onClick={handler}
      />,
    );
    const chip = screen.getByTestId('prx-source-chip');
    expect(chip).toHaveClass('prx-source-chip');
    expect(chip).toHaveAttribute('data-tone', 'ok');
    expect(chip).toHaveAttribute('data-active', 'true');
    expect(chip.querySelector('.prx-source-chip__label')).toHaveTextContent('workspace_records');
    expect(chip.querySelector('.prx-source-chip__sub')).toHaveTextContent('connected · 24 rows');
    expect(chip.querySelector('.prx-source-chip__dot')).toBeInTheDocument();
    fireEvent.click(chip);
    expect(handler).toHaveBeenCalledTimes(1);
  });
});
