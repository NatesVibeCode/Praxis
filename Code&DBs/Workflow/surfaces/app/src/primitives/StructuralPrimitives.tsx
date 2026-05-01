import React, { type CSSProperties, type HTMLAttributes, type Ref, type ReactNode } from 'react';
import { flowNodeProps, ledProps, statusCapProps, type FlowNodeState, type LedTone, type StatusTone } from '../primitives-prx/types';
import { ReceiptCard, TokenChip } from './DisplayPrimitives';

export interface AgentPillProps {
  name: ReactNode;
  role?: ReactNode;
  tone: LedTone;
  className?: string;
  style?: CSSProperties;
}

export function AgentPill({ name, role, tone, className = '', style }: AgentPillProps) {
  const led = ledProps(tone);
  return (
    <span className={className ? `prx-agent ${className}` : 'prx-agent'} style={style} data-testid="prx-agent-pill">
      <span {...led} />
      <span className="name">{name}</span>
      {role !== undefined ? <span className="role">{role}</span> : null}
    </span>
  );
}

export interface StatusRailItem {
  label: ReactNode;
  value: ReactNode;
  tone?: StatusTone;
}

export interface StatusRailProps {
  items: StatusRailItem[];
  className?: string;
  style?: CSSProperties;
}

export function StatusRail({ items, className = '', style }: StatusRailProps) {
  return (
    <div className={className ? `prx-status-rail ${className}` : 'prx-status-rail'} style={style} data-testid="prx-status-rail">
      {items.map((item, index) => (
        <React.Fragment key={index}>
          {index > 0 ? <span className="sep">·</span> : null}
          <span className="item">
            <span className="label">{item.label}</span>
            <span className="v" data-tone={item.tone}>
              {item.value}
            </span>
          </span>
        </React.Fragment>
      ))}
    </div>
  );
}

export type ClaimTrust = 'verified' | 'observed' | 'inferred' | 'claimed' | 'stale' | 'blocked';

export interface ClaimCapsuleRow {
  key: ReactNode;
  value: ReactNode;
}

export interface ClaimCapsuleProps {
  trust: ClaimTrust;
  title: ReactNode;
  rows: ClaimCapsuleRow[];
  className?: string;
  style?: CSSProperties;
}

export function ClaimCapsule({ trust, title, rows, className = '', style }: ClaimCapsuleProps) {
  return (
    <div
      className={className ? `prx-claim-capsule ${className}` : 'prx-claim-capsule'}
      data-trust={trust}
      style={style}
      data-testid="prx-claim-capsule"
    >
      <div className="claim-head">
        <span className="glyph" />
        <span className="trust">{trust}</span>
        <span className="claim">{title}</span>
      </div>
      {rows.map((row, index) => (
        <div className="claim-row" key={index}>
          <span className="k">{row.key}</span>
          <span className="v">{row.value}</span>
        </div>
      ))}
    </div>
  );
}

export interface ClaimGridProps {
  children: ReactNode;
  className?: string;
  style?: CSSProperties;
}

export function ClaimGrid({ children, className = '', style }: ClaimGridProps) {
  return (
    <div className={className ? `prx-claim-grid ${className}` : 'prx-claim-grid'} style={style} data-testid="prx-claim-grid">
      {children}
    </div>
  );
}

export interface StateTripletProps {
  current: ReactNode;
  desired: ReactNode;
  blockedBy: ReactNode;
  next?: ReactNode;
  className?: string;
  style?: CSSProperties;
}

export function StateTriplet({ current, desired, blockedBy, next, className = '', style }: StateTripletProps) {
  return (
    <div className={className ? `prx-state-triplet ${className}` : 'prx-state-triplet'} style={style} data-testid="prx-state-triplet">
      <div className="cell current">
        <span className="k">current</span>
        <span className="v">{current}</span>
      </div>
      <div className="arrow">→</div>
      <div className="cell desired">
        <span className="k">desired</span>
        <span className="v">{desired}</span>
      </div>
      <div className="arrow">→</div>
      <div className="cell blocked">
        <span className="k">blocked by</span>
        <span className="v">{blockedBy}</span>
      </div>
      {next !== undefined ? <div className="next">{next}</div> : null}
    </div>
  );
}

export type FreshnessState = 'fresh' | 'lagging' | 'stale' | 'failed' | 'unknown';

export interface FreshnessStampProps {
  generated: ReactNode;
  sourceMax: ReactNode;
  projectionLag: ReactNode;
  state: FreshnessState;
  className?: string;
  style?: CSSProperties;
}

export function FreshnessStamp({
  generated,
  sourceMax,
  projectionLag,
  state,
  className = '',
  style,
}: FreshnessStampProps) {
  return (
    <div
      className={className ? `prx-freshness-stamp ${className}` : 'prx-freshness-stamp'}
      data-state={state}
      style={style}
      data-testid="prx-freshness-stamp"
    >
      <div className="row">
        <span className="k">generated</span>
        <span className="v">{generated}</span>
      </div>
      <div className="row">
        <span className="k">source max</span>
        <span className="v">{sourceMax}</span>
      </div>
      <div className="row">
        <span className="k">projection lag</span>
        <span className="v">{projectionLag}</span>
      </div>
      <div className="row">
        <span className="k">state</span>
        <span className="state">{state}</span>
      </div>
    </div>
  );
}

export interface ActionPreviewRow {
  key: ReactNode;
  value: ReactNode;
}

export interface ActionPreviewProps {
  operation: ReactNode;
  rows: ActionPreviewRow[];
  onSelect?: () => void;
  footer?: ReactNode;
  className?: string;
  style?: CSSProperties;
}

export function ActionPreview({
  operation,
  rows,
  onSelect,
  footer = 'dry preview · no mutation',
  className = '',
  style,
}: ActionPreviewProps) {
  return (
    <div className={className ? `prx-action-preview ${className}` : 'prx-action-preview'} style={style} data-testid="prx-action-preview">
      <div className="preview-head">
        <span>{operation}</span>
        <span {...statusCapProps('warn')}>preview</span>
      </div>
      {rows.map((row, index) => (
        <div className="row" key={index}>
          <span className="k">{row.key}</span>
          <span className="v">{row.value}</span>
        </div>
      ))}
      <div className="preview-foot">
        <span>{footer}</span>
        {onSelect ? (
          <button type="button" onClick={onSelect}>
            select preview
          </button>
        ) : null}
      </div>
    </div>
  );
}

export interface EmptyStateExplainerProps {
  title: ReactNode;
  why: ReactNode;
  actionLabel: ReactNode;
  actionKeyHint?: ReactNode;
  onAction?: () => void;
  className?: string;
  style?: CSSProperties;
}

export function EmptyStateExplainer({
  title,
  why,
  actionLabel,
  actionKeyHint,
  onAction,
  className = '',
  style,
}: EmptyStateExplainerProps) {
  return (
    <div
      className={className ? `prx-empty-explainer ${className}` : 'prx-empty-explainer'}
      style={style}
      data-testid="prx-empty-explainer"
    >
      <div className="title">{title}</div>
      <div className="why">{why}</div>
      <button type="button" className="move" onClick={onAction}>
        {actionKeyHint !== undefined ? <span className="kbd">{actionKeyHint}</span> : null}
        {actionLabel}
      </button>
    </div>
  );
}

export type AuthorityCrumbKind = 'db' | 'table' | 'row' | 'decision' | 'route' | 'component' | 'operation';
export type AuthorityCrumbState = 'live' | 'held';

export interface AuthorityCrumb {
  kind: AuthorityCrumbKind;
  cap: ReactNode;
  label: ReactNode;
  state?: AuthorityCrumbState;
}

export interface AuthorityBreadcrumbProps {
  items: AuthorityCrumb[];
  className?: string;
  style?: CSSProperties;
}

export function AuthorityBreadcrumb({ items, className = '', style }: AuthorityBreadcrumbProps) {
  return (
    <div
      className={className ? `prx-authority-breadcrumb ${className}` : 'prx-authority-breadcrumb'}
      style={style}
      aria-label="authority path"
      data-testid="prx-authority-breadcrumb"
    >
      {items.map((item, index) => (
        <React.Fragment key={index}>
          {index > 0 ? <span className="join">›</span> : null}
          <span className="crumb" data-kind={item.kind} data-state={item.state}>
            <span className="cap">{item.cap}</span>
            <strong>{item.label}</strong>
          </span>
        </React.Fragment>
      ))}
    </div>
  );
}

export type ScopeRowState = 'read' | 'write' | 'locked' | 'held' | 'denied';

export interface ScopeFenceRow {
  scope: ScopeRowState;
  label: ReactNode;
  target: ReactNode;
  note?: ReactNode;
}

export interface ScopeFenceZone {
  zone: 'inside' | 'outside';
  title: ReactNode;
  rows: ScopeFenceRow[];
}

export interface ScopeFenceProps {
  title: ReactNode;
  tone?: StatusTone;
  toneLabel?: ReactNode;
  zones: ScopeFenceZone[];
  className?: string;
  style?: CSSProperties;
}

export function ScopeFence({
  title,
  tone = 'warn',
  toneLabel = 'bounded',
  zones,
  className = '',
  style,
}: ScopeFenceProps) {
  return (
    <div className={className ? `prx-scope-fence ${className}` : 'prx-scope-fence'} style={style} data-testid="prx-scope-fence">
      <div className="fence-head">
        <span>{title}</span>
        <span {...statusCapProps(tone)}>{toneLabel}</span>
      </div>
      {zones.map((zone, zoneIndex) => (
        <div className="fence-zone" data-zone={zone.zone} key={zoneIndex}>
          <div className="zone-title">{zone.title}</div>
          {zone.rows.map((row, rowIndex) => (
            <div className="scope-row" data-scope={row.scope} key={rowIndex}>
              <span className="glyph" />
              <span>{row.label}</span>
              <strong>{row.target}</strong>
              {row.note !== undefined ? <em>{row.note}</em> : null}
            </div>
          ))}
        </div>
      ))}
    </div>
  );
}

export type EvidenceKind = 'receipt' | 'test' | 'bug' | 'decision' | 'run';

export interface EvidenceItem {
  kind: EvidenceKind;
  title: ReactNode;
  meta: ReactNode;
  body?: ReactNode;
}

export interface EvidenceStackProps {
  items: EvidenceItem[];
  selectedIndex?: number;
  onSelect?: (item: EvidenceItem, index: number) => void;
  className?: string;
  style?: CSSProperties;
}

export function EvidenceStack({
  items,
  selectedIndex = 0,
  onSelect,
  className = '',
  style,
}: EvidenceStackProps) {
  return (
    <div className={className ? `prx-evidence-stack ${className}` : 'prx-evidence-stack'} style={style} data-testid="prx-evidence-stack">
      {items.map((item, index) => (
        <button
          className={`item${index === selectedIndex ? ' selected' : ''}`}
          data-kind={item.kind}
          key={index}
          onClick={() => onSelect?.(item, index)}
          type="button"
        >
          <span className="glyph" />
          <span className="main">{item.title}</span>
          <span className="meta">{item.meta}</span>
        </button>
      ))}
    </div>
  );
}

export interface EvidenceReaderProps {
  cap?: ReactNode;
  title: ReactNode;
  body: ReactNode;
  className?: string;
  style?: CSSProperties;
}

export function EvidenceReader({
  cap = 'selected evidence',
  title,
  body,
  className = '',
  style,
}: EvidenceReaderProps) {
  return (
    <div className={className ? `prx-evidence-reader ${className}` : 'prx-evidence-reader'} style={style} data-testid="prx-evidence-reader">
      <div className="cap">{cap}</div>
      <h3>{title}</h3>
      <p>{body}</p>
    </div>
  );
}

export type VerifierState = 'none' | 'available' | 'running' | 'passed' | 'failed' | 'blocked';

export interface VerifierSlotProps {
  state: VerifierState;
  name: ReactNode;
  tone?: StatusTone;
  label?: ReactNode;
  detail: ReactNode;
  className?: string;
  style?: CSSProperties;
}

export function VerifierSlot({
  state,
  name,
  tone,
  label = state,
  detail,
  className = '',
  style,
}: VerifierSlotProps) {
  const resolvedTone: StatusTone =
    tone ?? (state === 'passed' || state === 'available' ? 'ok' : state === 'failed' ? 'err' : state === 'none' ? 'dim' : 'warn');
  return (
    <div
      className={className ? `prx-verifier-slot ${className}` : 'prx-verifier-slot'}
      data-state={state}
      style={style}
      data-testid="prx-verifier-slot"
    >
      <div className="slot-head">
        <span className="glyph" />
        <strong>{name}</strong>
        <span {...statusCapProps(resolvedTone)}>{label}</span>
      </div>
      <p>{detail}</p>
    </div>
  );
}

export interface VerifierGridProps {
  children: ReactNode;
  className?: string;
  style?: CSSProperties;
}

export function VerifierGrid({ children, className = '', style }: VerifierGridProps) {
  return (
    <div className={className ? `prx-verifier-grid ${className}` : 'prx-verifier-grid'} style={style} data-testid="prx-verifier-grid">
      {children}
    </div>
  );
}

export interface LegalMove {
  action: string;
  glyph: ReactNode;
  label: ReactNode;
  why: ReactNode;
  detail?: ReactNode;
  denied?: boolean;
}

export interface LegalMovesRailProps {
  context: ReactNode;
  items: LegalMove[];
  selectedAction?: string;
  onSelect?: (item: LegalMove) => void;
  className?: string;
  style?: CSSProperties;
}

export function LegalMovesRail({
  context,
  items,
  selectedAction,
  onSelect,
  className = '',
  style,
}: LegalMovesRailProps) {
  return (
    <div className={className ? `prx-legal-rail ${className}` : 'prx-legal-rail'} style={style} data-testid="prx-legal-rail">
      <div className="rail-head">
        <span>selected</span>
        <strong>{context}</strong>
      </div>
      {items.map((item) => {
        const selected = item.action === selectedAction;
        const classes = ['move'];
        if (selected) classes.push('selected');
        if (item.denied) classes.push('denied');
        return (
          <button
            className={classes.join(' ')}
            data-action={item.action}
            key={item.action}
            onClick={() => onSelect?.(item)}
            type="button"
          >
            <span className="glyph">{item.glyph}</span>
            <span className="label">{item.label}</span>
            <span className="why">{item.why}</span>
          </button>
        );
      })}
    </div>
  );
}

export interface LegalReaderProps {
  title: ReactNode;
  body: ReactNode;
  hint?: ReactNode;
  className?: string;
  style?: CSSProperties;
}

export function LegalReader({
  title,
  body,
  hint,
  className = '',
  style,
}: LegalReaderProps) {
  return (
    <div className={className ? `prx-legal-reader ${className}` : 'prx-legal-reader'} style={style} data-testid="prx-legal-reader">
      <div className="cap">selected move</div>
      <h3>{title}</h3>
      <p>{body}</p>
      {hint !== undefined ? <div className="hint">{hint}</div> : null}
    </div>
  );
}

export interface WorkflowBarProps {
  name: ReactNode;
  meta: ReactNode;
  completed: number;
  total: number;
  currentIndex?: number;
  percentLabel?: ReactNode;
  controls?: ReactNode;
  className?: string;
  style?: CSSProperties;
}

export function WorkflowBar({
  name,
  meta,
  completed,
  total,
  currentIndex,
  percentLabel,
  controls,
  className = '',
  style,
}: WorkflowBarProps) {
  const clamped = Math.max(0, Math.min(completed, total));
  const current = currentIndex ?? (clamped < total ? clamped : -1);
  const pct = total > 0 ? `${Math.round((clamped / total) * 100)}%` : '0%';
  return (
    <div className={className ? `prx-workflow-bar ${className}` : 'prx-workflow-bar'} style={style} data-testid="prx-workflow-bar">
      <div className="id">
        <span className="name">{name}</span>
        <span className="meta">{meta}</span>
      </div>
      <div className="progress">
        <span>{clamped} / {total}</span>
        <span className="ticks">
          {Array.from({ length: total }, (_, index) => {
            let tickClass = 't';
            if (index < clamped) tickClass += ' on';
            if (index === current) tickClass += ' cur';
            return <span className={tickClass} key={index} />;
          })}
        </span>
        <span className="pct">{percentLabel ?? pct}</span>
      </div>
      {controls !== undefined ? <div className="controls">{controls}</div> : null}
    </div>
  );
}

export interface FlowNodeCardProps {
  state: FlowNodeState;
  glyph: ReactNode;
  name: ReactNode;
  ledTone?: LedTone;
  summary: ReactNode;
  footerLeft?: ReactNode;
  footerRight?: ReactNode;
  style?: CSSProperties;
  className?: string;
}

export function FlowNodeCard({
  state,
  glyph,
  name,
  ledTone,
  summary,
  footerLeft,
  footerRight,
  style,
  className = '',
}: FlowNodeCardProps) {
  const attrs = flowNodeProps(state);
  return (
    <div
      {...attrs}
      className={className ? `${attrs.className} ${className}` : attrs.className}
      style={style}
      data-testid="prx-flow-node"
    >
      <div className="hd">
        <span className="glyph">{glyph}</span>
        <span className="name">{name}</span>
        {ledTone ? <span {...ledProps(ledTone)} /> : null}
      </div>
      <div className="body">{summary}</div>
      {(footerLeft !== undefined || footerRight !== undefined) ? (
        <div className="ft">
          <span>{footerLeft}</span>
          <span className="dur">{footerRight}</span>
        </div>
      ) : null}
    </div>
  );
}

export interface LinearFlowProps {
  nodes: Array<FlowNodeCardProps & { edgeLabelAfter?: ReactNode }>;
  className?: string;
  style?: CSSProperties;
}

export function LinearFlow({ nodes, className = '', style }: LinearFlowProps) {
  return (
    <div className={className ? `prx-flow ${className}` : 'prx-flow'} data-layout="linear" style={style} data-testid="prx-linear-flow">
      {nodes.map((node, index) => (
        <React.Fragment key={index}>
          <FlowNodeCard {...node} />
          {index < nodes.length - 1 ? <span className="prx-flow-edge glyph">{node.edgeLabelAfter}</span> : null}
        </React.Fragment>
      ))}
    </div>
  );
}

export interface DagEdge {
  path: string;
  label?: ReactNode;
  labelX?: number;
  labelY?: number;
  tone?: 'live' | 'warn' | 'err';
  halo?: boolean;
}

export interface DagNode extends FlowNodeCardProps {
  left: number;
  top: number;
  minWidth?: number;
}

export interface DagFlowProps {
  width?: number;
  height: number;
  edges: DagEdge[];
  nodes: DagNode[];
  className?: string;
  style?: CSSProperties;
}

export function DagFlow({
  width = 760,
  height,
  edges,
  nodes,
  className = '',
  style,
}: DagFlowProps) {
  return (
    <div
      className={className ? `prx-flow ${className}` : 'prx-flow'}
      data-layout="dag"
      style={{ minHeight: height, ...style }}
      data-testid="prx-dag-flow"
    >
      <svg className="canvas-svg" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg">
        {edges.map((edge, index) => (
          <React.Fragment key={index}>
            <path d={edge.path} className={`${edge.tone ?? 'live'}${edge.halo ? ' halo' : ''}`} />
            {edge.label !== undefined && edge.labelX !== undefined && edge.labelY !== undefined ? (
              <text x={edge.labelX} y={edge.labelY} className="edge-label">
                {edge.label}
              </text>
            ) : null}
          </React.Fragment>
        ))}
      </svg>
      {nodes.map((node, index) => (
        <FlowNodeCard
          {...node}
          key={index}
          style={{ left: node.left, top: node.top, minWidth: node.minWidth, position: 'absolute' }}
        />
      ))}
    </div>
  );
}

export interface ReceiptDiffProps {
  left: React.ComponentProps<typeof ReceiptCard>;
  right: React.ComponentProps<typeof ReceiptCard>;
  delta?: ReactNode;
  deltaState?: 'same' | 'diff';
  className?: string;
  style?: CSSProperties;
}

export function ReceiptDiff({
  left,
  right,
  delta,
  deltaState,
  className = '',
  style,
}: ReceiptDiffProps) {
  return (
    <div className={className ? `prx-receipt-diff ${className}` : 'prx-receipt-diff'} style={style} data-testid="prx-receipt-diff">
      <div className="col-a">
        <ReceiptCard {...left} />
      </div>
      <div className="arrow">⇄</div>
      <div className="col-b">
        <ReceiptCard {...right} />
      </div>
      {delta !== undefined ? <div className="delta" data-state={deltaState}>{delta}</div> : null}
    </div>
  );
}

export interface NodeBandProps {
  receives: ReactNode[];
  icon: ReactNode;
  name: ReactNode;
  kind: ReactNode;
  produces: ReactNode[];
  className?: string;
  style?: CSSProperties;
}

export function NodeBand({
  receives,
  icon,
  name,
  kind,
  produces,
  className = '',
  style,
}: NodeBandProps) {
  return (
    <div className={className ? `prx-node-band ${className}` : 'prx-node-band'} style={style} data-testid="prx-node-band">
      <div className="receives">
        <span className="label">receives</span>
        {receives.map((item, index) => (
          <React.Fragment key={index}>{item}</React.Fragment>
        ))}
      </div>
      <div className="nucleus">
        {icon}
        <span className="name">{name}</span>
        <span className="kind">{kind}</span>
      </div>
      <div className="produces">
        <span className="label">produces</span>
        {produces.map((item, index) => (
          <React.Fragment key={index}>{item}</React.Fragment>
        ))}
      </div>
    </div>
  );
}

export interface TimelineBlock {
  tone: StatusTone;
  leftPct: number;
  widthPct: number;
  label: ReactNode;
}

export interface TimelineRow {
  actor: ReactNode;
  tone: LedTone;
  blocks: TimelineBlock[];
}

export interface TimelineProps {
  ticks: ReactNode[];
  rows: TimelineRow[];
  className?: string;
  style?: CSSProperties;
}

export function Timeline({ ticks, rows, className = '', style }: TimelineProps) {
  return (
    <div className={className ? `prx-timeline ${className}` : 'prx-timeline'} style={style} data-testid="prx-timeline">
      <div className="prx-timeline-head">
        <span className="axis-label">time</span>
        <span className="ticks">
          {ticks.map((tick, index) => (
            <span key={index}>{tick}</span>
          ))}
        </span>
      </div>
      {rows.map((row, index) => (
        <div className="prx-timeline-row" key={index}>
          <div className="actor">
            <span {...ledProps(row.tone)} />
            {row.actor}
          </div>
          <div className="lane">
            {row.blocks.map((block, blockIndex) => (
              <div
                className="prx-timeline-block"
                data-tone={block.tone}
                key={blockIndex}
                style={{ left: `${block.leftPct}%`, width: `${block.widthPct}%` }}
              >
                {block.label}
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

export type TableColumnKind = 'text' | 'mono' | 'bool' | 'stat' | 'chip';

export interface TableColumn<Row extends object> {
  key: keyof Row & string;
  label: ReactNode;
  kind?: TableColumnKind;
  sortable?: boolean;
  sortDirection?: 'asc' | 'desc' | null;
  onSort?: () => void;
  render?: (row: Row, index: number) => ReactNode;
  cellStyle?: CSSProperties;
}

export interface TableStatusValue {
  label: ReactNode;
  tone: StatusTone;
}

export type PrxTableRowProps = HTMLAttributes<HTMLTableRowElement> & {
  [key: `data-${string}`]: string | number | boolean | undefined;
};

export interface PrxTableProps<Row extends object> {
  columns: TableColumn<Row>[];
  rows: Row[];
  toolbar?: ReactNode;
  filters?: ReactNode;
  meta?: ReactNode;
  emptyState?: ReactNode;
  bodyRef?: Ref<HTMLDivElement>;
  rowKey?: (row: Row, index: number) => string | number;
  selectedRowKey?: string | number | null;
  onRowClick?: (row: Row, index: number) => void;
  getRowProps?: (row: Row, index: number) => PrxTableRowProps;
  className?: string;
  style?: CSSProperties;
}

export function PrxTable<Row extends object>({
  columns,
  rows,
  toolbar,
  filters,
  meta,
  emptyState,
  bodyRef,
  rowKey,
  selectedRowKey,
  onRowClick,
  getRowProps,
  className = '',
  style,
}: PrxTableProps<Row>) {
  return (
    <div className={className ? `prx-table ${className}` : 'prx-table'} style={style} data-testid="prx-table">
      {(toolbar !== undefined || filters !== undefined || meta !== undefined) ? (
        <div className="prx-table-bar">
          {toolbar}
          <div className="prx-table-chips">{filters}</div>
          <span className="prx-table-meta">{meta}</span>
        </div>
      ) : null}
      <div className="body" ref={bodyRef}>
        {rows.length === 0 ? (
          <div className="empty">{emptyState ?? 'no rows'}</div>
        ) : (
          <table>
            <thead>
              <tr>
                {columns.map((column) => {
                  const sortClass = column.sortDirection ? `sort-${column.sortDirection}` : '';
                  return (
                    <th
                      className={sortClass}
                      data-key={column.key}
                      key={column.key}
                      onClick={column.sortable === false ? undefined : column.onSort}
                      style={{ cursor: column.onSort && column.sortable !== false ? 'pointer' : undefined }}
                    >
                      {column.label}
                      {column.onSort ? <span className="arrow" /> : null}
                    </th>
                  );
                })}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, rowIndex) => {
                const key = rowKey?.(row, rowIndex) ?? rowIndex;
                const rowProps = getRowProps?.(row, rowIndex) ?? {};
                const selectedClass = selectedRowKey !== undefined && selectedRowKey !== null && key === selectedRowKey
                  ? 'selected'
                  : '';
                const mergedClassName = [rowProps.className, selectedClass].filter(Boolean).join(' ') || undefined;
                return (
                  <tr
                    {...rowProps}
                    className={mergedClassName}
                    key={key}
                    onClick={() => onRowClick?.(row, rowIndex)}
                  >
                    {columns.map((column) => {
                      const value = row[column.key];
                      return (
                        <td key={column.key} style={column.cellStyle}>
                          {column.render ? column.render(row, rowIndex) : renderTableValue(column.kind, value)}
                        </td>
                      );
                    })}
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

function renderTableValue(kind: TableColumnKind | undefined, value: unknown) {
  if (kind === 'stat' && isTableStatusValue(value)) {
    return <span {...statusCapProps(value.tone)}>{value.label}</span>;
  }
  if (kind === 'chip') {
    return <TokenChip>{String(value ?? '')}</TokenChip>;
  }
  if (kind === 'bool') {
    return String(Boolean(value));
  }
  return String(value ?? '');
}

function isTableStatusValue(value: unknown): value is TableStatusValue {
  return typeof value === 'object' && value !== null && 'label' in value && 'tone' in value;
}

export interface WizardStep {
  label: ReactNode;
  state?: 'active' | 'done';
}

export interface WizardScaffoldProps {
  steps: WizardStep[];
  form: ReactNode;
  preview: ReactNode;
  footer?: ReactNode;
  className?: string;
  style?: CSSProperties;
}

export function WizardScaffold({
  steps,
  form,
  preview,
  footer,
  className = '',
  style,
}: WizardScaffoldProps) {
  return (
    <div className={className ? `prx-wizard ${className}` : 'prx-wizard'} style={style} data-testid="prx-wizard">
      <div className="prx-wizard-steps">
        {steps.map((step, index) => (
          <React.Fragment key={index}>
            {index > 0 ? <span className="sep">···</span> : null}
            <span className={`prx-wizard-step${step.state ? ` ${step.state}` : ''}`}>
              <span className="num"><span>{index + 1}</span></span>
              <span>{step.label}</span>
            </span>
          </React.Fragment>
        ))}
      </div>
      <div className="prx-wizard-body">
        <div className="prx-wizard-form">{form}</div>
        <div className="prx-wizard-preview">{preview}</div>
      </div>
      {footer !== undefined ? <div className="prx-wizard-foot">{footer}</div> : null}
    </div>
  );
}
