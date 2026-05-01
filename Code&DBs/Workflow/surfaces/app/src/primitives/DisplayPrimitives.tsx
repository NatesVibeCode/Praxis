import React, { type AriaRole, type ButtonHTMLAttributes, type CSSProperties, type HTMLAttributes, type InputHTMLAttributes, type ReactNode } from 'react';
import {
  chipProps,
  gateProps,
  ledProps,
  receiptProps,
  type ChipTone,
  type GateState,
  type LedTone,
  type ReceiptState,
  type SourceKind,
  type StatusTone,
} from '../primitives-prx/types';

export interface SectionStripProps {
  number: number | string;
  label: string;
  className?: string;
  style?: CSSProperties;
}

export function SectionStrip({ number, label, className = '', style }: SectionStripProps) {
  return (
    <div
      className={className ? `prx-section-strip ${className}` : 'prx-section-strip'}
      style={style}
      data-testid="prx-section-strip"
    >
      <span className="num">{String(number).padStart(2, '0')}</span>
      <span className="sep">·</span>
      <span className="label">{label}</span>
    </div>
  );
}

export interface TokenChipProps extends HTMLAttributes<HTMLSpanElement> {
  children: ReactNode;
  tone?: ChipTone;
  source?: SourceKind;
}

export function TokenChip({ children, tone, source, className = '', ...rest }: TokenChipProps) {
  const attrs = chipProps({ tone, source });
  return (
    <span
      {...attrs}
      {...rest}
      className={className ? `${attrs.className} ${className}` : attrs.className}
      data-testid="prx-token-chip"
    >
      {children}
    </span>
  );
}

export interface RadioPillOption {
  value: string;
  label: ReactNode;
  disabled?: boolean;
}

export interface RadioPillGroupProps {
  options: RadioPillOption[];
  value: string;
  onChange: (value: string, option: RadioPillOption) => void;
  ariaLabel?: string;
  className?: string;
  style?: CSSProperties;
}

export function RadioPillGroup({
  options,
  value,
  onChange,
  ariaLabel,
  className = '',
  style,
}: RadioPillGroupProps) {
  function activate(option: RadioPillOption) {
    if (!option.disabled) onChange(option.value, option);
  }

  return (
    <div
      className={className ? `prx-radio-group ${className}` : 'prx-radio-group'}
      role="radiogroup"
      aria-label={ariaLabel}
      style={style}
      data-testid="prx-radio-pill-group"
    >
      {options.map((option) => {
        const checked = option.value === value;
        return (
          <span
            role="radio"
            aria-checked={checked}
            aria-disabled={option.disabled || undefined}
            className={checked ? 'prx-radio-pill checked' : 'prx-radio-pill'}
            data-value={option.value}
            key={option.value}
            tabIndex={checked ? 0 : -1}
            onClick={() => activate(option)}
            onKeyDown={(event) => {
              if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault();
                activate(option);
              }
            }}
          >
            {option.label}
          </span>
        );
      })}
    </div>
  );
}

export interface TableFilterInputProps extends InputHTMLAttributes<HTMLInputElement> {}

export function TableFilterInput({ className = '', autoComplete = 'off', spellCheck = false, ...props }: TableFilterInputProps) {
  return (
    <input
      {...props}
      autoComplete={autoComplete}
      className={className ? `prx-table-filter ${className}` : 'prx-table-filter'}
      data-testid="prx-table-filter"
      spellCheck={spellCheck}
    />
  );
}

export interface GateBadgeProps {
  state: GateState;
  children: ReactNode;
  className?: string;
  style?: CSSProperties;
}

export function GateBadge({ state, children, className = '', style }: GateBadgeProps) {
  const attrs = gateProps(state);
  return (
    <span
      {...attrs}
      className={className ? `${attrs.className} ${className}` : attrs.className}
      style={style}
      data-testid="prx-gate-badge"
    >
      <span className="glyph" aria-hidden="true" />
      {children}
    </span>
  );
}

export interface ManifestTreeRow {
  id?: string;
  glyph: string;
  label: ReactNode;
  meta?: ReactNode;
  tone?: 'default' | 'muted' | 'locked';
  className?: string;
  style?: CSSProperties;
  onClick?: () => void;
}

export interface ManifestTreeProps {
  rows: ManifestTreeRow[];
  role?: AriaRole;
  ariaLabel?: string;
  className?: string;
  style?: CSSProperties;
}

export function ManifestTree({ rows, role, ariaLabel, className = '', style }: ManifestTreeProps) {
  return (
    <div
      className={className ? `prx-tree ${className}` : 'prx-tree'}
      role={role}
      aria-label={ariaLabel}
      style={style}
      data-testid="prx-manifest-tree"
    >
      {rows.map((row, index) => {
        const toneClass = row.tone && row.tone !== 'default' ? ` ${row.tone}` : '';
        const extraClass = row.className ? ` ${row.className}` : '';
        return (
          <div
            className={`row${toneClass}${extraClass}`}
            key={row.id ?? index}
            onClick={row.onClick}
            style={row.style}
          >
            <span className="glyph">{row.glyph}</span>
            <span className="label">{row.label}</span>
            {row.meta !== undefined ? <span className="meta">{row.meta}</span> : null}
          </div>
        );
      })}
    </div>
  );
}

export interface RunlogRow {
  ts?: ReactNode;
  actor?: ReactNode;
  what: ReactNode;
  status?: ReactNode;
  tone?: StatusTone;
}

export interface RunlogProps {
  rows: RunlogRow[];
  className?: string;
  style?: CSSProperties;
}

export function Runlog({ rows, className = '', style }: RunlogProps) {
  return (
    <div
      className={className ? `prx-runlog ${className}` : 'prx-runlog'}
      style={style}
      data-testid="prx-runlog"
    >
      {rows.map((row, index) => (
        <div className="row" key={index}>
          <span className="ts">{row.ts ?? ''}</span>
          <span className="actor">{row.actor ?? ''}</span>
          <span className="what">{row.what}</span>
          <span className="stat" data-tone={row.tone ?? 'dim'}>
            {row.status ?? '—'}
          </span>
        </div>
      ))}
    </div>
  );
}

export interface DiffLine {
  mark: '+' | '-' | '=' | '!';
  text: string;
}

export interface DiffBlockProps {
  lines: DiffLine[];
  className?: string;
  style?: CSSProperties;
}

export function DiffBlock({ lines, className = '', style }: DiffBlockProps) {
  return (
    <div
      className={className ? `prx-diff ${className}` : 'prx-diff'}
      style={style}
      data-testid="prx-diff-block"
    >
      {lines.map((line, index) => (
        <span className="line" data-mark={line.mark} key={index}>
          {line.text}
        </span>
      ))}
    </div>
  );
}

export interface KbdClusterProps {
  keys: string[];
  joiner?: string;
  className?: string;
  style?: CSSProperties;
}

export function KbdCluster({ keys, joiner = '+', className = '', style }: KbdClusterProps) {
  return (
    <span
      className={className ? `prx-kbd-cluster ${className}` : 'prx-kbd-cluster'}
      style={style}
      data-testid="prx-kbd-cluster"
    >
      {keys.map((key, index) => (
        <React.Fragment key={`${key}-${index}`}>
          {index > 0 ? <span className="plus">{joiner}</span> : null}
          <span className="prx-kbd">{key}</span>
        </React.Fragment>
      ))}
    </span>
  );
}

export interface GaugeProps {
  filled: number;
  total?: number;
  label?: ReactNode;
  tone?: 'default' | 'warn' | 'bad';
  className?: string;
  style?: CSSProperties;
}

export function Gauge({
  filled,
  total = 10,
  label,
  tone = 'default',
  className = '',
  style,
}: GaugeProps) {
  const clamped = Math.max(0, Math.min(filled, total));
  return (
    <div
      className={className ? `prx-gauge ${className}` : 'prx-gauge'}
      data-tone={tone === 'default' ? undefined : tone}
      style={style}
      data-testid="prx-gauge"
    >
      <span className="ticks" aria-hidden="true">
        {Array.from({ length: total }, (_, index) => (
          <span className={`t${index < clamped ? ' on' : ''}`} key={index} />
        ))}
      </span>
      {label !== undefined ? <span className="val">{label}</span> : null}
    </div>
  );
}

export interface BargraphProps {
  bars: string;
  label?: ReactNode;
  value?: ReactNode;
  tone?: 'default' | 'warn' | 'bad';
  className?: string;
  style?: CSSProperties;
}

export function Bargraph({
  bars,
  label,
  value,
  tone = 'default',
  className = '',
  style,
}: BargraphProps) {
  return (
    <div
      className={className ? `prx-bargraph ${className}` : 'prx-bargraph'}
      data-tone={tone === 'default' ? undefined : tone}
      style={style}
      data-testid="prx-bargraph"
    >
      <span className="bars">{bars}</span>
      {label !== undefined ? <span className="label">{label}</span> : null}
      {value !== undefined ? <span className="val">{value}</span> : null}
    </div>
  );
}

export interface ReceiptField {
  key: ReactNode;
  value: ReactNode;
}

export interface ReceiptCardProps {
  state: ReceiptState;
  title: ReactNode;
  meta?: ReactNode;
  fields: ReceiptField[];
  hash?: ReactNode;
  seal?: ReactNode;
  className?: string;
  style?: CSSProperties;
}

export function ReceiptCard({
  state,
  title,
  meta,
  fields,
  hash,
  seal,
  className = '',
  style,
}: ReceiptCardProps) {
  const attrs = receiptProps(state);
  return (
    <div
      {...attrs}
      className={className ? `${attrs.className} ${className}` : attrs.className}
      style={style}
      data-testid="prx-receipt-card"
    >
      <div className="hd">
        <span>{title}</span>
        {meta !== undefined ? <span>{meta}</span> : <span>{state}</span>}
      </div>
      {fields.map((field, index) => (
        <div className="row" key={index}>
          <div className="k">{field.key}</div>
          <div className="v">{field.value}</div>
        </div>
      ))}
      {hash !== undefined || seal !== undefined ? (
        <div className="ft">
          <span className="hash">{hash ?? ''}</span>
          <span className="seal">{seal ?? ''}</span>
        </div>
      ) : null}
    </div>
  );
}

export interface SparklineProps {
  values: number[];
  tone?: 'default' | 'warn' | 'bad';
  className?: string;
  style?: CSSProperties;
}

export function Sparkline({ values, tone = 'default', className = '', style }: SparklineProps) {
  const normalized = values.length > 1 ? values : [0, ...values];
  const max = Math.max(...normalized, 1);
  const min = Math.min(...normalized, 0);
  const span = Math.max(max - min, 1);
  const points = normalized.map((value, index) => {
    const x = normalized.length === 1 ? 48 : (index / (normalized.length - 1)) * 96;
    const y = 18 - ((value - min) / span) * 16 - 1;
    return [x, y] as const;
  });
  const path = points.map(([x, y], index) => `${index === 0 ? 'M' : 'L'}${x} ${y}`).join(' ');
  const last = points[points.length - 1];

  return (
    <span
      className={className ? `prx-spark ${className}` : 'prx-spark'}
      data-tone={tone === 'default' ? undefined : tone}
      style={style}
      data-testid="prx-sparkline"
    >
      <svg viewBox="0 0 96 18" preserveAspectRatio="none" aria-hidden="true">
        <path d={path} />
        <circle cx={last[0]} cy={last[1]} r="1.75" />
      </svg>
    </span>
  );
}

export interface LedDotProps {
  tone: LedTone;
  className?: string;
  style?: CSSProperties;
}

export function LedDot({ tone, className = '', style }: LedDotProps) {
  const attrs = ledProps(tone);
  return (
    <span
      {...attrs}
      className={className ? `${attrs.className} ${className}` : attrs.className}
      style={style}
      data-testid="prx-led-dot"
    />
  );
}

// ── Button ─────────────────────────────────────────────────────
// Single source of truth for buttons. Tones: primary | ghost | danger.
// Sizes: sm | md (default) | lg. Toggle state via `active` prop.
export type ButtonTone = 'default' | 'primary' | 'ghost' | 'danger';
export type ButtonSize = 'sm' | 'md' | 'lg';

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  tone?: ButtonTone;
  size?: ButtonSize;
  active?: boolean;
}

export function Button({
  tone = 'default',
  size = 'md',
  active,
  className = '',
  type,
  children,
  ...rest
}: ButtonProps) {
  const cls = className ? `prx-button ${className}` : 'prx-button';
  return (
    <button
      type={type ?? 'button'}
      className={cls}
      data-tone={tone === 'default' ? undefined : tone}
      data-size={size === 'md' ? undefined : size}
      data-active={active ? 'true' : undefined}
      data-testid="prx-button"
      {...rest}
    >
      {children}
    </button>
  );
}

// ── PanelCard ──────────────────────────────────────────────────
// Slot-based panel shell. Use for sidebar panels, workflow cards,
// quadrant frames, anywhere a bordered card with a labelled header
// is needed. The tone prop adds a left-edge accent ribbon.
export type PanelCardTone = 'default' | 'ok' | 'warn' | 'err' | 'live';

export interface PanelCardProps {
  eyebrow?: ReactNode;
  title?: ReactNode;
  count?: ReactNode;
  action?: ReactNode;
  footer?: ReactNode;
  tone?: PanelCardTone;
  className?: string;
  style?: CSSProperties;
  bodyClassName?: string;
  tight?: boolean;
  children?: ReactNode;
}

export function PanelCard({
  eyebrow,
  title,
  count,
  action,
  footer,
  tone = 'default',
  className = '',
  style,
  bodyClassName,
  tight,
  children,
}: PanelCardProps) {
  const cls = className ? `prx-card ${className}` : 'prx-card';
  const hasHead = eyebrow !== undefined || title !== undefined || count !== undefined || action !== undefined;
  const bodyCls = ['prx-card__body', tight ? 'prx-card__body--tight' : '', bodyClassName ?? '']
    .filter(Boolean)
    .join(' ');
  return (
    <div
      className={cls}
      style={style}
      data-tone={tone === 'default' ? undefined : tone}
      data-testid="prx-panel-card"
    >
      {hasHead && (
        <div className="prx-card__head">
          <div className="prx-card__head-copy">
            {eyebrow !== undefined && <span className="eyebrow">{eyebrow}</span>}
            {title !== undefined && <span className="title">{title}</span>}
          </div>
          {(count !== undefined || action !== undefined) && (
            <div className="prx-card__head-tail">
              {count !== undefined && <span className="prx-card__count">{count}</span>}
              {action}
            </div>
          )}
        </div>
      )}
      {children !== undefined && <div className={bodyCls}>{children}</div>}
      {footer !== undefined && <div className="prx-card__foot">{footer}</div>}
    </div>
  );
}

// ── MetricTile ─────────────────────────────────────────────────
// Bare metric tile for at-a-glance dashboard metrics.
// label / value / detail / action stacked. NO border, NO bg-tint.
// This is the canonical primitive for overview metrics — never compose
// ReceiptCard for at-a-glance numbers (its key/value rows are wrong here).
export type MetricTileTone = 'default' | 'ok' | 'warn' | 'err';

export interface MetricTileProps extends Omit<ButtonHTMLAttributes<HTMLButtonElement>, 'children'> {
  label: ReactNode;
  value: ReactNode;
  detail?: ReactNode;
  action?: ReactNode;
  tone?: MetricTileTone;
}

export function MetricTile({
  label,
  value,
  detail,
  action,
  tone = 'default',
  className = '',
  type,
  ...rest
}: MetricTileProps) {
  const cls = className ? `prx-tile ${className}` : 'prx-tile';
  return (
    <button
      type={type ?? 'button'}
      className={cls}
      data-tone={tone === 'default' ? undefined : tone}
      data-testid="prx-tile"
      {...rest}
    >
      <span className="prx-tile__label">{label}</span>
      <span className="prx-tile__value">{value}</span>
      {detail !== undefined && <span className="prx-tile__detail">{detail}</span>}
      {action !== undefined && <span className="prx-tile__action">{action}</span>}
    </button>
  );
}

// ── ListPanel ──────────────────────────────────────────────────
// Bare side / list panel: kicker + title + count badge + body.
// NO border, NO bg-tint, NO rim. The structure is type-only.
// Use for sidebar groups (Toolbelt Review, Recent Runs, etc.).
// NEVER use PanelCard for these — that adds a rim that doesn't earn its keep.
export interface ListPanelProps {
  eyebrow?: ReactNode;
  title: ReactNode;
  count?: ReactNode;
  action?: ReactNode;
  className?: string;
  style?: CSSProperties;
  children?: ReactNode;
}

export function ListPanel({
  eyebrow,
  title,
  count,
  action,
  className = '',
  style,
  children,
}: ListPanelProps) {
  const cls = className ? `prx-list-panel ${className}` : 'prx-list-panel';
  return (
    <section className={cls} style={style} data-testid="prx-list-panel">
      <header className="prx-list-panel__head">
        <div className="prx-list-panel__head-copy">
          {eyebrow !== undefined && <span className="prx-list-panel__eyebrow">{eyebrow}</span>}
          <span className="prx-list-panel__title">{title}</span>
        </div>
        {(count !== undefined || action !== undefined) && (
          <div className="prx-list-panel__head-tail">
            {count !== undefined && <span className="prx-list-panel__count">{count}</span>}
            {action}
          </div>
        )}
      </header>
      <div className="prx-list-panel__body">{children}</div>
    </section>
  );
}

// ── StatusRow ──────────────────────────────────────────────────
// Single row in a list: LedDot + (title + detail) + meta.
// NO border per-row. Hover-able if onClick provided, static otherwise.
// Use for sidebar list rows (review items, recent runs, opportunities).
export type StatusRowTone = 'live' | 'ok' | 'err' | 'idle';

export interface StatusRowProps {
  tone?: StatusRowTone;
  title: ReactNode;
  detail?: ReactNode;
  detailMono?: boolean;
  meta?: ReactNode;
  onClick?: () => void;
  className?: string;
  ariaLabel?: string;
}

export function StatusRow({
  tone = 'idle',
  title,
  detail,
  detailMono,
  meta,
  onClick,
  className = '',
  ariaLabel,
}: StatusRowProps) {
  const cls = ['prx-status-row', onClick ? '' : 'prx-status-row--static', className]
    .filter(Boolean)
    .join(' ');
  const detailCls = detailMono
    ? 'prx-status-row__detail prx-status-row__detail--mono'
    : 'prx-status-row__detail';
  const body = (
    <>
      <LedDot tone={tone} />
      <span className="prx-status-row__body">
        <span className="prx-status-row__title">{title}</span>
        {detail !== undefined && <span className={detailCls}>{detail}</span>}
      </span>
      {meta !== undefined && <span className="prx-status-row__meta">{meta}</span>}
    </>
  );
  if (onClick) {
    return (
      <button
        type="button"
        className={cls}
        onClick={onClick}
        aria-label={typeof ariaLabel === 'string' ? ariaLabel : undefined}
        data-testid="prx-status-row"
      >
        {body}
      </button>
    );
  }
  return (
    <div
      className={cls}
      aria-label={typeof ariaLabel === 'string' ? ariaLabel : undefined}
      data-testid="prx-status-row"
    >
      {body}
    </div>
  );
}

// ── SourceChip ─────────────────────────────────────────────────
// Source-option pill with palette-tone dot. Replaces inline-color
// chips in SourceOptionPills and surface tabs.
export type SourceChipTone = 'default' | 'ok' | 'warn' | 'err' | 'live';

export interface SourceChipProps extends Omit<ButtonHTMLAttributes<HTMLButtonElement>, 'children'> {
  tone?: SourceChipTone;
  active?: boolean;
  label: ReactNode;
  subtitle?: ReactNode;
}

export function SourceChip({
  tone = 'default',
  active,
  label,
  subtitle,
  className = '',
  type,
  ...rest
}: SourceChipProps) {
  const cls = className ? `prx-source-chip ${className}` : 'prx-source-chip';
  return (
    <button
      type={type ?? 'button'}
      className={cls}
      data-tone={tone === 'default' ? undefined : tone}
      data-active={active ? 'true' : undefined}
      data-testid="prx-source-chip"
      {...rest}
    >
      <span className="prx-source-chip__dot" />
      <span className="prx-source-chip__copy">
        <span className="prx-source-chip__label">{label}</span>
        {subtitle !== undefined && <span className="prx-source-chip__sub">{subtitle}</span>}
      </span>
    </button>
  );
}
