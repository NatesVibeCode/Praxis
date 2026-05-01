import React, { type AriaRole, type CSSProperties, type HTMLAttributes, type InputHTMLAttributes, type ReactNode } from 'react';
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
