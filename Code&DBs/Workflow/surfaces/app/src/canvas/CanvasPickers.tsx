import React, { useEffect, useMemo, useRef, useState } from 'react';

// ── <CanvasPickerInput> ──────────────────────────────────────────────────
// Input + datalist. Suggestions are cached per endpoint for the session.
// Fields stay free-text but users see what's already in the system.

interface PickerSuggestion {
  value: string;
  label?: string;
  group?: string;
}

interface CanvasPickerInputProps {
  id?: string;
  value: string;
  onChange: (next: string) => void;
  placeholder?: string;
  /** Endpoint to fetch suggestions from. Shape: `{ [key]: {value,label}[] }`. */
  suggestionsUrl?: string;
  /** Key in the response body to read (e.g. "sources", "authorities", "providers"). */
  suggestionsKey?: string;
  /** Optional extra static suggestions merged with fetched ones. */
  extraSuggestions?: PickerSuggestion[];
  /** Helper text rendered under the input. */
  hint?: React.ReactNode;
  ariaLabel?: string;
  disabled?: boolean;
  inputClassName?: string;
  style?: React.CSSProperties;
  type?: 'text' | 'url';
}

const pickerCache = new Map<string, PickerSuggestion[]>();
const pickerPending = new Map<string, Promise<PickerSuggestion[]>>();

function fetchPickerSuggestions(url: string, key: string): Promise<PickerSuggestion[]> {
  const cached = pickerCache.get(url);
  if (cached) return Promise.resolve(cached);
  const pending = pickerPending.get(url);
  if (pending) return pending;
  const p = fetch(url)
    .then(async (response) => {
      if (!response.ok) return [] as PickerSuggestion[];
      const body = await response.json().catch(() => ({}));
      const list = Array.isArray(body?.[key]) ? body[key] : [];
      const items: PickerSuggestion[] = list
        .map((raw: any) => {
          const value = typeof raw?.value === 'string'
            ? raw.value
            : typeof raw?.key === 'string'
              ? raw.key
              : '';
          if (!value) return null;
          return {
            value,
            label: typeof raw?.label === 'string' && raw.label ? raw.label : value,
            group: typeof raw?.group === 'string' ? raw.group : (typeof raw?.kind === 'string' ? raw.kind : undefined),
          } as PickerSuggestion;
        })
        .filter((entry: PickerSuggestion | null): entry is PickerSuggestion => Boolean(entry));
      pickerCache.set(url, items);
      return items;
    })
    .catch(() => [] as PickerSuggestion[])
    .finally(() => {
      pickerPending.delete(url);
    });
  pickerPending.set(url, p);
  return p;
}

/** Clear cached picker suggestions. Call after mutations that change the source list. */
export function invalidateCanvasPickerCache(url?: string) {
  if (url) {
    pickerCache.delete(url);
  } else {
    pickerCache.clear();
  }
}

let _datalistSerial = 0;
function nextDatalistId(): string {
  _datalistSerial += 1;
  return `canvas-picker-dl-${Date.now()}-${_datalistSerial}`;
}

export function CanvasPickerInput({
  id,
  value,
  onChange,
  placeholder,
  suggestionsUrl,
  suggestionsKey = 'items',
  extraSuggestions = [],
  hint,
  ariaLabel,
  disabled,
  inputClassName = 'canvas-dock-form__input',
  style,
  type = 'text',
}: CanvasPickerInputProps) {
  const [fetched, setFetched] = useState<PickerSuggestion[]>([]);
  const listId = useMemo(() => nextDatalistId(), []);

  useEffect(() => {
    if (!suggestionsUrl) return;
    let cancelled = false;
    fetchPickerSuggestions(suggestionsUrl, suggestionsKey).then((items) => {
      if (!cancelled) setFetched(items);
    });
    return () => {
      cancelled = true;
    };
  }, [suggestionsUrl, suggestionsKey]);

  const merged = useMemo<PickerSuggestion[]>(() => {
    const seen = new Set<string>();
    const out: PickerSuggestion[] = [];
    for (const entry of [...extraSuggestions, ...fetched]) {
      if (seen.has(entry.value)) continue;
      seen.add(entry.value);
      out.push(entry);
    }
    return out;
  }, [extraSuggestions, fetched]);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 2, ...style }}>
      <input
        id={id}
        type={type}
        className={inputClassName}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        list={merged.length ? listId : undefined}
        aria-label={ariaLabel}
        disabled={disabled}
      />
      {merged.length > 0 && (
        <datalist id={listId}>
          {merged.map((s) => (
            <option key={s.value} value={s.value}>
              {s.label || s.value}
            </option>
          ))}
        </datalist>
      )}
      {hint && <div className="canvas-dock__item-desc" style={{ marginTop: 0 }}>{hint}</div>}
    </div>
  );
}

// ── <CanvasCronBuilder> ─────────────────────────────────────────────────
// Visual cron builder with preset chips, custom mode, and a live preview
// of the next five fire times. Cron parsing covers the common subset used
// in Praxis: `@daily`, `@hourly`, `@weekly`, `@monthly`, and five-field
// expressions `min hour dom month dow`. Out-of-subset expressions fall
// through to custom mode without a preview rather than pretending to parse.

type CronPreset = {
  value: string;
  label: string;
  description: string;
};

const CRON_PRESETS: CronPreset[] = [
  { value: '*/15 * * * *', label: 'Every 15 min', description: 'Fires 4 times an hour' },
  { value: '0 * * * *', label: 'Hourly', description: 'Top of every hour' },
  { value: '@daily', label: 'Daily (midnight)', description: 'Midnight UTC every day' },
  { value: '0 9 * * *', label: 'Every day 9am', description: '9:00 AM UTC every day' },
  { value: '0 9 * * 1-5', label: 'Weekdays 9am', description: '9:00 AM UTC, Mon–Fri' },
  { value: '@weekly', label: 'Weekly (Sunday)', description: 'Sunday midnight UTC' },
  { value: '@monthly', label: 'Monthly (1st)', description: 'First of each month, UTC' },
];

interface CronFields {
  minute: number[];
  hour: number[];
  dom: number[];
  month: number[];
  dow: number[];
}

function expandCronField(spec: string, min: number, max: number): number[] | null {
  const result = new Set<number>();
  for (const part of spec.split(',')) {
    const trimmed = part.trim();
    if (!trimmed) return null;
    let stepStr: string | undefined;
    let rangeStr = trimmed;
    if (trimmed.includes('/')) {
      const slashIdx = trimmed.indexOf('/');
      rangeStr = trimmed.slice(0, slashIdx);
      stepStr = trimmed.slice(slashIdx + 1);
    }
    const step = stepStr ? Number(stepStr) : 1;
    if (!Number.isFinite(step) || step <= 0) return null;
    let start = min;
    let end = max;
    if (rangeStr === '*') {
      // keep defaults
    } else if (rangeStr.includes('-')) {
      const [a, b] = rangeStr.split('-').map((x) => Number(x.trim()));
      if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
      start = a;
      end = b;
    } else {
      const n = Number(rangeStr);
      if (!Number.isFinite(n)) return null;
      if (stepStr) {
        start = n;
        end = max;
      } else {
        start = n;
        end = n;
      }
    }
    if (start < min || end > max || start > end) return null;
    for (let i = start; i <= end; i += step) result.add(i);
  }
  return [...result].sort((a, b) => a - b);
}

function parseCronExpression(expr: string): CronFields | null {
  const trimmed = expr.trim();
  if (!trimmed) return null;
  const shortcut: Record<string, string> = {
    '@yearly': '0 0 1 1 *',
    '@annually': '0 0 1 1 *',
    '@monthly': '0 0 1 * *',
    '@weekly': '0 0 * * 0',
    '@daily': '0 0 * * *',
    '@midnight': '0 0 * * *',
    '@hourly': '0 * * * *',
  };
  const expanded = shortcut[trimmed.toLowerCase()] || trimmed;
  const parts = expanded.split(/\s+/);
  if (parts.length !== 5) return null;
  const minute = expandCronField(parts[0], 0, 59);
  const hour = expandCronField(parts[1], 0, 23);
  const dom = expandCronField(parts[2], 1, 31);
  const month = expandCronField(parts[3], 1, 12);
  const dow = expandCronField(parts[4], 0, 6);
  if (!minute || !hour || !dom || !month || !dow) return null;
  return { minute, hour, dom, month, dow };
}

function computeNextFireTimes(expr: string, count: number, from: Date = new Date()): Date[] | null {
  const fields = parseCronExpression(expr);
  if (!fields) return null;
  const out: Date[] = [];
  const cursor = new Date(from.getTime());
  cursor.setUTCSeconds(0, 0);
  cursor.setUTCMinutes(cursor.getUTCMinutes() + 1);
  let safety = 525_600; // one year of minutes
  while (out.length < count && safety > 0) {
    safety -= 1;
    const m = cursor.getUTCMinutes();
    const h = cursor.getUTCHours();
    const d = cursor.getUTCDate();
    const mo = cursor.getUTCMonth() + 1;
    const dw = cursor.getUTCDay();
    if (
      fields.minute.includes(m)
      && fields.hour.includes(h)
      && fields.dom.includes(d)
      && fields.month.includes(mo)
      && fields.dow.includes(dw)
    ) {
      out.push(new Date(cursor.getTime()));
    }
    cursor.setUTCMinutes(cursor.getUTCMinutes() + 1);
  }
  return out;
}

function formatFireTime(date: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())} ${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())} UTC`;
}

interface CanvasCronBuilderProps {
  value: string;
  onChange: (next: string) => void;
  id?: string;
}

export function CanvasCronBuilder({ value, onChange, id }: CanvasCronBuilderProps) {
  const [custom, setCustom] = useState(() => {
    const match = CRON_PRESETS.find((p) => p.value === value.trim());
    return !match;
  });
  const preview = useMemo(() => computeNextFireTimes(value, 5), [value]);
  const activePreset = CRON_PRESETS.find((p) => p.value === value.trim());
  return (
    <div className="canvas-cron-builder" style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
      <div className="canvas-http-request__preset-grid" role="radiogroup" aria-label="Schedule preset">
        {CRON_PRESETS.map((preset) => {
          const isActive = !custom && activePreset?.value === preset.value;
          return (
            <button
              key={preset.value}
              type="button"
              role="radio"
              aria-checked={isActive}
              className={`canvas-http-request__preset${isActive ? ' canvas-http-request__preset--active' : ''}`}
              onClick={() => {
                setCustom(false);
                onChange(preset.value);
              }}
            >
              <span className="canvas-http-request__preset-title">{preset.label}</span>
              <span className="canvas-http-request__preset-desc">{preset.description}</span>
            </button>
          );
        })}
        <button
          key="custom"
          type="button"
          role="radio"
          aria-checked={custom}
          className={`canvas-http-request__preset${custom ? ' canvas-http-request__preset--active' : ''}`}
          onClick={() => setCustom(true)}
        >
          <span className="canvas-http-request__preset-title">Custom</span>
          <span className="canvas-http-request__preset-desc">Write a cron expression</span>
        </button>
      </div>
      {custom && (
        <input
          id={id}
          type="text"
          className="canvas-dock-form__input"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder="e.g. 0 9 * * 1-5"
          aria-label="Cron expression"
        />
      )}
      <div className="canvas-dock__item-desc" style={{ marginTop: 0 }}>
        {preview && preview.length > 0 ? (
          <>
            <div style={{ fontWeight: 600 }}>Next fire times (UTC):</div>
            <ul style={{ margin: '2px 0 0', padding: '0 0 0 16px' }}>
              {preview.map((d, i) => (
                <li key={i}>{formatFireTime(d)}</li>
              ))}
            </ul>
          </>
        ) : (
          <span style={{ color: 'var(--canvas-warning, #c07)' }}>
            Cron expression not recognized — preview unavailable. Fires according to the server clock.
          </span>
        )}
      </div>
    </div>
  );
}

// ── <CanvasLocalHistoryRail> ────────────────────────────────────────────
// Keeps the last N saves of a text value in localStorage, keyed by a
// caller-provided scope. Shows a compact rail the user can click to
// restore a prior version.

interface CanvasLocalHistoryRailProps {
  scopeKey: string;
  currentValue: string;
  onRestore: (value: string) => void;
  limit?: number;
  label?: string;
}

interface HistoryEntry {
  value: string;
  savedAt: number;
}

function historyStorageKey(scopeKey: string): string {
  return `canvas.history:${scopeKey}`;
}

function readHistory(scopeKey: string): HistoryEntry[] {
  try {
    const raw = window.localStorage.getItem(historyStorageKey(scopeKey));
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter(
      (entry): entry is HistoryEntry =>
        entry
        && typeof entry === 'object'
        && typeof (entry as any).value === 'string'
        && typeof (entry as any).savedAt === 'number',
    );
  } catch {
    return [];
  }
}

function writeHistory(scopeKey: string, entries: HistoryEntry[]): void {
  try {
    window.localStorage.setItem(historyStorageKey(scopeKey), JSON.stringify(entries));
  } catch {
    /* quota or disabled — degrade silently */
  }
}

/** Record `value` into history for `scopeKey`. No-op on empty / unchanged input. */
export function recordCanvasHistory(scopeKey: string, value: string, limit = 10): void {
  if (!scopeKey) return;
  const trimmed = (value ?? '').trim();
  if (!trimmed) return;
  const entries = readHistory(scopeKey);
  if (entries[0]?.value === value) return;
  const next: HistoryEntry[] = [{ value, savedAt: Date.now() }, ...entries].slice(0, limit);
  writeHistory(scopeKey, next);
}

function relativeTime(savedAt: number): string {
  const delta = Math.max(0, Date.now() - savedAt);
  const sec = Math.floor(delta / 1000);
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const day = Math.floor(hr / 24);
  return `${day}d ago`;
}

export function CanvasLocalHistoryRail({
  scopeKey,
  currentValue,
  onRestore,
  limit = 10,
  label = 'Recent saves',
}: CanvasLocalHistoryRailProps) {
  const [entries, setEntries] = useState<HistoryEntry[]>(() => readHistory(scopeKey));
  const scopeRef = useRef(scopeKey);

  useEffect(() => {
    scopeRef.current = scopeKey;
    setEntries(readHistory(scopeKey));
  }, [scopeKey]);

  // Re-read on focus so updates from sibling tabs / other mounts surface.
  useEffect(() => {
    function refresh() {
      setEntries(readHistory(scopeRef.current));
    }
    window.addEventListener('focus', refresh);
    window.addEventListener('storage', refresh);
    return () => {
      window.removeEventListener('focus', refresh);
      window.removeEventListener('storage', refresh);
    };
  }, []);

  const displayable = entries.slice(0, limit).filter((entry) => entry.value !== currentValue);
  if (displayable.length === 0) return null;

  return (
    <div className="canvas-history-rail" style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      <div className="canvas-dock__item-desc" style={{ marginTop: 0, fontWeight: 600 }}>{label}</div>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
        {displayable.map((entry) => {
          const preview = entry.value.trim().slice(0, 28);
          return (
            <button
              key={entry.savedAt}
              type="button"
              className="canvas-trigger-pill"
              title={entry.value}
              onClick={() => onRestore(entry.value)}
              style={{ padding: '2px 6px', fontSize: 11 }}
            >
              <span style={{ opacity: 0.75 }}>{relativeTime(entry.savedAt)}</span>
              <span style={{ margin: '0 4px', opacity: 0.4 }}>·</span>
              <span>{preview || '(empty)'}{entry.value.length > 28 ? '…' : ''}</span>
            </button>
          );
        })}
        <button
          type="button"
          className="canvas-trigger-pill canvas-trigger-pill--add"
          style={{ padding: '2px 6px', fontSize: 11 }}
          onClick={() => {
            if (!window.confirm('Clear history for this field?')) return;
            writeHistory(scopeRef.current, []);
            setEntries([]);
          }}
        >
          Clear
        </button>
      </div>
    </div>
  );
}

// ── <CanvasJsonEditor> ───────────────────────────────────────────────────
// Textarea + live parse + Format/Compact/Template controls. Accepts empty
// string as "unset" (no error). Templates insert snippets inside the top-level
// object when value is a valid object, otherwise replace the buffer.

type JsonEditorStatus =
  | { kind: 'empty' }
  | { kind: 'valid'; summary: string }
  | { kind: 'error'; message: string };

export interface CanvasJsonEditorTemplate {
  label: string;
  /** Snippet to insert. For objects, pass a "key": value fragment without braces. */
  snippet: string;
  /** If true, replace the whole buffer instead of inserting. */
  replace?: boolean;
}

interface CanvasJsonEditorProps {
  value: string;
  onChange: (next: string) => void;
  id?: string;
  placeholder?: string;
  rows?: number;
  minHeight?: number;
  ariaLabel?: string;
  templates?: CanvasJsonEditorTemplate[];
  /** When true the editor also validates arrays/primitives. Default: allow anything JSON-parseable. */
  allowNonObject?: boolean;
  /** Called with parsed value on successful validation (useful for live preview). */
  onValid?: (parsed: unknown) => void;
  disabled?: boolean;
}

function summarizeJson(parsed: unknown): string {
  if (parsed === null) return 'null';
  if (Array.isArray(parsed)) return `Valid JSON array (${parsed.length} item${parsed.length === 1 ? '' : 's'})`;
  if (typeof parsed === 'object') {
    const keys = Object.keys(parsed as Record<string, unknown>);
    return `Valid JSON (${keys.length} key${keys.length === 1 ? '' : 's'})`;
  }
  return `Valid JSON (${typeof parsed})`;
}

function computeStatus(value: string, allowNonObject: boolean): { status: JsonEditorStatus; parsed?: unknown } {
  const trimmed = value.trim();
  if (!trimmed) return { status: { kind: 'empty' } };
  try {
    const parsed = JSON.parse(trimmed);
    if (!allowNonObject && (parsed === null || typeof parsed !== 'object')) {
      return { status: { kind: 'error', message: 'Expected a JSON object like { … }.' } };
    }
    return { status: { kind: 'valid', summary: summarizeJson(parsed) }, parsed };
  } catch (err: any) {
    const msg = String(err?.message || err || 'Parse error').replace(/^JSON\.parse: /, '');
    return { status: { kind: 'error', message: msg } };
  }
}

export function CanvasJsonEditor({
  value,
  onChange,
  id,
  placeholder,
  rows = 5,
  minHeight = 118,
  ariaLabel,
  templates,
  allowNonObject = true,
  onValid,
  disabled,
}: CanvasJsonEditorProps) {
  const { status, parsed } = useMemo(() => computeStatus(value, allowNonObject), [value, allowNonObject]);
  const lastValidJsonRef = useRef<string>('');
  const [templateOpen, setTemplateOpen] = useState(false);

  useEffect(() => {
    if (status.kind === 'valid' && onValid) onValid(parsed);
  }, [status, parsed, onValid]);

  const handleFormat = () => {
    if (status.kind !== 'valid') return;
    try {
      const pretty = JSON.stringify(JSON.parse(value), null, 2);
      onChange(pretty);
      lastValidJsonRef.current = pretty;
    } catch { /* ignore */ }
  };

  const handleCompact = () => {
    if (status.kind !== 'valid') return;
    try {
      const compact = JSON.stringify(JSON.parse(value));
      onChange(compact);
    } catch { /* ignore */ }
  };

  const applyTemplate = (tpl: CanvasJsonEditorTemplate) => {
    setTemplateOpen(false);
    if (tpl.replace) {
      onChange(tpl.snippet);
      return;
    }
    // Insert into an existing top-level object if possible.
    const trimmed = value.trim();
    if (!trimmed) {
      onChange(`{\n  ${tpl.snippet}\n}`);
      return;
    }
    try {
      const parsedExisting = JSON.parse(trimmed);
      if (parsedExisting && typeof parsedExisting === 'object' && !Array.isArray(parsedExisting)) {
        // Build a merged object by parsing the fragment as a single-key record.
        const frag = JSON.parse(`{${tpl.snippet}}`);
        const merged = { ...parsedExisting, ...frag };
        onChange(JSON.stringify(merged, null, 2));
        return;
      }
    } catch { /* fall through */ }
    // Fallback: append
    onChange(`${value.replace(/\s+$/, '')}\n${tpl.snippet}`);
  };

  const statusColor = status.kind === 'error' ? 'var(--danger)' : status.kind === 'valid' ? 'var(--success)' : 'var(--text-muted)';
  const statusText =
    status.kind === 'empty'
      ? '—'
      : status.kind === 'valid'
        ? `✓ ${status.summary}`
        : `⚠ ${status.message}`;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
      <textarea
        id={id}
        className="canvas-dock-form__input"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        rows={rows}
        style={{ minHeight, resize: 'vertical', fontFamily: 'var(--canvas-mono, ui-monospace, SFMono-Regular, Menlo, monospace)', fontSize: 12 }}
        aria-label={ariaLabel}
        spellCheck={false}
        disabled={disabled}
      />
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap', fontSize: 11 }}>
        <span style={{ color: statusColor, flex: '0 0 auto', minWidth: 0 }}>{statusText}</span>
        <span style={{ flex: 1 }} />
        <button
          type="button"
          className="canvas-trigger-pill"
          style={{ padding: '2px 8px', fontSize: 11, opacity: status.kind === 'valid' ? 1 : 0.4 }}
          disabled={status.kind !== 'valid' || disabled}
          onClick={handleFormat}
          title="Pretty-print"
        >
          Format
        </button>
        <button
          type="button"
          className="canvas-trigger-pill"
          style={{ padding: '2px 8px', fontSize: 11, opacity: status.kind === 'valid' ? 1 : 0.4 }}
          disabled={status.kind !== 'valid' || disabled}
          onClick={handleCompact}
          title="Minify to a single line"
        >
          Compact
        </button>
        {templates && templates.length > 0 && (
          <div style={{ position: 'relative' }}>
            <button
              type="button"
              className="canvas-trigger-pill"
              style={{ padding: '2px 8px', fontSize: 11 }}
              onClick={() => setTemplateOpen((open) => !open)}
              disabled={disabled}
              title="Insert a snippet"
            >
              Insert ▾
            </button>
            {templateOpen && (
              <div
                style={{
                  position: 'absolute',
                  top: 'calc(100% + 4px)',
                  right: 0,
                  background: 'rgba(20,20,20,0.98)',
                  border: '1px solid rgba(255,255,255,0.2)',
                  borderRadius: 6,
                  padding: 4,
                  zIndex: 20,
                  display: 'flex',
                  flexDirection: 'column',
                  gap: 2,
                  minWidth: 200,
                }}
              >
                {templates.map((tpl) => (
                  <button
                    key={tpl.label}
                    type="button"
                    className="canvas-trigger-pill"
                    style={{ padding: '4px 8px', fontSize: 11, textAlign: 'left', justifyContent: 'flex-start' }}
                    onClick={() => applyTemplate(tpl)}
                  >
                    {tpl.label}
                  </button>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// ── <CanvasConfirmButton> ────────────────────────────────────────────────
// Destructive-action guard. First click primes the button into a "Click again
// to confirm" state for `timeoutMs` ms. Second click within that window fires
// `onConfirm`. Outside the window, state resets. Persistence of a form field
// already happens on every keystroke elsewhere in Canvas, so this pattern only
// guards the handful of true destroy-data actions (delete, clear, remove).

interface CanvasConfirmButtonProps {
  onConfirm: () => void | Promise<void>;
  label: React.ReactNode;
  confirmLabel?: React.ReactNode;
  timeoutMs?: number;
  className?: string;
  disabled?: boolean;
  /** Small inline hint rendered next to the button in primed state. */
  primedHint?: React.ReactNode;
  style?: React.CSSProperties;
  title?: string;
}

export function CanvasConfirmButton({
  onConfirm,
  label,
  confirmLabel = 'Click again to confirm',
  timeoutMs = 3000,
  className = 'canvas-dock-form__btn',
  disabled,
  primedHint,
  style,
  title,
}: CanvasConfirmButtonProps) {
  const [primed, setPrimed] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => () => { if (timerRef.current) clearTimeout(timerRef.current); }, []);

  const handleClick = async () => {
    if (disabled) return;
    if (!primed) {
      setPrimed(true);
      if (timerRef.current) clearTimeout(timerRef.current);
      timerRef.current = setTimeout(() => setPrimed(false), timeoutMs);
      return;
    }
    if (timerRef.current) { clearTimeout(timerRef.current); timerRef.current = null; }
    setPrimed(false);
    await onConfirm();
  };

  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
      <button
        type="button"
        className={className}
        style={primed ? { ...style, outline: '1px solid rgba(255,150,150,0.8)', outlineOffset: 0 } : style}
        disabled={disabled}
        onClick={handleClick}
        title={title}
      >
        {primed ? confirmLabel : label}
      </button>
      {primed && primedHint && (
        <span className="canvas-dock__item-desc" style={{ fontSize: 11, opacity: 0.75 }}>{primedHint}</span>
      )}
    </span>
  );
}
