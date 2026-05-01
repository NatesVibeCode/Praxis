import React, { useEffect, useRef, useState } from 'react';

export interface PromptRef {
  /** Reference name as it appears after `{` or `@` */
  name: string;
  /** Type slug from data_dictionary_objects, used as a small label */
  type?: string;
}

export interface PromptInputProps {
  /** Available upstream typed inputs surfaced as autocomplete */
  refs: PromptRef[];
  value?: string;
  defaultValue?: string;
  placeholder?: string;
  rows?: number;
  /** Fires on every keystroke with the live text */
  onChange?: (value: string) => void;
  /** Fires when the user inserts a `{ref}` or `@ref` from the autocomplete */
  onRefInsert?: (ref: PromptRef, char: '{' | '@') => void;
  /**
   * Custom classifier — receives the live text, returns either a string
   * (renders as the summary HTML) or null (renders nothing). If omitted,
   * the default classifier infers trigger / step count / referenced refs.
   */
  classify?: (value: string, refs: PromptRef[]) => string | null;
  /** Optional id for the textarea */
  id?: string;
}

interface OpenState {
  start: number;
  char: '{' | '@';
  query: string;
  cursor: number;
}

const REF_TOKEN = /[a-zA-Z0-9_.]/;

function defaultClassify(text: string, refs: PromptRef[]): string | null {
  const lower = text.toLowerCase();
  let trigger: string | null = null;
  if (/webhook|http|url|endpoint/.test(lower)) trigger = 'Webhook trigger';
  else if (/event|fires|on plan|on workflow/.test(lower)) trigger = 'Event trigger';
  else if (/schedule|every|cron|daily|hourly/.test(lower)) trigger = 'Schedule trigger';
  else if (text.length > 8) trigger = 'Manual trigger';

  const stepHints = (text.match(/\bthen\b|\bnext\b|\bafter\b|→|->|;\s/gi) || []).length;
  const stepCount = text.length > 8 ? Math.max(1, stepHints + 1) : 0;

  const referenced = refs.filter(
    (r) => text.includes('{' + r.name + '}') || new RegExp('@' + r.name + '\\b').test(text),
  );

  const parts: string[] = [];
  if (trigger) parts.push(`<strong>${trigger}</strong>`);
  if (stepCount > 0) parts.push(`<strong>${stepCount} step${stepCount === 1 ? '' : 's'}</strong>`);
  if (referenced.length) {
    parts.push(`<strong>${referenced.length}</strong> upstream ref${referenced.length === 1 ? '' : 's'}`);
  }
  if (parts.length === 0) return '<em>no inferences yet · keep typing</em>';
  return 'Looks like: ' + parts.join(' → ');
}

/**
 * PromptInput — textarea with `{ref}` / `@mention` autocomplete and a
 * live classification line below. Drops into any prx-* surface.
 *
 * Uses CSS class `prx-prompt-input` (defined in primitives.css). Behavior
 * is React-controlled here rather than via the global initPromptInput
 * autoinit, so consumers get controlled-component semantics.
 */
export function PromptInput({
  refs,
  value: controlledValue,
  defaultValue = '',
  placeholder,
  rows = 4,
  onChange,
  onRefInsert,
  classify = defaultClassify,
  id,
}: PromptInputProps) {
  const isControlled = controlledValue !== undefined;
  const [internal, setInternal] = useState<string>(defaultValue);
  const value = isControlled ? (controlledValue as string) : internal;
  const taRef = useRef<HTMLTextAreaElement | null>(null);
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const [open, setOpen] = useState<OpenState | null>(null);

  const filtered: PromptRef[] = open
    ? refs.filter((r) => r.name.toLowerCase().includes(open.query.toLowerCase())).slice(0, 8)
    : [];

  function setValue(next: string) {
    if (!isControlled) setInternal(next);
    onChange?.(next);
  }

  function detectOpen(text: string, pos: number): OpenState | null {
    let i = pos - 1;
    while (i >= 0 && REF_TOKEN.test(text[i])) i--;
    if (i < 0) return null;
    const char = text[i];
    if (char !== '{' && char !== '@') return null;
    return { start: i, char, query: text.slice(i + 1, pos), cursor: 0 };
  }

  function handleInput(e: React.ChangeEvent<HTMLTextAreaElement>) {
    const next = e.target.value;
    setValue(next);
    const pos = e.target.selectionStart ?? next.length;
    setOpen(detectOpen(next, pos));
  }

  function insert(ref: PromptRef) {
    if (!open || !taRef.current) return;
    const ta = taRef.current;
    const before = value.slice(0, open.start);
    const after = value.slice(ta.selectionStart ?? value.length);
    const close = open.char === '{' ? '}' : '';
    const piece = open.char + ref.name + close;
    const next = before + piece + after;
    setValue(next);
    setOpen(null);
    onRefInsert?.(ref, open.char);
    requestAnimationFrame(() => {
      const pos = (before + piece).length;
      ta.focus();
      ta.setSelectionRange(pos, pos);
    });
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (!open) return;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setOpen({ ...open, cursor: Math.min(open.cursor + 1, filtered.length - 1) });
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setOpen({ ...open, cursor: Math.max(open.cursor - 1, 0) });
    } else if (e.key === 'Enter' || e.key === 'Tab') {
      if (filtered[open.cursor]) {
        e.preventDefault();
        insert(filtered[open.cursor]);
      }
    } else if (e.key === 'Escape') {
      e.preventDefault();
      setOpen(null);
    }
  }

  // Close on outside click
  useEffect(() => {
    function handler(e: MouseEvent) {
      if (!wrapRef.current?.contains(e.target as Node)) setOpen(null);
    }
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const summaryHtml = classify(value, refs);

  return (
    <div className="prx-prompt-input" ref={wrapRef} data-testid="prx-prompt-input">
      <textarea
        ref={taRef}
        id={id}
        value={value}
        onChange={handleInput}
        onKeyDown={handleKeyDown}
        placeholder={placeholder}
        rows={rows}
        data-testid="prx-prompt-textarea"
      />
      {open && filtered.length > 0 && (
        <div className="refs open" data-testid="prx-prompt-refs">
          {filtered.map((r, i) => (
            <div
              key={r.name}
              className={'row' + (i === open.cursor ? ' sel' : '')}
              onMouseDown={(e) => {
                e.preventDefault();
                insert(r);
              }}
              data-ref-name={r.name}
            >
              <span className="glyph">{open.char}</span>
              <span className="name">{r.name}</span>
              <span className="type">{r.type ?? ''}</span>
            </div>
          ))}
        </div>
      )}
      {summaryHtml !== null && (
        <div className="summary" data-testid="prx-prompt-summary" dangerouslySetInnerHTML={{ __html: summaryHtml }} />
      )}
    </div>
  );
}
