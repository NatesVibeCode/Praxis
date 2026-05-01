import React, { useEffect, useMemo, useState } from 'react';
import { ScopeFence, StatusRail, VerifierSlot } from '../primitives';

export interface WorkspaceVerifierRef {
  verifier_ref: string;
  display_name?: string;
  description?: string;
  enabled?: boolean;
}

export function normalizeLineDraft(value: string[]): string[] {
  return value.map((line) => line.trim()).filter(Boolean);
}

export function deriveRequirements(intent: string): string[] {
  const text = intent.trim().replace(/\s+/g, ' ');
  if (!text) return [];
  return text
    .split(/\n+|[.;]\s+/)
    .map((line) => line.trim().replace(/[.。]+$/, ''))
    .filter(Boolean)
    .slice(0, 5)
    .map((line) => (line.length > 92 ? `${line.slice(0, 89)}...` : line));
}

export function compactId(value: string | null | undefined): string {
  if (!value) return 'none';
  return value.length > 22 ? `${value.slice(0, 12)}...${value.slice(-6)}` : value;
}

export function lineCountLabel(count: number, singular: string): string {
  return `${count} ${singular}${count === 1 ? '' : 's'}`;
}

export function verifierDisplayName(verifier: WorkspaceVerifierRef | null | undefined, fallbackRef?: string): string {
  const displayName = verifier?.display_name?.trim();
  if (displayName) return displayName.replace(/^verifier\s+/i, '').replace(/^job\s+/i, '');
  const ref = verifier?.verifier_ref || fallbackRef || '';
  if (!ref) return '';
  const segments = ref.split('.').filter(Boolean);
  const lastSegment = segments.length ? segments[segments.length - 1] : ref;
  return lastSegment
    .replace(/[_-]+/g, ' ')
    .replace(/\b\w/g, (letter: string) => letter.toUpperCase());
}

export function verifierHelpText(verifier: WorkspaceVerifierRef | null | undefined): string {
  const description = verifier?.description?.trim();
  if (description) return description;
  return 'Proof gate: the check that must pass before this work can be treated as sealed.';
}

interface WorkspacePathSuggestion {
  path: string;
  kind?: string;
  label?: string;
}

interface WorkspaceContractSuggestion {
  value: string;
  detail?: string;
  kind?: string;
  label?: string;
}

async function loadWorkspacePathSuggestions(query: string): Promise<WorkspacePathSuggestion[]> {
  const search = new URLSearchParams({
    q: query,
    limit: '12',
  });
  const response = await fetch(`/api/workspace-paths?${search.toString()}`);
  const payload = await response.json().catch(() => null);
  if (!response.ok) throw new Error(payload?.detail || payload?.error || 'Path catalog unavailable');
  return Array.isArray(payload?.items)
    ? payload.items.filter((item: WorkspacePathSuggestion) => typeof item?.path === 'string')
    : [];
}

async function loadWorkspaceContractSuggestions(query: string): Promise<WorkspaceContractSuggestion[]> {
  const search = new URLSearchParams({
    q: query,
    limit: '12',
  });
  const response = await fetch(`/api/workspace-contract-fields?${search.toString()}`);
  const payload = await response.json().catch(() => null);
  if (!response.ok) throw new Error(payload?.detail || payload?.error || 'Field catalog unavailable');
  return Array.isArray(payload?.items)
    ? payload.items.filter((item: WorkspaceContractSuggestion) => typeof item?.value === 'string')
    : [];
}

function slashQuery(value: string): string | null {
  const match = value.match(/(^|[\s(=<>])\/([A-Za-z0-9._:-]*)$/);
  return match ? match[2].toLowerCase() : null;
}

function insertContractPill(value: string, pill: string): string {
  const match = value.match(/(^|[\s(=<>])\/([A-Za-z0-9._:-]*)$/);
  if (!match || typeof match.index !== 'number') {
    const spacer = value.trim() && !value.endsWith(' ') ? ' ' : '';
    return `${value}${spacer}{${pill}}`;
  }
  const prefix = value.slice(0, match.index);
  return `${prefix}${match[1]}{${pill}}`;
}

function clausePills(value: string): string[] {
  const pills: string[] = [];
  const seen = new Set<string>();
  for (const match of value.matchAll(/\{([^{}]+)\}/g)) {
    const pill = match[1]?.trim();
    if (pill && !seen.has(pill)) {
      seen.add(pill);
      pills.push(pill);
    }
  }
  return pills;
}

interface WorkspaceClauseEditorProps {
  ordinal: string;
  label: string;
  hint: string;
  explanation: string;
  clauses: string[];
  placeholder: string;
  addLabel: string;
  tone: 'requirement' | 'anti';
  onChange: (clauses: string[]) => void;
}

const CLAUSE_OPERATOR_PRESETS: Array<{ label: string; text: string }> = [
  { label: 'must equal', text: '{field_a} must equal {field_b}' },
  { label: 'cannot touch', text: 'cannot touch {field}' },
  { label: 'must not exceed', text: '{field} must not exceed {value}' },
  { label: 'second review', text: 'each {item} needs a second review' },
  { label: 'call flow', text: 'if {field_a} mismatches {field_b}, call {flow}' },
];

export function WorkspaceClauseEditor({
  ordinal,
  label,
  hint,
  explanation,
  clauses,
  placeholder,
  addLabel,
  tone,
  onChange,
}: WorkspaceClauseEditorProps) {
  const rows = clauses.length ? clauses : [''];
  const [focusedIndex, setFocusedIndex] = useState<number | null>(null);
  const activeValue = focusedIndex === null ? '' : rows[focusedIndex] ?? '';
  const query = slashQuery(activeValue);
  const [suggestions, setSuggestions] = useState<WorkspaceContractSuggestion[]>([]);
  const [loading, setLoading] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    if (query === null) {
      setSuggestions([]);
      setLoading(false);
      setLoadError(null);
      return () => {
        cancelled = true;
      };
    }
    const timer = window.setTimeout(() => {
      setLoading(true);
      setLoadError(null);
      void loadWorkspaceContractSuggestions(query)
        .then((items) => {
          if (!cancelled) setSuggestions(items);
        })
        .catch((error) => {
          if (!cancelled) {
            setSuggestions([]);
            setLoadError(error instanceof Error ? error.message : 'Field catalog unavailable');
          }
        })
        .finally(() => {
          if (!cancelled) setLoading(false);
        });
    }, 120);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [query]);

  const updateClause = (index: number, nextValue: string) => {
    const nextClauses = clauses.length ? [...clauses] : [''];
    nextClauses[index] = nextValue;
    onChange(nextClauses);
  };

  const removeClause = (index: number) => {
    onChange(clauses.filter((_, clauseIndex) => clauseIndex !== index));
  };

  const insertSuggestion = (index: number, suggestion: WorkspaceContractSuggestion) => {
    updateClause(index, insertContractPill(rows[index] ?? '', suggestion.value));
    setSuggestions([]);
  };

  const appendPreset = (text: string) => {
    const nextClauses = normalizeLineDraft(clauses);
    onChange([...nextClauses, text]);
  };

  return (
    <div className={`workspace-compose__field workspace-compose__field--clauses workspace-compose__field--clauses-${tone}`}>
      <div className="workspace-compose__field-label">
        <span className="workspace-compose__ord">{ordinal}</span>
        <span>{label}</span>
        <span className="workspace-compose__field-hint">{hint}</span>
      </div>
      <p className="workspace-compose__field-help">{explanation}</p>
      <div className="workspace-compose__clause-presets" aria-label={`${label} clause presets`}>
        {CLAUSE_OPERATOR_PRESETS.map((preset) => (
          <button key={`${label}-${preset.label}`} type="button" onClick={() => appendPreset(preset.text)}>
            {preset.label}
          </button>
        ))}
      </div>
      <div className="workspace-compose__clauses">
        {rows.map((clause, index) => {
          const pills = clausePills(clause);
          const showSuggestions = focusedIndex === index && query !== null;
          return (
            <div className="workspace-compose__clause" key={`${label}-${index}`}>
              <span className="workspace-compose__lock" aria-hidden="true" />
              <div className="workspace-compose__clause-body">
                <textarea
                  value={clause}
                  onChange={(event) => updateClause(index, event.target.value)}
                  onFocus={() => setFocusedIndex(index)}
                  onBlur={() => window.setTimeout(() => setFocusedIndex((current) => (current === index ? null : current)), 140)}
                  placeholder={placeholder}
                  rows={2}
                  spellCheck
                />
                {pills.length ? (
                  <div className="workspace-compose__clause-pills">
                    {pills.map((pill) => <code key={`${label}-${index}-${pill}`}>{pill}</code>)}
                  </div>
                ) : (
                  <div className="workspace-compose__clause-empty">Type / to insert an object, field, or flow.</div>
                )}
                {showSuggestions ? (
                  <div className="workspace-compose__clause-menu" role="listbox" aria-label={`${label} object and field suggestions`}>
                    {suggestions.map((suggestion) => (
                      <button
                        key={`${label}-${index}-${suggestion.value}`}
                        type="button"
                        role="option"
                        onMouseDown={(event) => {
                          event.preventDefault();
                          insertSuggestion(index, suggestion);
                        }}
                      >
                        <span>{suggestion.kind || 'field'}</span>
                        <code>{suggestion.label || suggestion.value}</code>
                        {suggestion.detail ? <em>{suggestion.detail}</em> : null}
                      </button>
                    ))}
                    {!loading && !suggestions.length ? (
                      <div className="workspace-compose__clause-menu-empty">
                        {loadError || 'No matching object or field yet.'}
                      </div>
                    ) : null}
                    {loading ? <div className="workspace-compose__clause-menu-empty">Looking up fields...</div> : null}
                  </div>
                ) : null}
              </div>
              <button
                type="button"
                className="workspace-compose__clause-remove"
                aria-label={`Remove ${label} clause`}
                disabled={!clauses.length}
                onClick={() => removeClause(index)}
              >
                x
              </button>
            </div>
          );
        })}
      </div>
      <button
        type="button"
        className="workspace-compose__pathlist-add"
        onClick={() => onChange([...clauses, ''])}
      >
        {addLabel}
      </button>
    </div>
  );
}

interface WorkspacePathScopePickerProps {
  ordinal: string;
  label: string;
  hint: string;
  explanation: string;
  lines: string[];
  placeholder: string;
  standardPaths: string[];
  standardDescription: string;
  emptyDescription: string;
  onChange: (lines: string[]) => void;
}

export function WorkspacePathScopePicker({
  ordinal,
  label,
  hint,
  explanation,
  lines,
  placeholder,
  standardPaths,
  standardDescription,
  emptyDescription,
  onChange,
}: WorkspacePathScopePickerProps) {
  const selectedPaths = useMemo(() => normalizeLineDraft(lines), [lines]);
  const [query, setQuery] = useState('');
  const [suggestions, setSuggestions] = useState<WorkspacePathSuggestion[]>([]);
  const [loading, setLoading] = useState(false);
  const [presetBusy, setPresetBusy] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const trimmedQuery = query.trim();
    if (!trimmedQuery) {
      setSuggestions([]);
      setLoading(false);
      setLoadError(null);
      return () => {
        cancelled = true;
      };
    }
    const timer = window.setTimeout(() => {
      setLoading(true);
      setLoadError(null);
      void loadWorkspacePathSuggestions(trimmedQuery)
        .then((items) => {
          if (!cancelled) setSuggestions(items);
        })
        .catch((error) => {
          if (!cancelled) {
            setSuggestions([]);
            setLoadError(error instanceof Error ? error.message : 'Path catalog unavailable');
          }
        })
        .finally(() => {
          if (!cancelled) setLoading(false);
        });
    }, 140);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [query]);

  const addPath = (path: string) => {
    const nextPath = path.trim();
    if (!nextPath || selectedPaths.includes(nextPath)) return;
    onChange([...selectedPaths, nextPath]);
    setQuery('');
    setSuggestions([]);
  };

  const removePath = (path: string) => {
    onChange(selectedPaths.filter((selectedPath) => selectedPath !== path));
  };

  const applyStandard = async () => {
    setPresetBusy(true);
    setLoadError(null);
    try {
      const verifiedPaths = await Promise.all(
        standardPaths.map(async (path) => {
          const matches = await loadWorkspacePathSuggestions(path);
          return matches.some((suggestion) => suggestion.path === path) ? path : null;
        }),
      );
      const nextPaths = [...selectedPaths];
      for (const path of verifiedPaths) {
        if (path && !nextPaths.includes(path)) nextPaths.push(path);
      }
      if (nextPaths.length === selectedPaths.length) {
        setLoadError('Standard ruleset paths are unavailable in this workspace.');
      }
      onChange(nextPaths);
    } catch (error) {
      setLoadError(error instanceof Error ? error.message : 'Standard ruleset unavailable');
    } finally {
      setPresetBusy(false);
    }
  };

  const visibleSuggestions = suggestions.filter((suggestion) => !selectedPaths.includes(suggestion.path));

  return (
    <div className="workspace-compose__field workspace-compose__field--pathscope">
      <div className="workspace-compose__field-label">
        <span className="workspace-compose__ord">{ordinal}</span>
        <span>{label}</span>
        <span className="workspace-compose__field-hint">{hint}</span>
      </div>
      <p className="workspace-compose__field-help">{explanation}</p>
      <div className="workspace-compose__scope-choices" aria-label={`${label} presets`}>
        <button
          type="button"
          className="workspace-compose__scope-choice"
          title={emptyDescription}
          onClick={() => onChange([])}
        >
          <strong>None</strong>
          <span>No paths selected</span>
        </button>
        <button
          type="button"
          className="workspace-compose__scope-choice workspace-compose__scope-choice--recommended"
          title={standardDescription}
          disabled={presetBusy}
          onClick={() => void applyStandard()}
        >
          <strong>Standard ruleset</strong>
          <span>{presetBusy ? 'Checking paths' : 'Suggested broad start'}</span>
        </button>
      </div>
      {selectedPaths.length ? (
        <ul className="workspace-compose__pathlist workspace-compose__pathlist--selected">
          {selectedPaths.map((path) => (
            <li key={path}>
              <span className="workspace-compose__lock" aria-hidden="true" />
              <code>{path}</code>
              <button type="button" aria-label={`Remove ${path}`} onClick={() => removePath(path)}>
                x
              </button>
            </li>
          ))}
        </ul>
      ) : (
        <div className="workspace-compose__scope-empty">{emptyDescription}</div>
      )}
      <div className="workspace-compose__path-search" role="combobox" aria-expanded={visibleSuggestions.length > 0}>
        <input
          value={query}
          onChange={(event) => {
            setQuery(event.target.value);
            setSuggestions([]);
          }}
          placeholder={placeholder}
          spellCheck={false}
          aria-label={`Search real paths for ${label}`}
        />
        <div className="workspace-compose__suggestions">
          {visibleSuggestions.slice(0, 8).map((suggestion) => (
            <button
              key={suggestion.path}
              type="button"
              className="workspace-compose__suggestion"
              onClick={() => addPath(suggestion.path)}
            >
              <span>{suggestion.kind === 'directory' ? 'dir' : 'file'}</span>
              <code>{suggestion.label || suggestion.path}</code>
            </button>
          ))}
          {!loading && !visibleSuggestions.length ? (
            <div className="workspace-compose__suggestion-empty">
              {query.trim()
                ? loadError || 'No matching real paths yet.'
                : 'Start typing, then click a real path to add it.'}
            </div>
          ) : null}
          {loading ? <div className="workspace-compose__suggestion-empty">Looking up real paths...</div> : null}
        </div>
      </div>
    </div>
  );
}

interface WorkspaceLineListEditorProps {
  ordinal: string;
  label: string;
  hint: string;
  lines: string[];
  placeholder: string;
  addLabel: string;
  onChange: (lines: string[]) => void;
}

export function WorkspaceLineListEditor({
  ordinal,
  label,
  hint,
  lines,
  placeholder,
  addLabel,
  onChange,
}: WorkspaceLineListEditorProps) {
  const rows = lines.length ? lines : [''];
  const updateLine = (index: number, nextValue: string) => {
    const nextLines = lines.length ? [...lines] : [''];
    nextLines[index] = nextValue;
    onChange(nextLines);
  };
  const removeLine = (index: number) => {
    onChange(lines.filter((_, lineIndex) => lineIndex !== index));
  };

  return (
    <div className="workspace-compose__field workspace-compose__field--pathlist">
      <div className="workspace-compose__field-label">
        <span className="workspace-compose__ord">{ordinal}</span>
        <span>{label}</span>
        <span className="workspace-compose__field-hint">{hint}</span>
      </div>
      <ul className="workspace-compose__pathlist workspace-compose__pathlist--editor">
        {rows.map((line, index) => (
          <li key={`${label}-${index}`}>
            <span className="workspace-compose__lock" aria-hidden="true" />
            <input
              value={line}
              onChange={(event) => updateLine(index, event.target.value)}
              placeholder={placeholder}
              spellCheck={false}
            />
            <button
              type="button"
              aria-label={`Remove ${label} item`}
              disabled={!lines.length}
              onClick={() => removeLine(index)}
            >
              x
            </button>
          </li>
        ))}
      </ul>
      <button
        type="button"
        className="workspace-compose__pathlist-add"
        onClick={() => onChange([...lines, ''])}
      >
        {addLabel}
      </button>
    </div>
  );
}

interface WorkspaceContractListProps {
  title: string;
  items: string[];
  empty: string;
  locked?: boolean;
  derived?: boolean;
}

interface WorkspaceBoundaryFenceProps {
  readScope: string[];
  writeScope: string[];
}

export function WorkspaceBoundaryFence({ readScope, writeScope }: WorkspaceBoundaryFenceProps) {
  const hasBoundary = readScope.length > 0 || writeScope.length > 0;
  const insideRows = [
    ...readScope.map((path) => ({
      scope: 'read' as const,
      label: 'read',
      target: path,
      note: 'allowed input',
    })),
    ...writeScope.map((path) => ({
      scope: 'write' as const,
      label: 'write',
      target: path,
      note: 'allowed edit',
    })),
  ];

  return (
    <ScopeFence
      className="workspace-compose__boundary"
      title="boundary"
      tone={hasBoundary ? 'ok' : 'warn'}
      toneLabel={hasBoundary ? 'selected' : 'missing'}
      zones={[
        {
          zone: 'inside',
          title: 'inside declared scope',
          rows: insideRows.length
            ? insideRows
            : [{
              scope: 'held',
              label: 'hold',
              target: 'no explicit read/write paths',
              note: 'drafting allowed; dispatch is weak without this',
            }],
        },
        {
          zone: 'outside',
          title: 'outside fence',
          rows: [{
            scope: 'denied',
            label: 'deny',
            target: 'anything not selected above',
            note: 'requires a contract update',
          }],
        },
      ]}
    />
  );
}

export function WorkspaceContractList({
  title,
  items,
  empty,
  locked = false,
  derived = false,
}: WorkspaceContractListProps) {
  const rows = items.length ? items : [empty];
  return (
    <div className="workspace-compose__mblock">
      <h4>
        {locked ? <span className="workspace-compose__lock" aria-hidden="true" /> : null}
        {title}
        {derived ? <span className="workspace-compose__mblock-note">derived</span> : null}
      </h4>
      <ul className="workspace-compose__mblock-list">
        {rows.map((line, index) => (
          <li
            key={`${title}-${index}-${line}`}
            className={!items.length || derived ? 'is-derived' : undefined}
          >
            <span className="workspace-compose__glyph" aria-hidden="true">{derived ? '->' : '-'}</span>
            <span>{line}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

interface WorkspaceVerifierCardProps {
  verifierLabel: string;
  verifierCount: number;
  verifierMissing: boolean;
  operationReceiptId: string;
  description?: string;
}

export function WorkspaceVerifierCard({
  verifierLabel,
  verifierCount,
  verifierMissing,
  operationReceiptId,
  description,
}: WorkspaceVerifierCardProps) {
  const verifierState = operationReceiptId
    ? 'passed'
    : verifierMissing
      ? 'blocked'
      : verifierCount
        ? 'available'
        : 'none';
  const dispatchLabel = verifierMissing ? 'select required' : verifierCount ? 'allowed' : 'blocked';
  return (
    <div className="workspace-compose__verifier-card">
      <VerifierSlot
        state={verifierState}
        name={verifierLabel}
        label={dispatchLabel}
        detail={description || 'Choose how done gets proven before dispatch.'}
      />
      <StatusRail
        className="workspace-compose__verifier-status"
        items={[
          {
            label: 'available',
            value: verifierCount ? lineCountLabel(verifierCount, 'proof gate') : 'empty',
            tone: verifierCount ? 'ok' : 'warn',
          },
          {
            label: 'dispatch',
            value: dispatchLabel,
            tone: verifierMissing || !verifierCount ? 'warn' : 'ok',
          },
          {
            label: 'seal',
            value: operationReceiptId ? compactId(operationReceiptId) : 'on success',
            tone: operationReceiptId ? 'ok' : 'dim',
          },
        ]}
      />
    </div>
  );
}

interface WorkspaceCompiledReceiptGridProps {
  generatedManifestId?: string;
  workflowId: string;
  runId: string;
  compiledSpec: unknown;
}

export function WorkspaceCompiledReceiptGrid({
  generatedManifestId,
  workflowId,
  runId,
  compiledSpec,
}: WorkspaceCompiledReceiptGridProps) {
  return (
    <div className="workspace-compose__compiled">
      <div className="workspace-compose__compiled-grid">
        <div>
          <span>manifest</span>
          <strong>{compactId(generatedManifestId)}</strong>
        </div>
        <div>
          <span>workflow</span>
          <strong>{compactId(workflowId)}</strong>
        </div>
        <div>
          <span>run</span>
          <strong>{compactId(runId)}</strong>
        </div>
      </div>
      <details>
        <summary>Compiled payload</summary>
        <pre>{JSON.stringify(compiledSpec ?? {}, null, 2)}</pre>
      </details>
    </div>
  );
}
