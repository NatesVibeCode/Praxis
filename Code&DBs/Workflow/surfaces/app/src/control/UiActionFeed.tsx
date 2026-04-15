import React, { useEffect, useMemo, useState } from 'react';
import { useSlice } from '../hooks/useSlice';
import { useToast } from '../primitives/Toast';
import { world } from '../world';
import { UI_ACTION_LOG_PATH, type UiActionEntry, undoUiAction } from './uiActionLedger';

function formatActionTime(value: number): string {
  return new Date(value).toLocaleTimeString([], {
    hour: 'numeric',
    minute: '2-digit',
  });
}

function formatSurfaceLabel(surface: string): string {
  if (!surface.trim()) return 'Control';
  return surface.charAt(0).toUpperCase() + surface.slice(1);
}

function formatCategoryLabel(category: UiActionEntry['category']): string {
  if (category === 'layout') return 'Layout';
  if (category === 'graph') return 'Graph';
  if (category === 'authority') return 'Authority';
  return 'Control';
}

function formatTarget(entry: UiActionEntry): string | null {
  if (!entry.target) return null;
  const kind = entry.target.kind.trim();
  const label = entry.target.label.trim();
  if (!label) return null;
  return kind ? `${kind}: ${label}` : label;
}

function recoveryMeta(entry: UiActionEntry): {
  label: string;
  color: string;
  background: string;
  border: string;
  detail: string;
} {
  if (entry.recovery === 'undone') {
    return {
      label: 'Undone',
      color: 'var(--text-muted)',
      background: 'color-mix(in srgb, var(--bg) 88%, var(--border) 12%)',
      border: 'color-mix(in srgb, var(--border) 78%, transparent)',
      detail: 'Rolled back through its original authority.',
    };
  }
  if (entry.recovery === 'undo_ready') {
    return {
      label: 'Undo Ready',
      color: 'var(--accent)',
      background: 'color-mix(in srgb, var(--accent) 16%, var(--bg-card) 84%)',
      border: 'color-mix(in srgb, var(--accent) 32%, var(--border) 68%)',
      detail: 'This action currently owns the active undo slot.',
    };
  }
  if (entry.recovery === 'superseded') {
    return {
      label: 'Locked',
      color: 'var(--warning, #b98b2f)',
      background: 'color-mix(in srgb, #f5b642 12%, var(--bg-card) 88%)',
      border: 'color-mix(in srgb, #f5b642 30%, var(--border) 70%)',
      detail: 'A newer action now owns the undo slot for this control lane.',
    };
  }
  return {
    label: 'Recorded',
    color: 'var(--text-secondary, var(--text-muted))',
    background: 'color-mix(in srgb, var(--bg-card) 94%, var(--border) 6%)',
    border: 'color-mix(in srgb, var(--border) 70%, transparent)',
    detail: 'No compensating action exists, so this entry is audit-only.',
  };
}

interface UiActionFeedProps {
  surface?: string;
  scope?: string;
  title?: string;
  subtitle?: string;
}

const CATEGORY_ORDER: UiActionEntry['category'][] = ['layout', 'graph', 'authority', 'control'];
type FeedCategoryFilter = 'all' | UiActionEntry['category'];

export function UiActionFeed({
  surface,
  scope,
  title = 'Recent Control',
  subtitle = 'Latest control actions with their authority, reason, and outcome.',
}: UiActionFeedProps) {
  const entries = ((useSlice(world, UI_ACTION_LOG_PATH) as UiActionEntry[] | null) ?? [])
    .filter((entry) => (!surface || entry.surface === surface) && (!scope || entry.undoScope === scope));
  const { show } = useToast();
  const categoryCounts = useMemo(() => entries.reduce(
    (counts, entry) => {
      counts[entry.category] += 1;
      return counts;
    },
    {
      layout: 0,
      graph: 0,
      authority: 0,
      control: 0,
    },
  ), [entries]);
  const availableCategories = useMemo(
    () => CATEGORY_ORDER.filter((category) => categoryCounts[category] > 0),
    [categoryCounts],
  );
  const [categoryFilter, setCategoryFilter] = useState<FeedCategoryFilter>('all');
  const [showOlderHistory, setShowOlderHistory] = useState(false);

  useEffect(() => {
    if (categoryFilter !== 'all' && !availableCategories.includes(categoryFilter)) {
      setCategoryFilter('all');
    }
  }, [availableCategories, categoryFilter]);

  useEffect(() => {
    setShowOlderHistory(false);
  }, [categoryFilter, entries.length]);

  const filteredEntries = useMemo(
    () => entries.filter((entry) => categoryFilter === 'all' || entry.category === categoryFilter),
    [categoryFilter, entries],
  );
  const collapsedEntries = useMemo(
    () => filteredEntries.filter((entry, index) => index >= 3 && entry.recovery !== 'undo_ready'),
    [filteredEntries],
  );
  const visibleEntries = useMemo(
    () => (showOlderHistory
      ? filteredEntries
      : filteredEntries.filter((entry, index) => index < 3 || entry.recovery === 'undo_ready')),
    [filteredEntries, showOlderHistory],
  );
  const summary = filteredEntries.reduce(
    (counts, entry) => {
      counts[entry.recovery] += 1;
      return counts;
    },
    {
      undo_ready: 0,
      superseded: 0,
      recorded: 0,
      undone: 0,
    },
  );

  if (entries.length === 0) return null;

  return (
    <section
      aria-label="Recent control actions"
      style={{
        marginBottom: 'var(--space-lg)',
        padding: 'var(--space-md)',
        border: '1px solid var(--border)',
        borderRadius: 'var(--radius)',
        background: 'color-mix(in srgb, var(--bg-card) 92%, var(--accent) 8%)',
        display: 'flex',
        flexDirection: 'column',
        gap: 'var(--space-sm)',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 'var(--space-sm)' }}>
        <div>
          <div style={{ fontSize: 12, fontWeight: 700, letterSpacing: '0.08em', textTransform: 'uppercase', color: 'var(--text-muted)' }}>
            {title}
          </div>
          <div style={{ fontSize: 14, color: 'var(--text-secondary, var(--text-muted))' }}>
            {subtitle}
          </div>
        </div>
        <div style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'flex-end', gap: 8 }}>
          {summary.undo_ready > 0 ? (
            <span style={{ fontSize: 11, color: 'var(--accent)' }}>{summary.undo_ready} undo ready</span>
          ) : null}
          {summary.superseded > 0 ? (
            <span style={{ fontSize: 11, color: 'var(--warning, #b98b2f)' }}>{summary.superseded} locked</span>
          ) : null}
          {summary.recorded > 0 ? (
            <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>{summary.recorded} recorded</span>
          ) : null}
          {summary.undone > 0 ? (
            <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>{summary.undone} undone</span>
          ) : null}
        </div>
      </div>
      {availableCategories.length > 1 || collapsedEntries.length > 0 ? (
        <div
          style={{
            display: 'flex',
            flexWrap: 'wrap',
            alignItems: 'center',
            justifyContent: 'space-between',
            gap: 10,
          }}
        >
          {availableCategories.length > 1 ? (
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
              {(['all', ...availableCategories] as FeedCategoryFilter[]).map((option) => {
                const active = categoryFilter === option;
                const count = option === 'all'
                  ? entries.length
                  : categoryCounts[option];
                return (
                  <button
                    key={option}
                    type="button"
                    onClick={() => setCategoryFilter(option)}
                    style={{
                      border: active ? '1px solid var(--accent)' : '1px solid var(--border)',
                      background: active
                        ? 'color-mix(in srgb, var(--accent) 16%, var(--bg-card) 84%)'
                        : 'var(--bg-card)',
                      color: active ? 'var(--accent)' : 'var(--text-secondary, var(--text-muted))',
                      borderRadius: 999,
                      padding: '6px 10px',
                      fontSize: 11,
                      fontWeight: 700,
                      letterSpacing: '0.04em',
                      textTransform: 'uppercase',
                      cursor: 'pointer',
                    }}
                  >
                    {option === 'all' ? 'All' : formatCategoryLabel(option)} {count}
                  </button>
                );
              })}
            </div>
          ) : <span />}
          {collapsedEntries.length > 0 || showOlderHistory ? (
            <button
              type="button"
              onClick={() => setShowOlderHistory((value) => !value)}
              style={{
                border: '1px solid var(--border)',
                background: 'var(--bg-card)',
                color: 'var(--text-secondary, var(--text-muted))',
                borderRadius: 999,
                padding: '6px 10px',
                fontSize: 11,
                fontWeight: 700,
                letterSpacing: '0.04em',
                textTransform: 'uppercase',
                cursor: 'pointer',
              }}
            >
              {showOlderHistory ? 'Focus Newest' : `Show ${collapsedEntries.length} Older`}
            </button>
          ) : null}
        </div>
      ) : null}
      {visibleEntries.map((entry) => {
        const recovery = recoveryMeta(entry);
        return (
          <article
            key={entry.id}
            style={{
              display: 'grid',
              gridTemplateColumns: 'minmax(0, 1fr) auto',
              gap: 'var(--space-sm)',
              alignItems: 'start',
              padding: '12px 14px',
              borderRadius: 10,
              border: `1px solid ${recovery.border}`,
              background: recovery.background,
            }}
          >
            <div style={{ minWidth: 0 }}>
              <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                <strong style={{ fontSize: 14 }}>{entry.label}</strong>
                <span
                  style={{
                    fontSize: 11,
                    fontWeight: 700,
                    letterSpacing: '0.04em',
                    textTransform: 'uppercase',
                    color: 'var(--accent)',
                  }}
                >
                  {formatCategoryLabel(entry.category)}
                </span>
                <span
                  style={{
                    fontSize: 11,
                    fontWeight: 700,
                    letterSpacing: '0.04em',
                    textTransform: 'uppercase',
                    color: 'var(--text-muted)',
                  }}
                >
                  {formatSurfaceLabel(entry.surface)}
                </span>
                <span
                  style={{
                    fontSize: 11,
                    fontWeight: 700,
                    letterSpacing: '0.04em',
                    textTransform: 'uppercase',
                    color: recovery.color,
                  }}
                >
                  {recovery.label}
                </span>
                <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>{formatActionTime(entry.occurredAt)}</span>
              </div>
              <div style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 4 }}>
                Authority: <code>{entry.authority}</code>
              </div>
              {formatTarget(entry) ? (
                <div style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 4 }}>
                  Touched: <strong>{formatTarget(entry)}</strong>
                </div>
              ) : null}
              {entry.changeSummary.length > 0 ? (
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginBottom: 6 }}>
                  {entry.changeSummary.map((item) => (
                    <span
                      key={item}
                      style={{
                        fontSize: 11,
                        color: 'var(--text-secondary, var(--text-muted))',
                        border: '1px solid color-mix(in srgb, var(--border) 76%, transparent)',
                        borderRadius: 999,
                        padding: '3px 8px',
                        background: 'color-mix(in srgb, var(--bg-card) 88%, transparent)',
                      }}
                    >
                      {item}
                    </span>
                  ))}
                </div>
              ) : null}
              <div style={{ fontSize: 13, marginBottom: 4 }}>
                <strong>Reason:</strong> {entry.reason}
              </div>
              <div style={{ fontSize: 13, color: 'var(--text-secondary, var(--text-muted))' }}>
                <strong>Outcome:</strong> {entry.outcome}
              </div>
              <div style={{ fontSize: 12, color: recovery.color, marginTop: 6 }}>
                {recovery.detail}
              </div>
            </div>
            {entry.recovery === 'undo_ready' && entry.status === 'applied' ? (
              <button
                type="button"
                onClick={() => {
                  void (async () => {
                    const result = await undoUiAction(entry.id);
                    if (!result.ok) {
                      show(result.error || 'Undo failed.', 'error');
                      return;
                    }
                    show(`Undid ${entry.label}.`, 'success');
                  })();
                }}
                style={{
                  border: '1px solid var(--border)',
                  background: 'var(--bg-card)',
                  borderRadius: 999,
                  padding: '6px 12px',
                  fontSize: 12,
                  fontWeight: 600,
                  cursor: 'pointer',
                }}
              >
                Undo
              </button>
            ) : (
              <span
                style={{
                  alignSelf: 'start',
                  borderRadius: 999,
                  padding: '6px 10px',
                  fontSize: 11,
                  fontWeight: 700,
                  letterSpacing: '0.04em',
                  textTransform: 'uppercase',
                  color: recovery.color,
                  border: `1px solid ${recovery.border}`,
                  background: 'color-mix(in srgb, var(--bg-card) 88%, transparent)',
                }}
              >
                {recovery.label}
              </span>
            )}
          </article>
        );
      })}
      {!showOlderHistory && collapsedEntries.length > 0 ? (
        <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>
          Older locked and recorded actions are hidden so the active control lane stays visible.
        </div>
      ) : null}
    </section>
  );
}
