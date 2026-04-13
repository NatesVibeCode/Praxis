import React, { useEffect, useState } from 'react';
import { emitPraxisOpenTab } from '../praxis/events';

interface DetailData {
  type: string;
  data: Record<string, unknown>;
}

const SEVERITY_COLORS: Record<string, string> = {
  P0: 'var(--danger)', P1: '#f0883e', P2: 'var(--warning)', P3: 'var(--text-muted)',
};

const STATUS_COLORS: Record<string, string> = {
  OPEN: 'var(--accent)', open: 'var(--accent)',
  resolved: 'var(--success)', fixed: 'var(--success)', closed: 'var(--success)',
  active: 'var(--success)', lead: 'var(--accent)', prospect: 'var(--warning)',
  churned: 'var(--danger)',
  'in-progress': '#f0883e', investigating: '#f0883e',
};

function StatusChip({ value }: { value: string }) {
  const color = STATUS_COLORS[value] ?? 'var(--text-muted)';
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', height: 22, padding: '0 10px',
      borderRadius: 11, fontSize: 11, fontWeight: 700, letterSpacing: '0.04em',
      color, background: `${color}1a`, border: `1px solid ${color}33`,
      textTransform: 'uppercase',
    }}>
      {value}
    </span>
  );
}

function SeverityBadge({ value }: { value: string }) {
  const color = SEVERITY_COLORS[value] ?? 'var(--text-muted)';
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
      width: 28, height: 22, borderRadius: 6, fontSize: 11, fontWeight: 700,
      color: '#fff', background: color,
    }}>
      {value}
    </span>
  );
}

function FieldRow({ label, value }: { label: string; value: unknown }) {
  const strVal = value == null ? '—' : String(value);
  const isLong = strVal.length > 80;
  const isStatus = label.toLowerCase() === 'status';
  const isSeverity = label.toLowerCase() === 'severity';
  const isMoney = label.toLowerCase().includes('value') || label.toLowerCase().includes('cost');

  return (
    <div style={{
      display: 'flex', flexDirection: isLong ? 'column' : 'row',
      gap: isLong ? 4 : 12, padding: '10px 0',
      borderBottom: '1px solid var(--border)',
    }}>
      <span style={{
        fontSize: 12, fontWeight: 600, color: 'var(--text-muted)',
        minWidth: 100, textTransform: 'capitalize',
      }}>
        {label.replace(/_/g, ' ')}
      </span>
      <span style={{ fontSize: 13, color: 'var(--text)', flex: 1, wordBreak: 'break-word' }}>
        {isStatus ? <StatusChip value={strVal} /> :
         isSeverity ? <SeverityBadge value={strVal} /> :
         isMoney ? `$${Number(strVal).toLocaleString()}` :
         strVal}
      </span>
    </div>
  );
}

interface PanelAction {
  label: string;
  onClick: () => void;
  tone?: 'accent' | 'neutral';
}

function pickString(data: Record<string, unknown>, keys: string[]): string | null {
  for (const key of keys) {
    const value = data[key];
    if (typeof value === 'string' && value.trim()) {
      return value.trim();
    }
  }
  return null;
}

function resolvePanelActions(detail: DetailData): PanelAction[] {
  const data = detail.data;
  const manifestId = pickString(data, ['manifest_id', 'manifestId']);
  const workflowId = pickString(data, ['workflow_id', 'workflowId']);
  const runId = pickString(data, ['run_id', 'runId']);
  const definitionType = pickString(data, ['definition_type', 'definitionType']);
  const typeHint = detail.type.toLowerCase();
  const isOperatingModel = definitionType === 'operating_model' || typeHint.includes('operating_model');

  if (runId) {
    const actions: PanelAction[] = [
      {
        label: 'Open Run',
        tone: 'neutral',
        onClick: () => emitPraxisOpenTab({ kind: 'run-detail', runId }),
      },
    ];
    if (workflowId) {
      actions.push({
        label: 'Open Workflow',
        tone: 'accent',
        onClick: () => emitPraxisOpenTab({ kind: 'build', workflowId }),
      });
    }
    return actions;
  }

  if (manifestId) {
    return [
      {
        label: 'Edit Manifest',
        tone: 'neutral',
        onClick: () => emitPraxisOpenTab({ kind: 'manifest-editor', manifestId }),
      },
      {
        label: 'Open Manifest',
        tone: 'accent',
        onClick: () => emitPraxisOpenTab({ kind: 'manifest', manifestId }),
      },
    ];
  }

  if (workflowId) {
    return [
      {
        label: isOperatingModel ? 'Edit Model' : 'Edit Workflow',
        tone: 'neutral',
        onClick: () => (
          isOperatingModel
            ? emitPraxisOpenTab({ kind: 'edit-model', workflowId })
            : emitPraxisOpenTab({ kind: 'build', workflowId })
        ),
      },
      {
        label: 'Open Workflow',
        tone: 'accent',
        onClick: () => emitPraxisOpenTab({ kind: 'build', workflowId }),
      },
    ];
  }

  return [];
}

export function DetailSlidePanel() {
  const [detail, setDetail] = useState<DetailData | null>(null);
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    const handler = (e: Event) => {
      const d = (e as CustomEvent<DetailData>).detail;
      if (d?.data) {
        setDetail(d);
        setVisible(true);
      }
    };
    window.addEventListener('module-selection', handler);
    return () => window.removeEventListener('module-selection', handler);
  }, []);

  const close = () => {
    setVisible(false);
    setTimeout(() => setDetail(null), 200);
  };

  // Close on escape
  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === 'Escape') close(); };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, []);

  if (!detail) return null;

  const entries = Object.entries(detail.data).filter(
    ([k]) => !k.startsWith('_') && k !== 'search_vector'
  );

  // Determine title from data
  const title = String(
    detail.data.title ?? detail.data.name ?? detail.data.object_id ?? detail.data.bug_id ?? 'Details'
  );
  const actions = resolvePanelActions(detail);

  return (
    <>
      {/* Backdrop */}
      {visible && (
        <div
          onClick={close}
          style={{
            position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)',
            zIndex: 50, transition: 'opacity 200ms',
            opacity: visible ? 1 : 0,
          }}
        />
      )}

      {/* Panel */}
      <aside style={{
        position: 'fixed', top: 0, right: 0, bottom: 0,
        width: 400, maxWidth: '90vw',
        background: 'var(--bg-card)', borderLeft: '1px solid var(--border)',
        boxShadow: '-16px 0 48px rgba(0,0,0,0.4)',
        transform: visible ? 'translateX(0)' : 'translateX(100%)',
        transition: 'transform 200ms ease',
        zIndex: 51, display: 'flex', flexDirection: 'column',
        overflow: 'hidden',
      }}>
        {/* Header */}
        <div style={{
          display: 'flex', alignItems: 'flex-start', gap: 12,
          padding: '20px 20px 16px', borderBottom: '1px solid var(--border)',
        }}>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{
              fontSize: 10, fontWeight: 700, letterSpacing: '0.08em',
              textTransform: 'uppercase', color: 'var(--text-muted)', marginBottom: 6,
            }}>
              {detail.type}
            </div>
            <h3 style={{
              margin: 0, fontSize: 16, fontWeight: 600, color: 'var(--text)',
              lineHeight: 1.3, wordBreak: 'break-word',
            }}>
              {title}
            </h3>
          </div>
          <button onClick={close} style={{
            width: 32, height: 32, borderRadius: 8,
            border: '1px solid var(--border)', background: 'var(--bg)',
            color: 'var(--text-muted)', cursor: 'pointer', fontSize: 16,
            flexShrink: 0, display: 'flex', alignItems: 'center', justifyContent: 'center',
          }} aria-label="Close detail panel" title="Close detail panel">
            ×
          </button>
        </div>

        {/* Body */}
        <div style={{
          flex: 1, overflowY: 'auto', padding: '8px 20px 24px',
        }}>
          {entries.map(([key, value]) => (
            <FieldRow key={key} label={key} value={value} />
          ))}
        </div>

        {/* Footer actions */}
        {actions.length > 0 && (
          <div style={{
            padding: '12px 20px', borderTop: '1px solid var(--border)',
            display: 'flex', gap: 8,
          }}>
            {actions.map((action) => (
              <button
                key={action.label}
                type="button"
                onClick={action.onClick}
                style={{
                  flex: 1, height: 36, borderRadius: 8,
                  border: action.tone === 'accent'
                    ? '1px solid rgba(88,166,255,0.3)'
                    : '1px solid var(--border)',
                  background: action.tone === 'accent' ? 'rgba(88,166,255,0.12)' : 'var(--bg)',
                  color: action.tone === 'accent' ? 'var(--accent)' : 'var(--text)',
                  cursor: 'pointer', fontSize: 13, fontWeight: 600,
                }}
              >
                {action.label}
              </button>
            ))}
          </div>
        )}
      </aside>
    </>
  );
}
