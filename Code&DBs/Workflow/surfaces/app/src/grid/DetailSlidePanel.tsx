import React, { useEffect, useState } from 'react';
import { emitPraxisOpenTab } from '../praxis/events';
import './DetailSlidePanel.css';

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
    <span
      className="detail-slide-panel__chip detail-slide-panel__chip--status"
      style={{ '--chip-color': color } as React.CSSProperties}
    >
      {value}
    </span>
  );
}

function SeverityBadge({ value }: { value: string }) {
  const color = SEVERITY_COLORS[value] ?? 'var(--text-muted)';
  return (
    <span
      className="detail-slide-panel__chip detail-slide-panel__chip--severity"
      style={{ '--badge-color': color } as React.CSSProperties}
    >
      {value}
    </span>
  );
}

function formatMoney(value: unknown): string | null {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      maximumFractionDigits: value >= 100 ? 0 : 2,
    }).format(value);
  }

  if (typeof value === 'string') {
    const normalized = value.replace(/[$,]/g, '').trim();
    if (!normalized) return null;
    const numeric = Number(normalized);
    if (Number.isFinite(numeric)) {
      return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        maximumFractionDigits: numeric >= 100 ? 0 : 2,
      }).format(numeric);
    }
  }

  return null;
}

function renderValue(label: string, value: unknown): React.ReactNode {
  const strVal = value == null ? '—' : String(value);
  const lowerLabel = label.toLowerCase();
  const isStatus = lowerLabel === 'status';
  const isSeverity = lowerLabel === 'severity';
  const isMoney = lowerLabel.includes('value') || lowerLabel.includes('cost') || lowerLabel.includes('price');
  const moneyValue = isMoney ? formatMoney(value) : null;

  if (isStatus) return <StatusChip value={strVal} />;
  if (isSeverity) return <SeverityBadge value={strVal} />;
  if (moneyValue) return moneyValue;
  if (typeof value === 'boolean') return value ? 'Yes' : 'No';
  if (Array.isArray(value)) {
    if (value.length === 0) return '—';
    return value.map((item) => String(item)).join(', ');
  }
  if (value && typeof value === 'object') {
    return JSON.stringify(value, null, 2);
  }
  return strVal || '—';
}

function FieldRow({ label, value }: { label: string; value: unknown }) {
  const strVal = value == null ? '—' : String(value);
  const renderedValue = renderValue(label, value);
  const renderedText = typeof renderedValue === 'string' ? renderedValue : strVal;
  const isLong = renderedText.length > 80 || renderedText.includes('\n');

  return (
    <div className={`detail-slide-panel__field ${isLong ? 'detail-slide-panel__field--stacked' : ''}`}>
      <span className="detail-slide-panel__label">
        {label.replace(/_/g, ' ')}
      </span>
      <span className="detail-slide-panel__value">{renderedValue}</span>
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
  const keyFacts = [
    detail.data.status ? String(detail.data.status) : null,
    detail.data.severity ? String(detail.data.severity) : null,
    detail.data.manifest_id ? `Manifest ${String(detail.data.manifest_id)}` : null,
    detail.data.workflow_id ? `Workflow ${String(detail.data.workflow_id)}` : null,
    detail.data.run_id ? `Run ${String(detail.data.run_id)}` : null,
  ].filter((value): value is string => Boolean(value));

  // Determine title from data
  const title = String(
    detail.data.title ?? detail.data.name ?? detail.data.object_id ?? detail.data.bug_id ?? 'Details'
  );
  const actions = resolvePanelActions(detail);

  return (
    <>
      {visible && (
        <div
          onClick={close}
          className={`detail-slide-panel__backdrop${visible ? ' detail-slide-panel__backdrop--open' : ''}`}
        />
      )}

      <aside className={`detail-slide-panel${visible ? ' detail-slide-panel--open' : ''}`}>
        <div className="detail-slide-panel__header">
          <div className="detail-slide-panel__heading">
            <div className="detail-slide-panel__kicker">
              {detail.type}
            </div>
            <h3 className="detail-slide-panel__title">
              {title}
            </h3>
            <div className="detail-slide-panel__summary">
              <span className="detail-slide-panel__summary-chip">{entries.length} fields</span>
              {keyFacts.slice(0, 3).map((fact, index) => (
                <span key={`${fact}-${index}`} className="detail-slide-panel__summary-chip detail-slide-panel__summary-chip--muted">
                  {fact}
                </span>
              ))}
            </div>
          </div>
          <button
            onClick={close}
            type="button"
            className="detail-slide-panel__close"
            aria-label="Close detail panel"
            title="Close detail panel"
          >
            ×
          </button>
        </div>

        <div className="detail-slide-panel__body">
          {entries.map(([key, value]) => (
            <FieldRow key={key} label={key} value={value} />
          ))}
        </div>

        {actions.length > 0 && (
          <div className="detail-slide-panel__footer">
            {actions.map((action) => (
              <button
                key={action.label}
                type="button"
                onClick={action.onClick}
                className={`detail-slide-panel__action${action.tone === 'accent' ? ' detail-slide-panel__action--accent' : ''}`}
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
