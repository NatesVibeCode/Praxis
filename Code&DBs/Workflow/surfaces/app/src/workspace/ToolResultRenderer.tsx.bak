import React from 'react';

interface ToolResult {
  type: 'table' | 'cards' | 'status' | 'text' | 'error';
  data: any;
  selectable?: boolean;
  summary?: string;
}

interface ToolResultRendererProps {
  result: ToolResult;
  onSelectItems?: (items: any[]) => void;
  selectedItems?: Set<string>;
}

export function ToolResultRenderer({ result, onSelectItems, selectedItems }: ToolResultRendererProps) {
  if (result.type === 'error') {
    return (
      <div className="ws-tool-error">
        <span className="ws-tool-error__icon">x</span>
        <span>{result.data?.message ?? 'Tool error'}</span>
      </div>
    );
  }

  if (result.type === 'text') {
    return <div className="ws-tool-text">{result.data?.content ?? ''}</div>;
  }

  if (result.type === 'status') {
    const d = result.data ?? {};
    const statusColor = d.status === 'succeeded' ? 'var(--success)'
                      : d.status === 'failed' ? 'var(--danger)'
                      : d.status === 'running' ? 'var(--accent)'
                      : 'var(--text-muted)';
    return (
      <div className="ws-tool-status">
        <div className="ws-tool-status__header">
          <span className="ws-tool-status__dot" style={{ background: statusColor }} />
          <span className="ws-tool-status__name">{d.spec_name || d.run_id}</span>
          <span className="ws-tool-status__badge" style={{ color: statusColor }}>{d.status}</span>
        </div>
        {d.total_jobs != null && (
          <div className="ws-tool-status__progress">
            {d.completed_jobs ?? 0} / {d.total_jobs} jobs
          </div>
        )}
        {Array.isArray(d.jobs) && d.jobs.length > 0 && (
          <div className="ws-tool-status__jobs">
            {d.jobs.map((j: any, i: number) => (
              <div key={i} className="ws-tool-status__job">
                <span style={{ color: j.status === 'succeeded' ? 'var(--success)' : j.status === 'failed' ? 'var(--danger)' : 'var(--text-muted)' }}>
                  {j.status === 'succeeded' ? 'ok' : j.status === 'failed' ? 'x' : '-'}
                </span>
                <span>{j.label}</span>
                {j.duration && <span className="ws-tool-status__dur">{j.duration}</span>}
              </div>
            ))}
          </div>
        )}
      </div>
    );
  }

  if (result.type === 'table') {
    const { columns, rows } = result.data ?? {};
    if (!columns || !rows || rows.length === 0) {
      return <div className="ws-tool-text" style={{ color: 'var(--text-muted)' }}>No results.</div>;
    }

    return (
      <div className="ws-tool-table-wrap">
        <table className="ws-tool-table">
          <thead>
            <tr>
              {result.selectable && (
                <th className="ws-tool-table__check">
                  <input type="checkbox" onChange={(e) => {
                    if (e.target.checked) onSelectItems?.(rows);
                    else onSelectItems?.([]);
                  }} />
                </th>
              )}
              {columns.map((col: any) => (
                <th key={col.key}>{col.label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row: any, i: number) => {
              const rowKey = JSON.stringify(row);
              const isSelected = selectedItems?.has(rowKey);
              return (
                <tr key={i} className={isSelected ? 'ws-tool-table__row--selected' : ''}>
                  {result.selectable && (
                    <td className="ws-tool-table__check">
                      <input
                        type="checkbox"
                        checked={isSelected ?? false}
                        onChange={() => {
                          // Toggle this row in selection
                          onSelectItems?.([row]);
                        }}
                      />
                    </td>
                  )}
                  {columns.map((col: any) => (
                    <td key={col.key}>{String(row[col.key] ?? '')}</td>
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
        <div className="ws-tool-table__footer">{rows.length} result{rows.length !== 1 ? 's' : ''}</div>
      </div>
    );
  }

  if (result.type === 'cards') {
    const items = result.data?.items ?? [];
    return (
      <div className="ws-tool-cards">
        {items.map((item: any, i: number) => (
          <div key={i} className="ws-tool-card">
            {Object.entries(item).map(([k, v]) => (
              <div key={k} className="ws-tool-card__field">
                <span className="ws-tool-card__key">{k}</span>
                <span className="ws-tool-card__value">{String(v)}</span>
              </div>
            ))}
          </div>
        ))}
      </div>
    );
  }

  return <div className="ws-tool-text">{result.summary ?? JSON.stringify(result.data)}</div>;
}
