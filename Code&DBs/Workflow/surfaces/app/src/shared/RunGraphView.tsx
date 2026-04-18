import React from 'react';
import type { RunGraph as RunGraphData } from '../dashboard/useLiveRunSnapshot';
import './run-graph.css';

function humanizeLabel(label: string): string {
  if (!label) {
    return 'Unnamed Step';
  }
  return label
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function getGraphNodeVariant(status: string): 'succeeded' | 'running' | 'pending' | 'failed' {
  if (status === 'succeeded') return 'succeeded';
  if (status === 'running' || status === 'claimed') return 'running';
  if (status === 'failed' || status === 'dead_letter' || status === 'cancelled') return 'failed';
  return 'pending';
}

export function RunGraphView({
  graph,
  onSelectJob,
}: {
  graph: RunGraphData;
  onSelectJob?: (label: string) => void;
}) {
  const depths: Record<string, number> = {};
  const inDegree: Record<string, number> = {};
  const outgoing: Record<string, string[]> = {};

  for (const node of graph.nodes) {
    depths[node.id] = 0;
    inDegree[node.id] = 0;
    outgoing[node.id] = [];
  }
  for (const edge of graph.edges) {
    inDegree[edge.to] = (inDegree[edge.to] || 0) + 1;
    outgoing[edge.from] = outgoing[edge.from] || [];
    outgoing[edge.from].push(edge.to);
  }

  const queue = graph.nodes.filter((node) => (inDegree[node.id] || 0) === 0).map((node) => node.id);
  while (queue.length > 0) {
    const current = queue.shift()!;
    for (const next of outgoing[current] || []) {
      depths[next] = Math.max(depths[next] || 0, (depths[current] || 0) + 1);
      inDegree[next] -= 1;
      if (inDegree[next] === 0) {
        queue.push(next);
      }
    }
  }

  const maxDepth = Math.max(0, ...Object.values(depths));
  const columns = Array.from({ length: maxDepth + 1 }, () => [] as RunGraphData['nodes']);
  for (const node of graph.nodes) {
    columns[depths[node.id] || 0].push(node);
  }

  return (
    <div className="shared-run-graph">
      {columns.map((column, columnIndex) => (
        <React.Fragment key={columnIndex}>
          {columnIndex > 0 && (
            <div className="shared-run-graph__edge">
              <svg width="32" height="2" style={{ display: 'block' }}>
                <line x1="0" y1="1" x2="32" y2="1" stroke="var(--border, rgba(255,255,255,0.12))" strokeWidth="1.5" />
                <polygon points="28,0 32,1 28,2" fill="var(--text-muted, rgba(255,255,255,0.45))" opacity="0.5" />
              </svg>
            </div>
          )}
          <div className="shared-run-graph__column">
            {column.map((node) => {
              const variant = getGraphNodeVariant(node.status);
              const subtitle = node.loop
                ? `${node.loop.succeeded}/${node.loop.count} done`
                : (node.error_code
                    ? node.error_code.replace(/^workflow_submission\./, '')
                    : node.status);
              return (
                <button
                  key={node.id}
                  type="button"
                  className={`shared-run-graph__node shared-run-graph__node--${variant}`}
                  onClick={() => onSelectJob?.(node.label)}
                >
                  <span className="shared-run-graph__node-title">{humanizeLabel(node.label)}</span>
                  <span className="shared-run-graph__node-sub">
                    {subtitle}
                    {node.duration_ms ? ` · ${(node.duration_ms / 1000).toFixed(1)}s` : ''}
                  </span>
                </button>
              );
            })}
          </div>
        </React.Fragment>
      ))}
    </div>
  );
}
