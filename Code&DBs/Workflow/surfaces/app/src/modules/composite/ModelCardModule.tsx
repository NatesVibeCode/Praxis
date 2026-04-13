import React from 'react';
import { QuadrantProps } from '../types';
import { useModuleData } from '../../hooks/useModuleData';
import { useSlice } from '../../hooks/useSlice';
import { LoadingSkeleton } from '../../primitives/LoadingSkeleton';
import { world } from '../../world';

interface Model {
  id: string;
  provider: string;
  name: string;
  status: 'active' | 'degraded' | 'offline';
  capabilities?: string[];
}

const statusColors: Record<string, string> = {
  active: 'var(--success, #3fb950)',
  degraded: 'var(--warning, #d29922)',
  offline: 'var(--danger, #f85149)',
};

function ModelCardModule({ config }: QuadrantProps) {
  void config;
  const { data, loading, error } = useModuleData<Model[]>('models', { refreshInterval: 30000 });
  const selectedModel = useSlice(world, 'shared.selected_model') as { id?: string } | null;

  function selectModel(model: Model) {
    world.set('shared.selected_model', { id: model.id, provider: model.provider, name: model.name });
    window.dispatchEvent(new CustomEvent('module-selection', {
      detail: { type: 'model', data: model },
    }));
  }

  return (
    <div style={{
      display: 'flex', flexDirection: 'column', gap: 'var(--space-sm, 8px)',
      padding: 'var(--space-md, 16px)', width: '100%', height: '100%',
      boxSizing: 'border-box', backgroundColor: 'var(--bg-card, #161b22)',
      borderRadius: 'var(--radius, 8px)',
    }}>
      <div style={{ color: 'var(--text, #c9d1d9)', fontSize: '15px', fontWeight: 600 }}>
        Models
      </div>

      {loading && !data && (
        <LoadingSkeleton lines={4} height={18} widths={['100%', '88%', '92%', '74%']} />
      )}

      {error && (
        <div style={{ color: 'var(--danger, #f85149)', fontSize: '13px' }}>{error}</div>
      )}

      <div style={{
        display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(180px, 1fr))',
        gap: 'var(--space-sm, 8px)', flex: 1, overflowY: 'auto', alignContent: 'start',
      }}>
        {data?.map(model => {
          const isSelected = selectedModel?.id === model.id;
          return (
          <div
            key={model.id}
            onClick={() => selectModel(model)}
            style={{
              backgroundColor: 'var(--bg, #0d1117)',
              border: isSelected ? '1px solid var(--accent, #58a6ff)' : '1px solid var(--border, #30363d)',
              borderRadius: 'var(--radius, 8px)',
              padding: '10px 12px', cursor: 'pointer',
              transition: 'border-color 0.15s',
              boxShadow: isSelected ? '0 0 0 1px rgba(88, 166, 255, 0.2)' : undefined,
            }}
            onMouseEnter={e => (e.currentTarget.style.borderColor = 'var(--accent, #58a6ff)')}
            onMouseLeave={e => (e.currentTarget.style.borderColor = isSelected ? 'var(--accent, #58a6ff)' : 'var(--border, #30363d)')}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '6px', marginBottom: '4px' }}>
              <span style={{
                width: '8px', height: '8px', borderRadius: '50%',
                backgroundColor: statusColors[model.status] ?? statusColors.offline,
                flexShrink: 0,
              }} />
              <span style={{
                color: 'var(--text, #c9d1d9)', fontSize: '13px', fontWeight: 600,
                overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
              }}>
                {model.name}
              </span>
            </div>
            <div style={{ color: 'var(--text-muted, #8b949e)', fontSize: '11px', marginBottom: '6px' }}>
              {model.provider}
            </div>
            {model.capabilities && model.capabilities.length > 0 && (
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                {model.capabilities.map(cap => (
                  <span key={cap} style={{
                    backgroundColor: 'rgba(88, 166, 255, 0.1)',
                    color: 'var(--accent, #58a6ff)',
                    padding: '1px 6px', borderRadius: '4px', fontSize: '10px', fontWeight: 500,
                  }}>
                    {cap}
                  </span>
                ))}
              </div>
            )}
          </div>
          );
        })}
      </div>
    </div>
  );
}

export default ModelCardModule;
