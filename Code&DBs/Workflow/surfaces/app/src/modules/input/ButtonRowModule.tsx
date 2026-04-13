import React, { useState } from 'react';
import { QuadrantProps } from '../types';
import { world } from '../../world';

interface ActionConfig {
  label: string;
  variant?: 'primary' | 'danger' | 'default';
  endpoint?: string;
  worldWrite?: { path: string; value: any };
  createObject?: { typeId: string; defaults?: Record<string, unknown> };
}

interface ButtonRowConfig {
  actions?: ActionConfig[];
}

export const ButtonRowModule: React.FC<QuadrantProps> = ({ config: rawConfig }) => {
  const config = (rawConfig || {}) as ButtonRowConfig;
  const [loadingAction, setLoadingAction] = useState<number | null>(null);
  const actions: ActionConfig[] = config.actions || [];

  const handleAction = async (action: ActionConfig, index: number) => {
    if (action.worldWrite) {
      world.set(action.worldWrite.path, action.worldWrite.value);
    }

    if (action.createObject) {
      const name = window.prompt('Name:');
      if (!name) return;
      setLoadingAction(index);
      try {
        const res = await fetch('/api/objects', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            type_id: action.createObject.typeId,
            properties: { name, ...action.createObject.defaults },
          }),
        });
        if (!res.ok) throw new Error(`${res.status}`);
        const created = await res.json();
        if (created.object_id) {
          world.set('shared.lastCreatedObject', created.object_id);
        }
      } catch (err) {
        console.error('Create object error:', err);
      } finally {
        setLoadingAction(null);
      }
      return;
    }

    if (action.endpoint) {
      setLoadingAction(index);
      try {
        await fetch(action.endpoint, { method: 'POST' });
      } catch (err) {
        console.error('Action error:', err);
      } finally {
        setLoadingAction(null);
      }
    }
  };

  if (!actions.length) {
    return (
      <div style={{ padding: 'var(--space-md, 16px)', color: 'var(--text-muted, #8b949e)', fontSize: '14px', fontFamily: 'var(--font-sans, sans-serif)' }}>
        No actions configured.
      </div>
    );
  }

  const getStyleForVariant = (variant?: 'primary' | 'danger' | 'default') => {
    const baseStyle: React.CSSProperties = {
      padding: '8px 16px',
      borderRadius: 'var(--radius, 8px)',
      fontSize: '14px',
      fontWeight: 'bold',
      cursor: 'pointer',
      fontFamily: 'var(--font-sans, sans-serif)'
    };

    switch (variant) {
      case 'primary':
        return {
          ...baseStyle,
          backgroundColor: 'var(--accent, #58a6ff)',
          color: '#ffffff',
          border: 'none',
        };
      case 'danger':
        return {
          ...baseStyle,
          backgroundColor: 'var(--danger, #f85149)',
          color: '#ffffff',
          border: 'none',
        };
      case 'default':
      default:
        return {
          ...baseStyle,
          backgroundColor: 'transparent',
          color: 'var(--text, #c9d1d9)',
          border: '1px solid var(--border, #30363d)',
        };
    }
  };

  return (
    <div style={{ 
      display: 'flex', 
      gap: '12px', 
      padding: 'var(--space-md, 16px)', 
      width: '100%', 
      height: '100%', 
      alignItems: 'center', 
      boxSizing: 'border-box',
      flexWrap: 'wrap'
    }}>
      {actions.map((action, i) => (
        <button
          key={i}
          onClick={() => handleAction(action, i)}
          disabled={loadingAction === i}
          style={{
            ...getStyleForVariant(action.variant),
            opacity: loadingAction === i ? 0.7 : 1
          }}
        >
          {loadingAction === i ? '...' : action.label}
        </button>
      ))}
    </div>
  );
};
