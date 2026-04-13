import React from 'react';
import { world } from '../world';
import { emitPraxisOpenTab } from './events';
import type { SourceOption } from './manifest';

interface SourceOptionPillsProps {
  options: SourceOption[];
}

function familyColor(option: SourceOption): string {
  if (option.family === 'connected') return 'var(--success)';
  if (option.family === 'external') return 'var(--warning)';
  if (option.family === 'reference') return 'var(--accent)';
  return 'var(--text-muted)';
}

function subtitle(option: SourceOption): string {
  if (option.availability === 'setup_required') return 'Setup required';
  if (option.availability === 'preview') return 'Preview';
  return option.kind.replace(/_/g, ' ');
}

export function SourceOptionPills({ options }: SourceOptionPillsProps) {
  if (options.length === 0) return null;

  const handleClick = (option: SourceOption) => {
    if (option.activation === 'configure' || option.availability === 'setup_required') {
      emitPraxisOpenTab({
        kind: 'build',
        intent: option.setup_intent ?? `Set up ${option.label}`,
      });
      return;
    }

    world.set('shared.active_source_option', option);
    window.dispatchEvent(new CustomEvent('module-selection', {
      detail: {
        type: `source:${option.kind}`,
        data: option,
      },
    }));
  };

  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
      {options.map((option) => {
        const color = familyColor(option);
        return (
          <button
            key={option.id}
            type="button"
            onClick={() => handleClick(option)}
            title={option.description || option.label}
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: 8,
              minHeight: 30,
              padding: '6px 10px',
              borderRadius: 999,
              border: `1px solid ${color}33`,
              background: `${color}14`,
              color: 'var(--text)',
              cursor: 'pointer',
            }}
          >
            <span style={{
              width: 8,
              height: 8,
              borderRadius: '50%',
              background: color,
              flexShrink: 0,
            }} />
            <span style={{ fontSize: 12, fontWeight: 600 }}>{option.label}</span>
            <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>{subtitle(option)}</span>
          </button>
        );
      })}
    </div>
  );
}
