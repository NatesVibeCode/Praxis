import React, { useEffect, useMemo } from 'react';
import { world } from '../world';
import { useSlice } from '../hooks/useSlice';
import { emitPraxisOpenTab } from './events';
import type { SourceOption } from './manifest';
import { activeSourceId } from '../modules/sourceBindings';

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
  const activeSource = useSlice(world, 'shared.active_source_option');
  const activeId = activeSourceId(activeSource);
  const defaultOption = useMemo(
    () => (
      options.find((option) => option.activation !== 'configure' && option.availability !== 'setup_required')
      ?? options[0]
    ),
    [options],
  );

  useEffect(() => {
    if (!defaultOption) return;
    if (activeId && options.some((option) => option.id === activeId)) return;
    world.set('shared.active_source_option', defaultOption);
  }, [activeId, defaultOption, options]);

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
    <div className="app-shell__surface-chip-list">
      {options.map((option) => {
        const color = familyColor(option);
        const selected = option.id === activeId;
        return (
          <button
            key={option.id}
            type="button"
            onClick={() => handleClick(option)}
            title={option.description || option.label}
            aria-pressed={selected}
            className={[
              'app-shell__surface-chip',
              selected ? 'app-shell__surface-chip--active' : '',
            ].filter(Boolean).join(' ')}
            style={{ borderColor: `${color}33`, background: `${color}14` }}
          >
            <span className="app-shell__surface-chip-dot" style={{ background: color }} />
            <span className="app-shell__surface-chip-copy">
              <span className="app-shell__surface-chip-label">{option.label}</span>
              <span className="app-shell__surface-chip-subtitle">{subtitle(option)}</span>
            </span>
          </button>
        );
      })}
    </div>
  );
}
