import React, { useEffect, useMemo } from 'react';
import { world } from '../world';
import { useSlice } from '../hooks/useSlice';
import { emitPraxisOpenTab } from './events';
import type { SourceOption } from './manifest';
import { activeSourceId } from '../modules/sourceBindings';
import { SourceChip, type SourceChipTone } from '../primitives';

interface SourceOptionPillsProps {
  options: SourceOption[];
}

function familyTone(option: SourceOption): SourceChipTone {
  if (option.family === 'connected') return 'ok';
  if (option.family === 'external') return 'warn';
  if (option.family === 'reference') return 'live';
  return 'default';
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
    <div className="prx-button-row">
      {options.map((option) => (
        <SourceChip
          key={option.id}
          tone={familyTone(option)}
          active={option.id === activeId}
          label={option.label}
          subtitle={subtitle(option)}
          onClick={() => handleClick(option)}
          title={option.description || option.label}
          aria-pressed={option.id === activeId}
        />
      ))}
    </div>
  );
}
