import React from 'react';
import { AgentRegistryRow } from '../shared/types';

interface Props {
  value: string | null | undefined;
  agents: AgentRegistryRow[];
  onChange: (ref: string | null) => void;
}

export function CanvasAgentPicker({ value, agents, onChange }: Props) {
  const visibleAgents = agents.filter(a => a.visibility === 'visible' || a.agent_principal_ref === value);

  return (
    <div className="canvas-run-contract__row">
      <span className="canvas-run-contract__label">Agent</span>
      <select
        className="canvas-dock-form__input"
        style={{ flex: 1, padding: '2px 4px', fontSize: '11px', height: 'auto', backgroundColor: 'var(--prx-surface-sunken)', color: 'var(--prx-text-body)' }}
        value={value || ''}
        onChange={(e) => onChange(e.target.value || null)}
      >
        <option value="">Inferred</option>
        {visibleAgents.map(a => (
          <option key={a.agent_principal_ref} value={a.agent_principal_ref}>
            {a.title} ({a.agent_principal_ref})
          </option>
        ))}
      </select>
    </div>
  );
}
