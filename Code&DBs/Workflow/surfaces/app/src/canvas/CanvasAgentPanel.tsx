import React, { useCallback, useMemo, useState } from 'react';
import { CanvasGlyph } from './CanvasGlyph';
import { useAgentRegistry } from '../shared/hooks/useAgentRegistry';
import { AgentRegistryRow } from '../shared/types';

interface DraftAgent {
  agent_principal_ref: string;
  title: string;
  description: string;
  system_prompt_template: string;
}

const EMPTY_DRAFT: DraftAgent = {
  agent_principal_ref: '',
  title: '',
  description: '',
  system_prompt_template: '',
};

async function _json(resp: Response): Promise<any> {
  let body: any = null;
  try {
    body = await resp.json();
  } catch {
    body = null;
  }
  if (!resp.ok) throw new Error(body?.error || body?.detail || `HTTP ${resp.status}`);
  return body;
}

async function postAgent(draft: DraftAgent): Promise<void> {
  await _json(await fetch('/api/operations', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      operation: 'agent_principal.register',
      payload: {
        agent_principal_ref: draft.agent_principal_ref.trim(),
        title: draft.title.trim(),
        description: draft.description.trim(),
        system_prompt_template: draft.system_prompt_template.trim(),
        visibility: 'visible',
        builder_category: 'custom',
        status: 'active',
      }
    }),
  }));
}

function AgentRow({ agent, onClick }: { agent: AgentRegistryRow, onClick: () => void }) {
  return (
    <div
      className="canvas-catalog-item canvas-catalog-item--draggable"
      onClick={onClick}
    >
      <div className="canvas-catalog-item__icon">
        <CanvasGlyph type={(agent.icon_hint as any) || 'build'} size={24} />
      </div>
      <div className="canvas-catalog-item__body">
        <div className="canvas-catalog-item__title">{agent.title}</div>
        {agent.description && (
          <div className="canvas-catalog-item__summary">{agent.description}</div>
        )}
      </div>
    </div>
  );
}

export function CanvasAgentPanel() {
  const [open, setOpen] = useState(false);
  const { agents, loading, reload } = useAgentRegistry();
  const [addOpen, setAddOpen] = useState(false);
  const [draft, setDraft] = useState<DraftAgent>(EMPTY_DRAFT);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const visibleAgents = useMemo(() => {
    const visible = agents.filter(a => a.visibility === 'visible');
    const builtins = visible.filter(a => a.builder_category === 'builtin');
    const customs = visible.filter(a => a.builder_category !== 'builtin');
    return [...builtins, ...customs];
  }, [agents]);

  const handleCreate = useCallback(async () => {
    setSaving(true);
    setError(null);
    try {
      await postAgent(draft);
      setDraft(EMPTY_DRAFT);
      setAddOpen(false);
      await reload();
    } catch (e: any) {
      setError(e?.message || 'Failed to create agent');
    } finally {
      setSaving(false);
    }
  }, [draft, reload]);

  return (
    <div className="canvas-dock-panel">
      <button
        type="button"
        className="canvas-dock-panel__header"
        onClick={() => setOpen(!open)}
        aria-expanded={open}
      >
        <span>Agents</span>
        <span className="canvas-dock-panel__badge">{visibleAgents.length}</span>
      </button>

      {open && (
        <div className="canvas-dock-panel__body">
          {loading && visibleAgents.length === 0 ? (
            <div className="canvas-dock-panel__empty">Loading...</div>
          ) : (
            <div className="canvas-catalog-grid">
              {visibleAgents.map((agent) => (
                <AgentRow key={agent.agent_principal_ref} agent={agent} onClick={() => {}} />
              ))}
            </div>
          )}

          <div style={{ marginTop: 12 }}>
            {!addOpen ? (
              <button
                type="button"
                className="canvas-dock-form__btn canvas-dock-form__btn--secondary"
                onClick={() => setAddOpen(true)}
              >
                + New agent
              </button>
            ) : (
              <div className="canvas-dock-form">
                <input
                  className="canvas-dock-form__input"
                  placeholder="agent.custom.my_expert"
                  value={draft.agent_principal_ref}
                  onChange={(e) => setDraft({ ...draft, agent_principal_ref: e.target.value })}
                />
                <input
                  className="canvas-dock-form__input"
                  placeholder="Title"
                  value={draft.title}
                  onChange={(e) => setDraft({ ...draft, title: e.target.value })}
                />
                <input
                  className="canvas-dock-form__input"
                  placeholder="Description"
                  value={draft.description}
                  onChange={(e) => setDraft({ ...draft, description: e.target.value })}
                />
                <textarea
                  className="canvas-dock-form__textarea"
                  placeholder="System prompt template..."
                  value={draft.system_prompt_template}
                  onChange={(e) => setDraft({ ...draft, system_prompt_template: e.target.value })}
                  rows={3}
                />
                
                {error && <div className="canvas-dock-form__error">{error}</div>}

                <div className="canvas-dock-form__row">
                  <button
                    className="canvas-dock-form__btn"
                    onClick={handleCreate}
                    disabled={saving || !draft.agent_principal_ref || !draft.title}
                  >
                    {saving ? 'Creating...' : 'Create Agent'}
                  </button>
                  <button
                    className="canvas-dock-form__btn canvas-dock-form__btn--secondary"
                    onClick={() => {
                      setAddOpen(false);
                      setDraft(EMPTY_DRAFT);
                      setError(null);
                    }}
                  >
                    Cancel
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
