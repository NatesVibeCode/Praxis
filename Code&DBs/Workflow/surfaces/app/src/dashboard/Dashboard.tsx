import React, { useState, useEffect, useCallback, useRef } from 'react';
import { APP_CONFIG } from '../config';
import praxisSymbol from '../assets/praxis-symbol-inverse.svg';
import { useSystemStatus } from '../workspace/useSystemStatus';
import './dashboard.css';

interface Workflow {
  id: string;
  name: string;
  description?: string;
  definition_type?: string;  // 'operating_model' | 'pipeline' | undefined
  latest_run?: {
    run_id: string;
    spec_name?: string;
    status?: string;
    created_at?: string;
    finished_at?: string;
  };
  has_spec?: boolean;
  invocation_count?: number;
  last_invoked_at?: string;
  is_template?: boolean;
  trigger?: {
    id: string;
    event_type: string;
    enabled: boolean;
    cron_expression?: string;
    last_fired_at?: string;
    fire_count: number;
  } | null;
}

interface DashboardProps {
  onEditWorkflow: (id: string) => void;
  onEditModel: (id: string) => void;
  onViewRun: (runId: string) => void;
  onNewWorkflow: () => void;
  onChat: () => void;
  onDescribe: () => void;
}

function useWorkflows() {
  const [workflows, setWorkflows] = useState<Workflow[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    try {
      const res = await fetch('/api/workflows').then(r => r.ok ? r.json() : null).catch(() => null);
      setWorkflows((res?.workflows ?? []) as Workflow[]);
    } catch { /* silent */ }
    finally { setLoading(false); }
  }, []);

  useEffect(() => {
    refresh();
    const t = setInterval(() => { if (!document.hidden) refresh(); }, 15000);
    return () => clearInterval(t);
  }, [refresh]);

  return { workflows, loading, refresh };
}

function getStatusBadge(wf: Workflow): { label: string; className: string } {
  if (wf.trigger?.enabled && wf.trigger.cron_expression) return { label: 'SCHEDULED', className: 'wf-card__badge--scheduled' };
  if (wf.trigger?.enabled) return { label: 'LIVE', className: 'wf-card__badge--live' };
  if (wf.trigger && !wf.trigger.enabled) return { label: 'PAUSED', className: 'wf-card__badge--paused' };
  if ((wf.invocation_count ?? 0) > 0) return { label: 'ACTIVE', className: 'wf-card__badge--live' };
  return { label: 'READY', className: 'wf-card__badge--draft' };
}

function timeAgo(dateStr: string | undefined): string {
  if (!dateStr) return 'never';
  const diff = Date.now() - new Date(dateStr).getTime();
  if (diff < 60000) return 'just now';
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
  return `${Math.floor(diff / 86400000)}d ago`;
}

function StepChain({ spec }: { spec: any }) {
  if (!spec?.jobs?.length) return null;
  const jobs = spec.jobs as Array<{ label: string; agent: string }>;

  const getStepClass = (agent: string) => {
    if (agent.includes('integration/')) return 'wf-card__step--integration';
    if (agent.includes('review')) return 'wf-card__step--check';
    if (agent.includes('wiring')) return 'wf-card__step--api';
    if (agent.includes('human') || agent.includes('notification')) return 'wf-card__step--human';
    return 'wf-card__step--agent';
  };

  return (
    <div className="wf-card__chain">
      {jobs.map((job, i) => (
        <React.Fragment key={job.label}>
          {i > 0 && <span className="wf-card__arrow">→</span>}
          <span className={`wf-card__step ${getStepClass(job.agent)}`}>
            {job.label.replace(/^ps_\w+_\d+$/, `Step ${i + 1}`).replace(/_/g, ' ')}
          </span>
        </React.Fragment>
      ))}
    </div>
  );
}

function WorkflowCard({ wf, onEdit, onViewRun, onRunNow, onDelete }: {
  wf: Workflow;
  onEdit: () => void;
  onViewRun: () => void;
  onRunNow: () => void;
  onDelete: () => void;
}) {
  const badge = getStatusBadge(wf);

  return (
    <div className="wf-card">
      <div className="wf-card__header">
        <div>
          <div className="wf-card__name">{wf.name}</div>
          {wf.description && <div className="wf-card__desc">{wf.description}</div>}
        </div>
        <span className={`wf-card__badge ${badge.className}`}>{badge.label}</span>
      </div>

      <div className="wf-card__meta">
        {(wf.invocation_count ?? 0) > 0 && <span>{wf.invocation_count} runs</span>}
        {wf.last_invoked_at && <span>Last: {timeAgo(wf.last_invoked_at)}</span>}
        {wf.trigger?.cron_expression && <span>Cron: {wf.trigger.cron_expression}</span>}
        {wf.trigger?.fire_count ? <span>{wf.trigger.fire_count} triggered</span> : null}
      </div>

      <div className="wf-card__actions">
        {wf.latest_run?.run_id && (
          <button type="button" className="wf-card__btn" onClick={onViewRun}>View Results</button>
        )}
        <button type="button" className="wf-card__btn" onClick={onEdit}>Edit</button>
        <button type="button" className="wf-card__btn wf-card__btn--primary" onClick={onRunNow}>Run Now</button>
        <button
          type="button"
          className="wf-card__btn"
          onClick={onDelete}
          style={{ color: 'var(--danger)', borderColor: 'rgba(248,81,73,0.3)' }}
        >
          Delete
        </button>
      </div>
    </div>
  );
}

export function Dashboard({ onEditWorkflow, onEditModel, onViewRun, onNewWorkflow, onChat, onDescribe }: DashboardProps) {
  const { workflows, loading, refresh } = useWorkflows();
  const sys = useSystemStatus();
  const [instanceFiles, setInstanceFiles] = useState<Array<{ id: string; filename: string }>>([]);
  const instanceFileRef = useRef<HTMLInputElement>(null);

  const liveWorkflows = workflows.filter(w => w.trigger?.enabled);
  const savedWorkflows = workflows.filter(w => !w.trigger?.enabled && (w.invocation_count ?? 0) > 0);
  const draftWorkflows = workflows.filter(w => !w.trigger?.enabled && (w.invocation_count ?? 0) === 0);

  useEffect(() => {
    fetch('/api/files?scope=instance')
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (data?.files) setInstanceFiles(data.files);
        else if (Array.isArray(data)) setInstanceFiles(data);
      })
      .catch(() => {});
  }, []);

  const handleRunNow = async (wfId: string) => {
    try {
      await fetch(`/api/trigger/${wfId}`, { method: 'POST' });
      await refresh();
    } catch { /* silent */ }
  };

  const handleDelete = async (wfId: string) => {
    if (!window.confirm('Delete this workflow? This cannot be undone.')) return;
    try {
      const res = await fetch(`/api/workflows/delete/${wfId}`, { method: 'DELETE' });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        console.error('Delete failed:', err);
        return;
      }
      await refresh();
    } catch (err) {
      console.error('Delete error:', err);
    }
  };

  const handleInstanceFileUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    try {
      const content = await new Promise<string>((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => {
          const result = typeof reader.result === 'string' ? reader.result : '';
          const base64 = result.includes(',') ? result.split(',')[1] : result;
          if (base64) resolve(base64);
          else reject(new Error('Failed to read file'));
        };
        reader.onerror = () => reject(reader.error ?? new Error('Failed to read file'));
        reader.readAsDataURL(file);
      });

      const response = await fetch('/api/files', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          filename: file.name,
          content,
          content_type: file.type || 'application/octet-stream',
          scope: 'instance',
        }),
      });

      const data = response.ok ? await response.json() : null;
      if (data?.file?.id && data?.file?.filename) {
        setInstanceFiles(current => [{ id: data.file.id, filename: data.file.filename }, ...current]);
      }
    } catch {
      /* silent */
    } finally {
      event.target.value = '';
    }
  };

  const deleteInstanceFile = async (fileId: string) => {
    try {
      const response = await fetch(`/api/files/${fileId}`, { method: 'DELETE' });
      if (response.ok) {
        setInstanceFiles(current => current.filter(file => file.id !== fileId));
      }
    } catch {
      /* silent */
    }
  };

  return (
    <div className="dash-page">
      {/* Sidebar */}
      <div className="dash-sidebar">
        <div className="dash-sidebar__brand">
          <img className="dash-sidebar__logo" src={praxisSymbol} alt="Praxis symbol" />
          {APP_CONFIG.name}
        </div>

        {liveWorkflows.length > 0 && (
          <>
            <div className="dash-sidebar__section">LIVE</div>
            {liveWorkflows.map(w => (
              <button key={w.id} className="dash-sidebar__item" onClick={() => (w.definition_type === 'operating_model' ? onEditModel : onEditWorkflow)(w.id)}>
                <span className="dash-sidebar__dot dash-sidebar__dot--live" />
                <span className="dash-sidebar__title">{w.name}</span>
                {(w.invocation_count ?? 0) > 0 && <span className="dash-sidebar__count">{w.invocation_count}</span>}
              </button>
            ))}
          </>
        )}

        {(savedWorkflows.length > 0 || draftWorkflows.length > 0) && (
          <>
            <div className="dash-sidebar__section">WORKFLOWS</div>
            {[...savedWorkflows, ...draftWorkflows].map(w => (
              <button key={w.id} className="dash-sidebar__item" onClick={() => (w.definition_type === 'operating_model' ? onEditModel : onEditWorkflow)(w.id)}>
                <span className={`dash-sidebar__dot ${(w.invocation_count ?? 0) > 0 ? 'dash-sidebar__dot--done' : 'dash-sidebar__dot--draft'}`} />
                <span className="dash-sidebar__title">{w.name}</span>
              </button>
            ))}
          </>
        )}

        <div className="dash-sidebar__actions">
          <button className="dash-sidebar__action-btn" onClick={onDescribe}>+ New Operating Model</button>
          <button className="dash-sidebar__action-btn" onClick={onNewWorkflow}>+ Workflow Builder</button>
        </div>

        <div className="dash-sidebar__bottom">
          <div className="dash-sidebar__section">ASK ANYTHING</div>
          <button className="dash-sidebar__ask" onClick={onChat}>
            Ask anything...
          </button>

          {instanceFiles.length > 0 && (
            <>
              <div className="dash-sidebar__section" style={{ marginTop: 16 }}>KNOWLEDGE BASE</div>
              {instanceFiles.map(f => (
                <div key={f.id} className="dash-sidebar__item" style={{ cursor: 'default' }}>
                  <span className="dash-sidebar__title">{f.filename}</span>
                  <button
                    type="button"
                    onClick={() => deleteInstanceFile(f.id)}
                    className="dash-sidebar__count"
                    style={{ cursor: 'pointer', background: 'none', border: 'none', color: 'var(--text-muted)' }}
                  >
                    x
                  </button>
                </div>
              ))}
            </>
          )}
          <button
            type="button"
            className="dash-sidebar__action-btn"
            onClick={() => instanceFileRef.current?.click()}
            style={{ fontSize: 11, opacity: 0.7 }}
          >
            + Add to Knowledge Base
          </button>
          <input ref={instanceFileRef} type="file" hidden onChange={handleInstanceFileUpload} />
        </div>
      </div>

      {/* Main content */}
      <div className="dash-main">
        <div className="dash-content">
          {/* Header */}
          <div className="dash-header">
            <h1 className="dash-header__title">{APP_CONFIG.name} Workflows</h1>
            {!sys.loading && sys.totalRuns24h > 0 && (
              <div className="dash-header__stats">
                <span>{sys.totalRuns24h} workflows today</span>
                {sys.activeRuns > 0 && <span className="dash-header__active">● {sys.activeRuns} running</span>}
              </div>
            )}
          </div>

          {/* Quick actions */}
          <div style={{ display: 'flex', gap: 10, marginBottom: 20 }}>
            <button
              className="wf-card__btn wf-card__btn--primary"
              onClick={onDescribe}
              style={{ padding: '10px 20px', fontSize: 14 }}
            >
              Describe It
            </button>
            <button
              className="wf-card__btn"
              onClick={onNewWorkflow}
              style={{ padding: '10px 20px', fontSize: 14 }}
            >
              Start from Scratch
            </button>
          </div>

          {/* Workflow cards */}
          {loading ? (
            <div className="dash-loading">Loading workflows...</div>
          ) : (
            <>
              {/* Live workflows first */}
              {liveWorkflows.map(w => (
                <WorkflowCard
                  key={w.id}
                  wf={w}
                  onEdit={() => (w.definition_type === 'operating_model' ? onEditModel : onEditWorkflow)(w.id)}
                  onViewRun={() => w.latest_run?.run_id && onViewRun(w.latest_run.run_id)}
                  onRunNow={() => handleRunNow(w.id)}
                  onDelete={() => handleDelete(w.id)}
                />
              ))}

              {/* Active workflows */}
              {savedWorkflows.map(w => (
                <WorkflowCard
                  key={w.id}
                  wf={w}
                  onEdit={() => (w.definition_type === 'operating_model' ? onEditModel : onEditWorkflow)(w.id)}
                  onViewRun={() => w.latest_run?.run_id && onViewRun(w.latest_run.run_id)}
                  onRunNow={() => handleRunNow(w.id)}
                  onDelete={() => handleDelete(w.id)}
                />
              ))}

              {/* Draft workflows */}
              {draftWorkflows.map(w => (
                <WorkflowCard
                  key={w.id}
                  wf={w}
                  onEdit={() => (w.definition_type === 'operating_model' ? onEditModel : onEditWorkflow)(w.id)}
                  onViewRun={() => w.latest_run?.run_id && onViewRun(w.latest_run.run_id)}
                  onRunNow={() => handleRunNow(w.id)}
                  onDelete={() => handleDelete(w.id)}
                />
              ))}

              {/* Recent runs — clickable, shown first */}
              {(() => {
                const visibleRuns = sys.recentRuns.filter(r =>
                  !r.spec_name?.startsWith('compile_') &&
                  !r.spec_name?.startsWith('fix_bugs') &&
                  !r.spec_name?.startsWith('hardening_')
                );
                return visibleRuns.length > 0 && (
                <div className="dash-activity" style={{ marginBottom: 20 }}>
                  <h2 className="dash-activity__title">Recent Runs</h2>
                  {visibleRuns.slice(0, 10).map(r => (
                    <button
                      key={r.run_id}
                      className="dash-activity__item"
                      onClick={() => onViewRun(r.run_id)}
                      style={{ cursor: 'pointer', width: '100%', textAlign: 'left', background: 'none', border: 'none', font: 'inherit', color: 'inherit', padding: '8px 0', display: 'flex', alignItems: 'center', gap: 8 }}
                    >
                      <span className={`dash-activity__dot dash-activity__dot--${r.status}`} />
                      <span className="dash-activity__name" style={{ flex: 1 }}>{
                        (r.spec_name || r.run_id.slice(0, 12))
                          .replace(/[_-]+/g, ' ')
                          .replace(/\b\w/g, (c: string) => c.toUpperCase())
                      }</span>
                      <span className="dash-activity__status">{r.status}</span>
                      <span className="dash-activity__time">{r.created_at ? timeAgo(r.created_at) : ''}</span>
                    </button>
                  ))}
                </div>
              );
              })()}

              {/* Create CTA removed — action buttons are at the top */}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
