import React, { useCallback, useEffect, useRef, useState } from 'react';
import { APP_CONFIG } from '../config';
import praxisSymbol from '../assets/praxis-symbol-inverse.svg';
import { MoonWorkflowSilhouette } from './MoonWorkflowSilhouette';
import './dashboard.css';

interface Workflow {
  id: string;
  name: string;
  description?: string;
  definition_type?: string;
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
  dashboard_bucket?: 'live' | 'saved' | 'draft';
  dashboard_badge?: {
    label: string;
    tone: string;
    class_name: string;
  };
  trigger?: {
    id: string;
    event_type: string;
    enabled: boolean;
    cron_expression?: string;
    last_fired_at?: string;
    fire_count: number;
  } | null;
}

interface RecentRun {
  run_id: string;
  spec_name: string;
  status: 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled';
  total_jobs: number;
  completed_jobs: number;
  total_cost: number;
  created_at: string | null;
  finished_at: string | null;
}

interface DashboardSnapshot {
  generated_at: string;
  summary: {
    workflow_counts: {
      total: number;
      live: number;
      saved: number;
      draft: number;
    };
    health: {
      readiness: string;
      label: string;
      tone: 'neutral' | 'healthy' | 'warning' | 'danger';
      copy: string;
    };
    runs_24h: number;
    active_runs: number;
    pass_rate_24h: number | null;
    total_cost_24h: number;
    top_agent: string | null;
    models_online: number;
    queue: {
      depth: number;
      status: 'ok' | 'warning' | 'critical' | 'unknown';
      utilization_pct: number;
      pending: number;
      ready: number;
      claimed: number;
      running: number;
      error: string | null;
    };
  };
  sections: Array<{
    key: 'live' | 'saved' | 'draft';
    count: number;
    workflow_ids: string[];
  }>;
  workflows: Workflow[];
  recent_runs: RecentRun[];
}

interface DashboardProps {
  onEditWorkflow: (id: string) => void;
  onEditModel: (id: string) => void;
  onViewRun: (runId: string) => void;
  onNewWorkflow: () => void;
  onChat: () => void;
  onDescribe: () => void;
  onOpenCosts: () => void;
}

interface WorkflowSection {
  key: string;
  title: string;
  eyebrow: string;
  description: string;
  emptyTitle: string;
  emptyCopy: string;
  emptyAction: string;
  count: number;
  workflows: Workflow[];
  tone: 'live' | 'saved' | 'draft';
}

function useDashboardSnapshot() {
  const [snapshot, setSnapshot] = useState<DashboardSnapshot | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      setError(null);
      const res = await fetch('/api/dashboard').then((response) => (response.ok ? response.json() : null)).catch(() => null);
      setSnapshot((res ?? null) as DashboardSnapshot | null);
    } catch {
      setError('Dashboard snapshot is unavailable.');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
    const timer = setInterval(() => {
      if (!document.hidden) refresh();
    }, 15000);
    return () => clearInterval(timer);
  }, [refresh]);

  return { snapshot, loading, error, refresh };
}

function timeAgo(dateStr: string | undefined): string {
  if (!dateStr) return 'never';
  const diff = Date.now() - new Date(dateStr).getTime();
  if (diff < 60000) return 'just now';
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
  return `${Math.floor(diff / 86400000)}d ago`;
}

function formatPassRate(value: number | null): string {
  if (value === null || Number.isNaN(value)) return 'No receipts yet';
  return `${Math.round(value * 100)}%`;
}

function formatCurrency(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return '$0.00';
  if (value >= 100) return `$${value.toFixed(0)}`;
  if (value >= 10) return `$${value.toFixed(2)}`;
  return `$${value.toFixed(3)}`;
}

function formatAgentName(value: string | null): string {
  if (!value) return 'No leaderboard data yet';
  const [provider, model] = value.split('/');
  return model ? `${provider} / ${model}` : value;
}

function latestRunCopy(wf: Workflow): string {
  if (!wf.latest_run?.run_id) return 'No run history yet';
  const status = wf.latest_run.status ? wf.latest_run.status.toUpperCase() : 'RECORDED';
  return `${status} ${timeAgo(wf.latest_run.finished_at ?? wf.latest_run.created_at)}`;
}

function scheduleCopy(wf: Workflow): string {
  if (wf.trigger?.cron_expression) return wf.trigger.cron_expression;
  if (wf.trigger?.enabled) return wf.trigger.event_type || 'Live trigger enabled';
  return 'Manual launch';
}

function WorkflowCard({
  wf,
  onEdit,
  onViewRun,
  onRunNow,
  onDelete,
}: {
  wf: Workflow;
  onEdit: () => void;
  onViewRun: () => void;
  onRunNow: () => void;
  onDelete: () => void;
}) {
  const badge = wf.dashboard_badge || { label: 'Draft', class_name: 'wf-card__badge--draft' };
  const workflowKind = wf.definition_type === 'operating_model' ? 'Operating model' : 'Workflow';
  const hasRun = Boolean(wf.latest_run?.run_id);
  // Silhouette: we don't have graph shape on the dashboard payload yet, so
  // the face comes from what we do have — trigger kind, cron vs event, and
  // last-run outcome. Widen the API later to emit a real shape hash.
  const hasTrigger = Boolean(wf.trigger?.enabled);
  const isCron = Boolean(wf.trigger?.cron_expression);
  const silhouetteNodeCount = hasRun ? 3 : 2;

  return (
    <article className="wf-card">
      <div className="wf-card__header">
        <div className="wf-card__identity">
          <div className="wf-card__eyebrow-row">
            <MoonWorkflowSilhouette
              nodeCount={silhouetteNodeCount}
              hasTrigger={hasTrigger}
              isCron={isCron}
              lastRunStatus={wf.latest_run?.status}
              width={72}
              height={18}
              label={`${wf.name} silhouette`}
            />
            <div className="wf-card__eyebrow">{workflowKind}</div>
          </div>
          <div className="wf-card__name">{wf.name}</div>
        </div>
        <span className={`wf-card__badge ${badge.class_name}`}>{badge.label}</span>
      </div>

      <div className="wf-card__desc">
        {wf.description || 'No description yet. Use the editor to define authority, state, and execution rules.'}
      </div>

      <div className="wf-card__stat-grid">
        <div className="wf-card__stat">
          <span className="wf-card__stat-label">Latest run</span>
          <strong>{latestRunCopy(wf)}</strong>
        </div>
        <div className="wf-card__stat">
          <span className="wf-card__stat-label">Launch mode</span>
          <strong>{scheduleCopy(wf)}</strong>
        </div>
        <div className="wf-card__stat">
          <span className="wf-card__stat-label">Runs</span>
          <strong>{wf.invocation_count ?? 0}</strong>
        </div>
        <div className="wf-card__stat">
          <span className="wf-card__stat-label">Last activity</span>
          <strong>{timeAgo(wf.last_invoked_at)}</strong>
        </div>
      </div>

      <div className="wf-card__actions">
        <button type="button" className="wf-card__btn wf-card__btn--primary" onClick={onEdit}>
          Open
        </button>
        <button type="button" className="wf-card__btn" onClick={onRunNow}>
          Run now
        </button>
        {hasRun && (
          <button type="button" className="wf-card__btn" onClick={onViewRun}>
            View latest
          </button>
        )}
        <button type="button" className="wf-card__btn wf-card__btn--danger" onClick={onDelete}>
          Delete
        </button>
      </div>
    </article>
  );
}

function WorkflowSectionBlock({
  section,
  loading,
  onPrimaryAction,
  onEditWorkflow,
  onEditModel,
  onViewRun,
  onRunNow,
  onDelete,
  primaryActionLabel,
}: {
  section: WorkflowSection;
  loading: boolean;
  onPrimaryAction: () => void;
  onEditWorkflow: (id: string) => void;
  onEditModel: (id: string) => void;
  onViewRun: (runId: string) => void;
  onRunNow: (workflowId: string) => void;
  onDelete: (workflowId: string) => void;
  primaryActionLabel: string;
}) {
  return (
    <section className={`dash-section dash-section--${section.tone}`}>
      <div className="dash-section__header">
        <div>
          <div className="dash-section__eyebrow">{section.eyebrow}</div>
          <h2 className="dash-section__title">{section.title}</h2>
          <p className="dash-section__copy">{section.description}</p>
        </div>
        <div className="dash-section__count">{loading ? '...' : section.count}</div>
      </div>

      {loading && section.count === 0 ? (
        <div className="dash-section__loading">Refreshing workflow inventory...</div>
      ) : section.count > 0 ? (
        <div className="dash-section__grid">
          {section.workflows.map((workflow) => (
            <WorkflowCard
              key={workflow.id}
              wf={workflow}
              onEdit={() => (workflow.definition_type === 'operating_model' ? onEditModel : onEditWorkflow)(workflow.id)}
              onViewRun={() => workflow.latest_run?.run_id && onViewRun(workflow.latest_run.run_id)}
              onRunNow={() => onRunNow(workflow.id)}
              onDelete={() => onDelete(workflow.id)}
            />
          ))}
        </div>
      ) : (
        <div className="dash-empty">
          <div className="dash-empty__title">{section.emptyTitle}</div>
          <div className="dash-empty__copy">{section.emptyCopy}</div>
          <button type="button" className="dash-empty__action" onClick={onPrimaryAction}>
            {primaryActionLabel}
          </button>
        </div>
      )}
    </section>
  );
}

export function Dashboard({
  onEditWorkflow,
  onEditModel,
  onViewRun,
  onNewWorkflow,
  onChat,
  onDescribe,
  onOpenCosts,
}: DashboardProps) {
  const { snapshot, loading, error, refresh } = useDashboardSnapshot();
  const [instanceFiles, setInstanceFiles] = useState<Array<{ id: string; filename: string }>>([]);
  const instanceFileRef = useRef<HTMLInputElement>(null);
  const workflows = snapshot?.workflows ?? [];
  const workflowById = new Map(workflows.map((workflow) => [workflow.id, workflow] as const));
  const summary = snapshot?.summary ?? {
    workflow_counts: { total: 0, live: 0, saved: 0, draft: 0 },
    health: { readiness: 'calibrating', label: 'Calibrating', tone: 'neutral' as const, copy: 'Metrics will harden as receipts and leaderboard data accumulate.' },
    runs_24h: 0,
    active_runs: 0,
    pass_rate_24h: null,
    total_cost_24h: 0,
    top_agent: null,
    models_online: 0,
    queue: {
      depth: 0,
      status: 'unknown' as const,
      utilization_pct: 0,
      pending: 0,
      ready: 0,
      claimed: 0,
      running: 0,
      error: null,
    },
  };
  const health = summary.health;

  useEffect(() => {
    fetch('/api/files?scope=instance')
      .then((response) => (response.ok ? response.json() : null))
      .then((data) => {
        if (data?.files) setInstanceFiles(data.files);
        else if (Array.isArray(data)) setInstanceFiles(data);
      })
      .catch(() => {});
  }, []);

  const handleRunNow = async (workflowId: string) => {
    try {
      await fetch(`/api/trigger/${workflowId}`, { method: 'POST' });
      await refresh();
    } catch {
      // Silent: run state will settle on the next refresh tick.
    }
  };

  const handleDelete = async (workflowId: string) => {
    if (!window.confirm('Delete this workflow? This cannot be undone.')) return;
    try {
      const response = await fetch(`/api/workflows/delete/${workflowId}`, { method: 'DELETE' });
      if (!response.ok) {
        const errorPayload = await response.json().catch(() => ({}));
        console.error('Delete failed:', errorPayload);
        return;
      }
      await refresh();
    } catch (error) {
      console.error('Delete error:', error);
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
        setInstanceFiles((current) => [{ id: data.file.id, filename: data.file.filename }, ...current]);
      }
    } catch {
      // Silent: upload failures should not destabilize the dashboard surface.
    } finally {
      event.target.value = '';
    }
  };

  const deleteInstanceFile = async (fileId: string) => {
    try {
      const response = await fetch(`/api/files/${fileId}`, { method: 'DELETE' });
      if (response.ok) {
        setInstanceFiles((current) => current.filter((file) => file.id !== fileId));
      }
    } catch {
      // Silent by design.
    }
  };

  const visibleRuns = (snapshot?.recent_runs ?? []).filter(
    (run) => !run.spec_name?.startsWith('compile_')
      && !run.spec_name?.startsWith('fix_bugs')
      && !run.spec_name?.startsWith('hardening_'),
  );
  const queueTone = summary.queue.status === 'critical'
    ? 'danger'
    : summary.queue.status === 'warning'
      ? 'warning'
      : summary.queue.status === 'ok'
        ? 'healthy'
        : 'neutral';
  const stateSpine = [
    {
      label: 'Health',
      value: loading ? '...' : health.label,
      tone: health.tone,
    },
    {
      label: 'Queue',
      value: loading
        ? '...'
        : summary.queue.error
          ? 'Probe error'
          : `${summary.queue.depth} waiting`,
      tone: queueTone,
    },
    {
      label: 'Runs',
      value: loading ? '...' : `${summary.active_runs} active`,
      tone: summary.active_runs > 0 ? 'warning' : 'neutral',
    },
  ];
  const hasWorkflows = summary.workflow_counts.total > 0;
  const heroTitle = hasWorkflows
    ? 'Operate workflows with explicit control.'
    : 'Build the first workflow lane with real state authority.';
  const heroCopy = hasWorkflows
    ? 'Live lanes, saved builders, drafts, and recent executions stay visible in one control surface.'
    : 'Describe the operating model if you know the intent, or start from scratch if you want hands-on control over every step.';

  const sectionMeta: Record<'live' | 'saved' | 'draft', Omit<WorkflowSection, 'count' | 'workflows'>> = {
      live: {
        key: 'live',
        title: 'Live Lanes',
        eyebrow: 'Active execution',
        description: 'Workflows with a trigger or schedule already wired into the world.',
        emptyTitle: 'No live lanes yet',
        emptyCopy: 'Promote a validated workflow into a live lane once you trust the execution path.',
        emptyAction: 'Describe the first lane',
        tone: 'live',
      },
      saved: {
        key: 'saved',
        title: 'Validated Workflows',
        eyebrow: 'Reusable build assets',
        description: 'Saved workflows with history behind them, ready to inspect, rerun, or evolve.',
        emptyTitle: 'No validated workflows yet',
        emptyCopy: 'Run a workflow once to turn the draft into a reusable operating asset.',
        emptyAction: 'Open the builder',
        tone: 'saved',
      },
      draft: {
        key: 'draft',
        title: 'Draft Bench',
        eyebrow: 'Work in progress',
        description: 'Early models and builders that have not seen execution yet.',
        emptyTitle: 'The draft bench is empty',
        emptyCopy: 'Start a fresh workflow or describe an operating model to seed the first draft.',
        emptyAction: 'Start the first workflow',
        tone: 'draft',
      },
    };

  const workflowSections: WorkflowSection[] = (snapshot?.sections ?? [
    { key: 'live', count: 0, workflow_ids: [] },
    { key: 'saved', count: 0, workflow_ids: [] },
    { key: 'draft', count: 0, workflow_ids: [] },
  ]).map((section) => ({
    ...sectionMeta[section.key],
    count: section.count,
    workflows: section.workflow_ids
      .map((workflowId) => workflowById.get(workflowId))
      .filter((workflow): workflow is Workflow => workflow !== undefined),
  }));

  return (
    <div className="dash-page">
      <aside className="dash-sidebar">
        <div className="dash-sidebar__brand">
          <img className="dash-sidebar__logo" src={praxisSymbol} alt="Praxis symbol" />
          <div className="dash-sidebar__brand-copy">
            <span>Command Surface</span>
            <strong>{APP_CONFIG.name}</strong>
          </div>
        </div>

        <div className="dash-sidebar__overview">
          <div className="dash-sidebar__overview-kicker">State authority</div>
          <div className="dash-sidebar__overview-title">{APP_CONFIG.engineName}</div>
          <div className="dash-sidebar__overview-copy">
            One visible place to launch, inspect, and recover workflow lanes without losing the shape of the system.
          </div>
          <div className="dash-sidebar__spine" aria-label="Control state">
            {stateSpine.map((row) => (
              <div key={row.label} className="dash-sidebar__spine-row">
                <span className={`dash-sidebar__spine-dot dash-sidebar__spine-dot--${row.tone}`} />
                <span className="dash-sidebar__spine-label">{row.label}</span>
                <strong>{row.value}</strong>
              </div>
            ))}
          </div>
          <div className="dash-sidebar__overview-grid">
            <div className="dash-sidebar__overview-stat">
              <span>Live</span>
              <strong>{summary.workflow_counts.live}</strong>
            </div>
            <div className="dash-sidebar__overview-stat">
              <span>Saved</span>
              <strong>{summary.workflow_counts.saved}</strong>
            </div>
            <div className="dash-sidebar__overview-stat">
              <span>Drafts</span>
              <strong>{summary.workflow_counts.draft}</strong>
            </div>
            <div className="dash-sidebar__overview-stat">
              <span>Files</span>
              <strong>{instanceFiles.length}</strong>
            </div>
          </div>
        </div>

        <div className="dash-sidebar__stack">
          {workflowSections[0]?.workflows.length > 0 && (
            <div className="dash-sidebar__cluster">
              <div className="dash-sidebar__section">Live lanes</div>
              {workflowSections[0].workflows.map((workflow) => (
                <button
                  key={workflow.id}
                  className="dash-sidebar__item dash-sidebar__item--live"
                  onClick={() => (workflow.definition_type === 'operating_model' ? onEditModel : onEditWorkflow)(workflow.id)}
                >
                  <span className="dash-sidebar__dot dash-sidebar__dot--live" />
                  <span className="dash-sidebar__title">{workflow.name}</span>
                  {(workflow.invocation_count ?? 0) > 0 && (
                    <span className="dash-sidebar__count">{workflow.invocation_count}</span>
                  )}
                </button>
              ))}
            </div>
          )}

          {(workflowSections[1]?.workflows.length > 0 || workflowSections[2]?.workflows.length > 0) && (
            <div className="dash-sidebar__cluster">
              <div className="dash-sidebar__section">Workbench</div>
              {[...(workflowSections[1]?.workflows ?? []), ...(workflowSections[2]?.workflows ?? [])].map((workflow) => (
                <button
                  key={workflow.id}
                  className="dash-sidebar__item"
                  onClick={() => (workflow.definition_type === 'operating_model' ? onEditModel : onEditWorkflow)(workflow.id)}
                >
                  <span
                    className={`dash-sidebar__dot ${
                      (workflow.invocation_count ?? 0) > 0 ? 'dash-sidebar__dot--done' : 'dash-sidebar__dot--draft'
                    }`}
                  />
                  <span className="dash-sidebar__title">{workflow.name}</span>
                </button>
              ))}
            </div>
          )}
        </div>

        <div className="dash-sidebar__actions">
          <button className="dash-sidebar__action-btn dash-sidebar__action-btn--primary" onClick={onDescribe}>
            New operating model
          </button>
          <button className="dash-sidebar__action-btn" onClick={onNewWorkflow}>
            Workflow builder
          </button>
        </div>

        <div className="dash-sidebar__bottom">
          <div className="dash-sidebar__section">Operator lane</div>
          <button className="dash-sidebar__ask" onClick={onChat}>
            Ask anything...
          </button>
          <button type="button" className="dash-sidebar__upload" onClick={() => instanceFileRef.current?.click()}>
            Add knowledge file
          </button>
          <input ref={instanceFileRef} type="file" hidden onChange={handleInstanceFileUpload} />
        </div>
      </aside>

      <main className="dash-main">
        <div className="dash-content">
          <section className="dash-hero">
            <div className="dash-hero__copy">
              <div className="dash-hero__eyebrow">Workflow command center</div>
              <h1 className="dash-hero__title">{heroTitle}</h1>
              <p className="dash-hero__desc">{heroCopy}</p>

              <div className="dash-hero__actions">
                <button type="button" className="dash-hero__primary" onClick={onDescribe}>
                  Describe it
                </button>
                <button type="button" className="dash-hero__secondary" onClick={onNewWorkflow}>
                  Start from scratch
                </button>
                <button type="button" className="dash-hero__secondary" onClick={onChat}>
                  Open chat
                </button>
              </div>

              <div className="dash-hero__chips">
                <span className="dash-chip">{summary.workflow_counts.total} workflows in scope</span>
                <span className="dash-chip">{summary.workflow_counts.live} live lanes</span>
                <span className="dash-chip">{instanceFiles.length} knowledge files</span>
                <span className={`dash-pill dash-pill--${health.tone}`}>{health.label}</span>
              </div>

              {error && (
                <div className="dash-inline-alert">
                  Live metrics are unavailable right now. The workflow inventory still works, but health data is stale.
                </div>
              )}
            </div>

            <div className="dash-hero__rail">
              <div className="dash-hero-card dash-hero-card--spotlight">
                <div className="dash-hero-card__eyebrow">System read</div>
                <div className="dash-hero-card__title">{health.label}</div>
                <div className="dash-hero-card__copy">{health.copy}</div>
                <div className="dash-hero-card__grid">
                  <div className="dash-hero-card__stat">
                    <span>Pass rate</span>
                    <strong>{loading ? '...' : formatPassRate(summary.pass_rate_24h)}</strong>
                  </div>
                  <div className="dash-hero-card__stat">
                    <span>Runs today</span>
                    <strong>{loading ? '...' : summary.runs_24h}</strong>
                  </div>
                  <button
                    type="button"
                    className="dash-hero-card__stat dash-hero-card__stat--link"
                    onClick={onOpenCosts}
                    title="Open Cost Summary"
                  >
                    <span>Spend</span>
                    <strong>{loading ? '...' : formatCurrency(summary.total_cost_24h)}</strong>
                  </button>
                  <div className="dash-hero-card__stat">
                    <span>Leaderboard</span>
                    <strong>{loading ? '...' : summary.models_online}</strong>
                  </div>
                  <div className="dash-hero-card__stat">
                    <span>Top agent</span>
                    <strong>{loading ? '...' : formatAgentName(summary.top_agent)}</strong>
                  </div>
                  <div className="dash-hero-card__stat">
                    <span>Queue</span>
                    <strong>{loading ? '...' : summary.queue.depth}</strong>
                  </div>
                </div>
              </div>

            </div>
          </section>


          <div className="dash-board">
            <div className="dash-board__main">
              {workflowSections.map((section) => (
                <WorkflowSectionBlock
                  key={section.key}
                  section={section}
                  loading={loading}
                  onPrimaryAction={section.key === 'draft' ? onNewWorkflow : onDescribe}
                  primaryActionLabel={section.emptyAction}
                  onEditWorkflow={onEditWorkflow}
                  onEditModel={onEditModel}
                  onViewRun={onViewRun}
                  onRunNow={handleRunNow}
                  onDelete={handleDelete}
                />
              ))}
            </div>

            <aside className="dash-board__rail">
              <section className="dash-panel">
                <div className="dash-panel__header">
                  <div>
                    <div className="dash-panel__eyebrow">Recent execution</div>
                    <h2 className="dash-panel__title">Recent Runs</h2>
                  </div>
                </div>

                {visibleRuns.length > 0 ? (
                  <div className="dash-run-list">
                    {visibleRuns.slice(0, 8).map((run) => (
                      <button
                        key={run.run_id}
                        type="button"
                        className="dash-run"
                        onClick={() => onViewRun(run.run_id)}
                      >
                        <div className={`dash-run__status dash-run__status--${run.status}`} />
                        <div className="dash-run__body">
                          <div className="dash-run__title">
                            {(run.spec_name || run.run_id.slice(0, 12))
                              .replace(/[_-]+/g, ' ')
                              .replace(/\b\w/g, (character: string) => character.toUpperCase())}
                          </div>
                          <div className="dash-run__meta">
                            <span>{run.status}</span>
                            <span>{run.total_jobs > 0 ? `${run.completed_jobs}/${run.total_jobs} jobs` : 'Awaiting jobs'}</span>
                            <span>{run.created_at ? timeAgo(run.created_at) : 'Queued now'}</span>
                          </div>
                        </div>
                      </button>
                    ))}
                  </div>
                ) : (
                  <div className="dash-empty dash-empty--compact">
                    <div className="dash-empty__title">No recent runs</div>
                    <div className="dash-empty__copy">
                      Trigger a workflow and the latest executions will appear here with one-click inspection.
                    </div>
                  </div>
                )}
              </section>

              <section className="dash-panel">
                <div className="dash-panel__header">
                  <div>
                    <div className="dash-panel__eyebrow">Attached context</div>
                    <h2 className="dash-panel__title">Knowledge Base</h2>
                  </div>
                  <button type="button" className="dash-panel__action" onClick={() => instanceFileRef.current?.click()}>
                    Add file
                  </button>
                </div>

                {instanceFiles.length > 0 ? (
                  <div className="dash-file-list">
                    {instanceFiles.map((file) => (
                      <div key={file.id} className="dash-file">
                        <div className="dash-file__name">{file.filename}</div>
                        <button type="button" className="dash-file__remove" onClick={() => deleteInstanceFile(file.id)}>
                          Remove
                        </button>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="dash-empty dash-empty--compact">
                    <div className="dash-empty__title">No files attached</div>
                    <div className="dash-empty__copy">
                      Upload briefs, notes, or source files so the workspace can reason over them during execution.
                    </div>
                  </div>
                )}
              </section>
            </aside>
          </div>
        </div>
      </main>
    </div>
  );
}
