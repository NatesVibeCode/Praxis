import React, { useCallback, useEffect, useRef, useState } from 'react';
import { APP_CONFIG } from '../config';
import praxisSymbol from '../assets/praxis-symbol-inverse.svg';
import { MoonWorkflowSilhouette } from './MoonWorkflowSilhouette';
import { isAbortError } from '../shared/request';
import { ReceiptCard } from '../primitives';
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
  tool_opportunities?: ToolOpportunity[];
}

interface ToolOpportunity {
  shape_hash: string;
  decision_key: string;
  occurrence_count: number;
  distinct_surfaces: number;
  action_kinds: string[];
  operation_names: string[];
  sample_commands: string[];
  sample_path_shapes: string[];
  last_seen: string | null;
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

interface ToolbeltReviewItem {
  id: string;
  tone: 'neutral' | 'healthy' | 'warning' | 'danger';
  title: string;
  detail: string;
  meta: string;
  onClick?: () => void;
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

function runDisplayName(run: RecentRun): string {
  return (run.spec_name || run.run_id.slice(0, 12))
    .replace(/[_-]+/g, ' ')
    .replace(/\b\w/g, (character: string) => character.toUpperCase());
}

const ACTION_KIND_LABEL: Record<string, string> = {
  shell: 'shell commands',
  edit: 'file edits',
  multi_edit: 'file edits',
  write: 'file writes',
  read: 'file reads',
  gateway_op: 'tool calls',
};

function humanizeOpName(name: string): string {
  const parts = name.split('.');
  const tail = parts.length > 1 ? parts.slice(1).join(' ') : name;
  return tail.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
}

function extractExtension(shape: string): string {
  const m = shape.match(/\*\.(\w+)$/);
  return m?.[1] ?? '';
}

function toolOpportunityLabel(opp: ToolOpportunity): string {
  const kind = opp.action_kinds[0] ?? 'shape';

  if (kind === 'gateway_op') {
    const opName = (opp.operation_names ?? [])[0];
    return opName ? humanizeOpName(opName) : 'Gateway operation';
  }

  if (kind === 'shell') {
    const cmd = (opp.sample_commands ?? [])[0] ?? '';
    const verb = cmd.split(/\s+/)[0];
    return verb ? `${verb} pattern` : 'Shell pattern';
  }

  const shape = (opp.sample_path_shapes ?? [])[0] ?? '';
  const ext = extractExtension(shape);
  const kindLabel = kind === 'edit' || kind === 'multi_edit' ? 'Editing' : kind === 'write' ? 'Writing' : 'Reading';
  return ext ? `${kindLabel} .${ext} files` : (ACTION_KIND_LABEL[kind] ?? kind.replace(/_/g, ' '));
}

function toolOpportunityDetail(opp: ToolOpportunity): string {
  const kind = opp.action_kinds[0];
  if (kind === 'gateway_op') return (opp.operation_names ?? [])[0] ?? '';
  if (kind === 'shell') return (opp.sample_commands ?? [])[0] ?? '';
  return (opp.sample_path_shapes ?? [])[0] ?? '';
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
    const controller = new AbortController();
    fetch('/api/files?scope=instance', { signal: controller.signal })
      .then((response) => (response.ok ? response.json() : null))
      .then((data) => {
        if (data?.files) setInstanceFiles(data.files);
        else if (Array.isArray(data)) setInstanceFiles(data);
      })
      .catch((error) => {
        if (!isAbortError(error)) {
          // Silent by design: file inventory is auxiliary dashboard context.
        }
      });
    return () => controller.abort();
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
  const materializedDate = new Date().toISOString().slice(0, 10);
  const passRateLabel = formatPassRate(summary.pass_rate_24h);
  const spendLabel = formatCurrency(summary.total_cost_24h);
  const sealedRunCount = visibleRuns.filter((run) => run.status === 'succeeded').length;
  const queueTone = summary.queue.status === 'critical'
    ? 'danger'
    : summary.queue.status === 'warning'
      ? 'warning'
      : summary.queue.status === 'ok'
        ? 'healthy'
        : 'neutral';
  const stateSpine = [
    {
      label: 'State',
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
    ? 'Continue work'
    : 'Start work';
  const heroCopy = hasWorkflows
    ? `${summary.workflow_counts.total} workflow${summary.workflow_counts.total === 1 ? '' : 's'} in scope. Open a lane, inspect receipts, or add context.`
    : 'Describe the job, add context, or start blank. The contract keeps success, failure, scope, and receipts visible.';

  const sectionMeta: Record<'live' | 'saved' | 'draft', Omit<WorkflowSection, 'count' | 'workflows'>> = {
      live: {
        key: 'live',
        title: 'Live',
        eyebrow: 'Active',
        description: 'Workflow lanes already wired to the world.',
        emptyTitle: 'No live lanes',
        emptyCopy: 'Promote one when the contract and verifier are solid.',
        emptyAction: 'Describe lane',
        tone: 'live',
      },
      saved: {
        key: 'saved',
        title: 'Saved',
        eyebrow: 'Reusable',
        description: 'Workflows with enough shape to reopen, rerun, or evolve.',
        emptyTitle: 'No saved workflows',
        emptyCopy: 'Run one cleanly and it becomes reusable.',
        emptyAction: 'Open builder',
        tone: 'saved',
      },
      draft: {
        key: 'draft',
        title: 'Drafts',
        eyebrow: 'Working',
        description: 'Unproven shapes waiting for a contract and verifier.',
        emptyTitle: 'No drafts',
        emptyCopy: 'Start blank or describe the lane.',
        emptyAction: 'Start',
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
  const draftWorkflows = workflowSections.find((section) => section.key === 'draft')?.workflows ?? [];
  const failedRuns = visibleRuns.filter((run) => run.status === 'failed' || run.status === 'cancelled');
  const toolbeltReviewItems: ToolbeltReviewItem[] = [];

  failedRuns.slice(0, 2).forEach((run) => {
    toolbeltReviewItems.push({
      id: `run-${run.run_id}`,
      tone: 'danger',
      title: runDisplayName(run),
      detail: `${run.status} - ${run.total_jobs > 0 ? `${run.completed_jobs}/${run.total_jobs} jobs` : 'no job receipts yet'}`,
      meta: run.created_at ? timeAgo(run.created_at) : 'queued',
      onClick: () => onViewRun(run.run_id),
    });
  });

  if (health.tone === 'warning' || health.tone === 'danger') {
    toolbeltReviewItems.push({
      id: 'health',
      tone: health.tone,
      title: 'Platform state',
      detail: health.copy,
      meta: health.label,
      onClick: onOpenCosts,
    });
  }

  if (summary.queue.depth > 0 || summary.active_runs > 0) {
    toolbeltReviewItems.push({
      id: 'queue',
      tone: summary.queue.status === 'critical' ? 'danger' : summary.queue.status === 'warning' ? 'warning' : 'neutral',
      title: 'Execution queue',
      detail: `${summary.queue.pending} pending - ${summary.queue.ready} ready - ${summary.queue.running} running`,
      meta: `${summary.queue.depth} waiting`,
      onClick: onChat,
    });
  }

  draftWorkflows.slice(0, Math.max(0, 5 - toolbeltReviewItems.length)).forEach((workflow) => {
    toolbeltReviewItems.push({
      id: `draft-${workflow.id}`,
      tone: 'warning',
      title: workflow.name,
      detail: workflow.description || 'Draft lane waiting for a contract, verifier, or promotion decision.',
      meta: 'draft',
      onClick: () => (workflow.definition_type === 'operating_model' ? onEditModel : onEditWorkflow)(workflow.id),
    });
  });

  if (!loading && toolbeltReviewItems.length === 0) {
    toolbeltReviewItems.push({
      id: 'clear',
      tone: 'healthy',
      title: 'No review pressure',
      detail: 'Current snapshot has no failed runs, health alerts, queue backlog, or draft lanes.',
      meta: 'ready',
    });
  }

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
            New model
          </button>
          <button className="dash-sidebar__action-btn" onClick={onNewWorkflow}>
            Builder
          </button>
        </div>

        <div className="dash-sidebar__bottom">
          <div className="dash-sidebar__section">Operator lane</div>
          <button className="dash-sidebar__ask" onClick={onChat}>
            Ask...
          </button>
          <button type="button" className="dash-sidebar__upload" onClick={() => instanceFileRef.current?.click()}>
            Add file
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
                  Describe job
                </button>
                <button type="button" className="dash-hero__secondary" onClick={onNewWorkflow}>
                  Blank builder
                </button>
                <button type="button" className="dash-hero__secondary" onClick={onChat}>
                  Chat
                </button>
                <button type="button" className="dash-hero__secondary" onClick={() => instanceFileRef.current?.click()}>
                  Add file
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
              <div className="dash-contract-preview">
                <div className="dash-contract-preview__head">
                  <span>workflow_contract · tec_workflow_lane</span>
                  <span>{hasWorkflows ? 'scope · active' : 'scope · draft'}</span>
                </div>
                <div className="dash-contract-preview__body">
                  <div className="dash-contract-preview__row">
                    <div className="dash-contract-preview__key">task</div>
                    <div className="dash-contract-preview__val">
                      {hasWorkflows ? 'resume or inspect workflow lanes' : 'define first workflow job'}
                    </div>
                  </div>
                  <div className="dash-contract-preview__row">
                    <div className="dash-contract-preview__key">read scope</div>
                    <div className="dash-contract-preview__val">
                      <span className="dash-contract-pill dash-contract-pill--read">/workflows</span>
                      <span className="dash-contract-pill dash-contract-pill--read">/runs</span>
                      <span className="dash-contract-pill dash-contract-pill--read">/receipts</span>
                      <span className="dash-contract-pill dash-contract-pill--read">/knowledge</span>
                    </div>
                  </div>
                  <div className="dash-contract-preview__row">
                    <div className="dash-contract-preview__key">write scope</div>
                    <div className="dash-contract-preview__val">
                      <span className="dash-contract-pill dash-contract-pill--write">/draft</span>
                      <span className="dash-contract-pill dash-contract-pill--write">/success_if</span>
                      <span className="dash-contract-pill dash-contract-pill--write">/failure_if</span>
                    </div>
                  </div>
                  <div className="dash-contract-preview__row dash-contract-preview__row--locked">
                    <div className="dash-contract-preview__key">locked</div>
                    <div className="dash-contract-preview__val">
                      <span className="dash-contract-pill dash-contract-pill--locked">/destructive.write</span>
                      <span className="dash-contract-pill dash-contract-pill--locked">/unbounded.spend</span>
                      <span className="dash-contract-pill dash-contract-pill--locked">/approval.required</span>
                    </div>
                  </div>
                  <div className="dash-contract-preview__row">
                    <div className="dash-contract-preview__key">tools</div>
                    <div className="dash-contract-preview__val">
                      <span className="dash-contract-pill">workflow.builder</span>
                      <span className="dash-contract-pill">verifier.run</span>
                      <span className="dash-contract-pill">receipts.query</span>
                    </div>
                  </div>
                  <div className="dash-contract-preview__row">
                    <div className="dash-contract-preview__key">approval</div>
                    <div className="dash-contract-preview__val">human · for any locked.* match</div>
                  </div>
                  <div className="dash-contract-preview__row">
                    <div className="dash-contract-preview__key">verifier</div>
                    <div className="dash-contract-preview__val">success_if true · failure_if false · receipts sealed</div>
                  </div>
                  <div className="dash-contract-preview__row">
                    <div className="dash-contract-preview__key">retry</div>
                    <div className="dash-contract-preview__val">
                      requires <span className="dash-contract-code">previous_failure</span> + <span className="dash-contract-code">retry_delta</span>
                    </div>
                  </div>
                </div>
                <div className="dash-contract-preview__foot">
                  <span>materialized · {materializedDate}</span>
                  <span>✓ environment ready</span>
                </div>
              </div>

            </div>
          </section>

          <section className="dash-run-instrument">
            <div className="dash-terminal">
              <div className="dash-terminal__label">sandbox · overview · plan→execute→verify</div>
              <span className="dash-terminal__line"><span>$</span> praxis workflow query overview</span>
              <span className="dash-terminal__line dash-terminal__line--muted">
                › materializing dashboard snapshot · {summary.workflow_counts.total} workflows in scope
              </span>
              <span className="dash-terminal__line">
                agent · {hasWorkflows ? 'inspect lanes, receipts, and knowledge' : 'awaiting first contract'}
              </span>
              <span className="dash-terminal__line dash-terminal__line--ok">
                ✓ {summary.workflow_counts.live} live · {summary.workflow_counts.saved} saved · {summary.workflow_counts.draft} drafts
              </span>
              <span className={`dash-terminal__line ${summary.queue.depth > 0 ? 'dash-terminal__line--warn' : 'dash-terminal__line--ok'}`}>
                {summary.queue.depth > 0 ? '!' : '✓'} queue · {summary.queue.depth} waiting · {summary.active_runs} active
              </span>
              <span className="dash-terminal__line dash-terminal__line--muted">
                › knowledge · {instanceFiles.length} file{instanceFiles.length === 1 ? '' : 's'} attached
              </span>
              <span className="dash-terminal__line"><span>$</span> _</span>
            </div>

            <div className="dash-receipts">
              <div className="dash-receipts__head">
                <span>receipts</span>
                <span>{sealedRunCount} sealed</span>
              </div>

              <ReceiptCard
                className="dash-receipt-card dash-receipt-card--allow"
                state="sealed"
                title="workflow.inventory"
                meta="hydrated"
                fields={[
                  { key: 'live', value: summary.workflow_counts.live },
                  { key: 'saved', value: summary.workflow_counts.saved },
                  { key: 'drafts', value: summary.workflow_counts.draft },
                  { key: 'result', value: 'hydrated · visible' },
                ]}
                seal={`${sealedRunCount} sealed`}
              />

              <button
                type="button"
                className="dash-receipt-button"
                onClick={onOpenCosts}
                title="View token spend (stays under Overview)"
              >
                <ReceiptCard
                  className="dash-receipt-card dash-receipt-card--verify"
                  state={health.tone === 'danger' ? 'refused' : 'verify'}
                  title="dashboard.health"
                  meta="verifier"
                  fields={[
                    { key: 'pass', value: passRateLabel },
                    { key: 'runs', value: summary.runs_24h },
                    { key: 'spend', value: spendLabel },
                    { key: 'state', value: health.label },
                  ]}
                  seal="view spend"
                />
              </button>
            </div>
          </section>


          <div className="dash-board">
            <div className="dash-board__main">
              {hasWorkflows ? (
                workflowSections.map((section) => (
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
                ))
              ) : (
                <section className="dash-section dash-start-panel">
                  <div className="dash-section__header">
                    <div>
                      <h2 className="dash-section__title">Next step</h2>
                      <p className="dash-section__copy">Pick the way you want to start. The contract and receipt surfaces stay attached.</p>
                    </div>
                  </div>
                  <div className="dash-start-panel__grid">
                    <button type="button" className="dash-start-panel__action" onClick={onDescribe}>
                      <span>Describe job</span>
                      <strong>Plain language into contract</strong>
                    </button>
                    <button type="button" className="dash-start-panel__action" onClick={onNewWorkflow}>
                      <span>Blank builder</span>
                      <strong>Wire the graph by hand</strong>
                    </button>
                    <button type="button" className="dash-start-panel__action" onClick={onChat}>
                      <span>Chat</span>
                      <strong>Shape the first lane beside the app</strong>
                    </button>
                    <button type="button" className="dash-start-panel__action" onClick={() => instanceFileRef.current?.click()}>
                      <span>Add file</span>
                      <strong>Attach context before the run</strong>
                    </button>
                  </div>
                </section>
              )}
            </div>

            <aside className="dash-board__rail">
              <section className="dash-panel dash-toolbelt-panel">
                <div className="dash-panel__header">
                  <div>
                    <div className="dash-panel__eyebrow">Materialize</div>
                    <h2 className="dash-panel__title">Toolbelt Review</h2>
                  </div>
                  <span className="dash-review-count">{loading ? '...' : toolbeltReviewItems.length}</span>
                </div>

                <div className="dash-review-list">
                  {toolbeltReviewItems.map((item) => {
                    const body = (
                      <>
                        <span className={`dash-review-item__dot dash-review-item__dot--${item.tone}`} />
                        <span className="dash-review-item__body">
                          <strong>{item.title}</strong>
                          <span>{item.detail}</span>
                        </span>
                        <em>{item.meta}</em>
                      </>
                    );
                    if (item.onClick) {
                      return (
                        <button
                          key={item.id}
                          type="button"
                          className="dash-review-item"
                          onClick={item.onClick}
                        >
                          {body}
                        </button>
                      );
                    }
                    return (
                      <div key={item.id} className="dash-review-item dash-review-item--static">
                        {body}
                      </div>
                    );
                  })}
                </div>
              </section>

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
                            {runDisplayName(run)}
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

              <section className="dash-panel">
                <div className="dash-panel__header">
                  <div>
                    <div className="dash-panel__eyebrow">Repeated work</div>
                    <h2 className="dash-panel__title">Tool Opportunities</h2>
                  </div>
                  <span className="dash-review-count">
                    {loading ? '...' : (snapshot?.tool_opportunities?.length ?? 0)}
                  </span>
                </div>

                {(snapshot?.tool_opportunities?.length ?? 0) > 0 ? (
                  <div className="dash-review-list">
                    {(snapshot?.tool_opportunities ?? []).map((opp) => {
                      const label = toolOpportunityLabel(opp);
                      const detail = toolOpportunityDetail(opp);
                      const tone = opp.distinct_surfaces > 1 ? 'healthy' : 'neutral';
                      return (
                        <div
                          key={opp.shape_hash}
                          className="dash-review-item dash-review-item--static"
                          title={`${opp.shape_hash.slice(0, 12)} · ${opp.action_kinds.join(', ')}`}
                        >
                          <span className={`dash-review-item__dot dash-review-item__dot--${tone}`} />
                          <span className="dash-review-item__body">
                            <strong>{label}</strong>
                            {detail ? (
                              <span className="dash-review-item__detail--mono">{detail}</span>
                            ) : null}
                          </span>
                          <em>
                            {`${opp.occurrence_count}×`}
                            {opp.distinct_surfaces > 1 ? ` · ${opp.distinct_surfaces} surfaces` : ''}
                            {opp.last_seen ? ` · ${timeAgo(opp.last_seen)}` : ''}
                          </em>
                        </div>
                      );
                    })}
                  </div>
                ) : (
                  <div className="dash-empty dash-empty--compact">
                    <div className="dash-empty__title">Nothing repeating yet</div>
                    <div className="dash-empty__copy">
                      Patterns appear here once the same action shape is captured 3+ times — a signal that it's worth folding into a tool.
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
