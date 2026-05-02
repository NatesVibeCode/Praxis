import React, { useCallback, useEffect, useRef, useState } from 'react';
import { MoonWorkflowSilhouette } from './MoonWorkflowSilhouette';
import { isAbortError } from '../shared/request';
import {
  Button,
  EmptyStateExplainer,
  ListPanel,
  MetricTile,
  PanelCard,
  ReceiptCard,
  StatusRail,
  StatusRow,
  type StatusRowTone,
} from '../primitives';

function rowToneFromTone(tone: 'healthy' | 'warning' | 'danger' | 'neutral'): StatusRowTone {
  if (tone === 'healthy') return 'ok';
  if (tone === 'warning') return 'live';
  if (tone === 'danger') return 'err';
  return 'idle';
}

function rowToneFromRunStatus(status: 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled'): StatusRowTone {
  if (status === 'succeeded') return 'ok';
  if (status === 'failed') return 'err';
  if (status === 'running') return 'live';
  return 'idle';
}
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
    <PanelCard
      eyebrow={
        <span className="wf-card__eyebrow-row">
          <MoonWorkflowSilhouette
            nodeCount={silhouetteNodeCount}
            hasTrigger={hasTrigger}
            isCron={isCron}
            lastRunStatus={wf.latest_run?.status}
            width={72}
            height={18}
            label={`${wf.name} silhouette`}
          />
          <span>{workflowKind}</span>
        </span>
      }
      title={wf.name}
      action={<span className={`wf-card__badge ${badge.class_name}`}>{badge.label}</span>}
      footer={
        <div className="prx-button-row">
          <Button tone="primary" size="sm" onClick={onEdit}>Open</Button>
          <Button size="sm" onClick={onRunNow}>Run now</Button>
          {hasRun && (
            <Button size="sm" onClick={onViewRun}>View latest</Button>
          )}
          <Button size="sm" tone="danger" onClick={onDelete}>Delete</Button>
        </div>
      }
    >
      {wf.description && (
        <p className="wf-card__desc">{wf.description}</p>
      )}

      <StatusRail
        items={[
          { label: 'Latest run', value: latestRunCopy(wf), tone: 'dim' },
          { label: 'Launch', value: scheduleCopy(wf), tone: 'dim' },
          { label: 'Runs', value: wf.invocation_count ?? 0, tone: 'dim' },
          { label: 'Last', value: timeAgo(wf.last_invoked_at), tone: 'dim' },
        ]}
      />
    </PanelCard>
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
      <header className="dash-section__bar">
        <span className="dash-section__label">{section.title}</span>
        <span className="dash-section__count-badge">{loading ? '…' : section.count}</span>
      </header>

      {loading && section.count === 0 ? (
        <div className="dash-section__loading">Refreshing…</div>
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
        <EmptyStateExplainer
          title={section.emptyTitle}
          why={section.emptyCopy}
          actionLabel={primaryActionLabel}
          onAction={onPrimaryAction}
        />
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
  const [instanceFilesError, setInstanceFilesError] = useState<string | null>(null);
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
      .then((response) => {
        if (!response.ok) throw new Error(`File inventory returned ${response.status}`);
        return response.json();
      })
      .then((data) => {
        if (data?.files) {
          setInstanceFiles(data.files);
          setInstanceFilesError(null);
        } else if (Array.isArray(data)) {
          setInstanceFiles(data);
          setInstanceFilesError(null);
        } else {
          setInstanceFiles([]);
          setInstanceFilesError('File inventory returned an unexpected shape.');
        }
      })
      .catch((error) => {
        if (!isAbortError(error)) {
          setInstanceFilesError(error instanceof Error ? error.message : 'File inventory is unavailable.');
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
        setInstanceFilesError(null);
      } else {
        setInstanceFilesError('File upload did not return a saved file record.');
      }
    } catch {
      setInstanceFilesError('File upload failed.');
    } finally {
      event.target.value = '';
    }
  };

  const deleteInstanceFile = async (fileId: string) => {
    try {
      const response = await fetch(`/api/files/${fileId}`, { method: 'DELETE' });
      if (response.ok) {
        setInstanceFiles((current) => current.filter((file) => file.id !== fileId));
      } else {
        setInstanceFilesError('File removal failed.');
      }
    } catch {
      setInstanceFilesError('File removal failed.');
    }
  };

  const visibleRuns = (snapshot?.recent_runs ?? []).filter(
    (run) => !run.spec_name?.startsWith('compile_')
      && !run.spec_name?.startsWith('fix_bugs')
      && !run.spec_name?.startsWith('hardening_'),
  );
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

  const overviewCards = [
    {
      id: 'inventory',
      title: 'Workflow Inventory',
      source: '/api/dashboard',
      value: `${summary.workflow_counts.total} workflow${summary.workflow_counts.total === 1 ? '' : 's'}`,
      detail: `${summary.workflow_counts.live} live - ${summary.workflow_counts.saved} saved - ${summary.workflow_counts.draft} draft`,
      action: hasWorkflows ? 'Open builder' : 'Start first lane',
      onClick: hasWorkflows ? onNewWorkflow : onDescribe,
      tone: 'neutral' as const,
    },
    {
      id: 'health',
      title: 'Health Receipts',
      source: 'summary.health',
      value: health.label,
      detail: `${passRateLabel} pass rate - ${spendLabel} spend`,
      action: 'Inspect spend',
      onClick: onOpenCosts,
      tone: health.tone,
    },
    {
      id: 'queue',
      title: 'Execution Queue',
      source: 'summary.queue',
      value: `${summary.queue.depth} waiting`,
      detail: `${summary.queue.pending} pending - ${summary.queue.ready} ready - ${summary.queue.running} running`,
      action: 'Open operator lane',
      onClick: onChat,
      tone: queueTone,
    },
    {
      id: 'files',
      title: 'Attached Context',
      source: '/api/files?scope=instance',
      value: instanceFilesError ? 'Unavailable' : `${instanceFiles.length} file${instanceFiles.length === 1 ? '' : 's'}`,
      detail: instanceFilesError ?? 'Instance-scoped files available to the workspace',
      action: 'Add file',
      onClick: () => instanceFileRef.current?.click(),
      tone: instanceFilesError ? 'danger' as const : 'neutral' as const,
    },
  ];

  return (
    <div className="dash-page">
      <main className="dash-main">
        <div className="dash-content">
          <input ref={instanceFileRef} type="file" hidden onChange={handleInstanceFileUpload} />

          <section className="dash-hero">
            <h1 className="dash-hero__title">{heroTitle}</h1>
            <div className="dash-hero__actions prx-button-row">
              <Button tone="primary" size="sm" onClick={onDescribe}>Describe job</Button>
              <Button size="sm" onClick={onNewWorkflow}>Blank builder</Button>
              <Button size="sm" onClick={onChat}>Chat</Button>
              <Button size="sm" onClick={() => instanceFileRef.current?.click()}>Add file</Button>
            </div>
          </section>

          {error && (
            <div className="dash-inline-alert">
              Live metrics unavailable — health data may be stale.
            </div>
          )}

          <div className="prx-tile-grid" aria-label="Dashboard metrics">
            {overviewCards.map((card) => (
              <MetricTile
                key={card.id}
                label={card.title}
                value={card.value}
                detail={card.detail}
                action={`${card.action} →`}
                onClick={card.onClick}
                aria-label={`${card.title}: ${card.value}`}
              />
            ))}
          </div>

          <div className="dash-receipts">
            <ReceiptCard
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
              title="View token spend"
            >
              <ReceiptCard
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
                <EmptyStateExplainer
                  title="No workflows yet"
                  why="Use the buttons above to describe a job, open the builder, or start a chat."
                  actionLabel="Describe a job"
                  onAction={onDescribe}
                />
              )}
            </div>

            <aside className="dash-board__rail">
              <ListPanel
                eyebrow="Materialize"
                title="Toolbelt Review"
                count={loading ? '…' : toolbeltReviewItems.length}
              >
                {toolbeltReviewItems.map((item) => (
                  <StatusRow
                    key={item.id}
                    tone={rowToneFromTone(item.tone)}
                    title={item.title}
                    detail={item.detail}
                    meta={item.meta}
                    onClick={item.onClick}
                  />
                ))}
              </ListPanel>

              <ListPanel eyebrow="Recent execution" title="Recent Runs">
                {visibleRuns.length > 0 ? (
                  visibleRuns.slice(0, 8).map((run) => (
                    <StatusRow
                      key={run.run_id}
                      tone={rowToneFromRunStatus(run.status)}
                      title={runDisplayName(run)}
                      detail={`${run.status} · ${run.total_jobs > 0 ? `${run.completed_jobs}/${run.total_jobs} jobs` : 'Awaiting jobs'}`}
                      meta={run.created_at ? timeAgo(run.created_at) : 'Queued now'}
                      onClick={() => onViewRun(run.run_id)}
                    />
                  ))
                ) : (
                  <EmptyStateExplainer
                    title="No recent runs"
                    why="Trigger a workflow and the latest executions appear here with one-click inspection."
                    actionLabel="Open Moon"
                    onAction={onNewWorkflow}
                  />
                )}
              </ListPanel>

              <ListPanel
                eyebrow="Attached context"
                title="Knowledge Base"
                action={
                  <Button size="sm" tone="ghost" onClick={() => instanceFileRef.current?.click()}>
                    Add file
                  </Button>
                }
              >
                {instanceFiles.length > 0 ? (
                  instanceFiles.map((file) => (
                    <StatusRow
                      key={file.id}
                      tone="idle"
                      title={file.filename}
                      meta={
                        <Button size="sm" tone="danger" onClick={() => deleteInstanceFile(file.id)}>
                          Remove
                        </Button>
                      }
                    />
                  ))
                ) : instanceFilesError ? (
                  <EmptyStateExplainer
                    title="File inventory unavailable"
                    why={instanceFilesError}
                    actionLabel="Retry"
                    onAction={() => instanceFileRef.current?.click()}
                  />
                ) : (
                  <EmptyStateExplainer
                    title="No files attached"
                    why="Upload briefs, notes, or source files so the workspace can reason over them during execution."
                    actionLabel="Add file"
                    onAction={() => instanceFileRef.current?.click()}
                  />
                )}
              </ListPanel>

              <ListPanel
                eyebrow="Repeated work"
                title="Tool Opportunities"
                count={loading ? '…' : (snapshot?.tool_opportunities?.length ?? 0)}
              >
                {(snapshot?.tool_opportunities?.length ?? 0) > 0 ? (
                  (snapshot?.tool_opportunities ?? []).map((opp) => {
                    const label = toolOpportunityLabel(opp);
                    const detail = toolOpportunityDetail(opp);
                    const tone: 'healthy' | 'neutral' = opp.distinct_surfaces > 1 ? 'healthy' : 'neutral';
                    const meta = [
                      `${opp.occurrence_count}×`,
                      opp.distinct_surfaces > 1 ? `${opp.distinct_surfaces} surfaces` : null,
                      opp.last_seen ? timeAgo(opp.last_seen) : null,
                    ]
                      .filter(Boolean)
                      .join(' · ');
                    return (
                      <StatusRow
                        key={opp.shape_hash}
                        tone={rowToneFromTone(tone)}
                        title={label}
                        detail={detail}
                        detailMono
                        meta={meta}
                        onClick={onDescribe}
                      />
                    );
                  })
                ) : (
                  <EmptyStateExplainer
                    title="Nothing repeating yet"
                    why="Patterns appear here once the same action shape is captured 3+ times — a signal that it's worth folding into a tool."
                    actionLabel="Open builder"
                    onAction={onDescribe}
                  />
                )}
              </ListPanel>
            </aside>
          </div>
        </div>
      </main>
    </div>
  );
}
