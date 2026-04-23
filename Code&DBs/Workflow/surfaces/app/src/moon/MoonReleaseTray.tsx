import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { ReleaseStatus } from './moonBuildPresenter';
import type { BuildPayload } from '../shared/types';
import { planDefinition, commitDefinition, triggerWorkflow, createWorkflow } from '../shared/buildController';
import { resolveReleasePlanSource } from '../shared/buildGraphDefinition';

interface PlannedReleaseState {
  compiled_spec?: {
    jobs?: Array<{
      label?: string;
      agent?: string;
      depends_on?: string[];
      prompt?: string;
    }>;
    triggers?: Array<{
      event_type?: string;
      source_ref?: string;
      title?: string;
    }>;
  };
  definition?: Record<string, unknown>;
  buildGraph?: BuildPayload['build_graph'] | null;
  fingerprint: string;
  title: string;
  workflow?: BuildPayload['workflow'] | null;
}

interface Props {
  release: ReleaseStatus;
  payload: BuildPayload | null;
  workflowId: string | null;
  onClose: () => void;
  onSelectNode?: (nodeId: string) => void;
  onOpenDock?: (dock: string) => void;
  onViewRun?: (runId: string) => void;
  onDispatchSuccess?: (runId: string) => void;
  onWorkflowCreated?: (workflowId: string) => void;
}

type PillState = 'blocked' | 'plan-needed' | 'stale' | 'ready' | 'dispatching' | 'confirming' | 'done' | 'error';

function summarizeTriggers(triggers: Array<{ event_type?: string; source_ref?: string; title?: string }>): string {
  const labels = triggers
    .map(trigger => trigger.event_type || trigger.source_ref || trigger.title || 'trigger')
    .filter((label, index, all) => all.indexOf(label) === index);
  if (labels.length === 0) return 'no trigger';
  if (labels.length <= 3) return labels.join(' + ');
  return `${labels.slice(0, 3).join(' + ')} + ${labels.length - 3} more`;
}

function disabledReason(release: ReleaseStatus, payload: BuildPayload | null, jobCount: number): string | null {
  if (!payload) return 'No workflow — build first';
  const blockingIssue = (payload.build_issues || []).find(issue => issue.severity === 'blocking');
  if (blockingIssue) return blockingIssue.summary || blockingIssue.label || 'Resolve blocking build issues first';
  const projectionState = payload.projection_status?.state || (payload.build_graph as any)?.projection_status?.state;
  if (projectionState === 'blocked') return 'Resolve blocking build issues first';
  const firstBlocker = release.blockers[0];
  if (release.readiness === 'blocked' && firstBlocker) return firstBlocker.message;
  const hasDefinition = payload.definition && Object.keys(payload.definition).length > 0;
  const hasNodes = (payload.build_graph?.nodes || []).some(n => n.route);
  if (!hasDefinition && !hasNodes) return 'No steps configured — pick a trigger and add steps';
  if (jobCount === 0 && !hasNodes) return 'No jobs projected — compile first';
  return null;
}

function blockerActionLabel(issue: NonNullable<BuildPayload['build_issues']>[number]): string {
  if (issue.kind === 'missing_route') return 'Choose route';
  if (issue.kind === 'missing_workflow_target') return 'Choose target';
  return 'Resolve';
}

export function MoonReleaseTray({
  release,
  payload,
  workflowId,
  onClose,
  onSelectNode,
  onOpenDock,
  onViewRun,
  onDispatchSuccess,
  onWorkflowCreated,
}: Props) {
  const [dispatching, setDispatching] = useState(false);
  const [dispatchResult, setDispatchResult] = useState<string | null>(null);
  const [dispatchError, setDispatchError] = useState<string | null>(null);
  const [planning, setPlanning] = useState(false);
  const [plannedRelease, setPlannedRelease] = useState<PlannedReleaseState | null>(null);
  const [planInvalidated, setPlanInvalidated] = useState(false);
  const [confirmingDispatch, setConfirmingDispatch] = useState(false);
  const [planExpanded, setPlanExpanded] = useState(false);

  const releaseSource = useMemo(() => resolveReleasePlanSource(payload), [payload]);
  const releaseFingerprint = releaseSource?.fingerprint ?? null;
  const releaseFingerprintRef = useRef<string | null>(releaseFingerprint);
  const projectedJobs = payload?.compiled_spec_projection?.compiled_spec?.jobs || [];
  const activePlannedRelease = useMemo(() => {
    if (!plannedRelease || !releaseSource) return null;
    return plannedRelease.fingerprint === releaseSource.fingerprint ? plannedRelease : null;
  }, [plannedRelease, releaseSource]);
  const plannedJobs = activePlannedRelease?.compiled_spec?.jobs || [];
  const jobs = activePlannedRelease ? plannedJobs : projectedJobs;
  const projectedTriggers = payload?.compiled_spec_projection?.compiled_spec?.triggers || [];
  const plannedTriggers = activePlannedRelease?.compiled_spec?.triggers || [];
  const triggers = plannedTriggers.length > 0 ? plannedTriggers : projectedTriggers;
  const hasFullPlan = activePlannedRelease !== null;
  const agentSummary = (hasFullPlan ? plannedJobs : projectedJobs).reduce<Record<string, number>>((acc, job) => {
    const agent = job.agent || 'auto/build';
    acc[agent] = (acc[agent] || 0) + 1;
    return acc;
  }, {});
  const topAgents = Object.entries(agentSummary)
    .sort(([, a], [, b]) => b - a)
    .slice(0, 2)
    .map(([agent, count]) => agentSummary[agent] > 1 ? `${agent}×${count}` : agent)
    .join(' · ');
  const triggerSummary = summarizeTriggers(triggers);

  useEffect(() => {
    releaseFingerprintRef.current = releaseFingerprint;
  }, [releaseFingerprint]);

  useEffect(() => {
    if (!plannedRelease) return;
    if (!releaseSource || plannedRelease.fingerprint !== releaseSource.fingerprint) {
      setPlannedRelease(null);
      setConfirmingDispatch(false);
      setPlanInvalidated(true);
    }
  }, [plannedRelease, releaseSource]);

  const handlePlan = useCallback(async () => {
    if (!releaseSource) return;
    const requestFingerprint = releaseSource.fingerprint;
    setPlanning(true);
    setDispatchError(null);
    setConfirmingDispatch(false);
    setPlanInvalidated(false);
    try {
      const result = await planDefinition({
        workflowId: payload?.workflow?.id ?? workflowId,
        title: releaseSource.title,
        definition: releaseSource.definition,
        buildGraph: releaseSource.buildGraph,
      });
      if (releaseFingerprintRef.current !== requestFingerprint) {
        setPlanInvalidated(true);
        return;
      }
      setPlannedRelease({
        ...result,
        definition: releaseSource.definition,
        buildGraph: releaseSource.buildGraph,
        fingerprint: releaseSource.fingerprint,
        title: releaseSource.title,
      });
      const nextWorkflowId = result?.workflow?.id;
      if (nextWorkflowId && nextWorkflowId !== workflowId) onWorkflowCreated?.(nextWorkflowId);
    } catch (e: any) {
      setDispatchError(e.message || 'Planning failed');
    } finally {
      setPlanning(false);
    }
  }, [onWorkflowCreated, releaseSource, workflowId]);

  const reason = disabledReason(release, payload, jobs.length);
  const canPlan = !reason;
  const canDispatch = !reason && hasFullPlan && plannedJobs.length > 0;

  const handleDispatch = useCallback(() => {
    if (!canDispatch) return;
    setDispatchError(null);
    setConfirmingDispatch(true);
  }, [canDispatch]);

  const handleConfirmDispatch = useCallback(async () => {
    if (!activePlannedRelease) return;
    setDispatching(true);
    setConfirmingDispatch(false);
    setDispatchError(null);
    setDispatchResult(null);
    try {
      let wfId = workflowId || activePlannedRelease.workflow?.id || null;
      const { definition, buildGraph, title, compiled_spec } = activePlannedRelease;
      if (!wfId) {
        const created = await createWorkflow(title, { definition, buildGraph, compiled_spec });
        wfId = created.id || (created as any).workflow_id;
        if (!wfId) throw new Error('Failed to create workflow');
        onWorkflowCreated?.(wfId);
      }
      await commitDefinition(wfId, {
        title,
        definition,
        buildGraph,
        compiled_spec,
      });
      const result = await triggerWorkflow(wfId);
      const runId = result.run_id;
      setDispatchResult(runId || 'submitted');
      if (runId && onDispatchSuccess) {
        onDispatchSuccess(runId);
      } else if (runId && onViewRun) {
        onViewRun(runId);
      }
    } catch (e: any) {
      setDispatchError(e.message || 'Dispatch failed');
    } finally {
      setDispatching(false);
    }
  }, [workflowId, activePlannedRelease, onDispatchSuccess, onViewRun, onWorkflowCreated]);

  // Pill state drives the ring geometry & animation. One visual per mode.
  const pillState: PillState = dispatchError
    ? 'error'
    : dispatchResult
      ? 'done'
      : dispatching
        ? 'dispatching'
        : confirmingDispatch
          ? 'confirming'
          : reason
            ? 'blocked'
            : planInvalidated
              ? 'stale'
              : !hasFullPlan
                ? 'plan-needed'
                : 'ready';

  const summaryLine = reason
    ? reason
    : `${jobs.length} job${jobs.length === 1 ? '' : 's'}${topAgents ? ` · ${topAgents}` : ''} · triggered by ${triggerSummary}`;

  const passedChecks = release.checklist.filter(c => c.passed).length;
  const totalChecks = release.checklist.length;
  const failedChecks = release.checklist.filter(c => !c.passed);
  const firstBlocker = release.blockers[0];
  const blockingIssues = (payload?.build_issues || []).filter(issue => issue.severity === 'blocking');

  return (
    <>
      <button className="moon-dock__close" onClick={onClose} aria-label="Close release tray">&times;</button>
      <div className="moon-dock__title">Release</div>
      <div className="moon-dock__sep" />

      <div className={`moon-release moon-release--${pillState}`}>
        {/* Summary row — the charged statement of intent. */}
        <div className="moon-release__summary" aria-live="polite">
          <span className="moon-release__summary-line">{summaryLine}</span>
          {totalChecks > 0 && (
            <span className="moon-release__summary-checks" aria-label={`${passedChecks} of ${totalChecks} checks passed`}>
              {passedChecks}/{totalChecks} checks
            </span>
          )}
        </div>

        {/* The commit pill itself — rotating dotted ring around a wide button.
            Ring language tracks the edge-grammar (dashed = after_any). */}
        <div className={`moon-release__pill moon-release__pill--${pillState}`}>
          <span className="moon-release__pill-ring" aria-hidden="true" />
          <span className="moon-release__pill-ring moon-release__pill-ring--inner" aria-hidden="true" />
          {dispatchResult ? (
            <div className="moon-release__pill-inner moon-release__pill-inner--done">
              <span className="moon-release__pill-label">Run dispatched</span>
              <button
                type="button"
                className="moon-release__run-link"
                onClick={() => onViewRun?.(dispatchResult)}
              >
                View run
              </button>
            </div>
          ) : confirmingDispatch && activePlannedRelease ? (
            <div className="moon-release__pill-inner moon-release__pill-inner--confirming">
              <span className="moon-release__pill-label">
                Commit {plannedJobs.length} job{plannedJobs.length === 1 ? '' : 's'}?
              </span>
              <div className="moon-release__pill-actions">
                <button
                  type="button"
                  className="moon-release__pill-btn moon-release__pill-btn--cancel"
                  onClick={() => setConfirmingDispatch(false)}
                  disabled={dispatching}
                >
                  Cancel
                </button>
                <button
                  type="button"
                  className="moon-release__pill-btn moon-release__pill-btn--confirm"
                  onClick={handleConfirmDispatch}
                  disabled={dispatching}
                >
                  Confirm Release
                </button>
              </div>
            </div>
          ) : !hasFullPlan ? (
            <>
              <button
                type="button"
                className="moon-release__pill-inner moon-release__pill-inner--plan"
                onClick={handlePlan}
                disabled={planning || !canPlan}
                aria-label="Preview plan"
              >
                <span className="moon-release__pill-label">
                  {planning ? 'Previewing…' : 'Preview plan'}
                </span>
                <span className="moon-release__pill-sub">
                  {planInvalidated ? 'plan went stale' : 'lock the final job list'}
                </span>
              </button>
              {/* Ghost dispatch affordance — tells you what the pill becomes
                  after preview, without being clickable yet. */}
              <button
                type="button"
                className="moon-release__pill-ghost"
                aria-label="Dispatch"
                disabled
                tabIndex={-1}
              >
                Dispatch
              </button>
            </>
          ) : (
            <button
              type="button"
              className="moon-release__pill-inner moon-release__pill-inner--dispatch"
              onClick={handleDispatch}
              disabled={dispatching || !canDispatch}
              aria-label="Dispatch"
            >
              <span className="moon-release__pill-label">
                {dispatching ? 'Dispatching…' : 'Dispatch'}
              </span>
              <span className="moon-release__pill-sub">
                sweep the ring closed
              </span>
            </button>
          )}
        </div>

        {/* Stale/blocker hint — inline beneath the pill, never a separate pane. */}
        {planInvalidated && !dispatchResult && (
          <div className="moon-release__hint moon-release__hint--stale">
            The workflow changed after preview. Preview plan again before dispatch.
          </div>
        )}
        {firstBlocker && !dispatchResult && blockingIssues.length === 0 && (
          <div className="moon-release__hint moon-release__hint--blocker">
            <span className="moon-release__hint-dot" aria-hidden="true" />
            <span>{firstBlocker.message}</span>
            {firstBlocker.nodeIds.length > 0 && onSelectNode && (
              <button
                type="button"
                className="moon-release__hint-link"
                onClick={() => onSelectNode(firstBlocker.nodeIds[0])}
              >
                locate
              </button>
            )}
            {release.blockers.length > 1 && (
              <span className="moon-release__hint-more">+{release.blockers.length - 1} more</span>
            )}
          </div>
        )}
        {blockingIssues.length > 0 && !dispatchResult && (
          <div className="moon-release__blocker-panel" aria-label="Blocking build issues">
            <div className="moon-release__blocker-panel-head">
              <span className="moon-release__blocker-panel-kicker">Needs decisions</span>
              <span className="moon-release__blocker-panel-count">{blockingIssues.length}</span>
            </div>
            <div className="moon-release__blocker-list">
              {blockingIssues.map(issue => (
                <button
                  key={issue.issue_id}
                  type="button"
                  className="moon-release__blocker-card"
                  onClick={() => issue.node_id && onSelectNode?.(issue.node_id)}
                  disabled={!issue.node_id || !onSelectNode}
                >
                  <span className="moon-release__blocker-card-action">{blockerActionLabel(issue)}</span>
                  <span className="moon-release__blocker-card-copy">
                    <strong>{issue.label || 'Blocked step'}</strong>
                    <span>{issue.summary || 'This must be resolved before the workflow can run.'}</span>
                  </span>
                </button>
              ))}
            </div>
          </div>
        )}
        {dispatchError && (
          <div className="moon-release__hint moon-release__hint--error">
            {dispatchError}
          </div>
        )}

        {/* Expandable plan preview — collapsed by default. */}
        {(jobs.length > 0 || failedChecks.length > 0) && !dispatchResult && (
          <details
            className="moon-release__plan"
            open={planExpanded}
            onToggle={(e) => setPlanExpanded((e.currentTarget as HTMLDetailsElement).open)}
          >
            <summary className="moon-release__plan-summary">
              <span>Show plan</span>
              <span className="moon-release__plan-summary-meta">
                {hasFullPlan ? 'planned' : 'projected'}
              </span>
            </summary>
            <div className="moon-release__plan-body">
              {/* Route ribbon — ordered step labels, one line. */}
              {jobs.length > 0 && (
                <div className="moon-release__route">
                  {jobs.map((j: any, i: number) => (
                    <React.Fragment key={i}>
                      {i > 0 && <span className="moon-release__route-arrow">&rarr;</span>}
                      <span className="moon-release__route-step">{j.label || '?'}</span>
                    </React.Fragment>
                  ))}
                </div>
              )}

              {/* Jobs. Compact rows, full metadata only when planned. */}
              {jobs.length > 0 && (
                <div className="moon-release__plan-jobs">
                  {jobs.map((j: any, i: number) => (
                    <div key={i} className="moon-release__job">
                      <div className="moon-release__job-main">
                        <span className="moon-release__job-label">{j.label || `job-${i}`}</span>
                        <span className="moon-release__job-agent">{j.agent || 'auto/build'}</span>
                      </div>
                      {hasFullPlan && j.depends_on?.length > 0 && (
                        <span className="moon-release__job-meta">
                          after: {j.depends_on.join(', ')}
                        </span>
                      )}
                      {hasFullPlan && j.prompt && (
                        <span className="moon-release__job-meta moon-release__job-meta--truncate">
                          {j.prompt.slice(0, 80)}{j.prompt.length > 80 ? '…' : ''}
                        </span>
                      )}
                    </div>
                  ))}
                </div>
              )}

              {/* Failing checklist items — keep the "Fix" affordance for fast jumps. */}
              {failedChecks.length > 0 && (
                <div className="moon-release__plan-checks">
                  <div className="moon-dock__section-label">Unblock</div>
                  {failedChecks.map((item, i) => (
                    <div key={i} className="moon-proof-check">
                      <div className="moon-proof-dot moon-proof-dot--blocked" />
                      <span>{item.message}</span>
                      {item.nodeId && onSelectNode && (
                        <button
                          type="button"
                          className="moon-dock-form__btn moon-dock-form__btn--small"
                          onClick={() => {
                            onSelectNode(item.nodeId!);
                            if (item.dock && onOpenDock) onOpenDock(item.dock);
                          }}
                        >
                          Fix
                        </button>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          </details>
        )}
      </div>
    </>
  );
}
