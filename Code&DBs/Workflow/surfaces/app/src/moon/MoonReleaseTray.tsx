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
  };
  definition?: Record<string, unknown>;
  buildGraph?: BuildPayload['build_graph'] | null;
  fingerprint: string;
  title: string;
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
}

function disabledReason(release: ReleaseStatus, payload: BuildPayload | null, jobCount: number): string | null {
  if (!payload) return 'No workflow — build first';
  const hasDefinition = payload.definition && Object.keys(payload.definition).length > 0;
  const hasNodes = (payload.build_graph?.nodes || []).some(n => n.route);
  if (!hasDefinition && !hasNodes) return 'No steps configured — pick a trigger and add steps';
  if (jobCount === 0 && !hasNodes) return 'No jobs projected — compile first';
  return null;
}

export function MoonReleaseTray({ release, payload, workflowId, onClose, onSelectNode, onOpenDock, onViewRun, onDispatchSuccess }: Props) {
  const [dispatching, setDispatching] = useState(false);
  const [dispatchResult, setDispatchResult] = useState<string | null>(null);
  const [dispatchError, setDispatchError] = useState<string | null>(null);
  const [planning, setPlanning] = useState(false);
  const [plannedRelease, setPlannedRelease] = useState<PlannedReleaseState | null>(null);
  const [planInvalidated, setPlanInvalidated] = useState(false);
  const [confirmingDispatch, setConfirmingDispatch] = useState(false);

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
  const triggers = payload?.compiled_spec_projection?.compiled_spec?.triggers || [];
  const hasFullPlan = activePlannedRelease !== null;
  const agentSummary = plannedJobs.reduce<Record<string, number>>((acc, job) => {
    const agent = job.agent || 'auto/build';
    acc[agent] = (acc[agent] || 0) + 1;
    return acc;
  }, {});
  const agentSummaryText = Object.entries(agentSummary)
    .map(([agent, count]) => `${agent} (${count})`)
    .join(', ');

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
    } catch (e: any) {
      setDispatchError(e.message || 'Planning failed');
    } finally {
      setPlanning(false);
    }
  }, [releaseSource]);
  const reason = disabledReason(release, payload, jobs.length);
  const canPlan = !reason;
  const canDispatch = !reason && hasFullPlan && plannedJobs.length > 0;
  const planGuidance = planInvalidated
    ? 'The workflow changed after preview. Preview plan again before dispatch.'
    : !hasFullPlan && !dispatchResult
      ? 'Plan the release to lock the final job list before dispatch.'
      : null;

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
      let wfId = workflowId;
      const { definition, buildGraph, title, compiled_spec } = activePlannedRelease;
      if (!wfId) {
        const created = await createWorkflow(title, { definition, buildGraph, compiled_spec });
        wfId = created.id || (created as any).workflow_id;
        if (!wfId) throw new Error('Failed to create workflow');
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
  }, [workflowId, activePlannedRelease, onViewRun, onDispatchSuccess]);

  return (
    <>
      <button className="moon-dock__close" onClick={onClose} aria-label="Close release tray">&times;</button>
      <div className="moon-dock__title">Release</div>
      <div className="moon-dock__sep" />

      <div className="moon-release__columns">
        {/* Column 1: What fires */}
        <div className="moon-release__col">
          <div className="moon-release__panel">
            <div className="moon-dock__section-label">
              Projected jobs ({jobs.length})
            </div>
            {jobs.length > 0 ? jobs.map((j: any, i: number) => (
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
                    {j.prompt.slice(0, 80)}{j.prompt.length > 80 ? '...' : ''}
                  </span>
                )}
              </div>
            )) : (
              <div className="moon-dock__empty">No jobs yet.</div>
            )}

            {triggers.length > 0 && (
              <>
                <div className="moon-dock__section-label moon-release__section-spacer">
                  Triggers ({triggers.length})
                </div>
                {triggers.map((t, i) => (
                  <div key={i} className="moon-release__job moon-release__job--trigger">
                    <div className="moon-release__job-main">
                      <span className="moon-release__job-label">{t.event_type || t.source_ref || 'trigger'}</span>
                      {t.source_ref && <span className="moon-release__job-agent">{t.source_ref}</span>}
                    </div>
                  </div>
                ))}
              </>
            )}

            {jobs.length > 0 && (
              <>
                <div className="moon-dock__section-label moon-release__section-spacer">Route</div>
                <div className="moon-release__route">
                  {jobs.map((j: any, i: number) => (
                    <React.Fragment key={i}>
                      {i > 0 && <span className="moon-release__route-arrow">&rarr;</span>}
                      <span className="moon-release__route-step">{j.label || '?'}</span>
                    </React.Fragment>
                  ))}
                </div>
              </>
            )}
          </div>
        </div>

        {/* Column 2: Readiness + blockers */}
        <div className="moon-release__col">
          <div className="moon-release__panel">
            <div className="moon-dock__section-label">Readiness</div>

            <div className="moon-proof-checks">
              {release.checklist.map((item, i) => (
                <div key={i} className="moon-proof-check">
                  <div className={`moon-proof-dot${!item.passed ? ' moon-proof-dot--blocked' : ''}`} />
                  <span>{item.message}</span>
                  {!item.passed && item.nodeId && onSelectNode && (
                    <button
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

            {release.blockers.length > 0 && (
              <>
                <div className="moon-dock__section-label moon-release__section-spacer">
                  Blockers
                </div>
                {release.blockers.map((b, i) => (
                  <div key={i} className="moon-release__blocker">
                    <span>{b.message}</span>
                    {b.nodeIds.length > 0 && (
                      <span className="moon-release__blocker-nodes">
                        {b.nodeIds.join(', ')}
                      </span>
                    )}
                  </div>
                ))}
              </>
            )}
          </div>
        </div>

        {/* Column 3: Plan + Dispatch */}
        <div className="moon-release__col moon-release__col--dispatch">
          <div className="moon-release__panel moon-release__panel--dispatch">
            <div className="moon-dock__section-label">Dispatch</div>

            {!hasFullPlan && !dispatchResult && (
              <button
                className="moon-release__dispatch-btn moon-release__dispatch-btn--secondary"
                onClick={handlePlan}
                disabled={planning || !canPlan}
              >
                {planning ? 'Planning...' : 'Preview plan'}
              </button>
            )}

            {dispatchResult ? (
              <div className="moon-release__result">
                <div className="moon-release__result-label">Run dispatched</div>
                <button
                  className="moon-release__run-link"
                  onClick={() => onViewRun?.(dispatchResult)}
                >
                  View Run
                </button>
              </div>
            ) : (
              <>
                <button
                  className={`moon-release__dispatch-btn${canDispatch ? ' moon-release__dispatch-btn--ready' : ''}`}
                  onClick={handleDispatch}
                  disabled={dispatching || !canDispatch}
                >
                  {dispatching ? 'Dispatching...' : 'Dispatch'}
                </button>
                {planGuidance && (
                  <div className="moon-release__blocked-reason">
                    {planGuidance}
                  </div>
                )}
                {confirmingDispatch && activePlannedRelease && (
                  <div className="moon-release__confirm">
                    <div className="moon-release__confirm-title">Confirm release</div>
                    <div className="moon-release__confirm-body">
                      {plannedJobs.length} job{plannedJobs.length === 1 ? '' : 's'} will be dispatched.
                    </div>
                    <div className="moon-release__confirm-meta">
                      Agents: {agentSummaryText}
                    </div>
                    <div className="moon-release__confirm-actions">
                      <button
                        className="moon-release__dispatch-btn moon-release__dispatch-btn--ready"
                        onClick={handleConfirmDispatch}
                        disabled={dispatching}
                      >
                        Confirm Release
                      </button>
                      <button
                        className="moon-dock-form__btn"
                        onClick={() => setConfirmingDispatch(false)}
                        disabled={dispatching}
                      >
                        Cancel
                      </button>
                    </div>
                  </div>
                )}
                {reason && (
                  <div className="moon-release__blocked-reason">{reason}</div>
                )}
              </>
            )}

            {dispatchError && (
              <div className="moon-dock-form__error">{dispatchError}</div>
            )}
          </div>
        </div>
      </div>
    </>
  );
}
