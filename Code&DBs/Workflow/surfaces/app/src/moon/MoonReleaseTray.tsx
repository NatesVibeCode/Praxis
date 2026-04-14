import React, { useState, useCallback } from 'react';
import type { ReleaseStatus } from './moonBuildPresenter';
import type { BuildPayload } from '../shared/types';
import { planDefinition, commitDefinition, triggerWorkflow, createWorkflow } from '../shared/buildController';
import { buildGraphToDefinition } from '../shared/buildGraphDefinition';

interface PlannedReleaseState {
  compiled_spec?: {
    jobs?: Array<{
      label?: string;
      agent?: string;
      depends_on?: string[];
      prompt?: string;
    }>;
  };
  definition: Record<string, unknown>;
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
  const [confirmingDispatch, setConfirmingDispatch] = useState(false);

  const projectedJobs = payload?.compiled_spec_projection?.compiled_spec?.jobs || [];
  const plannedJobs = plannedRelease?.compiled_spec?.jobs || [];
  const jobs = plannedRelease ? plannedJobs : projectedJobs;
  const triggers = payload?.compiled_spec_projection?.compiled_spec?.triggers || [];
  const hasFullPlan = plannedRelease !== null;
  const agentSummary = plannedJobs.reduce<Record<string, number>>((acc, job) => {
    const agent = job.agent || 'auto/build';
    acc[agent] = (acc[agent] || 0) + 1;
    return acc;
  }, {});

  const handlePlan = useCallback(async () => {
    if (!payload) return;
    setPlanning(true);
    setDispatchError(null);
    setConfirmingDispatch(false);
    try {
      const definition = (payload.definition && Object.keys(payload.definition).length > 0)
        ? payload.definition as Record<string, unknown>
        : buildGraphToDefinition(payload.build_graph);
      const title = String(payload.workflow?.name || (definition as any)?.title || 'moon-workflow');
      const result = await planDefinition(definition, title);
      setPlannedRelease({
        ...result,
        definition,
        title,
      });
    } catch (e: any) {
      setDispatchError(e.message || 'Planning failed');
    } finally {
      setPlanning(false);
    }
  }, [payload]);
  const reason = disabledReason(release, payload, jobs.length);
  const canPlan = !reason;
  const canDispatch = !reason && hasFullPlan && plannedJobs.length > 0;

  const handleDispatch = useCallback(() => {
    if (!canDispatch) return;
    setDispatchError(null);
    setConfirmingDispatch(true);
  }, [canDispatch]);

  const handleConfirmDispatch = useCallback(async () => {
    if (!plannedRelease) return;
    setDispatching(true);
    setConfirmingDispatch(false);
    setDispatchError(null);
    setDispatchResult(null);
    try {
      let wfId = workflowId;
      const { definition, title, compiled_spec } = plannedRelease;
      if (!wfId) {
        const created = await createWorkflow(title, definition);
        wfId = created.id || (created as any).workflow_id;
        if (!wfId) throw new Error('Failed to create workflow');
      }
      await commitDefinition(wfId, {
        title,
        definition,
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
  }, [workflowId, plannedRelease, onViewRun, onDispatchSuccess]);

  return (
    <>
      <button className="moon-dock__close" onClick={onClose} aria-label="Close release tray">&times;</button>
      <div className="moon-dock__title">Release</div>
      <div className="moon-dock__sep" />

      <div className="moon-release__columns">
        {/* Column 1: What fires */}
        <div className="moon-release__col">
          <div className="moon-dock__section-label">
            Projected jobs ({jobs.length})
          </div>
          {jobs.length > 0 ? jobs.map((j: any, i: number) => (
            <div key={i} className="moon-release__job">
              <span className="moon-release__job-label">{j.label || `job-${i}`}</span>
              <span className="moon-release__job-agent">{j.agent || 'auto/build'}</span>
              {hasFullPlan && j.depends_on?.length > 0 && (
                <span className="moon-release__job-deps" style={{ fontSize: 10, color: '#8b949e', display: 'block' }}>
                  after: {j.depends_on.join(', ')}
                </span>
              )}
              {hasFullPlan && j.prompt && (
                <span className="moon-release__job-prompt" style={{ fontSize: 10, color: '#8b949e', display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 260 }}>
                  {j.prompt.slice(0, 80)}{j.prompt.length > 80 ? '...' : ''}
                </span>
              )}
            </div>
          )) : (
            <div className="moon-dock__empty">No jobs yet.</div>
          )}

          {triggers.length > 0 && (
            <>
              <div className="moon-dock__section-label" style={{ marginTop: 12 }}>
                Triggers ({triggers.length})
              </div>
              {triggers.map((t, i) => (
                <div key={i} className="moon-release__job">
                  <span className="moon-release__job-label">{t.event_type || t.source_ref || 'trigger'}</span>
                  {t.source_ref && <span className="moon-release__job-agent">{t.source_ref}</span>}
                </div>
              ))}
            </>
          )}

          {/* Route */}
          {jobs.length > 0 && (
            <>
              <div className="moon-dock__section-label" style={{ marginTop: 12 }}>Route</div>
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

        {/* Column 2: Readiness + blockers */}
        <div className="moon-release__col">
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
              <div className="moon-dock__section-label" style={{ marginTop: 12 }}>
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

        {/* Column 3: Plan + Dispatch */}
        <div className="moon-release__col moon-release__col--dispatch">
          <div className="moon-dock__section-label">Dispatch</div>

          {!hasFullPlan && !dispatchResult && (
            <button
              className="moon-release__dispatch-btn"
              onClick={handlePlan}
              disabled={planning || !canPlan}
              style={{ marginBottom: 8 }}
            >
              {planning ? 'Planning...' : 'Preview plan'}
            </button>
          )}

          {dispatchResult ? (
            <div className="moon-release__result">
              <div className="moon-release__result-label">Run dispatched</div>
              <button
                className="moon-release__run-link"
                style={{ background: 'none', border: '1px solid var(--moon-accent)', color: 'var(--moon-accent)', padding: '6px 12px', borderRadius: 6, cursor: 'pointer', font: 'inherit', fontSize: 12 }}
                onClick={() => onViewRun?.(dispatchResult)}
              >
                View Run →
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
              {!hasFullPlan && !dispatchResult && (
                <div className="moon-release__blocked-reason">
                  Plan the release to lock the final job list before dispatch.
                </div>
              )}
              {confirmingDispatch && plannedRelease && (
                <div
                  style={{
                    marginTop: 12,
                    padding: 12,
                    border: '1px solid rgba(124, 92, 255, 0.35)',
                    borderRadius: 10,
                    background: 'rgba(124, 92, 255, 0.08)',
                  }}
                >
                  <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 6 }}>Confirm release</div>
                  <div style={{ fontSize: 12, color: '#c9d1d9', marginBottom: 8 }}>
                    {plannedJobs.length} job{plannedJobs.length === 1 ? '' : 's'} will be dispatched.
                  </div>
                  <div style={{ fontSize: 11, color: '#8b949e', marginBottom: 10 }}>
                    Agents: {Object.entries(agentSummary).map(([agent, count]) => `${agent} (${count})`).join(', ')}
                  </div>
                  <div style={{ display: 'flex', gap: 8 }}>
                    <button
                      className="moon-release__dispatch-btn moon-release__dispatch-btn--ready"
                      onClick={handleConfirmDispatch}
                      disabled={dispatching}
                      style={{ flex: 1 }}
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
    </>
  );
}
