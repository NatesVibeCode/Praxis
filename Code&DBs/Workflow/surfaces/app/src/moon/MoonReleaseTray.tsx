import React, { useState, useCallback } from 'react';
import type { ReleaseStatus } from './moonBuildPresenter';
import type { BuildPayload } from '../shared/types';
import { planDefinition, commitDefinition, triggerWorkflow, createWorkflow } from '../shared/buildController';

function buildGraphToDefinition(buildGraph: BuildPayload['build_graph']): Record<string, unknown> {
  const nodes = buildGraph?.nodes || [];
  const edges = buildGraph?.edges || [];
  const incoming: Record<string, string[]> = {};
  for (const e of edges) {
    if (e.kind === 'authority_gate') continue;
    if (e.to_node_id && e.from_node_id) {
      incoming[e.to_node_id] = incoming[e.to_node_id] || [];
      incoming[e.to_node_id].push(e.from_node_id);
    }
  }
  const stepNodes = nodes.filter(n => !n.kind || n.kind === 'step');
  const draft_flow = stepNodes.map((n, i) => ({
    id: n.node_id,
    order: i,
    title: n.title || `Step ${i + 1}`,
    summary: n.summary || n.title || '',
    depends_on: incoming[n.node_id] || [],
    source_block_ids: n.source_block_ids || [],
  }));
  const phases = stepNodes
    .filter(n => n.route)
    .map(n => ({ step_id: n.node_id, agent_route: n.route! }));
  return { draft_flow, execution_setup: { phases } };
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
  const [plannedSpec, setPlannedSpec] = useState<any>(null);

  const projectedJobs = payload?.compiled_spec_projection?.compiled_spec?.jobs || [];
  const plannedJobs = plannedSpec?.compiled_spec?.jobs || [];
  const jobs = plannedJobs.length > 0 ? plannedJobs : projectedJobs;
  const triggers = payload?.compiled_spec_projection?.compiled_spec?.triggers || [];
  const hasFullPlan = plannedJobs.length > 0;

  const handlePlan = useCallback(async () => {
    if (!payload) return;
    setPlanning(true);
    setDispatchError(null);
    try {
      const definition = (payload.definition && Object.keys(payload.definition).length > 0)
        ? payload.definition as Record<string, unknown>
        : buildGraphToDefinition(payload.build_graph);
      const result = await planDefinition(definition, payload.workflow?.name);
      setPlannedSpec(result);
    } catch (e: any) {
      setDispatchError(e.message || 'Planning failed');
    } finally {
      setPlanning(false);
    }
  }, [payload]);
  const reason = disabledReason(release, payload, jobs.length);
  const canDispatch = !reason;

  const handleDispatch = useCallback(async () => {
    if (!payload) return;
    setDispatching(true);
    setDispatchError(null);
    setDispatchResult(null);
    try {
      // Resolve definition — fall back to synthesizing from build_graph for manually-built chains
      const definition = (payload.definition && Object.keys(payload.definition).length > 0)
        ? payload.definition as Record<string, unknown>
        : buildGraphToDefinition(payload.build_graph);

      // 0. Create workflow if needed
      let wfId = workflowId;
      const title = String(payload.workflow?.name || (definition as any)?.title || 'moon-workflow');
      if (!wfId) {
        const created = await createWorkflow(title, definition);
        wfId = created.id || (created as any).workflow_id;
        if (!wfId) throw new Error('Failed to create workflow');
      }
      // 1. Plan: convert definition → full compiled spec
      const planResult = await planDefinition(definition, title);
      // 2. Commit: persist definition + compiled spec to DB
      await commitDefinition(wfId, {
        title,
        definition,
        compiled_spec: planResult.compiled_spec,
      });
      // 3. Trigger: dispatch via the correct endpoint
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
  }, [workflowId, payload, onViewRun, onDispatchSuccess]);

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
              disabled={planning || !canDispatch}
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
