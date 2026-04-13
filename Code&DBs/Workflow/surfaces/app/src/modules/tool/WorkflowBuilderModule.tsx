import React, { useEffect, useState } from 'react';
import { QuadrantProps } from '../types';
import { LoadingSkeleton } from '../../primitives/LoadingSkeleton';

type WorkflowStepStatus = 'pending' | 'running' | 'succeeded' | 'failed';

interface EditableWorkflowStep {
  id: string;
  model: string;
  prompt: string;
  dependsOnStepId: string | null;
}

interface WorkflowJobResult {
  label?: string;
  status?: string;
  stdout?: string;
  stderr?: string;
}

interface StepExecutionState {
  status: WorkflowStepStatus;
  stdoutPreview: string;
  stderrPreview: string;
}

let stepSequence = 0;

const statusColors: Record<WorkflowStepStatus, string> = {
  pending: 'var(--text-muted, #8b949e)',
  running: 'var(--warning, #d29922)',
  succeeded: 'var(--success, #3fb950)',
  failed: 'var(--danger, #f85149)',
};

const statusLabels: Record<WorkflowStepStatus, string> = {
  pending: 'Pending',
  running: 'Running',
  succeeded: 'Succeeded',
  failed: 'Failed',
};

const RUN_POLL_MS = 2000;
const RUN_TIMEOUT_MS = 300_000;

function nextStepId(): string {
  stepSequence += 1;
  return `workflow-builder-step-${stepSequence}`;
}

function createStep(defaultModel = ''): EditableWorkflowStep {
  return {
    id: nextStepId(),
    model: defaultModel,
    prompt: '',
    dependsOnStepId: null,
  };
}

function extractModelName(value: unknown): string {
  if (typeof value === 'string') return value;
  if (!value || typeof value !== 'object') return '';

  const model = value as {
    name?: unknown;
    provider_slug?: unknown;
    model_slug?: unknown;
  };

  if (typeof model.name === 'string' && model.name.trim()) {
    return model.name;
  }

  if (
    typeof model.provider_slug === 'string' &&
    model.provider_slug &&
    typeof model.model_slug === 'string' &&
    model.model_slug
  ) {
    return `${model.provider_slug}/${model.model_slug}`;
  }

  return '';
}

function extractModelOptions(data: unknown): string[] {
  const source = Array.isArray(data)
    ? data
    : data && typeof data === 'object'
      ? ((data as { models?: unknown; active_models?: unknown }).models
        ?? (data as { models?: unknown; active_models?: unknown }).active_models
        ?? [])
      : [];

  if (!Array.isArray(source)) return [];

  return Array.from(
    new Set(
      source
        .map(extractModelName)
        .filter((value): value is string => Boolean(value))
    )
  );
}

function normalizeStatus(status: string | undefined): WorkflowStepStatus {
  const normalized = status?.toLowerCase().trim();

  if (!normalized) return 'pending';
  if (['running', 'in_progress', 'in-progress', 'processing'].includes(normalized)) return 'running';
  if (['succeeded', 'success', 'done', 'completed', 'passed'].includes(normalized)) return 'succeeded';
  if (['failed', 'failure', 'error', 'errored'].includes(normalized)) return 'failed';

  return 'pending';
}

function previewText(value: unknown, limit = 280): string {
  if (typeof value !== 'string') return '';

  const trimmed = value.trim();
  if (!trimmed) return '';

  return trimmed.length > limit ? `${trimmed.slice(0, limit)}...` : trimmed;
}

function WorkflowBuilderModule({ config }: QuadrantProps) {
  const [models, setModels] = useState<string[]>([]);
  const [modelsError, setModelsError] = useState<string | null>(null);
  const [loadingModels, setLoadingModels] = useState(true);
  const [running, setRunning] = useState(false);
  const [runError, setRunError] = useState<string | null>(null);
  const [steps, setSteps] = useState<EditableWorkflowStep[]>(() => [createStep()]);
  const [resultsByStepId, setResultsByStepId] = useState<Record<string, StepExecutionState>>({});

  useEffect(() => {
    let cancelled = false;

    fetch('/api/models?task_type=build')
      .then(async (response) => {
        if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
        return response.json();
      })
      .then((data: unknown) => {
        if (cancelled) return;

        const names = extractModelOptions(data);
        setModels(names);
        setModelsError(names.length > 0 ? null : 'No models available.');
        setSteps((currentSteps) => currentSteps.map((step) => (
          step.model ? step : { ...step, model: names[0] ?? '' }
        )));
      })
      .catch((error: unknown) => {
        if (cancelled) return;

        setModels([]);
        setModelsError(error instanceof Error ? error.message : 'Unable to load models.');
      })
      .finally(() => {
        if (!cancelled) setLoadingModels(false);
      });

    return () => {
      cancelled = true;
    };
  }, []);

  const canRun = !running
    && steps.length > 0
    && steps.every((step) => step.model && step.prompt.trim())
    && models.length > 0;

  const addStep = () => {
    const fallbackModel = models[0] ?? steps[steps.length - 1]?.model ?? '';
    setSteps((currentSteps) => [...currentSteps, createStep(fallbackModel)]);
  };

  const updateStep = (
    stepId: string,
    patch: Partial<Pick<EditableWorkflowStep, 'model' | 'prompt' | 'dependsOnStepId'>>
  ) => {
    setSteps((currentSteps) => currentSteps.map((step) => (
      step.id === stepId ? { ...step, ...patch } : step
    )));
  };

  const removeStep = (stepId: string) => {
    setSteps((currentSteps) => currentSteps
      .filter((step) => step.id !== stepId)
      .map((step) => (
        step.dependsOnStepId === stepId ? { ...step, dependsOnStepId: null } : step
      )));

    setResultsByStepId((currentResults) => {
      const nextResults = { ...currentResults };
      delete nextResults[stepId];
      return nextResults;
    });
  };

  const handleRunWorkflow = async () => {
    const missingModelIndex = steps.findIndex((step) => !step.model);
    if (missingModelIndex >= 0) {
      setRunError(`Step ${missingModelIndex + 1} is missing a model.`);
      return;
    }

    const missingPromptIndex = steps.findIndex((step) => !step.prompt.trim());
    if (missingPromptIndex >= 0) {
      setRunError(`Step ${missingPromptIndex + 1} is missing a prompt.`);
      return;
    }

    const labelByStepId = new Map(
      steps.map((step, index) => [step.id, `step-${index}`])
    );

    const initialStates: Record<string, StepExecutionState> = {};
    for (const step of steps) {
      initialStates[step.id] = {
        status: step.dependsOnStepId ? 'pending' : 'running',
        stdoutPreview: '',
        stderrPreview: '',
      };
    }

    setRunError(null);
    setRunning(true);
    setResultsByStepId(initialStates);

    try {
      const payload = {
        name: 'Workflow Builder',
        workflow_id: `workflow-builder-${Date.now()}`,
        phase: 'build',
        outcome_goal: 'Run workflow builder steps',
        jobs: steps.map((step, index) => ({
          label: `step-${index}`,
          agent: step.model || 'auto/build',
          prompt: step.prompt,
          depends_on: step.dependsOnStepId
            ? [labelByStepId.get(step.dependsOnStepId)].filter(
              (value): value is string => Boolean(value)
            )
            : [],
        })),
      };

      const response = await fetch('/api/workflow-runs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      const data = await response.json().catch(() => null) as {
        error?: string;
        run_id?: string;
        jobs?: WorkflowJobResult[];
      } | null;

      if (!response.ok) {
        throw new Error(data?.error ?? `${response.status} ${response.statusText}`);
      }

      let jobs = Array.isArray(data?.jobs) ? data.jobs : [];
      if (!jobs.length && data?.run_id) {
        const deadline = Date.now() + RUN_TIMEOUT_MS;
        while (Date.now() < deadline) {
          const statusResponse = await fetch(`/api/workflow-runs/${encodeURIComponent(data.run_id)}/status`);
          const statusData = await statusResponse.json().catch(() => null) as {
            status?: string;
            jobs?: WorkflowJobResult[];
          } | null;
          if (Array.isArray(statusData?.jobs)) {
            jobs = statusData.jobs;
          }
          if (
            statusData?.status &&
            ['succeeded', 'failed', 'cancelled', 'done', 'complete'].includes(statusData.status)
          ) {
            break;
          }
          await new Promise((resolve) => window.setTimeout(resolve, RUN_POLL_MS));
        }
      }
      const jobsByLabel = new Map<string, WorkflowJobResult>();
      jobs.forEach((job, index) => {
        jobsByLabel.set(typeof job.label === 'string' ? job.label : `step-${index}`, job);
      });

      const nextResults: Record<string, StepExecutionState> = {};
      steps.forEach((step, index) => {
        const job = jobsByLabel.get(`step-${index}`);
        nextResults[step.id] = {
          status: normalizeStatus(job?.status),
          stdoutPreview: previewText(job?.stdout),
          stderrPreview: previewText(job?.stderr),
        };
      });

      setResultsByStepId(nextResults);
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : 'Workflow run failed.';

      setRunError(message);
      setResultsByStepId((currentResults) => {
        const nextResults = { ...currentResults };
        steps.forEach((step) => {
          nextResults[step.id] = {
            status: 'failed',
            stdoutPreview: currentResults[step.id]?.stdoutPreview ?? '',
            stderrPreview: previewText(message, 280),
          };
        });
        return nextResults;
      });
    } finally {
      setRunning(false);
    }
  };

  return (
    <div style={{
      display: 'flex',
      flexDirection: 'column',
      gap: 'var(--space-md, 16px)',
      padding: 'var(--space-lg, 24px)',
      width: '100%',
      height: '100%',
      boxSizing: 'border-box',
      backgroundColor: 'var(--bg-card, #161b22)',
      borderRadius: 'var(--radius, 8px)',
      color: 'var(--text, #c9d1d9)',
      border: '1px solid var(--border, #30363d)',
    }}>
      <div style={{
        display: 'flex',
        alignItems: 'flex-start',
        justifyContent: 'space-between',
        gap: 'var(--space-md, 16px)',
      }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
          <div style={{ fontSize: '16px', fontWeight: 700, color: 'var(--text, #c9d1d9)' }}>
            Workflow Builder
          </div>
          <div style={{ fontSize: '12px', color: 'var(--text-muted, #8b949e)' }}>
            Define ordered workflow steps, then run them as a single workflow.
          </div>
        </div>
        <button
          type="button"
          onClick={addStep}
          style={{
            backgroundColor: 'var(--bg, #0d1117)',
            color: 'var(--text, #c9d1d9)',
            border: '1px solid var(--border, #30363d)',
            borderRadius: 'var(--radius, 8px)',
            padding: '10px 14px',
            fontSize: '13px',
            fontWeight: 600,
            cursor: 'pointer',
            flexShrink: 0,
          }}
        >
          Add Step
        </button>
      </div>

      {(loadingModels || modelsError) && (
        <div style={{
          padding: '10px 12px',
          borderRadius: 'var(--radius, 8px)',
          border: '1px solid var(--border, #30363d)',
          backgroundColor: 'var(--bg, #0d1117)',
          color: modelsError ? 'var(--danger, #f85149)' : 'var(--text-muted, #8b949e)',
          fontSize: '12px',
        }}>
          {loadingModels ? <LoadingSkeleton lines={2} height={14} widths={['100%', '68%']} /> : modelsError}
        </div>
      )}

      {runError && (
        <div style={{
          padding: '10px 12px',
          borderRadius: 'var(--radius, 8px)',
          border: '1px solid var(--danger, #f85149)',
          backgroundColor: 'var(--bg, #0d1117)',
          color: 'var(--danger, #f85149)',
          fontSize: '12px',
        }}>
          {runError}
        </div>
      )}

      <div style={{
        display: 'flex',
        flexDirection: 'column',
        gap: 'var(--space-md, 16px)',
        flex: 1,
        minHeight: 0,
        overflowY: 'auto',
        paddingRight: '4px',
      }}>
        {steps.map((step, index) => {
          const result = resultsByStepId[step.id];
          const dependencyOptions = steps.slice(0, index);
          const statusColor = result ? statusColors[result.status] : 'var(--text-muted, #8b949e)';

          return (
            <div
              key={step.id}
              style={{
                display: 'flex',
                flexDirection: 'column',
                gap: 'var(--space-md, 16px)',
                padding: 'var(--space-md, 16px)',
                backgroundColor: 'var(--bg, #0d1117)',
                borderRadius: 'var(--radius, 8px)',
                border: '1px solid var(--border, #30363d)',
              }}
            >
              <div style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: 'var(--space-sm, 8px)',
              }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '10px', minWidth: 0 }}>
                  <div style={{ fontSize: '14px', fontWeight: 700, color: 'var(--text, #c9d1d9)' }}>
                    Step {index + 1}
                  </div>
                  {result && (
                    <span style={{
                      display: 'inline-flex',
                      alignItems: 'center',
                      gap: '6px',
                      padding: '4px 8px',
                      borderRadius: '999px',
                      backgroundColor: 'var(--bg-card, #161b22)',
                      border: '1px solid var(--border, #30363d)',
                      color: statusColor,
                      fontSize: '11px',
                      fontWeight: 700,
                      textTransform: 'uppercase',
                      letterSpacing: '0.04em',
                    }}>
                      <span style={{
                        width: '8px',
                        height: '8px',
                        borderRadius: '50%',
                        backgroundColor: statusColor,
                        boxShadow: result.status === 'running' ? `0 0 10px ${statusColor}` : 'none',
                      }} />
                      {statusLabels[result.status]}
                    </span>
                  )}
                </div>
                <button
                  type="button"
                  onClick={() => removeStep(step.id)}
                  style={{
                    width: '28px',
                    height: '28px',
                    borderRadius: '50%',
                    border: '1px solid var(--border, #30363d)',
                    backgroundColor: 'transparent',
                    color: 'var(--text-muted, #8b949e)',
                    fontSize: '14px',
                    fontWeight: 700,
                    lineHeight: 1,
                    cursor: 'pointer',
                    flexShrink: 0,
                  }}
                  aria-label={`Remove step ${index + 1}`}
                >
                  X
                </button>
              </div>

              <div style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
                gap: 'var(--space-md, 16px)',
              }}>
                <label style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                  <span style={{ fontSize: '11px', fontWeight: 700, color: 'var(--text-muted, #8b949e)' }}>
                    Model
                  </span>
                  <select
                    value={step.model}
                    onChange={(event) => updateStep(step.id, { model: event.target.value })}
                    disabled={loadingModels || models.length === 0}
                    style={{
                      backgroundColor: 'var(--bg-card, #161b22)',
                      color: 'var(--text, #c9d1d9)',
                      border: '1px solid var(--border, #30363d)',
                      borderRadius: 'var(--radius, 8px)',
                      padding: '10px 12px',
                      fontSize: '13px',
                    }}
                  >
                    {models.length === 0 && (
                      <option value="">
                        {loadingModels ? 'Loading models...' : 'No models available'}
                      </option>
                    )}
                    {models.map((model) => (
                      <option key={model} value={model}>{model}</option>
                    ))}
                  </select>
                </label>

                <label style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                  <span style={{ fontSize: '11px', fontWeight: 700, color: 'var(--text-muted, #8b949e)' }}>
                    Depends On
                  </span>
                  <select
                    value={step.dependsOnStepId ?? ''}
                    onChange={(event) => updateStep(step.id, {
                      dependsOnStepId: event.target.value || null,
                    })}
                    disabled={dependencyOptions.length === 0}
                    style={{
                      backgroundColor: 'var(--bg-card, #161b22)',
                      color: 'var(--text, #c9d1d9)',
                      border: '1px solid var(--border, #30363d)',
                      borderRadius: 'var(--radius, 8px)',
                      padding: '10px 12px',
                      fontSize: '13px',
                    }}
                  >
                    <option value="">None</option>
                    {dependencyOptions.map((candidate, candidateIndex) => (
                      <option key={candidate.id} value={candidate.id}>
                        Step {candidateIndex + 1}
                      </option>
                    ))}
                  </select>
                </label>
              </div>

              <label style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                <span style={{ fontSize: '11px', fontWeight: 700, color: 'var(--text-muted, #8b949e)' }}>
                  Prompt
                </span>
                <textarea
                  value={step.prompt}
                  onChange={(event) => updateStep(step.id, { prompt: event.target.value })}
                  rows={5}
                  placeholder="Describe what this step should do..."
                  style={{
                    width: '100%',
                    resize: 'vertical',
                    minHeight: '120px',
                    backgroundColor: 'var(--bg-card, #161b22)',
                    color: 'var(--text, #c9d1d9)',
                    border: '1px solid var(--border, #30363d)',
                    borderRadius: 'var(--radius, 8px)',
                    padding: '12px',
                    fontSize: '13px',
                    lineHeight: 1.5,
                    boxSizing: 'border-box',
                  }}
                />
              </label>

              {result && (
                <div style={{
                  display: 'flex',
                  flexDirection: 'column',
                  gap: 'var(--space-sm, 8px)',
                }}>
                  <div style={{ fontSize: '11px', fontWeight: 700, color: 'var(--text-muted, #8b949e)' }}>
                    Stdout Preview
                  </div>
                  <pre style={{
                    margin: 0,
                    padding: '12px',
                    borderRadius: 'var(--radius, 8px)',
                    border: '1px solid var(--border, #30363d)',
                    backgroundColor: 'var(--bg-card, #161b22)',
                    color: result.stdoutPreview ? 'var(--text, #c9d1d9)' : 'var(--text-muted, #8b949e)',
                    fontSize: '12px',
                    lineHeight: 1.5,
                    fontFamily: 'var(--font-mono, ui-monospace, SFMono-Regular, Menlo, monospace)',
                    whiteSpace: 'pre-wrap',
                    overflowX: 'auto',
                  }}>
                    {result.stdoutPreview || (
                      result.status === 'running'
                        ? 'Execution in progress...'
                        : result.status === 'pending'
                          ? 'Waiting on dependency...'
                          : 'No stdout returned.'
                    )}
                  </pre>

                  {result.stderrPreview && (
                    <>
                      <div style={{ fontSize: '11px', fontWeight: 700, color: 'var(--danger, #f85149)' }}>
                        Stderr
                      </div>
                      <pre style={{
                        margin: 0,
                        padding: '12px',
                        borderRadius: 'var(--radius, 8px)',
                        border: '1px solid var(--danger, #f85149)',
                        backgroundColor: 'var(--bg-card, #161b22)',
                        color: 'var(--danger, #f85149)',
                        fontSize: '12px',
                        lineHeight: 1.5,
                        fontFamily: 'var(--font-mono, ui-monospace, SFMono-Regular, Menlo, monospace)',
                        whiteSpace: 'pre-wrap',
                        overflowX: 'auto',
                      }}>
                        {result.stderrPreview}
                      </pre>
                    </>
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>

      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: 'var(--space-md, 16px)',
        paddingTop: 'var(--space-sm, 8px)',
        borderTop: '1px solid var(--border, #30363d)',
      }}>
        <div style={{ fontSize: '12px', color: 'var(--text-muted, #8b949e)' }}>
          Dependencies serialize to the workflow API as label references between steps.
        </div>
        <button
          type="button"
          onClick={handleRunWorkflow}
          disabled={!canRun}
          style={{
            backgroundColor: 'var(--accent, #58a6ff)',
            color: '#ffffff',
            border: 'none',
            borderRadius: 'var(--radius, 8px)',
            padding: '11px 18px',
            fontSize: '13px',
            fontWeight: 700,
            cursor: canRun ? 'pointer' : 'not-allowed',
            opacity: canRun ? 1 : 0.65,
            flexShrink: 0,
          }}
        >
          {running ? 'Running Workflow...' : 'Run Workflow'}
        </button>
      </div>
    </div>
  );
}

export default WorkflowBuilderModule;
