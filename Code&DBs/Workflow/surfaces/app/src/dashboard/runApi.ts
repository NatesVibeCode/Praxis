export function runsRecentPath(limit: number): string {
  return `/api/runs/recent?limit=${limit}`;
}

export function runDetailPath(runId: string): string {
  return `/api/runs/${encodeURIComponent(runId)}`;
}

export function runJobsPath(runId: string, jobId: number): string {
  return `/api/runs/${encodeURIComponent(runId)}/jobs/${jobId}`;
}

export function workflowRunStreamPath(runId: string): string {
  return `/api/workflow-runs/${encodeURIComponent(runId)}/stream`;
}

export function workflowRunStatusPath(runId: string): string {
  return `/api/workflow-runs/${encodeURIComponent(runId)}/status`;
}
