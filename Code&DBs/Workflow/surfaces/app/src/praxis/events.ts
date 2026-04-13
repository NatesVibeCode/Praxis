export interface PraxisOpenTabDetail {
  kind: 'build' | 'manifest' | 'manifest-editor' | 'run-detail' | 'edit-model';
  workflowId?: string | null;
  intent?: string | null;
  manifestId?: string | null;
  tabId?: string | null;
  runId?: string | null;
  editorSurface?: 'definition' | 'plan' | 'run' | 'details' | null;
}

export function emitPraxisOpenTab(detail: PraxisOpenTabDetail): void {
  window.dispatchEvent(new CustomEvent<PraxisOpenTabDetail>('praxis-open-tab', { detail }));
}
