// Canonical build system types — extracted from BuildWorkspace.tsx
// Every build-related component imports from here.

export type HttpRequestPreset = 'fetch_json' | 'post_json' | 'webhook_callback' | 'custom';

export interface NotificationIntegrationArgs extends Record<string, unknown> {
  title?: string;
  message?: string;
  status?: string;
  metadata?: Record<string, unknown>;
}

export interface HttpRequestIntegrationArgs extends Record<string, unknown> {
  request_preset?: HttpRequestPreset;
  url?: string;
  endpoint?: string;
  method?: string;
  headers?: Record<string, unknown>;
  body?: unknown;
  body_template?: unknown;
  timeout?: number;
  auth_strategy?: Record<string, unknown>;
  endpoint_map?: unknown[];
  connector_spec?: Record<string, unknown>;
}

export interface WorkflowInvokeIntegrationArgs extends Record<string, unknown> {
  workflow_id?: string;
  target_workflow_id?: string;
  payload?: unknown;
  input?: unknown;
  inputs?: unknown;
}

export type BuildNodeIntegrationArgs =
  | NotificationIntegrationArgs
  | HttpRequestIntegrationArgs
  | WorkflowInvokeIntegrationArgs
  | Record<string, unknown>;

export interface BuildGraphPayload {
  graph_id?: string;
  definition_revision?: string;
  compiler_revision?: string;
  schema_version?: number;
  nodes?: BuildNode[];
  edges?: BuildEdge[];
}

export interface BuildNode {
  node_id: string;
  kind: 'step' | 'gate' | 'state';
  title?: string;
  summary?: string;
  route?: string;
  integration_args?: BuildNodeIntegrationArgs;
  trigger?: {
    event_type?: string;
    cron_expression?: string;
    source_ref?: string;
    filter?: Record<string, unknown>;
  };
  prompt?: string;
  required_inputs?: string[];
  outputs?: string[];
  persistence_targets?: string[];
  handoff_target?: string | null;
  source_block_ids?: string[];
  binding_ids?: string[];
  issue_ids?: string[];
  status?: string;
  gate_rule?: Record<string, unknown>;
  source_node_ids?: string[];
}

export interface BuildEdge {
  edge_id: string;
  kind: string;
  from_node_id: string;
  to_node_id: string;
  release?: BuildEdgeRelease | null;
  gateLabel?: string;
}

export interface BuildEdgeRelease {
  family: string;
  edge_type: 'after_success' | 'after_failure' | 'after_any' | 'conditional';
  release_condition: Record<string, unknown>;
  label?: string;
  branch_reason?: string;
  state?: string;
  config?: {
    verify_refs?: string[];
    condition?: Record<string, unknown> | string;
    max_attempts?: number;
    fallback?: string;
    branch_side?: string;
    [key: string]: unknown;
  };
}

export interface BuildIssue {
  issue_id: string;
  kind?: string;
  node_id?: string;
  binding_id?: string | null;
  label?: string;
  summary?: string;
  severity?: string;
  gate_rule?: Record<string, unknown>;
}

export interface BindingTarget {
  target_ref?: string;
  label?: string;
  kind?: string;
  enrichment?: {
    integration_name?: string;
    provider?: string;
    auth_status?: string;
    description?: string;
    capability_title?: string;
  };
}

export interface BindingLedgerEntry {
  binding_id: string;
  source_kind?: string;
  source_label?: string;
  source_span?: unknown[] | null;
  source_node_ids?: string[];
  state?: string;
  candidate_targets?: BindingTarget[];
  accepted_target?: BindingTarget | null;
  rationale?: string;
  created_at?: string | null;
  updated_at?: string | null;
  freshness?: {
    state?: string;
    captured_at?: string | null;
    stale_after_at?: string | null;
  } | null;
}

export interface ImportSnapshot {
  snapshot_id: string;
  source_kind?: string;
  source_locator?: string;
  requested_shape?: Record<string, unknown>;
  payload?: unknown;
  freshness_ttl?: number;
  captured_at?: string | null;
  stale_after_at?: string | null;
  approval_state?: string;
  admitted_targets?: BindingTarget[];
  binding_id?: string | null;
  node_id?: string | null;
}

export interface AuthorityAttachment {
  attachment_id: string;
  node_id: string;
  authority_kind?: string;
  authority_ref?: string;
  role?: string;
  label?: string;
  promote_to_state?: boolean;
  state_node_id?: string | null;
}

export interface WorkflowJob {
  label?: string;
  agent?: string;
  prompt?: string;
  source_step_id?: string | null;
  source_node_id?: string | null;
}

export interface WorkflowTrigger {
  event_type?: string;
  source_trigger_id?: string | null;
  source_ref?: string;
}

export interface CompiledSpec {
  name?: string;
  jobs?: WorkflowJob[];
  triggers?: WorkflowTrigger[];
}

export interface CompiledSpecProjection {
  version?: number;
  graph_id?: string;
  definition_revision?: string;
  compiled_spec?: CompiledSpec | null;
}

export interface BuildUndoReceiptStep {
  subpath: string;
  body: Record<string, unknown>;
}

export interface BuildUndoReceipt {
  workflow_id: string;
  steps: BuildUndoReceiptStep[];
}

export interface ProgressiveBuildCheck {
  id: string;
  label: string;
  state: 'passed' | 'warning' | 'blocked';
  detail?: string;
  authority?: string;
}

export interface ProgressiveBuildUnit {
  ordinal?: number;
  node_id: string;
  edge_id?: string;
  title?: string;
  route?: string;
  summary?: string;
  status?: string;
  inputs?: string[];
  outputs?: string[];
  gate_label?: string;
}

export interface ProgressiveBuildState {
  version?: number;
  mode?: string;
  source_prose?: string;
  last_unit?: ProgressiveBuildUnit | null;
  accepted_units?: ProgressiveBuildUnit[];
  checks?: ProgressiveBuildCheck[];
  surfaces?: string[];
  next_index?: number;
  completion?: {
    accepted?: number;
    planned?: number;
  };
}

export interface BuildPayload {
  workflow?: { id: string; name: string; description?: string } | null;
  definition: Record<string, unknown>;
  compiled_spec?: CompiledSpec | null;
  planning_notes?: string[];
  build_state?: string;
  build_blockers?: BuildIssue[];
  build_graph?: BuildGraphPayload | null;
  binding_ledger?: BindingLedgerEntry[];
  import_snapshots?: ImportSnapshot[];
  authority_attachments?: AuthorityAttachment[];
  build_issues?: BuildIssue[];
  projection_status?: Record<string, unknown>;
  compiled_spec_projection?: CompiledSpecProjection | null;
  matched_building_blocks?: Array<{
    id: string;
    name: string;
    description?: string;
    category: string;
    rank: number;
  }>;
  composition_plan?: {
    confidence?: number;
    components?: string[];
    calculations?: string[];
    workflows?: string[];
  };
  undo_receipt?: BuildUndoReceipt | null;
  progressive_build?: ProgressiveBuildState | null;
  mutation_event_id?: number | null;
}

// Service bus event types
export interface BuildEvent {
  id: number;
  channel: string;
  event_type: string;
  entity_id: string;
  entity_kind: string;
  payload: Record<string, unknown>;
  emitted_at: string;
  emitted_by: string;
}
