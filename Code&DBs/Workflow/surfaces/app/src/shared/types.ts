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

export interface AgentRegistryRow {
  agent_principal_ref: string;
  title: string;
  description?: string;
  icon_hint?: string;
  visibility?: 'visible' | 'hidden' | 'archived';
  builder_category?: 'builtin' | 'custom';
  status: 'active' | 'paused' | 'killed';
  capability_refs?: string[];
  allowed_tools?: string[];
  write_envelope?: string[];
  system_prompt_template?: string;
  network_policy?: string;
  model_preference?: string;
  reasoning_effort?: string;
}

export interface BuildGraphPayload {
  graph_id?: string;
  definition_revision?: string;
  compiler_revision?: string;
  schema_version?: number;
  nodes?: BuildNode[];
  edges?: BuildEdge[];
  context_authority?: WorkflowContextAuthorityPayload | null;
}

export interface BuildNode {
  node_id: string;
  kind: 'step' | 'gate' | 'state';
  title?: string;
  summary?: string;
  route?: string;
  task_type?: string | null;
  agent?: string | null;
  capabilities?: string[];
  write_scope?: string[];
  agent_tool_plan?: {
    tool_name?: string;
    operation?: string;
    repeats?: number;
    focus?: string;
    cadence?: 'single' | 'sequential' | 'parallel';
    target_fields?: string[];
    notes?: string;
    [key: string]: unknown;
  };
  completion_contract?: {
    result_kind?: string;
    submit_tool_names?: string[];
    submission_required?: boolean;
    verification_required?: boolean;
    [key: string]: unknown;
  };
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
  typed_gap?: Record<string, unknown>;
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

export interface MaterializedSpec {
  name?: string;
  jobs?: WorkflowJob[];
  triggers?: WorkflowTrigger[];
}

export interface MaterializedSpecProjection {
  version?: number;
  graph_id?: string;
  definition_revision?: string;
  materialized_spec?: MaterializedSpec | null;
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

export interface CompilePreviewSpan {
  text?: string;
  kind?: string;
  normalized?: string;
  start?: number;
  end?: number;
}

export interface CompilePreviewMatch {
  span_text?: string;
  object_kind?: string;
  label?: string;
  category?: string | null;
  confidence?: string;
  reason?: string;
}

export interface CompilePreviewSuggestedStep {
  label?: string;
  source_ref?: string;
  reason?: string;
  status?: string;
  confidence?: string;
}

export interface CompilePreviewGap {
  span_text?: string;
  kind?: string;
  reason?: string;
}

export interface CompilePreviewPayload {
  kind?: 'compile_preview';
  cqrs_role?: 'query';
  ok?: boolean;
  intent?: string;
  input_fingerprint?: string;
  scope_packet?: {
    spans?: CompilePreviewSpan[];
    matches?: CompilePreviewMatch[];
    suggested_steps?: CompilePreviewSuggestedStep[];
    gaps?: CompilePreviewGap[];
  };
  enough_structure?: boolean;
  next_actions?: Array<Record<string, unknown>>;
}

export interface BuildPayload {
  workflow?: { id: string; name: string; description?: string } | null;
  definition: Record<string, unknown>;
  materialized_spec?: MaterializedSpec | null;
  planning_notes?: string[];
  build_state?: string;
  build_blockers?: BuildIssue[];
  build_graph?: BuildGraphPayload | null;
  binding_ledger?: BindingLedgerEntry[];
  import_snapshots?: ImportSnapshot[];
  authority_attachments?: AuthorityAttachment[];
  build_issues?: BuildIssue[];
  projection_status?: Record<string, unknown>;
  materialized_spec_projection?: MaterializedSpecProjection | null;
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
  compile_preview?: CompilePreviewPayload | null;
  workflow_context?: WorkflowContextAuthorityPayload | null;
  mutation_event_id?: number | null;
  operation_receipt?: Record<string, unknown> | null;
  graph_summary?: Record<string, unknown> | null;
}

export interface WorkflowContextAuthorityPayload {
  context_ref?: string;
  workflow_ref?: string | null;
  context_mode?: 'standalone' | 'inferred' | 'synthetic' | 'bound' | 'hybrid' | string;
  truth_state?: string;
  context_pill?: string;
  confidence_score?: number;
  confidence_state?: string;
  confidence?: {
    score?: number;
    state?: string;
    inputs?: Record<string, unknown>;
  };
  entities?: Array<{
    entity_ref?: string;
    entity_kind?: string;
    label?: string;
    truth_state?: string;
    io_mode?: string;
    context_pill?: string;
    confidence_score?: number;
    payload?: Record<string, unknown>;
  }>;
  blockers?: Array<{
    blocker_ref?: string;
    severity?: string;
    reason_code?: string;
    message?: string;
  }>;
  verifier_expectations?: Array<{
    verifier_ref?: string;
    expectation?: string;
    scenario_pack_ref?: string;
    required_before?: string;
  }>;
  guardrail?: {
    allowed?: boolean;
    review_required?: boolean;
    allowed_next_actions?: string[];
    safe_next_llm_actions?: string[];
    no_go_conditions?: Array<Record<string, unknown>>;
  };
  synthetic_world?: {
    world_ref?: string;
    synthetic?: boolean;
    records?: Array<Record<string, unknown>>;
    virtual_lab?: {
      environment_revision?: Record<string, unknown>;
      simulation_run_payload?: Record<string, unknown>;
      simulation_scenario?: Record<string, unknown>;
    };
  };
  latest_virtual_lab_simulation?: {
    run_id?: string;
    status?: string;
    stop_reason?: string;
    transition_count?: number;
    verifier_statuses?: string[];
  };
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
