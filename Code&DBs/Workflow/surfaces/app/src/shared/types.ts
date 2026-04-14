// Canonical build system types — extracted from BuildWorkspace.tsx
// Every build-related component imports from here.

export interface BuildNode {
  node_id: string;
  kind: 'step' | 'gate' | 'state';
  title?: string;
  summary?: string;
  route?: string;
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
  branch_reason?: string | null;
  gate?: {
    state: string;
    label?: string;
    family: string;
    config?: {
      verify_command?: string;
      condition?: string;
      max_attempts?: number;
      fallback?: string;
      [key: string]: unknown;
    };
    [key: string]: unknown;
  } | null;
  gateLabel?: string;
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
  source_node_ids?: string[];
  state?: string;
  candidate_targets?: BindingTarget[];
  accepted_target?: BindingTarget | null;
  rationale?: string;
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

export interface BuildPayload {
  workflow?: { id: string; name: string; description?: string } | null;
  definition: Record<string, unknown>;
  compiled_spec?: CompiledSpec | null;
  planning_notes?: string[];
  build_state?: string;
  build_blockers?: BuildIssue[];
  build_graph?: {
    graph_id?: string;
    definition_revision?: string;
    compiler_revision?: string;
    schema_version?: number;
    nodes?: BuildNode[];
    edges?: BuildEdge[];
  } | null;
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
