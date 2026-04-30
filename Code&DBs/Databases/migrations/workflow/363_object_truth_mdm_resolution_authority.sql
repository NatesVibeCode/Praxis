-- Migration 363: Object Truth MDM resolution authority.
--
-- Phase 3's deterministic MDM/source-authority primitives become durable
-- authority here: identity clusters, field comparisons, normalization rules,
-- authority evidence, hierarchy signals, typed gaps, and packet readbacks.

BEGIN;

CREATE TABLE IF NOT EXISTS object_truth_mdm_resolution_packets (
    packet_ref text PRIMARY KEY,
    resolution_packet_digest text NOT NULL,
    client_ref text NOT NULL,
    entity_type text NOT NULL,
    as_of timestamptz NOT NULL,
    identity_cluster_count integer NOT NULL DEFAULT 0,
    field_comparison_count integer NOT NULL DEFAULT 0,
    normalization_rule_count integer NOT NULL DEFAULT 0,
    authority_evidence_count integer NOT NULL DEFAULT 0,
    hierarchy_signal_count integer NOT NULL DEFAULT 0,
    typed_gap_count integer NOT NULL DEFAULT 0,
    packet_json jsonb NOT NULL,
    observed_by_ref text,
    source_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_object_truth_mdm_packets_client_entity
    ON object_truth_mdm_resolution_packets (client_ref, entity_type, as_of DESC);

CREATE INDEX IF NOT EXISTS idx_object_truth_mdm_packets_digest
    ON object_truth_mdm_resolution_packets (resolution_packet_digest);

CREATE TABLE IF NOT EXISTS object_truth_mdm_identity_clusters (
    packet_ref text NOT NULL REFERENCES object_truth_mdm_resolution_packets(packet_ref) ON DELETE CASCADE,
    cluster_id text NOT NULL,
    identity_cluster_digest text NOT NULL,
    entity_type text NOT NULL,
    review_status text NOT NULL,
    cluster_confidence numeric NOT NULL DEFAULT 0,
    member_count integer NOT NULL DEFAULT 0,
    cluster_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (packet_ref, cluster_id)
);

CREATE INDEX IF NOT EXISTS idx_object_truth_mdm_clusters_digest
    ON object_truth_mdm_identity_clusters (identity_cluster_digest);

CREATE INDEX IF NOT EXISTS idx_object_truth_mdm_clusters_status
    ON object_truth_mdm_identity_clusters (entity_type, review_status);

CREATE TABLE IF NOT EXISTS object_truth_mdm_field_comparisons (
    packet_ref text NOT NULL REFERENCES object_truth_mdm_resolution_packets(packet_ref) ON DELETE CASCADE,
    field_comparison_digest text NOT NULL,
    cluster_id text,
    canonical_record_id text NOT NULL,
    canonical_field text NOT NULL,
    entity_type text NOT NULL,
    selection_state text NOT NULL,
    conflict_flag boolean NOT NULL DEFAULT false,
    consensus_flag boolean NOT NULL DEFAULT false,
    typed_gap_count integer NOT NULL DEFAULT 0,
    comparison_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (packet_ref, field_comparison_digest)
);

CREATE INDEX IF NOT EXISTS idx_object_truth_mdm_field_comparisons_field
    ON object_truth_mdm_field_comparisons (entity_type, canonical_field, selection_state);

CREATE INDEX IF NOT EXISTS idx_object_truth_mdm_field_comparisons_conflict
    ON object_truth_mdm_field_comparisons (conflict_flag, typed_gap_count);

CREATE TABLE IF NOT EXISTS object_truth_mdm_normalization_rules (
    packet_ref text NOT NULL REFERENCES object_truth_mdm_resolution_packets(packet_ref) ON DELETE CASCADE,
    rule_ref text NOT NULL,
    normalization_rule_digest text NOT NULL,
    entity_type text NOT NULL,
    field_name text NOT NULL,
    reversible boolean NOT NULL DEFAULT true,
    loss_risk text NOT NULL,
    rule_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (packet_ref, rule_ref)
);

CREATE INDEX IF NOT EXISTS idx_object_truth_mdm_rules_field
    ON object_truth_mdm_normalization_rules (entity_type, field_name);

CREATE TABLE IF NOT EXISTS object_truth_mdm_source_authority_evidence (
    packet_ref text NOT NULL REFERENCES object_truth_mdm_resolution_packets(packet_ref) ON DELETE CASCADE,
    authority_evidence_digest text NOT NULL,
    entity_type text NOT NULL,
    field_name text NOT NULL,
    source_system text NOT NULL,
    authority_rank integer NOT NULL,
    evidence_type text NOT NULL,
    evidence_reference text NOT NULL,
    evidence_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (packet_ref, authority_evidence_digest)
);

CREATE INDEX IF NOT EXISTS idx_object_truth_mdm_authority_field
    ON object_truth_mdm_source_authority_evidence (entity_type, field_name, authority_rank);

CREATE INDEX IF NOT EXISTS idx_object_truth_mdm_authority_source
    ON object_truth_mdm_source_authority_evidence (source_system, authority_rank);

CREATE TABLE IF NOT EXISTS object_truth_mdm_hierarchy_signals (
    packet_ref text NOT NULL REFERENCES object_truth_mdm_resolution_packets(packet_ref) ON DELETE CASCADE,
    hierarchy_signal_digest text NOT NULL,
    entity_type text NOT NULL,
    signal_type text NOT NULL,
    source_system text NOT NULL,
    source_record_id text NOT NULL,
    authoritative boolean NOT NULL DEFAULT false,
    hierarchy_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (packet_ref, hierarchy_signal_digest)
);

CREATE INDEX IF NOT EXISTS idx_object_truth_mdm_hierarchy_signal_type
    ON object_truth_mdm_hierarchy_signals (entity_type, signal_type, authoritative);

CREATE TABLE IF NOT EXISTS object_truth_mdm_typed_gaps (
    packet_ref text NOT NULL REFERENCES object_truth_mdm_resolution_packets(packet_ref) ON DELETE CASCADE,
    gap_id text NOT NULL,
    gap_digest text NOT NULL,
    entity_type text NOT NULL,
    field_name text NOT NULL,
    gap_type text NOT NULL,
    severity text NOT NULL,
    gap_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (packet_ref, gap_id)
);

CREATE INDEX IF NOT EXISTS idx_object_truth_mdm_gaps_type
    ON object_truth_mdm_typed_gaps (entity_type, field_name, gap_type, severity);

CREATE OR REPLACE FUNCTION touch_object_truth_mdm_resolution_packets_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_object_truth_mdm_resolution_packets_touch ON object_truth_mdm_resolution_packets;
CREATE TRIGGER trg_object_truth_mdm_resolution_packets_touch
    BEFORE UPDATE ON object_truth_mdm_resolution_packets
    FOR EACH ROW EXECUTE FUNCTION touch_object_truth_mdm_resolution_packets_updated_at();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'object_truth_mdm_resolution_packets',
        'Object Truth MDM resolution packets',
        'table',
        'Receipt-backed MDM/source-authority resolution packets with identity, field, rule, hierarchy, and gap counts.',
        '{"migration":"363_object_truth_mdm_resolution_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.object_truth"}'::jsonb
    ),
    (
        'object_truth_mdm_identity_clusters',
        'Object Truth MDM identity clusters',
        'table',
        'Decomposed identity clusters and review status for stored MDM resolution packets.',
        '{"migration":"363_object_truth_mdm_resolution_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.object_truth"}'::jsonb
    ),
    (
        'object_truth_mdm_field_comparisons',
        'Object Truth MDM field comparisons',
        'table',
        'Field-level source-authority comparisons, canonical selection state, conflicts, and gap counts.',
        '{"migration":"363_object_truth_mdm_resolution_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.object_truth"}'::jsonb
    ),
    (
        'object_truth_mdm_normalization_rules',
        'Object Truth MDM normalization rules',
        'table',
        'Normalization rules used by MDM resolution packets, including reversibility and loss-risk flags.',
        '{"migration":"363_object_truth_mdm_resolution_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.object_truth"}'::jsonb
    ),
    (
        'object_truth_mdm_source_authority_evidence',
        'Object Truth MDM source authority evidence',
        'table',
        'Field-level source authority evidence and rank by source system.',
        '{"migration":"363_object_truth_mdm_resolution_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.object_truth"}'::jsonb
    ),
    (
        'object_truth_mdm_hierarchy_signals',
        'Object Truth MDM hierarchy signals',
        'table',
        'Hierarchy and flattening evidence associated with MDM resolution packets.',
        '{"migration":"363_object_truth_mdm_resolution_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.object_truth"}'::jsonb
    ),
    (
        'object_truth_mdm_typed_gaps',
        'Object Truth MDM typed gaps',
        'table',
        'Actionable MDM gaps for missing policy, conflicts, stale values, identity ambiguity, and hierarchy issues.',
        '{"migration":"363_object_truth_mdm_resolution_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.object_truth"}'::jsonb
    )
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO authority_object_registry (
    object_ref,
    object_kind,
    object_name,
    schema_name,
    authority_domain_ref,
    data_dictionary_object_kind,
    lifecycle_status,
    write_model_kind,
    owner_ref,
    source_decision_ref,
    metadata
) VALUES
    ('table.public.object_truth_mdm_resolution_packets', 'table', 'object_truth_mdm_resolution_packets', 'public', 'authority.object_truth', 'object_truth_mdm_resolution_packets', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.object_truth_mdm_identity_clusters', 'table', 'object_truth_mdm_identity_clusters', 'public', 'authority.object_truth', 'object_truth_mdm_identity_clusters', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.object_truth_mdm_field_comparisons', 'table', 'object_truth_mdm_field_comparisons', 'public', 'authority.object_truth', 'object_truth_mdm_field_comparisons', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.object_truth_mdm_normalization_rules', 'table', 'object_truth_mdm_normalization_rules', 'public', 'authority.object_truth', 'object_truth_mdm_normalization_rules', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.object_truth_mdm_source_authority_evidence', 'table', 'object_truth_mdm_source_authority_evidence', 'public', 'authority.object_truth', 'object_truth_mdm_source_authority_evidence', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.object_truth_mdm_hierarchy_signals', 'table', 'object_truth_mdm_hierarchy_signals', 'public', 'authority.object_truth', 'object_truth_mdm_hierarchy_signals', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb),
    ('table.public.object_truth_mdm_typed_gaps', 'table', 'object_truth_mdm_typed_gaps', 'public', 'authority.object_truth', 'object_truth_mdm_typed_gaps', 'active', 'registry', 'praxis.engine', 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences', '{}'::jsonb)
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO authority_event_contracts (
    event_contract_ref,
    event_type,
    authority_domain_ref,
    payload_schema_ref,
    aggregate_ref_policy,
    reducer_refs,
    projection_refs,
    receipt_required,
    replay_policy,
    enabled,
    decision_ref,
    metadata
) VALUES (
    'event_contract.object_truth.mdm_resolution_recorded',
    'object_truth.mdm_resolution_recorded',
    'authority.object_truth',
    'data_dictionary.object.object_truth_mdm_resolution_recorded_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    '{"expected_payload_fields":["packet_ref","resolution_packet_digest","client_ref","entity_type","identity_cluster_count","field_comparison_count","authority_evidence_count","typed_gap_count"]}'::jsonb
)
ON CONFLICT (authority_domain_ref, event_type) DO UPDATE SET
    payload_schema_ref = EXCLUDED.payload_schema_ref,
    aggregate_ref_policy = EXCLUDED.aggregate_ref_policy,
    receipt_required = EXCLUDED.receipt_required,
    replay_policy = EXCLUDED.replay_policy,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

SELECT register_operation_atomic(
    p_operation_ref         := 'object_truth.command.mdm_resolution_record',
    p_operation_name        := 'object_truth_mdm_resolution_record',
    p_handler_ref           := 'runtime.operations.commands.object_truth_mdm.handle_object_truth_mdm_resolution_record',
    p_input_model_ref       := 'runtime.operations.commands.object_truth_mdm.RecordObjectTruthMdmResolutionCommand',
    p_authority_domain_ref  := 'authority.object_truth',
    p_authority_ref         := 'authority.object_truth',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/object-truth/mdm/resolutions',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'object_truth.mdm_resolution_recorded',
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.object_truth_mdm_resolution_record.20260430',
    p_label                 := 'Object Truth Record MDM Resolution',
    p_summary               := 'Record receipt-backed Object Truth MDM/source-authority resolution packets and decomposed identity, field, authority, hierarchy, and gap evidence.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'object_truth.query.mdm_resolution_read',
    p_operation_name        := 'object_truth_mdm_resolution_read',
    p_handler_ref           := 'runtime.operations.queries.object_truth_mdm.handle_object_truth_mdm_resolution_read',
    p_input_model_ref       := 'runtime.operations.queries.object_truth_mdm.QueryObjectTruthMdmResolutionRead',
    p_authority_domain_ref  := 'authority.object_truth',
    p_authority_ref         := 'authority.object_truth',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/object-truth/mdm/resolutions',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_decision_ref          := 'architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences',
    p_binding_revision      := 'binding.operation_catalog_registry.object_truth_mdm_resolution_read.20260430',
    p_label                 := 'Object Truth Read MDM Resolutions',
    p_summary               := 'Read queryable Object Truth MDM/source-authority resolution packets and decomposed evidence records.'
);

COMMIT;
