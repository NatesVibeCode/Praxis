BEGIN;

CREATE TABLE IF NOT EXISTS execution_packets (
    execution_packet_id TEXT PRIMARY KEY,
    definition_revision TEXT NOT NULL,
    plan_revision TEXT NOT NULL,
    packet_revision TEXT NOT NULL,
    parent_artifact_ref TEXT,
    packet_version INTEGER NOT NULL CHECK (packet_version > 0),
    packet_hash TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    spec_name TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    authority_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
    model_messages JSONB NOT NULL DEFAULT '[]'::jsonb,
    reference_bindings JSONB NOT NULL DEFAULT '[]'::jsonb,
    capability_bindings JSONB NOT NULL DEFAULT '[]'::jsonb,
    verify_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
    authority_inputs JSONB NOT NULL DEFAULT '{}'::jsonb,
    file_inputs JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload JSONB NOT NULL,
    decision_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT execution_packets_authority_refs_array_check
        CHECK (jsonb_typeof(authority_refs) = 'array'),
    CONSTRAINT execution_packets_model_messages_array_check
        CHECK (jsonb_typeof(model_messages) = 'array'),
    CONSTRAINT execution_packets_reference_bindings_array_check
        CHECK (jsonb_typeof(reference_bindings) = 'array'),
    CONSTRAINT execution_packets_capability_bindings_array_check
        CHECK (jsonb_typeof(capability_bindings) = 'array'),
    CONSTRAINT execution_packets_verify_refs_array_check
        CHECK (jsonb_typeof(verify_refs) = 'array'),
    CONSTRAINT execution_packets_authority_inputs_object_check
        CHECK (jsonb_typeof(authority_inputs) = 'object'),
    CONSTRAINT execution_packets_file_inputs_object_check
        CHECK (jsonb_typeof(file_inputs) = 'object'),
    CONSTRAINT execution_packets_payload_object_check
        CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT execution_packets_unique_revision_lineage
        UNIQUE (definition_revision, plan_revision, packet_revision),
    CONSTRAINT execution_packets_unique_run_id
        UNIQUE (run_id, packet_revision)
);

CREATE INDEX IF NOT EXISTS execution_packets_definition_plan_idx
    ON execution_packets (definition_revision, plan_revision, created_at DESC);

CREATE INDEX IF NOT EXISTS execution_packets_run_idx
    ON execution_packets (run_id, created_at DESC);

CREATE INDEX IF NOT EXISTS execution_packets_packet_hash_idx
    ON execution_packets (packet_hash);

COMMENT ON TABLE execution_packets IS 'Shadow execution packets compiled from workflow definition and plan authority. Packet truth is recorded without changing execution outcomes.';
COMMENT ON COLUMN execution_packets.definition_revision IS 'Definition revision used to compile the packet snapshot.';
COMMENT ON COLUMN execution_packets.plan_revision IS 'Plan revision used to compile the packet snapshot.';
COMMENT ON COLUMN execution_packets.model_messages IS 'Exact model message payloads admitted for execution.';
COMMENT ON COLUMN execution_packets.reference_bindings IS 'Explicit reference bindings used to shape the packet.';
COMMENT ON COLUMN execution_packets.capability_bindings IS 'Explicit capability bindings used to shape the packet.';
COMMENT ON COLUMN execution_packets.verify_refs IS 'Verification bindings admitted into the packet snapshot.';
COMMENT ON COLUMN execution_packets.authority_inputs IS 'Exact authority rows and inputs used to compile the packet.';
COMMENT ON COLUMN execution_packets.file_inputs IS 'Exact file inputs used to compile the packet.';

COMMIT;
