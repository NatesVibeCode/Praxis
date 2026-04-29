-- Migration 326: operator repo-policy onboarding authority.
--
-- First-run setup for sensitive or repo-local systems should not live only in
-- chat. This authority stores one current repo-policy contract per repo root,
-- plus append-only revisions and small disclosure counters so Praxis can teach
-- the operator what bug/pattern promotion is doing during the early window.

BEGIN;

INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    current_projection_ref,
    storage_target_ref,
    enabled,
    decision_ref
) VALUES (
    'authority.operator_onboarding',
    'praxis.engine',
    'stream.authority.operator_onboarding',
    NULL,
    'praxis.primary_postgres',
    TRUE,
    'architecture-policy::operator-onboarding::first-run-repo-policy-contract-and-pattern-disclosure'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

CREATE TABLE IF NOT EXISTS operator_repo_policy_contracts (
    repo_policy_contract_id text PRIMARY KEY,
    repo_root text NOT NULL,
    status text NOT NULL DEFAULT 'active',
    current_revision_id text,
    current_revision_no integer NOT NULL DEFAULT 0,
    current_contract_hash text,
    disclosure_repeat_limit integer NOT NULL DEFAULT 5,
    bug_disclosure_count integer NOT NULL DEFAULT 0,
    pattern_disclosure_count integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT operator_repo_policy_contracts_repo_root_nonblank CHECK (btrim(repo_root) <> ''),
    CONSTRAINT operator_repo_policy_contracts_status_check CHECK (
        status IN ('draft', 'active', 'superseded', 'revoked')
    ),
    CONSTRAINT operator_repo_policy_contracts_repeat_limit_nonnegative CHECK (disclosure_repeat_limit >= 0),
    CONSTRAINT operator_repo_policy_contracts_bug_count_nonnegative CHECK (bug_disclosure_count >= 0),
    CONSTRAINT operator_repo_policy_contracts_pattern_count_nonnegative CHECK (pattern_disclosure_count >= 0),
    CONSTRAINT operator_repo_policy_contracts_unique_repo_root UNIQUE (repo_root)
);

CREATE TABLE IF NOT EXISTS operator_repo_policy_contract_revisions (
    repo_policy_contract_revision_id text PRIMARY KEY,
    repo_policy_contract_id text NOT NULL,
    revision_no integer NOT NULL,
    parent_revision_id text,
    contract_hash text NOT NULL,
    contract_body jsonb NOT NULL,
    change_reason text NOT NULL,
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT operator_repo_policy_contract_revisions_contract_fkey
        FOREIGN KEY (repo_policy_contract_id)
        REFERENCES operator_repo_policy_contracts (repo_policy_contract_id)
        ON DELETE CASCADE,
    CONSTRAINT operator_repo_policy_contract_revisions_hash_nonblank CHECK (btrim(contract_hash) <> ''),
    CONSTRAINT operator_repo_policy_contract_revisions_change_reason_nonblank CHECK (btrim(change_reason) <> ''),
    CONSTRAINT operator_repo_policy_contract_revisions_created_by_nonblank CHECK (btrim(created_by) <> ''),
    CONSTRAINT operator_repo_policy_contract_revisions_contract_body_object CHECK (
        jsonb_typeof(contract_body) = 'object'
    ),
    CONSTRAINT operator_repo_policy_contract_revisions_unique_revision UNIQUE (
        repo_policy_contract_id,
        revision_no
    )
);

CREATE INDEX IF NOT EXISTS operator_repo_policy_contracts_status_idx
    ON operator_repo_policy_contracts (status, updated_at DESC);

CREATE INDEX IF NOT EXISTS operator_repo_policy_contract_revisions_contract_idx
    ON operator_repo_policy_contract_revisions (repo_policy_contract_id, revision_no DESC);

CREATE OR REPLACE FUNCTION touch_operator_repo_policy_contracts_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_operator_repo_policy_contracts_touch
    ON operator_repo_policy_contracts;
CREATE TRIGGER trg_operator_repo_policy_contracts_touch
    BEFORE UPDATE ON operator_repo_policy_contracts
    FOR EACH ROW EXECUTE FUNCTION touch_operator_repo_policy_contracts_updated_at();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'operator_repo_policy_contracts',
        'Operator repo policy contracts',
        'table',
        'Current repo-policy onboarding contract head rows with disclosure counters.',
        '{"migration":"326_operator_repo_policy_onboarding.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.operator_onboarding"}'::jsonb
    ),
    (
        'operator_repo_policy_contract_revisions',
        'Operator repo policy contract revisions',
        'table',
        'Append-only revisions for repo-policy onboarding contracts.',
        '{"migration":"326_operator_repo_policy_onboarding.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.operator_onboarding"}'::jsonb
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
    (
        'table.public.operator_repo_policy_contracts',
        'table',
        'operator_repo_policy_contracts',
        'public',
        'authority.operator_onboarding',
        'operator_repo_policy_contracts',
        'active',
        'registry',
        'praxis.engine',
        'architecture-policy::operator-onboarding::first-run-repo-policy-contract-and-pattern-disclosure',
        '{"purpose":"repo policy onboarding contract heads"}'::jsonb
    ),
    (
        'table.public.operator_repo_policy_contract_revisions',
        'table',
        'operator_repo_policy_contract_revisions',
        'public',
        'authority.operator_onboarding',
        'operator_repo_policy_contract_revisions',
        'active',
        'registry',
        'praxis.engine',
        'architecture-policy::operator-onboarding::first-run-repo-policy-contract-and-pattern-disclosure',
        '{"purpose":"repo policy onboarding contract revisions"}'::jsonb
    )
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;
