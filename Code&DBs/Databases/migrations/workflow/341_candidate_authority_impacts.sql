-- Migration 341: candidate_authority_impacts — the missing contract between
-- "candidate patch" and "authority replacement".
--
-- code_change_candidate_payloads.superseded_by exists candidate-to-candidate.
-- It does NOT carry the claim that this candidate replaces, retires, extends,
-- or leaves compatible some unit of dispatchable authority (operation rows,
-- HTTP routes, MCP tools, CLI aliases, migrations, database objects,
-- handlers, verifiers, event types, provider routes, source paths).
--
-- Every authority-bearing edit must declare its impact rows before approval
-- can take effect. Discovery_source distinguishes agent-declared rows from
-- runtime-derived rows — the runtime is the authority on what is actually
-- being touched; the agent's claim is one input that gets validated against
-- the runtime view. Validation_status records whether claim and runtime
-- discovery agree, disagree, or whether the runtime found an overlap the
-- agent did not declare.

BEGIN;

-- Intent: what this candidate is doing to the named authority unit.
CREATE TYPE candidate_authority_impact_intent AS ENUM (
    'fix',          -- repair existing authority in place; no predecessor named
    'extend',       -- add to existing authority surface; no predecessor retired
    'replace',      -- supersede a predecessor authority unit; predecessor MUST be named
    'retire',       -- remove a predecessor authority unit; no successor authority added
    'adapter',      -- thin wrapper over an existing authority (compat shim, transport)
    'compat_only'   -- explicit no-new-authority claim (refactor inside existing scope)
);

-- The kind of authority unit being touched. Authority overlap is multi-vector.
CREATE TYPE candidate_authority_unit_kind AS ENUM (
    'operation_ref',                -- operation_catalog_registry.operation_ref
    'authority_object_ref',         -- authority_object_registry.object_ref
    'data_dictionary_object_kind',  -- data_dictionary_objects.object_kind
    'http_route',                   -- "POST /api/foo"
    'mcp_tool',                     -- praxis_* tool name
    'cli_alias',                    -- praxis workflow <alias>
    'migration_ref',                -- 234_register_plan_operations.sql
    'database_object',              -- table/view/index name
    'handler_ref',                  -- runtime.module.fn qualified name
    'verifier_ref',                 -- verifier.* authority ref
    'event_type',                   -- authority_events.event_type
    'provider_route_ref',           -- task_type_routing rows
    'source_path'                   -- file path; weakest binding, used when no canonical authority exists
);

-- The dispatch-surface change the candidate produces.
CREATE TYPE candidate_authority_dispatch_effect AS ENUM (
    'none',       -- no dispatch surface change (body refactor only)
    'register',   -- new authority unit becomes dispatchable
    'retire',     -- existing dispatch path moves to compat or is removed
    'reroute',    -- traffic moves from old unit to new unit
    'shadow'      -- new path runs alongside old; divergence captured
);

-- Where this impact row came from. Runtime-derived rows are the trust spine.
CREATE TYPE candidate_authority_discovery_source AS ENUM (
    'agent_declared',  -- the writing agent claimed this impact
    'runtime_derived'  -- preflight discovered this overlap (not agent self-report)
);

-- Whether agent claim and runtime discovery agree.
CREATE TYPE candidate_authority_validation_status AS ENUM (
    'pending',          -- not yet validated by preflight
    'validated',        -- agent-declared row matches a runtime-derived row
    'contested',        -- agent claim disagrees with runtime discovery
    'runtime_addition'  -- runtime found overlap the agent did not declare
);

CREATE TABLE IF NOT EXISTS candidate_authority_impacts (
    impact_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id uuid NOT NULL,
    intent candidate_authority_impact_intent NOT NULL,
    unit_kind candidate_authority_unit_kind NOT NULL,
    unit_ref text NOT NULL,
    predecessor_unit_kind candidate_authority_unit_kind,
    predecessor_unit_ref text,
    dispatch_effect candidate_authority_dispatch_effect NOT NULL,
    subsumption_evidence_ref text,
    rollback_path text,
    discovery_source candidate_authority_discovery_source NOT NULL,
    validation_status candidate_authority_validation_status NOT NULL DEFAULT 'pending',
    validation_evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
    notes text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT candidate_authority_impacts_candidate_fkey
        FOREIGN KEY (candidate_id)
        REFERENCES code_change_candidate_payloads (candidate_id)
        ON DELETE CASCADE,
    CONSTRAINT candidate_authority_impacts_predecessor_required CHECK (
        (intent NOT IN ('replace', 'retire'))
        OR (predecessor_unit_kind IS NOT NULL AND predecessor_unit_ref IS NOT NULL)
    ),
    CONSTRAINT candidate_authority_impacts_predecessor_forbidden CHECK (
        (intent NOT IN ('fix', 'extend', 'compat_only'))
        OR (predecessor_unit_kind IS NULL AND predecessor_unit_ref IS NULL)
    ),
    CONSTRAINT candidate_authority_impacts_subsumption_required_for_replace CHECK (
        (intent <> 'replace')
        OR (subsumption_evidence_ref IS NOT NULL)
        OR (validation_status = 'pending')
    )
);

CREATE INDEX IF NOT EXISTS candidate_authority_impacts_candidate_idx
    ON candidate_authority_impacts (candidate_id, validation_status, intent);

CREATE INDEX IF NOT EXISTS candidate_authority_impacts_unit_idx
    ON candidate_authority_impacts (unit_kind, unit_ref);

CREATE INDEX IF NOT EXISTS candidate_authority_impacts_predecessor_idx
    ON candidate_authority_impacts (predecessor_unit_kind, predecessor_unit_ref)
    WHERE predecessor_unit_ref IS NOT NULL;

COMMENT ON TABLE candidate_authority_impacts IS
    'Required contract between candidate patches and the authority units they change. Runtime-derived rows are the trust spine; agent-declared rows are validated against them.';
COMMENT ON COLUMN candidate_authority_impacts.discovery_source IS
    'agent_declared rows are the writing agents claim. runtime_derived rows are produced by preflight overlap scan. Validation compares the two.';
COMMENT ON COLUMN candidate_authority_impacts.dispatch_effect IS
    'How this candidate changes the live dispatch surface. register/retire/reroute/shadow are the dispatch-visible transitions; none means body-only refactor.';
COMMENT ON COLUMN candidate_authority_impacts.subsumption_evidence_ref IS
    'For intent=replace, points at a verification_runs.run_id (or equivalent verifier ledger) that proves the new unit subsumes the predecessor unit on the inputs that matter. Required at validated status.';

COMMIT;
