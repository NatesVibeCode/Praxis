-- Migration 247: Register workflow plumbing as data_dictionary_objects.
--
-- Operator standing order: every workflow item needs a reflected entry in
-- Praxis.db, so the LLM author's sandbox can read its target shape from
-- authority instead of hardcoded Python tables, and the dependency
-- synthesizer can walk gate / stage / capability rows the same way it
-- walks pill rows today.
--
-- This migration extends the data_dictionary_objects.category check to
-- admit four new families and seeds the rows behind them:
--
--   - 'gate'         : workflow gates (release, dispatch, verification,
--                      approval, type-flow, write-scope) with required_for
--                      stage list + scaffold/runtime distinction.
--   - 'stage'        : the 5 plan stages (build/fix/review/test/research)
--                      with explicit produces / consumes / required_gates.
--   - 'capability'   : capability slugs the auto-router consumes
--                      (code_generation, research, review, testing, etc.).
--   - 'plan_field'   : every PlanPacket field the LLM author must fill
--                      (label, description, write, stage, depends_on,
--                      consumes, produces, capabilities, gates, agent,
--                      prompt, workdir, parameters, on_failure, on_success,
--                      timeout, budget) so a sandbox can declare the
--                      target shape without re-reading the pydantic class.
--
-- Pairs with:
--   - runtime.intent_suggestion (Layer 0)
--   - runtime.intent_dependency  (synthesizer — reads stage/gate rows)
--   - runtime.plan_section_author (LLM author — reads plan_field rows)
--   - runtime.plan_section_validator (validator — reads plan_field rows)

BEGIN;

ALTER TABLE data_dictionary_objects
    DROP CONSTRAINT IF EXISTS data_dictionary_objects_category_check;

ALTER TABLE data_dictionary_objects
    ADD CONSTRAINT data_dictionary_objects_category_check
        CHECK (category IN (
            'table',
            'object_type',
            'integration',
            'dataset',
            'ingest',
            'decision',
            'receipt',
            'tool',
            'object',
            'command',
            'event',
            'projection',
            'service_bus_channel',
            'feedback_stream',
            'definition',
            'runtime_target',
            'gate',
            'stage',
            'capability',
            'plan_field'
        ));


-- =====================================================================
-- Gate registry
-- =====================================================================
-- Every gate kind the runtime can attach to a packet or workflow.
-- ``required_for`` lists stages that MUST scaffold this gate before
-- LLM authoring; ``runtime_check`` says whether the gate fires during
-- execution (true) or is a static-time check only (false).
INSERT INTO data_dictionary_objects (
    object_kind, label, category, summary, origin_ref, metadata
) VALUES
    ('release_gate', 'Release gate', 'gate',
     'Downstream release boundary. Must pass before a workflow''s output is published or a follow-on workflow is launched.',
     '{"source":"migration.247","authority":"runtime.edge_release"}'::jsonb,
     '{"required_for":["build","fix","review"],"runtime_check":true,"scaffold_priority":10}'::jsonb),

    ('dispatch_boundary', 'Dispatch boundary', 'gate',
     'Authoring vs. execution split. A draft can mutate freely; submitting to Run requires an approved execution manifest.',
     '{"source":"migration.247","authority":"workflow.execution_manifest"}'::jsonb,
     '{"required_for":["build","fix"],"runtime_check":true,"scaffold_priority":20}'::jsonb),

    ('verification_gate', 'Verification gate', 'gate',
     'Per-job verify_refs. Each packet that mutates code must declare verifiers; absence surfaces as a typed gap before launch.',
     '{"source":"migration.247","authority":"runtime.verification"}'::jsonb,
     '{"required_for":["build","fix"],"runtime_check":true,"scaffold_priority":30,"emits":["verify_refs"]}'::jsonb),

    ('approval_gate', 'Approval gate', 'gate',
     'Operator approval checkpoint. Only fires when policy or budget thresholds require human sign-off.',
     '{"source":"migration.247","authority":"runtime.spec_compiler.approve_proposed_plan"}'::jsonb,
     '{"required_for":[],"runtime_check":false,"scaffold_priority":40,"opt_in":true}'::jsonb),

    ('type_flow_gate', 'Type-flow gate', 'gate',
     'consumes/produces compatibility check across packet edges. Static — fires at compile, not runtime.',
     '{"source":"migration.247","authority":"runtime.workflow_type_contracts"}'::jsonb,
     '{"required_for":["build","fix","review","test","research"],"runtime_check":false,"scaffold_priority":5}'::jsonb),

    ('write_scope_gate', 'Write-scope gate', 'gate',
     'Per-packet write_scope envelope. Mutations outside the declared scope fail closed at admission.',
     '{"source":"migration.247","authority":"runtime.workflow._admission"}'::jsonb,
     '{"required_for":["build","fix"],"runtime_check":true,"scaffold_priority":15}'::jsonb),

    ('regression_gate', 'Regression gate', 'gate',
     'Test-suite regression check. Required after fix-stage packets; the test suite must pass against the prior baseline.',
     '{"source":"migration.247","authority":"runtime.verification"}'::jsonb,
     '{"required_for":["fix"],"runtime_check":true,"scaffold_priority":25}'::jsonb),

    ('budget_gate', 'Budget gate', 'gate',
     'Project-plan budget envelope. Authoring + run cost must stay inside the declared cap or escalate.',
     '{"source":"migration.247","authority":"runtime.project_plan_budget"}'::jsonb,
     '{"required_for":[],"runtime_check":true,"scaffold_priority":50,"opt_in":true}'::jsonb)

ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();


-- =====================================================================
-- Stage registry
-- =====================================================================
-- The 5 plan stages with explicit I/O contracts and required gates.
-- ``produces`` / ``consumes`` mirror the type-token rows from migration
-- 242; the synthesizer joins this row to those type rows when wiring
-- depends_on edges.
INSERT INTO data_dictionary_objects (
    object_kind, label, category, summary, origin_ref, metadata
) VALUES
    ('stage:build', 'Stage: build', 'stage',
     'Implementation stage. Mutates code or schema. Always produces a code_change + diff + execution_receipt and may consume drafts, research, analysis, or architecture plans.',
     '{"source":"migration.247","authority":"runtime.canonical_workflows.VALID_STAGES"}'::jsonb,
     '{"produces":["code_change","diff","execution_receipt"],"consumes":["draft","summary","research_findings","evidence_pack","analysis_result","architecture_plan","validated_input"],"required_gates":["verification_gate","write_scope_gate","release_gate","type_flow_gate","dispatch_boundary"],"capabilities":["code_generation","architecture"]}'::jsonb),

    ('stage:fix', 'Stage: fix', 'stage',
     'Repair stage. Targets a known failure or diagnosis and produces a code_change. Always followed by a regression_gate.',
     '{"source":"migration.247","authority":"runtime.canonical_workflows.VALID_STAGES"}'::jsonb,
     '{"produces":["code_change","diff","execution_receipt"],"consumes":["diagnosis","failure","error","review_result","research_findings"],"required_gates":["verification_gate","regression_gate","write_scope_gate","release_gate","type_flow_gate","dispatch_boundary"],"capabilities":["code_generation","debug"]}'::jsonb),

    ('stage:review', 'Stage: review', 'stage',
     'Audit / score stage. Reads upstream output and produces a review_result; never mutates code itself.',
     '{"source":"migration.247","authority":"runtime.canonical_workflows.VALID_STAGES"}'::jsonb,
     '{"produces":["review_result","analysis_result"],"consumes":["code_change","diff","draft","research_findings","evidence_pack","analysis_result"],"required_gates":["release_gate","type_flow_gate"],"capabilities":["review","analysis","validation"]}'::jsonb),

    ('stage:test', 'Stage: test', 'stage',
     'Test-authoring or test-execution stage. Produces an execution_receipt and review_result indicating pass/fail.',
     '{"source":"migration.247","authority":"runtime.canonical_workflows.VALID_STAGES"}'::jsonb,
     '{"produces":["execution_receipt","review_result"],"consumes":["code_change","diff","draft","architecture_plan"],"required_gates":["verification_gate","type_flow_gate"],"capabilities":["testing","code_generation"]}'::jsonb),

    ('stage:research', 'Stage: research', 'stage',
     'Discovery / search / analysis stage. Produces research_findings + evidence_pack and may produce an analysis_result. Never mutates code.',
     '{"source":"migration.247","authority":"runtime.canonical_workflows.VALID_STAGES"}'::jsonb,
     '{"produces":["research_findings","evidence_pack","analysis_result","architecture_plan"],"consumes":["input_text","validated_input","requirements"],"required_gates":["type_flow_gate"],"capabilities":["research","analysis","architecture"]}'::jsonb)

ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();


-- =====================================================================
-- Capability registry
-- =====================================================================
-- Capability slugs the auto-router and the LLM author both consume. Slugs
-- match the keys used in spec_compiler / capability resolution today.
INSERT INTO data_dictionary_objects (
    object_kind, label, category, summary, origin_ref, metadata
) VALUES
    ('capability:code_generation', 'Capability: code_generation', 'capability',
     'Authoring or mutating source code, schema, configuration files.',
     '{"source":"migration.247","authority":"runtime.spec_compiler"}'::jsonb,
     '{"stages":["build","fix"],"router_priority":10}'::jsonb),

    ('capability:code_review', 'Capability: code_review', 'capability',
     'Reading code or diffs and producing structured findings (severity, suggestion, rationale).',
     '{"source":"migration.247","authority":"runtime.spec_compiler"}'::jsonb,
     '{"stages":["review","build"],"router_priority":15}'::jsonb),

    ('capability:debug', 'Capability: debug', 'capability',
     'Diagnosing failures from logs, stack traces, or reproduction artifacts.',
     '{"source":"migration.247","authority":"runtime.spec_compiler"}'::jsonb,
     '{"stages":["fix"],"router_priority":20}'::jsonb),

    ('capability:research', 'Capability: research', 'capability',
     'Gathering external or internal context (docs, specs, dictionary lookups, search) into structured findings.',
     '{"source":"migration.247","authority":"runtime.spec_compiler"}'::jsonb,
     '{"stages":["research"],"router_priority":10}'::jsonb),

    ('capability:retrieval', 'Capability: retrieval', 'capability',
     'Fetching specific authoritative artifacts (OpenAPI specs, SDK source, schema) given a target reference.',
     '{"source":"migration.247","authority":"runtime.spec_compiler"}'::jsonb,
     '{"stages":["research"],"router_priority":15}'::jsonb),

    ('capability:analysis', 'Capability: analysis', 'capability',
     'Scoring / classifying / triaging structured input into a verdict or ranking.',
     '{"source":"migration.247","authority":"runtime.spec_compiler"}'::jsonb,
     '{"stages":["research","review"],"router_priority":20}'::jsonb),

    ('capability:review', 'Capability: review', 'capability',
     'Audit / compare / verdict — produces a review_result suitable for downstream gating.',
     '{"source":"migration.247","authority":"runtime.spec_compiler"}'::jsonb,
     '{"stages":["review"],"router_priority":10}'::jsonb),

    ('capability:validation', 'Capability: validation', 'capability',
     'Checking input against a contract or rule and emitting validated_input or a structured failure.',
     '{"source":"migration.247","authority":"runtime.spec_compiler"}'::jsonb,
     '{"stages":["review","test"],"router_priority":25}'::jsonb),

    ('capability:testing', 'Capability: testing', 'capability',
     'Authoring or executing tests; producing an execution_receipt with pass/fail verdict.',
     '{"source":"migration.247","authority":"runtime.spec_compiler"}'::jsonb,
     '{"stages":["test"],"router_priority":10}'::jsonb),

    ('capability:architecture', 'Capability: architecture', 'capability',
     'Designing structure and producing an architecture_plan or requirements set.',
     '{"source":"migration.247","authority":"runtime.spec_compiler"}'::jsonb,
     '{"stages":["research","build"],"router_priority":20}'::jsonb),

    ('capability:creative', 'Capability: creative', 'capability',
     'Open-ended drafting / brainstorming where many valid completions exist.',
     '{"source":"migration.247","authority":"runtime.spec_compiler"}'::jsonb,
     '{"stages":["research","build"],"router_priority":30}'::jsonb)

ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();


-- =====================================================================
-- PlanPacket field schema
-- =====================================================================
-- Every field a per-section LLM author must fill on a PlanPacket. The
-- ``required`` flag tells the validator to fail closed when missing;
-- ``allow_default`` signals the synthesizer may pre-fill from a stage
-- contract; ``picker_source`` tells the LLM which dictionary row family
-- to pick values from (e.g. capabilities pull from category='capability').
INSERT INTO data_dictionary_objects (
    object_kind, label, category, summary, origin_ref, metadata
) VALUES
    ('plan_field:label', 'PlanPacket field: label', 'plan_field',
     'Stable identifier for the packet (e.g. "research_search"). Referenced by depends_on edges.',
     '{"source":"migration.247","authority":"runtime.spec_compiler.PlanPacket"}'::jsonb,
     '{"required":true,"type":"string","picker_source":null,"order":1}'::jsonb),

    ('plan_field:description', 'PlanPacket field: description', 'plan_field',
     'Human-readable description of what this packet does. Drives prompt authoring.',
     '{"source":"migration.247","authority":"runtime.spec_compiler.PlanPacket"}'::jsonb,
     '{"required":true,"type":"string","picker_source":null,"order":2}'::jsonb),

    ('plan_field:stage', 'PlanPacket field: stage', 'plan_field',
     'One of the registered stages (build/fix/review/test/research).',
     '{"source":"migration.247","authority":"runtime.spec_compiler.PlanPacket"}'::jsonb,
     '{"required":true,"type":"enum","picker_source":"category=stage","order":3}'::jsonb),

    ('plan_field:write', 'PlanPacket field: write', 'plan_field',
     'List of file or directory globs the packet may mutate. Workspace-root ([\".\"]) requires explicit override.',
     '{"source":"migration.247","authority":"runtime.spec_compiler.PlanPacket"}'::jsonb,
     '{"required":true,"type":"array<string>","picker_source":null,"order":4,"forbid_workspace_root":true}'::jsonb),

    ('plan_field:depends_on', 'PlanPacket field: depends_on', 'plan_field',
     'Labels of upstream packets this packet waits for. Mostly synthesized from stage I/O + pill graph; LLM only adds intra-stage semantic edges.',
     '{"source":"migration.247","authority":"runtime.spec_compiler.PlanPacket"}'::jsonb,
     '{"required":false,"type":"array<string>","picker_source":"sibling.label","order":5,"synthesized":true}'::jsonb),

    ('plan_field:consumes', 'PlanPacket field: consumes', 'plan_field',
     'Type tokens this packet reads. Pre-filled from stage contract; LLM may add specifics.',
     '{"source":"migration.247","authority":"runtime.spec_compiler.PlanPacket"}'::jsonb,
     '{"required":true,"type":"array<string>","picker_source":"category=object_type","order":6,"floor_from":"stage.consumes"}'::jsonb),

    ('plan_field:produces', 'PlanPacket field: produces', 'plan_field',
     'Type tokens this packet emits. Pre-filled from stage contract; LLM may add specifics.',
     '{"source":"migration.247","authority":"runtime.spec_compiler.PlanPacket"}'::jsonb,
     '{"required":true,"type":"array<string>","picker_source":"category=object_type","order":7,"floor_from":"stage.produces"}'::jsonb),

    ('plan_field:capabilities', 'PlanPacket field: capabilities', 'plan_field',
     'Capability slugs the auto-router uses to pick a model. Pre-filled from stage contract; LLM refines.',
     '{"source":"migration.247","authority":"runtime.spec_compiler.PlanPacket"}'::jsonb,
     '{"required":true,"type":"array<string>","picker_source":"category=capability","order":8,"floor_from":"stage.capabilities"}'::jsonb),

    ('plan_field:gates', 'PlanPacket field: gates', 'plan_field',
     'Gate ids attached to this packet. Synthesizer scaffolds the required ones from stage.required_gates; LLM fills gate-specific params.',
     '{"source":"migration.247","authority":"runtime.spec_compiler.PlanPacket"}'::jsonb,
     '{"required":true,"type":"array<object>","picker_source":"category=gate","order":9,"floor_from":"stage.required_gates"}'::jsonb),

    ('plan_field:agent', 'PlanPacket field: agent', 'plan_field',
     'Concrete agent slug or auto/<task_type>. CLI-default per provider-routing standing order.',
     '{"source":"migration.247","authority":"runtime.spec_compiler.PlanPacket"}'::jsonb,
     '{"required":true,"type":"string","picker_source":"task_type_routing","order":10,"default":"auto/<stage>"}'::jsonb),

    ('plan_field:prompt', 'PlanPacket field: prompt', 'plan_field',
     'Actionable prompt the agent receives. Must reference parameters by {name} and upstream outputs by id; no vague placeholders.',
     '{"source":"migration.247","authority":"runtime.spec_compiler.PlanPacket"}'::jsonb,
     '{"required":true,"type":"string","picker_source":null,"order":11,"forbid_placeholders":["TBD","TODO","FIXME"]}'::jsonb),

    ('plan_field:workdir', 'PlanPacket field: workdir', 'plan_field',
     'Working directory relative to project root. Defaults to caller workdir.',
     '{"source":"migration.247","authority":"runtime.spec_compiler.PlanPacket"}'::jsonb,
     '{"required":false,"type":"string","picker_source":null,"order":12}'::jsonb),

    ('plan_field:parameters', 'PlanPacket field: parameters', 'plan_field',
     'Per-packet parameter bindings: which workflow input or upstream packet output feeds each {placeholder} in the prompt.',
     '{"source":"migration.247","authority":"runtime.spec_compiler.PlanPacket"}'::jsonb,
     '{"required":true,"type":"object","picker_source":"workflow.parameters+sibling.outputs","order":13}'::jsonb),

    ('plan_field:on_failure', 'PlanPacket field: on_failure', 'plan_field',
     'Policy when the packet fails: abort | continue | retry | escalate. Picked from policy catalog.',
     '{"source":"migration.247","authority":"runtime.spec_compiler.PlanPacket"}'::jsonb,
     '{"required":false,"type":"enum","picker_source":"policy.on_failure","order":14,"default":"abort"}'::jsonb),

    ('plan_field:on_success', 'PlanPacket field: on_success', 'plan_field',
     'Policy when the packet succeeds: continue | branch | terminate.',
     '{"source":"migration.247","authority":"runtime.spec_compiler.PlanPacket"}'::jsonb,
     '{"required":false,"type":"enum","picker_source":"policy.on_success","order":15,"default":"continue"}'::jsonb),

    ('plan_field:timeout', 'PlanPacket field: timeout', 'plan_field',
     'Per-packet wall-clock timeout in seconds.',
     '{"source":"migration.247","authority":"runtime.spec_compiler.PlanPacket"}'::jsonb,
     '{"required":false,"type":"integer","picker_source":null,"order":16,"default":300}'::jsonb),

    ('plan_field:budget', 'PlanPacket field: budget', 'plan_field',
     'Per-packet token / dollar budget envelope. Inherits from project_plan_budget when absent.',
     '{"source":"migration.247","authority":"runtime.project_plan_budget"}'::jsonb,
     '{"required":false,"type":"object","picker_source":null,"order":17}'::jsonb)

ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;

-- Verification (run manually):
--   SELECT category, count(*) FROM data_dictionary_objects
--    WHERE category IN ('gate','stage','capability','plan_field')
--    GROUP BY category ORDER BY category;
--     gate       8
--     stage      5
--     capability 11
--     plan_field 17
