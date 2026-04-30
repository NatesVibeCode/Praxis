# Phase 4 Build Packet: Hierarchy Management and Task Environment Contracts

## Purpose
Define a bounded, implementation-ready specification for Phase 4 of the client operating model covering:

- Business hierarchy representation
- Ownership and stewardship rules
- SOP references
- Task environment contracts
- Allowed tools
- Read/write scopes
- Model policy
- Verifier references
- Append-only revisions
- Staleness handling
- Tests and validation

This packet is intentionally limited to contract definition, governance rules, and validation criteria. It does not authorize application code changes, migration work, or production rollout.

## Phase Outcome
At the end of Phase 4, the system must support a durable task contract that can be attached to a unit of work and evaluated before execution. That contract must bind work to:

- A business hierarchy node
- An accountable owner and steward
- A known SOP or explicit SOP gap
- A constrained tool set
- Explicit read/write boundaries
- An approved model policy
- One or more verifier references
- A revision chain with append-only history
- A freshness signal and staleness behavior

## Bounded Scope

### In Scope

- Canonical hierarchy concepts and required fields
- Ownership and stewardship semantics
- Task contract schema requirements
- Policy rules for tools, scopes, models, and verifiers
- Revision and staleness rules
- Validation gates and acceptance criteria
- Test matrix for contract correctness

### Out of Scope

- UI design
- End-user training content
- Runtime orchestration implementation
- Secrets management implementation
- Code execution sandbox implementation
- Model vendor integration details
- Backfill of historical tasks

## Core Artifacts
Phase 4 produces the following logical artifacts:

1. `Hierarchy Registry`
   Stores business hierarchy nodes and their relationships.
2. `Responsibility Registry`
   Stores owner/steward assignments and escalation metadata.
3. `SOP Reference Catalog`
   Stores references to governing SOPs, playbooks, and exception records.
4. `Task Environment Contract`
   Stores execution constraints for an individual task or task template.
5. `Verifier Registry`
   Stores validators, test suites, review roles, and approval checks that may be attached to work.
6. `Revision Ledger`
   Stores append-only changes to hierarchy and contract state.

## Business Hierarchy Specification

### Objective
Represent the operating structure in a way that allows each task to inherit business context, control boundaries, and accountability.

### Required Hierarchy Levels
The hierarchy model must support at least these levels:

- `enterprise`
- `portfolio` or `business_unit`
- `program`
- `product` or `service`
- `capability`
- `workflow`
- `task_family`
- `task`

The exact labels may vary by client, but the model must preserve:

- Parent-child relationships
- A single canonical path for any task contract
- Stable IDs for each node
- Effective dates for node validity

### Required Fields Per Hierarchy Node

- `node_id`
- `node_type`
- `node_name`
- `parent_node_id`
- `status` (`draft`, `active`, `deprecated`, `retired`)
- `effective_from`
- `effective_to` (nullable)
- `owner_ref`
- `steward_ref`
- `default_sop_refs`
- `default_tool_policy_ref`
- `default_scope_policy_ref`
- `default_model_policy_ref`
- `default_verifier_refs`
- `revision_id`

### Hierarchy Rules

1. Every executable task must resolve to exactly one active hierarchy path.
2. No task may execute against a retired hierarchy node.
3. Inherited defaults may be narrowed at lower levels, but may not be broadened without an explicit exception record.
4. Parent-level policies remain authoritative unless a child-level override is explicitly allowed.
5. Hierarchy edits must create new revisions rather than mutating history in place.

## Ownership and Stewardship

### Accountability Model

- `Owner`
  Accountable for business outcome, priority, and risk acceptance.
- `Steward`
  Accountable for process integrity, policy conformance, and metadata quality.
- `Operator`
  Executes the task within the approved contract.
- `Verifier`
  Confirms the task output satisfies defined checks.

### Required Fields

- `owner_ref`
- `owner_role`
- `steward_ref`
- `steward_role`
- `escalation_ref`
- `approval_requirements`
- `delegation_policy_ref`

### Rules

1. Every active hierarchy node must have one named owner and one named steward.
2. A task contract must resolve owner and steward references at execution time.
3. If owner and steward are both missing, the task is invalid.
4. If the owner is present and the steward is missing, the task is blocked unless an exception policy allows temporary operation.
5. Delegation must be explicit, time-bounded, and revisioned.
6. Verifiers cannot self-approve if the policy requires independent review.

## SOP References

### Purpose
SOP references anchor each task to the approved way of working.

### Required SOP Metadata

- `sop_ref`
- `sop_title`
- `sop_version`
- `sop_status`
- `sop_owner_ref`
- `effective_from`
- `effective_to` (nullable)
- `source_uri` or repository locator
- `exception_policy_ref` (nullable)

### Rules

1. Each task contract must contain at least one SOP reference or an explicit `sop_gap_ref`.
2. If multiple SOPs apply, one must be designated primary.
3. Deprecated SOPs may remain readable for audit but may not be assigned to new task contracts.
4. SOP gaps require owner approval and a review expiry date.

## Task Environment Contract

### Purpose
The task environment contract defines exactly what a task is allowed to access, change, and use during execution.

### Required Contract Fields

- `contract_id`
- `task_ref`
- `hierarchy_node_id`
- `owner_ref`
- `steward_ref`
- `sop_refs`
- `allowed_tool_refs`
- `read_scope_refs`
- `write_scope_refs`
- `model_policy_ref`
- `verifier_refs`
- `input_classification`
- `output_classification`
- `data_retention_ref`
- `staleness_policy_ref`
- `revision_id`
- `status`
- `effective_from`
- `effective_to` (nullable)

### Contract Rules

1. A task may execute only when exactly one active contract revision is selected.
2. Missing tool, scope, model, or verifier policy makes the contract invalid.
3. Contracts must be explicit; broad implicit defaults are not sufficient for execution.
4. Runtime behavior must be narrowed by contract, not inferred from operator intent.

## Allowed Tools Policy

### Tool Classes
The policy model must support tool classification such as:

- `no_tool`
- `read_only_repo`
- `doc_authoring`
- `structured_data_read`
- `structured_data_write`
- `ticketing`
- `messaging`
- `web_research`
- `code_execution`
- `deployment`
- `admin_override`

### Required Tool Metadata

- `tool_ref`
- `tool_name`
- `tool_class`
- `capabilities`
- `data_domains`
- `approval_level`
- `logging_requirements`
- `allowed_operations`
- `prohibited_operations`

### Rules

1. Contracts must enumerate allowed tools, not just prohibited tools.
2. Tool rights must be least-privilege.
3. High-risk tool classes must require explicit approval and verifier coverage.
4. If a tool is not listed in the contract, it is denied by default.
5. Tool substitutions require a contract revision or approved policy alias.

## Read/Write Scope Policy

### Scope Objective
Prevent tasks from reading or modifying resources outside their approved operating boundary.

### Required Scope Dimensions

- Repository or document location
- Dataset or table group
- Application or service boundary
- Environment tier (`dev`, `test`, `prod`)
- Classification level
- Tenant or client boundary

### Required Read Scope Fields

- `scope_ref`
- `resource_type`
- `resource_locator`
- `access_mode` (`read`)
- `environment`
- `classification`
- `tenant_boundary`

### Required Write Scope Fields

- `scope_ref`
- `resource_type`
- `resource_locator`
- `access_mode` (`write`, `append`, `update`)
- `environment`
- `classification`
- `change_constraints`
- `rollback_requirement`

### Rules

1. Read and write scopes must be separately declared.
2. Write scopes must be narrower than or equal to read scopes unless an exception is approved.
3. Production write access requires explicit policy support and elevated verification.
4. Append-only locations must be marked as such and cannot be revised via overwrite semantics.
5. Cross-tenant access is invalid unless the task family explicitly permits it.

## Model Policy

### Purpose
Define what model classes may be used for a task and under what constraints.

### Required Model Policy Fields

- `model_policy_ref`
- `approved_model_classes`
- `approved_model_ids` or alias set
- `reasoning_limit`
- `tool_use_limit`
- `data_handling_constraints`
- `retention_constraints`
- `human_review_requirement`
- `disallowed_use_cases`

### Rules

1. Tasks must bind to an approved model policy before execution.
2. Policy may approve a model class with a managed alias, but the resolved model used at runtime must be logged.
3. High-impact tasks must require human review before finalization.
4. Tasks involving regulated or sensitive data must use the stricter applicable policy.
5. If the requested model falls outside policy, execution is denied rather than auto-downgraded unless fallback is explicitly approved.

## Verifier References

### Purpose
Ensure each task has a defined method of validation proportional to its risk and output type.

### Verifier Types

- Automated schema validator
- Unit/integration test suite
- Lint or static analysis
- Policy compliance check
- Human reviewer
- Business sign-off
- Audit log check

### Required Verifier Fields

- `verifier_ref`
- `verifier_type`
- `applicability_rule`
- `pass_criteria`
- `failure_severity`
- `independence_requirement`
- `evidence_output_ref`

### Rules

1. Every task contract must include at least one verifier.
2. Write-enabled tasks must include at least one nontrivial verifier beyond existence checks.
3. High-risk tasks must include an independent verifier.
4. Verifier failure blocks contract completion unless an approved override exists.

## Append-Only Revisions

### Revision Model
Hierarchy nodes, policies, and task contracts must be revisioned through append-only records.

### Required Revision Fields

- `revision_id`
- `entity_type`
- `entity_id`
- `prior_revision_id`
- `change_summary`
- `changed_by`
- `changed_at`
- `change_reason`
- `approval_ref` (nullable)
- `supersedes_effective_from`

### Rules

1. Prior revisions remain queryable for audit.
2. Direct destructive updates are disallowed.
3. A revision must identify the exact predecessor it supersedes.
4. Runtime selection must resolve to the latest valid revision effective at execution time.
5. Emergency overrides must create explicit revisions and expiry windows.

## Staleness Policy

### Purpose
Detect contracts that are no longer trustworthy because hierarchy, SOPs, models, tools, or scopes have changed.

### Staleness Triggers

- Referenced SOP version retired or superseded
- Owner or steward assignment removed
- Tool policy changed
- Read/write scope changed
- Model policy changed
- Verifier invalidated
- Underlying hierarchy node deprecated or retired
- Review interval exceeded

### Required Staleness Fields

- `staleness_policy_ref`
- `review_interval_days`
- `trigger_types`
- `block_on_stale`
- `grace_period_days`
- `revalidation_requirements`

### Rules

1. Contracts must be re-evaluated on any trigger event.
2. If `block_on_stale` is true, execution is denied until revalidated.
3. A stale contract may remain visible for audit but not eligible for new execution.
4. Revalidation must generate a new revision or an explicit reaffirmation record.

## Validation Gates

### Pre-Execution Validation
Before a task may run, the system must confirm:

1. Hierarchy node is active and uniquely resolved.
2. Owner and steward are present and valid.
3. SOP references are active, or a valid SOP gap exception exists.
4. Allowed tools are present and non-empty if tooling is required.
5. Read/write scopes are syntactically and semantically valid.
6. Model policy is approved for the task’s classification and risk.
7. Verifier references are present and applicable.
8. Contract is not stale.
9. Revision chain is intact.

### Post-Execution Validation
Before a task is marked complete, the system must confirm:

1. All required verifiers executed.
2. Evidence references were recorded.
3. Any write activity remained within declared scope.
4. Any exceptions or overrides were logged.

## Test Plan

### Contract Schema Tests

- Accept valid contract with all required fields
- Reject contract missing hierarchy reference
- Reject contract missing owner/steward resolution
- Reject contract with no verifier
- Reject contract with no model policy

### Hierarchy Resolution Tests

- Resolve a valid single hierarchy path
- Reject ambiguous hierarchy mapping
- Reject retired node
- Inherit defaults from parent where allowed
- Reject unauthorized child broadening of parent policy

### Ownership and SOP Tests

- Reject missing owner
- Reject missing steward when no temporary exception exists
- Accept active SOP reference
- Reject deprecated SOP on new contract
- Accept approved SOP gap with expiry

### Tool and Scope Tests

- Deny unlisted tool use
- Accept least-privilege allowed tool set
- Reject write scope broader than policy allows
- Reject cross-tenant access without approval
- Enforce append-only write rule where required

### Model Policy Tests

- Accept approved model alias resolving to logged runtime model
- Reject disallowed model
- Require human review for high-impact task
- Enforce stricter policy for sensitive data classification

### Revision and Staleness Tests

- Preserve prior revisions after update
- Reject destructive update attempt
- Mark contract stale when referenced SOP is superseded
- Block execution when stale and blocking is enabled
- Accept revalidated contract with new revision

### Verifier Tests

- Require independent verifier for high-risk task
- Block completion on verifier failure
- Record evidence output reference

## Acceptance Criteria
Phase 4 is complete when:

1. A written contract specification exists for hierarchy, ownership, SOP references, tool policy, scope policy, model policy, verifier references, revisions, and staleness.
2. Each executable task can be bound to exactly one active contract revision.
3. Validation gates are defined for both pre-execution and post-execution states.
4. Test cases cover happy path, denial path, stale path, and override path behavior.
5. The packet remains implementation-neutral and does not require code changes to be considered complete as a planning artifact.

## Delivery Notes

- This packet is the governing plan for Phase 4 contract design.
- Any future implementation should preserve the bounded scope defined here.
- Any expansion beyond these controls should be proposed as a later phase or as an approved change request.
