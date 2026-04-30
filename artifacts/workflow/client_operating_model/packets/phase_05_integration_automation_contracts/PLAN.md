# Phase 5 Plan: Integration Action and Automation Contract Capture

## Purpose

Define the bounded implementation and validation packet for capturing integration action contracts and automation behavior across external systems, internal workflow engines, and event-driven automations. This phase produces auditable contracts for what can be invoked, under what permissions, with what side effects, how retries behave, and how operators validate or roll back automation safely.

## Scope

In scope:

- Document each supported integration action as a typed contract.
- Capture idempotency expectations and replay behavior.
- Record side effects, downstream writes, and notification behavior.
- Define webhook and event ingress/egress behavior.
- Snapshot automation rules and trigger conditions at capture time.
- Specify rollback, compensating action, and operator intervention expectations.
- Record actor, role, token, and permission dependencies.
- Define observability, logging, metrics, and audit requirements.
- Enumerate typed gaps where implementation or schema coverage is incomplete.
- Define tests and validation evidence required to accept the phase.

Out of scope:

- Shipping new automation code, connector code, or workflow runtime changes.
- Refactoring existing integration implementations.
- Changing production permissions, secrets, or webhook endpoints.
- Creating monitoring dashboards beyond documented requirements.

## Objectives

1. Produce a complete inventory of integration actions and automations in the current operating model.
2. Make action execution behavior explicit enough for safe automation, review, and incident handling.
3. Identify contract gaps that block deterministic execution or reliable rollback.
4. Define acceptance criteria that can be validated without changing application code.

## Deliverables

- `PLAN.md` for Phase 5 with explicit capture requirements.
- Integration action contract inventory.
- Automation rule snapshot set with source-of-truth references.
- Gap register for missing typing, unclear side effects, or unknown retry behavior.
- Validation checklist with evidence requirements.

## Artifact Set

Create or populate the following artifacts under this phase packet as plain documentation artifacts:

- `action_contract_inventory.md`
- `automation_rule_snapshots.md`
- `webhook_event_matrix.md`
- `permissions_and_identities.md`
- `observability_and_audit_requirements.md`
- `typed_gap_register.md`
- `validation_checklist.md`

If repository conventions require alternate filenames, preserve the same content boundaries.

## Workstream 1: Action Contract Capture

For every integration action, capture:

- Stable action identifier.
- Human-readable action name.
- Owning domain or system.
- Source system and target system.
- Triggering surface: manual, workflow, scheduled automation, webhook reaction, or event consumer.
- Required input schema with field names, types, required/optional flags, defaults, and validation constraints.
- Output schema including success payload, partial-success payload, and error shape.
- Preconditions and invariants.
- Execution timeout and sync vs async behavior.
- Idempotency key strategy or explicit statement that idempotency is not supported.
- Retry policy and duplicate handling semantics.
- Known failure modes and operator-visible error surfaces.

Required action categories:

- Create/update/delete operations in external systems.
- State transition operations.
- Notification or message dispatch actions.
- File or document generation or transfer actions.
- Reconciliation or synchronization actions.
- Manual override or recovery actions.

### Action Contract Template

Use this structure for each action:

| Field | Requirement |
| --- | --- |
| `action_id` | Stable machine-readable identifier |
| `name` | Operator-facing label |
| `owner` | Team or function accountable for correctness |
| `systems` | Source and destination systems |
| `trigger_types` | Manual, scheduled, event-driven, webhook-driven, workflow step |
| `inputs` | Typed input fields and validation rules |
| `outputs` | Typed result and error payloads |
| `side_effects` | Persistent writes, notifications, external mutations |
| `idempotency` | Key source, dedupe window, replay semantics |
| `permissions` | Service account, role, scope, secret dependency |
| `observability` | Logs, metrics, traces, audit entries |
| `rollback` | Reversal, compensation, or operator-only remediation |
| `test_evidence` | Required validation proof |
| `open_gaps` | Unknowns or untyped fields |

## Workstream 2: Idempotency and Side Effects

Each action and automation must be classified into one of these idempotency states:

- Fully idempotent: repeated requests with the same idempotency key do not introduce additional side effects.
- Conditionally idempotent: repeated requests are safe only within a defined window or under explicit state preconditions.
- Non-idempotent: repeated requests can create duplicate or divergent outcomes.
- Unknown: implementation behavior cannot yet be evidenced.

For each contract, capture:

- Idempotency key origin: client-generated, workflow-run-generated, provider-generated, or unavailable.
- Deduplication scope: per resource, per workflow run, per tenant, or global.
- Deduplication retention window.
- Exact behavior on replay after success, replay after timeout, and replay after partial failure.
- Whether downstream systems independently de-duplicate requests.

Side effect capture must include:

- Records created, updated, or deleted.
- External messages emitted.
- Files uploaded, moved, or overwritten.
- Billing, quota, or rate-limit implications.
- Human-facing notifications or task generation.
- Cascading automations that may trigger downstream.

## Workstream 3: Webhook and Event Behavior

Document all inbound and outbound event pathways.

For inbound webhooks/events, capture:

- Event source and endpoint or queue/topic identifier.
- Authentication and signature verification expectations.
- Accepted event types and version identifiers.
- Ordering guarantees or lack of ordering.
- Delivery semantics: at-most-once, at-least-once, effectively-once, or unknown.
- Replay handling and duplicate suppression behavior.
- Dead-letter, quarantine, or manual review path.
- Schema evolution handling and version compatibility.

For outbound webhooks/events, capture:

- Producer system and emission trigger.
- Payload schema and version.
- Retry/backoff policy.
- Timeout behavior.
- Failure observability.
- Whether delivery failure blocks the parent action or is best-effort.

Build a matrix with:

- Event name.
- Producer.
- Consumer.
- Contract version.
- Delivery semantics.
- Idempotency expectations.
- Failure handling.
- Monitoring owner.

## Workstream 4: Automation Rule Snapshots

For each automation rule active or intended in scope, capture a snapshot containing:

- Rule identifier and name.
- Source-of-truth location.
- Snapshot timestamp.
- Trigger condition.
- Filter conditions.
- Execution steps or action chain.
- Suppression rules.
- Rate limits or batching logic.
- Dependency on business hours, calendar state, or environment.
- Linked action contracts.
- Manual pause/disable method.
- Last-known operator owner.

Snapshot rules from authoritative sources only:

- Workflow definitions.
- Integration platform rule exports.
- Admin console screenshots or exports if no structured export exists.
- Runbooks documenting manual automation steps when automation is partially manual.

If a rule cannot be exported in structured form, record the capture method and confidence level.

## Workstream 5: Rollback and Recovery Expectations

Every action and automation must declare one rollback class:

- Reversible: a defined inverse action exists and is safe to invoke.
- Compensatable: no true rollback exists, but an explicit compensating action restores business state acceptably.
- Forward-fix only: remediation requires subsequent corrective actions.
- Manual-only: recovery depends on operator intervention outside the system.

Capture:

- Rollback trigger criteria.
- Maximum safe rollback window.
- Data that cannot be restored automatically.
- Whether rollback itself is idempotent.
- Approval requirement for rollback.
- Operator playbook reference if manual steps are required.

## Workstream 6: Permissions and Identity Dependencies

For each action, webhook, and automation, record:

- Executing identity type: user, service account, bot, API key, OAuth client, or webhook secret.
- Required roles, scopes, and resource-level permissions.
- Secret or credential dependency location.
- Whether execution runs with caller identity, delegated identity, or shared system identity.
- Tenant or environment isolation expectations.
- Approval boundary for granting or rotating permissions.

Flag any of the following as release blockers:

- Unknown executing identity.
- Shared credentials without ownership.
- Missing least-privilege rationale.
- Automation that requires interactive user credentials.

## Workstream 7: Observability and Audit Requirements

Each contract must specify minimum observability:

- Structured logs with correlation identifiers.
- Action execution status metric: success, failure, timeout, duplicate, skipped.
- Latency metric and retry count.
- Audit log entry for operator-triggered and automation-triggered actions.
- Event receipt and event emission counters.
- Alert threshold for repeated failure or backlog growth.

Capture required dimensions:

- `action_id`
- `automation_rule_id`
- `source_system`
- `target_system`
- `tenant` or equivalent partition key
- `workflow_run_id` or correlation id
- `event_type`
- `result_state`

Where observability is absent, note whether the gap is:

- Documentation-only.
- Instrumentation missing.
- Data emitted but not retained.
- Data retained but not queryable.

## Workstream 8: Typed Gap Register

Maintain a typed gap register with severity and disposition.

Gap categories:

- Missing input schema typing.
- Missing output schema typing.
- Unknown side effects.
- Unknown idempotency behavior.
- Unclear permissions.
- Undocumented webhook/event versioning.
- Missing rollback path.
- Missing observability or audit coverage.
- Unverified automation snapshot.

Severity levels:

- `blocker`: phase cannot be accepted until resolved or formally waived.
- `high`: operational risk is material and requires mitigation before automation expansion.
- `medium`: acceptable temporarily with explicit operator awareness.
- `low`: clarity improvement with limited operational impact.

Each gap entry must include:

- Unique gap id.
- Related contract or rule id.
- Description.
- Evidence source.
- Severity.
- Proposed resolution owner.
- Required follow-up artifact or decision.

## Tests and Validation

Validation must remain documentation- and evidence-based unless existing non-invasive test tooling already exists.

Required validation activities:

1. Inventory completeness review against all known integration surfaces.
2. Contract review for every in-scope action with owner signoff.
3. Replay/idempotency walkthrough for each non-read-only action.
4. Side effect review confirming downstream writes and notifications.
5. Webhook/event matrix review against current configured endpoints or topics.
6. Automation snapshot review against current live/admin configuration.
7. Permissions review with executing identity confirmation.
8. Observability review confirming each required signal exists or is logged as a gap.
9. Rollback tabletop for the highest-risk automations.

Evidence examples:

- Exported workflow definitions.
- Admin console captures.
- Existing API specs or connector docs.
- Run logs with correlation identifiers redacted as needed.
- Permission screenshots or IAM policy excerpts.
- Operator signoff notes.

## Acceptance Criteria

Phase 5 is complete when:

- Every in-scope integration action has a documented contract.
- Every in-scope automation rule has a current snapshot and linked actions.
- Idempotency status is declared for all mutating actions.
- Side effects and rollback class are recorded for all mutating actions.
- Webhook/event delivery semantics are recorded for all event-driven behavior in scope.
- Executing identities and required permissions are documented.
- Observability minimums are either evidenced or recorded as gaps.
- Typed gaps are consolidated into a register with owners and severities.
- Validation checklist is completed with evidence references.

## Constraints

- Do not modify application code, workflow code, or infrastructure definitions in this phase.
- Prefer authoritative exports over narrative description.
- Mark unknowns explicitly; do not infer guarantees without evidence.
- Time-box deep investigation of low-severity gaps if blocker/high items remain unresolved.

## Suggested Execution Order

1. Build the system and automation inventory.
2. Capture action contracts for the highest-risk mutating flows first.
3. Build the webhook/event matrix.
4. Snapshot automation rules from source-of-truth systems.
5. Record permissions and identities.
6. Capture rollback and observability expectations.
7. Consolidate typed gaps.
8. Run validation checklist and collect signoff evidence.

## Validation Checklist

- [ ] In-scope systems list is complete.
- [ ] In-scope actions list is complete.
- [ ] All mutating actions have idempotency classification.
- [ ] All mutating actions have side effects documented.
- [ ] All event ingress paths have delivery semantics documented.
- [ ] All event egress paths have retry/failure behavior documented.
- [ ] All automation rules have snapshot timestamps and source references.
- [ ] All execution identities and permissions are documented.
- [ ] All actions define rollback class and operator expectations.
- [ ] Required observability signals are evidenced or logged as gaps.
- [ ] Gap register includes severity and ownership.
- [ ] Owner review/signoff is captured for the packet.

## Open Questions to Resolve During Capture

- Which integrations are considered production-critical versus advisory?
- Which automations are currently live, disabled, or manually replicated?
- What evidence source is authoritative when implementation and admin configuration disagree?
- Which actions rely on provider-specific idempotency semantics rather than internal guarantees?
- Which rollback paths are operationally safe versus theoretically possible?
