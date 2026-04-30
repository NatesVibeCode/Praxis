# Phase 1 Plan: Client System Discovery and Connector Census

## Purpose

This packet defines a bounded build for Phase 1 of the client operating model: discovering client systems, enumerating integrations, and producing a trustworthy census of connectors, credentials, data objects, limits, event surfaces, and automation-bearing tools.

The output of this phase is not automation deployment. It is a verified discovery substrate that later phases can use to decide what can be synchronized, automated, monitored, or governed.

## Bounded Scope

### In Scope

- Inventory every client-declared system that may participate in workflows, reporting, orchestration, or data exchange.
- Catalog integrations between systems, including native integrations, iPaaS connectors, custom API clients, ETL jobs, and file-based exchanges.
- Assess connector capabilities by object, operation, directionality, and known constraints.
- Record credential health and ownership status without storing raw secrets.
- Build object catalogs for systems that expose records, entities, files, events, or messages.
- Capture API limits, quota models, pagination behavior, concurrency constraints, and backoff requirements.
- Enumerate webhook, polling, event bus, and change-data-capture surfaces.
- Identify tools with automation-bearing behavior such as schedulers, workflow engines, CRMs, support tools, marketing tools, ERP platforms, and internal admin panels.
- Produce typed gaps where discovery is incomplete, blocked, or ambiguous.
- Validate the discovery packet using fixtures and repeatable checks.

### Out of Scope

- Writing production syncs, ETL jobs, or automations.
- Rotating credentials or changing client-side permissions.
- Mutating client systems except for safe read-only metadata probes.
- Designing the full canonical domain model for downstream phases.
- Building end-user UI beyond operator-facing packet artifacts and validation outputs.

## Objectives

1. Produce a system-of-record inventory for all candidate systems and tools.
2. Establish whether each system is reachable, authenticated, and operationally usable.
3. Describe what each connector can read, write, search, subscribe to, or trigger.
4. Expose operational constraints before downstream implementation starts.
5. Surface unknowns as explicit typed gaps rather than implicit omissions.

## Phase Deliverables

- `systems` census with ownership, environment, criticality, and discovery status.
- `integrations` registry covering native, managed, custom, and manual exchanges.
- `connectors` capability matrix with supported objects and operations.
- `credential_health` report with status, owner, last validation time, and remediation path.
- `object_catalog` for high-value entities and their discoverability characteristics.
- `api_surface` report covering auth method, limits, pagination, filtering, bulk support, and rate behavior.
- `event_surface` report for webhooks, polling endpoints, streams, and audit/event feeds.
- `automation_tools` registry for systems that can schedule, trigger, transform, or route work.
- `typed_gaps` register with severity, blocker status, evidence, and next action.
- Validation evidence from fixtures and command outputs.

## Discovery Principles

- Read-only first: Phase 1 should prefer metadata endpoints, schema endpoints, describe endpoints, admin panels, and existing docs.
- Evidence over inference: every claim should be backed by a probe result, documentation reference, or operator confirmation.
- Unknowns are first-class: missing capability data must become a typed gap.
- Separate observed from declared state: client claims and probe results should both be stored.
- Minimize blast radius: no writes, no destructive calls, no permission expansion during this phase.

## Authority Model

### Decision Rights

- Praxis delivery lead owns phase execution, gap triage, and acceptance criteria.
- Client technical owner approves system list, access path, and environment scope.
- Client security owner approves credential handling patterns and probe boundaries.
- Client business system owners confirm system purpose, critical objects, and operational sensitivity.

### Source Hierarchy

Use this precedence order when conflicting information exists:

1. Live probe result from read-only authenticated access.
2. Vendor metadata or official API description.
3. Client-admin screenshot or exported configuration.
4. Client interview or written assertion.
5. Historical packet content from prior engagements.

### Credential Handling

- Store credential references, not secrets.
- Persist only opaque identifiers such as vault path, secret label, auth profile name, or connection id.
- Credential health status may include `valid`, `expired`, `revoked`, `missing_scope`, `unknown`, or `probe_failed`.
- Any evidence artifact must redact tokens, cookies, API keys, and endpoint-specific secret material.

## Proposed Data Shape

The phase can be implemented with relational tables, documents, or typed files. If persisted relationally, use the following logical tables.

### Core Tables

#### `systems`

- `system_id`
- `client_id`
- `name`
- `category`
- `vendor`
- `deployment_model` (`saas`, `self_hosted`, `desktop`, `spreadsheet`, `file_drop`, `internal_tool`)
- `environment` (`prod`, `sandbox`, `staging`, `mixed`, `unknown`)
- `business_owner`
- `technical_owner`
- `criticality`
- `declared_purpose`
- `discovery_status`
- `last_verified_at`

#### `integrations`

- `integration_id`
- `source_system_id`
- `target_system_id`
- `integration_type` (`native`, `custom_api`, `ipaas`, `etl`, `webhook`, `file_transfer`, `manual`)
- `transport`
- `directionality` (`uni`, `bi`, `unknown`)
- `trigger_mode` (`event`, `poll`, `schedule`, `manual`, `batch`)
- `integration_owner`
- `observed_status`
- `evidence_ref`

#### `connectors`

- `connector_id`
- `system_id`
- `connector_name`
- `connector_kind` (`official`, `partner`, `internal`, `sdk_client`, `rpa`, `none`)
- `auth_method`
- `supports_read`
- `supports_write`
- `supports_search`
- `supports_bulk`
- `supports_webhooks`
- `supports_incremental`
- `status`
- `last_validated_at`

#### `connector_objects`

- `connector_object_id`
- `connector_id`
- `object_name`
- `object_type`
- `read_capability`
- `write_capability`
- `search_capability`
- `subscribe_capability`
- `key_fields`
- `cursor_field`
- `delete_semantics`
- `notes`

#### `credential_health`

- `credential_health_id`
- `system_id`
- `connector_id`
- `credential_ref`
- `credential_owner`
- `scope_summary`
- `health_status`
- `validated_by`
- `validated_at`
- `failure_reason`
- `remediation_action`

#### `api_surfaces`

- `api_surface_id`
- `system_id`
- `base_url`
- `api_style` (`rest`, `graphql`, `soap`, `rpc`, `sql`, `file`, `none`)
- `versioning_model`
- `pagination_model`
- `rate_limit_model`
- `burst_limit`
- `daily_quota`
- `concurrency_limit`
- `timeout_behavior`
- `bulk_endpoint_available`
- `filtering_notes`
- `evidence_ref`

#### `event_surfaces`

- `event_surface_id`
- `system_id`
- `surface_type` (`webhook`, `polling`, `stream`, `audit_log`, `cdc`, `queue`)
- `event_name`
- `delivery_semantics`
- `replay_support`
- `signature_scheme`
- `subscription_scope`
- `latency_class`
- `setup_complexity`
- `evidence_ref`

#### `automation_tools`

- `automation_tool_id`
- `system_id`
- `tool_name`
- `tool_class` (`workflow_engine`, `scheduler`, `crm_automation`, `support_automation`, `marketing_automation`, `rpa`, `script_runner`, `internal_admin`)
- `execution_mode`
- `supports_triggers`
- `supports_actions`
- `supports_branching`
- `supports_human_approval`
- `supports_observability`
- `notes`

#### `typed_gaps`

- `gap_id`
- `entity_type`
- `entity_id`
- `gap_type`
- `severity`
- `is_blocker`
- `description`
- `expected_evidence`
- `current_evidence`
- `next_action`
- `owner`
- `opened_at`
- `resolved_at`

### Projections

Use read-optimized projections for operator review:

- `system_discovery_dashboard`
  - one row per system with reachability, auth health, connector count, event surface count, and blocker count
- `connector_capability_matrix`
  - one row per connector-object pair with read/write/search/subscribe flags
- `credential_risk_report`
  - one row per credential reference with health, owner, scope risk, and remediation status
- `automation_readiness_view`
  - one row per system summarizing triggerability, actionability, and operational constraints
- `phase_1_exit_view`
  - counts of verified systems, blocked systems, unresolved high-severity gaps, and coverage percentage

## CQRS Model

Phase 1 benefits from explicit command/query separation because evidence collection, normalization, and operator review have different shapes.

### Commands

- `RegisterSystem`
  - create or upsert a system from client intake
- `RecordSystemProbe`
  - attach probe result, reachability, auth outcome, and evidence
- `RegisterIntegration`
  - create or update an observed integration edge
- `RegisterConnector`
  - persist connector metadata for a system
- `RecordConnectorCapability`
  - upsert object-level operation support
- `RecordCredentialHealth`
  - persist redacted auth validation result
- `RecordApiSurface`
  - store API characteristics and limits
- `RecordEventSurface`
  - store webhook/polling/stream metadata
- `RegisterAutomationTool`
  - mark systems or sub-tools that can execute automation logic
- `OpenTypedGap`
  - create explicit unknown/blocker records
- `ResolveTypedGap`
  - close a gap with evidence and timestamp
- `FinalizePhase1Assessment`
  - mark packet complete when exit criteria are met

### Queries

- `GetSystemDiscoveryStatus(system_id)`
- `ListSystemsByDiscoveryState(client_id)`
- `ListConnectorCapabilities(system_id)`
- `ListCredentialFailures(client_id)`
- `ListHighSeverityGaps(client_id)`
- `GetApiLimits(system_id)`
- `GetEventSurfaces(system_id)`
- `ListAutomationBearingTools(client_id)`
- `GetPhase1ExitReadiness(client_id)`

## Discovery Workflow

### Step 1: Intake and System Enumeration

- Gather client-provided system list, admin contacts, and environment references.
- Normalize aliases so the same system is not registered multiple times under different names.
- Register candidate systems even when access is missing.

### Step 2: Connectivity and Credential Validation

- Run safe authenticated probes against each declared system or connector.
- Validate that credentials are present, correctly scoped, and non-expired.
- Record failures as typed gaps rather than retrying indefinitely.

### Step 3: Connector Census

- Enumerate available connectors per system: native, iPaaS, SDK-based, internal wrappers, and file-mediated connectors.
- Record directional support and object-level operations.
- Distinguish vendor-advertised capability from client-enabled capability.

### Step 4: Object Cataloging

- Discover high-value objects such as accounts, contacts, deals, tickets, invoices, products, users, files, tasks, events, messages, and custom objects.
- Record key identifiers, mutable fields, cursor fields, and change-tracking support.
- Mark objects as `verified`, `declared_only`, or `inferred`.

### Step 5: API and Event Surface Assessment

- Capture auth type, pagination, rate limits, quotas, timeouts, and bulk support.
- Inventory webhook topics, replay semantics, signatures, and polling fallback options.
- Note any surfaces requiring elevated plans, add-ons, or admin enablement.

### Step 6: Automation Tool Sweep

- Identify workflow engines, schedulers, rule builders, macros, campaign automations, RPA tools, and internal scripts.
- Record whether they can trigger external actions, invoke webhooks, or mutate system records.

### Step 7: Gap Triage and Exit Review

- Convert missing access, missing docs, unsupported operations, and unclear ownership into typed gaps.
- Review blockers against downstream phase requirements.
- Finalize once exit criteria are met.

## Typed Gaps

Use a constrained taxonomy so unresolved discovery work remains queryable.

### Gap Types

- `missing_access`
- `invalid_credential`
- `insufficient_scope`
- `unknown_object_model`
- `unknown_rate_limit`
- `unknown_event_surface`
- `connector_unavailable`
- `connector_capability_unverified`
- `owner_unassigned`
- `doc_conflict`
- `probe_blocked`
- `environment_ambiguity`

### Severity

- `critical`: blocks Phase 2 design or any trustworthy automation plan
- `high`: materially weakens architecture choices or estimate accuracy
- `medium`: reduces convenience or completeness but not foundational correctness
- `low`: documentation or polish gap with limited delivery impact

## Fixture Strategy

Fixtures should support repeatable validation without requiring live client systems during every check.

### Fixture Types

- `system_intake_fixture`
  - sample client-declared systems with aliases, owners, and environments
- `probe_result_fixture`
  - sanitized auth success/failure responses and metadata probes
- `connector_catalog_fixture`
  - representative connector descriptors and capability payloads
- `object_schema_fixture`
  - example object metadata including custom fields and cursor fields
- `api_limit_fixture`
  - vendor limit responses, headers, and error payloads
- `event_surface_fixture`
  - sample webhook topic lists, audit feeds, or polling descriptors
- `gap_fixture`
  - unresolved and resolved gap examples for report testing

### Fixture Rules

- All fixtures must be sanitized and portable.
- No live secrets, tenant ids, or personal data.
- Prefer one happy-path and one constrained-path fixture per system class.
- Include explicit edge cases: expired token, no webhook support, custom object without cursor, low quota, and disabled bulk API.

## Tests

Since this packet avoids code edits, the test plan should define what any implementation must satisfy.

### Unit-Level Expectations

- normalize system aliases into stable ids
- reject credential records that include raw secret material
- classify connector capability flags consistently from heterogeneous metadata
- map rate limit headers into normalized fields
- emit typed gaps when required evidence is missing

### Integration-Level Expectations

- ingest declared systems and produce a stable `systems` census
- combine probe results with connector metadata into a coherent capability matrix
- produce projections with correct blocker counts
- preserve both declared and observed states without overwriting one with the other

### Regression Expectations

- adding a new system category should not break existing projections
- unsupported event surfaces should degrade to typed gaps, not packet failure
- multiple connectors for the same system should remain independently queryable

## Validation Commands

These commands assume packet artifacts are stored as Markdown, YAML, or JSON in the repo. Adjust file globs to match the eventual implementation.

```bash
test -f artifacts/workflow/client_operating_model/packets/phase_01_client_system_discovery/PLAN.md
```

```bash
rg -n "^## " artifacts/workflow/client_operating_model/packets/phase_01_client_system_discovery/PLAN.md
```

```bash
rg -n "typed_gaps|credential_health|connector_capability_matrix|FinalizePhase1Assessment" artifacts/workflow/client_operating_model/packets/phase_01_client_system_discovery/PLAN.md
```

If structured companion artifacts are later added, validate them with repository-standard tools such as:

```bash
yq eval '.' artifacts/**/*.yml
```

```bash
jq '.' artifacts/**/*.json
```

## Failure Containment

### Operational Containment

- A failed probe for one system must not stop discovery for others.
- Credential validation errors should be recorded and isolated per credential reference.
- Unclear rate limits should become typed gaps rather than guessed defaults.
- Systems with missing event surfaces should fall back to polling classification if supported; otherwise record an explicit gap.

### Data Containment

- Keep raw probe evidence separate from normalized projections.
- Redact sensitive headers and payload fields before persistence.
- Preserve source timestamps so stale evidence can be identified.

### Decision Containment

- Do not mark a system automation-ready based solely on vendor documentation.
- Do not infer write capability from read capability.
- Do not infer production suitability from sandbox-only validation.

## Exit Criteria

Phase 1 is complete when all of the following are true:

1. Every client-declared candidate system has a `systems` record.
2. Every system has either a verified probe result or an explicit blocker gap.
3. Every discovered connector has a capability record at least at the system level.
4. High-value objects are cataloged or blocked by typed gaps.
5. API limits and event surfaces are captured or explicitly unknown.
6. Automation-bearing tools are identified and classified.
7. No unresolved `critical` gap remains unowned.

## Assumptions

- The workspace did not contain an existing repository structure or packet template when this document was written.
- This plan is therefore self-contained and intended to be adapted into the eventual implementation format used by the broader project.
