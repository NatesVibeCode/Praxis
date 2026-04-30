# Phase 6 Build Packet: Virtual Lab Authority and Event-Sourced State Model

## Objective

Define a bounded implementation plan for a virtual lab state system that:

- establishes revisioned virtual environments as the authority for simulated execution;
- derives initial state seeds from Object Truth;
- applies copy-on-write object state overlays instead of mutating source truth;
- records all state transitions as causally ordered events;
- produces digests and receipts that make state transitions auditable;
- exposes clear CQRS-style command and query operations;
- ships with deterministic tests and validation criteria.

This packet is planning-only. It does not authorize unrelated architecture changes, production hardening outside the scope below, or edits to code in this phase artifact.

## Scope

### In Scope

- Virtual environment revision model
- State seed derivation from Object Truth
- Copy-on-write object state representation
- Event envelope and event taxonomy
- Causality and ordering rules
- State digests and receipt generation
- CQRS command/query API contracts
- Test matrix and acceptance validation

### Out of Scope

- UI or operator workflow redesign
- Physical infrastructure orchestration
- Multi-region replication design
- Long-term archival policy beyond receipt retention hooks
- General-purpose workflow engine replacement
- Authorization model redesign except where needed to bind actor identity into receipts

## Bounded Deliverables

1. A virtual environment revision specification.
2. A deterministic seed-generation specification from Object Truth snapshots.
3. A copy-on-write state model for objects instantiated inside a virtual lab.
4. A canonical event envelope with required fields and event kinds.
5. A causality model covering ordering, parentage, idempotency, and replay.
6. A digest model for environment state, object state, and event chains.
7. Receipt and event emission contracts for all state-changing operations.
8. A CQRS command/query contract with explicit read models.
9. A test plan covering unit, property, replay, fault, and validation cases.
10. Exit criteria for phase acceptance.

## Core Concepts

### Object Truth

`Object Truth` is the immutable or separately governed source-of-record for domain objects before they are projected into a virtual lab. Phase 6 does not redefine Object Truth. It consumes versioned snapshots or version-addressable object records as seed inputs.

### Virtual Environment Revision

A `virtual environment revision` is an immutable revision descriptor for a lab execution context. It identifies:

- environment identity;
- parent revision, if forked;
- seed set and seed digest;
- configuration inputs;
- applicable policies and simulation engine versions;
- creation actor and timestamp.

Every mutating lab run targets exactly one active environment revision. New revisions are created by explicit fork, reset, re-seed, or configuration change operations.

### State Seed

A `state seed` is the deterministic projection of Object Truth into the initial environment state. Seeds are generated from:

- object identifiers;
- source object version or snapshot reference;
- projection rules version;
- optional environment-local initialization parameters.

The same seed inputs must always produce the same seed digest and initial object state.

### Copy-on-Write Object State

Virtual lab objects must not mutate Object Truth directly. Each instantiated object is represented as:

- `source_ref`: pointer to Object Truth version or snapshot;
- `base_state`: canonical seeded projection derived from Object Truth;
- `overlay_state`: environment-local changes only;
- `effective_state`: resolved view of `base_state` plus `overlay_state`.

Only `overlay_state` is mutated by virtual lab commands. `base_state` changes only when a new environment revision or re-seed event is created.

## Required Data Model

### Environment Revision Record

Required fields:

- `environment_id`
- `revision_id`
- `parent_revision_id`
- `revision_reason`
- `seed_manifest`
- `seed_digest`
- `config_digest`
- `policy_digest`
- `created_at`
- `created_by`
- `status`

Rules:

- `revision_id` is immutable and unique within `environment_id`.
- `parent_revision_id` is required for forks and null for root revisions.
- `seed_manifest` is content-addressed or digestable.
- A closed revision cannot accept new write commands.

### Seed Manifest

Required fields per seed entry:

- `object_id`
- `object_truth_ref`
- `object_truth_version`
- `projection_version`
- `seed_parameters`
- `seed_digest`

Rules:

- The seed manifest is ordered canonically before digesting.
- Missing Object Truth references fail revision creation.
- Projection code or rule version must be explicit.

### Object State Record

Required fields:

- `environment_id`
- `revision_id`
- `object_id`
- `instance_id`
- `source_ref`
- `base_state_digest`
- `overlay_state_digest`
- `effective_state_digest`
- `last_event_id`
- `tombstone`

Rules:

- `instance_id` distinguishes multiple environment-local instances when allowed.
- `effective_state_digest` must be recomputable from base plus overlay.
- Tombstoned objects remain queryable historically.

## Event-Sourced State Model

### Event Envelope

Every state event must contain:

- `event_id`
- `environment_id`
- `revision_id`
- `stream_id`
- `event_type`
- `event_version`
- `occurred_at`
- `recorded_at`
- `actor_id`
- `actor_type`
- `command_id`
- `causation_id`
- `correlation_id`
- `parent_event_ids`
- `sequence_number`
- `pre_state_digest`
- `post_state_digest`
- `payload`
- `payload_digest`
- `schema_digest`

Rules:

- `occurred_at` captures domain occurrence if distinct from persistence time.
- `recorded_at` is assigned by the event store.
- `sequence_number` is monotonic within a stream.
- `pre_state_digest` and `post_state_digest` are required for all mutating events.

### Minimum Event Taxonomy

Environment lifecycle:

- `environment.created`
- `environment.forked`
- `environment.reseeded`
- `environment.closed`

Object lifecycle:

- `object.seeded`
- `object.instantiated`
- `object.patched`
- `object.replaced`
- `object.tombstoned`
- `object.restored`

Execution and system:

- `command.rejected`
- `receipt.issued`
- `digest.snapshotted`
- `projection.rebuilt`

Rules:

- Event names are stable, versioned contracts.
- Additive payload evolution is preferred over renaming event kinds.

## Causality and Ordering

### Ordering Model

- Order is guaranteed per stream, not globally.
- `stream_id` should be `environment_id`, `object_id`, or another explicit aggregate stream key.
- Cross-stream dependencies are represented through `causation_id`, `correlation_id`, and `parent_event_ids`.

### Causality Rules

- A command may emit zero or more domain events and exactly one terminal receipt.
- Every emitted event must reference the originating `command_id`.
- If a command depends on prior state, the command handler must validate expected digests or expected sequence numbers before append.
- Concurrent writes that invalidate expectations must fail with a conflict receipt rather than silently merge unless an explicit merge policy exists.

### Idempotency

- `command_id` must be idempotent within the target scope.
- Replayed or retried commands with identical inputs return the original receipt where possible.
- Event append must reject duplicate `(stream_id, sequence_number)` and duplicate `event_id`.

### Replay

- Replaying a stream from seed plus events must reproduce the same effective state digest.
- Non-deterministic handlers are not allowed in projection of authoritative state.

## Digests

### Digest Types

- `seed_digest`: digest of canonical seed manifest or seed entry.
- `base_state_digest`: digest of canonical seeded object state.
- `overlay_state_digest`: digest of canonical environment-local delta.
- `effective_state_digest`: digest of resolved effective object state.
- `revision_digest`: digest of revision metadata plus seed/config/policy digests.
- `event_chain_digest`: rolling digest across ordered events in a stream.

### Digest Requirements

- Canonical serialization must be defined and stable.
- Digest algorithm must be explicit and versioned.
- Digest mismatches must fail validation and trigger receipt errors, not warnings.
- Read models may cache digests but authoritative validation is derived from canonical state and events.

## Receipts and Auditability

### Receipt Contract

Every command must return a receipt containing:

- `receipt_id`
- `command_id`
- `environment_id`
- `revision_id`
- `status`
- `resulting_event_ids`
- `precondition_digest`
- `result_digest`
- `errors`
- `warnings`
- `issued_at`
- `issued_by`

Statuses:

- `accepted`
- `rejected`
- `conflict`
- `no_op`

### Audit Guarantees

- A user must be able to trace from receipt to command, to emitted events, to resulting object/environment digests.
- A validator must be able to recompute state and confirm the receipt’s `result_digest`.
- Rejections must still produce receipts, even when no domain event is appended.

## CQRS Contract

### Commands

Minimum command operations:

1. `CreateEnvironmentRevision`
2. `ForkEnvironmentRevision`
3. `ReseedEnvironmentRevision`
4. `InstantiateObject`
5. `PatchObjectOverlay`
6. `ReplaceObjectOverlay`
7. `TombstoneObject`
8. `RestoreObject`
9. `CloseEnvironmentRevision`
10. `ValidateEnvironmentRevision`

Command requirements:

- Each command declares target aggregate scope.
- Each command includes expected digest or expected version preconditions where mutation depends on prior state.
- Each command returns one receipt.

### Queries

Minimum query operations:

1. `GetEnvironmentRevision`
2. `ListEnvironmentRevisions`
3. `GetSeedManifest`
4. `GetObjectState`
5. `ListObjectStates`
6. `GetObjectEventStream`
7. `GetEnvironmentEventStream`
8. `GetReceipt`
9. `ValidateDigest`
10. `ReplayEnvironmentState`

Read model requirements:

- Queries return current read models plus authoritative digest references.
- Historical queries must support revision-scoped and time-scoped reads where event history exists.
- Read-side projections may be eventually consistent, but validation and replay queries must expose authoritative consistency state.

## Validation Plan

### Invariants

- No environment mutation without a valid target revision.
- No object effective state without a resolvable base state.
- No mutation event without pre and post digests.
- No accepted receipt without a complete event linkage.
- Replay of the same seed and event stream yields the same effective digests.
- Object Truth references remain immutable within a single seeded base state.

### Validation Operations

- seed manifest validation
- digest recomputation
- event chain continuity check
- orphan receipt detection
- orphan event detection
- duplicate command detection
- stream sequence gap detection
- replay equivalence check
- copy-on-write isolation check

### Failure Handling

- Validation failures are hard failures for authoritative operations.
- Read-model drift may be repairable through projection rebuild, but authoritative event log drift is not auto-healed.
- Any digest mismatch between stored and recomputed authoritative state must be surfaced as an integrity incident.

## Test Plan

### Unit Tests

- Seed generation is deterministic for identical Object Truth inputs.
- Canonical serialization produces stable digests.
- Overlay application preserves unchanged base fields.
- Tombstone and restore semantics preserve history correctly.
- Receipt generation is complete for accepted, rejected, and conflict outcomes.

### Property Tests

- Replay is deterministic across randomized valid event sequences.
- Copy-on-write overlays never mutate base state.
- Equivalent command retries under idempotency keys produce the same receipt semantics.

### Integration Tests

- Environment creation to object seeding to mutation to validation succeeds end-to-end.
- Forked revision inherits seed/base state and diverges only via overlay/event streams.
- Reseed creates a new authoritative base without mutating prior revision history.
- Conflicting concurrent mutations produce deterministic conflict receipts.

### Fault and Recovery Tests

- Missing seed entry is rejected before revision activation.
- Event sequence gaps are detected by validators.
- Corrupted digest payloads fail replay validation.
- Projection rebuild restores read model correctness from authoritative events.

### Regression Tests

- Historical receipts remain resolvable after schema version increments.
- Event version upcasters preserve digest semantics where required by policy.

## Acceptance Criteria

Phase 6 is complete when:

1. A written spec exists for environment revisions, seed manifests, object state overlays, events, digests, and receipts.
2. All mutating operations are modeled as commands returning receipts and appending authoritative events where appropriate.
3. Read and write concerns are separated through a documented CQRS contract.
4. Replay from seed plus event streams is specified as deterministic and validated by tests.
5. Copy-on-write isolation from Object Truth is explicit and test-covered.
6. Conflict, rejection, and integrity-failure behaviors are defined, not implicit.
7. Validation operations and pass/fail criteria are documented for implementation.

## Implementation Notes for the Next Phase

- Prefer append-only event storage for authoritative state.
- Keep canonical serialization and digest versioning centralized; digest drift from ad hoc serializers will invalidate the model.
- Treat Object Truth adapters and virtual lab projections as separate responsibilities.
- Resist implicit merge behavior. Require explicit conflict policies if concurrent object overlays must co-exist.
- Snapshotting is allowed for performance, but snapshots do not replace replayable authoritative events.

## Open Decisions

The following must be resolved before implementation starts:

- canonical serialization format for digest computation;
- digest algorithm and versioning policy;
- whether stream scope is per environment, per object, or hybrid by event kind;
- retention period and storage class for receipts and projections;
- policy for schema evolution and event upcasting;
- whether re-seed is modeled as new revision only, or also allowed within a revision boundary.

## Exit Artifact

This file is the bounded build packet for Phase 6. Any implementation work derived from it should preserve the scope boundaries above and trace changes back to:

- revision authority;
- Object Truth seeding;
- copy-on-write overlays;
- event sourcing;
- causality and digest validation;
- CQRS command/query separation.
