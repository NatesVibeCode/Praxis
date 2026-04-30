# Virtual Lab State - 2026-04-30

## Verdict

Phase 6 adds a pure Virtual Lab state authority, not a storage layer.

Object Truth remains the authority for observed client-system facts. Virtual
Lab state references Object Truth versions as seeds, then records predicted
consequences inside an environment revision through copy-on-write overlays and
event envelopes.

## Authority Model

Current boundary:

- `runtime.object_truth` owns observed snapshots, object versions, evidence,
  lineage, and readiness inputs.
- `runtime.virtual_lab.state` owns deterministic environment revision state,
  seed manifests, object overlays, event envelopes, receipts, and replay
  helpers.
- No Object Truth storage, operation registration, migration, or generated docs
  were changed in this phase.

The state module is deliberately in-memory domain code. It is safe for later
CQRS handlers, repositories, or simulation runtime workers to consume, but it
does not become hidden persistence authority on its own.

## Revision Contract

An environment revision records:

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

Status is explicit: `active` or `closed`. Mutating commands against a closed
revision return a terminal rejected receipt and append no state event.

Revision ids are deterministic when the caller supplies stable seed, config,
policy, timestamp, actor, parent, and metadata inputs.

## Seed Manifest

Seed entries bind Virtual Lab objects to Object Truth without mutating source
truth:

- `object_id`
- `instance_id`
- `object_truth_ref`
- `object_truth_version`
- `projection_version`
- `seed_parameters`
- `base_state`
- `base_state_digest`
- `seed_digest`

Manifest entries are ordered canonically before digesting. Duplicate
`object_id` plus `instance_id` entries fail closed.

`base_state` is included in the seed digest because the same Object Truth ref
and projection version must not silently point at different projected state.

## Object State

Each object state record has:

- `source_ref` pointing back to Object Truth ref, version, and projection
- `base_state` from the seed projection
- `overlay_state` for environment-local changes only
- `effective_state` resolved as base plus overlay
- `base_state_digest`
- `overlay_state_digest`
- `effective_state_digest`
- `state_digest`
- `last_event_id`
- `tombstone`

Only overlays are patched or replaced by mutating commands. `base_state` stays
immutable for the revision, so replay cannot back-write into Object Truth.

`last_event_id` is audit linkage, not content authority. It is exposed on the
record, but excluded from `state_digest` to avoid a circular dependency between
event id and post-state digest.

## Event Envelope

All state events use one canonical envelope:

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

Sequence is monotonic per stream. The module rejects duplicate event ids,
duplicate stream sequence numbers, skipped sequence numbers, and pre-state
digests that do not match the prior stream post-state digest.

## Commands and Receipts

The implemented primitive commands are object-scoped:

- patch overlay
- replace overlay
- tombstone object
- restore object

Every command returns a receipt with:

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

Accepted commands append one event. Closed revisions return `rejected`.
Expected digest mismatches return `conflict`. Repeated command ids in the same
stream return `no_op` with the original event id where available.

## Replay

Replay starts from seed-derived `ObjectStateRecord` values and applies ordered
object events. It validates:

- stream id matches the object state
- every object event has a seeded object state
- sequence is contiguous
- pre-state digest matches current state
- computed post-state digest matches the event envelope

`event_chain_digest()` provides a deterministic digest across ordered events.
It is suitable for later receipts, projection validation, and drift checks.

## Migration Need

No migration was added.

The next durable step is a DB-backed Virtual Lab repository and CQRS operation
registration only when these primitives need to become runtime authority. That
follow-up should define storage tables, operation catalog rows, receipt policy,
and event contracts instead of expanding this pure module into persistence by
accident.
