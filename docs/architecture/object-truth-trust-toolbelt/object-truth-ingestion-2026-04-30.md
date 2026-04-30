# Object Truth Ingestion Primitives - 2026-04-30

## Verdict

Phase 2 now has a deterministic domain layer for ingestion evidence, separate
from Object Truth storage and CQRS operations.

The authority split is deliberate:

- `core.object_truth_ops` owns inline object-version, schema, field, identity,
  and comparison primitives.
- `runtime.object_truth.ingestion` owns client-system ingestion records:
  system snapshots, sample captures, source query/cursor/window evidence,
  privacy-safe payload references, redacted previews, replay fixtures, and
  readiness inputs.
- Storage, command/query handlers, MCP/API surfaces, and migrations remain
  separate work. This packet did not add durable tables or gateway operations.

## What Was Added

`runtime.object_truth.ingestion` builds pure JSON-ready records:

- `build_system_snapshot_record`
- `build_source_query_evidence`
- `build_sample_capture_record`
- `normalize_ingestion_source_metadata`
- `build_raw_payload_reference`
- `build_redacted_preview`
- `build_ingestion_replay_fixture`
- `build_readiness_inputs`

All identifiers and digests are purpose-scoped SHA-256 values over canonical
JSON. Input order does not affect evidence identity.

## Payload Policy

Raw client payloads are reference-first.

By default, `build_raw_payload_reference` records only:

- raw payload reference
- raw payload hash
- normalized payload hash
- privacy classification
- retention policy ref

Inline raw payload content is included only when explicitly approved with both
`privacy_policy_ref` and `retention_policy_ref`. Without those, the primitive
fails closed.

## Redacted Preview Policy

`build_redacted_preview` preserves structure and field presence while hiding
restricted and confidential values. It redacts:

- tokens, secrets, credentials, auth/session values
- personal identifiers such as email, phone, address, names, and IDs
- free-text fields such as notes, comments, messages, descriptions, and body
  content
- policy-marked fields

Policy overrides support:

- `field_classifications`
- `restricted_fields`
- `confidential_fields`
- `internal_fields`
- `public_fields`

Redacted values carry classification, value kind, and digest, not content.

## Readiness Inputs

`build_readiness_inputs` does not replace `object_truth_readiness`. It builds
the fail-closed caller packet for that existing query:

- operation name: `object_truth_readiness`
- tool ref: `praxis_object_truth_readiness`
- payload: `client_payload_mode`, `planned_fanout`, `include_counts`, optional
  `privacy_policy_ref`
- fail-closed states: `blocked`, `unknown`, `revoked`
- ingestion requirements: system refs, object refs, connector refs, source refs,
  sample strategies, and sample count

Downstream workers should query readiness through the gateway before persisting
or promoting ingestion evidence.

## Migration Needs

No migration was added in this phase.

Durable ingestion still needs separately owned storage/CQRS work for system
snapshots, sample captures, raw payload reference rows, replay fixture rows, and
typed gap events if those records must become queryable authority instead of
domain artifacts.
