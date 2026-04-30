# Client System Discovery Substrate

Date: 2026-04-30

## Verdict

Phase 1 now has a small durable Python substrate for client-system discovery.
It is intentionally an evidence model, not an automation runner. Object Truth
owns observed client-system facts; later Virtual Lab work may consume those
facts to model consequences.

## Authority Model

The substrate keeps one job: represent what is known, what was observed, and
what is still missing.

- System census records carry tenant/workspace/system identity, ownership,
  deployment and environment metadata, connector evidence, and integration
  edges.
- Connector census records carry capabilities, object/API/event surface
  evidence, credential-health references, and automation-bearing
  classification.
- Credential records store opaque references and health status only. Raw
  tokens, keys, cookies, and bearer material fail validation before a census
  packet is built.
- Typed gaps expose missing access, unclear environments, unverified
  capabilities, unknown rate limits, unknown event surfaces, and bad
  credentials as queryable data.

## Reused Local Surfaces

- `runtime.integration_manifest` remains the source for TOML-backed connector
  capability declarations.
- `runtime.integrations.integration_registry` remains the registry shape for
  live integration rows and normalized capability payloads.
- `runtime.client_system_discovery.models` now converts manifest or registry
  evidence into the same deterministic census shape.

## Delivered Contract

Runtime contract:

- `connector_record_from_payload(...)`
- `connector_record_from_manifest(...)`
- `connector_record_from_registry_row(...)`
- `system_record_from_payload(...)`
- `summarize_system_census(...)`
- `validate_system_census(...)`
- `assert_no_secret_material(...)`

Validation report shape:

- `ok`
- deterministic `summary`
- typed `gaps`
- blocker and critical-gap counts
- stable gap ids derived from gap payloads

## Trade-Offs

The current database migration in this checkout stores the original compact
census shape. The richer Phase 1 fields are represented in the Python domain
packet and evidence hash, but they are not all first-class database columns
yet. That is the correct containment for this worker: no migrations were
applied or expanded.

## Migration Need

If Phase 1 review wants every new field queryable by SQL, add a follow-up
migration for:

- system category, vendor, deployment model, environment, owners, criticality,
  purpose, discovery status, and last verification time
- integration edge rows
- object/API/event surface detail columns beyond the compact evidence JSON
- typed gap projections or views for Phase 1 exit readiness

Until then, the substrate is deterministic and test-covered, but the durable
query surface remains narrower than the in-memory contract.
