# Portable Cartridge Contract

Date: 2026-04-30

## Verdict

Portable cartridges now have a pure Python contract authority under
`runtime.cartridge`. The authority is intentionally narrow: it validates and
normalizes a manifest, exposes deterministic dependency ordering, and emits
machine-readable findings and hooks. It does not package artifacts, resolve
external systems, mount runtimes, execute tasks, or persist deployment state.

## Authority Model

The manifest is the portable unit's single contract document. Runtime hosts,
deployment controllers, verifiers, and auditors should be able to determine
whether the cartridge is admissible from:

- the canonical manifest
- declared Object Truth dependencies
- declared bindings
- verifier suite evidence
- digest validation hooks
- drift hook references

No runtime may rely on hidden local environment state to fill missing
contract fields.

## Canonical Format

The implemented primitive accepts an in-memory JSON-compatible mapping and
normalizes it to canonical JSON for hashing. Canonicalization uses
`runtime.crypto_authority.canonical_json`, sorted object keys, and compact
separators.

Current implementation choice:

- Canonical hash: `sha256:<hex>` over the normalized manifest contract
- Supported schema version: `manifest_version = "1.0"`
- YAML parsing: not implemented in Phase 9
- Package envelope hashing: represented by `audit.content_digest`, not
  implemented by this primitive

## Required Contract Areas

The manifest must declare:

- cartridge identity: `cartridge_id`, `cartridge_version`, `build_id`
- UTC creation time
- producer name and version
- runtime compatibility assumptions
- symbolic lifecycle entrypoints: `load`, `execute`, `verify`, `retire`
- Object Truth dependencies grouped as `primary`, `optional`, and `derived`
- immutable assets with digest, size, role, and media type
- late-bound integration bindings
- runtime assumptions for environment, network, filesystem, and secrets
- compute floor and expected duration
- verifier suite version and required checks
- audit digest, dependency digests, and drift hooks
- signatures, even when empty for non-production modes

## Object Truth Dependencies

Dependency classes are ordered and validated separately:

1. `primary`: correctness-bearing truth. These must be required and must
   fail closed or use an explicit pinned fallback.
2. `optional`: behavior-improving truth. These may warn and continue unless
   a policy marks them required.
3. `derived`: recomputable truth. These must identify parent dependencies by
   digest, not name only.

The deterministic resolution order is:

1. manifest schema
2. cartridge integrity
3. primary truth
4. required bindings
5. optional truth
6. derived truth
7. verifier suite
8. runtime mount

## Bindings

Supported binding kinds:

- `object_reference`
- `secret_reference`
- `service_endpoint`
- `queue_topic`
- `model_handle`
- `policy_handle`

Bindings are declared by `binding_id` and resolved late. Runtime-supplied
binding values are rejected if the binding was not declared in the manifest.
Required bindings must be present before their resolution phase.

## Verifier Suite

The verifier suite is mandatory and versioned independently from cartridge
implementation. Required categories are:

- `schema`
- `integrity`
- `compatibility`
- `dependency`
- `binding`
- `runtime_policy`
- `compute`
- `drift`
- `smoke`

Each category has a fixed reason-code family:

| Category | Reason family |
| --- | --- |
| `schema` | `SCHEMA_` |
| `integrity` | `INTEGRITY_` |
| `compatibility` | `COMPAT_` |
| `dependency` | `DEPENDENCY_` |
| `binding` | `BINDING_` |
| `runtime_policy` | `RUNTIME_` |
| `compute` | `COMPUTE_` |
| `drift` | `DRIFT_` |
| `smoke` | `SMOKE_` |

## Drift Hooks

Required hook points:

- `build_time`
- `load_time`
- `execute_time`
- `post_run`
- `periodic_runtime`

Supported drift dimensions:

- `manifest`
- `dependency`
- `binding`
- `policy`
- `compute`
- `runtime_capability`
- `output_lineage`

Each hook must point at an evidence contract so drift events can be audited
without inferring meaning from logs.

## Deployment Modes

The primitive defines mode checks for:

- `local_verification`
- `staged_deployment`
- `production_deployment`
- `offline_air_gapped`

Production and offline modes require signatures. Staged, production, and
offline modes require drift hooks and authoritative primary truth behavior.
Offline mode rejects required live service-endpoint bindings.

## Validation Surface

Validation emits `ValidationFinding` records:

- `severity`
- `category`
- `reason_code`
- `message`
- `path`
- `details`

This is the contract future loaders and controller gates should consume. A
runtime should not scrape exception text or docs to decide admissibility.

## Non-Goals

Phase 9 does not implement:

- tar, zip, OCI, or other package envelope formats
- signature chain verification
- runtime mount behavior
- Object Truth lookup or registry calls
- deployment controller orchestration
- migrations or database persistence
- UI surfaces

Those belong in later build packets.
