# Phase 9: Portable Cartridge and Deployment Contract

## Intent

Define a bounded, portable artifact format for packaging a deployable workflow unit as a "cartridge" plus a strict deployment contract that any compatible runtime can validate, mount, execute, audit, and retire without relying on hidden environment state.

This phase produces the specification and verification surface only. It does not require implementation of orchestration code, compute backends, or user interface changes.

## Scope

In scope:

- Cartridge manifest shape and required fields
- Object truth dependencies and resolution order
- Task contracts for build, load, execute, verify, and retire operations
- Integration bindings between cartridge, runtime, object store, verifier, and deployment controller
- Verifier suite structure and pass/fail rules
- Deployment modes and their guarantees
- Runtime assumptions and compatibility boundaries
- Compute expectations and sizing guidance
- Drift audit hooks and traceability requirements
- Test matrix and validation gates

Out of scope:

- Writing runtime code
- Vendor-specific infrastructure templates
- Interactive deployment UI
- Cost optimization beyond baseline compute expectations
- Workflow business logic unrelated to cartridge portability

## Bounded Deliverables

This phase is complete when the following artifacts are defined and accepted:

1. A normative cartridge manifest contract
2. A dependency model for truth-bearing objects the cartridge references
3. Task contracts covering producer and runtime responsibilities
4. A stable integration binding table for external systems
5. A verifier suite with deterministic exit criteria
6. A deployment mode matrix with guarantees and restrictions
7. A runtime assumption sheet and compute expectation profile
8. A drift audit design with hook points and required evidence
9. A test plan and validation checklist

## Core Concepts

### Portable cartridge

A portable cartridge is a versioned deployment unit containing:

- A manifest
- Immutable referenced assets
- Declared runtime requirements
- Declared entrypoints and task contracts
- Verification metadata
- Audit identifiers

The cartridge must be portable across environments that satisfy the same deployment contract and capability profile.

### Deployment contract

The deployment contract is the compatibility agreement between:

- Cartridge producer
- Deployment controller
- Runtime host
- Object truth sources
- Verification and audit subsystems

No runtime may execute a cartridge unless the contract is fully validated.

## Cartridge Manifest Shape

The manifest should be a single canonical document, preferably `manifest.json` or `manifest.yaml`, with stable field ordering for hashing and diffability.

### Required top-level shape

```json
{
  "manifest_version": "1.0",
  "cartridge_id": "phase9-portable-cartridge-example",
  "cartridge_version": "2026.04.30",
  "build_id": "build_2026_04_30_0001",
  "created_at": "2026-04-30T00:00:00Z",
  "producer": {
    "name": "producer-system",
    "version": "1.0.0"
  },
  "compatibility": {
    "runtime_api": ">=1.0 <2.0",
    "os": ["linux"],
    "arch": ["amd64", "arm64"]
  },
  "entrypoints": {
    "load": "tasks/load",
    "execute": "tasks/execute",
    "verify": "tasks/verify",
    "retire": "tasks/retire"
  },
  "object_truth": {
    "primary": [],
    "optional": [],
    "derived": []
  },
  "assets": [],
  "bindings": [],
  "runtime": {
    "env": {},
    "network": "restricted",
    "filesystem": "read-mostly",
    "secrets_policy": "injected-at-runtime"
  },
  "compute": {
    "cpu": "2",
    "memory_mb": 4096,
    "disk_mb": 2048,
    "accelerator": null,
    "expected_duration_s": 300
  },
  "verification": {
    "suite_version": "1.0",
    "required_checks": []
  },
  "audit": {
    "content_digest": "sha256:...",
    "dependency_digests": [],
    "drift_hooks": []
  },
  "signatures": []
}
```

### Required manifest rules

- `manifest_version` must govern schema interpretation only, not business versioning.
- `cartridge_id` must be globally unique within the deployment estate.
- `cartridge_version` must identify the release of the cartridge contract surface.
- `build_id` must uniquely identify the produced artifact instance.
- `created_at` must be UTC in RFC 3339 format.
- `compatibility` must declare all runtime compatibility assumptions explicitly.
- `entrypoints` must be symbolic task contract identifiers, not environment-specific shell paths.
- `object_truth` must declare all external truth sources needed for correct execution.
- `assets` must list immutable packaged files with digest, size, and role.
- `bindings` must declare integration points and late-bound values.
- `runtime` must state execution assumptions, access restrictions, and secret handling.
- `compute` must state minimum supported resources and expected duration envelope.
- `verification` must enumerate mandatory checks required before promotion.
- `audit` must provide enough metadata to reconstruct provenance and detect drift.
- `signatures` must support at least one producer signature and optionally environment attestation.

### Asset record shape

Each asset record should include:

- `path`
- `role`
- `media_type`
- `size_bytes`
- `digest`
- `executable`
- `required`

### Binding record shape

Each binding record should include:

- `binding_id`
- `kind`
- `required`
- `resolution_phase`
- `source`
- `target`
- `contract_ref`

Supported binding kinds:

- `object_reference`
- `secret_reference`
- `service_endpoint`
- `queue_topic`
- `model_handle`
- `policy_handle`

## Object Truth Dependencies

Portable cartridges must distinguish between packaged content and truth-bearing external objects.

### Dependency classes

1. Primary truth objects
   These are authoritative inputs required for correctness. Example classes: policy snapshots, schema registries, model cards, reference datasets.
2. Optional truth objects
   These improve behavior but must not affect correctness if absent unless the manifest marks them required by policy.
3. Derived truth objects
   These are recomputable artifacts generated from primary truth objects and must declare their derivation lineage.

### Dependency requirements

- Every truth object must have a stable identifier.
- Every truth object must declare version or digest.
- Every truth object must declare authority source.
- Every truth object must declare freshness policy.
- Every truth object must declare failure policy if unavailable or stale.
- Derived objects must identify parent objects by digest, not name only.

### Resolution order

1. Resolve manifest schema version
2. Verify cartridge digest and signatures
3. Resolve primary truth objects
4. Resolve required bindings
5. Resolve optional truth objects
6. Materialize derived truth objects if absent and permitted
7. Execute verifier suite
8. Approve runtime mount

### Truth failure policies

- `fail_closed`: deployment blocked if object is missing, stale, or unverifiable
- `warn_and_continue`: deployment proceeds with audit event
- `recompute_then_validate`: derive replacement artifact and rerun checks
- `fallback_to_pinned`: use explicitly pinned prior object version and emit drift alert

## Task Contracts

Task contracts define the required behavior of each lifecycle stage. A runtime may implement them differently internally, but observable inputs and outputs must be stable.

### Contract: build

Purpose:
Produce a cartridge from declared sources and emit a manifest, digests, and verification metadata.

Inputs:

- Source assets
- Dependency declarations
- Compatibility target
- Build configuration

Outputs:

- Immutable cartridge package
- Canonical manifest
- Content digest
- Build report

Must guarantee:

- Deterministic manifest content for identical inputs
- Complete asset listing
- Complete dependency listing
- Reproducible digest computation rules

### Contract: load

Purpose:
Validate package integrity and prepare runtime mount without mutating business state.

Inputs:

- Cartridge package
- Runtime capability profile
- Binding values

Outputs:

- Load decision: accept or reject
- Mounted artifact view
- Load audit report

Must guarantee:

- No task execution before verification gate passes
- Clear rejection reason on incompatibility
- No silent fallback to undeclared dependencies

### Contract: execute

Purpose:
Run declared entrypoint workload under the manifest's runtime and binding constraints.

Inputs:

- Accepted mounted cartridge
- Resolved truth objects
- Resolved bindings
- Runtime execution context

Outputs:

- Task result envelope
- Structured logs
- Execution metrics
- Audit trail

Must guarantee:

- Execution only against verified dependency graph
- Explicit reporting of resource consumption
- Stable exit codes or status taxonomy

### Contract: verify

Purpose:
Run mandatory compliance, integrity, compatibility, and drift checks.

Inputs:

- Cartridge package
- Runtime profile
- Dependency resolutions

Outputs:

- Verifier report
- Pass/fail decision
- Evidence bundle

Must guarantee:

- Deterministic rule evaluation for same inputs
- Machine-readable evidence
- Named failed checks with actionable reason codes

### Contract: retire

Purpose:
Deactivate a deployed cartridge cleanly while preserving auditability.

Inputs:

- Deployment instance identifier
- Retirement policy

Outputs:

- Retirement status
- Final audit event
- Artifact retention decision

Must guarantee:

- No orphaned bindings or untracked state transitions
- Retention policy enforcement
- Traceable handoff to replacement version if applicable

## Integration Bindings

Bindings define how the cartridge connects to environment-specific systems without embedding environment-specific values into the artifact.

### Binding principles

- Bind late, validate early
- Never store secret values in the cartridge
- Every binding must have a contract reference
- Bindings must be typed and schema-checked
- Runtime must reject undeclared binding injection

### Binding table

| Binding class | Source | Resolution phase | Validation rule | Failure mode |
| --- | --- | --- | --- | --- |
| Object store reference | Deployment controller | Pre-load | Must match digest or version policy | Fail closed |
| Secret reference | Secret manager | Pre-execute | Must satisfy secret contract and scope | Fail closed |
| Service endpoint | Environment registry | Pre-execute | Must satisfy protocol and auth profile | Reject deployment |
| Queue or topic | Messaging registry | Pre-execute | Must satisfy durability and ACL policy | Reject deployment |
| Model handle | Model registry | Pre-verify | Must match allowed model family and revision | Fail closed |
| Policy handle | Policy registry | Pre-verify | Must match effective policy snapshot | Fail closed |

## Verifier Suite

The verifier suite is mandatory and versioned independently from the cartridge implementation.

### Required verifier categories

1. Schema verification
   Validate manifest schema, required fields, allowed enums, and canonical formatting.
2. Integrity verification
   Validate package digest, asset digests, and signature chain.
3. Compatibility verification
   Validate runtime API, OS, architecture, and declared capability needs.
4. Dependency verification
   Validate truth object presence, digest/version pins, and freshness policy.
5. Binding verification
   Validate all required bindings exist and conform to contract.
6. Runtime policy verification
   Validate network, filesystem, secret, and privilege assumptions.
7. Compute verification
   Validate requested resources fit supported runtime class and configured limits.
8. Drift verification
   Compare effective deployment inputs with manifest-declared expectations.
9. Smoke execution verification
   Run non-destructive verifier entrypoint if declared by policy.

### Verifier outputs

The suite must emit:

- Overall status
- Per-check status
- Evidence references
- Start and end timestamps
- Runtime profile used
- Effective dependency graph
- Reason codes for all failures and warnings

### Required reason code families

- `SCHEMA_*`
- `INTEGRITY_*`
- `COMPAT_*`
- `DEPENDENCY_*`
- `BINDING_*`
- `RUNTIME_*`
- `COMPUTE_*`
- `DRIFT_*`
- `SMOKE_*`

## Deployment Modes

The contract must support a small, explicit set of deployment modes.

### Mode: local verification

Purpose:
Validate cartridge shape and basic portability without external promotion.

Properties:

- No production bindings
- Mock or pinned truth objects allowed
- Smoke verification required

### Mode: staged deployment

Purpose:
Run full verification against production-like infrastructure before promotion.

Properties:

- Real bindings allowed
- Policy and drift checks mandatory
- Rollback path must be defined

### Mode: production deployment

Purpose:
Run approved cartridge against production traffic or production jobs.

Properties:

- Full verifier suite mandatory
- Only signed cartridges accepted
- Primary truth objects must be authoritative
- Drift hooks enabled continuously

### Mode: offline or air-gapped deployment

Purpose:
Support execution where network access is absent or restricted.

Properties:

- All required assets and truth objects must be packaged or pre-seeded
- Late-bound secrets may use local secure injection method
- No runtime dependency on public registries

## Runtime Assumptions

Every compatible runtime must satisfy these baseline assumptions:

- It can read the canonical manifest format.
- It can verify digests and signatures.
- It can enforce declared filesystem and network policy.
- It can inject bindings without mutating the cartridge.
- It can emit structured logs and audit events.
- It can expose CPU, memory, disk, and duration metrics.
- It can fail closed when verification or policy checks fail.

### Explicit non-assumptions

- Persistent writable disk is not assumed.
- Outbound internet access is not assumed.
- Elevated privileges are not assumed.
- Runtime-side inference or recomputation is not assumed unless manifest permits it.

## Compute Expectations

Compute expectations are declarations for safe scheduling and portability, not optimization promises.

### Required compute fields

- Minimum CPU
- Minimum memory
- Minimum disk
- Accelerator need, if any
- Expected duration
- Peak concurrency assumption
- Burst tolerance

### Scheduling policy expectations

- Runtime must reject cartridges whose minimum requirements exceed available class.
- Runtime may overprovision but may not silently underprovision.
- Runtime must record actual versus declared resource use for later drift and capacity audit.

### Baseline sizing classes

Use coarse classes to keep portability stable:

- `small`: up to 1 CPU, 2 GB RAM, 10 minutes
- `medium`: up to 2 CPU, 8 GB RAM, 30 minutes
- `large`: up to 8 CPU, 32 GB RAM, 2 hours
- `accelerated`: any GPU or specialized accelerator requirement

## Drift Audit Hooks

Drift audit hooks detect divergence between the cartridge contract and effective deployment behavior.

### Hook points

1. Build-time hook
   Record manifest digest, asset digests, producer identity, and dependency graph.
2. Load-time hook
   Record runtime profile, accepted bindings, verifier version, and compatibility decision.
3. Execute-time hook
   Record effective truth object versions, resource usage, network policy state, and task result taxonomy.
4. Post-run hook
   Record output artifact lineage, validation summary, and retention decision.
5. Periodic runtime hook
   Recheck long-lived deployments for binding changes, policy changes, and truth-object staleness.

### Drift dimensions

- Manifest drift
- Dependency drift
- Binding drift
- Policy drift
- Compute drift
- Runtime capability drift
- Output lineage drift

### Audit evidence requirements

- Immutable event IDs
- UTC timestamps
- Cartridge and deployment identifiers
- Effective versions and digests
- Check status and reason codes
- Actor or system principal

## Tests

The test plan should remain bounded and contract-focused.

### Unit-level contract tests

- Manifest schema accepts valid required shapes
- Manifest schema rejects missing required fields
- Asset digest validator rejects mismatched files
- Binding validator rejects undeclared bindings
- Truth dependency resolver enforces resolution order

### Integration-level tests

- Runtime accepts compatible cartridge with all required bindings
- Runtime rejects incompatible runtime API version
- Runtime rejects stale primary truth object under `fail_closed`
- Runtime accepts `fallback_to_pinned` when policy allows and emits audit warning
- Verifier suite emits machine-readable evidence for failures

### Mode-specific tests

- Local verification mode runs without production secrets
- Staged deployment mode performs full policy checks
- Production deployment mode rejects unsigned cartridge
- Offline deployment mode succeeds with pre-seeded dependencies only

### Negative tests

- Corrupt manifest signature
- Missing required asset
- Missing required secret binding
- Runtime class below declared compute floor
- Undeclared outbound network requirement
- Drift detected between verified and effective dependency versions

## Validation Checklist

The phase is valid only if all checklist items pass.

### Contract validation

- Manifest schema is complete and unambiguous
- All lifecycle task contracts have named inputs, outputs, and guarantees
- Binding types and failure modes are explicit
- Deployment modes are mutually distinguishable

### Portability validation

- No environment-specific absolute paths are required
- No secret values are embedded in cartridge payload
- No undeclared external dependency is necessary for successful execution
- Offline feasibility is defined when mode is claimed

### Audit validation

- Every major lifecycle stage emits an audit event
- Drift dimensions are covered by at least one hook
- Evidence bundle structure is machine-readable

### Operational validation

- Compute floor is declared
- Runtime assumptions are testable
- Failure policies are explicit for all truth dependencies
- Rollback or retirement path is defined for promoted deployments

## Acceptance Criteria

Phase 9 is accepted when:

1. A reviewer can determine cartridge validity from the manifest and verifier evidence alone.
2. A runtime implementer can build a compatible loader without consulting hidden tribal knowledge.
3. A deployment operator can distinguish supported deployment modes and their gates.
4. An auditor can trace artifact provenance, dependency versions, and drift events end to end.
5. No required behavior depends on unstated environment assumptions.

## Open Decisions

These choices may remain open, but must be resolved before implementation:

- Canonical manifest format: JSON only, YAML only, or dual-format with canonical normalization
- Signature standard and trust chain format
- Packaging envelope format: tarball, OCI-like artifact, or zip with canonical digest rules
- Whether smoke verification is mandatory for every deployment mode
- Whether derived truth objects may be materialized by runtime or only by build system

## Non-Goals Reminder

This packet is intentionally bounded. It defines the contract surface for a portable cartridge and its deployment lifecycle, but it does not implement packaging, registries, runtime orchestration, or product-specific business behavior.
