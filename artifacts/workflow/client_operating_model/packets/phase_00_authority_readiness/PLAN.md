# Phase 0: Authority Readiness and Boundary Gates

## Verdict

`READY WITH GUARDS`

The idea is valid and the repo has enough authority to proceed into staged packet work. The previous generated packet incorrectly claimed the repo was unavailable under `/workspace`; this operator-corrected packet replaces that claim with the actual local authority observed in `/Users/nate/Praxis`.

The next waves may proceed only as bounded planning packets. Production code, schema changes, and broad workflow fanout remain blocked until each packet has explicit build scope, CQRS authority, tests, and rollback conditions.

## Parent Authority

Parent roadmap item:

- `roadmap_item.object.truth.trust.toolbelt.authority`

Durable architecture policy:

- `architecture-policy::object-truth-virtual-lab::object-truth-discovers-client-systems-virtual-lab-proves-consequences`

Policy meaning:

- Object Truth owns client system discovery, data unification, normalization, identity resolution, lineage, freshness, and field/source authority.
- Virtual Lab references Object Truth evidence to emulate integration actions and automation consequences before live sandbox promotion.
- Live sandbox proves or falsifies the Virtual Lab prediction; drift loops back into Object Truth evidence.

## Existing Authority To Reuse

Source docs:

- `docs/architecture/object-truth-trust-toolbelt/README.md`
- `docs/architecture/object-truth-trust-toolbelt/build-plan.md`
- `docs/architecture/object-truth-trust-toolbelt/risk-mitigation.md`
- `docs/architecture/object-truth-trust-toolbelt/task-types-and-contracts.md`

Current object-truth surfaces:

- `praxis workflow object-truth`
- `praxis workflow object-truth-store`
- `praxis workflow object-truth-store-schema`
- `praxis workflow object-truth-compare`
- `praxis workflow object-truth-record-comparison`

Current API surfaces:

- `POST /api/object-truth/observe-record`
- `POST /api/object-truth/store-observed-record`
- `POST /api/object-truth/store-schema-snapshot`
- `GET /api/object-truth/compare-versions`
- `POST /api/object-truth/record-comparison-run`

Current code paths:

- `Code&DBs/Workflow/core/object_truth_ops.py`
- `Code&DBs/Workflow/storage/postgres/object_truth_repository.py`
- `Code&DBs/Workflow/runtime/operations/commands/object_truth.py`
- `Code&DBs/Workflow/runtime/operations/queries/object_truth.py`
- `Code&DBs/Workflow/runtime/integration_manifest.py`

## Authority Boundaries

Object Truth is the evidence authority. It may observe, normalize, hash, compare, score, and persist system/object evidence. It does not execute client automations.

Virtual Lab is the consequence authority. It may replay a proposed integration or automation against virtualized state and produce predicted effects. It does not claim client truth unless backed by Object Truth evidence.

Live Sandbox is the promotion authority. It proves whether a Virtual Lab prediction survives a controlled real-system test. It does not become the canonical planning model.

Operator Roadmap is the delivery authority. It owns phase order, acceptance gates, and stop/retry boundaries.

CQRS Gateway is the operation authority. New behavior must route through registered operations with receipts/events where required. No MCP tool or static HTTP route gets to become a hidden domain write path.

## Readiness Gates

The program may continue when all of these are true:

- A parent roadmap item exists and is queryable.
- Object Truth and Virtual Lab authority boundaries are explicit.
- Source discovery uses cataloged connectors/manifests, not one-off scripts.
- Sensitive client payloads are represented by hashes, redacted previews, metadata, and storage references unless a privacy policy explicitly allows raw storage.
- Every workflow packet writes only its declared packet artifact.
- Firecheck reports `can_fire: true`.
- One representative proof run succeeds before broader fanout.

## No-Go Conditions

Stop or block downstream waves if any of these appear:

- Object Truth evidence is recomputed ad hoc instead of persisted with receipts.
- Connector discovery bypasses integration manifests or operation catalog authority.
- A workflow succeeds mechanically but cannot see the repo authority it was asked to inspect.
- Raw client data is placed in logs, queue payloads, roadmap text, or unclassified artifact files.
- Provider/runtime health is degraded in a way that affects the selected route.
- Later waves depend on guessed schemas or guessed connector behavior.
- A live sandbox write is proposed before a Virtual Lab prediction and rollback path exist.

## Missing Capability To Build

The current toolbelt has useful object-truth primitives, but the full client operating model still needs:

- `object_truth.readiness` read model that reports source authority, DB health, safe fanout, privacy posture, and open no-go conditions.
- Client system discovery records: systems, connectors, object catalogs, credential health, API limits, webhook/event surfaces, automation-bearing tools, and last observed state.
- MDM/unification records: identity clusters, normalization rules, source authority signals, lineage, freshness, and conflict evidence.
- Virtual Lab state model: event-sourced virtual system snapshots, action proposals, predicted object deltas, automation triggers, and verifier results.
- Promotion records: live sandbox run, predicted-vs-observed diff, drift classification, rollback evidence, and contract revision impact.
- Operator surfaces: one inspectable place to see discovery, truth contracts, simulations, promotions, and blocked no-go conditions.

## Implementation Order

1. Build the `object_truth.readiness` query and fail-closed downstream gate.
2. Add client system discovery and connector census authority.
3. Extend Object Truth ingestion around schema snapshots, samples, object versions, field observations, and evidence receipts.
4. Add MDM source-authority and normalization contracts.
5. Define hierarchy/task environment contracts.
6. Capture integration and automation action contracts.
7. Build Virtual Lab state and event model.
8. Add simulation runtime and verifier records.
9. Add live sandbox promotion and drift feedback.
10. Package portable cartridges and optional managed runtime controls.
11. Expose operator/Canvas inspection surfaces.

## Validation Path

Before launching downstream work:

- `praxis workflow firecheck --json`
- `praxis workflow chain-status <chain_id>`
- `praxis workflow run-status <run_id> --summary`
- `praxis workflow tools call praxis_search --input-json '{"query":"Object Truth Virtual Lab authority readiness client operating model","sources":["code","knowledge","decisions"],"limit":8}'`

Before building production code:

- verify operation catalog entries exist or are explicitly planned
- verify migrations use the canonical migration authority
- verify MCP/API/CLI surfaces are thin wrappers over gateway operations
- verify tests cover fail-closed readiness, privacy redaction, deterministic hashing, receipt persistence, and prediction drift

## Downstream Consumption Contract

Later waves must treat this packet as a guardrail, not as implementation permission.

The next packet may plan client system discovery. It must consume:

- this Phase 0 packet
- the Object Truth docs
- the Object Truth/Virtual Lab architecture policy
- the current integration manifest and tool catalog

Any downstream packet that cannot name its authority, persistence model, query surface, write command, tests, and stop conditions is not ready for build.
