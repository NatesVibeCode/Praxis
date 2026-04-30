# Object Truth Readiness Gate - 2026-04-30

## Authority

`object_truth.readiness` is the Phase 0 gate for the client operating model.
It is a read-only CQRS query under `authority.object_truth`.

Object Truth owns observed evidence. Virtual Lab owns simulated consequences.
The readiness gate does not execute automations, ingest client records, or
promote anything into a live sandbox.

## What It Proves

The gate reports whether downstream Object Truth and Virtual Lab planning can
advance:

- evidence tables exist
- required Object Truth operations are registered and enabled
- evidence tables are covered by authority and data-dictionary registries
- write operations have receipt-required event contracts
- digest, sensitivity, metadata, and redacted-preview columns exist
- raw client payload mode is blocked unless an explicit privacy policy ref is supplied
- planned fanout is blocked if required operations are missing or mismatched

Blocked readiness is a normal query result. Callers must read `can_advance`,
`state`, `gates`, and `no_go_conditions`; they must not treat a successful tool
call as permission to proceed.

## Surfaces

- Operation: `object_truth_readiness`
- Tool: `praxis_object_truth_readiness`
- Alias: `object-truth-readiness`
- API route: `GET /api/object-truth/readiness`
- Handler: `runtime.operations.queries.object_truth.handle_readiness`
- Input model: `runtime.operations.queries.object_truth.QueryReadiness`

## Promotion Rule

Any client-system discovery, ingestion, MDM, or Virtual Lab packet must query
this gate first and stop when `can_advance` is false.
