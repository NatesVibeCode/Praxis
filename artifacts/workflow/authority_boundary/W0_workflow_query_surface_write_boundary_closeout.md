# W0 Workflow Query Surface Write Boundary Closeout

Date: 2026-04-09

Scope:
- Canonical workflow persistence cutover
- Workflow build and trigger reconciliation cutover
- Object, object-type, document, and document-attach cutover
- Proof that `workflow_query.py` no longer owns canonical writes

## Validation Set

- `PYTHONPATH='Code&DBs/Workflow' /opt/homebrew/bin/python3 -m pytest --noconftest -q 'Code&DBs/Workflow/tests/unit/test_workflow_query_handlers.py'`
- `PYTHONPATH='Code&DBs/Workflow' /opt/homebrew/bin/python3 -m pytest --noconftest -q 'Code&DBs/Workflow/tests/unit/test_object_lifecycle_boundary.py'`
- `./scripts/test.sh validate config/cascade/specs/authority_boundary/W0_workflow_query_surface_write_boundary_cleanup.json`

## Proof Summary

- `workflow_query.py` delegates workflow mutation paths to runtime owners instead of embedding canonical write SQL.
- `workflow_query.py` delegates object and document lifecycle mutations to runtime owners instead of request-layer SQL.
- Canonical workflow persistence, trigger reconciliation, workflow deletion, and manual trigger submission live in `runtime/canonical_workflows.py`.
- Object and document lifecycle ownership lives in `runtime/object_lifecycle.py`, with actual SQL mutation in `storage/postgres/object_lifecycle_repository.py`.
- Workflow persistence and trigger reconciliation live in `storage/postgres/workflow_runtime_repository.py`.

## Bug IDs Covered By This Boundary

Fixed in code/test evidence:
- `BUG-40DA68BB9804`
- `BUG-D3DFB6168BB6`
- `BUG-8C94783D4D9E`
- `BUG-751DB77C5276`
- `BUG-33FDDE3B0F99`
- `BUG-2A8C64BE882D`
- `BUG-C0ED2FB59CEA`
- `BUG-686B4FB7F137`
- `BUG-AFCB6ECD73F2`

Residual follow-on work:
- No product-code follow-on is identified for this boundary after the proof set above.
- Live bug-tracker terminal status updates were not applied in this sandbox, so the bug rows remain unclosed here until the canonical tracker is updated in an unrestricted runtime.

## Notes

- This record is proof-backed evidence, not a claim that the live bug tracker was mutated from this workspace.
- The closeout target is the boundary cutover itself; terminal bug state should only be recorded after the canonical closeout workflow runs against the live tracker.
