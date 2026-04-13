# Packet Index

Exact run order:

1. `W7_shared_inventory_and_parse_truth`
2. `W4_route_and_packet_package_consolidation`
3. `W8_watchlist_wire_or_delete`

## Validation Ledger

| Order | Packet | Spec Path | Validation Summary | Blocker |
| --- | --- | --- | --- | --- |
| 1 | W7 Shared Inventory And Parse Truth | `config/cascade/specs/repo_hygiene/W7_shared_inventory_and_parse_truth.json` | `./scripts/workflow.sh validate` printed `Spec Validation: PASSED` with 3 jobs; `./scripts/test.sh validate` returned `ok: true` with the same validation warning. | Agent resolution check was blocked by the sandbox permission surface (`[Errno 1] Operation not permitted`). |
| 2 | W4 Route And Packet Package Consolidation | `config/cascade/specs/authority_boundary/W4_route_and_packet_package_consolidation.json` | `./scripts/workflow.sh validate` printed `Spec Validation: PASSED` with 3 jobs; `./scripts/test.sh validate` returned `ok: true` with the same validation warning. | Agent resolution check was blocked by the sandbox permission surface (`[Errno 1] Operation not permitted`). |
| 3 | W8 Watchlist Wire Or Delete | `config/cascade/specs/repo_hygiene/W8_watchlist_wire_or_delete.json` | `./scripts/workflow.sh validate` printed `Spec Validation: PASSED` with 2 jobs; `./scripts/test.sh validate` returned `ok: true` with the same validation warning. | Agent resolution check was blocked by the sandbox permission surface (`[Errno 1] Operation not permitted`). |

## Dispatch Blockers

- No packet schema or field fixes were required. All three draft packets validated structurally as checked.
- The only blocker observed in this sandbox is agent resolution against Postgres-backed registry authority. That prevents a fully green local validate, but it is an environment limitation rather than a packet-content defect.
