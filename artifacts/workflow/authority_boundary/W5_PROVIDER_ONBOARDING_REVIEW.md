# W5 Provider Onboarding Review

Date: 2026-04-09

Source of truth:
- `artifacts/workflow/authority_boundary/BUG_TRIAGE_2026-04-09_R2.md`

Decision rule used here:
- `real_boundary_leak`: the wizard is crossing into a different authority surface or mutating external benchmark truth inline.
- `explicit_authority_by_design`: the wizard is the intended authority for the row it writes, and the table is part of the onboarding contract.
- `needs_narrower_followthrough`: the behavior is real, but the fix should narrow the selector/classification path onto the dedicated packet or sync surface instead of relabeling the whole wizard as a leak.

Summary:
- `real_boundary_leak`: 1
- `explicit_authority_by_design`: 2
- `needs_narrower_followthrough`: 1

## `real_boundary_leak`

- `BUG-7EC03E3841E1` `Provider onboarding applies benchmark match rules and market bindings directly`
  `Code&DBs/Workflow/runtime/provider_onboarding.py:2481-2571` does more than record onboarding output: `_apply_benchmark_plan()` writes `provider_model_market_match_rules`, clears and recreates source bindings, and rewrites `provider_model_candidates.benchmark_profile` by calling private helpers from `sync_market_model_registry.py`. That is a separate benchmark-registry authority path, not just onboarding transcript state. `Code&DBs/Workflow/scripts/sync_market_model_registry.py:1-8` explicitly says market benchmark data belongs in a dedicated registry surface, and `Code&DBs/Databases/migrations/workflow/047_market_model_registry.sql:1-34` says that registry is not the executable routing catalog. This one is still a real boundary leak.

## `explicit_authority_by_design`

- `BUG-8884CA96110D` `Provider onboarding records probe receipts as durable rows`
  `Code&DBs/Workflow/runtime/provider_onboarding.py:2151-2195` writes `provider_transport_probe_receipts` only after the probe step results are known, and `Code&DBs/Workflow/runtime/provider_onboarding.py:2822-2857` persists those receipts as part of the wizard finish path. `Code&DBs/Workflow/runtime/provider_onboarding.py:2648-2761` then reads the same receipts back in `_verification_report()`, so this is the wizard's durable evidence trail, not a second product authority. `Code&DBs/Databases/migrations/workflow/078_provider_transport_admission_receipts.sql:35-53` introduces the receipts table alongside the transport-admission table, which is exactly what you would expect for a probe transcript.

- `BUG-2FA46602CC35` `Provider onboarding writes provider transport admissions directly`
  `Code&DBs/Workflow/runtime/provider_onboarding.py:2052-2148` creates the `provider_transport_admissions` row after transport, model, and capacity probes, and `Code&DBs/Workflow/runtime/provider_onboarding.py:2839-2857` makes that write part of the onboarding closeout. `Code&DBs/Workflow/adapters/provider_registry.py:593-651` consumes `provider_transport_admissions` as lane policy truth when loading the provider registry, so this table is the explicit transport-admission authority for the wizard. `Code&DBs/Databases/migrations/workflow/078_provider_transport_admission_receipts.sql:3-33` also defines the table as a canonical admitted-lane record and backfills it from the legacy provider profile rows.

## `needs_narrower_followthrough`

- `BUG-62837FA9E7B3` `Provider onboarding writes model catalog and binding rows directly`
  `Code&DBs/Workflow/runtime/provider_onboarding.py:1257-1379` still infers `route_tier`, `latency_class`, and task affinities from slugs when overrides are absent, then `_resolve_models()` feeds that inferred classification into the `registry_write` step at `Code&DBs/Workflow/runtime/provider_onboarding.py:2978-3037`. That path is real and bounded, but it is still too loose for the explicit model-catalog packet: `Code&DBs/Workflow/docs/model_catalog_classification_2026-04-08.md:12-19` says slug inference is no longer acceptable, and `Code&DBs/Workflow/docs/model_catalog_classification_2026-04-08.md:109-111` says `sync_provider_model_catalog.py` must fail if a discovered model is not present in the JSON packet. The rows themselves are canonical `registry/` authority (`Code&DBs/Workflow/docs/10_V1_POSTGRES_TABLES.md:39-41,600-700`), but the classification path still needs to narrow onto the explicit packet instead of staying heuristic-heavy in the wizard.
