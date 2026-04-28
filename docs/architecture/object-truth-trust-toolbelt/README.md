# Object Truth Trust Toolbelt

Status: planning packet, grounded in live Praxis authority on 2026-04-28.

This packet defines the standalone build plan for turning Praxis into an
evidence-first object truth and task trust system.

The product is not an integration platform. The product is a trust toolbelt:
connect to business systems, observe schemas and records, build durable object
truth contracts, bind those contracts into task environments, and let agents do
business work only inside explicit, inspectable success boundaries.

## Documents

- [Build plan](build-plan.md) - DB, CQRS, registry, runtime paths, MCP, CLI, scripts, tests, and rollout phases.
- [Risk mitigation](risk-mitigation.md) - deeper risk review, mitigations, gates, and genuine architecture improvements.
- [Task types and contracts](task-types-and-contracts.md) - workflow task types, allowed models, success/failure contracts, and task environment contract shape.

## Existing Authority Used

The plan is anchored to these current Praxis decisions and facts:

| Authority | Current state |
| --- | --- |
| Object truth policy | Cross-system object truth must be inferred from sampled evidence, not hand-authored field precedence sprawl. |
| Deterministic substrate policy | Schema parsing, metadata extraction, identity grouping, field comparison, freshness scoring, lineage, hierarchy, and flattening are deterministic substrate, not LLM judgment. |
| Task environment contract policy | Task contracts are reused and evolved as append-only hashed manifests. |
| CQRS policy | New operations go through `operation_catalog_gateway`, with registry rows in `operation_catalog_registry`, `authority_object_registry`, and `data_dictionary_objects`. |
| Pattern policy | Failure patterns sit between evidence and bugs. |
| Provider reality | Current runnable routes for `architecture`, `analysis`, `build`, and `review` are `openai/gpt-5.4` and `openai/gpt-5.4-mini` over CLI. |
| Roadmap reality before this packet | Only one active roadmap item existed: gateway-dispatching static integration/admin/picker HTTP surfaces. |
| Roadmap item added by this packet | `roadmap_item.object.truth.trust.toolbelt.authority` |

## Architectural Verdict

Feasible, but only if object truth becomes its own authority domain.

Do not overload generic `objects` JSONB, ad hoc scripts, MCP tool internals, or
LLM prompts as the source of truth. The durable authority must be tables,
receipts, events, registry rows, manifests, and verifier results.

The clean shape:

```text
external systems
  -> schema snapshots
  -> record samples
  -> normalized object versions
  -> field observations
  -> identity clusters
  -> comparison runs
  -> object truth contracts
  -> task environment contract revisions
  -> workflow execution with verifiers and pattern feedback
```

## Non-Negotiables

- No LLM decision before deterministic parse, normalize, compare, and evidence linking.
- No direct SQL from MCP or static HTTP surfaces.
- No unversioned samples.
- No raw JSON-only object truth.
- No model route assumptions outside the provider control plane.
- No green-looking workflow success without independent proof.
- No new typed-gap table; emit `typed_gap.created` authority events.
- No fanout before one representative proof path succeeds.
