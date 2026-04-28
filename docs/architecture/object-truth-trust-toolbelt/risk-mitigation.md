# Object Truth Risk Mitigation

Status: deeper risk review, grounded in live Praxis authority on 2026-04-28.

## Verdict

The biggest risk is not whether this can be built. It can.

The biggest risk is accidentally recreating the integration-platform failure
mode: many connectors, many mappings, many rules, no single authority proving
what is true.

Mitigation is simple in principle and strict in execution:

```text
observe first
structure deterministically
compare deterministically
store evidence durably
emit gaps explicitly
let LLMs reason only over the evidence product
verify before promotion
```

## Risk Register

| Risk | Why it matters | Mitigation | Gate |
| --- | --- | --- | --- |
| DB connection ceiling | Discovery hit connection pressure earlier; object truth workflows will fan out across systems, samples, fields, receipts, and events. | Phase 0 must add an `object_truth.readiness` query that checks pool settings, current DB health, and safe fanout limits before launch. Use one-proof-before-fleet and cap sample jobs. | No multi-system workflow until one fixture run completes without pool exhaustion. |
| LLM over raw payloads | If the model compares raw JSON directly, truth becomes unreplayable and uninspectable. | LLM receives comparison summaries, field observations, hashes, confidence, and gaps, not raw unstructured dumps. | Contract-author task refuses input unless comparison run id and evidence refs exist. |
| Static integration HTTP bypass | `BUG-DF32D694` says integration admin routes still bypass gateway/receipts. | Object truth writes must use gateway operations only. UI/API integration paths should wait for or ride behind the gateway-dispatch roadmap item. | No object truth static handler may write domain state. |
| Pattern authority not materializing | `BUG-9A25C4A4` and `BUG-CE7534B9` show candidate patterns exist but durable materialization/bypass issues remain. | Object truth can read existing bugs/decisions/typed gaps initially, but pattern feedback is not a release gate until materialization is verified. | Pattern loop phase requires `praxis_patterns materialize` proof and readback. |
| Provider fantasy | Model access matrix lists many enabled controls, but provider control plane says most are not runnable. | New task types route only to `openai/gpt-5.4` and `openai/gpt-5.4-mini` until provider control plane proves more. | Workflow launch preflight checks `is_runnable=true` for each LLM task type. |
| Data dictionary category drift | Historical bug `BUG-36B00C79` shows query/command companion rows can violate constraints if guessed manually. | Use `praxis_operation_forge` and `praxis_register_operation`; do not hand-author operation triple rows. | Operation registry tests prove import resolution and dictionary/object rows. |
| Generic object table overload | Generic `objects.properties` cannot express evidence, samples, identity, comparisons, or revisions. | Domain tables own object truth. Object lifecycle may receive projections only. | No object truth acceptance test can pass if evidence only exists in generic JSONB. |
| Sample bias | 100 or 1000 records can still lie if the sample is stale, filtered, or not comparable. | Store sample strategy, query, source cursor, timestamp, returned count, and freshness. Require multiple strategies before `confirmed`. | Contract status cannot move to `confirmed` from one unqualified sample. |
| Identity false positives | Bad same-object grouping poisons every downstream field comparison. | Identity links are reversible, confidence-scored, evidence-linked, and statused. Low confidence emits typed gaps. | No contract can use `candidate` identity clusters as confirmed truth. |
| Hierarchy/flattening ambiguity | Flattened CRM fields and nested ERP objects can look like disagreement. | Store canonical field paths, cardinality kind, hierarchy signal, and transform signal separately. | Comparison run must classify hierarchy state before source authority inference. |
| Source timestamp lies | `updated_at` can mean sync time, automation touch, or human edit. | Treat freshness as a signal, not proof. Store source actor, source version, integration source, and field/source timestamps separately. | Field authority confidence must include timestamp provenance. |
| Raw data/privacy leakage | Object samples may contain client PII, secrets, or financial data. | Hash raw payloads, store redacted previews, keep raw payload storage optional and encrypted/retention-bound. | Fixture tests include redaction assertions. |
| Schema drift | Source systems change after contract creation. | Every contract revision binds schema hashes and sample hashes. Readiness checks stale dependency hashes. | Task environment contract refuses active use when dependency hash is stale beyond policy. |
| Workflow green-state illusion | A workflow can look submitted/running without real work. | Require receipt, event, artifact, verifier output, or heartbeat proof before declaring success. | Contract materialization requires independent proof artifacts. |
| Tool authority drift | MCP tool code can grow hidden behavior. | MCP wrapper only dispatches gateway operation. Domain behavior stays in runtime/repository. | Static scan rejects object truth domain writes from `surfaces/mcp`. |
| Registry drift | New tables/operations can exist without primitive/catalog/dictionary parity. | Record object truth in `primitive_catalog` and run primitive consistency scan. | Release gate includes `primitive.scan_consistency`. |
| Fanout and API rate limits | Sampling 1000 records across many systems can hit client APIs hard. | Add sample windows, source-specific concurrency caps, backoff, and partial sample states. | Capture commands return `partial` with receipt instead of retry storm. |
| Cost creep | LLM use across every field/object could become expensive and slow. | Deterministic tasks use no model. LLM sees compact evidence summaries only for contract author/review. | Compare phase succeeds without any LLM route. |
| Operator question burden | If every ambiguity asks Nate, the product becomes a fancy interruption generator. | Typed gaps are clustered, deduped, ranked, and converted into targeted questions only when blocking. | Question generation requires gap cluster id and blocking reason. |
| Roadmap invisibility | A markdown-only plan can drift from DB-backed roadmap truth. | Roadmap epic committed as `roadmap_item.object.truth.trust.toolbelt.authority`; keep these docs as the deep architecture packet. | Roadmap readback must show the project epic. |

## Phase 0 Mitigations

Before implementation, add or verify these gates:

| Gate | Proof |
| --- | --- |
| DB health gate | `object_truth.readiness` reports pool config, recent connection failures, and safe fanout setting. |
| Provider route gate | Provider control plane readback shows runnable routes for each LLM-backed task type. |
| Gateway gate | `praxis_operation_forge` previews each operation path before code edits. |
| Pattern gate | Existing pattern bugs are acknowledged; pattern feedback loop is not assumed working until materialization proves out. |
| Fixture gate | Salesforce/HubSpot/NetSuite fixture run completes before real client systems. |
| Privacy gate | Fixture includes sensitive-looking values and verifies redaction/hash behavior. |

## Mitigation Details

### DB Connection Ceiling

Current connection management uses a shared asyncpg pool in:

```text
/Users/nate/Praxis/Code&DBs/Workflow/storage/postgres/connection.py
```

Default max pool size is currently `40`, controlled by:

```text
WORKFLOW_POOL_MAX_SIZE
WORKFLOW_POOL_MIN_SIZE
WORKFLOW_POOL_ACQUIRE_TIMEOUT_S
```

Object truth should not solve this by raising the pool size blindly. That only
makes the database scream in a slightly deeper voice.

Required mitigation:

| Control | Implementation |
| --- | --- |
| One proof before fleet | First run must use one object across two fixture systems. |
| Fanout caps | Capture commands accept `max_systems`, `max_objects`, `max_samples`, `max_parallel_captures`. |
| Chunking | Sample capture and field extraction batch large records. |
| Receipts | Each child operation carries `cause_receipt_id` and shared `correlation_id`. |
| Readiness | `object_truth.readiness` surfaces DB pressure and refuses broad launch when unsafe. |

### LLM Boundary

LLMs may do:

| Allowed | Example |
| --- | --- |
| Explain structured comparison results | "Salesforce `AnnualRevenue` and NetSuite `revenue.amount` agree after currency normalization." |
| Propose source authority hypotheses | "ERP likely owns financial totals because CRM values lag and have sync actor metadata." |
| Classify ambiguity | "This looks like flattening, not disagreement." |
| Draft targeted questions | "Does HubSpot Company Domain or Salesforce Account Website own customer identity?" |
| Draft contracts | Use evidence refs, not raw guesswork. |

LLMs may not do:

| Forbidden | Replacement |
| --- | --- |
| Parse raw schemas | `schema_normalizer.py` |
| Flatten raw records | `field_observation.py` |
| Decide identity from vibes | `identity_resolution.py` with evidence and thresholds |
| Compare fields directly | `comparison.py` |
| Invent source authority rules | `contract_compiler.py` must require evidence links |
| Declare success without verifier proof | Workflow verification gate |

### Identity Resolution

Identity resolution should be conservative.

Statuses:

| Status | Meaning |
| --- | --- |
| `candidate` | Machine thinks these records may match. Not contract-grade. |
| `confirmed` | Deterministic strong key or operator-confirmed. |
| `rejected` | Explicitly not the same real-world object. |
| `split` | Prior cluster was too broad. |
| `superseded` | Replaced by newer cluster. |

Confidence thresholds:

| Threshold | Behavior |
| --- | --- |
| `>= 0.95` | Can be used in draft contract if evidence method is deterministic. |
| `0.80 - 0.94` | Can be used in comparison but blocks `confirmed` contract status. |
| `< 0.80` | Emit typed gap and targeted question. |

### Contract Staleness

Every task environment contract revision must include:

```text
contract_hash
dependency_hash
object_truth_contract_refs
object_truth_contract_hashes
sop_refs
sop_hashes
pattern_refs
pattern_hashes
verifier_refs
model_policy_ref
sample_refs
schema_hashes
created_at
expires_or_recheck_after
```

If any dependency changes, the old revision is not mutated. A new revision is
appended.

### Pattern Feedback

Pattern integration should wait until current open bugs are resolved or worked
around:

| Bug | Status | Plan impact |
| --- | --- | --- |
| `BUG-9A25C4A4` | Open P1 | Do not rely on durable pattern rows until materialization is verified. |
| `BUG-CE7534B9` | Open P2 | Ensure producers do not bypass pattern authority when promoting recurring failures. |

Initial release can still bind known anti-patterns from decisions, bugs, and
typed gaps. Durable pattern feedback becomes a later phase.

## Architecture Review

Review what you built could you have built anything differently mathematically or component wise?

### Mathematical / Data-Model Alternatives

| Alternative | Verdict |
| --- | --- |
| Use only generic `objects` plus JSONB properties | Reject. Too weak for evidence, identity, comparison, revision, and verifier authority. |
| Store one wide comparison JSON blob per run | Reject. Hard to query, hard to cluster, hard to verify field-level behavior. |
| Model identity as pairwise links only | Reject alone. Pairwise links are useful evidence, but clusters need first-class heads/status. |
| Treat source timestamps as field authority | Reject. Timestamps are a signal, not truth. |
| Store field observations as one row per field path/value signal | Accept. This is queryable, clusterable, and verifier-friendly. |
| Use append-only revision tables for contracts | Accept. This matches standing policy and avoids silent stale cache behavior. |

### Component / Architecture Alternatives

| Alternative | Verdict |
| --- | --- |
| Build as a `praxis_data` extension only | Reject. Data ops are substrate, not domain authority. |
| Build as a new MCP mega-tool with internal logic | Reject. MCP is surface only. |
| Build as operation catalog CQRS authority with repository and domain runtime | Accept. One front door, receipts, events, registry, tests. |
| Build task contracts inside control-plane manifests only | Mixed. Existing manifest lane is useful, but task contracts need dedicated domain tables because they carry success semantics and revision dependencies. |
| Use provider routing hardcoded in workflow specs | Reject. Provider control plane owns runnable truth. |
| Add UI first | Reject. UI should render authority after DB/CQRS substrate exists. |

Which ones are genuine improvements?

| Improvement | Why it is genuine |
| --- | --- |
| Dedicated object truth domain tables | Reduces ambiguity and makes evidence queryable. |
| Field observations as first-class rows | Enables deterministic comparison, drift detection, and pattern mining. |
| Identity clusters plus links | Separates cluster state from evidence and makes false positives reversible. |
| Task contracts as append-only revisions | Makes "what success means" reusable, inspectable, and safely stale-detectable. |
| Readiness query before fanout | Converts hidden operational risk into a preflight gate. |
| Provider control preflight per LLM task type | Prevents workflow specs from selecting disabled or non-admitted models. |

## Release Blockers

| Blocker | Required mitigation |
| --- | --- |
| DB connection pressure | Add readiness gate and prove one fixture run. |
| Gateway bypass in integration routes | Object truth cannot depend on static integration writes. |
| Pattern materialization gap | Treat pattern feedback as post-MVP unless verified. |
| Privacy/redaction | Raw payload retention and redacted previews must be explicit before client data. |
| Provider truth | Add task routing rows only for currently runnable models. |
