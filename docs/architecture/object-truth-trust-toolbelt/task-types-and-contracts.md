# Object Truth Task Types And Contracts

Status: workflow/task contract plan, grounded in live Praxis authority on 2026-04-28.

## Verdict

Most object truth work should not use an LLM at all.

The model should enter only after deterministic evidence exists, and only for
contract authoring, review, explanation, and targeted question generation.

Current runnable provider routes for relevant task families are:

```text
openai/gpt-5.4 CLI
openai/gpt-5.4-mini CLI
```

Anthropic and most Google/OpenAI legacy candidates are not release capacity
until `praxis_provider_control_plane` reports `is_runnable=true`.

## Workflow Task Types

Add task profiles and routing rows through DB authority, not hardcoded workflow
conventions.

Primary tables:

```text
task_type_profiles
task_type_routing
task_type_route_profiles
private_provider_control_plane_snapshot
private_model_access_control_matrix
```

Relevant files:

```text
/Users/nate/Praxis/Code&DBs/Databases/migrations/workflow/024_task_type_routing.sql
/Users/nate/Praxis/Code&DBs/Databases/migrations/workflow/044_task_type_route_profiles.sql
/Users/nate/Praxis/Code&DBs/Databases/migrations/workflow/099_task_type_profile_authority.sql
/Users/nate/Praxis/Code&DBs/Databases/migrations/workflow/276_task_type_routing_llm_knobs.sql
/Users/nate/Praxis/Code&DBs/Workflow/runtime/task_type_router.py
/Users/nate/Praxis/Code&DBs/Workflow/runtime/workflow/_routing.py
/Users/nate/Praxis/Code&DBs/Workflow/runtime/workflow/_execution_core.py
```

## Task Type Matrix

| Task type | Purpose | LLM allowed | Allowed model policy | Success |
| --- | --- | --- | --- | --- |
| `object_truth_schema_capture` | Introspect external schemas. | No | none | Schema snapshots stored, normalized, hashed, dictionary-projected, receipt-backed. |
| `object_truth_sample_capture` | Pull source records. | No | none | Sample row stored with strategy, query, counts, hashes, status, and receipt. |
| `object_truth_normalize` | Convert raw sampled records into normalized object versions. | No | none | Object versions created with raw/normalized hashes and metadata. |
| `object_truth_field_observation` | Extract field path/value/type/freshness signals. | No | none | Field observations exist for each normalized object version. |
| `object_truth_identity_resolution` | Create same-object clusters. | Limited | `openai/gpt-5.4-mini` for suggestions only, deterministic accept/reject rules required. | Clusters and links created with confidence, evidence, and reversible status. |
| `object_truth_comparison` | Compare fields across systems. | No | none | Comparison run and field comparison rows created with result hash. |
| `object_truth_contract_author` | Draft object truth contract. | Yes | `openai/gpt-5.4` only for MVP. | Draft contract references comparison runs, evidence, confidence, gaps, and verifiers. |
| `object_truth_contract_review` | Challenge contract against evidence. | Yes | `openai/gpt-5.4` primary, `openai/gpt-5.4-mini` only for low-risk review. | Review produces approval, required revisions, or evidence-linked rejection. |
| `object_truth_gap_question` | Turn blocking gaps into targeted questions. | Yes | `openai/gpt-5.4-mini` | Questions are deduped, gap-linked, and blocking reason is explicit. |
| `task_environment_contract_materialize` | Assemble task success contract revision. | Mostly no | `openai/gpt-5.4` only for human-readable explanation. | Append-only revision stored with contract hash and dependency hash. |
| `object_truth_verification` | Run deterministic verifiers. | No | none | Verifier run and receipt prove contract behavior or failure. |

## Task Failure Contracts

| Task type | Soft failure | Hard failure |
| --- | --- | --- |
| `object_truth_schema_capture` | Unsupported optional field metadata. | Auth failure, source unavailable, schema cannot be normalized, or schema hash cannot be computed. |
| `object_truth_sample_capture` | Partial sample or empty sample with explicit empty-state. | Permission denial, rate limit exhaustion, boundary violation, or missing source query proof. |
| `object_truth_normalize` | Unknown field kind classified as `json` with gap. | Payload cannot be parsed deterministically or normalized output would drop data. |
| `object_truth_field_observation` | Redacted preview unavailable. | Field path extraction fails or value hashing is unstable. |
| `object_truth_identity_resolution` | Confidence below threshold emits typed gap. | Resolver creates cluster without evidence or violates status transition. |
| `object_truth_comparison` | Field marked `ambiguous` or `incomparable`. | Missing samples, missing identity basis, stale dependency, or unversioned records. |
| `object_truth_contract_author` | Draft blocked by typed gaps. | Contract rule lacks evidence, references missing comparison run, or invents unsupported authority. |
| `object_truth_contract_review` | Requires revision. | Review cannot map findings to evidence or contradicts evidence without proof. |
| `object_truth_gap_question` | No non-duplicate question generated. | Question lacks gap id, blocking reason, or legal repair action. |
| `task_environment_contract_materialize` | Revision remains `draft` due gaps. | Dependency hash missing, verifier refs missing, or stale contract dependency. |
| `object_truth_verification` | Verifier marks `failed` and links evidence. | Verifier cannot execute, has no receipt, or target ref is missing. |

## Model Routing Rows

For MVP, insert only rows that are currently runnable.

Suggested initial `task_type_routing` rows:

| Task type | Provider | Model | Rank | Permitted | Reasoning | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| `object_truth_identity_resolution` | `openai` | `gpt-5.4-mini` | 1 | true | low | Suggestions only, deterministic rules accept/reject. |
| `object_truth_identity_resolution` | `openai` | `gpt-5.4` | 2 | true | medium | Fallback for complex identity ambiguity. |
| `object_truth_contract_author` | `openai` | `gpt-5.4` | 1 | true | high | High-risk contract drafting. |
| `object_truth_contract_author` | `openai` | `gpt-5.4-mini` | 2 | true | medium | Low-risk draft cleanup only. |
| `object_truth_contract_review` | `openai` | `gpt-5.4` | 1 | true | high | Evidence challenge and risk review. |
| `object_truth_gap_question` | `openai` | `gpt-5.4-mini` | 1 | true | low | Targeted question drafting. |
| `task_environment_contract_materialize` | `openai` | `gpt-5.4` | 1 | true | medium | Explanation only; manifest assembly is deterministic. |

No routing rows are needed for deterministic task types. Those should use
integration/runtime jobs, not model routes.

## Task Type Profiles

Suggested `task_type_profiles` entries:

| Task type | Allowed tools | Default tier | File attach | Prompt hint |
| --- | --- | --- | --- | --- |
| `object_truth_identity_resolution` | `["Read"]` | `economy` | false | Suggest identity matches from structured evidence only. |
| `object_truth_contract_author` | `["Read"]` | `frontier` | false | Draft object truth contracts with evidence refs and gaps. |
| `object_truth_contract_review` | `["Read"]` | `frontier` | false | Challenge contract claims against evidence. |
| `object_truth_gap_question` | `["Read"]` | `economy` | false | Convert blocking gaps into targeted questions. |
| `task_environment_contract_materialize` | `["Read"]` | `frontier` | false | Explain deterministic task contract revisions. |

## Task Environment Contract Shape

Each task environment contract revision should be machine-readable and
hashable.

Required envelope:

```json
{
  "kind": "task_environment_contract",
  "schema_version": 1,
  "task_environment_ref": "task_environment.customer_onboarding.account_sync",
  "revision_no": 1,
  "parent_revision_id": null,
  "contract_hash": "<sha256>",
  "dependency_hash": "<sha256>",
  "status": "draft",
  "object_truth_contract_refs": [],
  "sop_refs": [],
  "pattern_refs": [],
  "anti_pattern_refs": [],
  "typed_gap_refs": [],
  "allowed_tools": [],
  "allowed_task_types": [],
  "model_policy": {},
  "write_scope": {},
  "read_scope": {},
  "success_contract": {},
  "failure_contract": {},
  "verifier_refs": [],
  "created_from": {}
}
```

## Success Contract Body

The `success_contract` should include:

| Field | Meaning |
| --- | --- |
| `business_outcome` | What the task must accomplish in human/business terms. |
| `object_contracts` | Bound object truth contract refs and required statuses. |
| `field_rules` | Field authority, transformations, hierarchy handling, and write permissions. |
| `approval_gates` | Human or system approval requirements. |
| `verifier_requirements` | Verifiers that must pass. |
| `observability_requirements` | Receipts, events, artifacts, and readbacks required. |
| `staleness_policy` | When contract must be refreshed. |

## Failure Contract Body

The `failure_contract` should include:

| Field | Meaning |
| --- | --- |
| `hard_failures` | Conditions that must stop execution. |
| `soft_failures` | Conditions that continue but emit gaps or warnings. |
| `typed_gap_rules` | Which unresolved states become typed gaps. |
| `retry_policy` | Requires prior failure receipt and material retry delta. |
| `pattern_feedback_policy` | When repeated failures become pattern evidence. |
| `rollback_policy` | How write attempts are reversed or quarantined. |

## Contract Statuses

| Status | Meaning |
| --- | --- |
| `draft` | Contract exists but is not trusted for autonomous execution. |
| `active` | Contract can be used for bounded workflow execution. |
| `superseded` | Replaced by newer revision. |
| `revoked` | Explicitly unsafe or invalid. |
| `expired` | Stale by policy and requires refresh. |

## Workflow Admission Gates

Before any object-truth workflow launches:

| Gate | Required evidence |
| --- | --- |
| Workspace boundary | Workdir and write scope stay inside runtime profile workspace. |
| Provider route | Every LLM-backed task type has `is_runnable=true` in provider control plane. |
| DB readiness | `object_truth.readiness` says fanout is safe. |
| Integration readiness | Integration credentials and source permissions are verified. |
| Sample plan | Sample strategy, object names, count, and source query are explicit. |
| Privacy | Raw payload retention and redaction policy are explicit. |
| Verifier refs | Contract promotion includes verifier refs. |

## Workflow Spec Shape

Initial fixture workflow:

```text
object_truth_fixture_discovery
  -> object_truth_schema_capture.salesforce
  -> object_truth_schema_capture.hubspot
  -> object_truth_sample_capture.salesforce
  -> object_truth_sample_capture.hubspot
  -> object_truth_normalize.salesforce
  -> object_truth_normalize.hubspot
  -> object_truth_field_observation.salesforce
  -> object_truth_field_observation.hubspot
  -> object_truth_identity_resolution.account
  -> object_truth_comparison.account
  -> object_truth_contract_author.account
  -> object_truth_contract_review.account
  -> task_environment_contract_materialize.account_sync
  -> object_truth_verification.account_sync
```

The first real workflow should use the same shape with lower fanout and one
business object only.

## Readiness Query

Add `object_truth.readiness` before broad launch.

Required output:

| Field | Meaning |
| --- | --- |
| `ok` | Whether object truth workflows may launch. |
| `db_pool` | Pool min/max/acquire timeout and recent DB errors. |
| `provider_routes` | Runnable model routes for LLM-backed task types. |
| `integration_gate` | Whether gateway-dispatched integration operations are available. |
| `pattern_gate` | Whether pattern materialization is verified or deferred. |
| `privacy_gate` | Raw payload and redaction policy state. |
| `fanout_limits` | Max systems, objects, samples, and parallel captures. |
| `blocking_gaps` | Typed gaps or project blockers. |

## Release Criteria

| Criterion | Proof |
| --- | --- |
| Deterministic substrate | Compare run completes without LLM. |
| LLM boundedness | Contract author receives only evidence refs and summaries. |
| Model admissibility | Provider control plane proves selected routes. |
| Contract evolution | Materialization appends a new revision with prior hash and dependency hash. |
| Failure capture | Verifier failure links to receipt and emits pattern/typed-gap evidence. |
| Operator usefulness | Blocking gaps become few targeted questions, not a haystack. |

