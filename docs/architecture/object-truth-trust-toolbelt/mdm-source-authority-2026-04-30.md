# MDM Source Authority Primitives - 2026-04-30

## Verdict

Phase 3 now has a deterministic MDM/source-authority domain layer for Object
Truth evidence.

The authority split is deliberate:

- `core.object_truth_ops` owns low-level object versions, schema snapshots,
  field observations, and simple object comparisons.
- `runtime.object_truth.ingestion` owns source capture, replay fixture, payload
  reference, and readiness input records.
- `runtime.object_truth.mdm` owns identity clusters, match/anti-match signals,
  field normalization, reversible source links, freshness scoring, source
  authority evidence, field comparison, hierarchy/flattening signals, typed
  gaps, and stable packet digests.
- Storage, CQRS command/query handlers, MCP/API surfaces, migrations, and shared
  generated docs remain separate work.

## Authority Model

MDM selection is field-level, not record-level magic.

Source authority is recorded as explicit evidence with:

- `entity_type`
- `field_name`
- `source_system`
- `authority_rank`
- `authority_scope`
- `authority_reason`
- `evidence_type`
- `evidence_reference`
- `approved_by`
- `approved_at`
- `review_interval_days`

Lower `authority_rank` means stronger authority. If no authority evidence exists
for a field, `compare_field_candidates` leaves the canonical value unresolved
and emits a `policy-missing` gap. Consensus without authority is still not
authority. Cute, but not good enough.

## Identity Clusters

`build_identity_cluster` produces deterministic cluster evidence:

- `cluster_id`
- `entity_type`
- `member_records`
- `match_signals`
- `anti_match_signals`
- `cluster_confidence`
- `review_status`
- `cluster_state`
- `canonical_candidate`
- `identity_cluster_digest`

Positive signals are scored by class:

- exact identifier
- strong quasi-identifier
- weak descriptive similarity
- relational context
- temporal consistency
- source provenance confidence

Blocking anti-match signals force `split-required` for multi-member clusters.
This prevents deceptive near-duplicates from being merged just because names or
descriptions look friendly.

## Normalization And Reversibility

`build_normalization_rule_record` produces catalogable rule records with:

- field/entity scope
- input pattern
- ordered transform steps
- output type
- reversibility flag
- loss risk
- exception policy
- test examples
- locale assumptions

`normalize_field_value` preserves the raw source value, normalized value,
transform chain, loss risk, normalization status, and digest. Lossy transforms
are allowed only as auditable evidence; they do not erase source values.

`build_reversible_source_link` connects each selected canonical field back to:

- source system
- source record
- source field
- raw value
- normalized value
- transform chain
- selection reason
- authority basis
- observed/loaded timestamps

## Freshness

`score_freshness` is independent from authority ranking. It uses:

- `observed_at`
- `loaded_at`
- optional `effective_at`
- source update cadence
- source declared latency
- entity activity pattern
- field volatility
- explicit `as_of`

The explicit `as_of` keeps scoring deterministic. A stale authoritative source
can still win selection, but the comparison emits a `stale-value` gap when a
fresher lower-authority conflicting value exists.

## Field Comparison

`compare_field_candidates` produces the field comparison matrix:

- normalized equivalence groups
- authority rank by source
- freshness rank by source
- conflict flag
- consensus flag
- selected canonical value
- reversible selected source link
- rejected candidates with reasons
- typed gaps
- stable comparison digest

Selection rule:

1. No present candidate: emit `missing-required` when required.
2. No authority evidence: emit `policy-missing`, select nothing.
3. Missing highest-ranked source: emit `missing-authoritative`, select nothing.
4. Conflicting highest-ranked values: emit `conflicting-values` and
   `manual-review-pending`, select nothing.
5. One highest-ranked normalized value: select it and link it back to source.
6. If that selected value is stale while a lower-authority conflicting value is
   fresher, emit `stale-value` without silently overriding authority.

## Hierarchy And Flattening Signals

`build_hierarchy_signal` captures:

- parent-child
- ultimate parent
- rollup eligibility
- alias/DBA
- branch/headquarters
- site/mailing address
- account stewardship
- asset containment
- flattened parent

Flattening evidence can carry hierarchy depth, alternate parent candidates,
flattening logic, flattening authority, and information lost.

## Migration Needs

No migration was added in this phase.

Durable MDM authority still needs separately owned storage/CQRS work if identity
clusters, field comparisons, authority evidence, hierarchy signals, typed gaps,
or MDM resolution packets must become queryable Postgres-backed authority.
