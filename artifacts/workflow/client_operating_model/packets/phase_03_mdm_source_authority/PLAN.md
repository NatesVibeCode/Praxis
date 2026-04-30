# Phase 3 Build Packet: MDM Identity, Normalization, Lineage, and Source Authority

## Objective

Deliver a bounded Phase 3 design and validation packet for master data management that establishes:

- identity clustering across records and systems
- reversible record links back to source evidence
- deterministic normalization rules
- hierarchy and flattening signals
- field-level comparison outputs
- freshness scoring
- source authority evidence and decisioning
- typed gap capture
- repeatable tests and validation criteria

This packet defines what must be built and proven. It does not prescribe implementation language, tooling, or code structure.

## Scope

### In Scope

- Person, organization, account, location, and asset identity handling
- Candidate match, cluster, and merge recommendation outputs
- Canonical field normalization specifications
- Cross-source field comparison and conflict surfacing
- Record lineage from canonical view back to source rows or events
- Source authority scoring inputs and evidence retention
- Freshness scoring at record and field level
- Typed gap detection for missing, conflicting, stale, and ambiguous data
- Test fixtures, validation checks, and acceptance gates

### Out of Scope

- Production UI design
- Final merge automation without review
- New source onboarding beyond the minimum fixture set needed for validation
- Policy approval workflows outside the data decision record
- Runtime infrastructure, deployment, and orchestration specifics

## Bounded Deliverables

1. Identity cluster specification
2. Reversible lineage and link model
3. Normalization rule catalog
4. Hierarchy and flattening signal model
5. Field comparison matrix specification
6. Freshness scoring framework
7. Source authority evidence model
8. Typed gap taxonomy
9. Test fixture set and expected outcomes
10. Validation checklist with acceptance criteria

## Core Artifacts

Produce the following artifacts as documents, schemas, or tabular specs:

| Artifact | Purpose |
| --- | --- |
| `identity_cluster_spec` | Defines entity types, match signals, cluster states, and review triggers |
| `reversible_link_spec` | Defines how canonical records link back to exact source records and values |
| `normalization_rules_catalog` | Lists field rules, transforms, allowed values, and reversibility constraints |
| `hierarchy_signal_spec` | Captures parent-child, rollup, alias, and flattening indicators |
| `field_comparison_matrix` | Compares source values by field, confidence, freshness, and authority |
| `freshness_scoring_spec` | Defines scoring inputs, decay logic, and override handling |
| `source_authority_register` | Records field-level authority ranking and supporting evidence |
| `typed_gap_taxonomy` | Defines gap types, severity, remediation owner, and closure conditions |
| `validation_pack` | Test cases, fixtures, expected outcomes, and sign-off criteria |

## Entity Model

Phase 3 must support at minimum these entity classes:

- `person`
- `organization`
- `account`
- `location`
- `asset`

Each entity must define:

- canonical identifier format
- source identifier mapping rules
- required identity attributes
- optional enrichment attributes
- prohibited merge conditions
- hierarchy participation rules

## Identity Clusters

### Required Outputs

Each candidate entity cluster must expose:

- `cluster_id`
- `entity_type`
- `member_records[]`
- `match_signals[]`
- `anti_match_signals[]`
- `cluster_confidence`
- `review_status`
- `canonical_candidate`
- `created_at`
- `updated_at`

### Cluster States

Allowed cluster states:

- `proposed`
- `auto-accepted`
- `review-required`
- `rejected`
- `split-required`

### Match Signal Classes

Identity logic must distinguish the following signal classes:

- exact identifiers
- strong quasi-identifiers
- weak descriptive similarities
- relational context
- temporal consistency
- source provenance confidence

Examples:

| Signal Class | Examples |
| --- | --- |
| Exact identifiers | tax ID, employee ID, source GUID, device serial |
| Strong quasi-identifiers | legal name plus DOB, registered org name plus jurisdiction |
| Weak descriptive similarities | nickname, formatted address similarity, normalized phone match |
| Relational context | same parent org, same owner account, same site |
| Temporal consistency | active periods overlap plausibly, impossible date conflicts blocked |
| Source provenance confidence | trusted registry vs user-entered free text |

### Anti-Match Signals

Anti-match logic is mandatory. At minimum include:

- mutually exclusive official identifiers
- impossible temporal overlap
- contradictory legal entity type
- known deprecation or reassignment of identifiers
- hard conflict in parentage where parentage is authoritative

### Cluster Acceptance Rules

Phase 3 must define:

- auto-accept threshold
- manual review band
- auto-reject threshold
- split triggers when a cluster contains mutually inconsistent members

All thresholds must be explicit and testable.

## Reversible Links and Lineage

Every canonical field value must be traceable to its source evidence.

### Required Lineage Capabilities

- canonical record to source record link
- canonical field to source field link
- source value preservation before normalization
- transform history for each normalized value
- decision reason for selected canonical value
- timestamped evidence of when the source value was observed

### Required Link Fields

- `canonical_record_id`
- `canonical_field`
- `source_system`
- `source_record_id`
- `source_field`
- `source_value_raw`
- `source_value_normalized`
- `transform_chain[]`
- `selection_reason`
- `authority_basis`
- `observed_at`
- `loaded_at`

### Reversibility Rule

Normalization may transform values for comparison and canonicalization, but the original source value must remain recoverable for audit, replay, and dispute resolution.

## Normalization Rules

Normalization must be deterministic and cataloged by field.

### Rule Categories

- whitespace and punctuation normalization
- case normalization
- Unicode handling and transliteration policy
- date and timestamp standardization
- timezone handling
- phone, email, and URL standardization
- name parsing and formatting
- address component normalization
- legal entity suffix handling
- code set mapping
- boolean and enum coercion
- unit normalization

### Rule Record Requirements

Each rule entry must include:

- `field_name`
- `entity_type`
- `input_pattern`
- `normalization_steps[]`
- `output_type`
- `reversible`
- `loss_risk`
- `exception_policy`
- `test_examples[]`

### Normalization Constraints

- Do not collapse semantically distinct values just because they look similar.
- Lossy transforms must be flagged and justified.
- Locale-sensitive rules must declare locale assumptions.
- If a field cannot be safely normalized, preserve raw value and emit a typed gap or review flag.

## Hierarchy and Flattening Signals

Phase 3 must capture both explicit and inferred structure.

### Required Hierarchy Signals

- parent-child relationship
- ultimate parent indicator
- rollup eligibility
- alias or DBA relationship
- branch vs headquarters distinction
- site vs mailing address distinction
- account ownership and stewardship relationship
- asset containment or assignment relationship

### Flattening Signals

Where downstream systems require flattened views, preserve:

- source hierarchy depth
- flattened parent selection logic
- alternate parent candidates
- whether flattening is authoritative or convenience-only
- information lost during flattening

### Hierarchy Risk Flags

- cyclic parentage
- competing parents from authoritative sources
- missing root
- multiple active primaries
- stale parentage

## Field Comparison

Generate a field-level comparison model across all cluster members.

### Required Comparison Outputs

- per-field value set across sources
- normalized equivalence grouping
- authority rank by source
- freshness rank by observation date
- conflict flag
- consensus flag
- selected canonical value
- rejected candidate values with reasons

### Comparison Categories

- exact agreement
- normalized agreement
- compatible variation
- unresolved conflict
- missing in some sources
- stale authoritative value vs fresher lower-authority value

### Minimum Fields for Comparison

Define comparisons at least for:

- names
- identifiers
- addresses
- contact methods
- status
- parent references
- effective dates
- ownership or steward fields

## Freshness Scoring

Freshness scoring must work independently from authority ranking and then combine with it for final selection.

### Required Inputs

- `observed_at`
- `effective_at`
- `loaded_at`
- source update cadence
- source declared SLA or latency
- entity activity pattern
- field volatility classification

### Field Volatility Classes

At minimum:

- `stable`
- `moderate-change`
- `high-change`

### Freshness Output

For each field value candidate produce:

- `freshness_score`
- `staleness_band`
- `decay_basis`
- `override_reason` if manual or policy override applies

### Freshness Constraints

- A highly authoritative but stale source must not silently dominate recent evidence.
- A fresh low-authority source must not silently override controlled registries without explicit policy.
- Final selection must show the tradeoff between freshness and authority.

## Source Authority Evidence

Source authority must be field-aware, evidence-backed, and reviewable.

### Authority Model

Authority ranking must be definable by:

- entity type
- field
- jurisdiction or business domain
- source system
- collection mechanism
- certification or stewardship status

### Required Evidence Fields

- `source_system`
- `field_name`
- `authority_rank`
- `authority_scope`
- `authority_reason`
- `evidence_type`
- `evidence_reference`
- `approved_by`
- `approved_at`
- `review_interval`

### Evidence Types

- policy decision
- contractual source-of-record designation
- system stewardship assignment
- regulatory registry designation
- operational reliability evidence
- historical accuracy evidence

### Authority Decision Rules

The packet must explicitly define:

- when field-level authority overrides source-level default ranking
- when manual adjudication is mandatory
- when no source is authoritative and the field remains unresolved

## Typed Gaps

Gap capture must be typed, actionable, and measurable.

### Required Gap Types

- `missing-required`
- `missing-authoritative`
- `conflicting-values`
- `unverifiable-identity`
- `stale-value`
- `ambiguous-hierarchy`
- `non-normalizable`
- `broken-lineage`
- `policy-missing`
- `manual-review-pending`

### Required Gap Fields

- `gap_id`
- `entity_type`
- `canonical_record_id` or candidate cluster reference
- `field_name`
- `gap_type`
- `severity`
- `detected_at`
- `owner_role`
- `remediation_path`
- `closure_condition`

### Severity Bands

- `critical`
- `high`
- `medium`
- `low`

## Test Strategy

Testing must use controlled fixtures and explicit expected outputs.

### Fixture Set

Create fixtures that include:

- clean exact matches across sources
- near-duplicate entities with formatting differences
- ambiguous matches that require review
- hard non-matches with deceptive similarities
- stale authoritative vs fresh non-authoritative conflicts
- hierarchy conflicts
- lossy normalization edge cases
- lineage break scenarios
- missing policy scenarios

### Required Test Types

- normalization unit tests
- identity clustering tests
- anti-match tests
- field comparison tests
- freshness scoring tests
- source authority decision tests
- typed gap emission tests
- lineage reversibility tests
- end-to-end validation tests across a multi-source fixture

### Minimum Assertions

Tests must verify:

- exact expected cluster membership
- correct review routing
- canonical selection rationale is populated
- raw source values remain recoverable
- normalization outputs match catalog
- conflicts and gaps are emitted with correct types
- freshness and authority tradeoffs behave as specified

## Validation Checklist

Phase 3 is valid only if all items below pass:

| Validation Gate | Pass Condition |
| --- | --- |
| Identity determinism | Same inputs produce same cluster and comparison outputs |
| Auditability | Every canonical field traces to source evidence and transform history |
| Reversibility | Raw source value can be reconstructed or directly retrieved |
| Authority evidence | Every authority rank is backed by recorded evidence |
| Freshness transparency | Freshness score and staleness band are visible and explainable |
| Gap typing | Every unresolved issue is emitted as a typed gap |
| Hierarchy integrity | Cycles, competing parents, and flattening loss are detectable |
| Test coverage | All required fixture scenarios exist with expected outcomes |

## Acceptance Criteria

Phase 3 is complete when:

1. All bounded deliverables exist and are internally consistent.
2. Each in-scope entity type has explicit identity, normalization, comparison, and authority rules.
3. Canonical value selection is explainable at the field level using authority, freshness, and evidence.
4. Reversible lineage is specified for every canonical field.
5. Typed gaps cover every unresolved or policy-blocked state.
6. Validation fixtures demonstrate both successful resolution and controlled failure modes.
7. No part of the packet depends on undocumented manual judgment.

## Risks and Controls

| Risk | Control |
| --- | --- |
| Over-merging distinct entities | Hard anti-match rules, review band, split-required state |
| Under-merging duplicates | Quasi-identifier and relational signals with calibrated thresholds |
| Hidden lossy normalization | Reversibility field, loss-risk flag, edge-case tests |
| Stale system of record dominance | Freshness scoring and explicit authority-vs-recency rule |
| Undocumented source ranking | Evidence-backed authority register |
| Flattening destroys parentage meaning | Preserve hierarchy depth and flattening-loss signals |
| Audit failure | Required lineage fields and validation gate |

## Suggested Build Sequence

1. Define entity classes and required fields.
2. Publish normalization catalog with reversibility annotations.
3. Define identity signals, anti-signals, and cluster thresholds.
4. Define field comparison outputs and canonical selection rationale.
5. Define freshness scoring and authority evidence models.
6. Add hierarchy and flattening signals.
7. Add typed gap taxonomy.
8. Build fixture set and validation checklist.
9. Run document review to confirm no unresolved policy gaps remain.

## Dependencies

Phase 3 depends on:

- agreed entity inventory
- source inventory with source identifiers
- field dictionary and value domains
- policy owners for source authority decisions
- fixture data representing at least two conflicting sources per key entity type

## Exit Evidence

The packet should conclude with a sign-off bundle containing:

- approved source authority register
- approved normalization catalog
- approved fixture pack
- validation results against all required scenarios
- explicit list of open gaps, if any, with owners and dates
