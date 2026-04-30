# Task Environment Contracts

Date: 2026-04-30

## Verdict

Phase 4 now has a deterministic task-environment contract substrate. It is pure
domain code: no operation handlers, no migrations, no orchestration side
effects. The authority model is explicit:

- hierarchy resolves the business path and accountability
- task contracts mirror the resolved owner/steward and declare execution bounds
- inherited policy bounds may be narrowed, not broadened
- staleness turns changed dependencies into a typed execution decision
- revisions are append-only chains with exact predecessor links

## Implemented Primitives

- `HierarchyNode` and `HierarchyPath` resolve one active root-to-task path.
- `ResolvedResponsibility` compares hierarchy owner/steward authority with the
  contract mirror fields.
- `SopReference` and `SopGap` enforce either active SOP coverage or an explicit,
  approved, expiring gap.
- `AllowedTool`, `ScopeGrant`, `ModelPolicy`, and `VerifierReference` describe
  least-privilege tool, read, write, model, and verifier policy.
- `ContractPolicyBounds` carries inherited parent constraints so child contracts
  can be checked for unauthorized broadening.
- `StalenessPolicy`, `StalenessSignal`, and `StalenessDecision` convert SOP,
  hierarchy, tool, scope, model, verifier, and review-interval changes into
  deterministic freshness outcomes.
- `RevisionRecord`, `validate_append_only_revision_chain`, and
  `validate_next_revision` enforce linear append-only revision semantics.
- `ContractInvalidState` is the common invalid-state envelope with stable
  `reason_code`, `field_ref`, evidence refs, and details.

## Enforcement Shape

The entry point is `validate_task_environment_contract(contract, context)`.
It produces a `ContractEvaluationResult` with:

- `ok`
- `status`
- typed invalid states
- warnings
- resolved hierarchy path
- resolved responsibility
- staleness decision
- optional revision-chain check

This keeps pre-execution gating queryable without pretending this phase owns
runtime launch, database persistence, or UI surfaces.

## Migration Need

No migration was added in this worker scope. A later phase should persist these
contracts as append-only rows and register query/command operations through the
CQRS gateway if the runtime needs first-class task contract materialization.

