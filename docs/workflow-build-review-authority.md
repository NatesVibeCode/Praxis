# Workflow Build Review Authority

This note captures the current architectural decisions for the workflow build
review decisions table and the code that projects from it.

Cross-cutting architecture policy about decision-table design belongs in
`operator_decisions`, not here. This document is only about workflow-build
review authority.

## Core decisions

- `workflow_build_review_decisions` is the DB-native authority for build-review
  decisions. Approval and rejection truth lives in rows, not in transient
  Python state, scripts, or UI-only payloads.
- The table is append-only provenance. We keep a visible history of what was
  decided, by whom, under which approval mode, and why.
- The table is not a generic planner-state bucket. Candidate manifests, build
  graphs, review sessions, and other projections can be regenerated from the
  workflow definition plus the latest decision rows.
- Decisions are revision-scoped. A row applies to a specific
  `workflow_id + definition_revision + target scope` and does not silently
  carry over when the definition changes.
- Derived read models are allowed, but only as projections. If a cached build
  payload disagrees with `workflow_build_review_decisions`, the table wins.
- New runtime behavior should read and write through the review authority
  repository/runtime seams, not by inventing ad hoc scripts or side tables that
  compete with the decision rows.

## Practical implications

- `runtime/build_review_decisions.py` owns review-decision semantics.
- `runtime/build_planning_contract.py` is a projector over build state plus
  review-decision authority; it should not become a second planner authority.
- Any future "current review state" table or workflow projection must be
  explicitly derived from `workflow_build_review_decisions`, never treated as
  an independent source of truth.
