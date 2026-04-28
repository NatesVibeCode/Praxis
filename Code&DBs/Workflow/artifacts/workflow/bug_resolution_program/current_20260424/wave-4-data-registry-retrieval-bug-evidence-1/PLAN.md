# Wave 4 Data Registry Retrieval Bug Evidence Plan

## Authority Model

1. Canonical authority for bug state, receipt provenance, and verification evidence is the Postgres-backed workflow/bug tracker and receipt store exposed through `praxis_bugs`, `praxis_receipts`, and related workflow status tools.
2. Runtime, storage, observability, subscriptions, and surface-level dashboards are consumers of that authority, not independent sources of truth.
3. Any JSON receipt directory on disk is a legacy or shadow representation only. It must not be read as authoritative when DB-backed receipt authority is available.
4. Evidence for resolution must come from canonical bug packets, receipt records, verification runs, and replay/provenance state from the workflow toolchain.
5. The packet should treat authority conflicts as a defect to isolate, not as a reason to broaden the scope of the fix.

## Files To Read

Read first for context and evidence mapping:

1. The bug packets and history for:
   - `BUG-32E01522`
   - `BUG-121B5049`
   - `BUG-4BBE6C43`
   - `BUG-79695B46`
   - `BUG-A75BC81E`
2. Any workflow receipts or verification runs linked from those packets.
3. The code paths that own:
   - event and receipt projection authority
   - evidence/proof pipeline construction
   - observability hub and dashboard receipt loading
   - post-receipt hooks and ledger/evidence-link propagation
   - task wiring health checks and contract enforcement
4. Any tests or fixtures that already assert DB-backed receipt authority, replay provenance, or verification proof generation.

If the repository snapshot exposes concrete paths, prefer reading the smallest set of direct owners first, then expand only where the packets point.

## Files Allowed To Change

For this packet, no code files should be changed.

For the later implementation wave, limit edits to the minimal set of owners that remove split authority and redirect consumers to canonical DB-backed receipt and verification sources. That means:

1. Authority-owning runtime/storage projection code.
2. Observability readers that still walk receipt JSON directories.
3. Receipt evidence/proof pipeline code.
4. Post-receipt hook handling where failures are swallowed or demoted.
5. Tests that prove the authority boundary and regression behavior.

Do not touch unrelated UI, documentation, or refactor-only files unless they are the direct owner of one of the issues above.

## Verification Path

1. Confirm the canonical bug packets identify a single owning authority for each data flow.
2. Verify that observability and dashboard reads no longer consult JSON receipt directories beside the DB receipt authority.
3. Verify that the evidence/proof pipeline sources receipt and verification proof from the same canonical store used by runtime and storage.
4. Verify post-receipt hooks surface ledger and evidence-link failures instead of swallowing them.
5. Verify task wiring health still passes under the canonical authority model and no unresolved contract remains.
6. Re-run the relevant targeted tests or replay/verification checks associated with each bug before any resolution attempt.

## Stop Boundary

Stop when:

1. The authority model is explicit and localized.
2. The required owner files and regression tests are identified.
3. The exact later edit set is narrow enough to preserve the canonical authority boundary.

Do not proceed into broad architectural refactoring, unrelated cleanup, or speculative migrations once the authority split is clearly mapped.

## Per-Bug Intended Outcome

### BUG-32E01522

Event and receipt projection authority should be reduced to one canonical source, with runtime, storage, observability, subscriptions, and surface consumers aligned behind it.

### BUG-121B5049

The `maintain_wiring_health` task contract should be resolved by identifying the unresolved owner path and restoring a provable, single-authority wiring health check.

### BUG-4BBE6C43

The observability hub and dashboard should stop reading receipt JSON directories beside DB receipt authority and should consume canonical DB-backed receipt data only.

### BUG-79695B46

The receipts evidence and verification proof pipeline should be refactored so runtime, storage, and observability all derive proof from the same canonical receipt authority.

### BUG-A75BC81E

Post-receipt hooks should fail loudly on ledger and evidence-link errors instead of swallowing them, so downstream evidence stays trustworthy.

