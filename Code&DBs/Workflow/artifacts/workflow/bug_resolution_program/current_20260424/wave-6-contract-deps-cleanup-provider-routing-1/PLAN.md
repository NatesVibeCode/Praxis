# Wave 6 Contract/Deps Cleanup: Provider Routing Plan

## Authority Model

- Bug tracker records for `BUG-9ACEE016` and `BUG-D92E6B38` are the scope authority for this packet.
- The execution shard `workflow_331cdab96c86` is the workflow authority for read/write boundaries and submission.
- Repository source is the implementation authority, but this job is planning only and must not change code.
- Verification authority is the focused test path listed below; do not broaden scope into unrelated CLI or adapter behavior.

## Files To Read

- `adapters/provider_types.py`
- The modern workflow front door module that owns core command dispatch.
- The legacy `workflow_cli` module or shim that the front door currently routes through.
- Focused tests covering provider adapter contracts and command routing.
- Any local helper modules imported directly by those files, but only if needed to understand the dispatch or contract round-trip path.

## Files Allowed To Change

- None in this planning job.
- For the follow-on execution packet, only the provider adapter contract file, the modern workflow front door routing file, the legacy compatibility shim if required, and the narrow tests that prove the two fixes.

## Verification Path

- Add or update a targeted regression test that proves `ProviderAdapterContract` can round-trip through `to_contract` and back without losing information relevant to the contract surface.
- Add or update a targeted routing test that proves modern workflow core commands dispatch directly through the intended front door path instead of the legacy `workflow_cli` route.
- Run only the smallest test slice that covers those two behaviors.
- If a test exposes an unexpected dependency, stop and widen only the local helper reads needed to make the fix safe.

## Stop Boundary

- Stop once the plan is sufficient to implement both fixes without guessing at hidden dispatch layers.
- Do not expand into unrelated workflow commands, unrelated adapter types, or cleanup refactors outside the two bugs.
- Do not change public behavior beyond the inverse contract path and the core-command routing correction.
- This packet intentionally stops at planning; implementation belongs to the follow-on execution job.

## Per-Bug Intended Outcome

### BUG-9ACEE016

- Add the missing inverse `to_contract` round-trip for `ProviderAdapterContract`.
- Preserve the existing adapter contract shape so contract-drift is removed without changing unrelated fields.
- Prove the inverse path with a targeted regression test.

### BUG-D92E6B38

- Rewire the modern workflow front door so core commands no longer transit the legacy `workflow_cli` path.
- Preserve any required legacy compatibility only where the front door still needs an explicit shim.
- Prove the routing change with a narrow dispatch test that asserts the modern path is the one in use.
