# Closeout: provider_routing packet

Date: 2026-04-28
Job: `Verify and resolve provider_routing packet`
Bug in scope: `BUG-96F12329 [P2/WIRING]`

## Verification Result
- `FAILED`
- The bug was left open.
- No `attach_evidence` or `resolve` bug-state call could be executed from this session because the required workflow bug surface was not available in the exposed tool catalog.

## Narrowest Meaningful Verifier
The smallest verifier that could be run here was to confirm whether the workflow session exposed the required bug-state entrypoints.

### Commands Run
```bash
praxis workflow tools list
praxis workflow tools search attach_evidence
praxis workflow tools search resolve
praxis workflow bugs --help
praxis query "What are the available bug state tools and the current status for BUG-96F12329?"
```

### Proof
- `praxis workflow tools list` returned only:
  - `praxis_discover`
  - `praxis_health`
  - `praxis_integration`
  - `praxis_recall`
  - `praxis_orient`
  - `praxis_query`
  - `praxis_context_shard`
  - `praxis_submit_code_change`
  - `praxis_get_submission`
  - `praxis_workflow_validate`
- `praxis workflow tools search attach_evidence` reported: `no tools matched 'attach_evidence'`
- `praxis workflow tools search resolve` matched only `praxis_workflow_validate` and unrelated router tools, not a bug-state resolver
- `praxis workflow bugs --help` failed with: `Tool not allowed: praxis_bugs`
- `praxis query ...` failed with: `Tool cannot prove workflow shard enforcement yet: praxis_query`

## Status Outcome
- `BUG-96F12329 [P2/WIRING]`: `OPEN`
- Reason: verification could not reach the required bug-state surface, so there is no proof that the launcher routing issue is resolved.

## Unresolved Risks
- The global Praxis launcher may still route outside the active workspace.
- Repo-local fixes may still be hidden behind the wrong authority binding.
- No evidence could be attached through the mandated bug-state path in this session.

## Notes
- `PLAN.md` and `EXECUTION.md` remained unchanged.
- The packet remained documentation-only; no launcher, routing, provider, or test code was modified.
