# Native Recovery Troubleshooting

- If schema readiness fails, reapply the workflow schema through the canonical CLI front door.
- If smoke contract loading fails, verify the seeded row for `workflow_definition.native_self_hosted_smoke.v1` exists in `workflow_definitions`.
- If execution stalls before terminal success, inspect `workflow_runs.current_state`, canonical receipts, and the last outbox envelope before changing code.
- If final proof is missing, the system is not healthy even if submit succeeded.
