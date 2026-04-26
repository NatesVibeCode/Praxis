# cascade/specs — Historical Run Archives

These `.queue.json` files are **historical records** of past workflow runs, not live templates. The `agent` field in each job records which model actually executed that position at the time of the run. These slugs are facts about past execution, not active routing instructions.

**Do not re-run these specs without catalog validation.** An `agent` slug that was valid when the spec ran may now point to a blocked or deprecated route. Before re-running any spec, verify every `agent` value against the live provider catalog:

```bash
praxis workflow tools call praxis_circuits --input-json '{}'
praxis workflow query "effective provider catalog"
```

If a slug is no longer catalog-admitted, update the `agent` field to a current route before launching. Launcher-side catalog validation for `agent` slugs is a planned enforcement (tracked in the provider catalog authority work).
