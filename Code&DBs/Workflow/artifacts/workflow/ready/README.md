# ready/ — staged workflow specs

Every `*.queue.json` here is a spec that's ready to run. The launcher
(`Code&DBs/Workflow/scripts/fire_ready_specs.sh`) fans out due staged specs
through `praxis workflow run`, logs each run to `.logs/`, and records the
lifecycle in the `workflow_spec_ready` Postgres table.

Run all examples from the **Praxis repo root** (the parent of `Code&DBs/`).

## Fire everything now

```bash
bash "Code&DBs/Workflow/scripts/fire_ready_specs.sh"
```

Runs in parallel. Set `SEQUENTIAL=1` to run one at a time.

## Fire later (launchd or `at`)

```bash
# e.g. fire at 3am (repo root):
echo 'bash "Code&DBs/Workflow/scripts/fire_ready_specs.sh"' | at 0300
```

## Staging more specs

Drop any `*.queue.json` into this directory. Next launcher run picks it up.
To mark a spec as ready with an optional scheduled_at:

```sql
INSERT INTO workflow_spec_ready (spec_id, spec_path, scheduled_at)
VALUES ('my-spec', 'Code&DBs/Workflow/artifacts/workflow/ready/my_spec.queue.json', '2026-04-19 03:00+00');
```

The launcher now honors `scheduled_at`: rows stay staged until they are due,
then the script fires them in order.

## Inspect

The launcher and `psql` must use the **same** workflow DB as `WORKFLOW_DATABASE_URL`
(resolver: `source scripts/_workflow_env.sh && workflow_load_repo_env` from repo root).
Do **not** use ad hoc `localhost` DSNs; they are not portable and may not match
your active authority.

```bash
. ./scripts/_workflow_env.sh && workflow_load_repo_env
psql "$WORKFLOW_DATABASE_URL" -c "SELECT spec_id, status, scheduled_at, run_id, fired_at, created_at FROM workflow_spec_ready ORDER BY created_at DESC"
```

Or rely on the summary block printed at the end of `fire_ready_specs.sh` (it runs the same query shape against `"$DB"` = `"$WORKFLOW_DATABASE_URL"`).

Logs for each spec land in `artifacts/workflow/ready/.logs/<spec_id>.log` (repo-relative).
