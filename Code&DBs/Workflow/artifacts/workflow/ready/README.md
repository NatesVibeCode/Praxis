# ready/ — staged workflow specs

Every `*.queue.json` here is a spec that's ready to run. The launcher
(`Code&DBs/Workflow/scripts/fire_ready_specs.sh`) fans out due staged specs
through `praxis workflow run`, logs each run to `.logs/`, and records the
lifecycle in the `workflow_spec_ready` Postgres table.

## Fire everything now

From the Praxis repository root (so `Code&DBs/` resolves):

```bash
bash "Code&DBs/Workflow/scripts/fire_ready_specs.sh"
```

Runs in parallel. Set `SEQUENTIAL=1` to run one at a time.

## Fire later (launchd or `at`)

```bash
# e.g. fire at 3am:
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

Database authority is resolved through `WORKFLOW_DATABASE_URL`; do not bake a
machine-local DSN into ready specs or launcher docs.

## Inspect

```bash
./scripts/praxis db query "SELECT * FROM workflow_spec_ready ORDER BY created_at DESC"
```

Logs for each spec land in `.logs/<spec_id>.log`.
