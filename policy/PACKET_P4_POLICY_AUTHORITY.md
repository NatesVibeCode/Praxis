# Packet P4 — Policy Authority subsystem (DB-side enforcement)

Status: P4.1 shipped (migration safety validator); P4.2 scoped, not built.

## Why

JIT surfacing (Cursor rules, PreToolUse hooks, gateway-side
`_standing_orders_surfaced`) is **advisory**. The agent can ignore it.
P4 closes the loop with **data-layer teeth** so a confused agent — or a
later harness we haven't integrated — can't disable enforcement by
accident or design.

## P4.1 — Migration safety validator (shipped)

`scripts/check-migration-safety.py` rejects migrations that:

- `SET session_replication_role = replica`
- `ALTER TABLE ... DISABLE TRIGGER ...`
- `ALTER TABLE ... DISABLE ROW LEVEL SECURITY`
- `DROP TRIGGER policy_*` / `DROP POLICY policy_*`
- `DELETE FROM operator_decisions`
- `TRUNCATE operator_decisions / authority_operation_receipts / authority_events`

Bypass: same-line `-- safety-bypass: <reason>`. CI surfaces bypass count.
Wired into `.githooks/pre-commit` (staged migrations only) and
`.github/workflows/policy-artifacts.yml` (full scan). 12 unit tests.

## P4.2 — Policy Authority data layer (not built)

### 1. `policy_definitions` table

Projection from `operator_decisions` rows whose `decision_kind =
'architecture_policy'` AND that have an enforcement shape. Hand-authored
projection rule lives in `policy/operator-decision-triggers.json` (same
pattern as the trigger registry today).

Migration sketch:
```sql
CREATE TABLE policy_definitions (
    policy_id text PRIMARY KEY,
    decision_key text NOT NULL REFERENCES operator_decisions(decision_key),
    enforcement_kind text NOT NULL CHECK (enforcement_kind IN
        ('insert_reject','update_reject','delete_reject','update_clamp')),
    target_table text NOT NULL,
    target_column text,
    predicate_sql text NOT NULL,
    rationale text NOT NULL,
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT policy_definitions_window_valid
        CHECK (effective_to IS NULL OR effective_to >= effective_from)
);

CREATE INDEX policy_definitions_target_idx
    ON policy_definitions (target_table, enforcement_kind)
    WHERE effective_to IS NULL;
```

CQRS catalog: `policy_definitions.list` (query, idempotent),
`policy_definitions.upsert_from_operator_decisions` (command,
event_required, event_type=`policy.projected`).

### 2. BEFORE-INSERT/UPDATE triggers (per target table)

For each `target_table` referenced by an active policy_definitions row,
generate a trigger named `policy_<target_table>_enforce` that:

1. Looks up active policies for the table.
2. Evaluates `predicate_sql` against `NEW` (insert/update) or `OLD`
   (delete).
3. On violation: `RAISE EXCEPTION` with the policy's `decision_key` and
   `rationale` so the failure message points the agent at the actual
   standing order.
4. On admit: write a row to `authority_compliance_receipts` (see #3).

Generation, not hand-authored — a Python helper reads policy_definitions
and emits the trigger SQL into a stable migration. Same pattern as the
Cursor rules render.

Initial enforcement targets (start narrow, expand on demand):
- `operator_decisions` — BEFORE DELETE: reject (operator decisions are
  superseded, not deleted; mirror the validator rule at DB level).
- `authority_operation_receipts` — BEFORE DELETE/TRUNCATE: reject.
- `authority_events` — BEFORE DELETE/TRUNCATE: reject.

### 3. `authority_compliance_receipts`

Companion to `authority_operation_receipts`. The receipts table answers
"did this gateway call run?" The compliance receipts table answers "did
this action match a policy, and what did the policy do?"

```sql
CREATE TABLE authority_compliance_receipts (
    receipt_id text PRIMARY KEY,
    policy_id text NOT NULL REFERENCES policy_definitions(policy_id),
    decision_key text NOT NULL,
    target_table text NOT NULL,
    operation text NOT NULL CHECK (operation IN ('INSERT','UPDATE','DELETE')),
    outcome text NOT NULL CHECK (outcome IN ('admit','reject','clamp')),
    subject_pk jsonb,
    rejected_reason text,
    created_at timestamptz NOT NULL DEFAULT now()
);
```

A reject path raises (transaction aborts) — but the trigger emits the
receipt FIRST inside the same transaction, then rolls back via the
exception. We keep the rejection record by writing to a sibling
unlogged-via-dblink path or by structuring the trigger as a STATEMENT-
level AFTER trigger that fires on success only and writing rejections
through the gateway's exception handler. Resolve the trade-off when
building.

### 4. Gateway dispatch

New operations registered in `operation_catalog_registry`:

| operation_name | kind | event_type |
|---|---|---|
| `policy_definitions.list` | query | — |
| `policy_definitions.upsert_from_decisions` | command | `policy.projected` |
| `compliance.list_receipts` | query | — |
| `compliance.replay` | command | — |

Each ships with `input_model_ref` (Pydantic), `handler_ref`, and
`authority_object_registry` + `data_dictionary_objects` rows per the
CQRS-gateway-robust-determinism standing order.

### 5. Acceptance

- A test migration that tries to delete from `operator_decisions` fails
  with the policy's decision_key in the error message.
- A test migration that tries to bypass (e.g. `SET
  session_replication_role = replica`) is rejected by P4.1 before P4.2's
  triggers ever run — defense in depth.
- The compliance receipts table records every reject with the policy_id
  and a non-null `rejected_reason`.
- Existing test suites (`test_fresh_install_seed`, `test_check_migration_safety`)
  stay green.

### 6. Estimated scope

~3 migrations + ~600 LOC Python (projection + trigger generation +
gateway handlers) + 30-50 tests. One careful day, or one focused Codex
session.
