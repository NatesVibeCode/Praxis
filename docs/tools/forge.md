# Forge Tools — Previewing Platform Extensions

The Forge tools are read-only previews that show you the correct path before you commit to creating new platform infrastructure. They never create anything themselves; they return the validated payload you'd use if you proceed.

**Tools in this family:**
- `praxis_authority_domain_forge` — preview a new authority boundary
- `praxis_operation_forge` — preview a new CQRS operation

Think of Forge as the compiler's preflight check for platform extensions: run it first, read what it tells you, then decide whether to proceed.

## What "forge" means

Forge tools exist because the two most common platform-extension mistakes are:

1. **Creating a new authority domain when an existing one covers the use case.** Every domain adds a table, an event stream, and governance overhead. Reusing an existing domain is almost always cheaper.
2. **Adding a new CQRS operation without populating all three required tables.** If `operation_catalog_registry`, `authority_object_registry`, and `data_dictionary_objects` fall out of sync, the gateway silently skips receipts or events for that operation.

Forge prevents both by showing you what already exists, what's missing, and the exact payload to use if you proceed.

---

## `praxis_authority_domain_forge`

**When to use:** you're about to add a new capability that needs a home for durable truth — a new table group, a new event stream, or a new set of operations that don't belong to any existing authority boundary.

**What it returns:**
- Existing domain state (if the domain already exists, you don't need to create it)
- Nearby domains (domains with similar naming or scope — candidates for reuse)
- Attached operations and authority objects
- Missing required inputs
- Reject paths (why the forge would refuse a register call)
- `ok_to_register` — boolean. Only proceed to `praxis_register_authority_domain` when this is `true`.
- `safe_register_payload` — the exact payload to pass to `praxis_register_authority_domain`

**Example:**

```bash
praxis workflow tools call praxis_authority_domain_forge --input-json '{
  "authority_domain_ref": "authority.provider_cache",
  "owner_ref": "praxis.engine",
  "storage_target_ref": "praxis.primary_postgres",
  "decision_ref": "decision.provider_cache.rationale"
}'
```

**Decision checklist before creating a new domain:**

1. Does an existing domain cover this use case? Check `nearby_domains` in the forge output.
2. Is there a `decision_ref` that justifies this boundary? If not, file one first — the register operation requires it.
3. Does `ok_to_register` show `true`? If not, read `reject_paths` and address each.
4. Is the `storage_target_ref` correct? Default is `praxis.primary_postgres`.

**Proceeding to register:**

```bash
praxis workflow tools call praxis_register_authority_domain --input-json '<safe_register_payload from forge output>'
```

`praxis_register_authority_domain` is the write step. It verifies `storage_target_ref` exists, writes through the `authority_domain_register` gateway operation, and emits `authority.domain.registered` on completed command receipts.

---

## `praxis_operation_forge`

**When to use:** you're about to add a new MCP tool or operation and need to know the correct CQRS registration path before hand-building anything.

**What it returns:**
- The registration payload for `operation_catalog_registry`
- The matching rows for `authority_object_registry` and `data_dictionary_objects`
- Real tool binding and API route if the operation already exists
- The migration template (three INSERT statements — one per required table)
- Reject paths if the proposed operation conflicts with existing catalog entries
- `operation_kind` defaults (`query` → `read_only` idempotency, `observe` posture; `command` → `non_idempotent`, `operate` posture)

**Example:**

```bash
praxis workflow tools call praxis_operation_forge --input-json '{
  "operation_name": "provider_cache.read",
  "operation_kind": "query",
  "authority_domain_ref": "authority.provider_cache",
  "handler_ref": "runtime.provider_cache:read_cache",
  "input_model_ref": "runtime.provider_cache:ReadCacheInput"
}'
```

**What the forge output gives you:**

The `migration_template` section contains three SQL INSERT blocks:

```sql
-- 1. operation_catalog_registry
INSERT INTO operation_catalog_registry (
  operation_name, operation_ref, operation_kind, idempotency_policy,
  receipt_required, event_required, handler_ref, input_model_ref, ...
) VALUES (...);

-- 2. authority_object_registry
INSERT INTO authority_object_registry (
  object_kind, operation_ref, authority_domain_ref, ...
) VALUES (...);

-- 3. data_dictionary_objects
INSERT INTO data_dictionary_objects (
  name, category, authority_domain_ref, ...
) VALUES (...);
```

Copy these into a new migration file under `Code&DBs/Databases/migrations/workflow/`. All three must be in the same migration for the gateway to function correctly — partial registration causes silent failures.

**For command operations** (`operation_kind: "command"`), also set:
- `event_required: true`
- `event_type: "your.event.type"` — the event name emitted on completed receipts

**Proceeding to build:**

1. Copy the `migration_template` into a new migration file.
2. Implement the handler at `handler_ref`.
3. Create the Pydantic input model at `input_model_ref`.
4. Add the MCP tool wrapper in `surfaces/mcp/tools/<surface>.py` as a thin call to `execute_operation_from_subsystems`.
5. Run migrations and verify with `praxis workflow tools describe <your_new_tool>`.

---

## Example walkthrough: adding a new gateway operation end-to-end

Scenario: adding a `provider_cache.read` query operation.

**Step 1 — Check if an authority domain exists:**

```bash
praxis workflow tools call praxis_authority_domain_forge --input-json '{
  "authority_domain_ref": "authority.provider_cache"
}'
```

Result: `ok_to_register: false`, reason: `nearby_domains` shows `authority.provider_routing` already covers cache state. Decision: reuse `authority.provider_routing`.

**Step 2 — Forge the operation:**

```bash
praxis workflow tools call praxis_operation_forge --input-json '{
  "operation_name": "provider_cache.read",
  "operation_kind": "query",
  "authority_domain_ref": "authority.provider_routing",
  "tool_name": "praxis_provider_cache_read"
}'
```

Forge returns the migration template with all three rows pre-populated.

**Step 3 — Create the migration:**

Copy the `migration_template` into `Code&DBs/Databases/migrations/workflow/NNN_provider_cache_read.sql`. Assign the next available migration number.

**Step 4 — Implement the handler and MCP tool:**

- Handler at the `handler_ref` returned by forge
- Pydantic model at `input_model_ref`
- MCP tool wrapper in `surfaces/mcp/tools/`:

```python
def tool_praxis_provider_cache_read(params: dict) -> dict:
    return execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="provider_cache.read",
        payload=params,
    )
```

**Step 5 — Verify:**

```bash
praxis workflow tools describe praxis_provider_cache_read
```

If the tool appears in the catalog with the correct surface, tier, and schema — the operation is wired.

---

**See also:** [CONCEPTS.md](../CONCEPTS.md) — CQRS gateway and receipts. [OPERATOR_GUIDE.md](../OPERATOR_GUIDE.md) — authority decisions and day-2 operations. [ARCHITECTURE.md](../ARCHITECTURE.md) — the three CQRS tables and migration authority.
