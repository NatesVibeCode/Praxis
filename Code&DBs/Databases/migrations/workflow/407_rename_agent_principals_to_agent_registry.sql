-- Migration 407: Rename agent_principals → agent_registry to align with
-- planned A2A vocabulary (roadmap_item.a2a.native.stateful.server.for
-- .agentic.work.agent.registry.and.routing.control.plane).
--
-- agent_principal_ref column name is preserved as the durable identifier;
-- only the table name changes. Dependent FK columns in agent_wakes,
-- agent_delegations, and agent_tool_gaps automatically follow the
-- renamed table (PostgreSQL updates the FK target on RENAME TO).
--
-- Catalog rows are updated to point at the new table name. Operation
-- names like agent_principal.register are preserved as the public
-- catalog-API contract — they refer to the agent_principal concept,
-- which is the row identity, not the table name.

BEGIN;

ALTER TABLE IF EXISTS agent_principals RENAME TO agent_registry;

-- Update authority_object_registry to point at the new table name.
UPDATE authority_object_registry
   SET object_ref = 'table.public.agent_registry',
       object_name = 'agent_registry',
       data_dictionary_object_kind = 'agent_registry',
       updated_at = now()
 WHERE object_ref = 'table.public.agent_principals';

-- Rename the data_dictionary entry from agent_principals → agent_registry.
UPDATE data_dictionary_objects
   SET object_kind = 'agent_registry',
       label = 'Agent registry',
       summary = 'Durable LLM-actor identities. Renamed from agent_principals in migration 407 to align with the planned A2A authority vocabulary. Bind a status, scope envelope, integration set, capability set, allowed tools, default conversation, network policy, and standing-order keys. Wakes and delegations reference rows in this table.',
       metadata = metadata || '{"renamed_from":"agent_principals","renamed_in":"407_rename_agent_principals_to_agent_registry.sql"}'::jsonb,
       updated_at = now()
 WHERE object_kind = 'agent_principals';

INSERT INTO data_dictionary_objects (
    object_kind, label, category, summary, origin_ref, metadata
) VALUES (
    'agent_registry.rename_history',
    'agent_registry rename history',
    'definition',
    'Records the agent_principals → agent_registry rename in migration 407, anchoring forward references to the planned A2A authority.',
    '{"migration":"407_rename_agent_principals_to_agent_registry.sql"}'::jsonb,
    '{"renamed_from":"agent_principals","renamed_to":"agent_registry"}'::jsonb
)
ON CONFLICT (object_kind) DO UPDATE SET
    summary = EXCLUDED.summary,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;
