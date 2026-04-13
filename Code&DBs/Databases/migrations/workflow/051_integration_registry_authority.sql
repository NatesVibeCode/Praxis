BEGIN;

INSERT INTO integration_registry (
    id,
    name,
    description,
    provider,
    capabilities,
    auth_status,
    icon,
    mcp_server_id
) VALUES
    (
        'dag-dispatch',
        'DAG Dispatch',
        'Submit workflow jobs, inspect status, and search receipts.',
        'dag',
        '[
          {"action":"dispatch_job","description":"Submit a workflow job to the DAG dispatch runtime."},
          {"action":"check_status","description":"Inspect the status of an existing workflow run."},
          {"action":"search_receipts","description":"Search historical workflow runs and receipts."}
        ]'::jsonb,
        'connected',
        'bolt',
        NULL
    ),
    (
        'notifications',
        'Notifications',
        'Send notification messages through the platform notification channel.',
        'dag',
        '[
          {"action":"send","description":"Send a notification message."}
        ]'::jsonb,
        'connected',
        'bell',
        NULL
    ),
    (
        'webhook',
        'Webhook',
        'Post structured payloads to external HTTP endpoints.',
        'http',
        '[
          {"action":"post","description":"POST a payload to an external HTTP endpoint."}
        ]'::jsonb,
        'connected',
        'webhook',
        NULL
    ),
    (
        'workflow',
        'Workflow',
        'Invoke registered workflows from the runtime control plane.',
        'dag',
        '[
          {"action":"invoke","description":"Invoke a registered workflow by workflow id."}
        ]'::jsonb,
        'connected',
        'workflow',
        NULL
    )
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    provider = EXCLUDED.provider,
    capabilities = EXCLUDED.capabilities,
    auth_status = EXCLUDED.auth_status,
    icon = EXCLUDED.icon,
    mcp_server_id = EXCLUDED.mcp_server_id;

COMMIT;
