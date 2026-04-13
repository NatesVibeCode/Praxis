-- Track recent success/failure rates per route so the router can auto-demote
-- providers that keep failing (e.g. CLI timeouts, auth errors).

ALTER TABLE task_type_routing
  ADD COLUMN IF NOT EXISTS recent_successes integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS recent_failures  integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_failure_at  timestamptz,
  ADD COLUMN IF NOT EXISTS last_success_at  timestamptz;
