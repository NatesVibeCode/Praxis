-- Integration system hardening: per-connector timeout + call timestamps.

ALTER TABLE connector_registry
  ADD COLUMN IF NOT EXISTS timeout_s INTEGER DEFAULT 30,
  ADD COLUMN IF NOT EXISTS last_call_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_success_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_error_at TIMESTAMPTZ;
