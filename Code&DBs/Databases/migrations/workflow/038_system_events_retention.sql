-- Add index on created_at if missing (check pg_indexes first)
CREATE INDEX IF NOT EXISTS idx_system_events_created_at ON system_events (created_at);

-- Retention cleanup function: delete events older than 30 days
CREATE OR REPLACE FUNCTION cleanup_system_events(retain_days INT DEFAULT 30)
RETURNS INT LANGUAGE plpgsql AS $$
DECLARE
  deleted_count INT;
BEGIN
  DELETE FROM system_events
  WHERE created_at < NOW() - (retain_days || ' days')::INTERVAL;
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  RETURN deleted_count;
END;
$$;
