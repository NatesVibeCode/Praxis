-- Migration 107: Add container resource telemetry to workflow_notifications
--
-- Adds cpu_percent and mem_bytes columns so Docker container resource
-- usage captured during job execution flows through to the SSE stream.

ALTER TABLE workflow_notifications
    ADD COLUMN IF NOT EXISTS cpu_percent REAL,
    ADD COLUMN IF NOT EXISTS mem_bytes BIGINT;
