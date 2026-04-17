-- Migration 148: retire legacy workflow_notifications storage
--
-- Notification delivery is now projected from canonical receipts and wakeups
-- come from pg_notify/event_log. The workflow_notifications table is dead.

DROP TABLE IF EXISTS workflow_notifications;
