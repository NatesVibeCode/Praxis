-- Investigator handoff: structured JSON on bugs so work can be resumed without re-reading full history.

ALTER TABLE bugs ADD COLUMN IF NOT EXISTS resume_context jsonb NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN bugs.resume_context IS
'Optional JSON handoff for long investigations. Suggested top-level keys: hypothesis (text), verified (text), next_steps (json array of strings), related_bug_ids (json array of strings), blockers (text), last_touched_at (iso8601 text). Patches merge at the top level (jsonb ||); replace whole arrays when updating lists.';
