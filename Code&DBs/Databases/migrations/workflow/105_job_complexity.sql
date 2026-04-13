-- Add complexity field to workflow_jobs.
-- Spec authors declare step complexity ("low", "moderate", "high").
-- "low" maps to prefer_cost=True in the router, shifting composite
-- scoring weights toward cheaper models.
ALTER TABLE workflow_jobs
  ADD COLUMN IF NOT EXISTS complexity TEXT NOT NULL DEFAULT 'moderate';
