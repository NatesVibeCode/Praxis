-- Cluster discovery helper for roadmap authority rows.
--
-- Usage:
--   source "scripts/_workflow_env.sh" && workflow_load_repo_env
--   psql "$WORKFLOW_DATABASE_URL" -f "artifacts/workflow/db_only_phase/roadmap_cluster_discovery.sql"

\echo '=== Active cluster roots (largest first) ==='
SELECT
    r.acceptance_criteria->>'cluster_root_id' AS cluster_root_id,
    count(*) AS active_items
FROM roadmap_items AS r
WHERE lower(r.status) = 'active'
  AND coalesce(r.acceptance_criteria->>'cluster_root_id', '') <> ''
GROUP BY 1
ORDER BY active_items DESC, cluster_root_id;

\echo ''
\echo '=== Sample cluster paths (most specific first) ==='
SELECT
    r.roadmap_item_id,
    r.parent_roadmap_item_id,
    r.acceptance_criteria->>'cluster_family' AS cluster_family,
    (r.acceptance_criteria->>'cluster_depth')::int AS cluster_depth,
    r.acceptance_criteria->>'cluster_path' AS cluster_path
FROM roadmap_items AS r
WHERE lower(r.status) = 'active'
  AND coalesce(r.acceptance_criteria->>'cluster_path', '') <> ''
ORDER BY cluster_depth DESC, r.roadmap_item_id
LIMIT 60;

\echo ''
\echo '=== Orphan active items (missing cluster metadata) ==='
SELECT
    r.roadmap_item_id,
    r.parent_roadmap_item_id,
    r.title
FROM roadmap_items AS r
WHERE lower(r.status) = 'active'
  AND (
      r.acceptance_criteria IS NULL
      OR coalesce(r.acceptance_criteria->>'cluster_root_id', '') = ''
      OR coalesce(r.acceptance_criteria->>'cluster_path', '') = ''
  )
ORDER BY r.roadmap_item_id
LIMIT 100;
