-- Migration 368: Blank workspace compose surface seed.

BEGIN;

UPDATE app_manifests
   SET name = 'Blank Workspace',
       description = 'Workspace contract authoring surface.',
       manifest = '{
         "version": 4,
         "kind": "helm_surface_bundle",
         "title": "Blank Workspace",
         "default_tab_id": "main",
         "tabs": [
           {
             "id": "main",
             "label": "Workspace",
             "surface_id": "main",
             "source_option_ids": []
           }
         ],
         "surfaces": {
           "main": {
             "id": "main",
             "title": "Workspace",
             "kind": "compose",
             "draft": {}
           }
         },
         "source_options": {},
         "description": "Workspace contract authoring surface."
       }'::jsonb,
       updated_at = now()
 WHERE id = 'seed.workspace.blank'
   AND status = 'seed';

COMMIT;
