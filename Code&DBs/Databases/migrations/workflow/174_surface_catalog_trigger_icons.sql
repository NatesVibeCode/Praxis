-- Migration 174: Align trigger catalog icons with rendered glyphs.
--
-- The Moon popout reads icons from surface_catalog_registry; the canvas
-- renders OrbitNode glyphs from a route map. These were out of sync: the
-- popout showed `tool` (wrench) for Webhook and `trigger` (lightning) for
-- Schedule, while the canvas substituted `notify` (bell) and `metric`
-- (chart). Picking a trigger in the popout produced a different icon on the
-- node — icons did not "stick".
--
-- Fix: store the dedicated `webhook` and `schedule` glyphs on the registry
-- rows so both surfaces resolve to the same path.

UPDATE surface_catalog_registry
   SET icon = 'webhook'
 WHERE catalog_item_id = 'trigger-webhook'
   AND surface_name = 'moon';

UPDATE surface_catalog_registry
   SET icon = 'schedule'
 WHERE catalog_item_id = 'trigger-schedule'
   AND surface_name = 'moon';
