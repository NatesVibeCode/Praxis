-- Migration 190: Differentiate gate icons per family.
--
-- Every gate family except Human Review was shipping with icon = 'gate',
-- so Approval / Branch / On Failure / Retry / Validation all rendered as
-- the same glyph in Moon. Assign distinct glyphs from the MoonGlyph set
-- so the trigger dot on an edge communicates the gate's intent.

UPDATE surface_catalog_registry
   SET icon = 'diff'
 WHERE catalog_item_id = 'ctrl-branch'
   AND surface_name = 'moon';

UPDATE surface_catalog_registry
   SET icon = 'human'
 WHERE catalog_item_id = 'ctrl-approval'
   AND surface_name = 'moon';

UPDATE surface_catalog_registry
   SET icon = 'validate'
 WHERE catalog_item_id = 'ctrl-validation'
   AND surface_name = 'moon';

UPDATE surface_catalog_registry
   SET icon = 'webhook'
 WHERE catalog_item_id = 'ctrl-retry'
   AND surface_name = 'moon';

UPDATE surface_catalog_registry
   SET icon = 'blocked'
 WHERE catalog_item_id = 'ctrl-on-failure'
   AND surface_name = 'moon';
