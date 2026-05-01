-- Migration 402: Rename Compile→Materialize across DB tables, columns, and
-- the authority_object_registry rows that mirror them.
--
-- The earlier sweep (migrations 396, 401 + the source-code rename pass)
-- moved the runtime onto materialize-named identifiers and the operation
-- catalog onto materialize-named operations. The DB schema was the last
-- layer still using compile-named tables/columns; this migration lifts it.
--
-- Renames:
--   table  compile_artifacts                                → materialize_artifacts
--   table  compile_index_snapshots                          → materialize_index_snapshots
--   table  compile_runs                                     → materialize_runs
--   col    compile_artifacts.compile_artifact_id            → materialize_artifact_id
--   col    compile_index_snapshots.compile_index_ref        → materialize_index_ref
--   col    compile_index_snapshots.compile_surface_name     → materialize_surface_name
--   col    compile_index_snapshots.compile_surface_revision → materialize_surface_revision
--   col    compile_runs.compile_run_id                      → materialize_run_id
--
-- Preserved (different concept — workflow stage routing layer):
--   table  compiler_route_hints
--   task_type='compile' + variants in task_type_routing
--
-- Preserved (immutable history):
--   migration .sql filenames
--   authority_operation_receipts.operation_ref strings
--   workflow_*.compiled_* columns (past-tense label, separate concern)
--
-- ALTER TABLE/COLUMN RENAME in Postgres atomically updates indexes,
-- constraints, and dependent views. authority_object_registry rows that
-- reference the old paths via object_ref string keys are UPDATEd in place.

BEGIN;

ALTER TABLE compile_artifacts RENAME TO materialize_artifacts;
ALTER TABLE materialize_artifacts RENAME COLUMN compile_artifact_id TO materialize_artifact_id;

ALTER TABLE compile_index_snapshots RENAME TO materialize_index_snapshots;
ALTER TABLE materialize_index_snapshots RENAME COLUMN compile_index_ref TO materialize_index_ref;
ALTER TABLE materialize_index_snapshots RENAME COLUMN compile_surface_name TO materialize_surface_name;
ALTER TABLE materialize_index_snapshots RENAME COLUMN compile_surface_revision TO materialize_surface_revision;

ALTER TABLE compile_runs RENAME TO materialize_runs;
ALTER TABLE materialize_runs RENAME COLUMN compile_run_id TO materialize_run_id;

-- authority_object_registry: rename rows whose object_ref encodes the old
-- table/column names. object_ref is the PK; updating it cascades through
-- the authority projection layer. Format is
-- "table.public.<tname>" / "column.public.<tname>.<colname>".

UPDATE authority_object_registry
SET object_ref = 'table.public.materialize_artifacts'
WHERE object_ref = 'table.public.compile_artifacts';

UPDATE authority_object_registry
SET object_ref = REPLACE(object_ref, 'column.public.compile_artifacts.', 'column.public.materialize_artifacts.')
WHERE object_ref LIKE 'column.public.compile_artifacts.%';

-- Then the column-id rename inside the (now) materialize_artifacts row set:
UPDATE authority_object_registry
SET object_ref = 'column.public.materialize_artifacts.materialize_artifact_id'
WHERE object_ref = 'column.public.materialize_artifacts.compile_artifact_id';

UPDATE authority_object_registry
SET object_ref = 'table.public.materialize_index_snapshots'
WHERE object_ref = 'table.public.compile_index_snapshots';

UPDATE authority_object_registry
SET object_ref = REPLACE(object_ref, 'column.public.compile_index_snapshots.', 'column.public.materialize_index_snapshots.')
WHERE object_ref LIKE 'column.public.compile_index_snapshots.%';

UPDATE authority_object_registry
SET object_ref = 'column.public.materialize_index_snapshots.materialize_index_ref'
WHERE object_ref = 'column.public.materialize_index_snapshots.compile_index_ref';

UPDATE authority_object_registry
SET object_ref = 'column.public.materialize_index_snapshots.materialize_surface_name'
WHERE object_ref = 'column.public.materialize_index_snapshots.compile_surface_name';

UPDATE authority_object_registry
SET object_ref = 'column.public.materialize_index_snapshots.materialize_surface_revision'
WHERE object_ref = 'column.public.materialize_index_snapshots.compile_surface_revision';

-- compile_runs has no authority_object_registry entries per pre-migration
-- survey, so only the table rename above is needed.

-- data_dictionary_objects mirrors the registry — same key shape, same renames.

UPDATE data_dictionary_objects
SET object_kind = 'table.public.materialize_artifacts'
WHERE object_kind = 'table.public.compile_artifacts';

UPDATE data_dictionary_objects
SET object_kind = REPLACE(object_kind, 'column.public.compile_artifacts.', 'column.public.materialize_artifacts.')
WHERE object_kind LIKE 'column.public.compile_artifacts.%';

UPDATE data_dictionary_objects
SET object_kind = 'column.public.materialize_artifacts.materialize_artifact_id'
WHERE object_kind = 'column.public.materialize_artifacts.compile_artifact_id';

UPDATE data_dictionary_objects
SET object_kind = 'table.public.materialize_index_snapshots'
WHERE object_kind = 'table.public.compile_index_snapshots';

UPDATE data_dictionary_objects
SET object_kind = REPLACE(object_kind, 'column.public.compile_index_snapshots.', 'column.public.materialize_index_snapshots.')
WHERE object_kind LIKE 'column.public.compile_index_snapshots.%';

UPDATE data_dictionary_objects
SET object_kind = 'column.public.materialize_index_snapshots.materialize_index_ref'
WHERE object_kind = 'column.public.materialize_index_snapshots.compile_index_ref';

UPDATE data_dictionary_objects
SET object_kind = 'column.public.materialize_index_snapshots.materialize_surface_name'
WHERE object_kind = 'column.public.materialize_index_snapshots.compile_surface_name';

UPDATE data_dictionary_objects
SET object_kind = 'column.public.materialize_index_snapshots.materialize_surface_revision'
WHERE object_kind = 'column.public.materialize_index_snapshots.compile_surface_revision';

COMMIT;
