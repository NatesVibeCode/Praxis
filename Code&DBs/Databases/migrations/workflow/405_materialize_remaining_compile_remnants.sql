-- Migration 405: Final pass — rename the past-tense + task_type-slug remnants
-- of the Compile→Materialize sweep that earlier slices preserved.
--
-- Earlier slices renamed: runtime/compile/, spec_compiler.py + 16 sibling
-- modules, the Foundation Pydantic types, 3 operations (compile_preview,
-- compile_materialize, workflow_context_compile), 3 DB tables
-- (compile_artifacts/compile_index_snapshots/compile_runs), and
-- 2 event_types (compile.materialized + workflow_context.compiled).
--
-- This migration covers the remaining DB-side compile remnants. The user
-- pointed out that "compile" as a task_type slug + the past-tense "compiled_"
-- column prefixes are also the verb (just in different grammatical tenses
-- and surfaces); they're now renamed for full consistency.
--
-- Renames:
--   table  compiler_route_hints                          → materializer_route_hints
--   col    compiler_route_hints.compiler_route_hint_id   → materializer_route_hint_id
--   col    workflows.compiled_spec                       → materialized_spec
--   col    workflow_build_execution_manifests.compiled_spec_json → materialized_spec_json
--   col    workflow_context_packs.compiled_from_json     → materialized_from_json
--   col    workflow_surface_usage_events.compiled_job_count → materialized_job_count
--   row    task_type_routing.task_type IN ('compile', 'compile_author',
--          'compile_finalize', 'compile_pill_match', 'compile_synthesize')
--          → corresponding 'materialize*' values
--
-- Authority chain (authority_object_registry + data_dictionary_objects rows)
-- is updated in lockstep so the projection layer stays coherent.

BEGIN;

-- 1. Past-tense column renames -------------------------------------------

ALTER TABLE workflows
    RENAME COLUMN compiled_spec TO materialized_spec;

ALTER TABLE workflow_build_execution_manifests
    RENAME COLUMN compiled_spec_json TO materialized_spec_json;

ALTER TABLE workflow_context_packs
    RENAME COLUMN compiled_from_json TO materialized_from_json;

ALTER TABLE workflow_surface_usage_events
    RENAME COLUMN compiled_job_count TO materialized_job_count;

-- 2. compiler_route_hints rename -----------------------------------------

ALTER TABLE compiler_route_hints
    RENAME TO materializer_route_hints;
ALTER TABLE materializer_route_hints
    RENAME COLUMN compiler_route_hint_id TO materializer_route_hint_id;

-- 3. task_type_routing slug renames --------------------------------------
-- task_type_routing PK is composite (task_type, model_slug, provider_slug,
-- sub_task_type) so updating task_type in place is a PK update — Postgres
-- handles it; no FK constraints reference these specific values today.

UPDATE task_type_routing
SET task_type = 'materialize'
WHERE task_type = 'compile';

UPDATE task_type_routing
SET task_type = 'materialize_author'
WHERE task_type = 'compile_author';

UPDATE task_type_routing
SET task_type = 'materialize_finalize'
WHERE task_type = 'compile_finalize';

UPDATE task_type_routing
SET task_type = 'materialize_pill_match'
WHERE task_type = 'compile_pill_match';

UPDATE task_type_routing
SET task_type = 'materialize_synthesize'
WHERE task_type = 'compile_synthesize';

-- 4. authority_object_registry mirror updates ----------------------------

UPDATE authority_object_registry
SET object_ref = 'table.public.materializer_route_hints'
WHERE object_ref = 'table.public.compiler_route_hints';

UPDATE authority_object_registry
SET object_ref = REPLACE(object_ref, 'column.public.compiler_route_hints.', 'column.public.materializer_route_hints.')
WHERE object_ref LIKE 'column.public.compiler_route_hints.%';

UPDATE authority_object_registry
SET object_ref = 'column.public.materializer_route_hints.materializer_route_hint_id'
WHERE object_ref = 'column.public.materializer_route_hints.compiler_route_hint_id';

UPDATE authority_object_registry
SET object_ref = REPLACE(object_ref, 'compiled_spec_json', 'materialized_spec_json')
WHERE object_ref = 'column.public.workflow_build_execution_manifests.compiled_spec_json';

UPDATE authority_object_registry
SET object_ref = REPLACE(object_ref, 'compiled_job_count', 'materialized_job_count')
WHERE object_ref = 'column.public.workflow_surface_usage_events.compiled_job_count';

UPDATE authority_object_registry
SET object_ref = REPLACE(object_ref, 'compiled_spec', 'materialized_spec')
WHERE object_ref = 'column.public.workflows.compiled_spec';

-- 5. data_dictionary_objects mirror updates ------------------------------

UPDATE data_dictionary_objects
SET object_kind = 'table.public.materializer_route_hints'
WHERE object_kind = 'table.public.compiler_route_hints';

UPDATE data_dictionary_objects
SET object_kind = REPLACE(object_kind, 'column.public.compiler_route_hints.', 'column.public.materializer_route_hints.')
WHERE object_kind LIKE 'column.public.compiler_route_hints.%';

UPDATE data_dictionary_objects
SET object_kind = 'column.public.materializer_route_hints.materializer_route_hint_id'
WHERE object_kind = 'column.public.materializer_route_hints.compiler_route_hint_id';

UPDATE data_dictionary_objects
SET object_kind = REPLACE(object_kind, 'compiled_spec_json', 'materialized_spec_json')
WHERE object_kind = 'column.public.workflow_build_execution_manifests.compiled_spec_json';

UPDATE data_dictionary_objects
SET object_kind = REPLACE(object_kind, 'compiled_job_count', 'materialized_job_count')
WHERE object_kind = 'column.public.workflow_surface_usage_events.compiled_job_count';

UPDATE data_dictionary_objects
SET object_kind = REPLACE(object_kind, 'compiled_spec', 'materialized_spec')
WHERE object_kind = 'column.public.workflows.compiled_spec';

COMMIT;
