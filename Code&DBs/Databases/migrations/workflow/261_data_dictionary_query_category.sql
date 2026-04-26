-- Migration 261: Add 'query' to data_dictionary_objects category constraint.
--
-- register_operation_atomic (migration 239/240) writes category=p_operation_kind
-- which can be 'query' for query operations. The category CHECK constraint
-- last updated by 247_workflow_plumbing_data_dictionary_objects.sql does not
-- include 'query' — so query registrations through the helper fail.
--
-- Extends the allowed-category list with 'query' so query operations have
-- a first-class category in the dictionary, matching how 'command' already
-- works for commands.

BEGIN;

ALTER TABLE data_dictionary_objects
    DROP CONSTRAINT IF EXISTS data_dictionary_objects_category_check;

ALTER TABLE data_dictionary_objects
    ADD CONSTRAINT data_dictionary_objects_category_check
        CHECK (category IN (
            'table',
            'object_type',
            'integration',
            'dataset',
            'ingest',
            'decision',
            'receipt',
            'tool',
            'object',
            'command',
            'query',
            'event',
            'projection',
            'service_bus_channel',
            'feedback_stream',
            'definition',
            'runtime_target',
            'gate',
            'stage',
            'capability',
            'plan_field'
        ));

COMMIT;
