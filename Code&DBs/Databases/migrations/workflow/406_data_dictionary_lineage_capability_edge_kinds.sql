-- Migration 406: Extend data_dictionary_lineage.edge_kind for capability graph
-- edges projected from the MCP catalog and operation catalog:
--   dispatches   — tool -> operation (from TOOLS.operation_names)
--   governed_by  — operation -> authority domain (from operation_catalog_registry)

BEGIN;

ALTER TABLE data_dictionary_lineage
    DROP CONSTRAINT IF EXISTS data_dictionary_lineage_kind_check;

ALTER TABLE data_dictionary_lineage
    ADD CONSTRAINT data_dictionary_lineage_kind_check
        CHECK (edge_kind IN (
            'references',
            'derives_from',
            'projects_to',
            'ingests_from',
            'produces',
            'consumes',
            'promotes_to',
            'same_as',
            'dispatches',
            'governed_by'
        ));

COMMENT ON COLUMN data_dictionary_lineage.edge_kind IS
    'references=FK/manifest ref, derives_from=ETL, projects_to=projector, ingests_from=ingest, produces/consumes=workflow I/O and tool type_contract, promotes_to=dataset_promotion, same_as=dedup, dispatches=tool->operation, governed_by=operation->authority_domain.';

COMMIT;
