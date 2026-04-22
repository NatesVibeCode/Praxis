-- Migration 025: Document objects for knowledge surfaces
--
-- Seeds a canonical document object type and a partial FTS index so
-- knowledge documents can be searched efficiently from API surfaces.

DO $$
DECLARE
    document_properties jsonb := '{"type": "object", "properties": {"title": {"type": "string"}, "content": {"type": "string"}, "doc_type": {"type": "string", "enum": ["policy", "sop", "evidence", "context", "reference"]}, "tags": {"type": "array", "items": {"type": "string"}}, "version": {"type": "integer", "default": 1}, "attached_to": {"type": "array", "items": {"type": "string"}, "description": "Card IDs this document is attached to"}}}'::jsonb;
    has_property_definitions boolean;
    has_field_registry boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'object_types'
           AND column_name = 'property_definitions'
    )
      INTO has_property_definitions;

    has_field_registry := to_regclass('public.object_field_registry') IS NOT NULL;

    IF NOT has_property_definitions AND NOT has_field_registry THEN
        ALTER TABLE object_types
            ADD COLUMN IF NOT EXISTS property_definitions jsonb NOT NULL DEFAULT '[]'::jsonb;
        has_property_definitions := true;
    END IF;

    IF has_property_definitions THEN
        EXECUTE $sql$
            INSERT INTO object_types (
                type_id,
                name,
                description,
                icon,
                property_definitions,
                created_by
            )
            VALUES (
                'doc_type_document',
                'Document',
                'Knowledge document: policy, SOP, evidence, context, or reference material',
                '📄',
                $1,
                'system'
            )
            ON CONFLICT (type_id) DO UPDATE
                SET name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    icon = EXCLUDED.icon,
                    property_definitions = COALESCE(
                        NULLIF(object_types.property_definitions, '[]'::jsonb),
                        EXCLUDED.property_definitions
                    )
        $sql$ USING document_properties;
    ELSE
        INSERT INTO object_types (
            type_id,
            name,
            description,
            icon,
            created_by
        )
        VALUES (
            'doc_type_document',
            'Document',
            'Knowledge document: policy, SOP, evidence, context, or reference material',
            '📄',
            'system'
        )
        ON CONFLICT (type_id) DO UPDATE
            SET name = EXCLUDED.name,
                description = EXCLUDED.description,
                icon = EXCLUDED.icon;
    END IF;

    IF has_field_registry THEN
        INSERT INTO object_field_registry (
            type_id,
            field_name,
            label,
            field_kind,
            description,
            required,
            default_value,
            options,
            display_order,
            binding_revision,
            decision_ref
        )
        VALUES
            ('doc_type_document', 'title', 'Title', 'text', 'Document title', true, NULL, '[]'::jsonb, 10, 'migration_025', 'object_schema.document_type.migration_025'),
            ('doc_type_document', 'content', 'Content', 'text', 'Document content body', true, NULL, '[]'::jsonb, 20, 'migration_025', 'object_schema.document_type.migration_025'),
            ('doc_type_document', 'doc_type', 'Document type', 'enum', 'Document classification', true, NULL, '["policy", "sop", "evidence", "context", "reference"]'::jsonb, 30, 'migration_025', 'object_schema.document_type.migration_025'),
            ('doc_type_document', 'tags', 'Tags', 'json', 'Document tags', false, '[]'::jsonb, '[]'::jsonb, 40, 'migration_025', 'object_schema.document_type.migration_025'),
            ('doc_type_document', 'version', 'Version', 'number', 'Document version number', false, '1'::jsonb, '[]'::jsonb, 50, 'migration_025', 'object_schema.document_type.migration_025'),
            ('doc_type_document', 'attached_to', 'Attached to', 'json', 'Card IDs this document is attached to', false, '[]'::jsonb, '[]'::jsonb, 60, 'migration_025', 'object_schema.document_type.migration_025')
        ON CONFLICT (type_id, field_name) DO NOTHING;
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_objects_document_fts
    ON objects USING GIN (to_tsvector('english', properties::text))
    WHERE type_id = 'doc_type_document';
