-- Migration 025: Document objects for knowledge surfaces
--
-- Seeds a canonical document object type and a partial FTS index so
-- knowledge documents can be searched efficiently from API surfaces.

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
    '{"type": "object", "properties": {"title": {"type": "string"}, "content": {"type": "string"}, "doc_type": {"type": "string", "enum": ["policy", "sop", "evidence", "context", "reference"]}, "tags": {"type": "array", "items": {"type": "string"}}, "version": {"type": "integer", "default": 1}, "attached_to": {"type": "array", "items": {"type": "string"}, "description": "Card IDs this document is attached to"}}}'::jsonb,
    'system'
)
ON CONFLICT (type_id) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_objects_document_fts
    ON objects USING GIN (to_tsvector('english', properties::text))
    WHERE type_id = 'doc_type_document';
