-- Migration 017: Flexible Object System
-- Generic object storage that any app template can use

CREATE TABLE IF NOT EXISTS object_types (
    type_id text PRIMARY KEY,
    name text NOT NULL,
    description text NOT NULL DEFAULT '',
    icon text,
    property_definitions jsonb NOT NULL DEFAULT '[]',
    created_by text NOT NULL DEFAULT 'system',
    created_at timestamptz NOT NULL DEFAULT now(),
    search_vector tsvector GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(name, '') || ' ' || coalesce(description, ''))
    ) STORED
);

CREATE INDEX IF NOT EXISTS idx_object_types_fts ON object_types USING GIN (search_vector);

CREATE TABLE IF NOT EXISTS objects (
    object_id text PRIMARY KEY,
    type_id text NOT NULL REFERENCES object_types(type_id),
    properties jsonb NOT NULL DEFAULT '{}',
    status text NOT NULL DEFAULT 'active',
    created_by text NOT NULL DEFAULT 'user',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    search_vector tsvector GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(properties::text, ''))
    ) STORED
);

CREATE INDEX IF NOT EXISTS idx_objects_type ON objects (type_id);
CREATE INDEX IF NOT EXISTS idx_objects_status ON objects (type_id, status);
CREATE INDEX IF NOT EXISTS idx_objects_props ON objects USING GIN (properties);
CREATE INDEX IF NOT EXISTS idx_objects_fts ON objects USING GIN (search_vector);
