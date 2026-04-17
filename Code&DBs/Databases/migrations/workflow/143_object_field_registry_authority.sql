-- Migration 143: Object Field Registry Authority
-- Promote object fields to first-class rows and mirror legacy property_definitions.

CREATE TABLE IF NOT EXISTS object_field_registry (
    type_id text NOT NULL REFERENCES object_types(type_id) ON DELETE CASCADE,
    field_name text NOT NULL,
    label text NOT NULL DEFAULT '',
    field_kind text NOT NULL,
    description text NOT NULL DEFAULT '',
    required boolean NOT NULL DEFAULT false,
    default_value jsonb,
    options jsonb NOT NULL DEFAULT '[]'::jsonb,
    display_order integer NOT NULL DEFAULT 100,
    binding_revision text NOT NULL DEFAULT 'migration_143',
    decision_ref text NOT NULL DEFAULT 'object_schema.field_registry.migration_143',
    retired_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (type_id, field_name),
    CHECK (field_kind IN ('text', 'number', 'boolean', 'enum', 'json', 'date', 'datetime', 'reference')),
    CHECK (jsonb_typeof(options) = 'array')
);

CREATE INDEX IF NOT EXISTS idx_object_field_registry_active_type_order
    ON object_field_registry (type_id, retired_at, display_order, field_name);

CREATE OR REPLACE FUNCTION sync_object_type_property_definitions()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_type_id text;
BEGIN
    target_type_id := COALESCE(NEW.type_id, OLD.type_id);

    UPDATE object_types
       SET property_definitions = COALESCE(
           (
               SELECT jsonb_agg(
                   jsonb_strip_nulls(
                       jsonb_build_object(
                           'name', field_name,
                           'label', NULLIF(label, ''),
                           'type', field_kind,
                           'description', NULLIF(description, ''),
                           'required', required,
                           'default', default_value,
                           'options', CASE
                               WHEN jsonb_array_length(options) > 0 THEN options
                               ELSE NULL
                           END,
                           'display_order', display_order
                       )
                   )
                   ORDER BY display_order ASC, field_name ASC
               )
                 FROM object_field_registry
                WHERE type_id = target_type_id
                  AND retired_at IS NULL
           ),
           '[]'::jsonb
       )
     WHERE type_id = target_type_id;

    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_object_field_registry_sync_property_definitions
    ON object_field_registry;

CREATE TRIGGER trg_object_field_registry_sync_property_definitions
AFTER INSERT OR UPDATE OR DELETE
ON object_field_registry
FOR EACH ROW
EXECUTE FUNCTION sync_object_type_property_definitions();

WITH normalized_fields AS (
    SELECT
        ot.type_id,
        arr.value AS field,
        arr.ordinality::integer AS display_order
    FROM object_types AS ot
    JOIN LATERAL jsonb_array_elements(
        CASE
            WHEN jsonb_typeof(ot.property_definitions) = 'array' THEN ot.property_definitions
            WHEN jsonb_typeof(ot.property_definitions) = 'object'
                AND jsonb_typeof(ot.property_definitions -> 'fields') = 'array'
                THEN ot.property_definitions -> 'fields'
            ELSE '[]'::jsonb
        END
    ) WITH ORDINALITY AS arr(value, ordinality) ON TRUE

    UNION ALL

    SELECT
        ot.type_id,
        jsonb_build_object('name', obj.key) ||
            CASE
                WHEN jsonb_typeof(obj.value) = 'object' THEN obj.value
                ELSE jsonb_build_object('type', obj.value)
            END AS field,
        row_number() OVER (PARTITION BY ot.type_id ORDER BY obj.key)::integer AS display_order
    FROM object_types AS ot
    JOIN LATERAL jsonb_each(
        CASE
            WHEN jsonb_typeof(ot.property_definitions) = 'object'
                AND NOT (ot.property_definitions ? 'fields')
                THEN ot.property_definitions
            ELSE '{}'::jsonb
        END
    ) AS obj(key, value) ON TRUE
),
backfilled AS (
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
    SELECT
        type_id,
        trim(COALESCE(field ->> 'field_name', field ->> 'name')) AS field_name,
        trim(COALESCE(field ->> 'label', field ->> 'field_name', field ->> 'name')) AS label,
        CASE lower(COALESCE(field ->> 'field_kind', field ->> 'type', 'text'))
            WHEN 'string' THEN 'text'
            WHEN 'str' THEN 'text'
            WHEN 'varchar' THEN 'text'
            WHEN 'integer' THEN 'number'
            WHEN 'int' THEN 'number'
            WHEN 'float' THEN 'number'
            WHEN 'double' THEN 'number'
            WHEN 'decimal' THEN 'number'
            WHEN 'bool' THEN 'boolean'
            WHEN 'object' THEN 'json'
            WHEN 'array' THEN 'json'
            WHEN 'list' THEN 'json'
            WHEN 'map' THEN 'json'
            WHEN 'dict' THEN 'json'
            WHEN 'jsonb' THEN 'json'
            WHEN 'timestamp' THEN 'datetime'
            WHEN 'ref' THEN 'reference'
            ELSE lower(COALESCE(field ->> 'field_kind', field ->> 'type', 'text'))
        END AS field_kind,
        trim(COALESCE(field ->> 'description', '')) AS description,
        COALESCE((field ->> 'required')::boolean, false) AS required,
        field -> 'default' AS default_value,
        CASE
            WHEN jsonb_typeof(field -> 'options') = 'array' THEN field -> 'options'
            WHEN jsonb_typeof(field -> 'values') = 'array' THEN field -> 'values'
            ELSE '[]'::jsonb
        END AS options,
        COALESCE((field ->> 'display_order')::integer, display_order, 100) AS display_order,
        'migration_143' AS binding_revision,
        'object_schema.field_registry.backfill_143' AS decision_ref
    FROM normalized_fields
    WHERE trim(COALESCE(field ->> 'field_name', field ->> 'name')) <> ''
    ON CONFLICT (type_id, field_name) DO NOTHING
    RETURNING type_id
)
SELECT count(*) FROM backfilled;

UPDATE object_types AS ot
   SET property_definitions = COALESCE(
       (
           SELECT jsonb_agg(
               jsonb_strip_nulls(
                   jsonb_build_object(
                       'name', ofr.field_name,
                       'label', NULLIF(ofr.label, ''),
                       'type', ofr.field_kind,
                       'description', NULLIF(ofr.description, ''),
                       'required', ofr.required,
                       'default', ofr.default_value,
                       'options', CASE
                           WHEN jsonb_array_length(ofr.options) > 0 THEN ofr.options
                           ELSE NULL
                       END,
                       'display_order', ofr.display_order
                   )
               )
               ORDER BY ofr.display_order ASC, ofr.field_name ASC
           )
             FROM object_field_registry AS ofr
            WHERE ofr.type_id = ot.type_id
              AND ofr.retired_at IS NULL
       ),
       '[]'::jsonb
   );
