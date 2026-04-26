-- Seed missing object_type:contact authority row so the data-dictionary
-- stewardship projector can apply its explicit_owners step without FK fails.
--
-- Fixes the bug filed under cluster_key
-- bug.title_anchor:data_dictionary_stewardship.projector.explicit_owners
-- (BUG-A745DE65). The stewardship projector references
-- _EXPLICIT_OWNERS["object_type:contact"] = "data_dictionary_authority"
-- (memory/data_dictionary_stewardship_projector.py), but no migration ever
-- seeded the parent row in data_dictionary_objects. Result: the projector
-- raised ForeignKeyViolationError on every run, flooding api-server logs
-- and burning a connection-pool slot per attempt.
--
-- Pattern matches the existing object_type rows (e.g. object_type:doc_type_document).

BEGIN;

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
)
VALUES (
    'object_type:contact',
    'Contact',
    'object_type',
    'Person or party reachable through one or more channels (email, phone, slack, etc.). Steward: data_dictionary_authority.',
    jsonb_build_object('seeded_by', '268_seed_object_type_contact.sql'),
    '{}'::jsonb
)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref;

COMMIT;
