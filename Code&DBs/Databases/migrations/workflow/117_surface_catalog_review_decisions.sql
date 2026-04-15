BEGIN;

CREATE TABLE IF NOT EXISTS surface_catalog_review_decisions (
    review_decision_id TEXT PRIMARY KEY,
    surface_name TEXT NOT NULL,
    target_kind TEXT NOT NULL CHECK (target_kind IN ('catalog_item', 'source_policy')),
    target_ref TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('approve', 'reject', 'defer', 'widen', 'revoke')),
    actor_type TEXT NOT NULL CHECK (actor_type IN ('model', 'human', 'policy')),
    actor_ref TEXT NOT NULL,
    approval_mode TEXT NOT NULL,
    rationale TEXT,
    candidate_payload JSONB,
    decided_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT surface_catalog_review_decisions_target_ref_nonblank
        CHECK (btrim(target_ref) <> ''),
    CONSTRAINT surface_catalog_review_decisions_surface_name_nonblank
        CHECK (btrim(surface_name) <> ''),
    CONSTRAINT surface_catalog_review_decisions_actor_ref_nonblank
        CHECK (btrim(actor_ref) <> ''),
    CONSTRAINT surface_catalog_review_decisions_approval_mode_nonblank
        CHECK (btrim(approval_mode) <> ''),
    CONSTRAINT surface_catalog_review_decisions_candidate_payload_object_check
        CHECK (
            candidate_payload IS NULL
            OR jsonb_typeof(candidate_payload) = 'object'
        )
);

CREATE INDEX IF NOT EXISTS surface_catalog_review_decisions_surface_target_idx
    ON surface_catalog_review_decisions (
        surface_name,
        target_kind,
        target_ref,
        decided_at DESC,
        created_at DESC
    );

CREATE INDEX IF NOT EXISTS surface_catalog_review_decisions_surface_decided_idx
    ON surface_catalog_review_decisions (
        surface_name,
        decided_at DESC,
        created_at DESC
    );

COMMENT ON TABLE surface_catalog_review_decisions IS 'Append-only review authority for builder surface catalog overrides. Approved decisions overlay seeded registry rows without mutating the base authority.';
COMMENT ON COLUMN surface_catalog_review_decisions.target_kind IS 'Review target class. catalog_item overlays seeded surface rows; source_policy overlays dynamic source-kind policy rows.';
COMMENT ON COLUMN surface_catalog_review_decisions.candidate_payload IS 'Approved or rejected partial overlay payload for the target. Applied only when the latest decision is approve or widen.';

COMMIT;
