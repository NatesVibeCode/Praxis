-- Migration 182: audit_exclusions — soft-allowlist for audit false positives.
--
-- Some audit findings are genuinely false positives (e.g. code-orphan
-- detection flagging SQL views, which by definition aren't imported by
-- Python). Rather than deleting the finding row-by-row every cycle, we
-- record a persistent exclusion: (audit_kind, finding_kind, subject)
-- that the scanner honors.
--
-- Any exclusion can be undone by deleting its row; audit coverage
-- restores automatically on the next heartbeat.
--
-- Decision: operator_decision.architecture_policy.data_dictionary.audit_rule_exclusions

CREATE TABLE IF NOT EXISTS audit_exclusions (
    exclusion_id     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    audit_kind       text NOT NULL,
    finding_kind     text NOT NULL,
    subject          text NOT NULL,
    rationale        text NOT NULL DEFAULT '',
    created_at       timestamptz NOT NULL DEFAULT now(),
    created_by       text NOT NULL DEFAULT 'system',
    UNIQUE (audit_kind, finding_kind, subject),
    CONSTRAINT audit_exclusions_audit_kind_nonblank
        CHECK (btrim(audit_kind) <> ''),
    CONSTRAINT audit_exclusions_subject_nonblank
        CHECK (btrim(subject) <> '')
);

CREATE INDEX IF NOT EXISTS idx_audit_exclusions_audit_finding
    ON audit_exclusions (audit_kind, finding_kind);
