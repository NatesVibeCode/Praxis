BEGIN;

CREATE TABLE IF NOT EXISTS compiler_route_hints (
    compiler_route_hint_id TEXT PRIMARY KEY,
    hint_text TEXT NOT NULL,
    route_slug TEXT NOT NULL,
    priority INTEGER NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    decision_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT compiler_route_hints_unique_hint UNIQUE (hint_text, route_slug)
);

INSERT INTO compiler_route_hints (
    compiler_route_hint_id,
    hint_text,
    route_slug,
    priority,
    enabled,
    decision_ref
) VALUES
    ('compiler_route_hint.review', 'review', 'auto/review', 10, TRUE, 'decision.compiler_route_hints.bootstrap.20260408'),
    ('compiler_route_hint.check', 'check', 'auto/review', 20, TRUE, 'decision.compiler_route_hints.bootstrap.20260408'),
    ('compiler_route_hint.validate', 'validate', 'auto/review', 30, TRUE, 'decision.compiler_route_hints.bootstrap.20260408'),
    ('compiler_route_hint.approve', 'approve', 'auto/review', 40, TRUE, 'decision.compiler_route_hints.bootstrap.20260408'),
    ('compiler_route_hint.audit', 'audit', 'auto/review', 50, TRUE, 'decision.compiler_route_hints.bootstrap.20260408'),
    ('compiler_route_hint.design', 'design', 'auto/architecture', 60, TRUE, 'decision.compiler_route_hints.bootstrap.20260408'),
    ('compiler_route_hint.architect', 'architect', 'auto/architecture', 70, TRUE, 'decision.compiler_route_hints.bootstrap.20260408'),
    ('compiler_route_hint.plan', 'plan', 'auto/architecture', 80, TRUE, 'decision.compiler_route_hints.bootstrap.20260408'),
    ('compiler_route_hint.wire', 'wire', 'auto/wiring', 90, TRUE, 'decision.compiler_route_hints.bootstrap.20260408'),
    ('compiler_route_hint.integrat', 'integrat', 'auto/wiring', 100, TRUE, 'decision.compiler_route_hints.bootstrap.20260408'),
    ('compiler_route_hint.route', 'route', 'auto/wiring', 110, TRUE, 'decision.compiler_route_hints.bootstrap.20260408'),
    ('compiler_route_hint.sync', 'sync', 'auto/wiring', 120, TRUE, 'decision.compiler_route_hints.bootstrap.20260408'),
    ('compiler_route_hint.notify', 'notify', 'auto/wiring', 130, TRUE, 'decision.compiler_route_hints.bootstrap.20260408'),
    ('compiler_route_hint.send', 'send', 'auto/wiring', 140, TRUE, 'decision.compiler_route_hints.bootstrap.20260408'),
    ('compiler_route_hint.build', 'build', 'auto/build', 150, TRUE, 'decision.compiler_route_hints.bootstrap.20260408'),
    ('compiler_route_hint.write', 'write', 'auto/build', 160, TRUE, 'decision.compiler_route_hints.bootstrap.20260408'),
    ('compiler_route_hint.draft', 'draft', 'auto/build', 170, TRUE, 'decision.compiler_route_hints.bootstrap.20260408'),
    ('compiler_route_hint.triage', 'triage', 'auto/build', 180, TRUE, 'decision.compiler_route_hints.bootstrap.20260408')
ON CONFLICT (compiler_route_hint_id) DO UPDATE SET
    hint_text = EXCLUDED.hint_text,
    route_slug = EXCLUDED.route_slug,
    priority = EXCLUDED.priority,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

COMMIT;
