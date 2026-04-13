-- OAuth2 token store for integration credential lifecycle.

CREATE TABLE IF NOT EXISTS credential_tokens (
    integration_id  text NOT NULL,
    token_kind      text NOT NULL DEFAULT 'access',
    access_token    text NOT NULL,
    refresh_token   text,
    expires_at      timestamptz,
    scopes          text[] NOT NULL DEFAULT '{}',
    token_type      text NOT NULL DEFAULT 'Bearer',
    provider_hint   text NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (integration_id, token_kind)
);

CREATE INDEX IF NOT EXISTS idx_credential_tokens_expires
    ON credential_tokens (expires_at)
    WHERE token_kind = 'access';

COMMENT ON TABLE credential_tokens IS 'OAuth2 token store. Postgres-authoritative.';
