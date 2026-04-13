-- Migration 095: Rate limit configuration per provider
CREATE TABLE IF NOT EXISTS rate_limit_configs (
    provider_slug TEXT PRIMARY KEY,
    tokens_per_second REAL NOT NULL DEFAULT 10.0,
    burst_size INT NOT NULL DEFAULT 20,
    updated_at TIMESTAMPTZ DEFAULT now()
);

INSERT INTO rate_limit_configs (provider_slug, tokens_per_second, burst_size) VALUES
    ('openai', 10.0, 20),
    ('anthropic', 5.0, 10),
    ('google', 15.0, 30),
    ('openrouter', 5.0, 10),
    ('deepseek', 5.0, 10)
ON CONFLICT (provider_slug) DO NOTHING;
