-- Migration 099: Task type profile authority
--
-- Moves task type profiles (allowed_tools, default_tier, file_attach,
-- system_prompt_hint) and keyword routing rules from Python to Postgres.
-- Also adds catalog_dispatch flag to integration_registry so the MCP
-- dispatch allowlist lives in the DB instead of a frozen Python set.

BEGIN;

CREATE TABLE IF NOT EXISTS task_type_profiles (
    task_type           TEXT PRIMARY KEY,
    allowed_tools       JSONB NOT NULL DEFAULT '[]'::jsonb
                            CHECK (jsonb_typeof(allowed_tools) = 'array'),
    default_tier        TEXT NOT NULL DEFAULT 'mid'
                            CHECK (default_tier IN ('frontier', 'mid', 'economy')),
    file_attach         BOOLEAN NOT NULL DEFAULT false,
    system_prompt_hint  TEXT NOT NULL DEFAULT '',
    status              TEXT NOT NULL DEFAULT 'active',
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Keyword → task_type routing rules.
-- sort_order controls evaluation priority (lower = checked first).
-- context_code_clues / context_creative_clues are used for disambiguation
-- when a keyword is ambiguous (e.g. "write" can be code or creative).
-- If both are non-empty and the keyword matched, the clues resolve the type.
CREATE TABLE IF NOT EXISTS task_type_keyword_rules (
    id                  SERIAL PRIMARY KEY,
    keywords            TEXT[] NOT NULL,
    task_type           TEXT NOT NULL REFERENCES task_type_profiles(task_type),
    sort_order          INTEGER NOT NULL DEFAULT 99,
    context_code_clues  TEXT[] NOT NULL DEFAULT '{}',
    context_creative_clues TEXT[] NOT NULL DEFAULT '{}',
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_task_type_keyword_rules_sort_order
    ON task_type_keyword_rules (sort_order);

-- DB-backed MCP catalog dispatch routing: replaces _CATALOG_MCP_SERVER_IDS
-- frozen set in Python. Set true for any integration_registry row whose
-- execution should be routed through the catalog MCP tool handler.
ALTER TABLE integration_registry
    ADD COLUMN IF NOT EXISTS catalog_dispatch BOOLEAN NOT NULL DEFAULT false;

UPDATE integration_registry
SET catalog_dispatch = true
WHERE provider = 'mcp'
  AND mcp_server_id IN ('praxis-workflow-mcp', 'dag-workflow-mcp');

-- Seed task type profiles
INSERT INTO task_type_profiles (task_type, allowed_tools, default_tier, file_attach, system_prompt_hint) VALUES
    ('research',        '["WebSearch","WebFetch","Read"]',           'mid',      false, 'Search for information and cite sources.'),
    ('code_generation', '["Read","Edit","Write","Bash"]',            'mid',      false, 'Write clean, tested code.'),
    ('code_edit',       '["Read","Edit","Bash"]',                    'mid',      false, 'Make targeted edits only.'),
    ('code_review',     '["Read","Grep","Glob"]',                    'mid',      false, 'Review code for issues. Be specific.'),
    ('analysis',        '["Read"]',                                  'economy',  false, 'Analyze data. Output structured results.'),
    ('creative',        '[]',                                        'mid',      false, 'Write with voice and personality.'),
    ('debug',           '["Read","Bash","Grep","Glob"]',             'mid',      false, 'Find the root cause. Be systematic.'),
    ('extraction',      '["Read"]',                                  'economy',  false, 'Extract structured data. Output JSON.'),
    ('ocr',             '["Read"]',                                  'mid',      true,  'Read and transcribe the image content.'),
    ('debate',          '[]',                                        'frontier', false, 'Take a strong position. Be specific. No hedging.'),
    ('brainstorm',      '[]',                                        'mid',      false, 'Generate ideas. Be creative and concrete.'),
    ('architecture',    '["Read","Grep","Glob"]',                    'frontier', false, 'Design systems with clear contracts and tradeoffs.'),
    ('review',          '["Read","Grep","Glob"]',                    'mid',      false, 'Review thoroughly. Score on dimensions, not pass/fail.'),
    ('general',         '[]',                                        'mid',      false, '')
ON CONFLICT (task_type) DO UPDATE SET
    allowed_tools      = EXCLUDED.allowed_tools,
    default_tier       = EXCLUDED.default_tier,
    file_attach        = EXCLUDED.file_attach,
    system_prompt_hint = EXCLUDED.system_prompt_hint,
    updated_at         = now();

-- Seed keyword routing rules (sort_order = evaluation priority).
-- "write" has context clues to disambiguate code vs. creative intent.
INSERT INTO task_type_keyword_rules
    (keywords, task_type, sort_order, context_code_clues, context_creative_clues)
VALUES
    (ARRAY['debate','argue','position','perspective','crossfire'], 'debate',          1,  '{}', '{}'),
    (ARRAY['brainstorm','ideate','explore','possibilities'],       'brainstorm',      2,  '{}', '{}'),
    (ARRAY['architect','design','system design','tradeoff'],       'architecture',    3,  '{}', '{}'),
    (ARRAY['debug','diagnose','trace','troubleshoot'],             'debug',           4,  '{}', '{}'),
    (ARRAY['research','discover','search','find','gather'],        'research',        5,  '{}', '{}'),
    (ARRAY['review','audit','check','lint','inspect'],             'code_review',     6,  '{}', '{}'),
    (ARRAY['edit','fix','rename','format','refactor'],             'code_edit',       7,  '{}', '{}'),
    (ARRAY['build','create','implement','generate'],               'code_generation', 8,  '{}', '{}'),
    (ARRAY['extract','parse','scrape','pull'],                     'extraction',      9,  '{}', '{}'),
    (ARRAY['score','evaluate','analyze','analyse','rank','assess'],'analysis',        10, '{}', '{}'),
    (ARRAY['draft','email','outreach','compose','copywrite'],      'creative',        11, '{}', '{}'),
    (ARRAY['ocr','image','scan','transcribe'],                     'ocr',             12, '{}', '{}'),
    -- "write" alone is ambiguous: default to code_generation, use clues to resolve
    (ARRAY['write'],  'code_generation', 13,
        ARRAY['function','class','module','test','script','code'],
        ARRAY['email','message','outreach','blog','post'])
ON CONFLICT DO NOTHING;

COMMIT;
