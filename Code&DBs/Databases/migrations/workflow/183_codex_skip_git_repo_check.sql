-- Migration 183: Make `codex exec` safe to probe from any CWD.
--
-- The daily_heartbeat provider probe runs each CLI in an ephemeral
-- TemporaryDirectory sandbox so the CLIs do not ingest the repo context
-- (codex reads AGENTS.md, gemini walks the workspace, claude honors
-- CLAUDE.md). Without a sandbox the probe ballooned to tens of thousands
-- of input tokens per cycle and inflated latency with no signal.
--
-- Under that sandbox, codex refuses to start with:
--     "Not inside a trusted directory and --skip-git-repo-check was not
--     specified."
--
-- `--skip-git-repo-check` only has an effect when codex cannot find a
-- surrounding git repo; inside a working repo (which is every real
-- worker invocation) it is a no-op. That makes it safe to bake into
-- base_flags for all invocations rather than carrying sandbox-specific
-- branching in the probe.
--
-- Verification:
--   SELECT base_flags FROM provider_cli_profiles WHERE provider_slug='openai';
--   -> expect ["exec","--skip-git-repo-check","-","--json"]

BEGIN;

UPDATE public.provider_cli_profiles
   SET base_flags = '["exec", "--skip-git-repo-check", "-", "--json"]'::jsonb,
       updated_at = now()
 WHERE provider_slug = 'openai';

COMMIT;
