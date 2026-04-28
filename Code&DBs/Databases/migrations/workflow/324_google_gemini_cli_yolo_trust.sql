-- Migration 324: Admit Gemini CLI yolo mode inside the Praxis sandbox.
--
-- The outer Praxis Docker sandbox owns workspace and network blast radius.
-- Gemini CLI still requires an explicit trusted-workspace declaration before
-- it will run headless with yolo/autonomy enabled.

BEGIN;

UPDATE public.provider_cli_profiles AS profile
   SET base_flags = '["--yolo","-p",".","-o","json"]'::jsonb,
       forbidden_flags = (
           SELECT COALESCE(jsonb_agg(flag ORDER BY ord), '[]'::jsonb)
             FROM jsonb_array_elements_text(COALESCE(profile.forbidden_flags, '[]'::jsonb))
                  WITH ORDINALITY AS existing(flag, ord)
            WHERE flag NOT IN ('--yolo', '-y')
       ),
       sandbox_env_overrides = jsonb_set(
           COALESCE(profile.sandbox_env_overrides, '{}'::jsonb),
           '{set}',
           COALESCE(COALESCE(profile.sandbox_env_overrides, '{}'::jsonb)->'set', '{}'::jsonb)
               || '{"GEMINI_CLI_TRUST_WORKSPACE":"true"}'::jsonb,
           true
       ),
       updated_at = now()
 WHERE profile.provider_slug IN ('google', 'gemini')
   AND profile.binary_name = 'gemini';

UPDATE public.provider_model_candidates AS candidate
   SET cli_config = jsonb_set(
           COALESCE(candidate.cli_config, '{}'::jsonb),
           '{cmd_template}',
           '["gemini","--yolo","-p",".","-o","json","--model","{model}"]'::jsonb,
           true
       )
 WHERE candidate.provider_slug IN ('google', 'gemini')
   AND COALESCE(candidate.cli_config, '{}'::jsonb) <> '{}'::jsonb;

COMMIT;
