-- Migration 419: Provider CLI auth seed catalog.
--
-- Sandbox runtime no longer owns provider-specific auth copy behavior.
-- Auth mounts declare both the read-only host source and, when needed, the
-- root-readable seed file that is copied into the writable CLI home before
-- dropping privileges.

BEGIN;

UPDATE provider_transport_admissions
   SET probe_contract = COALESCE(probe_contract, '{}'::jsonb)
       || jsonb_build_object(
           'auth_mounts',
           jsonb_build_array(
               jsonb_build_object(
                   'host_relative_path', '.codex/auth.json',
                   'container_relative_path', '.codex/auth.json',
                   'container_seed_filename', 'openai-auth.json'
               )
           ),
           'cli_home_tmpfs_dirs',
           jsonb_build_array('.codex')
       ),
       updated_at = now()
 WHERE provider_slug = 'openai'
   AND adapter_type = 'cli_llm'
   AND status = 'active';

UPDATE provider_transport_admissions
   SET probe_contract = COALESCE(probe_contract, '{}'::jsonb)
       || jsonb_build_object(
           'auth_mounts',
           jsonb_build_array(
               jsonb_build_object(
                   'host_relative_path', '.claude/.credentials.json',
                   'container_relative_path', '.claude/.credentials.json'
               )
           ),
           'cli_home_tmpfs_dirs',
           jsonb_build_array('.claude')
       ),
       updated_at = now()
 WHERE provider_slug = 'anthropic'
   AND adapter_type = 'cli_llm'
   AND status = 'active';

UPDATE provider_transport_admissions
   SET probe_contract = COALESCE(probe_contract, '{}'::jsonb)
       || jsonb_build_object(
           'auth_mounts',
           jsonb_build_array(
               jsonb_build_object(
                   'host_relative_path', '.gemini/oauth_creds.json',
                   'container_relative_path', '.gemini/oauth_creds.json',
                   'container_seed_filename', 'google-gemini-oauth_creds.json'
               ),
               jsonb_build_object(
                   'host_relative_path', '.gemini/google_accounts.json',
                   'container_relative_path', '.gemini/google_accounts.json'
               ),
               jsonb_build_object(
                   'host_relative_path', '.gemini/settings.json',
                   'container_relative_path', '.gemini/settings.json'
               )
           ),
           'cli_home_tmpfs_dirs',
           jsonb_build_array('.gemini')
       ),
       updated_at = now()
 WHERE provider_slug IN ('google', 'gemini')
   AND adapter_type = 'cli_llm'
   AND status = 'active';

COMMIT;
