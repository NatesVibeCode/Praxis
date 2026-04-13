-- Repair stale native runtime-profile authority rows.
--
-- The checked-in native runtime profile config is the source of truth for the
-- repo's default runtime profile. The live model/profile/provider bindings are
-- refreshed from heartbeat-backed routing tables by the runtime-profile sync
-- layer, so this migration only removes stale registry rows that advertise
-- dead native refs.

DELETE FROM registry_runtime_profile_authority
WHERE runtime_profile_ref <> 'dag-project';

DELETE FROM registry_runtime_profile_authority
WHERE runtime_profile_ref = 'dag-project'
  AND (
      model_profile_id <> 'model_profile.dag-project.default'
      OR provider_policy_id <> 'provider_policy.dag-project.default'
  );
