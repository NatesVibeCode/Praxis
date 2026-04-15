# Sandbox Cleanup Salvage

Generated at: `2026-04-14T20:23:54`
Temp root scanned: `/var/folders/3x/pw0pr72s2l366jdgq4qmyjch0000gn/T`
Sandboxes scanned: `80`
Sandboxes with code/text diffs: `12`
Unique captured variants: `332`

This salvage excludes runtime database files, artifact outputs, caches, and node modules. It keeps source/text variants under `variants/<sha1>/<relative-path>` plus a full manifest.

Top sandboxes by captured code diffs:
- `praxis-docker-sandbox-jj2h6s9u`: `241` code/text diffs, `5197` files, `6527797547` bytes
- `praxis-docker-sandbox-l3mqe81h`: `241` code/text diffs, `5196` files, `6524917063` bytes
- `praxis-docker-sandbox-r0m108w0`: `241` code/text diffs, `5195` files, `6521637513` bytes
- `praxis-docker-sandbox-ag5iurp4`: `212` code/text diffs, `14431` files, `7944154161` bytes
- `praxis-docker-sandbox-a7xodrdz`: `24` code/text diffs, `2312` files, `5765823865` bytes
- `praxis-docker-sandbox-awq723al`: `24` code/text diffs, `779` files, `1601831021` bytes
- `praxis-docker-sandbox-clui0afi`: `24` code/text diffs, `779` files, `1606025325` bytes
- `praxis-docker-sandbox-sdiovaik`: `24` code/text diffs, `2299` files, `6045491969` bytes
- `praxis-docker-sandbox-ya8zrijp`: `24` code/text diffs, `779` files, `1606025325` bytes
- `praxis-docker-sandbox-5tt9bduu`: `23` code/text diffs, `2136` files, `5757095228` bytes

Representative unique variants:
- `Code&DBs/Databases/migrations/workflow/034_reference_catalog.sql` from `praxis-docker-sandbox-5tt9bduu` -> `artifacts/sandbox_cleanup_20260414/variants/12a298e9984fdf5c41c8bdf29a2a9719e01e39d3/Code&DBs/Databases/migrations/workflow/034_reference_catalog.sql`
- `Code&DBs/Databases/migrations/workflow/037_reference_catalog.sql` from `praxis-docker-sandbox-5tt9bduu` -> `artifacts/sandbox_cleanup_20260414/variants/941f2557191a3e7fa80d2c34616e91909ef8c4b9/Code&DBs/Databases/migrations/workflow/037_reference_catalog.sql`
- `Code&DBs/Databases/migrations/workflow/041_workflow_runtime_cutover.sql` from `praxis-docker-sandbox-5tt9bduu` -> `artifacts/sandbox_cleanup_20260414/variants/602423b04434e4dfa7abf29ad16b5664cfea538f/Code&DBs/Databases/migrations/workflow/041_workflow_runtime_cutover.sql`
- `Code&DBs/Databases/migrations/workflow/042_workflow_control_command_types.sql` from `praxis-docker-sandbox-5tt9bduu` -> `artifacts/sandbox_cleanup_20260414/variants/e5aca0dd2dd8c87ef9d74b3f13cd2ed962a76619/Code&DBs/Databases/migrations/workflow/042_workflow_control_command_types.sql`
- `Code&DBs/Databases/migrations/workflow/046_provider_model_candidate_profiles.sql` from `praxis-docker-sandbox-5tt9bduu` -> `artifacts/sandbox_cleanup_20260414/variants/8c375be007d35818daaea0e0fd8000af85e80a22/Code&DBs/Databases/migrations/workflow/046_provider_model_candidate_profiles.sql`
- `Code&DBs/Databases/migrations/workflow/057_remove_legacy_dispatch_completion_triggers.sql` from `praxis-docker-sandbox-5tt9bduu` -> `artifacts/sandbox_cleanup_20260414/variants/dceeb9dcee99b05fc8550b2d3aa165fcdff3cae9/Code&DBs/Databases/migrations/workflow/057_remove_legacy_dispatch_completion_triggers.sql`
- `Code&DBs/Databases/migrations/workflow/071_repo_snapshots_runtime_breadth_repair.sql` from `praxis-docker-sandbox-5tt9bduu` -> `artifacts/sandbox_cleanup_20260414/variants/52526c0ac54f696837157d3c8856628cd9bc3900/Code&DBs/Databases/migrations/workflow/071_repo_snapshots_runtime_breadth_repair.sql`
- `Code&DBs/Databases/migrations/workflow/076_provider_cli_profile_transport_metadata.sql` from `praxis-docker-sandbox-5tt9bduu` -> `artifacts/sandbox_cleanup_20260414/variants/052cbefc7830c83a300613dec2e22f239aa22338/Code&DBs/Databases/migrations/workflow/076_provider_cli_profile_transport_metadata.sql`
- `Code&DBs/Databases/migrations/workflow/081_observability_lineage_and_metrics.sql` from `praxis-docker-sandbox-5tt9bduu` -> `artifacts/sandbox_cleanup_20260414/variants/f9c258a952f3e1be1bed0cf6d3d430a839c3da8f/Code&DBs/Databases/migrations/workflow/081_observability_lineage_and_metrics.sql`
- `Code&DBs/Databases/migrations/workflow/088_workflow_chain_dependency_and_adoption_authority.sql` from `praxis-docker-sandbox-5tt9bduu` -> `artifacts/sandbox_cleanup_20260414/variants/a7de027d742b557b9e1ec1d01a76bedb805fad8c/Code&DBs/Databases/migrations/workflow/088_workflow_chain_dependency_and_adoption_authority.sql`
- `Code&DBs/Databases/migrations/workflow/100_adapter_config_authority.sql` from `praxis-docker-sandbox-5tt9bduu` -> `artifacts/sandbox_cleanup_20260414/variants/672ff576a1ccf9751d1f0ba774da5e2ee21dc5bf/Code&DBs/Databases/migrations/workflow/100_adapter_config_authority.sql`
- `Code&DBs/Databases/migrations/workflow/106_acceptance_status_index.sql` from `praxis-docker-sandbox-5tt9bduu` -> `artifacts/sandbox_cleanup_20260414/variants/80183a14c63a8d8453dd2138a0a69d376f442f0e/Code&DBs/Databases/migrations/workflow/106_acceptance_status_index.sql`
- `Code&DBs/Workflow/MCP_SERVER_INDEX.md` from `praxis-docker-sandbox-ag5iurp4` -> `artifacts/sandbox_cleanup_20260414/variants/72d01f9b9b7e667e69158d698c9be72563ea7ebb/Code&DBs/Workflow/MCP_SERVER_INDEX.md`
- `Code&DBs/Workflow/adapters/mcp_task.py` from `praxis-docker-sandbox-ag5iurp4` -> `artifacts/sandbox_cleanup_20260414/variants/9dd8a23d17dadc92a4ca60de10231456c6553341/Code&DBs/Workflow/adapters/mcp_task.py`
- `Code&DBs/Workflow/artifacts/sandbox_probe_workdir/seed.txt` from `praxis-docker-sandbox-a7xodrdz` -> `artifacts/sandbox_cleanup_20260414/variants/27a3608bfce6595de32ebe1eb4f1f853d8a09859/Code&DBs/Workflow/artifacts/sandbox_probe_workdir/seed.txt`
- `Code&DBs/Workflow/authority/__init__.py` from `praxis-docker-sandbox-jj2h6s9u` -> `artifacts/sandbox_cleanup_20260414/variants/6cff28b19f9fb87afa110165f0768aba109b38e4/Code&DBs/Workflow/authority/__init__.py`
- `Code&DBs/Workflow/contracts/__init__.py` from `praxis-docker-sandbox-ag5iurp4` -> `artifacts/sandbox_cleanup_20260414/variants/20edd2b4de1a462cf4a53854ccb255804b254a15/Code&DBs/Workflow/contracts/__init__.py`
- `Code&DBs/Workflow/policy/native_primary_cutover.py` from `praxis-docker-sandbox-jj2h6s9u` -> `artifacts/sandbox_cleanup_20260414/variants/3c9dfc4d74b298e6ed21e633500cfe1a85ec8536/Code&DBs/Workflow/policy/native_primary_cutover.py`
- `Code&DBs/Workflow/pyproject.toml` from `praxis-docker-sandbox-ag5iurp4` -> `artifacts/sandbox_cleanup_20260414/variants/cb8d484ddda46b2e61e1f356a20c4fbc78b04501/Code&DBs/Workflow/pyproject.toml`
- `Code&DBs/Workflow/registry/agent_config.py` from `praxis-docker-sandbox-ag5iurp4` -> `artifacts/sandbox_cleanup_20260414/variants/51702454b3104ddb9a6007fbafa2e42570a3d265/Code&DBs/Workflow/registry/agent_config.py`
- `Code&DBs/Workflow/registry/agent_config.py` from `praxis-docker-sandbox-jj2h6s9u` -> `artifacts/sandbox_cleanup_20260414/variants/be8f239d813c02750723943c9f6a3afc6d94acfd/Code&DBs/Workflow/registry/agent_config.py`
- `Code&DBs/Workflow/registry/config_registry.py` from `praxis-docker-sandbox-ag5iurp4` -> `artifacts/sandbox_cleanup_20260414/variants/254c12238cecb564fc3b220f1a30f404e14137cc/Code&DBs/Workflow/registry/config_registry.py`
- `Code&DBs/Workflow/registry/integration_registry_sync.py` from `praxis-docker-sandbox-ag5iurp4` -> `artifacts/sandbox_cleanup_20260414/variants/541928e1a4a2db53c94e1b70d244f884f6353f4d/Code&DBs/Workflow/registry/integration_registry_sync.py`
- `Code&DBs/Workflow/registry/model_context_limits.py` from `praxis-docker-sandbox-ag5iurp4` -> `artifacts/sandbox_cleanup_20260414/variants/12425a8f47c87d804bcc512363c00f49ef40cd5b/Code&DBs/Workflow/registry/model_context_limits.py`
- `Code&DBs/Workflow/registry/native_runtime_profile_sync.py` from `praxis-docker-sandbox-ag5iurp4` -> `artifacts/sandbox_cleanup_20260414/variants/25a649729b8b6d0a4960269b4b0b066cd922fdfe/Code&DBs/Workflow/registry/native_runtime_profile_sync.py`
- `Code&DBs/Workflow/registry/provider_execution_registry.py` from `praxis-docker-sandbox-ag5iurp4` -> `artifacts/sandbox_cleanup_20260414/variants/911135b379dffcd432e42f0bdc96e0c6e801d953/Code&DBs/Workflow/registry/provider_execution_registry.py`
- `Code&DBs/Workflow/requirements.runtime.txt` from `praxis-docker-sandbox-ag5iurp4` -> `artifacts/sandbox_cleanup_20260414/variants/022f1126deaab21e2f08fd31ca10154d5e489357/Code&DBs/Workflow/requirements.runtime.txt`
- `Code&DBs/Workflow/runtime/_helpers.py` from `praxis-docker-sandbox-jj2h6s9u` -> `artifacts/sandbox_cleanup_20260414/variants/ff62df22621ba00f85f91b28812740bfdc1ecf04/Code&DBs/Workflow/runtime/_helpers.py`
- `Code&DBs/Workflow/runtime/build_authority.py` from `praxis-docker-sandbox-jj2h6s9u` -> `artifacts/sandbox_cleanup_20260414/variants/761a7dfc06b4d148ca3295d7852cdc7bf0901622/Code&DBs/Workflow/runtime/build_authority.py`
- `Code&DBs/Workflow/runtime/build_authority.py` from `praxis-docker-sandbox-ag5iurp4` -> `artifacts/sandbox_cleanup_20260414/variants/7a89a485c675bc13efdee25c50f768719ac6441a/Code&DBs/Workflow/runtime/build_authority.py`
- `Code&DBs/Workflow/runtime/canonical_workflows.py` from `praxis-docker-sandbox-ag5iurp4` -> `artifacts/sandbox_cleanup_20260414/variants/5ce1587e1303dc0b5a5932de52409fd62c8a120e/Code&DBs/Workflow/runtime/canonical_workflows.py`
- `Code&DBs/Workflow/runtime/canonical_workflows.py` from `praxis-docker-sandbox-jj2h6s9u` -> `artifacts/sandbox_cleanup_20260414/variants/d43790eda4609203aca0cf985664e3089ea90bc1/Code&DBs/Workflow/runtime/canonical_workflows.py`
- `Code&DBs/Workflow/runtime/capability_feedback.py` from `praxis-docker-sandbox-jj2h6s9u` -> `artifacts/sandbox_cleanup_20260414/variants/0e956d418ca7d9fe2d54ab64979f204c638f82c9/Code&DBs/Workflow/runtime/capability_feedback.py`
- `Code&DBs/Workflow/runtime/command_handlers.py` from `praxis-docker-sandbox-jj2h6s9u` -> `artifacts/sandbox_cleanup_20260414/variants/6d21372c490967d5ee0a6dc22168e0a03a42eec3/Code&DBs/Workflow/runtime/command_handlers.py`
- `Code&DBs/Workflow/runtime/command_handlers.py` from `praxis-docker-sandbox-ag5iurp4` -> `artifacts/sandbox_cleanup_20260414/variants/f2f8215c8b881196f20cf75ec29a10df811c9410/Code&DBs/Workflow/runtime/command_handlers.py`
- `Code&DBs/Workflow/runtime/compile_artifacts.py` from `praxis-docker-sandbox-jj2h6s9u` -> `artifacts/sandbox_cleanup_20260414/variants/ffb71dc85e556850a7f96a9599bd33ce6934c1bb/Code&DBs/Workflow/runtime/compile_artifacts.py`
- `Code&DBs/Workflow/runtime/compile_index.py` from `praxis-docker-sandbox-ag5iurp4` -> `artifacts/sandbox_cleanup_20260414/variants/fa4912d5f728e4af9da1d7a9c5e40b333865100d/Code&DBs/Workflow/runtime/compile_index.py`
- `Code&DBs/Workflow/runtime/compiler.py` from `praxis-docker-sandbox-ag5iurp4` -> `artifacts/sandbox_cleanup_20260414/variants/9d66ac9429d46424b457beef08204be2e943977c/Code&DBs/Workflow/runtime/compiler.py`
- `Code&DBs/Workflow/runtime/compiler_output_builders.py` from `praxis-docker-sandbox-jj2h6s9u` -> `artifacts/sandbox_cleanup_20260414/variants/3bf6c71699d14bde0529ef9b36365ed97f68544b/Code&DBs/Workflow/runtime/compiler_output_builders.py`
- `Code&DBs/Workflow/runtime/compiler_output_builders.py` from `praxis-docker-sandbox-ag5iurp4` -> `artifacts/sandbox_cleanup_20260414/variants/3d818abffb566ae9b03ecf55dbd818fba874c729/Code&DBs/Workflow/runtime/compiler_output_builders.py`
