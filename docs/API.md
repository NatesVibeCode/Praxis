# Praxis API Surface

The HTTP API is a client surface over Praxis runtime authority.

This file is generated from the live FastAPI route catalog exposed by `GET /api/routes`.
If it disagrees with runtime output, trust `praxis workflow routes --json` and regenerate this file.
Canonical regeneration command: `PYTHONPATH="Code&DBs/Workflow" .venv/bin/python -m scripts.generate_mcp_docs`.
Generate and test API docs in the same runtime environment; route counts are authority-sensitive.

## Discovery Commands

- `praxis workflow routes --json`
- `praxis workflow api routes --search <text> --method GET --tag <tag>`
- `GET /api/routes`
- `GET /api/routes?visibility=all` for internal and public routes
- Interactive docs: `/docs`
- OpenAPI JSON: `/openapi.json`
- ReDoc: `/redoc`

## Public Routes

| Methods | Path | Visibility | Tags | Summary |
| --- | --- | --- | --- | --- |
| `GET` | `/v1/catalog` | `public` | `public`, `catalog` | public_get_catalog |
| `GET` | `/v1/events` | `public` | `public`, `events` | public_get_events |
| `GET` | `/v1/receipts/{receipt_id}` | `public` | `public`, `receipts` | public_get_receipt |
| `GET` | `/v1/runs` | `public` | `public`, `runs` | public_list_runs |
| `POST` | `/v1/runs` | `public` | `public`, `runs` | public_create_run |
| `GET` | `/v1/runs/{run_id}` | `public` | `public`, `runs` | public_get_run |
| `GET` | `/v1/runs/{run_id}/jobs` | `public` | `public`, `runs` | public_list_run_jobs |
| `POST` | `/v1/runs/{run_id}:cancel` | `public` | `public`, `runs` | public_cancel_run |

## All Routes

Public route count: `8`. All route count: `358`.

| Methods | Path | Visibility | Tags | Summary |
| --- | --- | --- | --- | --- |
| `GET` | `/` | `internal` | - | root_redirect |
| `POST` | `/api/access_control` | `internal` | `operations` | access_control |
| `GET` | `/api/agent-sessions` | `internal` | - | agent_sessions_index_get |
| `GET` | `/api/atlas.html` | `internal` | - | Send legacy Atlas artifact traffic to the live Atlas surface. |
| `GET` | `/api/atlas/graph` | `internal` | - | Return the canonical Atlas graph payload for native app rendering. |
| `GET` | `/api/atlas/graph/stream` | `internal` | - | Stream Atlas-relevant committed workflow evidence from the DB outbox. |
| `POST` | `/api/audit/apply` | `internal` | - | audit_apply_post |
| `GET` | `/api/audit/contracts` | `internal` | - | audit_contracts_get |
| `POST` | `/api/audit/execute_all_contracts` | `internal` | - | audit_execute_all_contracts_post |
| `POST` | `/api/audit/execute_contract` | `internal` | - | audit_execute_contract_post |
| `GET` | `/api/audit/plan` | `internal` | - | audit_plan_get |
| `GET` | `/api/audit/playbook` | `internal` | - | audit_playbook_get |
| `GET` | `/api/audit/registered` | `internal` | - | audit_registered_get |
| `POST` | `/api/authority-domain/forge` | `internal` | `operations` | authority_domain_forge |
| `POST` | `/api/authority-domain/register` | `internal` | `operations` | authority_domain_register |
| `GET` | `/api/authority/objects` | `internal` | `operations` | authority.objects.list |
| `GET` | `/api/authority/objects/adoption` | `internal` | `operations` | authority.objects.adoption |
| `GET` | `/api/authority/objects/domain-summary` | `internal` | `operations` | authority.objects.domain_summary |
| `GET` | `/api/authority/objects/drift` | `internal` | `operations` | authority.objects.drift |
| `POST` | `/api/bug_attach_evidence` | `internal` | `operations` | bug_attach_evidence |
| `POST` | `/api/bug_file` | `internal` | `operations` | bug_file |
| `POST` | `/api/bug_patch_resume` | `internal` | `operations` | bug_patch_resume |
| `POST` | `/api/bug_resolve` | `internal` | `operations` | bug_resolve |
| `GET` | `/api/bugs` | `internal` | - | bugs_get |
| `GET` | `/api/bugs/replay-ready` | `internal` | - | bugs_replay_ready_get |
| `GET` | `/api/catalog` | `internal` | - | Return live catalog items from platform registries + static primitives. |
| `GET` | `/api/catalog/operations` | `internal` | - | Return DB-backed CQRS operation definitions and source policies. |
| `GET` | `/api/catalog/review-decisions` | `internal` | - | catalog_review_decisions_get |
| `POST` | `/api/catalog/review-decisions` | `internal` | - | catalog_review_decisions_post |
| `POST` | `/api/catalog_operation_register` | `internal` | `operations` | catalog_operation_register |
| `POST` | `/api/catalog_operation_retire` | `internal` | `operations` | catalog_operation_retire |
| `GET` | `/api/chat/conversations` | `internal` | - | chat_conversations_get |
| `POST` | `/api/chat/conversations` | `internal` | - | chat_conversations_post |
| `GET` | `/api/chat/conversations/{conversation_id}` | `internal` | - | chat_conversation_get |
| `POST` | `/api/chat/conversations/{conversation_id}/messages` | `internal` | - | chat_messages_post |
| `GET` | `/api/checkpoints` | `internal` | - | checkpoints_get |
| `POST` | `/api/checkpoints` | `internal` | - | checkpoints_post |
| `GET` | `/api/checkpoints/{checkpoint_id}` | `internal` | - | checkpoints_detail_get |
| `POST` | `/api/checkpoints/{checkpoint_id}/approve` | `internal` | - | checkpoints_approve_post |
| `GET` | `/api/circuits` | `internal` | `operations` | operator.circuit_states |
| `POST` | `/api/circuits` | `internal` | `operations` | operator.circuit_override |
| `GET` | `/api/circuits/history` | `internal` | `operations` | operator.circuit_history |
| `POST` | `/api/cli_auth_doctor` | `internal` | `operations` | cli_auth_doctor |
| `POST` | `/api/compile/materialize` | `internal` | `operations` | compile_materialize |
| `POST` | `/api/compile/preview` | `internal` | - | compile_preview_post |
| `POST` | `/api/compile_materialize` | `internal` | - | Legacy compatibility alias for stale compile materialize callers. |
| `POST` | `/api/compliance.list_receipts` | `internal` | `operations` | compliance.list_receipts |
| `POST` | `/api/compose_experiment` | `internal` | `operations` | compose_experiment |
| `POST` | `/api/compose_plan` | `internal` | `operations` | compose_plan |
| `GET` | `/api/costs` | `internal` | - | Return the cost summary from the Postgres-backed cost tracker. |
| `POST` | `/api/credential/capture/keychain` | `internal` | `operations` | credential_capture_keychain |
| `GET` | `/api/dashboard` | `internal` | - | dashboard_get |
| `GET` | `/api/data-dictionary` | `internal` | - | data_dictionary_list_get |
| `DELETE` | `/api/data-dictionary/classifications` | `internal` | - | data_dictionary_classifications_clear_delete |
| `GET` | `/api/data-dictionary/classifications` | `internal` | - | data_dictionary_classifications_summary_get |
| `PUT` | `/api/data-dictionary/classifications` | `internal` | - | data_dictionary_classifications_set_put |
| `GET` | `/api/data-dictionary/classifications/by-tag` | `internal` | - | data_dictionary_classifications_by_tag_get |
| `POST` | `/api/data-dictionary/classifications/reproject` | `internal` | - | data_dictionary_classifications_reproject_post |
| `GET` | `/api/data-dictionary/classifications/{object_kind:path}` | `internal` | - | data_dictionary_classifications_describe_get |
| `GET` | `/api/data-dictionary/drift` | `internal` | - | data_dictionary_drift_get |
| `GET` | `/api/data-dictionary/drift/diff` | `internal` | - | data_dictionary_drift_diff_get |
| `POST` | `/api/data-dictionary/drift/snapshot` | `internal` | - | data_dictionary_drift_snapshot_post |
| `GET` | `/api/data-dictionary/drift/snapshots` | `internal` | - | data_dictionary_drift_snapshots_get |
| `GET` | `/api/data-dictionary/governance` | `internal` | - | data_dictionary_governance_get |
| `GET` | `/api/data-dictionary/governance/clusters` | `internal` | - | data_dictionary_governance_clusters_get |
| `POST` | `/api/data-dictionary/governance/drain` | `internal` | - | data_dictionary_governance_drain_post |
| `POST` | `/api/data-dictionary/governance/enforce` | `internal` | - | data_dictionary_governance_enforce_post |
| `GET` | `/api/data-dictionary/governance/pending` | `internal` | - | data_dictionary_governance_pending_get |
| `GET` | `/api/data-dictionary/governance/remediate` | `internal` | - | data_dictionary_governance_remediate_get |
| `GET` | `/api/data-dictionary/governance/scans` | `internal` | - | data_dictionary_governance_scans_list_get |
| `GET` | `/api/data-dictionary/governance/scans/{scan_id}` | `internal` | - | data_dictionary_governance_scan_detail_get |
| `GET` | `/api/data-dictionary/governance/scorecard` | `internal` | - | data_dictionary_governance_scorecard_get |
| `GET` | `/api/data-dictionary/impact/{object_kind:path}` | `internal` | - | data_dictionary_impact_get |
| `DELETE` | `/api/data-dictionary/lineage` | `internal` | - | data_dictionary_lineage_clear_edge_delete |
| `GET` | `/api/data-dictionary/lineage` | `internal` | - | data_dictionary_lineage_summary_get |
| `PUT` | `/api/data-dictionary/lineage` | `internal` | - | data_dictionary_lineage_set_edge_put |
| `POST` | `/api/data-dictionary/lineage/reproject` | `internal` | - | data_dictionary_lineage_reproject_post |
| `GET` | `/api/data-dictionary/lineage/{object_kind:path}` | `internal` | - | data_dictionary_lineage_describe_get |
| `DELETE` | `/api/data-dictionary/quality` | `internal` | - | data_dictionary_quality_clear_delete |
| `GET` | `/api/data-dictionary/quality` | `internal` | - | data_dictionary_quality_summary_get |
| `PUT` | `/api/data-dictionary/quality` | `internal` | - | data_dictionary_quality_set_put |
| `POST` | `/api/data-dictionary/quality/evaluate` | `internal` | - | data_dictionary_quality_evaluate_post |
| `POST` | `/api/data-dictionary/quality/reproject` | `internal` | - | data_dictionary_quality_reproject_post |
| `GET` | `/api/data-dictionary/quality/rules` | `internal` | - | data_dictionary_quality_rules_get |
| `GET` | `/api/data-dictionary/quality/runs` | `internal` | - | data_dictionary_quality_runs_get |
| `GET` | `/api/data-dictionary/quality/runs/{object_kind}/{rule_kind}` | `internal` | - | data_dictionary_quality_run_history_get |
| `POST` | `/api/data-dictionary/reproject` | `internal` | - | data_dictionary_reproject_post |
| `DELETE` | `/api/data-dictionary/stewardship` | `internal` | - | data_dictionary_stewardship_clear_delete |
| `GET` | `/api/data-dictionary/stewardship` | `internal` | - | data_dictionary_stewardship_summary_get |
| `PUT` | `/api/data-dictionary/stewardship` | `internal` | - | data_dictionary_stewardship_set_put |
| `GET` | `/api/data-dictionary/stewardship/by-steward` | `internal` | - | data_dictionary_stewardship_by_steward_get |
| `POST` | `/api/data-dictionary/stewardship/reproject` | `internal` | - | data_dictionary_stewardship_reproject_post |
| `GET` | `/api/data-dictionary/stewardship/{object_kind:path}` | `internal` | - | data_dictionary_stewardship_describe_get |
| `GET` | `/api/data-dictionary/wiring-audit` | `internal` | - | data_dictionary_wiring_audit_get |
| `GET` | `/api/data-dictionary/wiring-audit/decisions` | `internal` | - | data_dictionary_wiring_audit_decisions_get |
| `GET` | `/api/data-dictionary/wiring-audit/hard-paths` | `internal` | - | data_dictionary_wiring_audit_hard_paths_get |
| `GET` | `/api/data-dictionary/wiring-audit/orphans` | `internal` | - | data_dictionary_wiring_audit_orphans_get |
| `GET` | `/api/data-dictionary/wiring-audit/trend` | `internal` | - | data_dictionary_wiring_audit_trend_get |
| `GET` | `/api/data-dictionary/{object_kind:path}` | `internal` | - | data_dictionary_describe_get |
| `DELETE` | `/api/data-dictionary/{object_kind}/{field_path:path}` | `internal` | - | data_dictionary_clear_override_delete |
| `PUT` | `/api/data-dictionary/{object_kind}/{field_path:path}` | `internal` | - | data_dictionary_set_override_put |
| `GET` | `/api/documents` | `internal` | - | documents_get |
| `POST` | `/api/documents` | `internal` | - | documents_post |
| `POST` | `/api/documents/{doc_id}/attach` | `internal` | - | documents_attach_post |
| `GET` | `/api/events` | `internal` | - | Return recent platform events from the durable event log. |
| `POST` | `/api/experiment_promote_winner` | `internal` | `operations` | experiment_promote_winner |
| `GET` | `/api/feedback/events` | `internal` | `operations` | feedback.list |
| `POST` | `/api/feedback/events` | `internal` | `operations` | feedback.record |
| `GET` | `/api/files` | `internal` | - | files_get |
| `POST` | `/api/files` | `internal` | - | files_post |
| `DELETE` | `/api/files/{rest_of_path:path}` | `internal` | - | files_path_delete |
| `GET` | `/api/files/{rest_of_path:path}` | `internal` | - | files_path_get |
| `GET` | `/api/fitness` | `internal` | - | Return capability fitness matrix. |
| `GET` | `/api/handoff/history` | `internal` | - | handoff_history_get |
| `GET` | `/api/handoff/latest` | `internal` | - | handoff_latest_get |
| `GET` | `/api/handoff/lineage` | `internal` | - | handoff_lineage_get |
| `GET` | `/api/handoff/status` | `internal` | - | handoff_status_get |
| `GET` | `/api/health` | `internal` | - | Platform health from bounded Postgres probes. |
| `GET` | `/api/integrations` | `internal` | - | integrations_get |
| `POST` | `/api/integrations` | `internal` | - | integrations_post |
| `POST` | `/api/integrations/reload` | `internal` | - | integrations_reload_post |
| `GET` | `/api/integrations/{integration_id}` | `internal` | - | integrations_describe_get |
| `PUT` | `/api/integrations/{integration_id}/secret` | `internal` | - | integrations_secret_put |
| `POST` | `/api/integrations/{integration_id}/test` | `internal` | - | integrations_test_post |
| `GET` | `/api/intent/analyze` | `internal` | - | intent_analyze_get |
| `POST` | `/api/launch_plan` | `internal` | `operations` | launch_plan |
| `POST` | `/api/launcher/recover` | `internal` | - | Run bounded launcher recovery through the preferred launcher command. |
| `GET` | `/api/launcher/resolve` | `internal` | - | Return launcher workspace/base-path authority for the global command. |
| `GET` | `/api/launcher/status` | `internal` | - | launcher_status_get |
| `GET` | `/api/leaderboard` | `internal` | - | Return the agent leaderboard as a list of AgentScore dicts. |
| `GET` | `/api/manifest-heads` | `internal` | - | manifest_heads_get |
| `GET` | `/api/manifests` | `internal` | - | manifests_list |
| `POST` | `/api/manifests/generate` | `internal` | - | manifests_generate_post |
| `POST` | `/api/manifests/generate-quick` | `internal` | - | manifests_generate_quick_post |
| `GET` | `/api/manifests/history` | `internal` | - | manifests_history_get |
| `POST` | `/api/manifests/refine` | `internal` | - | manifests_refine_post |
| `POST` | `/api/manifests/save` | `internal` | - | manifests_save_post |
| `POST` | `/api/manifests/save-as` | `internal` | - | manifests_save_as_post |
| `GET` | `/api/manifests/{manifest_id}` | `internal` | - | manifests_get |
| `POST` | `/api/match_rules.backfill` | `internal` | `operations` | match_rules.backfill |
| `GET` | `/api/metrics` | `internal` | - | Return the core metrics summary for the last N days. |
| `GET` | `/api/metrics/heatmap` | `internal` | - | Return the failure code x provider heatmap for the last N days. |
| `GET` | `/api/metrics/surface-usage` | `internal` | - | Return durable frontdoor surface-usage counters for the last N days. |
| `GET` | `/api/models` | `internal` | - | models_get |
| `GET` | `/api/models/market` | `internal` | - | models_market_get |
| `POST` | `/api/models/run` | `internal` | - | models_run_post |
| `GET` | `/api/models/runs/{rest_of_path:path}` | `internal` | - | models_runs_path_get |
| `POST` | `/api/models/runs/{rest_of_path:path}` | `internal` | - | models_runs_path_post |
| `GET` | `/api/moon/pickers/{rest_of_path:path}` | `internal` | - | moon_pickers_get |
| `GET` | `/api/object-truth/compare-versions` | `internal` | `operations` | object_truth_compare_versions |
| `POST` | `/api/object-truth/observe-record` | `internal` | `operations` | object_truth_observe_record |
| `POST` | `/api/object-truth/record-comparison-run` | `internal` | `operations` | object_truth_record_comparison_run |
| `POST` | `/api/object-truth/store-observed-record` | `internal` | `operations` | object_truth_store_observed_record |
| `POST` | `/api/object-truth/store-schema-snapshot` | `internal` | `operations` | object_truth_store_schema_snapshot |
| `GET` | `/api/object-types` | `internal` | `operations` | object_schema.type_list |
| `POST` | `/api/object-types` | `internal` | `operations` | object_schema.type_upsert |
| `DELETE` | `/api/object-types/{type_id}` | `internal` | `operations` | object_schema.type_delete |
| `GET` | `/api/object-types/{type_id}` | `internal` | `operations` | object_schema.type_get |
| `PUT` | `/api/object-types/{type_id}` | `internal` | `operations` | object_schema.type_upsert_by_id |
| `GET` | `/api/object-types/{type_id}/fields` | `internal` | `operations` | object_schema.field_list |
| `POST` | `/api/object-types/{type_id}/fields` | `internal` | `operations` | object_schema.field_upsert |
| `DELETE` | `/api/object-types/{type_id}/fields/{field_name}` | `internal` | `operations` | object_schema.field_retire |
| `GET` | `/api/objects` | `internal` | - | objects_get |
| `POST` | `/api/objects` | `internal` | - | objects_post |
| `DELETE` | `/api/objects/delete` | `internal` | - | objects_delete |
| `PUT` | `/api/objects/update` | `internal` | - | objects_update_put |
| `DELETE` | `/api/objects/{rest_of_path:path}` | `internal` | - | objects_path_delete |
| `GET` | `/api/objects/{rest_of_path:path}` | `internal` | - | objects_path_get |
| `PUT` | `/api/objects/{rest_of_path:path}` | `internal` | - | objects_path_put |
| `GET` | `/api/observability/bug-scoreboard` | `internal` | - | Return aggregate bug observability focused on replay readiness, regressions, and recurrence. |
| `GET` | `/api/observability/code-hotspots` | `internal` | - | Return merged code hotspot rollups across static health, receipt risk, and bug packets. |
| `GET` | `/api/observability/platform` | `internal` | - | Return operator-facing platform probe status with lane cues and degraded causes. |
| `POST` | `/api/operate` | `internal` | - | Call one catalog-backed operator operation through the unified gateway. |
| `GET` | `/api/operate/catalog` | `internal` | - | Return the unified operator gateway catalog. |
| `POST` | `/api/operation.evolve_field` | `internal` | `operations` | operation.evolve_field |
| `POST` | `/api/operator.next` | `internal` | `operations` | operator.next |
| `POST` | `/api/operator/architecture-policy` | `internal` | `operations` | operator.architecture_policy_record |
| `GET` | `/api/operator/bug-triage-packet` | `internal` | `operations` | operator.bug_triage_packet |
| `POST` | `/api/operator/daily-heartbeat-refresh` | `internal` | `operations` | operator.daily_heartbeat_refresh |
| `GET` | `/api/operator/data-dictionary` | `internal` | `operations` | operator.data_dictionary |
| `POST` | `/api/operator/decision` | `internal` | `operations` | operator.decision_record |
| `GET` | `/api/operator/decisions` | `internal` | `operations` | operator.decision_list |
| `POST` | `/api/operator/execution-proof` | `internal` | `operations` | operator.execution_proof |
| `GET` | `/api/operator/execution-truth` | `internal` | `operations` | operator.execution_truth |
| `GET` | `/api/operator/firecheck` | `internal` | `operations` | operator.firecheck |
| `POST` | `/api/operator/functional-area` | `internal` | `operations` | operator.functional_area_record |
| `GET` | `/api/operator/graph` | `internal` | `operations` | operator.graph_projection |
| `POST` | `/api/operator/ideas` | `internal` | `operations` | operator.ideas |
| `GET` | `/api/operator/issue-backlog` | `internal` | `operations` | operator.issue_backlog |
| `POST` | `/api/operator/legal-tools` | `internal` | `operations` | operator.legal_tools |
| `POST` | `/api/operator/maintenance/backfill-bug-replay-provenance` | `internal` | `operations` | operator.bug_replay_provenance_backfill |
| `POST` | `/api/operator/maintenance/backfill-semantic-bridges` | `internal` | `operations` | operator.semantic_bridges_backfill |
| `POST` | `/api/operator/maintenance/refresh-semantic-projection` | `internal` | `operations` | operator.semantic_projection_refresh |
| `POST` | `/api/operator/maintenance/reset-metrics` | `internal` | `operations` | operator.metrics_reset |
| `GET` | `/api/operator/model-access-control-matrix` | `internal` | `operations` | operator.model_access_control_matrix |
| `POST` | `/api/operator/native-primary-cutover-gate` | `internal` | `operations` | operator.native_primary_cutover_gate |
| `GET` | `/api/operator/next-work` | `internal` | `operations` | operator.next_work |
| `POST` | `/api/operator/object-relation` | `internal` | `operations` | operator.object_relation_record |
| `GET` | `/api/operator/operation-forge` | `internal` | `operations` | operator.operation_forge |
| `POST` | `/api/operator/provider-availability-refresh` | `internal` | `operations` | operator.provider_availability_refresh |
| `GET` | `/api/operator/provider-control-plane` | `internal` | `operations` | operator.provider_control_plane |
| `POST` | `/api/operator/provider-onboarding` | `internal` | `operations` | operator.provider_onboarding |
| `GET` | `/api/operator/provider-route-truth` | `internal` | `operations` | operator.provider_route_truth |
| `GET` | `/api/operator/refactor-heatmap` | `internal` | `operations` | operator.refactor_heatmap |
| `POST` | `/api/operator/remediation-apply` | `internal` | `operations` | operator.remediation_apply |
| `GET` | `/api/operator/remediation-plan` | `internal` | `operations` | operator.remediation_plan |
| `GET` | `/api/operator/replay-ready-bugs` | `internal` | `operations` | operator.replay_ready_bugs |
| `POST` | `/api/operator/roadmap-write` | `internal` | `operations` | operator.roadmap_write |
| `GET` | `/api/operator/roadmap/tree/{root_roadmap_item_id}` | `internal` | `operations` | operator.roadmap_tree |
| `GET` | `/api/operator/runs/{run_id}/graph` | `internal` | `operations` | operator.run_graph |
| `GET` | `/api/operator/runs/{run_id}/lineage` | `internal` | `operations` | operator.run_lineage |
| `GET` | `/api/operator/runs/{run_id}/scoreboard` | `internal` | `operations` | operator.run_scoreboard |
| `GET` | `/api/operator/runs/{run_id}/status` | `internal` | `operations` | operator.run_status |
| `GET` | `/api/operator/runtime-truth` | `internal` | `operations` | operator.runtime_truth_snapshot |
| `POST` | `/api/operator/task-route-eligibility` | `internal` | `operations` | operator.task_route_eligibility |
| `POST` | `/api/operator/transport-support` | `internal` | `operations` | operator.transport_support |
| `GET` | `/api/operator/ui/experience-graph` | `internal` | `operations` | operator.ui_experience_graph |
| `GET` | `/api/operator/work-assignment-matrix` | `internal` | `operations` | operator.work_assignment_matrix |
| `POST` | `/api/operator/work-item-closeout` | `internal` | `operations` | operator.work_item_closeout |
| `POST` | `/api/operator_patterns` | `internal` | `operations` | operator_patterns |
| `POST` | `/api/pattern_materialize_candidates` | `internal` | `operations` | pattern_materialize_candidates |
| `GET` | `/api/platform-overview` | `internal` | - | platform_overview_get |
| `POST` | `/api/policy.list` | `internal` | `operations` | policy.list |
| `GET` | `/api/primitives` | `internal` | `operations` | primitive.list |
| `POST` | `/api/primitives` | `internal` | `operations` | primitive.record |
| `GET` | `/api/primitives/scan-consistency` | `internal` | `operations` | primitive.scan_consistency |
| `GET` | `/api/primitives/{primitive_slug}` | `internal` | `operations` | primitive.get |
| `GET` | `/api/projections/{projection_ref}` | `internal` | - | projection_get |
| `POST` | `/api/queue/cancel/{job_id}` | `internal` | - | Cancel a queue-backed workflow through the workflow command bus. |
| `GET` | `/api/queue/jobs` | `internal` | - | List workflow jobs, optionally filtered by status. |
| `GET` | `/api/queue/stats` | `internal` | - | Return workflow job statistics grouped by status. |
| `POST` | `/api/queue/submit` | `internal` | - | Submit a one-job workflow through the workflow command bus. |
| `GET` | `/api/receipts` | `internal` | - | Return a listing of recent workflow receipts (metadata, not full content). |
| `GET` | `/api/receipts/{receipt_id}` | `internal` | - | Return the full JSON content of one receipt by id. |
| `GET` | `/api/references` | `internal` | - | references_get |
| `GET` | `/api/registries/search` | `internal` | - | registries_search_get |
| `GET` | `/api/reviews` | `internal` | - | Return author review summaries with dimension scores. |
| `GET` | `/api/routes` | `internal` | - | Return the live HTTP route catalog for CLI and API discovery. |
| `GET` | `/api/runs/recent` | `internal` | - | Return recent workflow runs with job progress summaries. |
| `GET` | `/api/runs/{run_id}` | `internal` | - | Return one workflow run with ordered job details. |
| `GET` | `/api/runs/{run_id}/jobs/{job_id}` | `internal` | - | Return one workflow job with best-available output content. |
| `GET` | `/api/scope` | `internal` | - | Resolve read scope, blast radius, and test scope for write-scope files. |
| `GET` | `/api/search` | `internal` | - | search_get |
| `POST` | `/api/search/bugs` | `internal` | `operations` | search.bugs |
| `POST` | `/api/search/code` | `internal` | `operations` | search.code |
| `POST` | `/api/search/db` | `internal` | `operations` | search.db |
| `POST` | `/api/search/decisions` | `internal` | `operations` | search.decisions |
| `POST` | `/api/search/federated` | `internal` | `operations` | search.federated |
| `POST` | `/api/search/files` | `internal` | `operations` | search.files |
| `POST` | `/api/search/git-history` | `internal` | `operations` | search.git_history |
| `POST` | `/api/search/knowledge` | `internal` | `operations` | search.knowledge |
| `POST` | `/api/search/receipts` | `internal` | `operations` | search.receipts |
| `POST` | `/api/search/research` | `internal` | `operations` | search.research |
| `GET` | `/api/semantic-invariants/scan` | `internal` | `operations` | semantic_invariant.scan |
| `GET` | `/api/semantic-predicates` | `internal` | `operations` | semantic_predicate.list |
| `POST` | `/api/semantic-predicates` | `internal` | `operations` | semantic_predicate.record |
| `GET` | `/api/semantic-predicates/{predicate_slug}` | `internal` | `operations` | semantic_predicate.get |
| `POST` | `/api/semantic/assertion` | `internal` | `operations` | semantic_assertions.record |
| `POST` | `/api/semantic/assertion/retract` | `internal` | `operations` | semantic_assertions.retract |
| `GET` | `/api/semantic/assertions` | `internal` | `operations` | semantic_assertions.list |
| `POST` | `/api/semantic/predicate` | `internal` | `operations` | semantic_assertions.register_predicate |
| `POST` | `/api/service-lifecycle/desired-state` | `internal` | `operations` | service.lifecycle.declare_desired_state |
| `POST` | `/api/service-lifecycle/events` | `internal` | `operations` | service.lifecycle.record_event |
| `GET` | `/api/service-lifecycle/projection/{service_ref}/{runtime_target_ref}` | `internal` | `operations` | service.lifecycle.get_projection |
| `POST` | `/api/service-lifecycle/services` | `internal` | `operations` | service.lifecycle.register_service |
| `GET` | `/api/service-lifecycle/targets` | `internal` | `operations` | service.lifecycle.list_targets |
| `POST` | `/api/service-lifecycle/targets` | `internal` | `operations` | service.lifecycle.register_target |
| `POST` | `/api/setup/apply` | `internal` | `operations` | runtime.setup.apply |
| `GET` | `/api/setup/doctor` | `internal` | `operations` | runtime.setup.doctor |
| `GET` | `/api/setup/graph` | `internal` | - | setup_graph_get |
| `GET` | `/api/setup/plan` | `internal` | `operations` | runtime.setup.plan |
| `POST` | `/api/shell/draft-guard/consulted` | `internal` | `operations` | shell.draft.guard.consulted |
| `POST` | `/api/shell/history/popped` | `internal` | `operations` | shell.history.popped |
| `GET` | `/api/shell/routes` | `internal` | - | shell_routes_get |
| `POST` | `/api/shell/session/bootstrapped` | `internal` | `operations` | shell.session.bootstrapped |
| `GET` | `/api/shell/state/stream` | `internal` | - | Stream session-scoped shell-navigation events from authority_events. |
| `POST` | `/api/shell/surface/opened` | `internal` | `operations` | shell.surface.opened |
| `POST` | `/api/shell/tab/closed` | `internal` | `operations` | shell.tab.closed |
| `GET` | `/api/source-options` | `internal` | - | source_options_get |
| `GET` | `/api/status` | `internal` | `operations` | operator.status_snapshot |
| `GET` | `/api/structured-documents/context-selections` | `internal` | `operations` | structured_documents.list_context_selection_receipts |
| `POST` | `/api/structured-documents/context-selections` | `internal` | `operations` | structured_documents.record_context_selection |
| `POST` | `/api/surface/action` | `internal` | `operations` | surface.action.performed |
| `POST` | `/api/surface/templates` | `internal` | `operations` | surface.template.register |
| `GET` | `/api/templates` | `internal` | - | templates_get |
| `POST` | `/api/trace.walk` | `internal` | `operations` | trace.walk |
| `POST` | `/api/trigger/{rest_of_path:path}` | `internal` | - | trigger_post |
| `GET` | `/api/trust` | `internal` | - | Return ELO-based trust scores for all (provider, model) pairs. |
| `POST` | `/api/webhooks/endpoints` | `internal` | `webhooks` | Register a new webhook endpoint. Auto-creates workflow_trigger if target_workflow_id is set. |
| `POST` | `/api/webhooks/{slug}` | `internal` | `webhooks` | Receive an incoming webhook, validate signature, store event. |
| `POST` | `/api/workflow-job` | `internal` | - | workflow_job_post |
| `POST` | `/api/workflow-runs` | `internal` | - | workflow_runs_handler_post |
| `POST` | `/api/workflow-runs/spawn` | `internal` | - | workflow_runs_spawn_post |
| `GET` | `/api/workflow-runs/{run_id}/status` | `internal` | - | workflow_runs_status_get |
| `GET` | `/api/workflow-runs/{run_id}/stream` | `internal` | - | workflow_runs_stream_get |
| `GET` | `/api/workflow-status` | `internal` | - | workflow_status_alias_get |
| `GET` | `/api/workflow-templates` | `internal` | - | workflow_templates_get |
| `GET` | `/api/workflow-triggers` | `internal` | - | workflow_triggers_get |
| `POST` | `/api/workflow-triggers` | `internal` | - | workflow_triggers_post |
| `PUT` | `/api/workflow-triggers` | `internal` | - | workflow_triggers_put |
| `PUT` | `/api/workflow-triggers/{rest_of_path:path}` | `internal` | - | workflow_triggers_path_put |
| `POST` | `/api/workflow_chain_submit` | `internal` | `operations` | workflow_chain_submit |
| `GET` | `/api/workflows` | `internal` | - | workflows_get |
| `POST` | `/api/workflows` | `internal` | - | workflows_post |
| `POST` | `/api/workflows/run` | `internal` | - | workflows_run_post |
| `DELETE` | `/api/workflows/{rest_of_path:path}` | `internal` | - | workflows_path_delete |
| `GET` | `/api/workflows/{rest_of_path:path}` | `internal` | - | workflows_path_get |
| `POST` | `/api/workflows/{rest_of_path:path}` | `internal` | - | workflows_path_post |
| `PUT` | `/api/workflows/{rest_of_path:path}` | `internal` | - | workflows_path_put |
| `POST` | `/api/workflows/{workflow_id}/build/suggest-next` | `internal` | `operations` | workflow_build.suggest_next |
| `POST` | `/api/workflows/{workflow_id}/build/{subpath:path}` | `internal` | `operations` | workflow_build.mutate |
| `GET` | `/app` | `internal` | - | launcher_app_root |
| `GET` | `/app/` | `internal` | - | launcher_app_root_slash |
| `GET` | `/app/manifest.webmanifest` | `internal` | - | launcher_manifest |
| `GET` | `/app/sw.js` | `internal` | - | launcher_service_worker |
| `GET` | `/app/{path:path}` | `internal` | - | launcher_app_path |
| `POST` | `/artifacts` | `internal` | - | artifacts_post |
| `POST` | `/bugs` | `internal` | - | bugs_post |
| `GET` | `/console` | `internal` | - | Compatibility doorway for the retired standalone console UI. |
| `GET` | `/console/` | `internal` | - | Compatibility doorway for the retired standalone console UI. |
| `GET` | `/console/icon-{size}.png` | `internal` | - | Serve Android/Chrome installability icons. |
| `GET` | `/console/icon.svg` | `internal` | - | Serve the real Praxis logo SVG for the installed phone console. |
| `GET` | `/console/manifest.webmanifest` | `internal` | - | Serve the installable operator-console PWA manifest. |
| `GET` | `/console/sw.js` | `internal` | - | Serve the console service worker for install + notification clicks. |
| `GET` | `/console/vendor/{name}` | `internal` | - | Serve inlined JS dependencies for the operator console (dev-only). |
| `POST` | `/constraints` | `internal` | - | constraints_post |
| `POST` | `/decompose` | `internal` | - | decompose_post |
| `POST` | `/friction` | `internal` | - | friction_post |
| `POST` | `/governance` | `internal` | - | governance_post |
| `POST` | `/graph` | `internal` | - | graph_post |
| `POST` | `/heal` | `internal` | - | heal_post |
| `POST` | `/health` | `internal` | - | health_post |
| `POST` | `/heartbeat` | `internal` | - | heartbeat_post |
| `POST` | `/ingest` | `internal` | - | ingest_post |
| `GET` | `/manifest.webmanifest` | `internal` | - | launcher_manifest |
| `POST` | `/manifest/generate` | `internal` | - | manifest_generate_standard_post |
| `POST` | `/manifest/get` | `internal` | - | manifest_get_standard_post |
| `POST` | `/manifest/refine` | `internal` | - | manifest_refine_standard_post |
| `POST` | `/mcp` | `internal` | - | mcp_bridge |
| `POST` | `/orient` | `internal` | - | orient_post |
| `POST` | `/query` | `internal` | - | query_post |
| `POST` | `/recall` | `internal` | - | recall_post |
| `POST` | `/receipts` | `internal` | - | receipts_post |
| `POST` | `/research` | `internal` | - | research_post |
| `POST` | `/session` | `internal` | - | session_post |
| `POST` | `/status` | `internal` | - | status_standard_post |
| `GET` | `/sw.js` | `internal` | - | launcher_service_worker |
| `GET` | `/v1/catalog` | `public` | `public`, `catalog` | public_get_catalog |
| `GET` | `/v1/events` | `public` | `public`, `events` | public_get_events |
| `GET` | `/v1/receipts/{receipt_id}` | `public` | `public`, `receipts` | public_get_receipt |
| `GET` | `/v1/runs` | `public` | `public`, `runs` | public_list_runs |
| `POST` | `/v1/runs` | `public` | `public`, `runs` | public_create_run |
| `GET` | `/v1/runs/{run_id}` | `public` | `public`, `runs` | public_get_run |
| `GET` | `/v1/runs/{run_id}/jobs` | `public` | `public`, `runs` | public_list_run_jobs |
| `POST` | `/v1/runs/{run_id}:cancel` | `public` | `public`, `runs` | public_cancel_run |
| `POST` | `/wave` | `internal` | - | wave_post |
| `POST` | `/workflow-runs` | `internal` | - | workflow_runs_standard_post |
| `POST` | `/workflow-validate` | `internal` | - | workflow_validate_post |
