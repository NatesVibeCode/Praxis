# Phase 001-010 Synthesis

This wave is mostly serial. The packets are narrow, but they intentionally converge on shared authority seams, so the main execution rule is still "close one seam, then open the next."

## 1. Dependency map

- `phase_001_workspace_boundary_contract` is the root boundary proof. It fixes how `config/runtime_profiles.json` becomes `repo_root` and `workdir` inside `runtime/instance.py`, `_admission.py`, and `runtime_setup.py`.
- `phase_002_control_plane_core` depends on Phase 1 because the native frontdoor must resolve the repo-local instance before it can prove `submit(...) -> persist_workflow_admission(...) -> status(...)`.
- `phase_003_registry_authority` depends on Phases 1 and 2 because it needs the runtime-profile authority contract to be coherent before `load_registry_resolver(...)` can stop relying on manual row seeding.
- `phase_004_gate_and_promotion_policy` depends on the submission/review stack that Phase 2 exposes, because it proves durable gate and promotion rows through the live review path.
- `phase_005_workflow_outbox` depends on `storage/migrations.py` plus the outbox runtime seam. Its real dependency is bootstrap authority, not notification architecture.
- `phase_006_platform_authority_schema` depends on Phase 5 because the subscription seam already rides the outbox-backed runtime path. It is a scoped bootstrap filter for `event_subscriptions` and `subscription_checkpoints`, not a rewrite of the broader Phase 6 packet.
- `phase_007_provider_route_health_budget` depends on the native runtime sync path because that is the first place a real writer for `provider_route_health_windows` can attach to existing repo facts.
- `phase_008_workflow_class_and_schedule_schema` depends on the canonical schedule repository and authority catalog. It is a read-authority convergence packet, not a scheduler rewrite.
- `phase_009_bug_and_roadmap_authority` depends on the operator closeout surface and the closeout repository. It removes one duplicated commit-time write owner from `operator_write.py`.
- `phase_010_operator_control_authority` depends on the operator control runtime, repository, and public frontdoor. It proves the native primary cutover gate as a real write-to-read seam.
- The strict registry spine is `001 -> 002 -> 003 -> 004 -> 005 -> 006 -> 007 -> 008 -> 009 -> 010`.
- The hidden dependency layer is shared authority code: `runtime/instance.py`, `storage/migrations.py`, `native_runtime_profile_sync.py`, `operator_write.py`, `workflow_schedule_repository.py`, `work_item_closeout_repository.py`, and `operator_control_repository.py`.

## 2. Natural parallel groups

- Group A is the native instance and registry authority band: `phase_001`, `phase_002`, and `phase_003`. These are not safe to write in parallel if anyone is changing `config/runtime_profiles.json`, `runtime/instance.py`, or `native_runtime_profile_sync.py`; treat them as a serial lane with shared review context.
- Group B is the frontdoor/bootstrap band: `phase_004` and `phase_005`. They can be staffed independently once upstream prerequisites are closed because they sit in different runtime surfaces, but both are sensitive to shared bootstrap helpers.
- Group C is the platform/scheduler band: `phase_006`, `phase_007`, and `phase_008`. The safe split is `phase_006` plus `phase_008`, with `phase_007` held unless `phase_006` stays completely out of `native_runtime_profile_sync.py`. Do not let `phase_007` expand into `phase_006` bootstrap code or `phase_008` reader cleanup.
- Group D is the operator-control band: `phase_009` and `phase_010`. Do not run these as parallel write efforts because they both converge on `operator_write.py`.
- If you need the highest-confidence concurrency plan, use this split: one worker on `phase_006`, one worker on `phase_008`, one worker on `phase_009`, hold `phase_007` until the native runtime sync scope is stable, and hold `phase_010` until the closeout frontdoor is stable.
- If you need the lowest-risk order, keep the registry chain strictly serial and only parallelize read-only review or verification work.

## 3. Collision risks

- `phase_001` and `phase_003` both touch `config/runtime_profiles.json`, `runtime/instance.py`, and native runtime-profile semantics. A change to path resolution or profile grammar in one will invalidate the other’s proof.
- `phase_002`, `phase_004`, and `phase_010` all live on public frontdoors that shape requests before writing state. They do not share tables, but they do share test harness patterns, request builders, and database bootstrap code.
- `phase_005` and `phase_006` both rely on bootstrap authority helpers. A refactor of `storage/migrations.py` or bootstrap statement loading can spill across both packets.
- `phase_006` and `phase_007` can collide if `phase_006` is widened into `native_runtime_profile_sync.py` while `phase_007` is adding the health-window writer there.
- `phase_008` can drift into `scheduler_window_repository.py` or `default_path_pilot.py` because those are the nearest adjacent readers. The packet explicitly forbids that drift, but it is still the most likely failure mode.
- `phase_009` and `phase_010` are the clearest file-level collision pair. Both use `operator_write.py`, so any rewrite of the operator frontdoor can destabilize the other phase.
- `phase_009` also overlaps with `bug_evidence_repository.py`, `roadmap_authoring_repository.py`, and `post_workflow_sync.py`. If a sprint tries to solve all Phase 9 writers at once, the boundary disappears.

## 4. Which packets are ready to turn into real build workflows first

- `phase_001_workspace_boundary_contract` is ready. It has one exact seam, one exact failure mode, and a minimal proof target.
- `phase_002_control_plane_core` is ready. The packet already names the live submit/status path and the exact tables that must round-trip.
- `phase_003_registry_authority` is ready. It has a clear anti-pattern to remove: manual authority-row seeding before `load_registry_resolver(...)`.
- `phase_004_gate_and_promotion_policy` is ready. It is specific about the live review seam, the required evidence fields, and the exact rows that must be persisted.
- `phase_005_workflow_outbox` is ready. It is a bootstrap-authority repair with one concrete proof that `dispatch_notifications` appears after the bootstrap helper runs.
- `phase_006_platform_authority_schema` is ready. It is tightly scoped to subscription bootstrap filtering and has a crisp absence check for unrelated Phase 6 tables.
- `phase_009_bug_and_roadmap_authority` is ready. The packet already identifies the duplicated commit-time SQL and the repository methods that should own it.
- `phase_010_operator_control_authority` is ready. It has the cleanest end-to-end proof target after Phase 9: one valid cutover admission, one invalid multi-target failure, and authority readback.
- These are the packets I would hand to build automation first because each one has a single write owner, an explicit proof gap, and a narrow stop condition.

## 5. Which packets are still too vague and why

- `phase_007_provider_route_health_budget` is the vaguest packet in the set. It has no existing canonical writer for `provider_route_health_windows`, and the packet currently identifies `task_type_routing` as the only concrete first-bridge source named in repo evidence.
- `phase_008_workflow_class_and_schedule_schema` is narrower than Phase 7, but it still has an unresolved payload mismatch: the canonical catalog requires `recurring_run_window`, while the public scheduler payload only exposes `schedule_definition` and `workflow_class`.
- `phase_008` also admits that `runtime/scheduler_window_repository.py` remains another direct reader after the sprint, so the packet improves one seam without fully closing the broader authority story.
- `phase_007` and `phase_008` are still buildable, but they need stricter owner-file discipline than the earlier phases because both sit on partially converged authority surfaces.
- No packet here is "too vague" in the sense of being unworkable. The issue is degree of ambiguity: 007 still needs a source-of-truth decision for the new writer, and 008 still needs a clearer statement about the scheduler payload boundary.

## Bottom line

- Execute the wave in registry order unless you have separate workers and disjoint files.
- Treat `001` through `006`, `009`, and `010` as the cleanest next executable-wave candidates.
- Treat `007` and `008` as executable but higher-risk because `007` still needs a first-bridge writer decision and `008` still needs payload adaptation discipline around `recurring_run_window`.
