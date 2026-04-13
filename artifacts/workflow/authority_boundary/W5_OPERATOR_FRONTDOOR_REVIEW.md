# W5 Operator Frontdoor Review

Date: 2026-04-09

Source of truth:
- `artifacts/workflow/authority_boundary/BUG_TRIAGE_2026-04-09_R2.md`

Decision rule used here:
- `real_boundary_leak`: the surface is still creating truth, owning policy/storage logic, or shaping raw SQL in place.
- `explicit_authority_by_design`: the module is the intended operator/frontdoor gate and either delegates to runtime/storage authority or emits a derived read model only.
- `needs_narrower_followthrough`: the behavior is real and bounded, but the surface still owns too much selector/compositor logic to call it cleanly closed.

## `real_boundary_leak`

- `BUG-433129C7156B` `Orient endpoint hardcodes the platform instruction packet`
  `Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py:22-179` hardcodes the endpoint catalog, the `instruction_authority` packet, and the long operator instruction string inline. `Code&DBs/Workflow/surfaces/README.md:3-10` says surfaces render derived views only and must not create truth, so this is still a real surface-owned authority leak.

- `BUG-F792909C6665` `Transport eligibility logic is mixed into workflow_admin`
  `Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py:245-447` does provider-registry validation, live transport probes, direct `provider_model_candidates` SQL, and `TaskTypeRouter.resolve_failover_chain()` inside the HTTP handler. That is policy and routing logic in the request surface, not thin input parsing plus output rendering.

- `BUG-63C0AB529C8D` `Operator read surface is shaping raw SQL in place`
  `Code&DBs/Workflow/surfaces/api/operator_read.py:930-1337` embeds the bug, roadmap, dependency, cutover, binding, and packet-inspection SQL directly in the frontdoor. That conflicts with `Code&DBs/Workflow/surfaces/README.md:3-10`, which says surfaces do not own storage rules.

## `explicit_authority_by_design`

- `BUG-B6D5E1EC557F` `Native primary cutover gate admission is exposed through operator write`
  `Code&DBs/Workflow/surfaces/api/operator_write.py:624-666` delegates admission to `NativePrimaryCutoverRuntime` instead of writing rows ad hoc in a handler. The bounded repository contract for the decision/gate pair lives in `Code&DBs/Workflow/policy/native_primary_cutover.py:379-520`, so this is an explicit operator-control gate.

- `BUG-931EA1E523A9` `Work item closeout mutates bugs and roadmap items in one surface`
  `Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py:526-540` is just a forwarding shim into the shared closeout gate. The actual proof-backed mutation path is intentionally centralized in `Code&DBs/Workflow/surfaces/api/operator_write.py:1302-1539`, and `Code&DBs/Workflow/tests/unit/test_native_operator_work_item_closeout_cli.py:15-66` asserts callers go through that shared gate.

- `BUG-5D2C5B976C0A` `Task route eligibility window is written from operator write`
  `Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py:196-242` only validates input and forwards. `Code&DBs/Workflow/surfaces/api/operator_write.py:668-760` delegates persistence to `Code&DBs/Workflow/storage/postgres/task_route_eligibility_repository.py:18-178`, and `Code&DBs/Workflow/tests/integration/test_task_route_eligibility_write.py:22-87` proves the canonical superseding-window behavior.

- `BUG-DB53A803470F` `Roadmap write commits roadmap rows from the API surface`
  `Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py:471-510` only forwards to the shared roadmap gate. `Code&DBs/Workflow/surfaces/api/operator_write.py:1128-1187` performs preview/commit orchestration and persists via `Code&DBs/Workflow/storage/postgres/roadmap_authoring_repository.py:46-239`; `Code&DBs/Workflow/tests/integration/test_roadmap_write_gate.py:124-234` treats that gate as the intended authority.

- `BUG-31F654D08235` `Native operator surface stitches route, persona, and fork authorities in HTTP`
  `Code&DBs/Workflow/surfaces/api/native_operator_surface.py:1762-1834` explicitly defines the consolidated native operator frontdoor, and `Code&DBs/Workflow/tests/integration/test_native_operator_surface_consolidation.py:30-260` verifies that consolidation contract. The stitching itself is by design; the narrower selector seams are the separate followthrough items below.

- `BUG-118A3F229AC1` `Frontdoor submit assembles canonical admission writes`
  `Code&DBs/Workflow/surfaces/api/frontdoor.py:466-500` plans intake through `WorkflowIntakePlanner`, builds a `WorkflowAdmissionSubmission` in `Code&DBs/Workflow/surfaces/api/frontdoor.py:300-340`, and persists it atomically through `Code&DBs/Workflow/storage/postgres/admission.py:399-439`. `Code&DBs/Workflow/runtime/intake.py:163-340` marks the planner as runtime-owned, and `Code&DBs/Workflow/tests/integration/test_native_frontdoor.py:151-273` treats this submit path as the authoritative repo-local frontdoor.

- `BUG-FD7B1D0E7F19` `Operator query surface computes assessments and closeout recommendations`
  `Code&DBs/Workflow/surfaces/api/operator_read.py:1339-1445` computes derived read-only assessments from canonical rows by calling `Code&DBs/Workflow/runtime/work_item_assessment.py:166-320`. `Code&DBs/Workflow/tests/integration/test_native_operator_query_surface.py:100-227` asserts those assessment and closeout fields are part of the query contract and that the surface remains read-only.

- `BUG-E585032448ED` `Operator smoke-contract resolution is owned by the read surface`
  `Code&DBs/Workflow/surfaces/api/operator_read.py:1756-1800` resolves the smoke contract from the canonical `workflow_definitions` row and `Code&DBs/Workflow/surfaces/api/operator_read.py:1918-1937` reuses it as a bounded diagnostic flow. `Code&DBs/Workflow/tests/integration/test_operator_flow.py:272-352` proves the path is DB-backed and fails closed when the authority row is missing.

- `BUG-691C6690094D` `Operator read surface manufactures instruction authority packets`
  `Code&DBs/Workflow/surfaces/api/operator_read.py:392-445` and `Code&DBs/Workflow/surfaces/api/operator_read.py:675-761` build instruction metadata from the already-loaded snapshot; they are not inventing new durable truth. `Code&DBs/Workflow/tests/integration/test_native_operator_query_surface.py:122-168` treats that packet as the supported derived read-model contract.

## `needs_narrower_followthrough`

- `BUG-BAFE8666079B` `Native operator surface resolves fork/worktree ownership from run and route context`
  `Code&DBs/Workflow/surfaces/api/native_operator_surface.py:1288-1579` reads `workflow_runs`, `workflow_claim_lease_proposal_runtime`, and `fork_worktree_bindings` directly in the surface to derive the selector, and only then delegates to `PostgresPersonaAndForkAuthorityRepository.load_fork_worktree_binding()`. `Code&DBs/Workflow/tests/integration/test_bounded_fork_ownership_adoption.py:349-500` shows the path is real and bounded, but the selector logic is still too surface-owned to call clean.

- `BUG-32887453A9A1` `Native operator surface resolves persona activation from run context`
  `Code&DBs/Workflow/surfaces/api/native_operator_surface.py:1203-1286` reads `workflow_runs.request_envelope` in the frontdoor to recover `workspace_ref` and `runtime_profile_ref`, then calls `PostgresPersonaAndForkAuthorityRepository.load_persona_activation()`. `Code&DBs/Workflow/tests/integration/test_native_operator_persona_adoption.py:106-218` proves this is a real operator path, but the run-context extraction should move behind a narrower authority seam if this cluster is touched again.

- `BUG-B62E4AFDA0C0` `Native operator surface resolves workflow class dispatch in the read compositor`
  `Code&DBs/Workflow/surfaces/api/native_operator_surface.py:1582-1648` combines authoritative work-binding selection with workflow-class and lane-policy matching in the frontdoor after loading `Code&DBs/Workflow/authority/workflow_class_resolution.py:214-241`. That is read-only and authority-backed, but the compositor still owns more dispatch matching than a thin frontdoor should.
