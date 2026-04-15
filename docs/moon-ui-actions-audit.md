# Moon UI Action Audit

Updated source audit for the Moon builder as of 2026-04-14.

## Scope
- Frontend: `Code&DBs/Workflow/surfaces/app/src/moon/*`
- Shared build transforms: `Code&DBs/Workflow/surfaces/app/src/shared/buildGraphDefinition.ts`
- API handler: `Code&DBs/Workflow/surfaces/api/handlers/workflow_query.py`
- Planner/runtime: `Code&DBs/Workflow/runtime/operating_model_planner.py`, `Code&DBs/Workflow/runtime/build_authority.py`

## Current Count
- Step action buttons traced: `12`
- Distinct runnable lanes: `9`
- Aliases: `2`
- Missing verified lane: `1`
- Gate buttons traced: `6`
- Gates that affect execution today: `2`
- Gates that only persist metadata today: `4`

## Curated Surface Cut
- Core step buttons now: `9`
- Advanced / later step buttons: `0`
- Removed step buttons from the main UI: `3`
- Core gate buttons now: `2`
- Advanced / later gate buttons: `2`
- Removed gate buttons from the main UI: `2`

## Gate Interaction Model
- Every connection now renders a visible midpoint gate pod instead of relying on the thin edge line as the primary affordance.
- `build_graph.edges[].release` is now the canonical edge-release authority for Moon, build projection, planner, and runtime mapping.
- New graphs and persisted edge gates now write only the canonical `release` object for edge control semantics.
- Old top-level gate fields are accepted only as a narrow read-compat shim while older saved records are reopened.
- Empty connections expose the only two inline gate actions Moon trusts today: `Branch` and `On Failure`.
- The detail dock now mirrors that product cut with three explicit buckets: `Control now`, `Worth building later`, and `Hard choices`.
- `/api/catalog` now ships truth and surface policy metadata so the backend, action dock, popout, and gate editor classify the same catalog row the same way.
- Conditional branches now edit through a Then/Else composer first, with JSON kept as an escape hatch for nested condition trees.
- `On Failure` now edits as a structural failure path with one honest reset control, not as a fake fallback-settings form.
- `Approval` and `Validation` stay in the detail dock as preview-only advanced shapes instead of mutating edge state.
- `Human Review` and `Retry` stay off the inline gate surface entirely.
- Selecting a gate no longer needs to mean "open a dock immediately"; the pod is now the first-class entry point and the dock is the deeper editor.
- `Fan Out` now has a verified runtime lane and is no longer treated as a hidden builder-only concept.

## Summary
- The old first-pass assumption that most Moon node actions were decorative is no longer accurate.
- Ready node actions are preserved through `build_graph -> definition.execution_setup.phases -> compiled_spec.jobs`.
- Trigger buttons are preserved through `build_graph -> trigger_intent -> compiled_spec.triggers`.
- Gate truth is split:
  - `conditional` and `after_failure` change planned dependency behavior now.
  - `approval`, `human_review`, `validation`, and `retry` are persisted into `execution_setup.edge_gates`, but the planner does not turn them into executable control behavior yet.
- Product cut for the main Moon surface:
  - Keep only the primitives that are already crisp and trustworthy in the primary UI.
  - Keep promising but not-yet-clean actions in an advanced/later bucket.
  - Remove aliases and wrong-shaped controls from the main surface entirely.

## Hard Choices

| Button | Decision | Why |
| --- | --- | --- |
| `Docs` | remove from main UI | Same route as `Web Research`; one route should have one obvious button. |
| `Retry` | remove from gate UI | Retry is job-level runtime policy, not edge semantics. |
| `Human Review` | remove from main UI | Collapse into one future human gate concept: `Approval`. |
| `Notify` | keep in core | Real route with an opinionated config surface in the inspector. |
| `HTTP Request` | keep in core | Real route with presets, method, header, and body controls in the inspector. |
| `Run Workflow` | keep in core | Real route with saved-workflow selection in the inspector. |
| `Fan Out` | hide from main UI | Real concept, but still missing one verified Moon-to-runtime lane. |
| `Validation` | keep as preview-only advanced | Worth building, but should become real verification policy rather than decorative edge metadata. |
| `Approval` | keep as preview-only advanced | Worth building once the graph runtime has one clean human-in-the-loop primitive. |

## Dashboard Buttons

These are wired to real navigation or API mutations now:

| Button | Actual behavior |
| --- | --- |
| `+ New Operating Model` | Opens Moon builder in operating-model flow. |
| `+ Workflow Builder` | Opens build surface for a new workflow. |
| `Ask anything...` | Opens chat panel. |
| `+ Add to Knowledge Base` | Opens file picker and uploads to `/api/files`. |
| knowledge-base `x` | Deletes uploaded instance file through `/api/files/{id}`. |
| workflow list/sidebar items | Opens the selected workflow/model editor. |
| `Describe It` | Opens operating-model flow. |
| `Start from Scratch` | Opens empty workflow builder. |
| `View Results` | Opens run detail for latest run. |
| `Edit` | Opens workflow/model editor. |
| `Run Now` | Calls `/api/trigger/{workflow_id}`. |
| `Delete` | Calls `/api/workflows/delete/{workflow_id}` after confirmation. |
| recent run rows | Open run detail. |

## Moon Step Buttons

These are the step/action buttons shown in the trigger picker, node popout, and action catalog.

| Button | Route / effect | Truth class | Notes |
| --- | --- | --- | --- |
| `Manual` | `trigger` | runnable | Compiles into `trigger_intent` and then into a real manual trigger. |
| `Webhook` | `trigger/webhook` | runnable | Preserves webhook trigger config into `trigger_intent` and `compiled_spec.triggers`. |
| `Schedule` | `trigger/schedule` | runnable | Preserves cron config into `trigger_intent` and `compiled_spec.triggers`. |
| `Web Research` | `auto/research` | runnable | Planned as a real job route. |
| `Docs` | `auto/research` | alias | Same route as `Web Research` today; not distinct functionality. |
| `Classify` | `auto/classify` | runnable | Planned as a real job route. |
| `Draft` | `auto/draft` | runnable | Planned as a real job route. |
| `Fan Out` | `workflow.fanout` | runnable | Real capability target with a verified builder-to-runtime lane. |
| `Fan Out (Legacy)` | `auto/fan-out` | alias | Compatibility token kept so older saved graphs still open cleanly. |
| `Notify` | `@notifications/send` | runnable | Real platform integration path, but still requires downstream message config to be useful. |
| `HTTP Request` | `@webhook/post` | runnable | Real webhook integration path, but requires URL/auth/body config. |
| `Run Workflow` | `@workflow/invoke` | runnable | Real workflow invoke path, but requires target workflow config. |

## Moon Gate Buttons

| Button | Gate family | Truth class | Notes |
| --- | --- | --- | --- |
| `Approval` | `approval` | saved only | Stored into `execution_setup.edge_gates`; Moon now shows it as a preview-only advanced gate until planner enforcement exists. |
| `Human Review` | `human_review` | saved only | Stored, not executable yet. |
| `Validation` | `validation` | saved only | Stored, not compiled into runtime validation flow yet; shown as preview-only in Detail. |
| `Branch` | `conditional` | executable | Compiles into `dependency_edges` with `edge_type=conditional`. |
| `Retry` | `retry` | saved only | Stored, not compiled into retry policy yet. |
| `On Failure` | `after_failure` | executable | Compiles into `dependency_edges` with `edge_type=after_failure`. |

## Builder / Editor Buttons

| Button | Actual behavior |
| --- | --- |
| `Choose a trigger` | Opens trigger picker. |
| trigger picker items | Seed a local `build_graph` immediately. |
| `Or describe in words` | Switches to prose compile flow. |
| example prompt chips | Fill the prose box only. |
| `Build` | Calls `/api/compile`; compiler output replaces local build state. |
| `Refine` | Calls `/api/refine-definition`; recompiles from prose against current definition endpoint behavior. |
| `Save draft` | Creates or commits workflow definition through `/api/workflows` or `/api/commit`. |
| dock half-moons | Open or close docks only. |
| node popout action buttons | Assign node route and persist `build_graph`; they do not execute immediately. |
| append `+` | Adds a new blank node and persists `build_graph` when workflow id exists. |
| gate pod | Selects a connection and shows the inline core gate actions Moon trusts today. |
| `Branch above` / `Branch below` | Creates conditional branch edges and persists `build_graph`. |
| `On Failure` (gate pod) | Applies `after_failure` to the edge and persists `build_graph`. |
| `Save trigger` | Saves trigger config into `build_graph`; later compiles into `trigger_intent`. |
| DB object chips | Attach object types through `/build/attachments`. |
| `Approve` import | Calls `/build/imports/{snapshot_id}/admit`. |
| `Attach` | Calls `/build/attachments`. |
| `Stage` | Calls `/build/imports`. |
| `Materialize here` | Calls `/build/materialize-here`. |
| binding target buttons | Call `/build/bindings/{binding_id}/accept`. |
| `Custom target` / `Use` | Call `/build/bindings/{binding_id}/replace`. |
| `Reject` | Calls `/build/bindings/{binding_id}/reject`. |
| `Save branch condition` | Persists branch condition JSON into `build_graph`. |

## Release / Run Buttons

| Button | Actual behavior |
| --- | --- |
| `Preview plan` | Calls `/api/plan`; does not persist or dispatch. |
| `Dispatch` | Starts confirmation after a plan exists. |
| `Confirm Release` | Creates workflow if needed, commits definition/plan, then calls `/api/trigger/{workflow_id}`. |
| `Fix` | Navigates to the blocking node/dock only. |
| `View Run` | Opens run panel or run detail. |
| `Re-run` | Calls `/api/trigger/{workflow_id}` from the run panel. |
| run history rows | Switch active run in the run panel. |
| run graph nodes / job rows | Expand existing run/job detail only. |

## UI Direction

The right polish move is not hiding everything that is not perfect. It is making the truth obvious:

- Step buttons should advertise whether they become a real runtime route, alias another route, or lack a verified lane.
- Gate buttons should advertise whether they affect execution now or only persist metadata.
- The builder should keep compile/plan/dispatch controls visually distinct from route-assignment controls.
