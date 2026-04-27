# Orient Endpoint Audit — p1_foundation

**Job label:** step_1
**Scope:** orient endpoint (CLI `praxis workflow orient`, MCP `praxis_orient`, and `/orient` references)
**Audit run:** 2026-04-27
**Workspace state:** `/workspace` was empty in the review sandbox; source files were not directly readable. Findings are grounded in the praxis knowledge graph (`praxis_query`) — bug records, operator decisions, schema projections, and edge graph — rather than direct source inspection. See finding F-09 below.

## Why orient matters (authority frame)

Orient is bound by the operator decision
`operator_decision.architecture_policy.orient.mandatory_authority_envelope`
which projects to `authority_domain::orient` (edge confirmed via knowledge-graph projection). The decision text is canonical:

> "Fresh operator sessions must begin from the runtime authority envelope instead of sidecar docs. The envelope binds standing orders, workspace boundary, database authority, health, and live operator surface."

That makes orient the single chokepoint for cold-start authority. Anything that degrades orient degrades every downstream session. The findings below are scored against that contract.

## Findings

### F-01 — `/orient` standing_orders projection fails: missing `scope_clamp` column (CRITICAL)
- **Source:** Bug "/orient standing_orders projection fails with missing scope_clamp column" (knowledge graph; `bugs|task`).
- **Symptom:** Both `praxis workflow orient` and `praxis_orient` return `standing_orders unavailable: column "scope_clamp" does not exist`. `praxis_operator_decisions` continues to serve, so the underlying decision data is reachable — but the orient envelope cannot project it.
- **Schema reality (verified via praxis_query → data_dictionary):** the column **does exist** on `operator_decisions` (with default `'{"applies_to":["pending_review"]}'`, plus indexes `operator_decisions_scope_clamp_gin_idx` and `operator_decisions_scope_clamp_pending_review_idx`). The error is therefore not on the source-of-truth table but on the **standing_orders projection / view** the orient handler reads — i.e. a schema/projection drift between `operator_decisions` and the orient-side reducer or view.
- **Impact:** P1. The "mandatory runtime authority envelope" returns a degraded envelope on every cold start. Standing orders are silently dropped from the orient response.

### F-02 — `cli_surface.tool_count` reports zero despite live catalog of 76 tools (HIGH)
- **Source:** Bug "orient cli_surface tool_count reports zero despite live tool catalog" (knowledge graph; `bugs|task`).
- **Symptom:** During the Mac front-door audit, `praxis workflow tools list --json | jq length` returned **76**, but `praxis workflow tools call praxis_orient --input-json '{fast:true, skip_engineering_observability:true}'` reported `cli_surface.tool_count = 0`.
- **Diagnosis:** The orient envelope's `cli_surface` block does not consult the same catalog source the CLI listing does — likely a divergent reader (or the `fast` / `skip_engineering_observability` flag short-circuits the catalog count instead of reading from the live tool registry). Either way, the orient envelope misreports a primary capability metric.
- **Impact:** Agents reading orient at cold-start cannot trust `cli_surface.tool_count` — including agents using it as a precondition gate. Fast-mode answers are wrong rather than partial.

### F-03 — `scripts/praxis workflow orient` hangs without output (HIGH)
- **Source:** Bug "scripts/praxis workflow front door hangs for orient and bugs commands without output" (knowledge graph; `bugs|task`).
- **Symptom:** Canonical shell front door stalls on `./scripts/praxis workflow orient --json`, `./scripts/praxis workflow bugs stats --json`, and `./scripts/praxis workflow bugs list --…` with no stdout.
- **Impact:** Operators / agents using the documented front door cannot orient, even though the underlying MCP path returns (degraded). The CLI front door is the path the operator decision actually mandates; that surface is broken.

### F-04 — `praxis workflow --help` cold start takes ~20 s (MEDIUM)
- **Source:** Bug "workflow help cold start imports too much before showing orientation help" (knowledge graph; `bugs|task`).
- **Symptom:** `praxis workflow --help` no longer requires DB authority, but from outside the repo it takes ~20 seconds before printing static help. Fresh agents and MCP/operator clients can mistake the long pause for a hang.
- **Impact:** Agents will give up or re-issue commands during the silent window, producing duplicate workflows. This is a tax on the very moment orient is supposed to lower friction.

### F-05 — Stale orient/help expectations in CLI surface tests (MEDIUM)
- **Source:** Bug "CLI tools surface tests encode stale orient and help expectations" (knowledge graph; `bugs|task`).
- **Symptom:** `Code&DBs/Workflow/tests/unit/test_cli_tools_surface.py` contains test expectations that no longer match the orient/help output (caught while validating an unrelated CLI/MCP authority patch).
- **Impact:** The orient surface has no reliable regression gate — landed changes can break orient without tripping a test, and conversely, the failing tests are noise during unrelated patches.

### F-06 — Active standing orders flagged as unwired in the wiring audit (MEDIUM)
- **Source:** Bug "[hygiene-2026-04-22/wiring-audit] Active standing orders are reported as unwired decisions" (knowledge graph; `bugs|task`).
- **Symptom:** `praxis_data_dictionary_wiring_audit --input-json '{action:"all"}'` returns 23 `unreferenced_decision` findings, including live standing orders that the operator decision says orient must surface.
- **Tie-in to F-01:** Both findings point at the same gap — the projection contract from `operator_decisions` (with `scope_clamp`) into the orient/standing-orders read model is not wired correctly, and the wiring audit confirms that orient is no longer treated as a referencer of those decisions.

### F-07 — `praxis workflow recall` is unavailable in the same Mac front-door audit (MEDIUM)
- **Source:** Bug "praxis workflow recall fails on operator decision recall payload shape" (knowledge graph; `bugs|task`).
- **Symptom:** During the same Mac front-door audit that exposed F-02, `praxis workflow recall "<canonical operator surface front door CLI API MCP skills Praxis Mac>" --json` returned `status=unavailable` with `error_type=RecallAuthorityError`.
- **Why it counts here:** orient is meant to be paired with `recall` for cold-start (orient gives the envelope; recall surfaces the prior context). With both broken on the same audit run, the operator decision's intent — sessions begin from the envelope rather than sidecar docs — is unmet on Mac front-door.

### F-08 — Process running stale code (INFO)
- **Source:** `praxis_query` response payload contained a `code_drift_signal` block: `code_out_of_date: true`, `drift_seconds: 640`, `scanned_roots: ["runtime","registry","surfaces","storage","adapters"]`, hint: "This process is running stale code… restart the container before trusting subsequent results."
- **Why it matters for an orient audit:** any orient-shape inference taken from this MCP session is potentially against an out-of-date binary. Bugs F-01 through F-07 are corroborated by stable knowledge-graph rows (decisions/bugs), but live-payload measurements (e.g. cli_surface fields, exact response keys) should be re-measured after a daemon bounce.

### F-09 — Empty `/workspace` blocks source-level reverification (INFO)
- **Source:** Direct sandbox observation (`ls -la /workspace` shows only `.` / `..`); reinforced by recent bugs BUG-17CC1088, BUG-632E6F45, and BUG-D2363EB8 ("/workspace started empty"; "submission claims completion but no files were changed on disk"; "bundled praxis CLI shim also fails due missing json stdlib").
- **Impact for this audit:** orient's source files (likely under `Code&DBs/Workflow/surfaces/mcp/tools/` and `Code&DBs/Workflow/runtime/`) cannot be opened in this sandbox; CLI catalog code path `Code&DBs/Workflow/surfaces/mcp/catalog.py` is referenced as `failure | code_unit` in the knowledge graph but unreadable here. Findings F-01 through F-07 stand on KG/bug evidence; full source-level diffing is deferred to a session where the repo is hydrated.

## Conclusions

The orient envelope is, by operator decision, the single mandatory cold-start authority for fresh sessions — yet it is currently degraded on at least three independent dimensions: (1) the standing_orders projection fails on a `scope_clamp` schema mismatch (F-01), (2) the `cli_surface.tool_count` value silently lies under fast/skip flags (F-02), and (3) the documented shell front door hangs (F-03). The wiring audit and stale tests (F-05, F-06) say there is also no live regression gate to catch further drift. None of these are isolated UX papercuts: they hit the contract that every other session leans on.

## Recommendations

1. **Fix F-01 first (P1).** Audit the standing_orders read-model definition (view/projection that the orient handler queries) and align its column set with the current `operator_decisions` schema — `scope_clamp` is present on the source table with two GIN indexes, so the consumer is the broken party. Add a registered verifier that calls `praxis_orient` and asserts `standing_orders.status != "unavailable"` on a fresh DB.
2. **Reconcile `cli_surface.tool_count` against the live catalog (F-02).** Make orient read from the same registry `praxis workflow tools list` reads, or make the `fast`/`skip_engineering_observability` flags exclude only observability subblocks, not catalog metrics. Add an assertion: `orient.cli_surface.tool_count == len(tools_list())`.
3. **Diagnose the front-door hang (F-03).** The CLI path is what the operator decision actually mandates; restore parity between `./scripts/praxis workflow orient --json` and `praxis workflow tools call praxis_orient`, even when degraded — surface degradation, do not stall.
4. **Trim cold-start imports (F-04).** Lazy-import everything that `praxis workflow --help` does not need until first command dispatch. Target sub-second help output from outside the repo.
5. **Refresh `tests/unit/test_cli_tools_surface.py` orient/help expectations (F-05) and add an orient envelope contract test** that pins the keys orient must return (`standing_orders`, `cli_surface`, `workspace_boundary`, `database_authority`, `health`, `operator_surface`). Without this, F-01/F-02-class regressions will keep landing silently.
6. **Re-run the wiring audit after F-01 lands (F-06).** The 23 unreferenced standing-order decisions should drop once the projection is wired again.
7. **Pair-fix orient + recall (F-07).** Ship a Mac front-door smoke that calls orient *and* recall, since the operator decision binds the two as the cold-start replacement for sidecar docs.
8. **Re-run this audit against a hydrated workspace (F-09) and after a daemon bounce (F-08)** so that orient source files and a non-stale runtime can confirm or extend the bug-grounded findings here.

## Links

- Operator decision: `operator_decision.architecture_policy.orient.mandatory_authority_envelope` → `authority_domain::orient`
- Bug (knowledge graph): `/orient standing_orders projection fails with missing scope_clamp column`
- Bug (knowledge graph): `orient cli_surface tool_count reports zero despite live tool catalog`
- Bug (knowledge graph): `scripts/praxis workflow front door hangs for orient and bugs commands without output`
- Bug (knowledge graph): `workflow help cold start imports too much before showing orientation help`
- Bug (knowledge graph): `CLI tools surface tests encode stale orient and help expectations`
- Bug (knowledge graph): `[hygiene-2026-04-22/wiring-audit] Active standing orders are reported as unwired decisions`
- Bug (knowledge graph): `praxis workflow recall fails on operator decision recall payload shape`
- Sandbox runtime bugs: BUG-17CC1088, BUG-632E6F45, BUG-D2363EB8 (workspace hydration / phantom_ship)
- Code path (referenced, not readable from this sandbox): `Code&DBs/Workflow/surfaces/mcp/catalog.py`
- Test path (referenced): `Code&DBs/Workflow/tests/unit/test_cli_tools_surface.py`
- Schema row (verified): `operator_decisions.scope_clamp jsonb` with default `'{"applies_to":["pending_review"]}'`
