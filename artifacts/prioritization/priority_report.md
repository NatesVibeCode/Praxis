# Praxis Roadmap + Bug Priority Review

Generated from DB-backed authority reads on 2026-04-27. Roadmap rows: 16. Bug rows: 445. Bug stats: total 445, open 175, pending verification 7, fixed 251, deferred 9, wont-fix 3.

## Verdict
The critical path is authority before features: foundation readback and health truth, then provider/routing/credential authority, then DB/CQRS proof surfaces, then durable work execution, then compiler reliability, then cockpit/adapter work. Several bug rows are misleading queue entries and should be quarantined or merged instead of implemented as stated.

## Roadmap Priority
|priority_rank|priority_group|title|status|severity_or_priority|recommended_action|
|---|---|---|---|---|---|
|R00|Umbrella, not a build packet|A2A-native stateful server for agentic work|active|p1|Keep as governing initiative; do not assign directly.|
|R01|Foundation first|Foundation reliability and authority health|active|p1|Fix cold-start authority, health truth, graph projection, worker force-fail, and query/readback reliability.|
|R02|Protocol contract|A2A protocol surface|active|p1|Define the external A2A task/message/artifact/status surface after foundation reads are trustworthy.|
|R03|DB/CQRS kernel|A2A DB and CQRS kernel|active|p1|Map A2A objects to durable work programs, receipts, events, evidence, and query surfaces.|
|R04|Agent registry and routing|Agent registry and routing control plane|active|p1|Make provider/agent availability, credentials, circuit breakers, costs, and denial reasons queryable and fail-closed.|
|R05|Durable work programs|Durable work programs over A2A tasks|active|p1|Give agents durable resume state across tasks, attempts, blockers, decisions, artifacts, and recommended next actions.|
|R06|Compiler parent|Intent-aware synthesis prompt compiler|active|p1|Treat as current compile reliability wave under durable work programs; reassess scope because related files changed.|
|R06.1|Compiler: recognized steps|Wire recognize_intent output into synthesis prompt|active|p1|Highest leverage: stop the prompt from inventing 20 packets when intent already has recognized steps.|
|R06.2|Compiler: seed schema only|Slim PlanPacket schema in synthesis to seed-only fields|active|p1|Remove schema confusion: synthesis should see only fields it emits.|
|R06.3|Compiler: ranked data pills|Embedding-rank pill suggestions instead of alphabetical pull|active|p1|Stop irrelevant tool-parameter pills from steering plans.|
|R06.4|Compiler: stage filtering|Filter stage dictionary by recognized verbs|active|p2|Only include stages reachable from recognized verbs.|
|R06.5|Compiler: typed output example|Replace OUTPUT placeholder template with rendered typed example|active|p2|Replace metavariable placeholder with concrete JSON shape.|
|R06.6|Compiler: temp tweak|Synthesis temperature 0.2 instead of 0.0|active|p2|Use only after shape fixes; tiny stochasticity is not an authority substitute.|
|R06.7|Compiler: prior examples|Few-shot worked example retrieved from prior compile receipts|active|p1|Ship after receipts/examples can be retrieved by authority and similarity.|
|R07|Operator cockpit|Operator cockpit for stateful A2A work|active|p1|Build human control view after the underlying read models are authoritative.|
|R08|Adapter consolidation|Adapter consolidation into the A2A-native kernel|active|p1|Collapse MCP/CLI/API/A2A ingress into one work-program model after the kernel is real.|

## Bug Priority Groups
- closed_history: 251
- queue_poison_quarantine: 40
- db_cqrs_receipts_evidence: 29
- provider_routing_credentials: 27
- backlog_low_signal: 15
- foundation_authority: 10
- security_input_validation: 9
- cockpit_ui_readmodels: 8
- local_ops_and_docs: 8
- symptom_not_root_cause: 6
- adapter_consolidation: 6
- workspace_runtime_boundary: 5
- verify_provider_catalog_fix: 4
- unmapped_p1_review: 4
- compile_plan_generation: 4
- deferred_until_dependency: 4
- durable_work_execution: 3
- deferred_not_live_authority: 3
- not_real_or_rejected: 3
- verify_foundation_fix: 2
- verify_or_close_stale_deferred: 2
- verify_pending_fix: 1
- not_a_failure_without_expectation: 1

## First 40 Real Bug Actions
|priority_rank|priority_group|id|severity_or_priority|status|title|recommended_action|
|---|---|---|---|---|---|---|
|B001 (110)|foundation_authority|BUG-BD989209|P1|OPEN|/orient standing_orders projection fails with missing scope_clamp column|Fix early: these surfaces tell agents what is true; leaving them broken poisons every other decision.|
|B002 (110)|foundation_authority|BUG-0C0C55B1|P1|OPEN|Worker force-fails every admitted run with workflow.execution_crash; effective workflow-runtime outage|Fix early: these surfaces tell agents what is true; leaving them broken poisons every other decision.|
|B003 (110)|foundation_authority|BUG-1DBACCD8|P1|OPEN|[hygiene-2026-04-22/db-authority] Bug surface and documented Postgres fallback read different bug tables|Fix early: these surfaces tell agents what is true; leaving them broken poisons every other decision.|
|B004 (110)|foundation_authority|BUG-026AB2E7|P1|OPEN|praxis_graph_projection fails on empty decision_ref row|Fix early: these surfaces tell agents what is true; leaving them broken poisons every other decision.|
|B005 (110)|foundation_authority|BUG-D39EBC3F|P1|OPEN|workflow health reports healthy while projections are critical and routes are unhealthy|Fix early: these surfaces tell agents what is true; leaving them broken poisons every other decision.|
|B046 (115)|verify_foundation_fix|BUG-2907B68C|P2|FIX_PENDING_VERIFICATION|Worker background-consumer loop dies with NameError: _evaluate_ready_specs not defined|Verify and close before new foundation work; pending rows distort outage state.|
|B047 (115)|verify_foundation_fix|BUG-D2ED53B4|P2|FIX_PENDING_VERIFICATION|WorkflowMigrationError on praxis-api-server boot|Verify and close before new foundation work; pending rows distort outage state.|
|B048 (130)|foundation_authority|BUG-F8C9F5B5|P2|OPEN|[hygiene-2026-04-24/db-authority-orphans] Data dictionary still exposes DB authority tables with no production runtime owner|Fix early: these surfaces tell agents what is true; leaving them broken poisons every other decision.|
|B049 (130)|foundation_authority|BUG-507AB442|P2|OPEN|operator_write commit succeeds without roadmap item readback|Fix early: these surfaces tell agents what is true; leaving them broken poisons every other decision.|
|B050 (130)|foundation_authority|BUG-91D41A89|P2|OPEN|praxis query routes readback questions to empty quality_views rollup|Fix early: these surfaces tell agents what is true; leaving them broken poisons every other decision.|
|B051 (130)|foundation_authority|BUG-17601F93|P2|OPEN|praxis workflow query "is the workflow runtime healthy" returns overall=healthy while worker force-fails every run|Fix early: these surfaces tell agents what is true; leaving them broken poisons every other decision.|
|B052 (130)|foundation_authority|BUG-247C050D|P2|OPEN|roadmap tree view omits direct child roadmap items|Fix early: these surfaces tell agents what is true; leaving them broken poisons every other decision.|
|B053 (135)|workspace_runtime_boundary|BUG-9BB04947|P1|OPEN|Database environment authority is split across shell bootstrap, runtime, surface resolvers, and tests|Fix after core foundation: wrong workspace or database authority makes all later validation suspect.|
|B054 (135)|workspace_runtime_boundary|BUG-A5FE235C|P1|OPEN|Native cutover can leave workspace and database authority split across old mount and local runtime|Fix after core foundation: wrong workspace or database authority makes all later validation suspect.|
|B055 (135)|workspace_runtime_boundary|BUG-90E70AA6|P1|OPEN|OrbStack migration can leave Docker authority unavailable with root-owned or corrupted VM data|Fix after core foundation: wrong workspace or database authority makes all later validation suspect.|
|B056 (135)|workspace_runtime_boundary|BUG-46A6C7F2|P1|OPEN|Workspace and scope boundary authority is spread across runtime, registry, adapters, shell wrappers, and tests|Fix after core foundation: wrong workspace or database authority makes all later validation suspect.|
|B057 (155)|workspace_runtime_boundary|BUG-96F12329|P2|OPEN|Global praxis launcher can route outside the active workspace and hide repo-local fixes|Fix after core foundation: wrong workspace or database authority makes all later validation suspect.|
|B058 (205)|verify_provider_catalog_fix|BUG-EBE27625|P1|FIX_PENDING_VERIFICATION|CQRS provider capability matrix is missing for effective provider job catalog|Verify Group A provider-catalog fixes first; later provider work depends on this contract.|
|B059 (205)|verify_provider_catalog_fix|BUG-5444AA3C|P1|FIX_PENDING_VERIFICATION|Route explanation CQRS is missing for provider/model removal reasons|Verify Group A provider-catalog fixes first; later provider work depends on this contract.|
|B060 (205)|verify_provider_catalog_fix|BUG-1B959922|P1|FIX_PENDING_VERIFICATION|Runtime profiles hardcode provider env vars and model allowlists outside effective catalog|Verify Group A provider-catalog fixes first; later provider work depends on this contract.|
|B061 (205)|verify_provider_catalog_fix|BUG-D4CC68A9|P1|FIX_PENDING_VERIFICATION|provider_transport built-in defaults remain executable fallback authority|Verify Group A provider-catalog fixes first; later provider work depends on this contract.|
|B062 (210)|provider_routing_credentials|BUG-724759AE|P1|OPEN|Circuit breaker authority unavailable skips provider preflight|Fix before broad execution work: provider capability must be catalog-backed, explainable, credential-aware, and fail-closed.|
|B063 (210)|provider_routing_credentials|BUG-023252F7|P1|OPEN|Docker and sandbox setup install and mount provider CLIs outside catalog authority|Fix before broad execution work: provider capability must be catalog-backed, explainable, credential-aware, and fail-closed.|
|B064 (210)|provider_routing_credentials|BUG-2D9A6DED|P1|OPEN|Manual circuit breaker override query failure erases force-open decisions|Fix before broad execution work: provider capability must be catalog-backed, explainable, credential-aware, and fail-closed.|
|B065 (210)|provider_routing_credentials|BUG-ADAEB359|P1|OPEN|Permission matrix hardcodes provider allowlists outside provider catalog|Fix before broad execution work: provider capability must be catalog-backed, explainable, credential-aware, and fail-closed.|
|B066 (210)|provider_routing_credentials|BUG-EEE3E88E|P1|OPEN|Provider routing and admission authority is duplicated across registry, adapters, runtime profiles, and transport code|Fix before broad execution work: provider capability must be catalog-backed, explainable, credential-aware, and fail-closed.|
|B067 (210)|provider_routing_credentials|BUG-5DFF1C68|P1|OPEN|Provider transport admission filter fail-opens when admissions table is missing|Fix before broad execution work: provider capability must be catalog-backed, explainable, credential-aware, and fail-closed.|
|B068 (210)|provider_routing_credentials|BUG-BF734C00|P1|OPEN|Refactor secrets OAuth and credential resolution spread across adapters registry runtime and memory|Fix before broad execution work: provider capability must be catalog-backed, explainable, credential-aware, and fail-closed.|
|B069 (210)|provider_routing_credentials|BUG-70706DC9|P1|OPEN|Sandbox auth seeding hardcodes provider credential homes outside credential CQRS|Fix before broad execution work: provider capability must be catalog-backed, explainable, credential-aware, and fail-closed.|
|B070 (210)|provider_routing_credentials|BUG-2337DB51|P1|OPEN|Secret and credential resolution is spread across keychain, OAuth, env forwarding, provider transport, and execution backends|Fix before broad execution work: provider capability must be catalog-backed, explainable, credential-aware, and fail-closed.|
|B071 (210)|provider_routing_credentials|BUG-F65A9A98|P1|OPEN|Together API decoder corruption in V4-Pro responses|Fix before broad execution work: provider capability must be catalog-backed, explainable, credential-aware, and fail-closed.|
|B072 (210)|provider_routing_credentials|BUG-25829630|P1|OPEN|[architecture] Build per-sandbox per-provider credential authority to replace launch-context credential forwarding|Fix before broad execution work: provider capability must be catalog-backed, explainable, credential-aware, and fail-closed.|
|B073 (210)|provider_routing_credentials|BUG-25224975|P1|OPEN|[hygiene-2026-04-22/secret-authority] OAuth refresh reads client credentials directly from process env|Fix before broad execution work: provider capability must be catalog-backed, explainable, credential-aware, and fail-closed.|
|B074 (210)|provider_routing_credentials|BUG-2CF335E3|P1|OPEN|[hygiene-2026-04-22/secret-authority] Sandbox env assembly copies host env and dotenv before secret allowlist|Fix before broad execution work: provider capability must be catalog-backed, explainable, credential-aware, and fail-closed.|
|B075 (210)|provider_routing_credentials|BUG-FD42FE1D|P1|OPEN|compile/submission path calls api_llm despite operator decision + registry binding cli_llm — MCP timeouts on multi-packet submissions whe...|Fix before broad execution work: provider capability must be catalog-backed, explainable, credential-aware, and fail-closed.|
|B076 (230)|provider_routing_credentials|BUG-80CF188A|P2|OPEN|Compiler route catalog bypasses effective provider job catalog|Fix before broad execution work: provider capability must be catalog-backed, explainable, credential-aware, and fail-closed.|
|B077 (230)|provider_routing_credentials|BUG-DF343C7D|P2|OPEN|Credential availability is not projected into provider capability CQRS|Fix before broad execution work: provider capability must be catalog-backed, explainable, credential-aware, and fail-closed.|
|B078 (230)|provider_routing_credentials|BUG-1DEEBF2D|P2|OPEN|Implement provider onboarding capacity probing for all protocol families|Fix before broad execution work: provider capability must be catalog-backed, explainable, credential-aware, and fail-closed.|
|B079 (230)|provider_routing_credentials|BUG-C7B9D6F2|P2|OPEN|Provider cost and budget posture is not exposed through effective catalog CQRS|Fix before broad execution work: provider capability must be catalog-backed, explainable, credential-aware, and fail-closed.|
|B080 (230)|provider_routing_credentials|BUG-91A91284|P2|OPEN|Refactor provider routing and runtime-profile admission spread across registry runtime and adapters|Fix before broad execution work: provider capability must be catalog-backed, explainable, credential-aware, and fail-closed.|

## Misleading / Not-Real / Harmful Rows To Quarantine
|priority_rank|priority_group|id|status|title|recommended_action|
|---|---|---|---|---|---|
|B006 (110)|queue_poison_quarantine|bug.2a66ce827c.activity-truth.cockpit|OPEN|Activity truth evidence drift requires explicit review|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B007 (110)|queue_poison_quarantine|bug.67f9968048.activity-truth.cockpit|OPEN|Activity truth evidence drift requires explicit review|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B008 (110)|queue_poison_quarantine|bug.7e0f249aa7.activity-truth.cockpit|OPEN|Activity truth evidence drift requires explicit review|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B009 (110)|queue_poison_quarantine|bug.9557bfd3bc.activity-truth.cockpit|OPEN|Activity truth evidence drift requires explicit review|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B010 (110)|queue_poison_quarantine|bug.6bd2c3bee7.activity-truth.cockpit|OPEN|Activity truth evidence drift requires explicit review|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B011 (110)|queue_poison_quarantine|bug.d48079a039.activity-truth.cockpit|OPEN|Activity truth evidence drift requires explicit review|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B012 (110)|queue_poison_quarantine|bug.3bab0a06e3.activity-truth.cockpit|OPEN|Activity truth evidence drift requires explicit review|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B013 (110)|queue_poison_quarantine|bug.4379d5d19c.activity-truth.cockpit|OPEN|Activity truth evidence drift requires explicit review|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B014 (110)|queue_poison_quarantine|bug.227b891bbf.activity-truth.cockpit|OPEN|Activity truth evidence drift requires explicit review|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B015 (110)|queue_poison_quarantine|bug.f2d2213feb.activity-truth.cockpit|OPEN|Activity truth evidence drift requires explicit review|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B016 (110)|queue_poison_quarantine|bug.044572c78f.activity-truth.cockpit|OPEN|Activity truth evidence drift requires explicit review|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B017 (110)|queue_poison_quarantine|bug.780c33a110.activity-truth.cockpit|OPEN|Activity truth evidence drift requires explicit review|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B018 (110)|queue_poison_quarantine|bug.7ff24e26a4.other.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B019 (110)|queue_poison_quarantine|bug.7ff24e26a4.governing.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B020 (110)|queue_poison_quarantine|bug.18b8f30517.other.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B021 (110)|queue_poison_quarantine|bug.18b8f30517.governing.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B022 (110)|queue_poison_quarantine|bug.db79c7cf95.other.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B023 (110)|queue_poison_quarantine|bug.db79c7cf95.governing.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B024 (110)|queue_poison_quarantine|bug.701142f92e.other.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B025 (110)|queue_poison_quarantine|bug.701142f92e.governing.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B026 (110)|queue_poison_quarantine|bug.5f7f34b6e5.other.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B027 (110)|queue_poison_quarantine|bug.5f7f34b6e5.governing.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B028 (110)|queue_poison_quarantine|bug.7c47901e45.other.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B029 (110)|queue_poison_quarantine|bug.7c47901e45.governing.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B030 (110)|queue_poison_quarantine|bug.4402d457dd.other.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B031 (110)|queue_poison_quarantine|bug.4402d457dd.governing.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B032 (110)|queue_poison_quarantine|bug.fba4f99a60.other.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B033 (110)|queue_poison_quarantine|bug.fba4f99a60.governing.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B034 (110)|queue_poison_quarantine|bug.fc24c406c2.other.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B035 (110)|queue_poison_quarantine|bug.fc24c406c2.governing.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B036 (110)|queue_poison_quarantine|bug.cf5203b26d.other.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B037 (110)|queue_poison_quarantine|bug.cf5203b26d.governing.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B038 (110)|queue_poison_quarantine|bug.2bd43612d2.other.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B039 (110)|queue_poison_quarantine|bug.2bd43612d2.governing.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B040 (110)|queue_poison_quarantine|bug.98a0019d7b.other.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B041 (110)|queue_poison_quarantine|bug.98a0019d7b.governing.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B042 (110)|queue_poison_quarantine|bug.c131a81647.other.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B043 (110)|queue_poison_quarantine|bug.c131a81647.governing.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B044 (110)|queue_poison_quarantine|bug.3a960d0db7.other.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B045 (110)|queue_poison_quarantine|bug.3a960d0db7.governing.cockpit|OPEN|Cockpit truth binding bug|Quarantine/merge first: generated duplicate with vague title and no evidence context; harmful to future prioritizers.|
|B130 (430)|symptom_not_root_cause|BUG-EAD36E8A|OPEN|Repeated receipt failure: route.unhealthy|Do not patch this row directly; solve the root failure class and attach/merge evidence.|
|B131 (430)|symptom_not_root_cause|BUG-9CA0CB78|OPEN|Repeated receipt failure: sandbox_error|Do not patch this row directly; solve the root failure class and attach/merge evidence.|
|B132 (430)|symptom_not_root_cause|BUG-7C5D8AE4|OPEN|Repeated receipt failure: sandbox_error|Do not patch this row directly; solve the root failure class and attach/merge evidence.|
|B133 (430)|symptom_not_root_cause|BUG-39D02693|OPEN|Repeated receipt failure: sandbox_error|Do not patch this row directly; solve the root failure class and attach/merge evidence.|
|B134 (430)|symptom_not_root_cause|BUG-A3B51E8D|OPEN|Repeated receipt failure: workflow.timeout|Do not patch this row directly; solve the root failure class and attach/merge evidence.|
|B135 (430)|symptom_not_root_cause|BUG-1980557E|OPEN|Repeated receipt failure: workflow_submission.required_missing|Do not patch this row directly; solve the root failure class and attach/merge evidence.|
|B180 (720)|not_a_failure_without_expectation|BUG-D7801F5D|OPEN|No existing workflow receipts for ui_atlas_refinement wave IDs before edit|Likely an absence observation, not a defect. Convert to evidence note or close unless a missing receipt contract is proven.|
|B189 (805)|deferred_not_live_authority|BUG-698CCCF9|DEFERRED|Cascade queue specs contain hardcoded provider routing guidance outside catalog|Do not solve now; the record itself says the premise is not live authority. Keep out of active queues.|
|B190 (805)|deferred_not_live_authority|BUG-E17B3DFB|DEFERRED|Proof and install scripts hardcode provider/model slugs outside catalog authority|Do not solve now; the record itself says the premise is not live authority. Keep out of active queues.|
|B191 (805)|deferred_not_live_authority|BUG-C803B5EB|DEFERRED|Provider picker and helper surfaces bypass effective provider catalog|Do not solve now; the record itself says the premise is not live authority. Keep out of active queues.|
|B192 (850)|not_real_or_rejected|BUG-5A882BAB|WONT_FIX|ButtonRowModule config POSTs to arbitrary endpoints; any manifest can fire cross-surface requests|Do not solve. Keep only if the rejected rationale is useful; otherwise hide from default queues.|
|B193 (850)|not_real_or_rejected|BUG-4820B3A1|WONT_FIX|probe|Do not solve. Keep only if the rejected rationale is useful; otherwise hide from default queues.|
|B194 (850)|not_real_or_rejected|BUG-6C5D628C|WONT_FIX|test placeholder|Do not solve. Keep only if the rejected rationale is useful; otherwise hide from default queues.|

## Full Table
Full row-by-row priority for every roadmap item and every bug is in `artifacts/prioritization/all_items_priority.csv`.
