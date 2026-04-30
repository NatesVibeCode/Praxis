# Preview-only payloads — multi-model chat policy/authority changes

These three changes mutate `operator_decisions` and `authority_domains`. Per option (b) of the multi-model chat build, **I am not landing them automatically** — review the rationale and run the commands yourself when you approve.

Hook context: filing operator_decision rows and creating authority domains is a high-severity infrastructure change. The first multi-workflow attempt was correctly denied by the standing-order hook because these are policy-shaped writes that should be operator-authored, not agent-authored.

---

## 1. Retire the operator-console single-lane standing order

**Why:** You stated 2026-04-30 that chat is being re-exposed in the app, so the original "no picker exposed" constraint receded.

**Target:** `operator_decision.architecture_policy.operator_console.together_deepseek_v4_pro_chat_single_lane`

**Apply:**

```bash
bin/praxis-agent praxis_operator_decisions --input-json '{
  "action": "retire",
  "decision_ref": "operator_decision.architecture_policy.operator_console.together_deepseek_v4_pro_chat_single_lane",
  "retired_by": "nate@praxis",
  "retirement_reason": "Operator (nate) explicitly stated 2026-04-30 that chat is being re-exposed in the app, so the original no-picker-exposed constraint no longer applies. Multi-model chat work supersedes — operator console will surface a picker drawer driven by task_type_routing.available_candidates, defaulting to Together V4 Pro and allowing per-turn override."
}' --yes
```

If `praxis_operator_decisions` doesn't accept `retire`, fall back to a numbered migration calling the canonical retirement function.

---

## 2. Justifying decision for `authority.chat_conversations`

**Why:** The forge warned that `authority.workflow_runs` is not a parking lot for unrelated product truth. Chat-turn receipts and events need their own authority boundary so multi-model chat work doesn't pollute the workflow-runs domain.

**New decision row:**

```json
{
  "decision_ref": "operator_decision.architecture_policy.authority_domain.chat_conversations_authority_domain",
  "decision_kind": "architecture_policy",
  "lifecycle": "active",
  "title": "authority.chat_conversations is the durable boundary for chat turns",
  "rationale": "Chat conversations need their own durable authority domain so chat-turn receipts (chat.turn.execute), events (chat.turn_completed), and any future per-conversation product truth do not parking-lot under authority.workflow_runs. Required prerequisite for the chat.turn.execute CQRS-wrap and any multi-participant turn operations. Forge previewed authority.chat_conversations as a new_domain with ok_to_register=false pending this decision_ref.",
  "authored_by": "nate@praxis",
  "tags": ["authority_domain", "chat", "multi_model_chat_build"]
}
```

**Apply:**

```bash
bin/praxis-agent praxis_operator_write --input-json '{
  "action": "create",
  "decision_ref": "operator_decision.architecture_policy.authority_domain.chat_conversations_authority_domain",
  "decision_kind": "architecture_policy",
  "lifecycle": "active",
  "title": "authority.chat_conversations is the durable boundary for chat turns",
  "rationale": "Chat conversations need their own durable authority domain so chat-turn receipts (chat.turn.execute), events (chat.turn_completed), and any future per-conversation product truth do not parking-lot under authority.workflow_runs. Required prerequisite for the chat.turn.execute CQRS-wrap and any multi-participant turn operations. Forge previewed authority.chat_conversations as a new_domain with ok_to_register=false pending this decision_ref.",
  "authored_by": "nate@praxis",
  "tags": ["authority_domain", "chat", "multi_model_chat_build"]
}' --yes
```

(If `praxis_operator_write` field names differ, the canonical writer will surface them — adjust accordingly.)

---

## 3. Register `authority.chat_conversations`

**Depends on #2 above** (decision_ref must exist first).

**Apply:**

```bash
bin/praxis-agent praxis_register_authority_domain --input-json '{
  "authority_domain_ref": "authority.chat_conversations",
  "owner_ref": "praxis.engine",
  "event_stream_ref": "stream.authority.chat_conversations",
  "storage_target_ref": "praxis.primary_postgres",
  "decision_ref": "operator_decision.architecture_policy.authority_domain.chat_conversations_authority_domain"
}' --yes
```

**Verify:**

```bash
bin/praxis-agent praxis_authority_domain_forge --input-json '{
  "authority_domain_ref": "authority.chat_conversations"
}'
# expect state="existing_domain" and ok_to_register=true (or the existing-domain branch)
```

---

## 4. Register `participant_role` data_dictionary types

**Why:** Templates (`ask_all`, `review`) and the multi-participant chat turn op need typed participant roles so the type-flow validator can clamp legal turn graphs (per the "legal-equals-computable-to-non-gap-output" Phase 2 policy). These are type-slug registrations, not policy — borderline mechanical, but mutating `data_dictionary_objects` is still authority-shaped.

**Rows to register:**

| type_slug | description |
|---|---|
| `participant_role.primary` | Speaks first; produces the user-visible answer in single-turn mode. |
| `participant_role.reviewer` | Critiques the primary's output; consumes `primary_output`, produces `critique`. |
| `participant_role.critic` | Adversarial challenge to a primary or synthesizer claim; consumes `proposition`, produces `challenge`. |
| `participant_role.synthesizer` | Consolidates multiple participant outputs; consumes `[primary_output, critic_output, ...]`, produces `synthesis`. |
| `participant_role.observer` | Silent participant; receives turn context but does not produce output. |

**Apply:** Numbered migration after the next CQRS-plumbing migration that lands. Each row: `category=participant_role`, `type_slug=<role>`, `owner_ref=praxis.engine`, description as above.

The worker doing `cqrs_wrap` and `templates` will reference these slugs, so they must exist before those code-work runs lock in their input contracts.

---

## Once #1, #2, #3, #4 are applied

I'll launch `cqrs_wrap` (refactor `_call_llm_with_http_failover` as `chat.turn.execute` command op registered against `authority.chat_conversations`). That's the biggest architectural fix — it turns every chat turn into a CQRS receipt + event. Then `ask_all` and `review` templates depend on `chat.turn.execute` and the `participant_role` types.

The other code work (`picker_query`, `drawer`) doesn't depend on these and is launching now.
