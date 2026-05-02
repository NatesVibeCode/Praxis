# Operator Console (God-Mode)

Chat-first agent dispatcher for the single operator. Mobile-ergonomic, Tailscale-gated, dev-only. Not a product surface — a tool for Nate to build while away from keyboard and, longer term, a persistent home-agent dispatcher.

## Scope

The operator console is **not**:

- Canvas. Canvas is the graph-first product surface for workflow authoring and review. The operator console is a separate chat surface with no canvas.
- A public / external-user product. External-user mobile was archived on 2026-04-24 (`decision.2026-04-24.mobile-v1-archived`) because it conflated two incompatible use cases. The console serves exactly one principal.
- A standalone mobile app. It is a responsive web page served by the existing API, reached over Tailscale from phone or laptop.

The operator console is:

- A chat interface over the CLIs and APIs the operator already uses (Claude Code CLI, Codex CLI, Gemini CLI, and direct provider APIs once B.1 expands).
- A normalized permission control panel that hides each provider's native permission surface behind a common vocabulary.
- A turn stream with plan rendering and approval gates.
- A dispatcher that persists session state via the existing `interactive_agent_session` authority so phone-initiated conversations resume on laptop and vice versa.

## Anti-Patterns (Mobile v1 Post-Mortem)

These bind future work on this surface. Every anti-pattern below was something mobile v1 got wrong.

1. **Do not conflate solo-operator with multi-user.** Different threat models need different architectures. Mobile v1 tried to serve both in one package and neither worked. The operator console is explicitly single-principal.
2. **No WebAuthn RP-ID ceremony for a single operator on trusted LAN.** The assertion / challenge / registration flow is correct for stranger-to-stranger auth and overbuilt for you-on-your-tailnet. Tailscale device presence is the auth primitive.
3. **No approval-request lifecycle for self-approved commands.** One principal cannot form a quorum. Remove the "open a request, then ratify it" dance entirely. Plans get approved inline in the chat turn, not through a separate endpoint.
4. **No bootstrap-token exchange for new devices.** Pre-seed one operator record with a signed cookie. Skip the device-registration ceremony.
5. **No new mobile app surface.** The operator console is served by the existing API at a new route and rendered by a small React app. No PWA manifest, no service worker, no `/mobile/*` route family.
6. **No public-internet auth exposure when tunnel-gated LAN works.** Tailscale is the transport. Public auth only becomes the plan if and when real external users exist.
7. **Honor CLI-only Anthropic.** The operator console routes Opus calls through the `claude` binary via the existing CLI transport. Never `api.anthropic.com`. The standing order `decision.2026-04-20.anthropic-cli-only-restored` applies.
8. **Do not invent new workflow-launch endpoints.** Mobile v1 shipped `/workflows/launch` and `/workflows/commands/{id}/approve` on the agent sessions app. That was a shim. The canonical workflow submission surface already exists; the operator console calls it directly.

## Design

### Surface

- A single self-contained HTML page at `Code&DBs/Workflow/surfaces/console/index.html`. React + htm loaded via `esm.sh`; no Vite build step, no npm install, no bundler. The page is small enough to iterate in seconds and ship over a Tailscale link without a build pipeline.
- Served by the existing API at `GET /console`, gated on `PRAXIS_OPERATOR_DEV_MODE=1`. When the gate is off, the route returns 404.
- Bound to the Tailscale interface in production. The console is not reachable from public internet.

### Interaction model

- Top bar: provider picker (Claude CLI / Codex CLI / Gemini CLI / direct API), permission-mode dropdown, sandbox toggle.
- Turn stream: user and assistant bubbles; plans rendered as foldable cards with `approve / reject / continue` inline.
- Composer: single-line input with attach (files from workspace tree) and send.
- History sidebar (slide-in on narrow viewport): conversation resume.
- Status strip: token / cost counter per turn, running-command preview, cancel.

### Normalized permission matrix

Five common modes mapped to each provider's native flags. Full matrix lives in the `ProviderCLIProfile` row per provider (B.1 packet).

| Mode | Intent |
| --- | --- |
| `read_only` | Observe workspace. No mutations, no command execution. |
| `plan_only` | Produce a plan and halt. No edits, no commands. |
| `propose_edits` | Suggest edits and commands. Each action gated by explicit approval. |
| `auto_edits` | Apply edits automatically. Command execution still approved. |
| `full_autonomy` | Apply edits and run commands without prompting. |

Plus a `sandbox: bool` that wraps execution in the Docker sandbox when on (respects `execution_backends` authority).

### Persistence

Session state lives in `interactive_agent_session` (already exists, used by `agent_sessions.py`). No new tables. Persistence is a side-effect of the existing surface, not a new subsystem.

### Budget and audit

The surviving piece of mobile v1's budget infrastructure is `capability_grants`, used by workflow admission audit. The operator console does not re-create per-session budget envelopes for V1. If runaway-cost protection becomes necessary, it uses the existing admission-layer budget, not a new one.

## Implementation Phases

- **B.0** — Durable records (this doc + `operator_decisions` row).
- **B.1** — Normalized permission matrix. Extend `ProviderCLIProfile` with `normalized_permission_modes`; register mappings for claude, codex, gemini; extend `agent_sessions.py` create/send endpoints to accept a normalized mode; unit tests across providers × modes. No UI.
- **B.2** — Chat UI skeleton at `surfaces/console/index.html`. Provider picker, permission-mode dropdown, agent list, turn stream, composer, bearer-token gate. Self-contained HTML + React via esm.sh. Reads from and writes to the existing `agent_sessions` API (`/api/agent-sessions/agents`, `/agents/{id}/messages`, `/agents/{id}/messages` GET for history).
- **B.3** — Plan rendering + approval gate. Plans as structured turn events; approval updates agent state via a new `permission_step_up` event kind on the existing session events table.
- **B.4** — Tailscale VPS setup. Pure networking: Tailscale node on VPS, one on phone. Bind the API to the tailnet interface. No Praxis code change.

Later: direct-API permission mapping (B.1 extension), persistent home-agent dispatcher (queue tasks from phone for later execution on home rig).

## References

- Standing order: `decision.2026-04-20.anthropic-cli-only-restored` (CLI-only Anthropic).
- Standing order: `decision.2026-04-24.mobile-v1-archived` (mobile v1 archive — preserves learning).
- Mobile v1 archive branch: `archive/mobile-v1-2026-04-24`; revival doc: [docs/archive/mobile-v1.md](../archive/mobile-v1.md).
- Backend leverage: `Code&DBs/Workflow/surfaces/api/agent_sessions.py` (existing CLI subprocess management, SSE stream, interactive session persistence).
- Provider profile authority: `Code&DBs/Workflow/adapters/provider_types.py::ProviderCLIProfile`.
