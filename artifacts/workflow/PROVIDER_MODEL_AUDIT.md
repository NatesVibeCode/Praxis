# Provider & Model Audit

Mark each row: **KEEP**, **REMOVE**, or **UPDATE** (with new slug).
Add notes in the Notes column for anything I should know.

---

## CLIs Installed

| Provider | CLI Binary | Authenticated? | Notes |
|----------|-----------|----------------|-------|
| Anthropic | `claude` | | |
| OpenAI | `codex` | | |
| Google | `gemini` | | |
| Cursor | `cursor-agent` | | |

---

## Anthropic Models (via `claude` CLI)

| Model Slug | Tier | Affinities | Keep/Remove/Update | Notes |
|---|---|---|---|---|
| claude-haiku-4-5-20251001 | low | chat, quick-analysis, batch | | |
| claude-sonnet-4-6 | medium | review, build, chat | | |
| claude-opus-4-6 | high | architecture, review, research | | |

---

## Google Models (via `gemini` CLI)

| Model Slug | Tier | Affinities | Keep/Remove/Update | Notes |
|---|---|---|---|---|
| gemini-1.5-pro-002 | high | research, analysis | | |
| gemini-2.0-flash | medium | chat, build | | |
| gemini-2.0-flash-001 | medium | chat, build | | |
| gemini-2.0-flash-lite-001 | low | chat, batch | | |
| gemini-2.5-flash | medium | chat, build | | |
| gemini-2.5-flash-lite | low | chat, batch | | |
| gemini-2.5-flash-preview-04-17 | medium | chat, build | | |
| gemini-2.5-flash-tts | low | tts | | |
| gemini-2.5-pro | high | research, architecture | | |
| gemini-2.5-pro-exp-03-25 | high | research, analysis | | |
| gemini-2.5-pro-tts | medium | tts | | |
| gemini-3-flash-preview | high | build, agentic-coding | | |
| gemini-3.1-flash-image-preview | medium | image-generation | | |
| gemini-3.1-flash-lite-preview | medium | chat, multimodal | | |
| gemini-3.1-pro-preview | high | research, architecture | | |
| gemini-live-2.5-flash-native-audio | medium | live-audio | | |

---

## OpenAI Models (via `codex` CLI)

| Model Slug | Tier | Affinities | Keep/Remove/Update | Notes |
|---|---|---|---|---|
| gpt-5 | high | analysis, research | | |
| gpt-5-codex | high | build, review, debug | | |
| gpt-5-codex-mini | medium | build, wiring | | |
| gpt-5.1 | high | analysis, research | | |
| gpt-5.1-codex | high | build, review, debug | | |
| gpt-5.1-codex-max | high | build, agentic-coding | | |
| gpt-5.1-codex-mini | medium | build, wiring | | |
| gpt-5.2 | high | analysis, research | | |
| gpt-5.2-codex | high | build, review, debug | | |
| gpt-5.3-codex | high | build, review, debug | | |
| gpt-5.3-codex-spark | medium | wiring, fast-build | | |
| gpt-5.4 | high | build, review, architecture | | |
| gpt-5.4-mini | medium | build, wiring, subagents | | |

---

## Task Route Table (auto/)

Mark any routes that need rank changes.

| Route | Rank 1 | Rank 2 | Rank 3 | Rank 4 | Changes? |
|---|---|---|---|---|---|
| auto/build | gpt-5.4 | gemini-3.1-pro-preview | claude-sonnet-4-6 | gpt-5.4-mini | |
| auto/architecture | claude-opus-4-6 | gpt-5.4 | gemini-3.1-pro-preview | — | |
| auto/wiring | gpt-5.4-mini | gemini-3.1-pro-preview | gpt-5.4 | claude-sonnet-4-6 | |
| auto/test | gpt-5.4 | gemini-3.1-pro-preview | claude-sonnet-4-6 | gpt-5.4-mini | |
| auto/review | gpt-5.4 | claude-sonnet-4-6 | gemini-3.1-pro-preview | gpt-5.4-mini | |
| auto/debate | claude-opus-4-6 | gpt-5.4 | gemini-3.1-pro-preview | claude-sonnet-4-6 | |
| auto/refactor | gpt-5.4 | gemini-3.1-pro-preview | claude-sonnet-4-6 | gpt-5.4-mini | |

---

## New Models to Add?

| Provider | Model Slug | Tier | Affinities | Notes |
|---|---|---|---|---|
| | | | | |
| | | | | |
| | | | | |
