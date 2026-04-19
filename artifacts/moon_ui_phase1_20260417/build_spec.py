#!/usr/bin/env python3
"""Generate the Moon UI Phase 1 spec with inlined source contents.

Rationale: the ephemeral CLI container's workspace materialization is not
hydrating source files for agents to read. Workaround: embed each target
file's contents directly in the prompt. Agents still write outputs via
their declared write_scope, which is the path that works.
"""
from __future__ import annotations
import json
from pathlib import Path

REPO = Path("/Users/nate/Praxis")

def read_src(rel: str) -> str:
    return (REPO / rel).read_text(encoding="utf-8")

MOON_BUILD_PAGE = read_src("Code&DBs/Workflow/surfaces/app/src/moon/MoonBuildPage.tsx")
TOOL_RESULT = read_src("Code&DBs/Workflow/surfaces/app/src/workspace/ToolResultRenderer.tsx")
CHAT_PANEL = read_src("Code&DBs/Workflow/surfaces/app/src/dashboard/ChatPanel.tsx")
USE_CHAT = read_src("Code&DBs/Workflow/surfaces/app/src/workspace/useChat.ts")
MARKDOWN_RENDERER = read_src("Code&DBs/Workflow/surfaces/app/src/workspace/MarkdownRenderer.tsx")

ART = "artifacts/moon_ui_phase1_20260417"

spec = {
    "name": "Moon UI Phase One — Spec + Diff (embedded source)",
    "workflow_id": "moon_ui_phase1_20260417",
    "phase": "execute",
    "workdir": str(REPO),
    "queue_id": "moon_ui_phase1_20260417",
    "outcome_goal": "Produce (a) architectural decomposition for MoonBuildPage.tsx and (b) three typechecked, tested diffs against ToolResultRenderer.tsx, ChatPanel.tsx+useChat.ts, and MarkdownRenderer.tsx.",
    "jobs": [
        {
            "label": "analyze_monolith",
            "agent": "anthropic/claude-sonnet-4-6",
            "task_type": "architecture",
            "description": "Analyze MoonBuildPage.tsx — layout regions, state ownership, coupling",
            "outcome_goal": "Written analysis with regions, state map, coupling surfaces sections.",
            "prompt": f"""Analyze the MoonBuildPage.tsx React component below. Produce a structured architectural analysis.

Write the analysis to `{ART}/01_analysis.md` (relative to your writable workdir). The file MUST contain these four section headers exactly:
- `## Layout Regions`
- `## State Ownership`
- `## Coupling Surfaces`
- `## Decomposition Hints`

For each section:

**Layout Regions** — name every distinct visual/functional region rendered from the top-level return. Cite approximate line ranges.

**State Ownership** — for each `useState`/`useReducer`/top-level `useMemo`/`useCallback`: name, which layout regions read/write it, and whether it could be lifted, colocated, or extracted into a custom hook.

**Coupling Surfaces** — the 3-5 tightest coupling surfaces that make the file hard to modify. Cite line ranges.

**Decomposition Hints** — 5-10 observations that should inform the next job's decomposition proposal. Don't propose subcomponents yet.

Do NOT write code. Do NOT propose the decomposition yet. This is analysis only.

Source file `surfaces/app/src/moon/MoonBuildPage.tsx` ({len(MOON_BUILD_PAGE.splitlines())} lines):

```tsx
{MOON_BUILD_PAGE}
```
""",
            "allowed_tools": ["Read", "Write", "Edit", "Grep", "Glob", "Bash"],
            "write_scope": [f"{ART}/01_analysis.md"],
            "verify_command": f"test -s /workspace/{ART}/01_analysis.md && grep -q '## Layout Regions' /workspace/{ART}/01_analysis.md && grep -q '## State Ownership' /workspace/{ART}/01_analysis.md && grep -q '## Coupling Surfaces' /workspace/{ART}/01_analysis.md"
        },
        {
            "label": "propose_decomposition",
            "agent": "google/gemini-2.5-pro",
            "task_type": "architecture",
            "depends_on": ["analyze_monolith"],
            "description": "Propose subcomponent decomposition",
            "outcome_goal": "Decomposition plan naming every subcomponent, props contract, migration order.",
            "prompt": f"""Propose a concrete decomposition of MoonBuildPage.tsx. The prior job's analysis is at `{ART}/01_analysis.md` — read it if accessible, otherwise work from the source below.

Write the proposal to `{ART}/02_decomposition.md` with these sections (exact headers):
- `## Proposed Subcomponents`
- `## State Ownership After Decomposition`
- `## Migration Order`
- `## Coupling Risks`
- `## Open Questions`

**Proposed Subcomponents** — 4-8 subcomponents. For each: Name, Responsibility (one sentence), TypeScript Props interface (real types), Internal state, target file path like `surfaces/app/src/moon/components/MoonActionDock.tsx`.

**State Ownership After Decomposition** — table: for each current state variable, where it lives after decomposition (parent, which child, or custom hook).

**Migration Order** — ordered list of extractions. Each step must be individually shippable and not break the build. Justify the order.

**Coupling Risks** — for each coupling surface from the analysis: how the decomposition handles it, or why it's left.

**Open Questions** — design questions the reviewer should pressure-test.

Be opinionated. A vague plan fails this job.

Source file for reference ({len(MOON_BUILD_PAGE.splitlines())} lines):

```tsx
{MOON_BUILD_PAGE}
```
""",
            "allowed_tools": ["Read", "Write", "Edit", "Grep", "Glob", "Bash"],
            "write_scope": [f"{ART}/02_decomposition.md"],
            "verify_command": f"test -s /workspace/{ART}/02_decomposition.md && grep -q '## Proposed Subcomponents' /workspace/{ART}/02_decomposition.md && grep -q '## Migration Order' /workspace/{ART}/02_decomposition.md && grep -q 'interface' /workspace/{ART}/02_decomposition.md"
        },
        {
            "label": "adversarial_review_spec",
            "agent": "openai/gpt-5.4",
            "task_type": "review",
            "depends_on": ["propose_decomposition"],
            "description": "Red-team the decomposition proposal",
            "outcome_goal": "Every weakness, unspoken assumption, speculative claim called out with references.",
            "prompt": f"""Red-team the decomposition plan at `{ART}/02_decomposition.md`. Write to `{ART}/03_redteam.md` with these sections:
- `## Decomposition Weaknesses`
- `## Migration Order Risks`
- `## Unstated Assumptions`
- `## Coupling Surfaces Not Addressed`
- `## Verdict`

**Verdict** must be one of: `READY-TO-EXECUTE`, `READY-WITH-NOTES`, `CONTESTED-NEEDS-REVISION`. One paragraph justifying. If CONTESTED, name the single highest-priority revision needed.

Be specific. Cite proposal text verbatim when calling out issues. No hedging. If the plan is solid, say so — only after you tried to break it.

Source file for cross-reference ({len(MOON_BUILD_PAGE.splitlines())} lines):

```tsx
{MOON_BUILD_PAGE}
```
""",
            "allowed_tools": ["Read", "Write", "Edit", "Grep", "Glob", "Bash"],
            "write_scope": [f"{ART}/03_redteam.md"],
            "verify_command": f"test -s /workspace/{ART}/03_redteam.md && grep -qE 'READY-TO-EXECUTE|READY-WITH-NOTES|CONTESTED-NEEDS-REVISION' /workspace/{ART}/03_redteam.md"
        },
        {
            "label": "diff_A_tool_result_renderer",
            "agent": "anthropic/claude-sonnet-4-6",
            "task_type": "refactor",
            "depends_on": ["adversarial_review_spec"],
            "description": "Typed refactor of ToolResultRenderer.tsx",
            "outcome_goal": "Discriminated union, extracted subcomponents, empty/error boundaries, stable keys. Output written.",
            "prompt": f"""Refactor `surfaces/app/src/workspace/ToolResultRenderer.tsx`. Write the **complete refactored file contents** (full TypeScript, not a diff) to `{ART}/diff_A_ToolResultRenderer.tsx`. Also write tests to `{ART}/diff_A_ToolResultRenderer.test.tsx`.

Required changes:
1. Define a discriminated union `ToolResultType` covering every branch currently handled. Use `type` as the discriminator.
2. Extract each branch into an isolated function component (`ToolResultTable`, `ToolResultStatus`, etc.).
3. Every branch must handle its empty state explicitly (0 rows, 0 jobs, etc.).
4. Every branch must handle malformed/unexpected data with a fallback.
5. Map keys must be stable identifiers from data (id, name, etc.) — not `JSON.stringify` or array index. If no stable id exists, explain in a comment.
6. No `any` types. Use the discriminated union throughout.

Constraints: do NOT change the public export or its prop shape.

Source file ({len(TOOL_RESULT.splitlines())} lines):

```tsx
{TOOL_RESULT}
```
""",
            "allowed_tools": ["Read", "Write", "Edit", "Grep", "Glob", "Bash"],
            "write_scope": [
                f"{ART}/diff_A_ToolResultRenderer.tsx",
                f"{ART}/diff_A_ToolResultRenderer.test.tsx"
            ],
            "verify_command": f"test -s /workspace/{ART}/diff_A_ToolResultRenderer.tsx && grep -q 'type ToolResultType' /workspace/{ART}/diff_A_ToolResultRenderer.tsx"
        },
        {
            "label": "diff_B_chat_panel",
            "agent": "anthropic/claude-sonnet-4-6",
            "task_type": "build",
            "depends_on": ["diff_A_tool_result_renderer"],
            "description": "Fix ChatPanel + useChat bugs",
            "outcome_goal": "abortRef wired, Cmd+Enter send, textarea maxLength, Escape focus-trap check, SSE reset on error, fetch timeout.",
            "prompt": f"""Fix the bugs in `surfaces/app/src/dashboard/ChatPanel.tsx` and `surfaces/app/src/workspace/useChat.ts`. Write the **complete fixed file contents** to:
- `{ART}/diff_B_ChatPanel.tsx`
- `{ART}/diff_B_useChat.ts`
- `{ART}/diff_B_ChatPanel.test.tsx` (new test file)
- `{ART}/diff_B_useChat.test.ts` (new test file)

Known issues:
1. `ChatPanel.tsx` global `keydown` listener for Escape doesn't check whether another modal/dialog has focus — add focus-trap check.
2. `useChat.ts` declares `abortRef` but never calls `.abort()`. Wire it on new send AND on unmount cleanup.
3. `ChatPanel.tsx` textarea has no `maxLength` — set 8000, show counter when within 200 of limit.
4. Add `Cmd+Enter` (or Ctrl+Enter on non-mac) shortcut to submit. Use `e.metaKey || e.ctrlKey`. Plain Enter keeps newline behavior.
5. `useChat.ts` SSE parser doesn't reset `streamingText` on error — reset to '' and surface the error in messages as an error-flavored assistant message.
6. `useChat.ts` fetch has no timeout — add AbortController-based 60s timeout.

Constraints: do NOT change public exports / hook return shape.

ChatPanel.tsx source ({len(CHAT_PANEL.splitlines())} lines):

```tsx
{CHAT_PANEL}
```

useChat.ts source ({len(USE_CHAT.splitlines())} lines):

```ts
{USE_CHAT}
```
""",
            "allowed_tools": ["Read", "Write", "Edit", "Grep", "Glob", "Bash"],
            "write_scope": [
                f"{ART}/diff_B_ChatPanel.tsx",
                f"{ART}/diff_B_useChat.ts",
                f"{ART}/diff_B_ChatPanel.test.tsx",
                f"{ART}/diff_B_useChat.test.ts"
            ],
            "verify_command": f"test -s /workspace/{ART}/diff_B_ChatPanel.tsx && test -s /workspace/{ART}/diff_B_useChat.ts && grep -q 'abortRef' /workspace/{ART}/diff_B_useChat.ts"
        },
        {
            "label": "diff_C_markdown_renderer",
            "agent": "openai/gpt-5.4",
            "task_type": "build",
            "depends_on": ["diff_B_chat_panel"],
            "description": "Fix MarkdownRenderer.tsx parser correctness + security",
            "outcome_goal": "Nested lists wrap, code indentation preserved, rel='noreferrer' on external links.",
            "prompt": f"""Fix `surfaces/app/src/workspace/MarkdownRenderer.tsx`. Write the **complete fixed file contents** to `{ART}/diff_C_MarkdownRenderer.tsx`. Also write tests to `{ART}/diff_C_MarkdownRenderer.test.tsx`.

Known issues:
1. Nested lists (bulleted inside numbered, vice versa) don't wrap in `<ul>/<ol>` correctly — each line is orphan `<li>`.
2. External links (`href` starts http://) lack `rel=\"noopener noreferrer\"` — security leak.
3. Code blocks lose leading whitespace — `escapeHtml` strips indentation.

Requirements:
- Nested lists wrap correctly (track nesting state).
- Code blocks preserve exact leading whitespace (tabs and spaces).
- External links get `rel=\"noopener noreferrer\" target=\"_blank\"`; internal/relative links do NOT get noreferrer.
- Renders plain text and empty string without error.

Test file must cover: nested bullet-in-numbered, 4-space-indent code block preserved, external link rel, internal link (no rel), empty string, plain text.

Don't change public prop shape. If you add a runtime dep, note why in a top-of-file comment.

Source file ({len(MARKDOWN_RENDERER.splitlines())} lines):

```tsx
{MARKDOWN_RENDERER}
```
""",
            "allowed_tools": ["Read", "Write", "Edit", "Grep", "Glob", "Bash"],
            "write_scope": [
                f"{ART}/diff_C_MarkdownRenderer.tsx",
                f"{ART}/diff_C_MarkdownRenderer.test.tsx"
            ],
            "verify_command": f"test -s /workspace/{ART}/diff_C_MarkdownRenderer.tsx && grep -q 'noopener noreferrer' /workspace/{ART}/diff_C_MarkdownRenderer.tsx"
        },
        {
            "label": "final_validation",
            "agent": "anthropic/claude-sonnet-4-6",
            "task_type": "test",
            "depends_on": [
                "diff_A_tool_result_renderer",
                "diff_B_chat_panel",
                "diff_C_markdown_renderer"
            ],
            "description": "Summary of what was produced",
            "outcome_goal": "A summary file listing every produced artifact + its status.",
            "prompt": f"""Six diff/artifact files should now exist in `{ART}/`:
- `diff_A_ToolResultRenderer.tsx` + `.test.tsx`
- `diff_B_ChatPanel.tsx` + `.test.tsx`
- `diff_B_useChat.ts` + `.test.ts`
- `diff_C_MarkdownRenderer.tsx` + `.test.tsx`

Write a summary to `{ART}/05_validation_summary.md` with sections:
- `## Artifacts Produced`
- `## Next Steps`

**Artifacts Produced** — list each expected file and note whether it exists (check via shell if you have access; otherwise note that a follow-up human check is needed).

**Next Steps** — for each diff, the command a human would run to apply it to the actual source tree and run typecheck + tests.

This job is summary-only — don't try to run typecheck/tests directly (the sandboxed agent likely can't). Just enumerate and hand off.
""",
            "allowed_tools": ["Read", "Write", "Edit", "Grep", "Glob", "Bash"],
            "write_scope": [f"{ART}/05_validation_summary.md"],
            "verify_command": f"test -s /workspace/{ART}/05_validation_summary.md"
        },
        {
            "label": "file_followup_bugs",
            "agent": "anthropic/claude-sonnet-4-6",
            "task_type": "build",
            "depends_on": ["final_validation"],
            "description": "File P3 bug for MoonBuildPage decomposition execution",
            "outcome_goal": "One P3 bug filed linking the decomposition plan as evidence.",
            "prompt": f"""File a single P3 bug in Praxis.db tracking the MoonBuildPage.tsx decomposition work.

Write a brief note to `{ART}/06_filed_bugs.md` with:
- `## Filed Bugs` header
- For each bug: id, title, severity

The bug to file (one):
- Title: `Execute MoonBuildPage.tsx decomposition per moon_ui_phase1_20260417 plan`
- Severity: `P3`
- Category: `UI`
- Description: reference `{ART}/02_decomposition.md` as the planning artifact; cite this workflow's run_id (from PRAXIS_EXECUTION_BUNDLE env var) as the planning evidence.

You can call the bug tool via:
```bash
./scripts/praxis workflow tools call praxis_bugs --input-json '{{...}}' --yes
```

If the tool is unavailable in your environment, just write the bug details to `{ART}/06_filed_bugs.md` as a pending filing for a human to submit.
""",
            "allowed_tools": ["Read", "Write", "Edit", "Grep", "Glob", "Bash"],
            "write_scope": [f"{ART}/06_filed_bugs.md"],
            "verify_command": f"test -s /workspace/{ART}/06_filed_bugs.md"
        },
        {
            "label": "register_decision_record",
            "agent": "anthropic/claude-sonnet-4-6",
            "task_type": "build",
            "depends_on": ["file_followup_bugs"],
            "description": "Consolidated decision record",
            "outcome_goal": "Single decision record at 00_decision_record.md citing all prior artifacts.",
            "prompt": f"""Write a consolidated decision record to `{ART}/00_decision_record.md`.

Required sections:
```
# Moon UI Phase One — Decision Record

**Run ID:** (from PRAXIS_EXECUTION_BUNDLE if accessible, otherwise note)
**Date:** 2026-04-17
**Status:** SHIPPED | SHIPPED-WITH-NOTES | BLOCKED

## Outcome Summary
<2-3 sentence narrative>

## Artifacts Produced
- Analysis: {ART}/01_analysis.md
- Decomposition: {ART}/02_decomposition.md
- Red-team: {ART}/03_redteam.md
- Diff A (ToolResultRenderer): {ART}/diff_A_*.tsx
- Diff B (ChatPanel + useChat): {ART}/diff_B_*.{{tsx,ts}}
- Diff C (MarkdownRenderer): {ART}/diff_C_*.tsx
- Validation summary: {ART}/05_validation_summary.md
- Filed bugs: {ART}/06_filed_bugs.md

## Evidence
Every artifact above was produced by this workflow run, across Anthropic Sonnet + Google Gemini Pro + OpenAI GPT-5.4 agents, with deterministic per-job verify gates. Multi-provider chain succeeded.

## Follow-up
- Execute the decomposition (filed as P3 bug in 06_filed_bugs.md)
- Apply the three diffs to source tree + run typecheck + tests
```

Populate placeholders honestly — if something failed or is uncertain, say so.
""",
            "allowed_tools": ["Read", "Write", "Edit", "Grep", "Glob", "Bash"],
            "write_scope": [f"{ART}/00_decision_record.md"],
            "verify_command": f"test -s /workspace/{ART}/00_decision_record.md && grep -qE '^\\*\\*Status:\\*\\* (SHIPPED|SHIPPED-WITH-NOTES|BLOCKED)' /workspace/{ART}/00_decision_record.md"
        }
    ]
}

out_path = REPO / ART / "moon_ui_phase1.queue.json"
out_path.write_text(json.dumps(spec, indent=2) + "\n", encoding="utf-8")
print(f"wrote {out_path}")
print(f"spec size: {out_path.stat().st_size} bytes")
print(f"jobs: {len(spec['jobs'])}")
