# Phase 1 Validation Summary

**Date:** 2026-04-18  
**Phase:** canvas_ui_phase1_20260417  
**Job:** final_validation

---

## Artifacts Produced

All 6 expected diff/artifact files were checked via shell. Status at time of validation:

| File | Expected | Status |
|---|---|---|
| `diff_A_ToolResultRenderer.tsx` | yes | **EXISTS** (12 327 bytes) |
| `diff_A_ToolResultRenderer.test.tsx` | yes | **EXISTS** (16 854 bytes) |
| `diff_B_ChatPanel.tsx` | yes | **EXISTS** (7 602 bytes) |
| `diff_B_ChatPanel.test.tsx` | yes | **EXISTS** (14 171 bytes) |
| `diff_B_useChat.ts` | yes | **EXISTS** (9 632 bytes) |
| `diff_B_useChat.test.ts` | yes | **EXISTS** (14 505 bytes) |
| `diff_C_MarkdownRenderer.tsx` | yes | **EXISTS** (5 875 bytes) |
| `diff_C_MarkdownRenderer.test.tsx` | yes | **EXISTS** (2 295 bytes) |

All 8 files are present (6 implementation diffs + 2 test companions, matching the 3-diff × 2-file pattern).

Supporting documents also present: `01_analysis.md`, `02_decomposition.md`, `03_redteam.md`, `build_spec.py`.

---

## Next Steps

Each diff is a **drop-in replacement** for the corresponding source file. Apply each one by copying it over the live source, then running typecheck and tests from the app root.

Assumed app root: `Code&DBs/Workflow/surfaces/app/`  
Assumed test runner: `vitest`  
Assumed type checker: `tsc` (via package script)

---

### Diff A — `ToolResultRenderer`

**Target path (adjust to match your repo layout):**
```
surfaces/app/src/workspace/ToolResultRenderer.tsx
```

**Apply + verify:**
```bash
# 1. Back up current file
cp surfaces/app/src/workspace/ToolResultRenderer.tsx \
   surfaces/app/src/workspace/ToolResultRenderer.tsx.bak

# 2. Apply diff
cp artifacts/canvas_ui_phase1_20260417/diff_A_ToolResultRenderer.tsx \
   surfaces/app/src/workspace/ToolResultRenderer.tsx

# 3. Stage test file alongside implementation
cp artifacts/canvas_ui_phase1_20260417/diff_A_ToolResultRenderer.test.tsx \
   surfaces/app/src/workspace/ToolResultRenderer.test.tsx

# 4. Typecheck
cd surfaces/app && npm run typecheck

# 5. Run targeted tests
cd surfaces/app && npx vitest run src/workspace/ToolResultRenderer.test.tsx
```

**Coverage note:** 37 test cases covering all 5 discriminated-union branches, empty states, malformed-data fallbacks, selection mechanics, footer pluralization, and `deriveRowKey` export.

---

### Diff B — `useChat` + `ChatPanel`

These two files ship together (hook + consumer). Apply both before running tests.

**Target paths:**
```
surfaces/app/src/workspace/useChat.ts
surfaces/app/src/workspace/ChatPanel.tsx
```

**Apply + verify:**
```bash
# 1. Back up
cp surfaces/app/src/workspace/useChat.ts    surfaces/app/src/workspace/useChat.ts.bak
cp surfaces/app/src/workspace/ChatPanel.tsx surfaces/app/src/workspace/ChatPanel.tsx.bak

# 2. Apply diffs
cp artifacts/canvas_ui_phase1_20260417/diff_B_useChat.ts \
   surfaces/app/src/workspace/useChat.ts
cp artifacts/canvas_ui_phase1_20260417/diff_B_ChatPanel.tsx \
   surfaces/app/src/workspace/ChatPanel.tsx

# 3. Stage test files
cp artifacts/canvas_ui_phase1_20260417/diff_B_useChat.test.ts \
   surfaces/app/src/workspace/useChat.test.ts
cp artifacts/canvas_ui_phase1_20260417/diff_B_ChatPanel.test.tsx \
   surfaces/app/src/workspace/ChatPanel.test.tsx

# 4. Typecheck
cd surfaces/app && npm run typecheck

# 5. Run targeted tests (hook + panel together)
cd surfaces/app && npx vitest run \
  src/workspace/useChat.test.ts \
  src/workspace/ChatPanel.test.tsx
```

**Fixes covered by these diffs:**
- FIX #1: Escape key focus-trap (panel-scoped, modal-aware)
- FIX #2: Unmount abort cleanup for SSE/fetch streams
- FIX #3: `maxLength=8000` + live character counter (≤200 remaining)
- FIX #4: Send on Cmd/Ctrl+Enter; plain Enter inserts newline
- FIX #5: SSE error → reset `streamingText` + inline error message
- FIX #6: 60-second hard timeout with discriminated abort reason

---

### Diff C — `MarkdownRenderer`

**Target path:**
```
surfaces/app/src/workspace/MarkdownRenderer.tsx
```

**Apply + verify:**
```bash
# 1. Back up
cp surfaces/app/src/workspace/MarkdownRenderer.tsx \
   surfaces/app/src/workspace/MarkdownRenderer.tsx.bak

# 2. Apply diff
cp artifacts/canvas_ui_phase1_20260417/diff_C_MarkdownRenderer.tsx \
   surfaces/app/src/workspace/MarkdownRenderer.tsx

# 3. Stage test file
cp artifacts/canvas_ui_phase1_20260417/diff_C_MarkdownRenderer.test.tsx \
   surfaces/app/src/workspace/MarkdownRenderer.test.tsx

# 4. Typecheck
cd surfaces/app && npm run typecheck

# 5. Run targeted tests
cd surfaces/app && npx vitest run src/workspace/MarkdownRenderer.test.tsx
```

**Note:** The diff_C upstream job encountered a missing source file during sandbox execution (`MarkdownRenderer.tsx` not found at the expected path). The artifact was written based on behavioral inference from surrounding context. **Before applying, confirm the actual source path** with:
```bash
find surfaces/app/src -name "MarkdownRenderer.tsx" 2>/dev/null
```
If the file lives elsewhere, update the target path above accordingly.

---

### Full suite smoke test (after all three diffs are applied)

```bash
cd surfaces/app && npm run typecheck && npx vitest run
```

A clean run with no new type errors and all pre-existing tests passing constitutes a green hand-off for Phase 1.
