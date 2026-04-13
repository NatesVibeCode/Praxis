#!/usr/bin/env python3
"""Prove run_workflow() works with all 3 providers through the platform.

No hand-rolled CLI calls. Just workflow specs through the router.
"""

from __future__ import annotations

import sys
import shutil
import tempfile
from pathlib import Path

WORKFLOW_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKFLOW_ROOT))

from runtime.workflow import run_workflow, WorkflowSpec
from runtime.output_writer import apply_structured_output


TARGET = '''\
"""Greeting module."""


def greet(name: str) -> str:
    return f"Hello, {name}!"
'''

PROVIDERS = [
    {
        "provider_slug": "anthropic",
        "model_slug": "claude-sonnet-4-6",
        "task": "Add a `farewell` function that takes a name and returns a goodbye message.",
    },
    {
        "provider_slug": "openai",
        "model_slug": None,  # use codex default
        "task": "Add a `formal_greet` function that takes a name and title, returns a formal greeting.",
    },
    {
        "provider_slug": "google",
        "model_slug": "gemini-2.5-flash",
        "task": "Add a `greet_group` function that takes a list of names and greets them all.",
    },
]


def main() -> int:
    workspace = tempfile.mkdtemp(prefix="praxis_workflow_proof_")
    target_path = Path(workspace) / "greeting.py"
    print(f"Workspace: {workspace}\n")

    results = {}
    for p in PROVIDERS:
        slug = p["provider_slug"]
        print(f"--- {slug} ({p['model_slug'] or 'default'}) ---")

        # Reset target file for each provider
        target_path.write_text(TARGET)

        spec = WorkflowSpec(
            prompt=(
                f"{p['task']} Keep the existing greet function.\n\n"
                "Return your response as JSON:\n"
                '{"code_blocks": [{"file_path": "greeting.py", '
                '"content": "<FULL FILE>", "language": "python", '
                '"action": "replace"}], '
                '"explanation": "<what you changed>"}'
            ),
            provider_slug=slug,
            model_slug=p["model_slug"],
            adapter_type="cli_llm",
            timeout=120,
            workdir=workspace,
            scope_write=["greeting.py"],
            context_sections=[
                {"name": "FILE: greeting.py", "content": TARGET},
            ],
            system_prompt="You are a code editor. Return ONLY valid JSON.",
        )

        result = run_workflow(spec)

        print(f"  status: {result.status}")
        print(f"  reason: {result.reason_code}")
        print(f"  latency: {result.latency_ms}ms")
        print(f"  author: {result.author_model}")

        if result.status == "succeeded":
            outputs = dict(result.outputs)
            so = outputs.get("structured_output", {})
            wm = outputs.get("write_manifest", {})
            print(f"  has_code: {so.get('has_code')}")
            print(f"  parse_strategy: {so.get('parse_strategy', 'n/a')}")
            print(f"  files_written: {wm.get('total_files', 0)}")
            completion = result.completion or ""
            if completion and not so.get("has_code"):
                print(f"  completion preview: {completion[:300]}")

            if wm.get("all_succeeded"):
                content = target_path.read_text()
                has_greet = "def greet" in content
                has_new = any(kw in content for kw in ["farewell", "formal_greet", "greet_group"])
                print(f"  original greet: {has_greet}")
                print(f"  new function: {has_new}")
                results[slug] = has_greet and has_new
            else:
                results[slug] = False
        else:
            fc = result.failure_code or ""
            print(f"  failure: {fc}")
            completion = result.completion or ""
            if completion:
                print(f"  output preview: {completion[:200]}")
            results[slug] = False

        print()

    # Summary
    print("=" * 50)
    for slug, ok in results.items():
        print(f"  {slug:12s}: {'PASS' if ok else 'FAIL'}")

    passed = sum(1 for ok in results.values() if ok)
    print(f"\n  {passed}/{len(results)} providers passed.")

    shutil.rmtree(workspace, ignore_errors=True)
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
