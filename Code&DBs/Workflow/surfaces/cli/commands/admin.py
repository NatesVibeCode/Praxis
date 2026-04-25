"""Administrative CLI command handlers."""

from __future__ import annotations

from typing import Any, TextIO

from surfaces.cli._db import cli_sync_conn


def _compile_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle ``workflow compile [intent.json|--description DESC --write FILE --stage STAGE]``.

    Takes minimal executable intent (description, write files, stage) and
    produces a fully-contexted workflow spec that can be run directly. When
    only ``--description`` is provided, compile performs intent recognition:
    source-ordered spans, authority matches, prerequisite suggestions, gaps.

    Usage:
        workflow compile --description "Add retry logic" --write runtime/workflow/unified.py --stage build
        workflow compile intent.json
        workflow compile --description "..." --write file1.py,file2.py --stage build [--read extra.py] [--timeout 300]
    """

    import json as _json

    from runtime.spec_compiler import compile_intent_from_file, compile_spec

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow compile [intent-file | --description DESC --write FILES --stage STAGE]\n"
            "\n"
            "Compile minimal intent into a workflow spec, or recognize free-form intent.\n"
            "\n"
            "Positional argument:\n"
            "  intent-file          path to a JSON intent file\n"
            "\n"
            "Options (alternative to intent-file):\n"
            "  --description TEXT   task description (required)\n"
            "  --write FILES        comma-separated list of files to write (required for spec output)\n"
            "  --stage STAGE        build|fix|review|test|research (required for spec output)\n"
            "  --read FILES         comma-separated list of files to read (optional)\n"
            "  --label LABEL        custom label (optional, auto-generated if omitted)\n"
            "  --timeout SECS       timeout in seconds (default: 300)\n"
            "  --max-tokens TOKENS  max tokens (default: 4096)\n"
            "  --temperature TEMP   temperature 0.0-2.0 (default: 0.0)\n"
            "  --json               output as JSON (default)\n"
            "\n"
            "Examples:\n"
            "  workflow compile intent.json\n"
            "  workflow compile --description 'Add retry logic' \\\n"
            "    --write runtime/workflow/unified.py --stage build\n"
            "  workflow compile --description 'Fix bug' --write src/app.py --stage fix \\\n"
            "    --read src/util.py --timeout 600\n"
        )
        return 2

    intent_dict: dict[str, Any] = {}

    if args and not args[0].startswith("--"):
        intent_path = args[0]
        intent, errors = compile_intent_from_file(intent_path)
        if errors:
            for error in errors:
                stdout.write(f"error: {error}\n")
            return 1
        if intent is None:
            stdout.write(f"error: failed to load intent from {intent_path}\n")
            return 1
        intent_dict = {
            "description": intent.description,
            "write": intent.write,
            "stage": intent.stage,
            "read": intent.read,
            "label": intent.label,
            "timeout": intent.timeout,
            "max_tokens": intent.max_tokens,
            "temperature": intent.temperature,
        }
    else:
        i = 0
        while i < len(args):
            arg = args[i]
            if arg == "--description":
                i += 1
                if i < len(args):
                    intent_dict["description"] = args[i]
                i += 1
            elif arg == "--write":
                i += 1
                if i < len(args):
                    intent_dict["write"] = [file_path.strip() for file_path in args[i].split(",")]
                i += 1
            elif arg == "--read":
                i += 1
                if i < len(args):
                    intent_dict["read"] = [file_path.strip() for file_path in args[i].split(",")]
                i += 1
            elif arg == "--stage":
                i += 1
                if i < len(args):
                    intent_dict["stage"] = args[i]
                i += 1
            elif arg == "--label":
                i += 1
                if i < len(args):
                    intent_dict["label"] = args[i]
                i += 1
            elif arg == "--timeout":
                i += 1
                if i < len(args):
                    try:
                        intent_dict["timeout"] = int(args[i])
                    except ValueError:
                        stdout.write(f"error: --timeout must be an integer, got {args[i]!r}\n")
                        return 1
                i += 1
            elif arg == "--max-tokens":
                i += 1
                if i < len(args):
                    try:
                        intent_dict["max_tokens"] = int(args[i])
                    except ValueError:
                        stdout.write(f"error: --max-tokens must be an integer, got {args[i]!r}\n")
                        return 1
                i += 1
            elif arg == "--temperature":
                i += 1
                if i < len(args):
                    try:
                        intent_dict["temperature"] = float(args[i])
                    except ValueError:
                        stdout.write(f"error: --temperature must be a float, got {args[i]!r}\n")
                        return 1
                i += 1
            elif arg in {"--json"}:
                i += 1
            else:
                stdout.write(f"error: unknown option: {arg}\n")
                return 1

    try:
        try:
            conn = cli_sync_conn()
        except Exception:
            conn = None

        if intent_dict.get("description") and not intent_dict.get("write") and not intent_dict.get("stage"):
            if conn is None:
                stdout.write("error: WORKFLOW_DATABASE_URL authority is required for intent recognition\n")
                return 1
            from runtime.intent_recognition import recognize_intent

            recognition = recognize_intent(str(intent_dict["description"]), conn=conn)
            payload = recognition.to_dict()
            payload["kind"] = "intent_recognition"
            payload["ok"] = True
            stdout.write(_json.dumps(payload, indent=2) + "\n")
            return 0

        spec, warnings = compile_spec(intent_dict, conn=conn)
    except ValueError as exc:
        stdout.write(f"error: {exc}\n")
        return 1

    for warning in warnings:
        stdout.write(f"warning: {warning}\n")

    spec_dict = spec.to_dispatch_spec_dict()
    stdout.write(_json.dumps(spec_dict, indent=2) + "\n")
    return 0


def _parse_pr_spec(spec_str: str) -> tuple[str, str, int]:
    """Parse 'owner/repo#number' format into (owner, repo, pr_number)."""

    if "#" not in spec_str:
        raise ValueError(f"PR spec must include #number: {spec_str}")

    repo_part, pr_part = spec_str.rsplit("#", 1)
    if "/" not in repo_part:
        raise ValueError(f"Invalid repo spec (need owner/repo): {repo_part}")

    owner, repo = repo_part.rsplit("/", 1)
    try:
        pr_number = int(pr_part)
    except ValueError as exc:
        raise ValueError(f"PR number must be numeric: {pr_part}") from exc

    return owner, repo, pr_number


def _github_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow github <subcommand> <owner/repo#number>`.

    Subcommands:
    - review: Run a review of a PR and post findings as a comment
    - diff: Show the PR diff (raw)

    Example:
        workflow github review anthropic/anthropic-sdk-python#456
        workflow github diff anthropic/anthropic-sdk-python#456
    """

    from runtime.github_integration import GitHubClient, dispatch_pr_review, post_review_to_pr

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow github review <owner/repo#number> [--tier tier]\n"
            "       workflow github diff <owner/repo#number>\n"
        )
        return 2

    subcommand = args[0] if args else None
    if subcommand not in {"review", "diff"}:
        stdout.write(f"unknown github subcommand: {subcommand}\n")
        return 2

    if len(args) < 2:
        stdout.write(f"error: {subcommand} requires <owner/repo#number>\n")
        return 2

    try:
        owner, repo, pr_number = _parse_pr_spec(args[1])
    except ValueError as exc:
        stdout.write(f"error: {exc}\n")
        return 2

    if subcommand == "review":
        tier = "mid"
        if len(args) > 2 and args[2] == "--tier":
            if len(args) < 4:
                stdout.write("error: --tier requires a value\n")
                return 2
            tier = args[3]

        stdout.write(f"Reviewing {owner}/{repo}#{pr_number} (tier={tier})...\n")
        try:
            result = dispatch_pr_review(owner, repo, pr_number, tier=tier)
        except Exception as exc:
            stdout.write(f"error: review failed: {exc}\n")
            return 1

        if result.status == "succeeded":
            stdout.write(f"✓ Review run succeeded (run_id={result.run_id})\n")
            stdout.write(
                f"  Model: {result.author_model or f'{result.provider_slug}/{result.model_slug}'}\n"
            )
            stdout.write(f"  Duration: {result.latency_ms}ms\n\n")
            stdout.write("Posting to PR...\n")

            try:
                comment = post_review_to_pr(owner, repo, pr_number, result)
                stdout.write(f"✓ Comment posted (ID: {comment.get('id')})\n")
                stdout.write(f"  URL: {comment.get('html_url', 'N/A')}\n")
                return 0
            except Exception as exc:
                stdout.write(f"⚠ Review succeeded but comment post failed: {exc}\n")
                return 1

        stdout.write(
            f"✗ Review failed\n"
            f"  Status: {result.status}\n"
            f"  Code: {result.reason_code}\n"
        )
        if result.failure_code:
            stdout.write(f"  Failure: {result.failure_code}\n")
        return 1

    stdout.write(f"Fetching diff for {owner}/{repo}#{pr_number}...\n\n")
    try:
        client = GitHubClient()
        diff = client.get_pr_diff(owner, repo, pr_number)
        stdout.write(diff)
        return 0
    except Exception as exc:
        stdout.write(f"error: {exc}\n")
        return 1
