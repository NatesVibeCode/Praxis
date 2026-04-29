"""Administrative CLI command handlers."""

from __future__ import annotations

from typing import TextIO

from surfaces.cli._db import cli_sync_conn


def _plan_generation_help(stdout: TextIO) -> int:
    stdout.write(
        "usage: workflow generate-plan --description DESC [--match-limit N]\n"
        "       workflow materialize-plan --description DESC [--workflow-id ID] [--title TITLE]\n"
        "\n"
        "Generate or materialize a workflow plan from prose. Materialize creates draft build state only;\n"
        "execution still goes through approval and run surfaces.\n"
        "\n"
        "Options:\n"
        "  --description TEXT   task description (required)\n"
        "  --match-limit N      authority candidates per recognized span for generate-plan\n"
        "  --workflow-id ID     workflow to update when materializing\n"
        "  --title TITLE        workflow title when materializing\n"
        "  --enable-llm         allow LLM-assisted build materialization\n"
        "  --no-llm             disable LLM-assisted build materialization\n"
        "  --json               output as JSON (default)\n"
    )
    return 2


def _parse_plan_generation_args(args: list[str], *, stdout: TextIO) -> tuple[dict[str, object], int | None]:
    parsed: dict[str, object] = {}
    i = 0
    while i < len(args):
        arg = args[i]
        if arg in {"-h", "--help"}:
            return parsed, _plan_generation_help(stdout)
        if arg in {"--description", "--workflow-id", "--title", "--match-limit"}:
            if i + 1 >= len(args):
                stdout.write(f"error: {arg} requires a value\n")
                return parsed, 1
            value = args[i + 1]
            if arg == "--description":
                parsed["description"] = value
            elif arg == "--workflow-id":
                parsed["workflow_id"] = value
            elif arg == "--title":
                parsed["title"] = value
            else:
                try:
                    parsed["match_limit"] = int(value)
                except ValueError:
                    stdout.write(f"error: --match-limit must be an integer, got {value!r}\n")
                    return parsed, 1
            i += 2
            continue
        if arg == "--enable-llm":
            parsed["enable_llm"] = True
            i += 1
            continue
        if arg == "--no-llm":
            parsed["enable_llm"] = False
            i += 1
            continue
        if arg == "--json":
            i += 1
            continue
        if not arg.startswith("--"):
            stdout.write(
                f"error: unexpected argument: {arg}. Use --description; intent-file plan generation is not a user-facing surface.\n"
            )
            return parsed, 1
        stdout.write(f"error: unknown option: {arg}\n")
        return parsed, 1
    return parsed, None


def _run_plan_generation(
    args: list[str],
    *,
    stdout: TextIO,
    materialize: bool,
) -> int:
    """Handle ``workflow generate-plan`` and ``workflow materialize-plan``."""

    import json as _json

    if not args or args[0] in {"-h", "--help"}:
        return _plan_generation_help(stdout)

    parsed, early_exit = _parse_plan_generation_args(args, stdout=stdout)
    if early_exit is not None:
        return early_exit

    description = str(parsed.get("description") or "").strip()
    if not description:
        stdout.write("error: --description is required\n")
        return 1

    try:
        try:
            conn = cli_sync_conn()
        except Exception:
            conn = None

        if materialize:
            from runtime.compile_cqrs import materialize_workflow

            payload = materialize_workflow(
                description,
                conn=conn,
                workflow_id=str(parsed.get("workflow_id") or "").strip() or None,
                title=str(parsed.get("title") or "").strip() or None,
                enable_llm=(
                    bool(parsed["enable_llm"])
                    if "enable_llm" in parsed
                    else None
                ),
            )
        else:
            from runtime.compile_cqrs import preview_compile

            payload = preview_compile(
                description,
                conn=conn,
                match_limit=int(parsed.get("match_limit") or 5),
            ).to_dict()
    except ValueError as exc:
        stdout.write(f"error: {exc}\n")
        return 1

    stdout.write(_json.dumps(payload, indent=2) + "\n")
    return 0


def _generate_plan_command(args: list[str], *, stdout: TextIO) -> int:
    return _run_plan_generation(args, stdout=stdout, materialize=False)


def _materialize_plan_command(args: list[str], *, stdout: TextIO) -> int:
    return _run_plan_generation(args, stdout=stdout, materialize=True)


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
