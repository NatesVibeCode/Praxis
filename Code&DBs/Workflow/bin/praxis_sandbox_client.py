#!/usr/bin/env python3
"""In-sandbox CLI shim for the workflow MCP bridge.

This is the `praxis` command that gets baked into every sandbox image
(`praxis-worker`, `praxis-codex`, `praxis-claude`, `praxis-gemini`). It gives
the agent a single, uniform shell-callable interface for the workflow MCP
surface — no per-provider CLI-flag template, no `.claude.json` / `config.toml`
overlay, no auto-discovered client config. The agent just calls:

    praxis discover "retry logic"
    praxis query "what is failing right now?"
    praxis submit_code_change --summary "..." --primary-paths '["foo.py"]' --result-kind code_change

Each invocation is one HTTP POST to ``$PRAXIS_WORKFLOW_MCP_URL`` with the
signed session token from ``$PRAXIS_WORKFLOW_MCP_TOKEN`` as a bearer. This
replaces the per-provider MCP client plumbing, which consistently rots on
CLI upgrades (see architecture-policy::sandbox::uniform-shell-tool-surface).

Exit codes:
    0 — tool call succeeded
    1 — usage error (bad subcommand, missing required arg)
    2 — environment error (missing URL or token)
    3 — bridge returned an HTTP or JSON-RPC error
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
import uuid
from typing import Any

# In-sandbox shim — the praxis surfaces source tree is NOT available here,
# only this single file is COPYed into /usr/local/bin/praxis. Importing from
# `surfaces.cli.commands.tools` raises ModuleNotFoundError at startup and
# prevents the shim from running at all (BUG-632E6F45 surfaced this when an
# audit agent tried to call `praxis discover` and crashed before the HTTP
# bridge call). Use minimal in-shim stand-ins backed by the live tools list
# fetched from the bridge — exact-name match wins, otherwise an unambiguous
# substring/alias match wins, otherwise we report ambiguity from the bridge
# response.
def _resolve_tool_definition(tool_name):  # type: ignore[no-redef]
    # The shim's other path uses _call_tools_list to fetch the live catalog;
    # this helper is only called BEFORE that fetch in older code paths. Just
    # treat the input as the resolved name and let _match_tool_definition do
    # the actual lookup against the bridge-supplied list.
    class _StubDef:
        def __init__(self, name):
            self.name = name
    return _StubDef(tool_name), []


def _render_tool_lookup_failure(tool_name, candidates, *, stdout):  # type: ignore[no-redef]
    if not candidates:
        stdout.write(f"unknown tool: {tool_name}\n")
        stdout.write("tip: run `praxis workflow tools list` to browse available tools.\n")
        return 2
    stdout.write(f"ambiguous tool name: {tool_name}\n")
    stdout.write("did you mean:\n")
    for definition in candidates[:5]:
        name = getattr(definition, "name", str(definition))
        stdout.write(f"  {name}\n")
    return 2

_ENV_URL = "PRAXIS_WORKFLOW_MCP_URL"
_ENV_TOKEN = "PRAXIS_WORKFLOW_MCP_TOKEN"
_ENV_ALLOWED = "PRAXIS_ALLOWED_MCP_TOOLS"

# Subcommand → (MCP tool name, argspec)
# argspec is a list of (flag, dest, kind, required). kind ∈ {"str", "json", "positional"}.
_COMMANDS: dict[str, tuple[str, list[tuple[str, str, str, bool]]]] = {
    "discover": (
        "praxis_discover",
        [("query", "query", "positional", True)],
    ),
    "query": (
        "praxis_query",
        [("question", "question", "positional", True)],
    ),
    "recall": (
        "praxis_recall",
        [
            ("query", "query", "positional", True),
            ("--entity-type", "entity_type", "str", False),
            ("--limit", "limit", "str", False),
        ],
    ),
    "context_shard": (
        "praxis_context_shard",
        [
            ("--view", "view", "str", False),
            ("--section-name", "section_name", "str", False),
            ("--include-bundle", "include_bundle", "str", False),
        ],
    ),
    "health": ("praxis_health", []),
    "integration": (
        "praxis_integration",
        [
            ("action", "action", "positional", True),
            ("--integration-id", "integration_id", "str", False),
            ("--integration-action", "integration_action", "str", False),
            ("--args", "args", "json", False),
        ],
    ),
    "submit_code_change": (
        "praxis_submit_code_change",
        [
            ("--summary", "summary", "str", True),
            ("--primary-paths", "primary_paths", "json", True),
            ("--result-kind", "result_kind", "str", True),
            ("--tests-ran", "tests_ran", "json", False),
            ("--notes", "notes", "str", False),
            ("--declared-operations", "declared_operations", "json", False),
        ],
    ),
    "submit_artifact_bundle": (
        "praxis_submit_artifact_bundle",
        [
            ("--summary", "summary", "str", True),
            ("--primary-paths", "primary_paths", "json", True),
            ("--result-kind", "result_kind", "str", True),
            ("--notes", "notes", "str", False),
        ],
    ),
    "get_submission": (
        "praxis_get_submission",
        [
            ("--submission-id", "submission_id", "str", False),
            ("--job-label", "job_label", "str", False),
        ],
    ),
}


def _die(exit_code: int, message: str) -> None:
    sys.stderr.write(f"praxis: {message}\n")
    sys.exit(exit_code)


def _parse_subcommand_args(
    subcommand: str, specs: list[tuple[str, str, str, bool]], argv: list[str]
) -> dict[str, Any]:
    parser = argparse.ArgumentParser(
        prog=f"praxis {subcommand}",
        description=f"Call the {_COMMANDS[subcommand][0]} MCP tool.",
    )
    for flag, dest, kind, required in specs:
        if kind == "positional":
            parser.add_argument(dest, help=f"{dest} ({kind})")
        else:
            parser.add_argument(flag, dest=dest, required=required, help=f"{dest} ({kind})")
    namespace = parser.parse_args(argv)
    arguments: dict[str, Any] = {}
    for _flag, dest, kind, _required in specs:
        value = getattr(namespace, dest, None)
        if value is None:
            continue
        if kind == "json":
            try:
                arguments[dest] = json.loads(value)
            except json.JSONDecodeError as exc:
                _die(1, f"--{dest.replace('_', '-')} must be valid JSON: {exc}")
        else:
            arguments[dest] = value
    return arguments


def _post_jsonrpc(
    *, url: str, token: str, tool_name: str, arguments: dict[str, Any], allowed_tools: str
) -> Any:
    payload = {
        "jsonrpc": "2.0",
        "id": str(uuid.uuid4()),
        "method": "tools/call",
        "params": {"name": tool_name, "arguments": arguments},
    }
    body_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body_bytes,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "X-Praxis-Allowed-MCP-Tools": allowed_tools,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310
            response_bytes = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        _die(3, f"bridge returned HTTP {exc.code}: {detail or exc.reason}")
    except urllib.error.URLError as exc:
        _die(3, f"bridge unreachable at {url}: {exc.reason}")
    except Exception as exc:  # pragma: no cover - defensive catch-all
        _die(3, f"bridge call failed: {type(exc).__name__}: {exc}")

    try:
        response_payload = json.loads(response_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        _die(3, f"bridge returned non-JSON response: {exc}")

    if not isinstance(response_payload, dict):
        _die(3, "bridge returned non-object JSON-RPC envelope")

    if "error" in response_payload:
        err = response_payload["error"]
        code = err.get("code", "unknown") if isinstance(err, dict) else "unknown"
        message = err.get("message", err) if isinstance(err, dict) else err
        _die(3, f"tool call returned error {code}: {message}")

    return response_payload.get("result")


def _call_tools_list(*, url: str, token: str, allowed_tools: str) -> list[dict[str, Any]]:
    list_payload = {
        "jsonrpc": "2.0",
        "id": str(uuid.uuid4()),
        "method": "tools/list",
        "params": {},
    }
    body_bytes = json.dumps(list_payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body_bytes,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "X-Praxis-Allowed-MCP-Tools": allowed_tools,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            response_payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        _die(3, f"tools/list HTTP {exc.code}: {exc.read().decode('utf-8', 'replace')}")
    except urllib.error.URLError as exc:
        _die(3, f"bridge unreachable at {url}: {exc.reason}")
    except Exception as exc:  # pragma: no cover - defensive catch-all
        _die(3, f"tools/list failed: {type(exc).__name__}: {exc}")

    if not isinstance(response_payload, dict):
        _die(3, "tools/list returned a non-object JSON-RPC envelope")
    if "error" in response_payload:
        err = response_payload["error"]
        code = err.get("code", "unknown") if isinstance(err, dict) else "unknown"
        message = err.get("message", err) if isinstance(err, dict) else err
        _die(3, f"tools/list returned error {code}: {message}")

    result = response_payload.get("result")
    if not isinstance(result, dict):
        _die(3, "tools/list returned an unexpected payload")

    tools = result.get("tools")
    if not isinstance(tools, list):
        _die(3, "tools/list result did not include a tool list")

    return [tool for tool in tools if isinstance(tool, dict)]


def _match_tool_definition(tools: list[dict[str, Any]], tool_name: str) -> dict[str, Any] | None:
    candidates = [tool_name]
    if not tool_name.startswith("praxis_"):
        candidates.append(f"praxis_{tool_name}")

    for candidate in candidates:
        for tool in tools:
            name = str(tool.get("name") or "").strip()
            if name == candidate:
                return tool
    return None


def _tool_search_text(tool: dict[str, Any]) -> str:
    return " ".join(
        part
        for part in (
            str(tool.get("name") or ""),
            str(tool.get("description") or ""),
            json.dumps(tool.get("inputSchema") or {}, sort_keys=True, default=str),
        )
        if part
    )


def _rank_tool(tool: dict[str, Any], query: str) -> tuple[int, int, int, str]:
    normalized_query = " ".join(query.lower().split())
    normalized_name = " ".join(str(tool.get("name") or "").lower().split())
    normalized_description = " ".join(str(tool.get("description") or "").lower().split())
    if not normalized_query:
        return (3, 0, 0, normalized_name)
    if normalized_name == normalized_query:
        return (0, 0, 0, normalized_name)
    if normalized_name.startswith(normalized_query):
        return (1, 0, 0, normalized_name)
    if normalized_description.startswith(normalized_query):
        return (1, 1, 0, normalized_name)
    search_text = " ".join(_tool_search_text(tool).lower().split())
    index = search_text.find(normalized_query)
    if index >= 0:
        return (2, 0, index, normalized_name)
    return (3, len(search_text), len(normalized_query), normalized_name)


def _search_tools(
    tools: list[dict[str, Any]],
    query: str,
    *,
    exact: bool,
) -> list[dict[str, Any]]:
    normalized_query = " ".join(query.lower().split())
    if not normalized_query:
        return []

    matches: list[dict[str, Any]] = []
    for tool in tools:
        normalized_name = " ".join(str(tool.get("name") or "").lower().split())
        normalized_description = " ".join(str(tool.get("description") or "").lower().split())
        search_text = " ".join(_tool_search_text(tool).lower().split())
        if exact:
            if normalized_query in {normalized_name, normalized_description}:
                matches.append(tool)
            continue
        if (
            normalized_query in normalized_name
            or normalized_query in normalized_description
            or normalized_query in search_text
        ):
            matches.append(tool)
    return sorted(matches, key=lambda tool: _rank_tool(tool, query))


def _render_tool_search_result(tool: dict[str, Any]) -> None:
    name = str(tool.get("name") or "").strip() or "(unknown)"
    description = str(tool.get("description") or "").strip()
    print(name)
    if description:
        print(f"  description: {description}")
    input_schema = tool.get("inputSchema")
    if isinstance(input_schema, dict):
        print(f"  inputSchema: {json.dumps(input_schema, sort_keys=True, default=str)}")


def _render_tool_description(tool: dict[str, Any]) -> None:
    name = str(tool.get("name") or "").strip() or "(unknown)"
    description = str(tool.get("description") or "").strip()
    input_schema = tool.get("inputSchema")

    print(f"tool: {name}")
    if description:
        print(f"description: {description}")
    if input_schema is None:
        print("inputSchema: {}")
        return
    print("inputSchema:")
    print(json.dumps(input_schema, indent=2, sort_keys=True, default=str))


def _print_result(result: Any) -> None:
    # MCP tool/call results typically come back as
    #   { "content": [{"type": "text", "text": "..."}, ...], "isError": bool }
    # When the `text` block contains JSON, pretty-print it so the agent sees
    # structured output. Otherwise pass text through verbatim. This makes the
    # binary ergonomic in shell pipelines without burying machine-readable
    # output behind extra wrappers.
    if isinstance(result, dict) and isinstance(result.get("content"), list):
        for block in result["content"]:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            text_value = block.get("text")
            if block_type == "text" and isinstance(text_value, str):
                stripped = text_value.strip()
                if stripped.startswith("{") or stripped.startswith("["):
                    try:
                        parsed = json.loads(stripped)
                        print(json.dumps(parsed, indent=2, sort_keys=True))
                        continue
                    except json.JSONDecodeError:
                        pass
                print(text_value)
            elif text_value is not None:
                print(text_value)
        if result.get("isError"):
            sys.exit(3)
        return
    print(json.dumps(result, indent=2, sort_keys=True, default=str))


def _parse_workflow_tools_call_args(argv: list[str]) -> tuple[str, dict[str, Any]]:
    """Support the canonical host-CLI shape `praxis workflow tools call <tool|alias|entrypoint> --input-json '{...}'`.

    The agent's prompts and CLAUDE.md instructions teach this exact syntax for
    every tool call on the host CLI. Keeping the sandbox binary compatible
    avoids teaching the agent a second vocabulary. `tools list`,
    `tools search`, and `tools describe` are also supported.
    """
    if not argv:
        _die(1, "usage: praxis workflow tools <list|search|describe|call> [args...]")
    verb = argv[0]
    if verb == "list":
        # tools/list is handled server-side; we pass through an empty call.
        return ("__tools_list__", {})
    if verb == "search":
        if len(argv) < 2:
            _die(1, "usage: praxis workflow tools search <text> [--exact]")
        return ("__tools_search__", {"argv": argv[1:]})
    if verb == "describe":
        if len(argv) < 2:
            _die(1, "usage: praxis workflow tools describe <tool|alias|entrypoint>")
        return ("__tools_describe__", {"argv": argv[1:]})
    if verb != "call":
        _die(1, f"unknown workflow tools verb: {verb!r}")
    if len(argv) < 2:
        _die(1, "usage: praxis workflow tools call <tool|alias|entrypoint> [--input-json '{...}'] [--yes]")
    return ("__tools_call__", {"argv": argv[1:]})


def _split_tool_reference_and_flags(argv: list[str]) -> tuple[str, list[str]]:
    """Split a tool reference from trailing flags.

    Tool references can contain spaces, so we keep consuming tokens until the
    first flag-style argument and then hand the rest to the normal parser.
    """

    reference_tokens: list[str] = []
    for index, arg in enumerate(argv):
        if arg.startswith("--"):
            return (" ".join(reference_tokens).strip(), argv[index:])
        reference_tokens.append(arg)
    return (" ".join(reference_tokens).strip(), [])


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if not argv or argv[0] in {"-h", "--help", "help"}:
        print(
            "praxis — in-sandbox MCP shim\n\n"
            "Two call shapes are supported:\n\n"
            "1) Short form:\n"
            "  praxis discover \"<query>\"\n"
            "  praxis query \"<question>\"\n"
            "  praxis recall \"<query>\" [--entity-type X] [--limit N]\n"
            "  praxis context_shard [--view X]\n"
            "  praxis health\n"
            "  praxis integration <list|describe|call> ...\n"
            "  praxis submit_code_change --summary ... --primary-paths ... --result-kind ...\n"
            "  praxis submit_artifact_bundle --summary ... --primary-paths ... --result-kind ...\n"
            "  praxis get_submission --submission-id X | --job-label Y\n\n"
            "2) Canonical workflow shape (matches the host CLI):\n"
            "  praxis workflow tools list\n"
            "  praxis workflow tools search <text> [--exact]\n"
            "  praxis workflow tools describe <tool|alias|entrypoint>\n"
            "  praxis workflow tools call <tool|alias|entrypoint> --input-json '{...}' [--yes]\n\n"
            "Aliases — `praxis workflow query`/`discover`/`recall`/`bugs`/`artifacts`/`health` also work.\n"
            "\nEnvironment:\n"
            f"  {_ENV_URL}      MCP bridge endpoint (injected by worker)\n"
            f"  {_ENV_TOKEN}    Signed session token (injected by worker)\n"
        )
        return 0

    url = str(os.environ.get(_ENV_URL, "")).strip()
    token = str(os.environ.get(_ENV_TOKEN, "")).strip()
    if not url:
        _die(2, f"{_ENV_URL} is not set — this binary must run inside a workflow sandbox.")
    if not token:
        _die(2, f"{_ENV_TOKEN} is not set — this binary must run inside a workflow sandbox.")

    # Route 1: canonical `praxis workflow ...` shape used by CLAUDE.md-trained
    # prompts and host-CLI muscle memory. The worker's host CLI exposes
    # `praxis workflow tools call <tool|alias|entrypoint> --input-json '{...}'`; we mirror
    # that exactly inside the sandbox so the agent's vocabulary is stable
    # regardless of context.
    if argv[0] == "workflow":
        sub = argv[1] if len(argv) >= 2 else ""
        if sub == "tools":
            tool_name, arguments = _parse_workflow_tools_call_args(argv[2:])
            if tool_name == "__tools_list__":
                allowed_tools = str(os.environ.get(_ENV_ALLOWED, "")).strip() or ""
                _print_result(_call_tools_list(url=url, token=token, allowed_tools=allowed_tools))
                return 0
            if tool_name == "__tools_search__":
                allowed_tools = str(os.environ.get(_ENV_ALLOWED, "")).strip() or ""
                tools = _call_tools_list(url=url, token=token, allowed_tools=allowed_tools)
                search_argv = list(arguments.get("argv") or [])
                query_parts: list[str] = []
                exact = False
                as_json = False
                for arg in search_argv:
                    if arg == "--exact":
                        exact = True
                    elif arg == "--json":
                        as_json = True
                    elif arg.startswith("--"):
                        _die(1, f"unknown search flag: {arg}")
                    else:
                        query_parts.append(arg)
                query = " ".join(query_parts).strip()
                if not query:
                    _die(1, "usage: praxis workflow tools search <text> [--exact] [--json]")
                matches = _search_tools(tools, query, exact=exact)
                if as_json:
                    print(json.dumps(matches, indent=2, sort_keys=True, default=str))
                    return 0
                if not matches:
                    print(f"no tools matched {query!r}")
                    return 0
                for tool in matches:
                    _render_tool_search_result(tool)
                    print()
                return 0
            if tool_name == "__tools_describe__":
                allowed_tools = str(os.environ.get(_ENV_ALLOWED, "")).strip() or ""
                tools = _call_tools_list(url=url, token=token, allowed_tools=allowed_tools)
                describe_argv = list(arguments.get("argv") or [])
                tool_reference, extra_flags = _split_tool_reference_and_flags(describe_argv)
                if not tool_reference:
                    _die(1, "usage: praxis workflow tools describe <tool|alias|entrypoint>")
                parser = argparse.ArgumentParser(prog="praxis workflow tools describe", add_help=False)
                parser.add_argument("--json", dest="as_json", action="store_true", default=False)
                ns, remaining = parser.parse_known_args(extra_flags)
                if remaining:
                    _die(1, f"unknown describe flag: {remaining[0]}")
                resolved_definition, candidates = _resolve_tool_definition(tool_reference)
                if resolved_definition is None:
                    _render_tool_lookup_failure(tool_reference, candidates, stdout=sys.stdout)
                    return 2
                tool = _match_tool_definition(tools, resolved_definition.name)
                if tool is None:
                    _render_tool_lookup_failure(tool_reference, [], stdout=sys.stdout)
                    return 2
                if ns.as_json:
                    print(json.dumps(tool, indent=2, sort_keys=True, default=str))
                    return 0
                _render_tool_description(tool)
                return 0
            if tool_name == "__tools_call__":
                allowed_tools = str(os.environ.get(_ENV_ALLOWED, "")).strip() or ""
                tools = _call_tools_list(url=url, token=token, allowed_tools=allowed_tools)
                call_argv = list(arguments.get("argv") or [])
                tool_reference, extra_flags = _split_tool_reference_and_flags(call_argv)
                if not tool_reference:
                    _die(1, "usage: praxis workflow tools call <tool|alias|entrypoint> [--input-json '{...}'] [--yes]")
                resolved_definition, candidates = _resolve_tool_definition(tool_reference)
                if resolved_definition is None:
                    _render_tool_lookup_failure(tool_reference, candidates, stdout=sys.stdout)
                    return 2
                if _match_tool_definition(tools, resolved_definition.name) is None:
                    _render_tool_lookup_failure(tool_reference, [], stdout=sys.stdout)
                    return 2
                parser = argparse.ArgumentParser(prog="praxis workflow tools call", add_help=False)
                parser.add_argument("--input-json", dest="input_json", default="{}")
                parser.add_argument("--yes", dest="yes", action="store_true", default=False)
                parser.add_argument("--args", dest="args_json", default=None)
                ns, remaining = parser.parse_known_args(extra_flags)
                if remaining:
                    _die(1, f"unknown call flag: {remaining[0]}")
                raw = ns.args_json if ns.args_json is not None else ns.input_json
                try:
                    call_arguments = json.loads(raw) if raw else {}
                except json.JSONDecodeError as exc:
                    _die(1, f"--input-json must be valid JSON: {exc}")
                if not isinstance(call_arguments, dict):
                    _die(1, "--input-json must parse to a JSON object")
                result = _post_jsonrpc(
                    url=url,
                    token=token,
                    tool_name=resolved_definition.name,
                    arguments=call_arguments,
                    allowed_tools=allowed_tools or resolved_definition.name,
                )
                _print_result(result)
                return 0
            # Fall through to tools/call POST for the older canonical MCP tool-name path.
            allowed_tools = str(os.environ.get(_ENV_ALLOWED, "")).strip() or tool_name
            result = _post_jsonrpc(
                url=url, token=token, tool_name=tool_name,
                arguments=arguments, allowed_tools=allowed_tools,
            )
            _print_result(result)
            return 0
        # `praxis workflow <short-subcommand>` aliases: query, discover, recall,
        # health, artifacts, bugs. Map to the tool_name directly.
        alias_map = {
            "query": "praxis_query",
            "discover": "praxis_discover",
            "recall": "praxis_recall",
            "health": "praxis_health",
            "artifacts": "praxis_artifacts",
            "bugs": "praxis_bugs",
        }
        if sub in alias_map:
            tool_name = alias_map[sub]
            remaining = argv[2:]
            # Reuse the short-form parser by mapping to a known _COMMANDS entry.
            short_name = sub
            if short_name in _COMMANDS:
                _, specs = _COMMANDS[short_name]
                arguments = _parse_subcommand_args(short_name, specs, remaining)
            else:
                # Default: treat first positional as the primary string input.
                arguments = {"query": " ".join(remaining)} if remaining else {}
            allowed_tools = str(os.environ.get(_ENV_ALLOWED, "")).strip() or tool_name
            result = _post_jsonrpc(
                url=url, token=token, tool_name=tool_name,
                arguments=arguments, allowed_tools=allowed_tools,
            )
            _print_result(result)
            return 0
        _die(1, f"unknown workflow subcommand: {sub!r}. Try `praxis workflow tools call ...`.")

    # Route 2: short form `praxis <subcommand> ...`.
    subcommand = argv[0]
    if subcommand not in _COMMANDS:
        _die(1, f"unknown subcommand: {subcommand!r}. Run `praxis --help` for the list.")

    tool_name, specs = _COMMANDS[subcommand]
    arguments = _parse_subcommand_args(subcommand, specs, argv[1:])
    allowed_tools = str(os.environ.get(_ENV_ALLOWED, "")).strip() or tool_name

    result = _post_jsonrpc(
        url=url, token=token, tool_name=tool_name, arguments=arguments, allowed_tools=allowed_tools
    )
    _print_result(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
