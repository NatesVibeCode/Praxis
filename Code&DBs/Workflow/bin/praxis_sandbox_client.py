#!/usr/bin/env python3
"""In-sandbox workflow MCP front door.

This is the `praxis` command that gets baked into every sandbox image
(`praxis-worker`, `praxis-codex`, `praxis-claude`, `praxis-gemini`). It gives
the agent a single shell-callable interface for the workflow MCP surface:

    praxis workflow tools search "retry logic"
    praxis workflow tools call praxis_query --input-json '{"question":"what is failing right now?"}'
    praxis workflow tools call praxis_submit_code_change --input-json '{"summary":"...","primary_paths":["foo.py"],"result_kind":"code_change"}'

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

_ENV_URL = "PRAXIS_WORKFLOW_MCP_URL"
_ENV_TOKEN = "PRAXIS_WORKFLOW_MCP_TOKEN"
_ENV_ALLOWED = "PRAXIS_ALLOWED_MCP_TOOLS"


def _die(exit_code: int, message: str) -> None:
    sys.stderr.write(f"praxis: {message}\n")
    sys.exit(exit_code)


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
    """Support the canonical host-CLI shape `praxis workflow tools call <tool> --input-json '{...}'`.

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
            _die(1, "usage: praxis workflow tools describe <tool_name>")
        return ("__tools_describe__", {"tool": argv[1]})
    if verb != "call":
        _die(1, f"unknown workflow tools verb: {verb!r}")
    if len(argv) < 2:
        _die(1, "usage: praxis workflow tools call <tool_name> [--input-json '{...}'] [--yes]")
    tool_name = argv[1]
    if not tool_name.startswith("praxis_"):
        tool_name = f"praxis_{tool_name}"
    parser = argparse.ArgumentParser(prog="praxis workflow tools call", add_help=False)
    parser.add_argument("--input-json", dest="input_json", default="{}")
    parser.add_argument("--yes", dest="yes", action="store_true", default=False)
    parser.add_argument("--args", dest="args_json", default=None)
    ns, _extra = parser.parse_known_args(argv[2:])
    raw = ns.args_json if ns.args_json is not None else ns.input_json
    try:
        arguments = json.loads(raw) if raw else {}
    except json.JSONDecodeError as exc:
        _die(1, f"--input-json must be valid JSON: {exc}")
    if not isinstance(arguments, dict):
        _die(1, "--input-json must parse to a JSON object")
    return (tool_name, arguments)


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if not argv or argv[0] in {"-h", "--help", "help"}:
        print(
            "praxis — in-sandbox workflow MCP front door\n\n"
            "Canonical workflow shape:\n"
            "  praxis workflow tools list\n"
            "  praxis workflow tools search <text> [--exact]\n"
            "  praxis workflow tools describe <tool_name>\n"
            "  praxis workflow tools call <tool_name> --input-json '{...}' [--yes]\n\n"
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

    # Canonical `praxis workflow ...` shape used by CLAUDE.md-trained
    # prompts and host-CLI muscle memory. The worker's host CLI exposes
    # `praxis workflow tools call <tool> --input-json '{...}'`; we mirror
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
                tool = _match_tool_definition(tools, str(arguments["tool"]))
                if tool is None:
                    _die(2, f"unknown tool: {arguments['tool']}")
                _render_tool_description(tool)
                return 0
            # Fall through to tools/call POST.
            allowed_tools = str(os.environ.get(_ENV_ALLOWED, "")).strip() or tool_name
            result = _post_jsonrpc(
                url=url, token=token, tool_name=tool_name,
                arguments=arguments, allowed_tools=allowed_tools,
            )
            _print_result(result)
            return 0
        _die(1, f"unknown workflow subcommand: {sub!r}. Try `praxis workflow tools call ...`.")

    _die(1, "unknown command shape. Use `praxis workflow tools call ...`.")


if __name__ == "__main__":
    sys.exit(main())
