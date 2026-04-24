"""Operational CLI command handlers."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen
from urllib.parse import urlsplit

from typing import Any, TextIO

from surfaces.cli.mcp_tools import (
    get_definition,
    print_json,
    render_health_payload,
    require_confirmation,
    run_cli_tool,
)


def _workspace_repo_root():
    from runtime.workspace_paths import repo_root as workspace_repo_root

    return workspace_repo_root()


def _workflow_tool(params: dict[str, object]) -> dict[str, object]:
    from surfaces.mcp.tools.workflow import tool_praxis_workflow

    return tool_praxis_workflow(params)


def _api_discovery_text() -> str:
    return (
        "Discovery shortcuts:\n"
        "  workflow routes                alias for live HTTP route discovery\n"
        "  workflow api help              show API route-discovery help from the API namespace\n"
        "  workflow api help routes       show the full live HTTP route catalog help\n"
        "  workflow api help integrations  show the /api/integrations scoped help\n"
        "  workflow api help data-dictionary  show the /api/data-dictionary scoped help\n"
        "  workflow integrations          alias for /api/integrations route discovery\n"
        "  workflow api integrations      same scoped route discovery from the api namespace\n"
        "  workflow api data-dictionary   same scoped route discovery for the data dictionary API\n"
        "  workflow routes --json         machine-readable route catalog\n"
        "  workflow help routes           same discovery help from the root help system\n"
        "  workflow tools list            browse catalog-backed MCP tools\n"
        "  workflow tools search <text>    search tools by topic, alias, or entrypoint\n"
    )


def _as_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _coerce_text(value: object, fallback: str = "<unset>") -> str:
    text = str(value or "").strip()
    return text if text else fallback


def _endpoint_signature(raw: str) -> str:
    parsed = urlsplit(raw)
    if not parsed.scheme and not parsed.netloc:
        return raw.strip().rstrip("/") if raw else "<unset>"
    host = parsed.hostname or ""
    if not host:
        return raw.strip().rstrip("/") if raw else "<unset>"
    port = parsed.port
    if port is None:
        return f"{parsed.scheme}://{host}"
    return f"{parsed.scheme}://{host}:{port}"


def _database_authority_signature(raw: str) -> str:
    parsed = urlsplit(raw)
    if not parsed.scheme or not parsed.netloc:
        return raw.strip() if raw else "<unset>"
    host = (parsed.hostname or "").lower()
    if host in {"localhost", "127.0.0.1", "::1", "host.docker.internal"}:
        host = "local-runtime-db-host"
    port = parsed.port or 5432
    path = parsed.path.rstrip("/")
    return f"{parsed.scheme}://{host}:{port}{path}"


def _instances_command(args: list[str], *, stdout: TextIO) -> int:
    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow instances [check] [--json] [--include-routes]\n"
            "\n"
            "Show resolved API/MCP/DB authority and report any obvious instance drift.\n"
            "Use `workflow instances check` to print an alignment report.\n"
            "Use `workflow instances --json` for machine-readable output.\n"
            "Use `workflow instances --include-routes` to include the live HTTP route count.\n"
        )
        return 0

    perform_check = False
    as_json = False
    include_routes = False
    for arg in args:
        if arg == "check":
            perform_check = True
        elif arg == "--json":
            as_json = True
        elif arg == "--include-routes":
            include_routes = True
        elif arg.startswith("--"):
            stdout.write(f"error: unknown flag: {arg}\n")
            return 2
        else:
            stdout.write(f"error: unknown argument: {arg}\n")
            return 2

    from runtime.primitive_contracts import redact_url
    from runtime.setup_wizard import setup_payload_for_cli
    from surfaces._workflow_database import workflow_database_authority_for_repo
    from surfaces.mcp.subsystems import workflow_database_env as mcp_workflow_database_env

    setup_payload = setup_payload_for_cli("doctor", repo_root=_workspace_repo_root())
    runtime_target = _as_dict(setup_payload.get("runtime_target"))
    orient_exit_code, orient_payload = run_cli_tool(
        "praxis_orient",
        {"fast": True, "skip_engineering_observability": True, "compact": True},
    )
    orient_data = _as_dict(orient_payload if orient_exit_code == 0 else {})
    authority_envelope = _as_dict(orient_data.get("authority_envelope"))
    primitive_contracts = _as_dict(authority_envelope.get("primitive_contracts"))
    if not primitive_contracts and isinstance(orient_data.get("primitive_contracts"), dict):
        primitive_contracts = _as_dict(orient_data.get("primitive_contracts"))
    runtime_binding = _as_dict(primitive_contracts.get("runtime_binding"))
    binding_http = _as_dict(runtime_binding.get("http_endpoints"))
    binding_db = _as_dict(runtime_binding.get("database"))
    native_instance = _as_dict(orient_data.get("native_instance"))
    if native_instance.get("status") == "skipped":
        native_instance = _as_dict(setup_payload.get("native_instance"))

    route_payload: dict[str, object] = {"status": "skipped", "reason": "use --include-routes"}
    if include_routes:
        from surfaces.api.rest import list_api_routes

        route_payload = list_api_routes()

    cli_surface = _as_dict(orient_data.get("cli_surface"))
    tool_count = 0
    raw_tool_count = cli_surface.get("tool_count")
    if raw_tool_count is None:
        raw_tool_count = orient_data.get("tool_guidance", {}).get("preferred_operator_surface", {}).get("tool_count")
    if raw_tool_count is not None:
        try:
            tool_count = int(raw_tool_count)
        except (TypeError, ValueError):
            tool_count = 0

    setup_api = _coerce_text(runtime_target.get("api_authority"))
    setup_db = _coerce_text(runtime_target.get("db_authority"))
    binding_api = _coerce_text(binding_http.get("api_base_url"))
    binding_db_display = _coerce_text(binding_db.get("redacted_url"), fallback=_coerce_text(binding_db.get("configured")))
    cli_db_display = "<unset>"
    cli_db_source = "unresolved"
    try:
        cli_authority = workflow_database_authority_for_repo(
            _workspace_repo_root(),
            env=os.environ,
        )
        cli_db_display = _coerce_text(redact_url(cli_authority.database_url))
        cli_db_source = str(cli_authority.source)
    except Exception as exc:
        cli_db_source = f"error:{type(exc).__name__}"

    mcp_env = mcp_workflow_database_env()
    mcp_db_display = _coerce_text(redact_url(mcp_env.get("WORKFLOW_DATABASE_URL")))
    mcp_db_source = _coerce_text(mcp_env.get("WORKFLOW_DATABASE_AUTHORITY_SOURCE"))
    api_match = _endpoint_signature(setup_api) == _endpoint_signature(binding_api)
    setup_db_match = (
        _database_authority_signature(setup_db)
        == _database_authority_signature(binding_db_display)
    )
    cli_mcp_db_match = (
        _database_authority_signature(cli_db_display)
        == _database_authority_signature(mcp_db_display)
    )
    cli_orient_db_match = (
        _database_authority_signature(cli_db_display)
        == _database_authority_signature(binding_db_display)
    )
    db_match = setup_db_match and cli_mcp_db_match and cli_orient_db_match

    alignment_errors: list[str] = []
    if setup_api == "<unset>":
        alignment_errors.append("runtime target API authority is not set")
    if setup_db == "<unset>":
        alignment_errors.append("runtime target DB authority is not set")
    if not api_match:
        alignment_errors.append(
            f"API authority mismatch: runtime_target={setup_api} orient={binding_api}"
        )
    if not db_match:
        if not setup_db_match:
            alignment_errors.append(
                f"DB authority mismatch: runtime_target={setup_db} orient={binding_db_display}"
            )
        if not cli_mcp_db_match:
            alignment_errors.append(
                f"DB authority mismatch: cli={cli_db_display} mcp={mcp_db_display}"
            )
        if not cli_orient_db_match:
            alignment_errors.append(
                f"DB authority mismatch: cli={cli_db_display} orient={binding_db_display}"
            )
    if orient_exit_code != 0:
        alignment_errors.append("praxis_orient call failed")

    payload = {
        "setup": setup_payload,
        "orient": orient_data,
        "route_catalog": {
            "included": include_routes,
            "visibility": "public",
            "count": route_payload.get("count"),
            "docs_url": route_payload.get("docs_url"),
            "openapi_url": route_payload.get("openapi_url"),
            "redoc_url": route_payload.get("redoc_url"),
            "status": route_payload.get("status"),
            "reason": route_payload.get("reason"),
        },
        "instances": {
            "native_instance": native_instance,
            "runtime_binding": runtime_binding,
            "setup_api": setup_api,
            "bind_api": binding_api,
            "setup_db": setup_db,
            "bind_db": binding_db_display,
            "cli_db": cli_db_display,
            "cli_db_source": cli_db_source,
            "mcp_db": mcp_db_display,
            "mcp_db_source": mcp_db_source,
            "mcp_transport": os.environ.get("PRAXIS_MCP_STDIO_TRANSPORT", "mirror"),
            "tool_count": tool_count,
            "api_routes": route_payload.get("count"),
            "setup_authority_reachable": bool(
                setup_payload.get("api_mcp_authority_reachable", True)
            ),
            "db_signatures": {
                "setup": _database_authority_signature(setup_db),
                "orient": _database_authority_signature(binding_db_display),
                "cli": _database_authority_signature(cli_db_display),
                "mcp": _database_authority_signature(mcp_db_display),
            },
        },
        "checks": {
            "do_check": perform_check,
            "api_match": api_match,
            "db_match": db_match,
            "setup_db_match": setup_db_match,
            "cli_mcp_db_match": cli_mcp_db_match,
            "cli_orient_db_match": cli_orient_db_match,
            "errors": alignment_errors,
        },
    }

    if as_json:
        stdout.write(json.dumps(payload, indent=2, default=str) + "\n")
        if perform_check and alignment_errors:
            return 1
        if orient_exit_code != 0 or setup_payload.get("error"):
            return 1
        return 0

    native_instance_name = _coerce_text(
        native_instance.get("instance_name") or native_instance.get("praxis_instance_name"),
        fallback="<unknown>",
    )
    runtime_profile_ref = _coerce_text(
        native_instance.get("runtime_profile_ref") or native_instance.get("praxis_runtime_profile"),
        fallback="<unknown>",
    )
    repo_root = _coerce_text(
        native_instance.get("repo_root"),
        fallback=_coerce_text(setup_payload.get("workspace")),
    )
    route_count = route_payload.get("count")
    route_count_text = str(route_count) if isinstance(route_count, int) else "n/a"
    stdout.write("runtime instances:\n")
    stdout.write(f"  native_instance_name: {native_instance_name}\n")
    stdout.write(f"  runtime_profile_ref: {runtime_profile_ref}\n")
    stdout.write(f"  repo_root: {repo_root}\n")
    stdout.write("  authority_targets:\n")
    stdout.write(f"    api_target_setup: {setup_api}\n")
    stdout.write(f"    api_target_bind:  {binding_api}\n")
    stdout.write(f"    db_target_setup:  {setup_db}\n")
    stdout.write(f"    db_target_bind:   {binding_db_display}\n")
    stdout.write(f"    db_target_cli:    {cli_db_display} ({cli_db_source})\n")
    stdout.write(f"    db_target_mcp:    {mcp_db_display} ({mcp_db_source})\n")
    stdout.write("  mcp_surface:\n")
    stdout.write(f"    stdio_transport: {os.environ.get('PRAXIS_MCP_STDIO_TRANSPORT', 'mirror')}\n")
    stdout.write(f"    catalog_tools:   {tool_count}\n")
    if include_routes:
        stdout.write("  api_contract:\n")
        stdout.write(f"    public_routes:  {route_count_text}\n")
        stdout.write(f"    docs_url:       {_coerce_text(route_payload.get('docs_url'))}\n")
        stdout.write(f"    openapi_url:    {_coerce_text(route_payload.get('openapi_url'))}\n")
        stdout.write(f"    redoc_url:      {_coerce_text(route_payload.get('redoc_url'))}\n")
    else:
        stdout.write("  api_contract: skipped (use --include-routes)\n")

    if perform_check:
        if alignment_errors:
            stdout.write("\nalignment: FAIL\n")
            for issue in alignment_errors:
                stdout.write(f"  - {issue}\n")
            return 1
        stdout.write("\nalignment: PASS\n")
        stdout.write(
            "  API, MCP, and DB contracts are aligned across setup + /orient surfaces.\n"
        )

    return 0


def _api_help_text() -> str:
    return (
        "usage: workflow api [routes|integrations|data-dictionary|--host HOST|--port PORT]\n"
        "\n"
        "Start the Praxis REST API server.\n"
        "Reads the runtime dependency contract from requirements.runtime.txt\n"
        "Flat alias: workflow routes\n"
        "Help alias usage: workflow api [help|routes|integrations|data-dictionary|--host HOST|--port PORT]\n"
        "\n"
        "  help          show API route-discovery help without starting the server\n"
        "  routes        show and filter the live HTTP route catalog without starting the server\n"
        "  integrations  show and filter the /api/integrations route scope without starting the server\n"
        "  data-dictionary show and filter the /api/data-dictionary route scope without starting the server\n"
        "  --host HOST   bind address (default: 127.0.0.1; 0.0.0.0 for LAN/container)\n"
        "  --port PORT   TCP port     (default: 8420)\n"
        "\n"
        f"{_api_discovery_text()}"
    )


def _integration_help_text() -> str:
    return "\n".join(
        [
            "usage: workflow integration [list|describe|health|test|call|create|secret|reload|help] [args]",
            "",
            "Integration authority:",
            "  workflow integration help",
            "  workflow integration list [--json]",
            "  workflow integration describe <integration_id> [--json]",
            "  workflow integration health [--json]",
            "  workflow integration test <integration_id> [--json]",
            "  workflow integration call <integration_id> <integration_action> [--args-json '<json>'] [--json] [--yes]",
            "  workflow integration create --id <id> --name <name> --capabilities-json '<json>' [--auth-json '<json>'] [--description <text>] [--provider <slug>] [--manifest-source <source>] [--json] [--yes]",
            "  workflow integration secret <integration_id> --value <secret> [--json] [--yes]",
            "  workflow integration reload [--json] [--yes]",
            "",
            "Tip: `workflow integrations` and `workflow api integrations` discover the HTTP route scope; `workflow integration` manages the integration registry.",
            "Tip: run `workflow tools describe praxis_integration` to inspect the catalog-backed tool metadata.",
            "Tip: run `workflow tools list` to discover the same entrypoint through the shared tool catalog.",
        ]
    )


def _orient_command(args: list[str], *, stdout: TextIO) -> int:
    if args and args[0] in {"-h", "--help", "help"}:
        stdout.write(
            "usage: workflow orient [--json]\n\n"
            "Return the canonical /orient authority envelope through the catalog-backed MCP tool.\n"
        )
        return 0
    unknown = [arg for arg in args if arg != "--json"]
    if unknown:
        stdout.write(f"error: unknown orient argument: {unknown[0]}\n")
        stdout.write("usage: workflow orient [--json]\n")
        return 2
    exit_code, payload = run_cli_tool(
        "praxis_orient",
        {"fast": True, "skip_engineering_observability": True, "compact": True},
    )
    print_json(stdout, payload)
    return exit_code


def _load_json_value(raw: str, *, field_name: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON for {field_name}: {exc}") from exc


def _integration_capabilities_actions(payload: object) -> list[str]:
    if not isinstance(payload, list):
        return []
    actions: list[str] = []
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        action = str(entry.get("action") or entry.get("name") or "").strip()
        if action:
            actions.append(action)
    return actions


def _render_integration_list(payload: dict[str, Any], *, stdout: TextIO) -> None:
    integrations = payload.get("integrations")
    if not isinstance(integrations, list):
        print_json(stdout, payload)
        return
    if not integrations:
        stdout.write("no integrations found\n")
        return

    header = f"{'INTEGRATION':<24} {'STATUS':<12} {'PROVIDER':<12} CAPABILITIES"
    stdout.write(header + "\n")
    stdout.write("-" * len(header) + "\n")
    for row in integrations:
        if not isinstance(row, dict):
            continue
        cap_text = ", ".join(_integration_capabilities_actions(row.get("capabilities"))[:4]) or "-"
        stdout.write(
            f"{str(row.get('id') or row.get('integration_id') or ''):<24} "
            f"{str(row.get('auth_status') or ''):<12} "
            f"{str(row.get('provider') or ''):<12} "
            f"{cap_text}\n"
        )
    stdout.write(f"\n{len(integrations)} integration(s)\n")


def _render_integration_describe(payload: dict[str, Any], *, stdout: TextIO) -> None:
    if not isinstance(payload, dict) or payload.get("error"):
        print_json(stdout, payload)
        return

    integration_id = str(payload.get("id") or payload.get("integration_id") or "").strip()
    if integration_id:
        stdout.write(f"Integration: {integration_id}\n")
    for key in ("name", "description", "provider", "auth_status", "manifest_source", "connector_slug"):
        value = payload.get(key)
        if value not in (None, ""):
            stdout.write(f"  {key}: {value}\n")

    capabilities = _integration_capabilities_actions(payload.get("capabilities"))
    if capabilities:
        stdout.write("  capabilities:\n")
        for action in capabilities:
            stdout.write(f"    - {action}\n")


def _run_integration_tool(
    params: dict[str, object],
    *,
    stdout: TextIO,
    as_json: bool,
    render: str,
) -> int:
    exit_code, payload = run_cli_tool("praxis_integration", params)
    if as_json or payload.get("error"):
        print_json(stdout, payload)
        return exit_code
    if render == "list":
        _render_integration_list(payload, stdout=stdout)
        return exit_code
    if render == "describe":
        _render_integration_describe(payload, stdout=stdout)
        return exit_code
    print_json(stdout, payload)
    return exit_code


def _integrations_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help", "help"}:
        stdout.write(_integration_help_text() + "\n")
        return 0

    subcommand = args[0]
    tail = args[1:]
    if subcommand == "list":
        as_json = False
        if tail:
            if tail == ["--json"]:
                as_json = True
            else:
                stdout.write(f"unknown integration argument: {' '.join(tail)}\n")
                return 2
        return _run_integration_tool({"action": "list"}, stdout=stdout, as_json=as_json, render="list")

    if subcommand == "describe":
        as_json = False
        integration_id = ""
        i = 0
        while i < len(tail):
            token = tail[i]
            if token == "--json":
                as_json = True
                i += 1
                continue
            if token.startswith("--"):
                stdout.write(f"unknown integration argument: {token}\n")
                return 2
            if not integration_id:
                integration_id = token
                i += 1
                continue
            stdout.write(f"unexpected argument: {token}\n")
            return 2
        if not integration_id:
            stdout.write("usage: workflow integration describe <integration_id> [--json]\n")
            return 2
        return _run_integration_tool(
            {"action": "describe", "integration_id": integration_id},
            stdout=stdout,
            as_json=as_json,
            render="describe",
        )

    if subcommand == "health":
        as_json = False
        if tail:
            if tail == ["--json"]:
                as_json = True
            else:
                stdout.write(f"unknown integration argument: {' '.join(tail)}\n")
                return 2
        return _run_integration_tool({"action": "health"}, stdout=stdout, as_json=as_json, render="json")

    if subcommand == "test":
        as_json = False
        integration_id = ""
        i = 0
        while i < len(tail):
            token = tail[i]
            if token == "--json":
                as_json = True
                i += 1
                continue
            if token.startswith("--"):
                stdout.write(f"unknown integration argument: {token}\n")
                return 2
            if not integration_id:
                integration_id = token
                i += 1
                continue
            stdout.write(f"unexpected argument: {token}\n")
            return 2
        if not integration_id:
            stdout.write("usage: workflow integration test <integration_id> [--json]\n")
            return 2
        return _run_integration_tool(
            {"action": "test_credentials", "integration_id": integration_id},
            stdout=stdout,
            as_json=as_json,
            render="json",
        )

    if subcommand == "call":
        as_json = False
        confirmed = False
        integration_id = ""
        integration_action = ""
        args_json: dict[str, object] | None = None
        i = 0
        while i < len(tail):
            token = tail[i]
            if token == "--json":
                as_json = True
                i += 1
                continue
            if token == "--yes":
                confirmed = True
                i += 1
                continue
            if token == "--args-json":
                if i + 1 >= len(tail):
                    stdout.write("error: --args-json requires a value\n")
                    return 2
                try:
                    raw_args = _load_json_value(tail[i + 1], field_name="--args-json")
                except ValueError as exc:
                    stdout.write(f"error: {exc}\n")
                    return 2
                if not isinstance(raw_args, dict):
                    stdout.write("error: --args-json must decode to a JSON object\n")
                    return 2
                args_json = dict(raw_args)
                i += 2
                continue
            if token.startswith("--"):
                stdout.write(f"unknown integration argument: {token}\n")
                return 2
            if not integration_id:
                integration_id = token
            elif not integration_action:
                integration_action = token
            else:
                stdout.write(f"unexpected argument: {token}\n")
                return 2
            i += 1
        if not integration_id or not integration_action:
            stdout.write(
                "usage: workflow integration call <integration_id> <integration_action> [--args-json '<json>'] [--json] [--yes]\n"
            )
            return 2
        definition = get_definition("praxis_integration")
        if definition is None:
            stdout.write("tool definition not found: praxis_integration\n")
            return 2
        params: dict[str, object] = {
            "action": "call",
            "integration_id": integration_id,
            "integration_action": integration_action,
            "args": args_json or {},
        }
        confirmation_result = require_confirmation(
            definition,
            params,
            confirmed=confirmed,
            stdout=stdout,
        )
        if confirmation_result is not None:
            return confirmation_result
        return _run_integration_tool(params, stdout=stdout, as_json=as_json, render="json")

    if subcommand == "create":
        as_json = False
        confirmed = False
        integration_id = ""
        name = ""
        description = ""
        provider = "http"
        manifest_source = "cli"
        capabilities: list[dict[str, object]] | None = None
        auth: dict[str, object] | None = None
        i = 0
        while i < len(tail):
            token = tail[i]
            if token == "--json":
                as_json = True
                i += 1
                continue
            if token == "--yes":
                confirmed = True
                i += 1
                continue
            if token in {"--id", "--name", "--description", "--provider", "--manifest-source", "--capabilities-json", "--auth-json"}:
                if i + 1 >= len(tail):
                    stdout.write(f"error: {token} requires a value\n")
                    return 2
                value = tail[i + 1]
                if token == "--id":
                    integration_id = value
                elif token == "--name":
                    name = value
                elif token == "--description":
                    description = value
                elif token == "--provider":
                    provider = value
                elif token == "--manifest-source":
                    manifest_source = value
                elif token == "--capabilities-json":
                    try:
                        raw_capabilities = _load_json_value(value, field_name="--capabilities-json")
                    except ValueError as exc:
                        stdout.write(f"error: {exc}\n")
                        return 2
                    if not isinstance(raw_capabilities, list) or not raw_capabilities:
                        stdout.write("error: --capabilities-json must decode to a non-empty JSON array\n")
                        return 2
                    capabilities = []
                    for item in raw_capabilities:
                        if not isinstance(item, dict):
                            stdout.write("error: each capability must be a JSON object\n")
                            return 2
                        capabilities.append(dict(item))
                elif token == "--auth-json":
                    try:
                        raw_auth = _load_json_value(value, field_name="--auth-json")
                    except ValueError as exc:
                        stdout.write(f"error: {exc}\n")
                        return 2
                    if not isinstance(raw_auth, dict):
                        stdout.write("error: --auth-json must decode to a JSON object\n")
                        return 2
                    auth = dict(raw_auth)
                i += 2
                continue
            stdout.write(f"unknown integration argument: {token}\n")
            return 2
        if not integration_id or not name or not capabilities:
            stdout.write(
                "usage: workflow integration create --id <id> --name <name> --capabilities-json '<json>' [--auth-json '<json>'] [--description <text>] [--provider <slug>] [--manifest-source <source>] [--json] [--yes]\n"
            )
            return 2
        definition = get_definition("praxis_integration")
        if definition is None:
            stdout.write("tool definition not found: praxis_integration\n")
            return 2
        params = {
            "action": "create",
            "integration_id": integration_id,
            "name": name,
            "description": description,
            "provider": provider,
            "manifest_source": manifest_source,
            "capabilities": capabilities,
        }
        if auth is not None:
            params["auth"] = auth
        confirmation_result = require_confirmation(
            definition,
            params,
            confirmed=confirmed,
            stdout=stdout,
        )
        if confirmation_result is not None:
            return confirmation_result
        return _run_integration_tool(params, stdout=stdout, as_json=as_json, render="json")

    if subcommand == "secret":
        as_json = False
        confirmed = False
        integration_id = ""
        value = ""
        i = 0
        while i < len(tail):
            token = tail[i]
            if token == "--json":
                as_json = True
                i += 1
                continue
            if token == "--yes":
                confirmed = True
                i += 1
                continue
            if token == "--value":
                if i + 1 >= len(tail):
                    stdout.write("error: --value requires a value\n")
                    return 2
                value = tail[i + 1]
                i += 2
                continue
            if token.startswith("--"):
                stdout.write(f"unknown integration argument: {token}\n")
                return 2
            if not integration_id:
                integration_id = token
                i += 1
                continue
            stdout.write(f"unexpected argument: {token}\n")
            return 2
        if not integration_id or not value:
            stdout.write("usage: workflow integration secret <integration_id> --value <secret> [--json] [--yes]\n")
            return 2
        definition = get_definition("praxis_integration")
        if definition is None:
            stdout.write("tool definition not found: praxis_integration\n")
            return 2
        params = {"action": "set_secret", "integration_id": integration_id, "value": value}
        confirmation_result = require_confirmation(
            definition,
            params,
            confirmed=confirmed,
            stdout=stdout,
        )
        if confirmation_result is not None:
            return confirmation_result
        return _run_integration_tool(params, stdout=stdout, as_json=as_json, render="json")

    if subcommand == "reload":
        as_json = False
        confirmed = False
        if tail:
            for token in tail:
                if token == "--json":
                    as_json = True
                elif token == "--yes":
                    confirmed = True
                else:
                    stdout.write(f"unknown integration argument: {token}\n")
                    return 2
        definition = get_definition("praxis_integration")
        if definition is None:
            stdout.write("tool definition not found: praxis_integration\n")
            return 2
        params = {"action": "reload"}
        confirmation_result = require_confirmation(
            definition,
            params,
            confirmed=confirmed,
            stdout=stdout,
        )
        if confirmation_result is not None:
            return confirmation_result
        return _run_integration_tool(params, stdout=stdout, as_json=as_json, render="json")

    stdout.write(f"unknown integrations subcommand: {subcommand}\n")
    return 2


def _api_routes_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow api routes` — list the live HTTP route catalog."""

    import json as _json

    from surfaces.api.rest import list_api_routes

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow api routes [--search TEXT] [--method METHOD] [--tag TAG] [--path-prefix PREFIX] [--visibility public|internal|all] [--json]\n"
            "\n"
            "Show the live FastAPI route catalog without starting the server.\n"
            "By default this shows the public `/v1` contract. Use --visibility all to inspect internal compatibility routes too.\n"
            "\n"
            "Examples:\n"
            "  praxis workflow api routes\n"
            "  praxis workflow routes\n"
            "  praxis workflow api routes --search runs --method GET\n"
            "  praxis workflow routes --visibility all --path-prefix /api --json\n"
            "  praxis workflow api routes --path-prefix /v1/runs --json\n"
            "  workflow api routes --json\n"
            "\n"
            "Tip: plain output also shows the most common methods and tags, plus a suggested follow-up filter.\n"
            "Tip: JSON output includes a summary facet block for downstream tooling.\n"
            "Tip: `workflow routes --json` is the flat alias when you already know the surface you want.\n"
            "\n"
            f"{_api_discovery_text()}"
        )
        return 0

    as_json = False
    search = None
    method = None
    tag = None
    path_prefix = None
    visibility = "public"
    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--json":
            as_json = True
            i += 1
        elif arg == "--search":
            if i + 1 >= len(args):
                stdout.write("error: --search requires a value\n")
                return 2
            search = args[i + 1]
            i += 2
        elif arg == "--method":
            if i + 1 >= len(args):
                stdout.write("error: --method requires a value\n")
                return 2
            method = args[i + 1]
            i += 2
        elif arg == "--tag":
            if i + 1 >= len(args):
                stdout.write("error: --tag requires a value\n")
                return 2
            tag = args[i + 1]
            i += 2
        elif arg == "--path-prefix":
            if i + 1 >= len(args):
                stdout.write("error: --path-prefix requires a value\n")
                return 2
            path_prefix = args[i + 1]
            i += 2
        elif arg == "--visibility":
            if i + 1 >= len(args):
                stdout.write("error: --visibility requires a value\n")
                return 2
            visibility = args[i + 1]
            if visibility not in {"public", "internal", "all"}:
                stdout.write("error: --visibility must be one of: public, internal, all\n")
                return 2
            i += 2
        else:
            stdout.write(f"error: unknown argument: {arg}\n")
            return 2

    payload = list_api_routes(
        search=search,
        method=method,
        tag=tag,
        path_prefix=path_prefix,
        visibility=visibility,
    )
    if as_json:
        print_json(stdout, payload)
        return 0

    routes = list(payload.get("routes", []))
    summary = payload.get("summary")
    stdout.write(f"API route catalog ({payload.get('count', len(routes))} routes)\n")
    stdout.write(f"  docs:    {payload.get('docs_url')}\n")
    stdout.write(f"  openapi: {payload.get('openapi_url')}\n")
    stdout.write(f"  redoc:   {payload.get('redoc_url')}\n")
    filters = payload.get("filters")
    if isinstance(filters, dict) and filters:
        rendered_filters = " ".join(f"{key}={value}" for key, value in sorted(filters.items()))
        stdout.write(f"  filters: {rendered_filters}\n")
    if isinstance(summary, dict):
        methods = _render_route_facets(summary.get("methods"), field_name="method")
        tags = _render_route_facets(summary.get("tags"), field_name="tag")
        suggested = summary.get("suggested_filters")
        if methods:
            stdout.write(f"  methods: {methods}\n")
        if tags:
            stdout.write(f"  tags:    {tags}\n")
        if (not isinstance(filters, dict) or not filters) and isinstance(suggested, dict):
            follow_up = []
            tag = str(suggested.get("tag") or "").strip()
            method = str(suggested.get("method") or "").strip()
            if tag:
                follow_up.append(f"--tag {tag}")
            if method:
                follow_up.append(f"--method {method}")
            if follow_up:
                stdout.write(f"  try:    workflow api routes {' '.join(follow_up)}\n")
    stdout.write(
        "  Tip: workflow routes is the flat alias; workflow help routes reopens the discovery help.\n"
    )
    stdout.write("\n")
    if not routes:
        stdout.write("No routes found.\n")
        return 0

    stdout.write(f"{'METHODS':<16} {'PATH':<40} SUMMARY\n")
    stdout.write("-" * 92 + "\n")
    for route in routes:
        methods = ", ".join(route.get("methods", [])) or "ANY"
        summary = str(route.get("summary") or route.get("description") or "").split("\n", 1)[0]
        stdout.write(f"{methods:<16} {route.get('path', ''):<40} {summary[:80]}\n")
    stdout.write(
        "\nTip: run `workflow api routes --json` or `workflow routes --json` for machine-readable discovery.\n"
    )
    return 0


def _api_scoped_routes_command(
    scope_name: str,
    path_prefix: str,
    args: list[str],
    *,
    stdout: TextIO,
) -> int:
    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            f"usage: workflow api {scope_name} [--search TEXT] [--method METHOD] [--tag TAG] [--path-prefix PREFIX] [--visibility public|internal|all] [--json]\n"
            "\n"
            f"Scoped route discovery for {path_prefix} and its children.\n"
            "\n"
            "Examples:\n"
            f"  workflow api {scope_name}\n"
            f"  workflow api {scope_name} --search create\n"
            f"  workflow api {scope_name} --json\n"
            f"  workflow api routes --path-prefix {path_prefix} --json\n"
            "\n"
            "Tip: this is a shortcut for the live HTTP route catalog, not a separate API.\n"
        )
        return 0

    forwarded = ["--path-prefix", path_prefix, *args]
    if "--path-prefix" in args:
        forwarded = list(args)
    return _api_routes_command(forwarded, stdout=stdout)


def _render_route_facets(rows: object, *, field_name: str, limit: int = 5) -> str:
    if not isinstance(rows, list):
        return ""
    parts: list[str] = []
    for row in rows[:limit]:
        if not isinstance(row, dict):
            continue
        value = str(row.get(field_name) or "").strip()
        count = row.get("count")
        if not value or not isinstance(count, int):
            continue
        parts.append(f"{value}={count}")
    return ", ".join(parts)


def _circuits_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow circuits [list|history|open|close|reset]`."""

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow circuits [list [provider_slug]]\n"
            "       workflow circuits history [provider_slug]\n"
            "       workflow circuits open <provider_slug> [--effective-to <iso8601>] [--reason <code>] [--rationale <text>] [--decided-by <principal>] [--decision-source <source>]\n"
            "       workflow circuits close <provider_slug> [--effective-to <iso8601>] [--reason <code>] [--rationale <text>] [--decided-by <principal>] [--decision-source <source>]\n"
            "       workflow circuits reset <provider_slug> [--reason <code>] [--rationale <text>] [--decided-by <principal>] [--decision-source <source>]\n"
            "\n"
            "Show effective circuit state or apply a durable manual override through operator-control authority.\n"
        )
        return 0

    action = "list"
    tail = list(args)
    if tail and tail[0] in {"list", "history", "open", "close", "reset"}:
        action = tail.pop(0)

    params: dict[str, object] = {"action": action}
    if action in {"list", "history"}:
        if tail:
            params["provider_slug"] = tail.pop(0)
        if tail:
            stdout.write(f"error: unexpected arguments for circuits {action}: {' '.join(tail)}\n")
            return 2
    else:
        if not tail:
            stdout.write(f"error: workflow circuits {action} requires <provider_slug>\n")
            return 2
        params["provider_slug"] = tail.pop(0)
        index = 0
        while index < len(tail):
            flag = tail[index]
            if index + 1 >= len(tail):
                stdout.write(f"error: missing value for {flag}\n")
                return 2
            value = tail[index + 1]
            if flag == "--effective-to":
                params["effective_to"] = value
            elif flag == "--reason":
                params["reason_code"] = value
            elif flag == "--rationale":
                params["rationale"] = value
            elif flag == "--decided-by":
                params["decided_by"] = value
            elif flag == "--decision-source":
                params["decision_source"] = value
            else:
                stdout.write(f"error: unknown flag for workflow circuits {action}: {flag}\n")
                return 2
            index += 2

    exit_code, payload = run_cli_tool("praxis_circuits", params)
    print_json(stdout, payload)
    return exit_code


def _slots_command(*, stdout: TextIO) -> int:
    """Handle `workflow slots` -- show current global provider concurrency slot usage."""

    import json as _json

    from runtime.load_balancer import get_load_balancer

    balancer = get_load_balancer()
    status = balancer.slot_status()

    if not status:
        stdout.write(
            "provider concurrency control is not active "
            "(WORKFLOW_DATABASE_URL not set or DB unavailable)\n"
        )
        return 0

    rows = []
    for slug, limit in sorted(status.items()):
        rows.append(
            {
                "provider": slug,
                "max_concurrent": limit.max_concurrent,
                "active_slots": round(limit.current_active, 2),
                "available": round(limit.available, 2),
                "cost_weight_default": limit.cost_weight,
            }
        )

    stdout.write(_json.dumps(rows, indent=2) + "\n")
    return 0


def _params_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow params [adapt|set|reset]`."""

    import json as _json

    from runtime.adaptive_params import get_adaptive_params

    store = get_adaptive_params()

    if not args or args[0] in {"-h", "--help"}:
        if not args:
            stdout.write(_json.dumps(store.all_params_detail(), indent=2) + "\n")
            return 0
        stdout.write(
            "usage: workflow params            show all adaptive parameters\n"
            "       workflow params adapt       run one adaptation cycle from receipts\n"
            "       workflow params set <name> <value>   manual override\n"
            "       workflow params reset       reset all to initial defaults\n"
        )
        return 2

    sub = args[0]

    if sub == "adapt":
        result = store.adapt_from_receipts()
        stdout.write(_json.dumps(result, indent=2) + "\n")
        return 0

    if sub == "set":
        if len(args) < 3:
            stdout.write("usage: workflow params set <name> <value>\n")
            return 2
        name = args[1]
        try:
            value = float(args[2])
        except ValueError:
            stdout.write(f"error: value must be numeric, got: {args[2]}\n")
            return 2
        try:
            clamped = store.set_param(name, value, reason="cli_manual")
        except KeyError as exc:
            stdout.write(f"error: {exc}\n")
            return 1
        stdout.write(_json.dumps({"name": name, "set_to": clamped}, indent=2) + "\n")
        return 0

    if sub == "reset":
        store.reset()
        stdout.write(_json.dumps(store.all_params(), indent=2) + "\n")
        return 0

    stdout.write(f"unknown params subcommand: {sub}\n")
    return 2


def _notifications_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow notifications [tail|drain]`."""

    import json as _json

    show_tail = False
    drain_live = False
    tail_count = 10
    if args:
        if args[0] in {"-h", "--help"}:
            stdout.write(
                "usage: workflow notifications            show all persisted notifications\n"
                "       workflow notifications tail [N]   show last N persisted notifications (default: 10)\n"
                "       workflow notifications drain      drain pending live notifications\n"
            )
            return 2
        if args[0] == "tail":
            show_tail = True
            if len(args) > 1:
                try:
                    tail_count = int(args[1])
                except ValueError:
                    stdout.write(f"error: tail count must be numeric, got: {args[1]}\n")
                    return 2
        elif args[0] == "drain":
            drain_live = True

    if drain_live:
        payload = _workflow_tool({"action": "notifications"})
        if payload.get("error"):
            print_json(stdout, payload)
            return 1
        notifications = str(payload.get("notifications") or "").rstrip()
        stdout.write((notifications or "No pending workflow notifications.") + "\n")
        return 0

    from surfaces.cli._db import cli_sync_conn

    conn = cli_sync_conn()
    from runtime.workflow_notifications import WorkflowNotificationConsumer

    consumer = WorkflowNotificationConsumer(conn)
    if show_tail:
        rows = [
            notification.to_dict() | {
                "id": notification.id,
                "run_id": notification.run_id,
            }
            for notification in (
                []
                if tail_count <= 0
                else consumer.recent(limit=tail_count)
            )
        ]
    else:
        rows = [
            notification.to_dict() | {
                "id": notification.id,
                "run_id": notification.run_id,
            }
            for notification in consumer.recent(limit=None)
        ]

    if not rows:
        stdout.write("no notifications found\n")
        return 0

    for notification in rows:
        stdout.write(_json.dumps(dict(notification), indent=2, default=str) + "\n")
        stdout.write("---\n")

    return 0


def _config_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle ``workflow config [set <key> <value>]``."""

    import json as _json

    from registry.config_registry import get_config

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow config             show all config entries\n"
            "       workflow config set <k> <v>  update one value\n"
        )
        return 2

    cfg = get_config()

    if args and args[0] == "seed":
        stdout.write(
            "config seed is no longer supported; platform_config authority must be present in Postgres\n"
        )
        return 1

    if args and args[0] == "set":
        if len(args) < 3:
            stdout.write("usage: workflow config set <key> <value>\n")
            return 2
        key, raw_value = args[1], args[2]
        value: float | int | str
        try:
            value = int(raw_value)
        except ValueError:
            try:
                value = float(raw_value)
            except ValueError:
                value = raw_value

        existing = cfg.all_entries().get(key)
        if existing:
            cat, desc = existing.category, existing.description
        else:
            cat, desc = "general", ""
        try:
            cfg.set(key, value, category=cat, description=desc)
            stdout.write(f"config: {key} = {value}\n")
            return 0
        except Exception as exc:
            stdout.write(f"config set failed: {exc}\n")
            return 1

    entries = cfg.all_entries()
    if not entries:
        stdout.write("no config entries found\n")
        return 0

    by_category: dict[str, list[dict[str, object]]] = {}
    for entry in sorted(entries.values(), key=lambda entry: (entry.category, entry.key)):
        by_category.setdefault(entry.category, []).append(
            {
                "key": entry.key,
                "value": entry.value,
                "description": entry.description,
            }
        )
    stdout.write(_json.dumps(by_category, indent=2) + "\n")
    return 0


def _dashboard_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow dashboard [--json]` — print the backend dashboard snapshot."""

    if args and args[0] in {"-h", "--help"}:
        stdout.write("usage: workflow dashboard [--json]\n")
        return 2

    data = _backend_dashboard_payload()
    if "--json" in args:
        print_json(stdout, data)
    else:
        stdout.write(_render_backend_dashboard(data) + "\n")
    return 0


def _backend_dashboard_payload() -> dict[str, Any]:
    from types import SimpleNamespace

    from surfaces.api.handlers import workflow_query as workflow_query_mod
    from surfaces.mcp.subsystems import _subs

    subsystems = SimpleNamespace(
        get_pg_conn=_subs.get_pg_conn,
        get_receipt_ingester=_subs.get_receipt_ingester,
    )
    return workflow_query_mod._build_dashboard_payload(subsystems)


def _render_backend_dashboard(payload: dict[str, Any]) -> str:
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return json.dumps(payload, indent=2, default=str)

    workflow_counts = summary.get("workflow_counts")
    queue = summary.get("queue")
    sections = payload.get("sections")
    leaderboard = payload.get("leaderboard")
    recent_runs = payload.get("recent_runs")

    lines: list[str] = []
    health = summary.get("health")
    health_label = str(health.get("label") if isinstance(health, dict) else health or "unknown")
    lines.append("dashboard_summary:")
    lines.append(f"  health={health_label}")

    if isinstance(workflow_counts, dict):
        lines.append(
            "  workflows="
            f"total={int(workflow_counts.get('total') or 0)} "
            f"live={int(workflow_counts.get('live') or 0)} "
            f"saved={int(workflow_counts.get('saved') or 0)} "
            f"draft={int(workflow_counts.get('draft') or 0)}"
        )

    lines.append(
        "  runs_24h="
        f"{int(summary.get('runs_24h') or 0)} "
        f"active_runs={int(summary.get('active_runs') or 0)} "
        f"pass_rate_24h={float(summary.get('pass_rate_24h') or 0.0) * 100:.1f} "
        f"total_cost_24h={float(summary.get('total_cost_24h') or 0.0):.4f}"
    )

    lines.append(
        "  top_agent="
        f"{str(summary.get('top_agent') or 'unknown')} "
        f"models_online={int(summary.get('models_online') or 0)}"
    )

    if isinstance(queue, dict):
        lines.append(
            "queue:"
            f" depth={int(queue.get('depth') or 0)}"
            f" status={str(queue.get('status') or 'unknown')}"
            f" utilization_pct={float(queue.get('utilization_pct') or 0.0):.1f}"
            f" pending={int(queue.get('pending') or 0)}"
            f" ready={int(queue.get('ready') or 0)}"
            f" claimed={int(queue.get('claimed') or 0)}"
            f" running={int(queue.get('running') or 0)}"
        )
        queue_error = str(queue.get("error") or "").strip()
        if queue_error:
            lines.append(f"  error={queue_error}")

    if isinstance(sections, list) and sections:
        section_bits: list[str] = []
        for section in sections:
            if not isinstance(section, dict):
                continue
            key = str(section.get("key") or "").strip()
            count = int(section.get("count") or 0)
            if key:
                section_bits.append(f"{key}={count}")
        if section_bits:
            lines.append(f"sections: {' '.join(section_bits)}")

    if isinstance(leaderboard, list) and leaderboard:
        lines.append("leaderboard_top:")
        for row in leaderboard[:5]:
            if not isinstance(row, dict):
                continue
            provider_slug = str(row.get("provider_slug") or "").strip()
            model_slug = str(row.get("model_slug") or "").strip()
            if not (provider_slug or model_slug):
                continue
            agent_slug = "/".join(part for part in (provider_slug, model_slug) if part)
            lines.append(
                f"  {agent_slug}"
                f" pass_rate_pct={float(row.get('pass_rate') or 0.0) * 100:.1f}"
                f" total_workflows={int(row.get('total_workflows') or 0)}"
            )

    if isinstance(recent_runs, list) and recent_runs:
        lines.append("recent_runs:")
        for row in recent_runs[:5]:
            if not isinstance(row, dict):
                continue
            run_id = str(row.get("run_id") or "").strip()
            status = str(row.get("status") or "").strip()
            completed_jobs = int(row.get("completed_jobs") or 0)
            total_jobs = int(row.get("total_jobs") or 0)
            total_cost = float(row.get("total_cost") or 0.0)
            if run_id:
                lines.append(
                    f"  {run_id} {status} jobs={completed_jobs}/{total_jobs} cost_usd={total_cost:.4f}"
                )

    return "\n".join(lines) if lines else json.dumps(payload, indent=2, default=str)


def _cache_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow cache stats` and `workflow cache clear`.

    Subcommands:
      stats — print cache statistics as JSON
      clear — clear all cache entries, or `--older-than HOURS` for selective
    """

    import json as _json

    from runtime.result_cache import get_result_cache

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow cache stats\n"
            "       workflow cache clear [--older-than HOURS]\n"
        )
        return 2

    subcommand = args[0]
    cache = get_result_cache()

    if subcommand == "stats":
        stats = cache.stats()
        stdout.write(_json.dumps(stats, indent=2) + "\n")
        return 0

    if subcommand == "clear":
        older_than_hours = None
        i = 1
        while i < len(args):
            if args[i] == "--older-than" and i + 1 < len(args):
                try:
                    older_than_hours = float(args[i + 1])
                except ValueError:
                    stdout.write(
                        f"error: --older-than value must be a number, got: {args[i + 1]}\n"
                    )
                    return 2
                i += 2
            else:
                stdout.write(f"unknown argument: {args[i]}\n")
                return 2

        deleted = cache.clear(older_than_hours=older_than_hours)
        result = {
            "status": "cleared",
            "entries_deleted": deleted,
        }
        if older_than_hours is not None:
            result["older_than_hours"] = older_than_hours
        stdout.write(_json.dumps(result, indent=2) + "\n")
        return 0

    stdout.write(f"unknown cache subcommand: {subcommand}\n")
    return 2


def _capabilities_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle ``workflow capabilities [accuracy|reclassify] [--json]``.

    Subcommands
    -----------
    (default)
        Show the model x capability matrix: attempts, successes, avg quality.

    accuracy
        Show inference accuracy per capability: what % of inferences were
        confirmed by output quality signals.

    reclassify
        Show suggested reclassifications: runs where the inferred
        capability had low quality but another capability scored high.
    """

    import json as _json

    from runtime.capability_feedback import get_capability_tracker
    from runtime.capability_router import TaskCapability

    if args and args[0] in {"-h", "--help"}:
        caps = ", ".join(TaskCapability.all())
        stdout.write(
            "usage: workflow capabilities [accuracy|reclassify] [--json]\n"
            "\n"
            "  (default)    show model x capability quality matrix\n"
            "  accuracy     show per-capability inference accuracy\n"
            "  reclassify   show suggested capability reclassifications\n"
            "\n"
            f"  known capabilities: {caps}\n"
        )
        return 2

    subcommand = args[0] if args and args[0] not in {"--json"} else None
    as_json = "--json" in args

    tracker = get_capability_tracker()

    if subcommand == "accuracy":
        rows = [tracker.capability_accuracy(capability) for capability in TaskCapability.all()]
        if as_json:
            stdout.write(_json.dumps(rows, indent=2) + "\n")
            return 0

        header = (
            f"{'capability':<20} {'runs':>10} {'matched':>8} "
            f"{'accuracy':>9} {'avg_quality':>12}"
        )
        sep = "-" * len(header)
        stdout.write(sep + "\n")
        stdout.write(header + "\n")
        stdout.write(sep + "\n")
        for row in rows:
            stdout.write(
                f"{row['capability']:<20} {row['total_workflows']:>10} "
                f"{row['quality_matched']:>8} "
                f"{row['accuracy_rate'] * 100:>8.1f}% "
                f"{row['avg_quality']:>12.4f}\n"
            )
        stdout.write(sep + "\n")
        return 0

    if subcommand == "reclassify":
        suggestions = tracker.suggest_capability_reclassification()
        if as_json:
            stdout.write(_json.dumps(suggestions, indent=2) + "\n")
            return 0

        if not suggestions:
            stdout.write("no reclassification candidates found\n")
            return 0

        stdout.write(f"Capability reclassification candidates ({len(suggestions)} found):\n\n")
        for suggestion in suggestions:
            model = f"{suggestion['provider_slug']}/{suggestion['model_slug']}"
            inferred_q = ", ".join(
                f"{capability}={quality:.2f}"
                for capability, quality in suggestion["inferred_quality"].items()
            )
            suggested_q = ", ".join(
                f"{capability}={quality:.2f}"
                for capability, quality in suggestion["suggested_quality"].items()
            )
            stdout.write(
                f"  run_id: {suggestion['run_id']}\n"
                f"  model:  {model}\n"
                f"  inferred:  {', '.join(suggestion['inferred_capabilities'])} (quality: {inferred_q})\n"
                f"  suggested: {', '.join(suggestion['suggested_capabilities'])} (quality: {suggested_q})\n"
                f"  recorded:  {suggestion['recorded_at']}\n\n"
            )
        return 0

    matrix = tracker.model_capability_matrix()
    if as_json:
        stdout.write(_json.dumps(matrix, indent=2) + "\n")
        return 0

    if not matrix:
        stdout.write(
            "no capability outcome data found\n"
            "(outcomes are recorded automatically after each run)\n"
        )
        return 0

    col_model = 32
    header = (
        f"{'provider/model':<{col_model}} {'capability':<18} "
        f"{'attempts':>8} {'successes':>9} {'quality_ok':>10} {'avg_quality':>12}"
    )
    sep = "-" * len(header)
    stdout.write(sep + "\n")
    stdout.write(header + "\n")
    stdout.write(sep + "\n")
    for model_key in sorted(matrix):
        cap_data = matrix[model_key]
        for capability in sorted(cap_data):
            data = cap_data[capability]
            stdout.write(
                f"{model_key:<{col_model}} {capability:<18} "
                f"{data['attempts']:>8} {data['successes']:>9} "
                f"{data['quality_matched']:>10} {data['avg_quality']:>12.4f}\n"
            )
    stdout.write(sep + "\n")
    return 0


def _events_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow events` command for event log queries.

    Subcommands:
      (no subcommand)      - show recent 20 events
      --run <run_id>       - show all events for a specific run
      --type <event_type>  - filter by event type
      --limit <count>      - change limit (default 50)
    """

    import json as _json

    from runtime.event_log import read_since, read_all_since
    from storage.dev_postgres import get_sync_connection

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow events [--run <run_id>] [--type <type>] [--limit <count>]\n"
            "\n"
            "Show recent workflow events from the event log.\n"
            "\n"
            "Examples:\n"
            "  workflow events                           - recent 20 events\n"
            "  workflow events --run abc123              - all events for run abc123\n"
            "  workflow events --type workflow.failed    - filter by type\n"
            "  workflow events --limit 100               - get up to 100 recent events\n"
        )
        return 0

    run_id = None
    event_type = None
    limit = 50
    i = 0

    while i < len(args):
        if args[i] == "--run" and i + 1 < len(args):
            run_id = args[i + 1]
            i += 2
        elif args[i] == "--type" and i + 1 < len(args):
            event_type = args[i + 1]
            i += 2
        elif args[i] == "--limit" and i + 1 < len(args):
            try:
                limit = int(args[i + 1])
            except ValueError:
                stdout.write(f"error: --limit must be a number, got: {args[i + 1]}\n")
                return 2
            i += 2
        else:
            stdout.write(f"unknown argument: {args[i]}\n")
            return 2

    try:
        conn = get_sync_connection()
        if run_id:
            events = read_since(conn, channel="job_lifecycle", entity_id=run_id, limit=limit)
            result = {
                "kind": "event_timeline",
                "run_id": run_id,
                "event_count": len(events),
                "events": [e.to_dict() for e in events],
            }
        else:
            if event_type:
                events = read_since(conn, channel=event_type, limit=limit)
            else:
                events = read_all_since(conn, limit=limit)
            result = {
                "kind": "event_list",
                "event_type_filter": event_type,
                "limit": limit,
                "event_count": len(events),
                "events": [e.to_dict() for e in events],
            }

        stdout.write(_json.dumps(result, indent=2) + "\n")
        return 0
    except Exception as exc:
        stdout.write(f"error: failed to query events: {exc}\n")
        return 1


def _health_map_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow health-map [--json] [cycles|complexity]`.

    Analyzes module health across the codebase. Shows health scores based on
    complexity, interface width, circular imports, coupling, and file size.
    """

    import json as _json
    from pathlib import Path

    from runtime.health_map import HealthMapper, format_health_map, format_health_map_json

    workflow_root = str(_workspace_repo_root())

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow health-map [--json] [cycles|complexity] [--limit N]\n"
            "\n"
            "  Analyze module health across the codebase.\n"
            "  Subcommands:\n"
            "    (default)   - show top 20 unhealthiest modules\n"
            "    cycles      - show modules with circular imports\n"
            "    complexity  - show modules with complex functions\n"
            "  Options:\n"
            "    --json      - output as JSON\n"
            "    --limit N   - limit output to N modules (default: 20)\n"
        )
        return 0

    as_json = False
    filter_mode = None
    limit = 20
    i = 0

    while i < len(args):
        if args[i] == "--json":
            as_json = True
            i += 1
        elif args[i] == "--limit" and i + 1 < len(args):
            try:
                limit = int(args[i + 1])
            except ValueError:
                stdout.write(f"invalid limit: {args[i + 1]}\n")
                return 2
            i += 2
        elif args[i] in {"cycles", "complexity"}:
            filter_mode = args[i]
            i += 1
        else:
            stdout.write(f"unknown argument: {args[i]}\n")
            return 2

    mapper = HealthMapper()
    modules = mapper.analyze_directory(workflow_root)

    cycles = mapper.detect_circular_imports(workflow_root)
    cycle_modules = set()
    for cycle in cycles:
        for module_name in cycle:
            cycle_modules.add(module_name)

    modules_with_cycles = [
        module.module_path for module in modules if Path(module.module_path).stem in cycle_modules
    ]
    modules = [
        (
            module
            if module.module_path not in modules_with_cycles
            else module.__class__(
                module_path=module.module_path,
                health_score=module.health_score + 15,
                function_count=module.function_count,
                line_count=module.line_count,
                complex_functions=module.complex_functions,
                very_complex_functions=module.very_complex_functions,
                wide_functions=module.wide_functions,
                import_count=module.import_count,
                has_circular_import=True,
            )
        )
        for module in modules
    ]

    if as_json:
        health_json = format_health_map_json(modules)

        if filter_mode == "cycles":
            health_json["modules"] = [
                module for module in health_json["modules"] if module["has_circular_import"]
            ]
        elif filter_mode == "complexity":
            health_json["modules"] = [
                module
                for module in health_json["modules"]
                if module["complex_functions"] > 0 or module["very_complex_functions"] > 0
            ]

        stdout.write(_json.dumps(health_json, indent=2) + "\n")
    else:
        filter_cycles = filter_mode == "cycles"
        filter_complex = filter_mode == "complexity"

        output = format_health_map(
            modules,
            limit=limit,
            filter_cycles=filter_cycles,
            filter_complex=filter_complex,
        )
        stdout.write(output + "\n")

        if cycles and filter_mode != "cycles":
            stdout.write("\n" + "=" * 120 + "\n")
            stdout.write(f"Circular Imports Detected ({len(cycles)} cycle(s)):\n")
            for index, cycle in enumerate(cycles, 1):
                stdout.write(f"  Cycle {index}: {' -> '.join(cycle)} -> {cycle[0]}\n")

    return 0


def _metrics_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow metrics [subcommand] [--json] [--days N]`."""

    import json as _json

    from runtime.observability import get_workflow_metrics_view

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            """usage: workflow metrics [subcommand] [--json] [--days N]

  (default)  show pass rate + cost + latency + observability summary
  heatmap    show failure code x provider matrix
  volume     show hourly workflow volume
  --json     output as JSON (works with any subcommand)
  --days N   look back N days (default: 7)
"""
        )
        return 2

    subcommand = None
    as_json = False
    days = 7

    i = 0
    while i < len(args):
        arg = args[i]
        if arg in {"--json"}:
            as_json = True
        elif arg == "--days":
            i += 1
            if i < len(args):
                try:
                    days = int(args[i])
                except ValueError:
                    stdout.write(f"error: --days must be an integer, got {args[i]!r}\n")
                    return 1
        elif arg in {"heatmap", "volume"}:
            subcommand = arg
        elif arg in {"-h", "--help"}:
            stdout.write("see above\n")
            return 0
        i += 1

    view = get_workflow_metrics_view()

    if subcommand is None:
        pass_rates = view.pass_rate_by_model(days=days)
        costs = view.cost_by_agent(days=days)
        latencies = view.latency_percentiles(days=days)
        efficiency = view.efficiency_summary(days=days)
        failure_breakdown = view.failure_category_breakdown(days=days)
        hourly_volume = view.hourly_workflow_volume(days=days)
        capability_distribution = view.capability_distribution(days=days)

        summary = {
            "pass_rate_by_model": pass_rates,
            "cost_by_agent": costs,
            "latency_percentiles": latencies,
            "efficiency_summary": efficiency,
            "failure_category_breakdown": failure_breakdown,
            "hourly_workflow_volume": hourly_volume,
            "capability_distribution": capability_distribution,
        }

        if as_json:
            stdout.write(_json.dumps(summary, indent=2) + "\n")
            return 0

        stdout.write(f"metrics_summary: window_days={days}\n")

        if pass_rates:
            stdout.write("pass_rate_by_model:\n")
            for row in pass_rates:
                stdout.write(
                    f"  provider={row['provider_slug']} model={str(row['model_slug'] or 'unknown')} "
                    f"total_workflows={row['total_workflows']} pass_rate_pct={row['pass_rate']:.1f}\n"
                )

        if costs:
            stdout.write("cost_by_agent:\n")
            for row in costs:
                stdout.write(
                    f"  provider={row['provider_slug']} total_cost_usd={row['total_cost_usd']:.4f} "
                    f"num_workflows={row['num_workflows']} avg_cost_per_workflow_usd={row['avg_cost_per_workflow']:.6f}\n"
                )

        stdout.write(
            "latency_percentiles: "
            f"p50_ms={latencies.get('p50', 0)} "
            f"p95_ms={latencies.get('p95', 0)} "
            f"p99_ms={latencies.get('p99', 0)}\n"
        )

        stdout.write(
            "observability_digest: "
            f"first_pass_success_rate_pct={efficiency.get('first_pass_success_rate', 0.0) * 100:.1f} "
            f"retry_success_rate_pct={efficiency.get('retry_success_rate', 0.0) * 100:.1f} "
            f"cost_per_success_usd={efficiency.get('cost_per_success_usd', 0.0):.6f} "
            f"tokens_per_success={efficiency.get('tokens_per_success', 0.0):.2f} "
            f"avg_latency_ms={efficiency.get('avg_latency_ms', 0.0):.2f} "
            f"avg_tool_uses={efficiency.get('avg_tool_uses', 0.0):.2f} "
            f"window_total_workflows={efficiency.get('total_workflows', 0)}\n"
        )
        stdout.write("failure_mix:")
        if failure_breakdown:
            parts = [
                f"{row.get('failure_category', 'unknown')}/{row.get('failure_zone', 'unknown')} "
                f"{row.get('count', 0)} ({row.get('pct', 0)}%)"
                for row in failure_breakdown[:3]
            ]
            stdout.write(" " + "; ".join(parts) + "\n")
        else:
            stdout.write("none\n")

        return 0

    if subcommand == "heatmap":
        heatmap = view.failure_heatmap(days=days)

        if as_json:
            stdout.write(_json.dumps(heatmap, indent=2) + "\n")
            return 0

        stdout.write(f"\nFailure Heatmap (last {days} days):\n")
        if not heatmap:
            stdout.write("  (no failures)\n\n")
            return 0

        stdout.write(f"  {'Failure Code':<25} {'Provider':<15} {'Count':>8}\n")
        stdout.write("  " + "-" * 50 + "\n")
        for row in heatmap:
            stdout.write(
                f"  {row['failure_code']:<25} {row['provider_slug']:<15} {row['count']:>8}\n"
            )
        stdout.write("\n")
        return 0

    if subcommand == "volume":
        volume = view.hourly_workflow_volume(days=days)

        if as_json:
            stdout.write(_json.dumps(volume, indent=2) + "\n")
            return 0

        stdout.write(f"\nHourly Workflow Volume (last {days} days):\n")
        if not volume:
            stdout.write("  (no data)\n\n")
            return 0

        stdout.write(f"  {'Hour':<30} {'Count':>8}\n")
        stdout.write("  " + "-" * 40 + "\n")
        for row in volume:
            hour_str = row["hour"] or "unknown"
            stdout.write(f"  {hour_str:<30} {row['count']:>8}\n")
        stdout.write("\n")
        return 0

    return 0


def _api_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow api [--host HOST] [--port PORT]`.

    Starts the Praxis REST API server after reading the declared runtime
    dependency contract from ``requirements.runtime.txt``.

    Options:
      routes         show the live route catalog without starting the server
      --host HOST   bind address (default: 127.0.0.1; use 0.0.0.0 for LAN/container)
      --port PORT   TCP port (default: 8420)
    """

    if args and args[0] in {"-h", "--help"}:
        stdout.write(_api_help_text())
        return 0

    if args and args[0] == "help":
        if len(args) == 1:
            stdout.write(_api_help_text())
            return 0

        help_topic = args[1]
        tail = args[2:]
        if tail:
            stdout.write(f"error: unknown argument: {' '.join(tail)}\n")
            return 2
        if help_topic == "routes":
            return _api_routes_command(["--help"], stdout=stdout)
        if help_topic == "integrations":
            return _api_scoped_routes_command("integrations", "/api/integrations", ["--help"], stdout=stdout)
        if help_topic == "data-dictionary":
            return _api_scoped_routes_command("data-dictionary", "/api/data-dictionary", ["--help"], stdout=stdout)
        stdout.write(f"error: unknown api help topic: {help_topic}\n")
        stdout.write("try: workflow api help routes | workflow api help integrations | workflow api help data-dictionary\n")
        return 2

    if args and args[0] == "routes":
        return _api_routes_command(args[1:], stdout=stdout)
    if args and args[0] == "integrations":
        return _api_scoped_routes_command("integrations", "/api/integrations", args[1:], stdout=stdout)
    if args and args[0] == "data-dictionary":
        return _api_scoped_routes_command("data-dictionary", "/api/data-dictionary", args[1:], stdout=stdout)

    host = os.environ.get("PRAXIS_API_HOST", "127.0.0.1")
    port = int(os.environ.get("PRAXIS_API_PORT", "8420"))

    i = 0
    while i < len(args):
        if args[i] == "--host" and i + 1 < len(args):
            host = args[i + 1]
            i += 2
        elif args[i] == "--port" and i + 1 < len(args):
            try:
                port = int(args[i + 1])
            except ValueError:
                stdout.write(f"error: --port must be an integer, got: {args[i + 1]}\n")
                return 2
            i += 2
        else:
            stdout.write(f"error: unknown argument: {args[i]}\n")
            return 2

    try:
        from surfaces.api.server import start_server
    except ImportError as exc:
        stdout.write(f"error: could not import API server: {exc}\n")
        return 1

    try:
        start_server(host=host, port=port)
    except RuntimeError as exc:
        stdout.write(f"error: {exc}\n")
        return 1
    except KeyboardInterrupt:
        pass

    return 0


def _read_repo_env(repo_root: object) -> dict[str, str]:
    root = repo_root if isinstance(repo_root, os.PathLike) else _workspace_repo_root()
    path = Path(root) / ".env"
    env: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return env

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        env[key] = value.strip().strip('"\'')
    return env


def _resolve_authority_env() -> tuple[dict[str, str], bool]:
    repo_root = _workspace_repo_root()
    authority_env = _read_repo_env(repo_root)
    authority_env.update({k: v for k, v in os.environ.items() if k and isinstance(v, str)})
    configured = bool(authority_env.get("WORKFLOW_DATABASE_URL"))
    return authority_env, configured


def _build_runtime_binding(env: dict[str, str], *, native_instance) -> dict[str, Any]:
    from runtime.primitive_contracts import build_runtime_binding_contract

    try:
        return build_runtime_binding_contract(
            workflow_env=env,
            native_instance=native_instance,
            workflow_env_error=None,
        )
    except Exception:
        return build_runtime_binding_contract(
            workflow_env=env,
            native_instance=None,
            workflow_env_error=None,
        )


def _probe_orient_url(base_url: str) -> tuple[bool, str]:
    orient_url = f"{base_url.rstrip('/')}/orient"
    try:
        request = Request(
            orient_url,
            data=b"{}",
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urlopen(request, timeout=2) as response:
            if response.status != 200:
                return False, f"HTTP {response.status} {response.reason}"
            return True, "ok"
    except URLError as exc:
        return False, f"network error: {exc}"
    except Exception as exc:
        return False, f"error: {exc}"


def _compare_authority_field(label: str, left: str, right: str) -> str:
    return (
        f"{label}: match" if left == right else
        f"{label}: mismatch\n    - cli:  {left}\n    - api:  {right}"
    )


def _global_launcher_resolution(repo_root: Path) -> dict[str, object]:
    praxis_bin = shutil.which("praxis")
    if not praxis_bin:
        return {
            "available": False,
            "status": "unavailable",
            "detail": "praxis binary was not found on PATH",
        }

    try:
        result = subprocess.run(
            [praxis_bin, "launcher", "resolve", "--json"],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
    except subprocess.TimeoutExpired:
        return {
            "available": True,
            "status": "timeout",
            "binary": praxis_bin,
            "detail": "praxis launcher resolve did not return within 3 seconds",
        }
    except OSError as exc:
        return {
            "available": True,
            "status": "error",
            "binary": praxis_bin,
            "detail": f"{type(exc).__name__}: {exc}",
        }

    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        return {
            "available": True,
            "status": "error",
            "binary": praxis_bin,
            "exit_code": result.returncode,
            "detail": detail,
        }

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        return {
            "available": True,
            "status": "error",
            "binary": praxis_bin,
            "detail": f"launcher resolve returned non-JSON output: {exc}",
        }

    resolution = payload.get("resolution") if isinstance(payload, dict) else None
    if not isinstance(resolution, dict):
        return {
            "available": True,
            "status": "error",
            "binary": praxis_bin,
            "detail": "launcher resolve did not include a resolution object",
        }

    resolved_repo_root = str(resolution.get("repo_root") or "")
    matches_repo = resolved_repo_root == str(repo_root)
    return {
        "available": True,
        "status": "ok",
        "binary": praxis_bin,
        "matches_current_repo": matches_repo,
        "repo_root": resolved_repo_root,
        "workdir": str(resolution.get("workdir") or ""),
        "executable_path": str(resolution.get("executable_path") or ""),
        "authority_source": str(resolution.get("authority_source") or ""),
    }


def _authority_command(args: list[str], *, stdout: TextIO) -> int:
    from runtime.instance import native_instance_contract
    from runtime.primitive_contracts import redact_url
    from surfaces._workflow_database import workflow_database_authority_for_repo
    from surfaces.mcp.subsystems import workflow_database_env as mcp_workflow_database_env

    if args and args[0] in {"-h", "--help", "help"}:
        stdout.write(
            "usage: workflow authority [--json] [--check] [--instance]\n"
            "\n"
            "Show resolved authority targets for CLI, MCP, and API surfaces and detect drift.\n"
            "\n"
            "Options:\n"
            "  --json   emit machine-readable payload\n"
            "  --check  probe API /orient endpoint for runtime liveness\n"
            "  --instance  include native-instance metadata (name, profile, and workdir)\n"
        )
        return 0

    unknown = [
        arg
        for arg in args
        if arg not in {"--json", "--check", "--instance"}
    ]
    if unknown:
        stdout.write(f"unknown authority argument: {unknown[0]}\n")
        stdout.write("usage: workflow authority [--json] [--check] [--instance]\n")
        return 2

    as_json = "--json" in args
    check_api = "--check" in args
    show_instance = "--instance" in args
    repo_root = _workspace_repo_root()
    authority_env, configured = _resolve_authority_env()
    launcher = _global_launcher_resolution(repo_root)

    cli_authority: dict[str, object]
    cli_source = "unknown"
    cli_db_url = ""
    cli_error = ""
    try:
        authority = workflow_database_authority_for_repo(repo_root, env=authority_env)
        cli_source = str(authority.source)
        cli_db_url = str(authority.database_url or "")
        cli_authority = {
            "url_redacted": redact_url(cli_db_url),
            "configured": bool(cli_db_url),
            "source": cli_source,
        }
    except Exception as exc:
        cli_error = str(exc)
        cli_authority = {"configured": False, "error": cli_error, "source": "unresolved"}

    mcp_authority_env = mcp_workflow_database_env()
    mcp_db_url = str(mcp_authority_env.get("WORKFLOW_DATABASE_URL") or "")
    mcp_source = str(mcp_authority_env.get("WORKFLOW_DATABASE_AUTHORITY_SOURCE") or "unknown")
    mcp_authority = {
        "url_redacted": redact_url(mcp_db_url),
        "configured": bool(mcp_db_url),
        "source": mcp_source,
    }

    try:
        native_instance = native_instance_contract(env=authority_env)
    except Exception as exc:
        native_instance = {"error": f"{type(exc).__name__}: {exc}"}

    binding_contract = _build_runtime_binding(authority_env, native_instance=native_instance)
    api_authority = binding_contract.get("database", {})
    api_workspace = binding_contract.get("workspace", {})
    if not isinstance(api_workspace, dict):
        api_workspace = {}
    api_http_endpoints = binding_contract.get("http_endpoints", {})
    api_base = str(api_http_endpoints.get("api_base_url") or "unknown")
    api_redacted_db = str(api_authority.get("redacted_url") or "")
    api_runtime_profile = str(api_workspace.get("runtime_profile") or "")

    orient_exit_code, orient_payload = run_cli_tool("praxis_orient", {})
    orient_contract = orient_payload.get("primitive_contracts", {})
    orient_contract = orient_contract if isinstance(orient_contract, dict) else {}
    orient_binding = orient_contract.get("runtime_binding", {})
    orient_binding = orient_binding if isinstance(orient_binding, dict) else {}
    orient_db = str(
        orient_binding.get("database", {}).get("redacted_url")
        if isinstance(orient_binding.get("database"), dict)
        else ""
    )
    orient_api = str(
        orient_binding.get("http_endpoints", {}).get("api_base_url", "")
        if isinstance(orient_binding.get("http_endpoints"), dict)
        else ""
    )

    api_check = None
    if check_api:
        api_check = {
            "api_base_url": api_base,
            "reachable": False,
            "detail": "not_checked",
        }
        if api_base != "unknown":
            ok, detail = _probe_orient_url(api_base)
            api_check.update({"reachable": ok, "detail": detail})
        else:
            api_check["detail"] = "api base URL is unknown"

    checks = {
        "global_launcher_matches_repo": (
            launcher.get("status") != "ok"
            or launcher.get("matches_current_repo") is True
        ),
        "cli_matches_mcp_db": cli_authority.get("url_redacted") == mcp_authority.get("url_redacted"),
        "cli_matches_orient_db": bool(orient_db) and cli_authority.get("url_redacted") == orient_db,
        "api_matches_orient_db": bool(orient_db) and api_redacted_db == orient_db,
        "api_matches_orient_http": bool(orient_api) and api_base == orient_api,
        "cli_instance_profile_known": bool(str(native_instance.get("praxis_runtime_profile") or ""))
        if isinstance(native_instance, dict)
        else False,
        "cli_api_runtime_profile_match": bool(
            isinstance(native_instance, dict)
            and native_instance.get("praxis_runtime_profile")
            and native_instance.get("praxis_runtime_profile") == api_runtime_profile
        ),
    }

    if as_json:
        print_json(
            stdout,
            {
                "kind": "workflow_authority_report",
                "cli": cli_authority,
                "mcp": mcp_authority,
                "global_launcher": launcher,
                "api": {
                    "api_base_url": api_base,
                    "http_endpoints": api_http_endpoints,
                    "database": api_authority,
                    "workspace": api_workspace,
                },
                "native_instance": native_instance,
                "orient": {
                    "exit_code": orient_exit_code,
                    "payload_ok": orient_exit_code == 0,
                    "runtime_binding": orient_binding,
                },
                "alignment_checks": checks,
                "api_instance_profile": api_runtime_profile,
                "api_probe": api_check,
                "db_configured": configured,
            },
        )
        return 0

    status = "ok"
    if cli_error or not configured:
        status = "warn"
    if api_check and not api_check.get("reachable", False):
        status = "warn"
    if not all(checks.values()):
        status = "warn"

    stdout.write(f"workflow authority report: {status}\n")
    if launcher.get("status") == "ok":
        stdout.write(
            f"  Global praxis: {launcher.get('repo_root') or 'unknown'} "
            f"({launcher.get('authority_source') or 'unknown'})\n"
        )
    else:
        stdout.write(
            f"  Global praxis: {launcher.get('status')} "
            f"({launcher.get('detail') or 'no detail'})\n"
        )
    stdout.write(f"  CLI DB:      {cli_authority.get('url_redacted') or 'unconfigured'} ({cli_authority.get('source')})\n")
    stdout.write(f"  MCP DB:      {mcp_authority.get('url_redacted') or 'unconfigured'} ({mcp_authority.get('source')})\n")
    stdout.write(f"  API base:    {api_base}\n")
    stdout.write(f"  API DB:      {api_redacted_db or 'unconfigured'}\n")
    if show_instance and isinstance(native_instance, dict):
        stdout.write("\nInstance context:\n")
        if "error" in native_instance:
            stdout.write(f"  CLI native instance: error ({native_instance.get('error')})\n")
        else:
            stdout.write(
                f"  CLI native instance: "
                f"{native_instance.get('praxis_instance_name') or 'unknown'}\n"
            )
            stdout.write(
                f"  CLI runtime profile: "
                f"{native_instance.get('praxis_runtime_profile') or 'unknown'}\n"
            )
            stdout.write(f"  CLI workdir:         {native_instance.get('workdir') or 'unknown'}\n")
            stdout.write(
                f"  API runtime profile:  {api_runtime_profile or 'unknown'}\n"
            )
    if orient_exit_code == 0:
        stdout.write("  /orient:    ok\n")
    else:
        stdout.write(f"  /orient:    unavailable ({orient_payload.get('error')})\n")

    stdout.write("\nAlignment checks:\n")
    if launcher.get("status") == "ok":
        stdout.write(f"  { _compare_authority_field('  global praxis repo', str(launcher.get('repo_root') or ''), str(repo_root)) }\n")
    stdout.write(f"  { _compare_authority_field('  cli==mcp db', str(cli_authority.get('url_redacted') or ''), str(mcp_authority.get('url_redacted') or '')) }\n")
    stdout.write(f"  { _compare_authority_field('  cli==orient db', str(cli_authority.get('url_redacted') or ''), orient_db) }\n")
    stdout.write(f"  { _compare_authority_field('  api==orient db', api_redacted_db, orient_db) }\n")
    stdout.write(f"  { _compare_authority_field('  api==orient http', api_base, orient_api) }\n")
    if show_instance and isinstance(native_instance, dict):
        stdout.write(
            "  "
            + _compare_authority_field(
                'cli==api runtime profile',
                str(native_instance.get('praxis_runtime_profile') or ''),
                api_runtime_profile,
            )
            + "\n"
        )

    if api_check:
        status_text = "reachable" if api_check["reachable"] else "not reachable"
        stdout.write(f"\nAPI /orient probe: {status_text}\n")
        if api_check.get("detail"):
            stdout.write(f"  {api_check['detail']}\n")

    if not checks.get("cli_matches_mcp_db"):
        stdout.write("\nTip: run `workflow setup doctor` to reconcile runtime environment authority values.\n")
    if not checks.get("global_launcher_matches_repo"):
        stdout.write("\nTip: run `praxis launcher resolve --json` to inspect the global front door target before using bare `praxis workflow ...`.\n")

    return 0


def _supervisor_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle legacy `workflow supervisor {install|uninstall|status|logs|restart}`."""

    import subprocess
    from pathlib import Path

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow supervisor {install|uninstall|status|logs|restart}\n"
            "\n"
            "Legacy compatibility wrapper around ./scripts/praxis for service management.\n"
            "\n"
            "  supervisor install   - Install and load all services\n"
            "  supervisor uninstall - Unload and remove all services\n"
            "  supervisor status    - Show status of all services\n"
            "  supervisor logs      - Tail all service logs\n"
            "  supervisor restart   - Restart all services\n"
        )
        return 2

    subcommand = args[0]
    if subcommand not in {"install", "uninstall", "status", "logs", "restart"}:
        stdout.write(f"error: unknown supervisor subcommand: {subcommand}\n")
        return 2

    workflow_root = _workspace_repo_root()
    repo_root = workflow_root.parents[1]
    launcher_script = repo_root / "scripts" / "praxis"

    if not launcher_script.exists():
        stdout.write(f"error: praxis launcher not found at {launcher_script}\n")
        return 1

    try:
        result = subprocess.run(
            [str(launcher_script), subcommand],
            capture_output=False,
            text=True,
            check=False,
        )
        return result.returncode
    except Exception as exc:
        stdout.write(f"error: failed to run praxis launcher: {exc}\n")
        return 1


# ---------------------------------------------------------------------------
# workflow health — full system preflight check
# ---------------------------------------------------------------------------

def _health_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow health [--json]` — full system health check.

    Runs preflight probes (Postgres, disk, provider transport), operator
    panel snapshot, lane recommendation, dependency truth, and content health.
    """

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow health [--json]\n"
            "\n"
            "  Run full system health check: DB probes, provider transport,\n"
            "  disk space, operator snapshot, and lane recommendation.\n"
        )
        return 2

    as_json = "--json" in args if args else False
    exit_code, payload = run_cli_tool("praxis_health", {})
    if as_json:
        print_json(stdout, payload)
        return exit_code
    render_health_payload(payload, stdout=stdout)
    return exit_code
