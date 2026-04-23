"""Host-local launcher resolution for the global Praxis command.

The launcher owns only bootstrap resolution: given an explicit local seed, find
the checked-out runtime through registry workspace/base-path authority and exec
that checkout's ``scripts/praxis``. Durable domain authority remains elsewhere.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, TextIO
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen


LAUNCHER_CONFIG_ENV = "PRAXIS_LAUNCHER_CONFIG"
DEFAULT_CONFIG_PATH = Path.home() / ".config" / "praxis" / "launcher.json"
SUPPORTED_SCHEMA_VERSION = 1
_ENV_TOKEN_RE = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$")


class LauncherAuthorityError(RuntimeError):
    """Raised when launcher authority cannot be resolved safely."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
        exit_code: int = 1,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.message = message
        self.details = dict(details or {})
        self.exit_code = exit_code

    def to_dict(self) -> dict[str, Any]:
        return {
            "reason_code": self.reason_code,
            "message": self.message,
            "details": self.details,
        }


@dataclass(frozen=True, slots=True)
class LauncherSeedConfig:
    config_path: Path
    workspace_ref: str
    host_ref: str
    api_url: str | None
    database_url: str | None
    environment: Mapping[str, str]
    schema_version: int = SUPPORTED_SCHEMA_VERSION

    def effective_env(self, env: Mapping[str, str] | None = None) -> dict[str, str]:
        merged = dict(os.environ if env is None else env)
        merged.update({key: value for key, value in self.environment.items() if value})
        return merged


@dataclass(frozen=True, slots=True)
class LauncherResolution:
    workspace_ref: str
    host_ref: str
    base_path_ref: str
    base_path: Path
    repo_root: Path
    workdir: Path
    executable_path: Path
    authority_source: str

    def to_dict(self) -> dict[str, str]:
        return {
            "workspace_ref": self.workspace_ref,
            "host_ref": self.host_ref,
            "base_path_ref": self.base_path_ref,
            "base_path": str(self.base_path),
            "repo_root": str(self.repo_root),
            "workdir": str(self.workdir),
            "executable_path": str(self.executable_path),
            "authority_source": self.authority_source,
        }


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LauncherAuthorityError(
            "launcher_config_invalid",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise LauncherAuthorityError(
            "launcher_config_invalid",
            f"{field_name} must be a string when provided",
            details={"field": field_name},
        )
    text = value.strip()
    return text or None


def _row_get(row: Mapping[str, Any], key: str, default: object = None) -> object:
    return row.get(key, default)


def launcher_config_path(
    *,
    explicit_path: str | Path | None = None,
    env: Mapping[str, str] | None = None,
) -> Path:
    if explicit_path is not None:
        return Path(explicit_path).expanduser()
    source = os.environ if env is None else env
    configured = str(source.get(LAUNCHER_CONFIG_ENV) or "").strip()
    if configured:
        return Path(configured).expanduser()
    return DEFAULT_CONFIG_PATH


def read_launcher_seed_config(
    path: str | Path | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> LauncherSeedConfig:
    config_path = launcher_config_path(explicit_path=path, env=env)
    if not config_path.exists():
        raise LauncherAuthorityError(
            "launcher_config_missing",
            "Praxis launcher config is missing; run ./scripts/bootstrap or "
            "praxis launcher configure with an explicit authority seed.",
            details={"config_path": str(config_path)},
        )

    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise LauncherAuthorityError(
            "launcher_config_invalid",
            f"Praxis launcher config is not valid JSON: {exc}",
            details={"config_path": str(config_path)},
        ) from exc

    if not isinstance(payload, dict):
        raise LauncherAuthorityError(
            "launcher_config_invalid",
            "Praxis launcher config must be a JSON object",
            details={"config_path": str(config_path)},
        )

    schema_version = int(payload.get("schema_version") or 0)
    if schema_version != SUPPORTED_SCHEMA_VERSION:
        raise LauncherAuthorityError(
            "launcher_config_unsupported",
            f"unsupported launcher config schema_version {schema_version}",
            details={
                "config_path": str(config_path),
                "supported_schema_version": SUPPORTED_SCHEMA_VERSION,
            },
        )

    authority = payload.get("authority") or {}
    if not isinstance(authority, dict):
        raise LauncherAuthorityError(
            "launcher_config_invalid",
            "launcher authority must be a JSON object",
            details={"config_path": str(config_path), "field": "authority"},
        )

    environment = payload.get("environment") or {}
    if not isinstance(environment, dict):
        raise LauncherAuthorityError(
            "launcher_config_invalid",
            "launcher environment must be a JSON object when provided",
            details={"config_path": str(config_path), "field": "environment"},
        )
    normalized_environment = {
        str(key).strip(): str(value).strip()
        for key, value in environment.items()
        if str(key).strip() and str(value).strip()
    }

    api_url = _optional_text(
        authority.get("api_url", payload.get("api_url")),
        field_name="authority.api_url",
    )
    database_url = _optional_text(
        authority.get("database_url", payload.get("database_url")),
        field_name="authority.database_url",
    )
    if not api_url and not database_url:
        raise LauncherAuthorityError(
            "launcher_authority_seed_missing",
            "launcher config must declare authority.api_url or authority.database_url",
            details={"config_path": str(config_path)},
        )

    return LauncherSeedConfig(
        config_path=config_path,
        workspace_ref=_require_text(payload.get("workspace_ref"), field_name="workspace_ref"),
        host_ref=_require_text(payload.get("host_ref"), field_name="host_ref"),
        api_url=api_url,
        database_url=database_url,
        environment=normalized_environment,
        schema_version=schema_version,
    )


def write_launcher_seed_config(
    path: str | Path,
    *,
    workspace_ref: str,
    host_ref: str,
    api_url: str | None = None,
    database_url: str | None = None,
    environment: Mapping[str, str] | None = None,
) -> Path:
    target = Path(path).expanduser()
    api_url = api_url.strip() if api_url else None
    database_url = database_url.strip() if database_url else None
    if not api_url and not database_url:
        raise LauncherAuthorityError(
            "launcher_authority_seed_missing",
            "launcher config requires api_url or database_url",
            details={"config_path": str(target)},
        )
    payload = {
        "schema_version": SUPPORTED_SCHEMA_VERSION,
        "workspace_ref": _require_text(workspace_ref, field_name="workspace_ref"),
        "host_ref": _require_text(host_ref, field_name="host_ref"),
        "authority": {
            key: value
            for key, value in {
                "api_url": api_url,
                "database_url": database_url,
            }.items()
            if value
        },
        "environment": {
            str(key): str(value)
            for key, value in (environment or {}).items()
            if str(key).strip() and str(value).strip()
        },
    }
    target.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    fd = os.open(
        target,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        stat.S_IRUSR | stat.S_IWUSR,
    )
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.chmod(target, stat.S_IRUSR | stat.S_IWUSR)
    return target


def looks_like_legacy_sql_locator(text: str) -> bool:
    return (
        "registry_workspace_base_path_authority" in text
        and "psql" in text
        and (
            "postgresql://localhost:5432/praxis" in text
            or "registry did not resolve an executable Praxis checkout" in text
        )
    )


def _resolve_base_path(
    raw_value: object,
    *,
    field_name: str,
    env: Mapping[str, str],
) -> Path:
    raw_text = _require_text(raw_value, field_name=field_name)
    match = _ENV_TOKEN_RE.match(raw_text)
    if match:
        env_name = match.group(1)
        env_value = str(env.get(env_name) or "").strip()
        if not env_value:
            raise LauncherAuthorityError(
                "workspace_base_path_unresolved",
                f"{field_name} references ${{{env_name}}}, but {env_name} is not set",
                details={"field": field_name, "env_name": env_name},
            )
        raw_text = env_value

    candidate = Path(raw_text).expanduser()
    if not candidate.is_absolute():
        raise LauncherAuthorityError(
            "workspace_base_path_not_absolute",
            f"{field_name} must resolve to an absolute path",
            details={"field": field_name, "value": raw_text},
        )
    return candidate.resolve(strict=False)


def _assert_inside(child: Path, parent: Path, *, field_name: str) -> Path:
    resolved_child = child.resolve(strict=False)
    resolved_parent = parent.resolve(strict=False)
    try:
        resolved_child.relative_to(resolved_parent)
    except ValueError as exc:
        raise LauncherAuthorityError(
            "workspace_boundary_violation",
            f"{field_name} resolves outside {resolved_parent}",
            details={
                "field": field_name,
                "path": str(resolved_child),
                "boundary": str(resolved_parent),
            },
        ) from exc
    return resolved_child


def _resolve_child_path(
    raw_value: object,
    *,
    field_name: str,
    base: Path,
) -> Path:
    raw_text = str(raw_value if raw_value is not None else ".").strip() or "."
    candidate = Path(raw_text).expanduser()
    if not candidate.is_absolute():
        candidate = base / candidate
    return _assert_inside(candidate, base, field_name=field_name)


def _resolution_from_authority_row(
    row: Mapping[str, Any],
    *,
    authority_source: str,
    env: Mapping[str, str],
) -> LauncherResolution:
    workspace_ref = _require_text(_row_get(row, "workspace_ref"), field_name="workspace_ref")
    host_ref = _require_text(_row_get(row, "host_ref"), field_name="host_ref")
    base_path_ref = _require_text(_row_get(row, "base_path_ref"), field_name="base_path_ref")
    base_path = _resolve_base_path(
        _row_get(row, "base_path"),
        field_name="base_path",
        env=env,
    )
    repo_root = _resolve_child_path(
        _row_get(row, "repo_root_path", "."),
        field_name="repo_root_path",
        base=base_path,
    )
    workdir = _resolve_child_path(
        _row_get(row, "workdir_path", "."),
        field_name="workdir_path",
        base=repo_root,
    )
    executable_path = repo_root / "scripts" / "praxis"
    if not executable_path.is_file() or not os.access(executable_path, os.X_OK):
        raise LauncherAuthorityError(
            "launcher_executable_missing",
            "resolved checkout does not contain executable scripts/praxis",
            details={
                "repo_root": str(repo_root),
                "executable_path": str(executable_path),
            },
        )
    return LauncherResolution(
        workspace_ref=workspace_ref,
        host_ref=host_ref,
        base_path_ref=base_path_ref,
        base_path=base_path,
        repo_root=repo_root,
        workdir=workdir,
        executable_path=executable_path,
        authority_source=authority_source,
    )


def _fetch_api_resolution(
    seed: LauncherSeedConfig,
    *,
    env: Mapping[str, str],
    urlopen_func: Callable[..., Any] = urlopen,
) -> LauncherResolution:
    if not seed.api_url:
        raise LauncherAuthorityError(
            "launcher_api_seed_missing",
            "launcher config has no API authority seed",
        )
    query = urlencode({"workspace_ref": seed.workspace_ref, "host_ref": seed.host_ref})
    endpoint = f"{seed.api_url.rstrip('/')}/api/launcher/resolve?{query}"
    try:
        with urlopen_func(endpoint, timeout=5) as response:
            raw_payload = response.read().decode("utf-8")
    except (OSError, URLError) as exc:
        raise LauncherAuthorityError(
            "launcher_api_unavailable",
            f"launcher API authority is unavailable: {exc}",
            details={"api_url": seed.api_url},
        ) from exc
    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise LauncherAuthorityError(
            "launcher_api_invalid",
            "launcher API authority returned invalid JSON",
            details={"api_url": seed.api_url},
        ) from exc
    if not isinstance(payload, dict):
        raise LauncherAuthorityError(
            "launcher_api_invalid",
            "launcher API authority returned a non-object payload",
            details={"api_url": seed.api_url},
        )
    row = payload.get("resolution", payload)
    if not isinstance(row, dict):
        raise LauncherAuthorityError(
            "launcher_api_invalid",
            "launcher API resolution must be a JSON object",
            details={"api_url": seed.api_url},
        )
    return _resolution_from_authority_row(
        row,
        authority_source=f"api:{seed.api_url.rstrip('/')}",
        env=env,
    )


def _fetch_database_resolution(
    seed: LauncherSeedConfig,
    *,
    env: Mapping[str, str],
    connect_factory: Callable[..., Any] | None = None,
) -> LauncherResolution:
    if not seed.database_url:
        raise LauncherAuthorityError(
            "launcher_database_seed_missing",
            "launcher config has no database authority seed",
        )
    if connect_factory is None:
        try:
            import psycopg2
        except ImportError as exc:
            raise LauncherAuthorityError(
                "launcher_database_driver_missing",
                "psycopg2 is required for launcher database authority resolution",
            ) from exc
        connect_factory = psycopg2.connect

    try:
        conn = connect_factory(seed.database_url, connect_timeout=5)
    except Exception as exc:  # pragma: no cover - driver-specific subclasses vary.
        raise LauncherAuthorityError(
            "launcher_database_unavailable",
            f"launcher database authority is unavailable: {exc}",
        ) from exc

    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT workspace.workspace_ref,
                           base_path.host_ref,
                           workspace.base_path_ref,
                           base_path.base_path,
                           workspace.repo_root_path,
                           workspace.workdir_path
                    FROM registry_workspace_authority workspace
                    JOIN registry_workspace_base_path_authority base_path
                      ON base_path.base_path_ref = workspace.base_path_ref
                     AND base_path.workspace_ref = workspace.workspace_ref
                     AND base_path.is_active = TRUE
                    WHERE workspace.workspace_ref = %s
                      AND base_path.host_ref = %s
                    ORDER BY base_path.priority DESC,
                             base_path.recorded_at DESC
                    LIMIT 2
                    """,
                    (seed.workspace_ref, seed.host_ref),
                )
                columns = [column[0] for column in cursor.description]
                rows = [dict(zip(columns, raw_row, strict=True)) for raw_row in cursor.fetchall()]
    finally:
        close = getattr(conn, "close", None)
        if callable(close):
            close()

    if not rows:
        raise LauncherAuthorityError(
            "launcher_workspace_unresolved",
            "registry workspace/base-path authority did not resolve this workspace and host",
            details={"workspace_ref": seed.workspace_ref, "host_ref": seed.host_ref},
        )
    if len(rows) > 1:
        raise LauncherAuthorityError(
            "launcher_workspace_ambiguous",
            "registry workspace/base-path authority returned multiple active rows",
            details={"workspace_ref": seed.workspace_ref, "host_ref": seed.host_ref},
        )
    return _resolution_from_authority_row(
        rows[0],
        authority_source="database",
        env=env,
    )


def resolve_launcher_workspace(
    seed: LauncherSeedConfig,
    *,
    env: Mapping[str, str] | None = None,
    urlopen_func: Callable[..., Any] = urlopen,
    connect_factory: Callable[..., Any] | None = None,
) -> LauncherResolution:
    effective_env = seed.effective_env(env)
    api_error: LauncherAuthorityError | None = None

    if seed.api_url:
        try:
            return _fetch_api_resolution(seed, env=effective_env, urlopen_func=urlopen_func)
        except LauncherAuthorityError as exc:
            api_error = exc
            if not seed.database_url:
                raise

    if seed.database_url:
        try:
            return _fetch_database_resolution(
                seed,
                env=effective_env,
                connect_factory=connect_factory,
            )
        except LauncherAuthorityError as exc:
            if api_error is not None:
                exc.details["api_fallback_error"] = api_error.to_dict()
            raise

    raise LauncherAuthorityError(
        "launcher_authority_seed_missing",
        "launcher config must declare authority.api_url or authority.database_url",
    )


def resolution_payload(resolution: LauncherResolution) -> dict[str, Any]:
    return {"ok": True, "resolution": resolution.to_dict(), "errors": [], "warnings": []}


def _redact_url(value: str | None) -> str | None:
    if not value:
        return None
    if "@" not in value:
        return value
    prefix, suffix = value.rsplit("@", 1)
    scheme, _, _rest = prefix.partition("://")
    return f"{scheme}://***:***@{suffix}" if scheme else f"***:***@{suffix}"


def _print_json(payload: Mapping[str, Any], stdout: TextIO) -> None:
    stdout.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _launcher_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="praxis launcher")
    parser.add_argument("--config", default=None, help="launcher config path")
    subparsers = parser.add_subparsers(dest="command", required=True)

    configure = subparsers.add_parser("configure")
    configure.add_argument("--workspace-ref", default="praxis")
    configure.add_argument("--host-ref", default="default")
    configure.add_argument("--api-url", default=None)
    configure.add_argument("--database-url", default=None)
    configure.add_argument("--workspace-base-path", default=None)
    configure.add_argument("--json", action="store_true")

    resolve = subparsers.add_parser("resolve")
    resolve.add_argument("--json", action="store_true")

    doctor = subparsers.add_parser("doctor")
    doctor.add_argument("--json", action="store_true")
    return parser


def launcher_cli(
    argv: list[str] | None = None,
    *,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    stdout = sys.stdout if stdout is None else stdout
    stderr = sys.stderr if stderr is None else stderr
    parser = _launcher_parser()
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    config_path = launcher_config_path(explicit_path=args.config)

    try:
        if args.command == "configure":
            environment = {}
            if args.workspace_base_path:
                environment["PRAXIS_WORKSPACE_BASE_PATH"] = args.workspace_base_path
            written = write_launcher_seed_config(
                config_path,
                workspace_ref=args.workspace_ref,
                host_ref=args.host_ref,
                api_url=args.api_url,
                database_url=args.database_url,
                environment=environment,
            )
            payload = {
                "ok": True,
                "config_path": str(written),
                "workspace_ref": args.workspace_ref,
                "host_ref": args.host_ref,
                "authority": {
                    "api_url": args.api_url,
                    "database_url": _redact_url(args.database_url),
                },
                "errors": [],
                "warnings": [],
            }
            if args.json:
                _print_json(payload, stdout)
            else:
                stdout.write(f"launcher config written: {written}\n")
            return 0

        seed = read_launcher_seed_config(config_path)
        resolution = resolve_launcher_workspace(seed)
        if args.command == "resolve":
            if args.json:
                _print_json(resolution_payload(resolution), stdout)
            else:
                for key, value in resolution.to_dict().items():
                    stdout.write(f"{key}: {value}\n")
            return 0

        if args.command == "doctor":
            _print_json(resolution_payload(resolution), stdout)
            return 0
    except LauncherAuthorityError as exc:
        payload = {"ok": False, "errors": [exc.to_dict()], "warnings": []}
        if getattr(args, "json", False):
            _print_json(payload, stdout)
        else:
            stderr.write(f"praxis launcher: {exc.message}\n")
        return exc.exit_code

    parser.error("unreachable launcher command")
    return 2


def delegated_environment(
    seed: LauncherSeedConfig,
    resolution: LauncherResolution,
    *,
    env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    target_env = seed.effective_env(env)
    if seed.database_url:
        target_env.setdefault("WORKFLOW_DATABASE_URL", seed.database_url)
        target_env.setdefault("WORKFLOW_DATABASE_AUTHORITY_SOURCE", "launcher_config")
    target_env.setdefault("PRAXIS_WORKSPACE_BASE_PATH", str(resolution.base_path))
    target_env["PRAXIS_LAUNCHER_RESOLVED_REPO_ROOT"] = str(resolution.repo_root)
    target_env["PRAXIS_LAUNCHER_AUTHORITY_SOURCE"] = resolution.authority_source
    return target_env


def launcher_main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if args and args[0] == "launcher":
        return launcher_cli(args[1:])

    try:
        seed = read_launcher_seed_config()
        resolution = resolve_launcher_workspace(seed)
    except LauncherAuthorityError as exc:
        sys.stderr.write(f"praxis: {exc.message}\n")
        sys.stderr.write("repair: run ./scripts/bootstrap or praxis launcher configure\n")
        return exc.exit_code

    os.execve(
        str(resolution.executable_path),
        [str(resolution.executable_path), *args],
        delegated_environment(seed, resolution),
    )
    return 127


if __name__ == "__main__":
    raise SystemExit(launcher_main())
