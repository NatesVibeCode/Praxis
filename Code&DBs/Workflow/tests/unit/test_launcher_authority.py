from __future__ import annotations

import json
import stat
from pathlib import Path
from typing import Any

import pytest

from runtime.launcher_authority import (
    LauncherAuthorityError,
    LauncherResolution,
    LauncherSeedConfig,
    launcher_main,
    looks_like_legacy_sql_locator,
    read_launcher_seed_config,
    resolve_launcher_workspace,
    write_launcher_seed_config,
)


def _make_checkout(base_path: Path, *, executable: bool = True) -> Path:
    repo_root = base_path / "repo"
    scripts = repo_root / "scripts"
    scripts.mkdir(parents=True)
    (repo_root / "Code&DBs" / "Workflow").mkdir(parents=True)
    config = repo_root / "config"
    config.mkdir()
    (config / "workspace_layout.json").write_text("{}", encoding="utf-8")
    command = scripts / "praxis"
    command.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    if executable:
        command.chmod(0o755)
    return repo_root


def _authority_row(base_path: Path, *, base_path_value: str | None = None) -> dict[str, str]:
    return {
        "workspace_ref": "praxis",
        "host_ref": "default",
        "base_path_ref": "workspace_base.praxis.default",
        "base_path": base_path_value or str(base_path),
        "repo_root_path": "repo",
        "workdir_path": ".",
    }


class _FakeApiResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def __enter__(self) -> "_FakeApiResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


class _FakeCursor:
    description = [
        ("workspace_ref",),
        ("host_ref",),
        ("base_path_ref",),
        ("base_path",),
        ("repo_root_path",),
        ("workdir_path",),
    ]

    def __init__(self, row: dict[str, str]) -> None:
        self._row = row

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, _sql: str, params: tuple[str, str]) -> None:
        assert params == ("praxis", "default")

    def fetchall(self) -> list[tuple[str, str, str, str, str, str]]:
        return [
            (
                self._row["workspace_ref"],
                self._row["host_ref"],
                self._row["base_path_ref"],
                self._row["base_path"],
                self._row["repo_root_path"],
                self._row["workdir_path"],
            )
        ]


class _FakeConnection:
    def __init__(self, row: dict[str, str]) -> None:
        self._row = row
        self.closed = False

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._row)

    def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        """Match SyncPostgresConnection.execute contract."""
        with self.cursor() as cursor:
            # Handle both (sql, (p1, p2)) and (sql, p1, p2) patterns
            params = args[0] if len(args) == 1 and isinstance(args[0], (tuple, list)) else args
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            colnames = [d[0] for d in cursor.description]
            return [dict(zip(colnames, row)) for row in rows]

    def close(self) -> None:
        self.closed = True


def test_fake_connection_implements_execute_contract() -> None:
    row = {
        "workspace_ref": "praxis",
        "host_ref": "default",
        "base_path_ref": "base",
        "base_path": "/tmp/base",
        "repo_root_path": "/tmp/repo",
        "workdir_path": "/tmp/work",
    }
    conn = _FakeConnection(row)
    # The unified workflow status code (and others) now use conn.execute(sql, *args)
    # and expect a list of dict-like objects.
    rows = conn.execute(
        "SELECT * FROM launcher_authority WHERE workspace_ref = $1 AND host_ref = $2",
        "praxis",
        "default",
    )

    assert len(rows) == 1
    assert rows[0]["workspace_ref"] == "praxis"
    assert rows[0]["host_ref"] == "default"
    assert rows[0]["repo_root_path"] == "/tmp/repo"


def test_missing_launcher_config_fails_closed(tmp_path: Path) -> None:
    missing = tmp_path / "launcher.json"

    with pytest.raises(LauncherAuthorityError) as exc_info:
        read_launcher_seed_config(missing)

    assert exc_info.value.reason_code == "launcher_config_missing"
    assert "run ./scripts/bootstrap" in exc_info.value.message


def test_write_launcher_seed_config_uses_0600_permissions(tmp_path: Path) -> None:
    config_path = tmp_path / "launcher.json"

    write_launcher_seed_config(
        config_path,
        workspace_ref="praxis",
        host_ref="default",
        database_url="postgresql://authority.example/praxis",
    )

    assert stat.S_IMODE(config_path.stat().st_mode) == 0o600


def test_api_seed_is_preferred_over_database_seed(tmp_path: Path) -> None:
    _make_checkout(tmp_path)
    seed = LauncherSeedConfig(
        config_path=tmp_path / "launcher.json",
        workspace_ref="praxis",
        host_ref="default",
        api_url="https://authority.example",
        database_url="postgresql://authority.example/praxis",
        environment={},
    )

    def fake_urlopen(url: str, *, timeout: int) -> _FakeApiResponse:
        assert url.startswith("https://authority.example/api/launcher/resolve?")
        assert timeout == 5
        return _FakeApiResponse({"resolution": _authority_row(tmp_path)})

    def forbidden_connect(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("database fallback should not be used when API resolves")

    resolution = resolve_launcher_workspace(
        seed,
        env={},
        urlopen_func=fake_urlopen,
        connect_factory=forbidden_connect,
    )

    assert resolution.authority_source == "api:https://authority.example"
    assert resolution.executable_path == tmp_path / "repo" / "scripts" / "praxis"


def test_database_seed_resolution_uses_python_runtime_not_shell_sql(tmp_path: Path) -> None:
    _make_checkout(tmp_path)
    seed = LauncherSeedConfig(
        config_path=tmp_path / "launcher.json",
        workspace_ref="praxis",
        host_ref="default",
        api_url=None,
        database_url="postgresql://authority.example/praxis",
        environment={},
    )
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def fake_connect(*args: object, **kwargs: object) -> _FakeConnection:
        calls.append((args, kwargs))
        return _FakeConnection(_authority_row(tmp_path))

    resolution = resolve_launcher_workspace(seed, env={}, connect_factory=fake_connect)

    assert resolution.authority_source == "database"
    assert calls == [(("postgresql://authority.example/praxis",), {"connect_timeout": 5})]


def test_legacy_sql_locator_content_is_detected() -> None:
    old_shim = """
    psql "${WORKFLOW_DATABASE_URL:-postgresql://localhost:5432/praxis}" -tAc "
      SELECT base_path FROM registry_workspace_base_path_authority
    "
    echo "registry did not resolve an executable Praxis checkout"
    """

    assert looks_like_legacy_sql_locator(old_shim)


def test_resolved_checkout_must_have_executable_scripts_praxis(tmp_path: Path) -> None:
    _make_checkout(tmp_path, executable=False)
    seed = LauncherSeedConfig(
        config_path=tmp_path / "launcher.json",
        workspace_ref="praxis",
        host_ref="default",
        api_url=None,
        database_url="postgresql://authority.example/praxis",
        environment={},
    )

    with pytest.raises(LauncherAuthorityError) as exc_info:
        resolve_launcher_workspace(
            seed,
            env={},
            connect_factory=lambda *_args, **_kwargs: _FakeConnection(_authority_row(tmp_path)),
        )

    assert exc_info.value.reason_code == "launcher_executable_missing"


def test_unresolved_workspace_base_path_token_fails_closed(tmp_path: Path) -> None:
    _make_checkout(tmp_path)
    seed = LauncherSeedConfig(
        config_path=tmp_path / "launcher.json",
        workspace_ref="praxis",
        host_ref="default",
        api_url=None,
        database_url="postgresql://authority.example/praxis",
        environment={},
    )

    with pytest.raises(LauncherAuthorityError) as exc_info:
        resolve_launcher_workspace(
            seed,
            env={},
            connect_factory=lambda *_args, **_kwargs: _FakeConnection(
                _authority_row(tmp_path, base_path_value="${PRAXIS_WORKSPACE_BASE_PATH}")
            ),
        )

    assert exc_info.value.reason_code == "workspace_base_path_unresolved"


def test_workspace_base_path_token_can_come_from_launcher_seed_environment(tmp_path: Path) -> None:
    _make_checkout(tmp_path)
    seed = LauncherSeedConfig(
        config_path=tmp_path / "launcher.json",
        workspace_ref="praxis",
        host_ref="default",
        api_url=None,
        database_url="postgresql://authority.example/praxis",
        environment={"PRAXIS_WORKSPACE_BASE_PATH": str(tmp_path)},
    )

    resolution = resolve_launcher_workspace(
        seed,
        env={},
        connect_factory=lambda *_args, **_kwargs: _FakeConnection(
            _authority_row(tmp_path, base_path_value="${PRAXIS_WORKSPACE_BASE_PATH}")
        ),
    )

    assert resolution.repo_root == tmp_path / "repo"


def test_global_launcher_fails_fast_inside_different_checkout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    active_checkout = tmp_path / "active"
    resolved_base = tmp_path / "resolved"
    _make_checkout(active_checkout)
    resolved_repo = _make_checkout(resolved_base)

    seed = LauncherSeedConfig(
        config_path=tmp_path / "launcher.json",
        workspace_ref="praxis",
        host_ref="default",
        api_url=None,
        database_url="postgresql://authority.example/praxis",
        environment={},
    )
    resolution = LauncherResolution(
        workspace_ref="praxis",
        host_ref="default",
        base_path_ref="workspace_base.praxis.default",
        base_path=resolved_base,
        repo_root=resolved_repo,
        workdir=resolved_repo,
        executable_path=resolved_repo / "scripts" / "praxis",
        authority_source="database",
    )

    monkeypatch.chdir(active_checkout / "repo")
    monkeypatch.setattr("runtime.launcher_authority.read_launcher_seed_config", lambda: seed)
    monkeypatch.setattr("runtime.launcher_authority.resolve_launcher_workspace", lambda _seed: resolution)
    monkeypatch.setattr(
        "runtime.launcher_authority.os.execve",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not delegate")),
    )

    assert launcher_main(["workflow", "--help"]) == 1
    captured = capsys.readouterr()
    assert "different Praxis checkout" in captured.err
    assert "run ./scripts/bootstrap or praxis launcher configure" in captured.err


def test_runtime_launcher_source_has_no_shell_sql_or_localhost_default() -> None:
    source = Path(__file__).resolve().parents[2] / "runtime" / "launcher_authority.py"
    text = source.read_text(encoding="utf-8")

    assert "subprocess" not in text
    assert "os.system" not in text
    assert "WORKFLOW_DATABASE_URL:-" not in text
    assert "os.getcwd" not in text
