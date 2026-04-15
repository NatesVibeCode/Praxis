from __future__ import annotations

import base64
import json
import subprocess
from pathlib import Path

import pytest

import runtime.sandbox_runtime as sandbox_runtime
from runtime.sandbox_runtime import (
    ArtifactReceipt,
    CloudflareRemoteSandboxProvider,
    DockerLocalSandboxProvider,
    SandboxExecutionResult,
    SandboxRuntime,
    SandboxSession,
    TeardownReceipt,
    derive_sandbox_identity,
)


class _RecordingProvider:
    provider_name = "fake"

    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def create_session(self, spec):
        self.calls.append(("create", spec))
        return SandboxSession(
            sandbox_session_id=spec.sandbox_session_id,
            sandbox_group_id=spec.sandbox_group_id,
            provider=self.provider_name,
            provider_session_id="provider-session",
            workspace_root=str(Path(spec.workdir) / ".sandbox"),
            network_policy=spec.network_policy,
            workspace_materialization=spec.workspace_materialization,
            metadata={},
        )

    def hydrate_workspace(self, session, snapshot):
        self.calls.append(("hydrate", snapshot))
        workspace_root = Path(session.workspace_root)
        workspace_root.mkdir(parents=True, exist_ok=True)
        for source in Path(snapshot.source_root).iterdir():
            if source.is_file():
                (workspace_root / source.name).write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
        from runtime.sandbox_runtime import HydrationReceipt

        return HydrationReceipt(
            sandbox_session_id=session.sandbox_session_id,
            workspace_root=session.workspace_root,
            hydrated_files=1,
            workspace_materialization=snapshot.materialization,
        )

    def exec(self, session, request):
        self.calls.append(("exec", request))
        changed = Path(session.workspace_root) / "changed.txt"
        changed.write_text("updated", encoding="utf-8")
        return SandboxExecutionResult(
            sandbox_session_id=session.sandbox_session_id,
            sandbox_group_id=session.sandbox_group_id,
            sandbox_provider=self.provider_name,
            execution_transport=request.execution_transport,
            exit_code=0,
            stdout="ok",
            stderr="",
            timed_out=False,
            artifact_refs=(),
            started_at="2026-04-09T00:00:00+00:00",
            finished_at="2026-04-09T00:00:01+00:00",
            network_policy=session.network_policy,
            provider_latency_ms=5,
            execution_mode=self.provider_name,
            workspace_root=session.workspace_root,
        )

    def collect_artifacts(self, session, before_manifest):
        self.calls.append(("artifacts", before_manifest))
        return ArtifactReceipt(
            sandbox_session_id=session.sandbox_session_id,
            artifact_refs=("changed.txt",),
            artifact_count=1,
        )

    def destroy_session(self, session, disposition):
        self.calls.append(("destroy", disposition))
        return TeardownReceipt(
            sandbox_session_id=session.sandbox_session_id,
            provider=self.provider_name,
            disposition=disposition,
        )


class _ArtifactStore:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def capture(self, file_path: str, content: str, sandbox_id: str):
        self.calls.append((file_path, content, sandbox_id))
        return type("ArtifactRecord", (), {"artifact_id": f"artifact:{file_path}"})()


def test_cli_auth_volume_flags_use_explicit_host_home(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_CLI_AUTH_HOME", "/Users/nate")
    monkeypatch.setattr(
        sandbox_runtime.os.path,
        "isfile",
        lambda path: path in {
            "/Users/nate/.codex/auth.json",
            "/Users/nate/.claude.json",
            "/Users/nate/.gemini/oauth_creds.json",
        },
    )

    flags = sandbox_runtime._cli_auth_volume_flags()

    assert "/Users/nate/.codex/auth.json:/root/.codex/auth.json:ro" in flags
    assert "/Users/nate/.claude.json:/root/.claude.json:ro" in flags
    assert "/Users/nate/.gemini/oauth_creds.json:/root/.gemini/oauth_creds.json:ro" in flags


def test_cli_auth_volume_flags_accept_host_home_with_worker_home_probe(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_CLI_AUTH_HOME", "/Users/nate")
    monkeypatch.setattr(sandbox_runtime.os.path, "expanduser", lambda value: "/root" if value == "~" else value)
    monkeypatch.setattr(
        sandbox_runtime.os.path,
        "isfile",
        lambda path: path in {
            "/root/.codex/auth.json",
            "/root/.claude.json",
            "/root/.gemini/oauth_creds.json",
        },
    )

    flags = sandbox_runtime._cli_auth_volume_flags()

    assert "/Users/nate/.codex/auth.json:/root/.codex/auth.json:ro" in flags
    assert "/Users/nate/.claude.json:/root/.claude.json:ro" in flags
    assert "/Users/nate/.gemini/oauth_creds.json:/root/.gemini/oauth_creds.json:ro" in flags


def test_cli_auth_volume_flags_limit_mounts_to_selected_provider(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_CLI_AUTH_HOME", "/Users/nate")
    monkeypatch.setattr(
        sandbox_runtime.os.path,
        "isfile",
        lambda path: path in {
            "/Users/nate/.codex/auth.json",
            "/Users/nate/.claude.json",
            "/Users/nate/.gemini/oauth_creds.json",
        },
    )

    openai_flags = sandbox_runtime._cli_auth_volume_flags(provider_slug="openai")
    anthropic_flags = sandbox_runtime._cli_auth_volume_flags(provider_slug="anthropic")

    assert openai_flags == [
        "-v",
        "/Users/nate/.codex/auth.json:/root/.codex/auth.json:ro",
    ]
    assert anthropic_flags == [
        "-v",
        "/Users/nate/.claude.json:/root/.claude.json:ro",
    ]




def test_sandbox_runtime_runs_provider_contract_and_persists_artifacts(tmp_path) -> None:
    (tmp_path / "seed.txt").write_text("seed", encoding="utf-8")
    expected_snapshot_ref = sandbox_runtime._workspace_snapshot_ref(str(tmp_path))
    runtime = SandboxRuntime()
    fake = _RecordingProvider()
    runtime._providers = {"fake": fake}  # type: ignore[attr-defined]
    artifact_store = _ArtifactStore()

    result = runtime.execute_command(
        provider_name="fake",
        sandbox_session_id="sandbox_session:run.alpha:job.alpha",
        sandbox_group_id="group:run.alpha",
        workdir=str(tmp_path),
        command="echo hi",
        stdin_text="payload",
        env={"OPENAI_API_KEY": "test-key"},
        timeout_seconds=15,
        network_policy="provider_only",
        workspace_materialization="copy",
        execution_transport="cli",
        metadata={
            "execution_bundle": {
                "access_policy": {"write_scope": ["changed.txt"]},
            }
        },
        artifact_store=artifact_store,
    )

    assert [name for name, _ in fake.calls] == ["create", "hydrate", "exec", "artifacts", "destroy"]
    assert result.artifact_refs == ("artifact:changed.txt",)
    assert result.workspace_snapshot_ref == expected_snapshot_ref
    assert artifact_store.calls == [
        ("changed.txt", "updated", "sandbox_session:run.alpha:job.alpha")
    ]


def test_sandbox_runtime_rejects_out_of_scope_artifacts_before_host_copyback(tmp_path) -> None:
    (tmp_path / "seed.txt").write_text("seed", encoding="utf-8")
    runtime = SandboxRuntime()
    fake = _RecordingProvider()
    runtime._providers = {"fake": fake}  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="outside declared write_scope"):
        runtime.execute_command(
            provider_name="fake",
            sandbox_session_id="sandbox_session:run.alpha:job.alpha",
            sandbox_group_id="group:run.alpha",
            workdir=str(tmp_path),
            command="echo hi",
            stdin_text="payload",
            env={"OPENAI_API_KEY": "test-key"},
            timeout_seconds=15,
            network_policy="provider_only",
            workspace_materialization="copy",
            execution_transport="cli",
            metadata={
                "execution_bundle": {
                    "access_policy": {"write_scope": ["allowed.txt"]},
                }
            },
        )

    assert not (tmp_path / "changed.txt").exists()
    assert fake.calls[-1] == ("destroy", "failed")


def test_sandbox_runtime_requires_docker_when_docker_is_unavailable(
    monkeypatch,
    tmp_path,
) -> None:
    (tmp_path / "seed.txt").write_text("seed", encoding="utf-8")
    monkeypatch.setattr("runtime.sandbox_runtime._docker_available", lambda: False)

    runtime = SandboxRuntime()
    with pytest.raises(RuntimeError, match="Docker is required"):
        runtime.execute_command(
            provider_name="docker_local",
            sandbox_session_id="sandbox_session:run.alpha:job.alpha",
            sandbox_group_id="group:run.alpha",
            workdir=str(tmp_path),
            command="printf updated > changed.txt",
            stdin_text="",
            env={},
            timeout_seconds=15,
            network_policy="provider_only",
            workspace_materialization="copy",
            execution_transport="cli",
        )


def test_derive_sandbox_identity_prefers_run_scoped_ids(tmp_path) -> None:
    session_id, group_id = derive_sandbox_identity(
        workdir=str(tmp_path),
        execution_bundle={"run_id": "run.alpha", "job_label": "job.alpha"},
        execution_transport="cli",
    )

    assert session_id == "sandbox_session:run.alpha:job.alpha"
    assert group_id == "group:run.alpha"


def test_workspace_snapshot_ref_is_content_addressed(tmp_path) -> None:
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    first_root.mkdir()
    second_root.mkdir()

    (first_root / "seed.txt").write_text("alpha", encoding="utf-8")
    (second_root / "seed.txt").write_text("alpha", encoding="utf-8")

    first_ref = sandbox_runtime._workspace_snapshot_ref(str(first_root))
    second_ref = sandbox_runtime._workspace_snapshot_ref(str(second_root))

    assert first_ref == second_ref

    (second_root / "seed.txt").write_text("beta", encoding="utf-8")

    changed_ref = sandbox_runtime._workspace_snapshot_ref(str(second_root))

    assert changed_ref != first_ref


def test_derive_sandbox_identity_is_deterministic_for_matching_adhoc_requests(tmp_path) -> None:
    first_session_id, first_group_id = derive_sandbox_identity(
        workdir=str(tmp_path),
        execution_bundle={"access_policy": {"write_scope": ["README.md"]}},
        execution_transport="cli",
        identity_payload={
            "provider_slug": "openai",
            "model_slug": "gpt-5.4-mini",
            "command": "wizard-cli --json",
            "stdin_text": "hello world",
        },
    )
    second_session_id, second_group_id = derive_sandbox_identity(
        workdir=str(tmp_path),
        execution_bundle={"access_policy": {"write_scope": ["README.md"]}},
        execution_transport="cli",
        identity_payload={
            "provider_slug": "openai",
            "model_slug": "gpt-5.4-mini",
            "command": "wizard-cli --json",
            "stdin_text": "hello world",
        },
    )

    assert first_session_id == second_session_id
    assert first_session_id.startswith("sandbox_session:adhoc:")
    assert first_group_id is None
    assert second_group_id is None


def test_derive_sandbox_identity_changes_when_adhoc_request_changes(tmp_path) -> None:
    first_session_id, _ = derive_sandbox_identity(
        workdir=str(tmp_path),
        execution_bundle=None,
        execution_transport="api",
        identity_payload={
            "provider_slug": "anthropic",
            "model_slug": "claude-haiku",
            "command": "python3 -m runtime.api_transport_worker --model claude-haiku",
            "stdin_text": "hello world",
        },
    )
    second_session_id, _ = derive_sandbox_identity(
        workdir=str(tmp_path),
        execution_bundle=None,
        execution_transport="api",
        identity_payload={
            "provider_slug": "anthropic",
            "model_slug": "claude-haiku",
            "command": "python3 -m runtime.api_transport_worker --model claude-haiku",
            "stdin_text": "hello again",
        },
    )

    assert first_session_id != second_session_id


def test_cloudflare_remote_provider_emits_expected_bridge_requests(monkeypatch, tmp_path) -> None:
    requests: list[tuple[str, dict]] = []

    class _FakeResponse:
        def __init__(self, payload: dict) -> None:
            self._payload = json.dumps(payload).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return self._payload

    responses = iter(
        [
            {"provider_session_id": "cf-session"},
            {"hydrated_files": 2},
            {
                "exit_code": 0,
                "stdout": "remote ok",
                "stderr": "",
                "timed_out": False,
                "artifact_refs": ["changed.txt"],
                "started_at": "2026-04-09T00:00:00+00:00",
                "finished_at": "2026-04-09T00:00:01+00:00",
                "provider_latency_ms": 9,
            },
            {"artifact_refs": ["changed.txt"]},
            {"ok": True},
        ]
    )

    def _fake_urlopen(request, timeout):
        del timeout
        requests.append((request.full_url, json.loads(request.data.decode("utf-8"))))
        return _FakeResponse(next(responses))

    monkeypatch.setenv("PRAXIS_CLOUDFLARE_SANDBOX_URL", "https://sandbox.example")
    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)
    (tmp_path / "seed.txt").write_text("seed", encoding="utf-8")

    provider = CloudflareRemoteSandboxProvider()
    session = provider.create_session(
        type(
            "Spec",
            (),
            {
                "sandbox_session_id": "sandbox_session:run.alpha:job.alpha",
                "sandbox_group_id": "group:run.alpha",
                "network_policy": "provider_only",
                "workspace_materialization": "copy",
                "timeout_seconds": 30,
                "metadata": {},
            },
        )()
    )
    provider.hydrate_workspace(session, type("Snapshot", (), {"source_root": str(tmp_path), "materialization": "copy"})())
    execution = provider.exec(
        session,
        type(
            "Request",
            (),
            {
                "command": "echo hi",
                "stdin_text": "payload",
                "env": {"OPENAI_API_KEY": "test-key"},
                "timeout_seconds": 30,
                "execution_transport": "api",
            },
        )(),
    )
    artifacts = provider.collect_artifacts(session, {})
    provider.destroy_session(session, "completed")

    assert execution.stdout == "remote ok"
    assert artifacts.artifact_refs == ("changed.txt",)
    assert [url for url, _ in requests] == [
        "https://sandbox.example/sessions/create",
        "https://sandbox.example/sessions/cf-session/hydrate",
        "https://sandbox.example/sessions/cf-session/exec",
        "https://sandbox.example/sessions/cf-session/artifacts",
        "https://sandbox.example/sessions/cf-session/destroy",
    ]


def test_cloudflare_remote_syncs_artifacts_for_capture(monkeypatch, tmp_path) -> None:
    requests: list[tuple[str, dict]] = []

    class _FakeResponse:
        def __init__(self, payload: dict) -> None:
            self._payload = json.dumps(payload).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return self._payload

    responses = iter(
        [
            {"provider_session_id": "cf-session"},
            {"hydrated_files": 1},
            {
                "exit_code": 0,
                "stdout": "remote ok",
                "stderr": "",
                "timed_out": False,
                "artifact_refs": ["changed.txt"],
                "started_at": "2026-04-09T00:00:00+00:00",
                "finished_at": "2026-04-09T00:00:01+00:00",
                "provider_latency_ms": 9,
            },
            {
                "artifact_refs": ["changed.txt"],
                "artifacts": [
                    {
                        "path": "changed.txt",
                        "content_base64": base64.b64encode(b"updated remotely").decode("ascii"),
                    }
                ],
            },
            {"ok": True},
        ]
    )

    def _fake_urlopen(request, timeout):
        del timeout
        requests.append((request.full_url, json.loads(request.data.decode("utf-8"))))
        return _FakeResponse(next(responses))

    monkeypatch.setenv("PRAXIS_CLOUDFLARE_SANDBOX_URL", "https://sandbox.example")
    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)
    (tmp_path / "seed.txt").write_text("seed", encoding="utf-8")
    artifact_store = _ArtifactStore()

    result = SandboxRuntime().execute_command(
        provider_name="cloudflare_remote",
        sandbox_session_id="sandbox_session:run.alpha:job.alpha",
        sandbox_group_id="group:run.alpha",
        workdir=str(tmp_path),
        command="echo hi",
        stdin_text="payload",
        env={"OPENAI_API_KEY": "test-key"},
        timeout_seconds=30,
        network_policy="provider_only",
        workspace_materialization="copy",
        execution_transport="api",
        artifact_store=artifact_store,
    )

    assert result.artifact_refs == ("artifact:changed.txt",)
    assert artifact_store.calls == [
        ("changed.txt", "updated remotely", "sandbox_session:run.alpha:job.alpha")
    ]
    assert requests[3][1] == {"include_content": True}


def test_cloudflare_remote_rejects_invalid_artifact_paths(monkeypatch, tmp_path) -> None:
    class _FakeResponse:
        def __init__(self, payload: dict) -> None:
            self._payload = json.dumps(payload).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return self._payload

    responses = iter(
        [
            {"provider_session_id": "cf-session"},
            {"hydrated_files": 1},
            {
                "exit_code": 0,
                "stdout": "remote ok",
                "stderr": "",
                "timed_out": False,
                "artifact_refs": ["changed.txt"],
                "started_at": "2026-04-09T00:00:00+00:00",
                "finished_at": "2026-04-09T00:00:01+00:00",
                "provider_latency_ms": 9,
            },
            {
                "artifact_refs": ["../escape.txt"],
                "artifacts": [
                    {
                        "path": "../escape.txt",
                        "content_base64": base64.b64encode(b"bad").decode("ascii"),
                    }
                ],
            },
            {"ok": True},
        ]
    )

    def _fake_urlopen(request, timeout):
        del timeout
        return _FakeResponse(next(responses))

    monkeypatch.setenv("PRAXIS_CLOUDFLARE_SANDBOX_URL", "https://sandbox.example")
    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)
    (tmp_path / "seed.txt").write_text("seed", encoding="utf-8")

    with pytest.raises(RuntimeError, match="sandbox workspace boundary"):
        SandboxRuntime().execute_command(
            provider_name="cloudflare_remote",
            sandbox_session_id="sandbox_session:run.alpha:job.alpha",
            sandbox_group_id="group:run.alpha",
            workdir=str(tmp_path),
            command="echo hi",
            stdin_text="payload",
            env={"OPENAI_API_KEY": "test-key"},
            timeout_seconds=30,
            network_policy="provider_only",
            workspace_materialization="copy",
            execution_transport="api",
            artifact_store=_ArtifactStore(),
        )


def test_docker_local_requires_available_image(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("runtime.sandbox_runtime._docker_available", lambda: True)
    monkeypatch.setattr("runtime.sandbox_runtime._docker_image_available", lambda image: False)
    monkeypatch.setattr(
        "runtime.sandbox_runtime.resolve_docker_image",
        lambda **kwargs: ("praxis-worker:latest", {"source": "default", "build_error": None}),
    )

    provider = DockerLocalSandboxProvider()
    session = provider.create_session(
        type(
            "Spec",
            (),
            {
                "sandbox_session_id": "sandbox_session:run.alpha:job.alpha",
                "sandbox_group_id": "group:run.alpha",
                "network_policy": "disabled",
                "workspace_materialization": "copy",
                "timeout_seconds": 30,
                "metadata": {},
            },
        )()
    )

    with pytest.raises(RuntimeError, match="PRAXIS_DOCKER_IMAGE"):
        provider.exec(
            session,
            type(
                "Request",
                (),
                {
                    "command": "echo hi",
                    "stdin_text": "",
                    "env": {"PATH": "/usr/bin:/bin"},
                    "timeout_seconds": 30,
                    "execution_transport": "cli",
                    "image": None,
                },
            )(),
        )


def test_docker_local_reads_image_from_env_per_exec(monkeypatch, tmp_path) -> None:
    seen: dict[str, str] = {}

    monkeypatch.setenv("PRAXIS_DOCKER_IMAGE", "dag-worker:test")
    monkeypatch.setattr("runtime.sandbox_runtime._docker_available", lambda: True)
    monkeypatch.setattr(
        "runtime.sandbox_runtime._docker_image_available",
        lambda image: seen.setdefault("image", image) or True,
    )
    monkeypatch.setattr(
        "runtime.sandbox_runtime.subprocess.Popen",
        lambda *args, **kwargs: type(
            "_Proc",
            (),
            {
                "returncode": 0,
                "communicate": staticmethod(lambda input=None, timeout=None: ("ok", "")),
            },
        )(),
    )

    provider = DockerLocalSandboxProvider()
    session = provider.create_session(
        type(
            "Spec",
            (),
            {
                "sandbox_session_id": "sandbox_session:run.alpha:job.alpha",
                "sandbox_group_id": "group:run.alpha",
                "network_policy": "disabled",
                "workspace_materialization": "copy",
                "timeout_seconds": 30,
                "metadata": {},
            },
        )()
    )

    result = provider.exec(
        session,
        type(
            "Request",
            (),
            {
                "command": "echo hi",
                "stdin_text": "",
                "env": {"PATH": "/usr/bin:/bin"},
                "timeout_seconds": 30,
                "execution_transport": "cli",
                "image": None,
            },
        )(),
    )

    assert seen["image"] == "dag-worker:test"
    assert result.execution_mode == "docker_local"


def test_docker_local_exec_mounts_only_provider_auth_files(monkeypatch, tmp_path) -> None:
    docker_cmds: list[list[str]] = []

    monkeypatch.setattr("runtime.sandbox_runtime._docker_available", lambda: True)
    monkeypatch.setattr("runtime.sandbox_runtime._docker_image_available", lambda image: True)
    monkeypatch.setattr(
        "runtime.sandbox_runtime.os.path.isfile",
        lambda path: path in {
            "/Users/nate/.codex/auth.json",
            "/Users/nate/.claude.json",
            "/Users/nate/.gemini/oauth_creds.json",
        },
    )
    monkeypatch.setattr("runtime.sandbox_runtime.os.path.expanduser", lambda value: "/Users/nate" if value == "~" else value)
    monkeypatch.setattr(
        "runtime.sandbox_runtime.subprocess.Popen",
        lambda args, **kwargs: (
            docker_cmds.append(list(args))
            or type(
                "_Proc",
                (),
                {
                    "returncode": 0,
                    "communicate": staticmethod(lambda input=None, timeout=None: ("ok", "")),
                },
            )()
        ),
    )

    provider = DockerLocalSandboxProvider()
    session = provider.create_session(
        type(
            "Spec",
            (),
            {
                "sandbox_session_id": "sandbox_session:run.alpha:job.alpha",
                "sandbox_group_id": "group:run.alpha",
                "network_policy": "disabled",
                "workspace_materialization": "copy",
                "timeout_seconds": 30,
                "metadata": {"provider_slug": "openai"},
            },
        )()
    )

    provider.exec(
        session,
        type(
            "Request",
            (),
            {
                "command": "echo hi",
                "stdin_text": "",
                "env": {"PATH": "/usr/bin:/bin"},
                "timeout_seconds": 30,
                "execution_transport": "cli",
                "image": None,
            },
        )(),
    )

    run_cmd = next(cmd for cmd in docker_cmds if len(cmd) >= 2 and cmd[:2] == ["docker", "run"])
    joined_cmd = " ".join(run_cmd)
    assert "/Users/nate/.codex/auth.json:/root/.codex/auth.json:ro" in joined_cmd
    assert "/Users/nate/.claude.json:/root/.claude.json:ro" not in joined_cmd
    assert "/Users/nate/.gemini/oauth_creds.json:/root/.gemini/oauth_creds.json:ro" not in joined_cmd


def test_docker_local_autobuilds_default_image_when_missing(monkeypatch, tmp_path) -> None:
    seen: dict[str, str] = {}

    monkeypatch.delenv("PRAXIS_DOCKER_IMAGE", raising=False)
    monkeypatch.setattr("runtime.sandbox_runtime._docker_available", lambda: True)
    monkeypatch.setattr(
        "runtime.sandbox_runtime.resolve_docker_image",
        lambda **kwargs: ("praxis-worker:latest", {"source": "default", "build_error": None, "built_default": True}),
    )
    monkeypatch.setattr(
        "runtime.sandbox_runtime._docker_image_available",
        lambda image: seen.setdefault("image", image) or True,
    )
    monkeypatch.setattr(
        "runtime.sandbox_runtime.subprocess.Popen",
        lambda *args, **kwargs: type(
            "_Proc",
            (),
            {
                "returncode": 0,
                "communicate": staticmethod(lambda input=None, timeout=None: ("ok", "")),
            },
        )(),
    )

    provider = DockerLocalSandboxProvider()
    session = provider.create_session(
        type(
            "Spec",
            (),
            {
                "sandbox_session_id": "sandbox_session:run.alpha:job.alpha",
                "sandbox_group_id": "group:run.alpha",
                "network_policy": "disabled",
                "workspace_materialization": "copy",
                "timeout_seconds": 30,
                "metadata": {},
            },
        )()
    )

    result = provider.exec(
        session,
        type(
            "Request",
            (),
            {
                "command": "echo hi",
                "stdin_text": "",
                "env": {"PATH": "/usr/bin:/bin"},
                "timeout_seconds": 30,
                "execution_transport": "cli",
                "image": None,
            },
        )(),
    )

    assert seen["image"] == "praxis-worker:latest"
    assert result.execution_mode == "docker_local"
