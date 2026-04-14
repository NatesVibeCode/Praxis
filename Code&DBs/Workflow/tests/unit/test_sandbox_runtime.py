from __future__ import annotations

import base64
import json
import subprocess
from pathlib import Path

import pytest

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




def test_sandbox_runtime_runs_provider_contract_and_persists_artifacts(tmp_path) -> None:
    (tmp_path / "seed.txt").write_text("seed", encoding="utf-8")
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
        artifact_store=artifact_store,
    )

    assert [name for name, _ in fake.calls] == ["create", "hydrate", "exec", "artifacts", "destroy"]
    assert result.artifact_refs == ("artifact:changed.txt",)
    assert artifact_store.calls == [
        ("changed.txt", "updated", "sandbox_session:run.alpha:job.alpha")
    ]


def test_sandbox_runtime_falls_back_to_host_local_when_docker_is_unavailable(
    monkeypatch,
    tmp_path,
) -> None:
    (tmp_path / "seed.txt").write_text("seed", encoding="utf-8")
    monkeypatch.setattr("runtime.sandbox_runtime._docker_available", lambda: False)

    runtime = SandboxRuntime()
    result = runtime.execute_command(
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

    assert result.sandbox_provider == "host_local"
    assert result.execution_mode == "host_local"
    assert result.artifact_refs == ("changed.txt",)
    assert (tmp_path / "changed.txt").read_text(encoding="utf-8") == "updated"


def test_derive_sandbox_identity_prefers_run_scoped_ids(tmp_path) -> None:
    session_id, group_id = derive_sandbox_identity(
        workdir=str(tmp_path),
        execution_bundle={"run_id": "run.alpha", "job_label": "job.alpha"},
        execution_transport="cli",
    )

    assert session_id == "sandbox_session:run.alpha:job.alpha"
    assert group_id == "group:run.alpha"


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

