from __future__ import annotations

import base64
import io
import json
import os
import subprocess
import tarfile
import tempfile
from pathlib import Path

import pytest

import runtime.sandbox_runtime as sandbox_runtime
from runtime.workspace_paths import container_auth_seed_dir, container_home
from runtime.sandbox_runtime import (
    ArtifactReceipt,
    CloudflareRemoteSandboxProvider,
    DockerLocalSandboxProvider,
    SandboxExecRequest,
    SandboxExecutionResult,
    SandboxRuntime,
    SandboxSession,
    TeardownReceipt,
    WorkspaceSnapshot,
    derive_sandbox_identity,
)

AUTH_HOME = str(Path(tempfile.gettempdir()) / "praxis-auth-home")
CONTAINER_HOME = str(container_home())
OPENAI_AUTH_SEED_PATH = str(container_auth_seed_dir() / "openai-auth.json")


@pytest.fixture(autouse=True)
def _allow_existing_legacy_workspace_copy_tests(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_ALLOW_LEGACY_WORKSPACE_COPY", "1")


def _test_cli_auth_catalog() -> sandbox_runtime._CliAuthCatalog:
    return sandbox_runtime._CliAuthCatalog(
        mount_specs=(
            sandbox_runtime._CliAuthMountSpec(
                provider_slug="openai",
                host_relative_path=".codex/auth.json",
                container_path=sandbox_runtime._OPENAI_AUTH_SEED_PATH,
            ),
            sandbox_runtime._CliAuthMountSpec(
                provider_slug="anthropic",
                host_relative_path=".claude.json",
                container_path=f"{CONTAINER_HOME}/.claude.json",
            ),
            sandbox_runtime._CliAuthMountSpec(
                provider_slug="google",
                host_relative_path=".gemini/oauth_creds.json",
                container_path=f"{CONTAINER_HOME}/.gemini/oauth_creds.json",
            ),
        ),
        home_tmpfs_dirs=(".claude", ".codex", ".gemini"),
    )


def _patch_cli_auth_catalog(monkeypatch) -> None:
    monkeypatch.setattr(
        sandbox_runtime,
        "_load_cli_auth_catalog",
        _test_cli_auth_catalog,
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
    _patch_cli_auth_catalog(monkeypatch)
    monkeypatch.setenv("PRAXIS_CLI_AUTH_HOME", AUTH_HOME)
    monkeypatch.setattr(
        sandbox_runtime.os.path,
        "isfile",
        lambda path: path in {
            f"{AUTH_HOME}/.codex/auth.json",
            f"{AUTH_HOME}/.claude.json",
            f"{AUTH_HOME}/.gemini/oauth_creds.json",
        },
    )

    flags = sandbox_runtime._cli_auth_volume_flags()

    assert f"{AUTH_HOME}/.codex/auth.json:{sandbox_runtime._OPENAI_AUTH_SEED_PATH}:ro" in flags
    assert f"{AUTH_HOME}/.claude.json:{CONTAINER_HOME}/.claude.json:ro" not in flags
    assert (
        f"{AUTH_HOME}/.gemini/oauth_creds.json:"
        f"{CONTAINER_HOME}/.gemini/oauth_creds.json:ro"
    ) in flags


def test_cli_auth_volume_flags_accept_host_home_with_worker_home_probe(monkeypatch) -> None:
    _patch_cli_auth_catalog(monkeypatch)
    monkeypatch.setenv("PRAXIS_CLI_AUTH_HOME", AUTH_HOME)
    monkeypatch.setattr(sandbox_runtime.os.path, "expanduser", lambda value: "/root" if value == "~" else value)
    monkeypatch.setattr(
        sandbox_runtime.os.path,
        "isfile",
        lambda path: path in {
            f"{AUTH_HOME}/.codex/auth.json",
            f"{AUTH_HOME}/.claude.json",
            f"{AUTH_HOME}/.gemini/oauth_creds.json",
        },
    )

    flags = sandbox_runtime._cli_auth_volume_flags()

    assert f"{AUTH_HOME}/.codex/auth.json:{sandbox_runtime._OPENAI_AUTH_SEED_PATH}:ro" in flags
    assert f"{AUTH_HOME}/.claude.json:{CONTAINER_HOME}/.claude.json:ro" not in flags
    assert (
        f"{AUTH_HOME}/.gemini/oauth_creds.json:"
        f"{CONTAINER_HOME}/.gemini/oauth_creds.json:ro"
    ) in flags


def test_cli_auth_volume_flags_limit_mounts_to_selected_provider(monkeypatch) -> None:
    _patch_cli_auth_catalog(monkeypatch)
    monkeypatch.setenv("PRAXIS_CLI_AUTH_HOME", AUTH_HOME)
    monkeypatch.setattr(
        sandbox_runtime.os.path,
        "isfile",
        lambda path: path in {
            f"{AUTH_HOME}/.codex/auth.json",
            f"{AUTH_HOME}/.claude.json",
            f"{AUTH_HOME}/.gemini/oauth_creds.json",
        },
    )

    openai_flags = sandbox_runtime._cli_auth_volume_flags(provider_slug="openai")
    anthropic_flags = sandbox_runtime._cli_auth_volume_flags(provider_slug="anthropic")

    assert openai_flags == [
        "-v",
        f"{AUTH_HOME}/.codex/auth.json:{sandbox_runtime._OPENAI_AUTH_SEED_PATH}:ro",
    ]
    assert anthropic_flags == []


def test_docker_local_exec_prefers_metadata_resource_limits(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    class _NoopThread:
        def __init__(self, *args, **kwargs):
            del args, kwargs

        def start(self) -> None:
            return None

        def join(self, timeout=None) -> None:
            del timeout
            return None

    class _FakePopen:
        def __init__(self, cmd, **kwargs):
            captured["cmd"] = cmd
            captured["popen_kwargs"] = kwargs
            self.returncode = 0

        def communicate(self, input=None, timeout=None):
            captured["stdin"] = input
            captured["timeout"] = timeout
            return ("ok", "")

    monkeypatch.setattr(
        sandbox_runtime,
        "resolve_docker_image",
        lambda requested_image, image_exists, **_kwargs: (
            requested_image or "praxis-worker:test",
            {"source": "requested"},
        ),
    )
    monkeypatch.setattr(sandbox_runtime, "_docker_image_available", lambda image: True)
    monkeypatch.setattr(sandbox_runtime.threading, "Thread", _NoopThread)
    monkeypatch.setattr(sandbox_runtime.subprocess, "Popen", _FakePopen)

    provider = DockerLocalSandboxProvider()
    session = SandboxSession(
        sandbox_session_id="sandbox_session:run.alpha:job.alpha",
        sandbox_group_id="group:run.alpha",
        provider="docker_local",
        provider_session_id="provider-session",
        workspace_root=str(tmp_path),
        network_policy="provider_only",
        workspace_materialization="copy",
        metadata={
            "docker_memory": "8g",
            "docker_cpus": "6",
            "provider_slug": "openai",
            "auth_mount_policy": "provider_scoped",
        },
    )

    provider.exec(
        session,
        SandboxExecRequest(
            command="echo hi",
            stdin_text="payload",
            env={"OPENAI_API_KEY": "test-key"},
            timeout_seconds=15,
            execution_transport="cli",
            image="praxis-worker:test",
        ),
    )

    docker_cmd = captured["cmd"]
    assert "--memory" in docker_cmd
    assert docker_cmd[docker_cmd.index("--memory") + 1] == "8g"
    assert "--cpus" in docker_cmd
    assert docker_cmd[docker_cmd.index("--cpus") + 1] == "6"


def test_docker_local_exec_skips_auth_mounts_when_policy_is_none(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    class _NoopThread:
        def __init__(self, *args, **kwargs):
            del args, kwargs

        def start(self) -> None:
            return None

        def join(self, timeout=None) -> None:
            del timeout
            return None

    class _FakePopen:
        def __init__(self, cmd, **kwargs):
            captured["cmd"] = cmd
            self.returncode = 0

        def communicate(self, input=None, timeout=None):
            del input, timeout
            return ("ok", "")

    monkeypatch.setattr(
        sandbox_runtime,
        "resolve_docker_image",
        lambda requested_image, image_exists, **_kwargs: (
            requested_image or "praxis-worker:test",
            {"source": "requested"},
        ),
    )
    monkeypatch.setattr(sandbox_runtime, "_docker_image_available", lambda image: True)
    monkeypatch.setattr(sandbox_runtime.threading, "Thread", _NoopThread)
    monkeypatch.setattr(sandbox_runtime.subprocess, "Popen", _FakePopen)
    monkeypatch.setattr(
        sandbox_runtime,
        "_cli_auth_volume_flags",
        lambda provider_slug=None: ["-v", f"/host/{provider_slug or 'all'}:/container/auth:ro"],
    )

    provider = DockerLocalSandboxProvider()
    session = SandboxSession(
        sandbox_session_id="sandbox_session:run.alpha:job.alpha",
        sandbox_group_id="group:run.alpha",
        provider="docker_local",
        provider_session_id="provider-session",
        workspace_root=str(tmp_path),
        network_policy="provider_only",
        workspace_materialization="copy",
        metadata={
            "provider_slug": "openai",
            "auth_mount_policy": "none",
        },
    )

    provider.exec(
        session,
        SandboxExecRequest(
            command="echo hi",
            stdin_text="payload",
            env={"OPENAI_API_KEY": "test-key"},
            timeout_seconds=15,
            execution_transport="cli",
            image="praxis-worker:test",
        ),
    )

    docker_cmd = captured["cmd"]
    assert "/host/openai:/container/auth:ro" not in docker_cmd
    assert "/host/all:/container/auth:ro" not in docker_cmd




def test_sandbox_runtime_runs_provider_contract_and_persists_artifacts(tmp_path) -> None:
    (tmp_path / "seed.txt").write_text("seed", encoding="utf-8")
    # Bundle below declares write_scope=["changed.txt"]; the shard materializer
    # now folds that into the snapshot path_filter, so the expected ref must
    # hash the same filter.
    expected_snapshot_ref = sandbox_runtime._workspace_snapshot_ref(
        str(tmp_path), path_filter=("changed.txt",)
    )
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


def test_sandbox_runtime_rejects_out_of_scope_artifacts_under_legacy_contract(tmp_path) -> None:
    # Legacy contract (no completion_contract.submission_required): filesystem
    # write_scope is the authority and out-of-scope artifacts fail the job hard.
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


def test_sandbox_runtime_captures_drift_under_submission_contract(tmp_path) -> None:
    # Under the sealed-submission contract (workflow_execution::mutating-jobs-
    # use-sealed-submission-and-verify-refs-only), the sandbox filesystem is
    # ephemeral scratch. The authoritative deliverable is the
    # workflow_job_submissions row; scratch drift is captured as structured
    # evidence and the job continues.
    (tmp_path / "seed.txt").write_text("seed", encoding="utf-8")
    runtime = SandboxRuntime()
    fake = _RecordingProvider()
    runtime._providers = {"fake": fake}  # type: ignore[attr-defined]

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
                "access_policy": {"write_scope": ["allowed.txt"]},
                "completion_contract": {"submission_required": True},
            }
        },
    )

    assert result.artifact_refs == ("changed.txt",)
    assert fake.calls[-1] == ("destroy", "completed")
    assert len(result.artifact_scope_drift) == 1
    drift = result.artifact_scope_drift[0]
    assert drift["artifact_ref"] == "changed.txt"
    assert drift["reason"] == "outside_write_scope"
    assert drift["submission_required"] is True
    assert drift["declared_write_scope"] == ("allowed.txt",)


def test_sandbox_runtime_skips_dehydrate_under_submission_contract(tmp_path) -> None:
    # When the sandbox runs in submission-contract mode, the agent's scratch
    # files must not be dehydrated back to the host repo — the sealed
    # submission carries the authoritative patch payload instead.
    (tmp_path / "seed.txt").write_text("seed", encoding="utf-8")
    runtime = SandboxRuntime()
    fake = _RecordingProvider()
    runtime._providers = {"fake": fake}  # type: ignore[attr-defined]

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
                "completion_contract": {"submission_required": True},
            }
        },
    )

    # No file should have been dehydrated to the host workdir — the only thing
    # present is the seed we placed before exec.
    assert not (tmp_path / "changed.txt").exists()


def test_sandbox_runtime_none_materialization_uses_empty_workspace_without_copyback(tmp_path) -> None:
    (tmp_path / "seed.txt").write_text("seed", encoding="utf-8")

    class _LocalRecordingProvider(_RecordingProvider):
        execution_lane = "local"

    runtime = SandboxRuntime()
    fake = _LocalRecordingProvider()
    runtime._providers = {"fake": fake}  # type: ignore[attr-defined]

    result = runtime.execute_command(
        provider_name="fake",
        sandbox_session_id="sandbox_session:run.alpha:job.alpha",
        sandbox_group_id="group:run.alpha",
        workdir=str(tmp_path),
        command="echo hi",
        stdin_text="payload",
        env={"OPENAI_API_KEY": "test-key"},
        timeout_seconds=15,
        network_policy="enabled",
        workspace_materialization="none",
        execution_transport="cli",
        metadata={
            "execution_bundle": {
                "access_policy": {"write_scope": ["changed.txt"]},
            }
        },
    )

    hydrate_snapshot = fake.calls[1][1]
    assert getattr(hydrate_snapshot, "materialization") == "none"
    assert not (tmp_path / ".sandbox" / "seed.txt").exists()
    assert (tmp_path / ".sandbox" / "changed.txt").read_text(encoding="utf-8") == "updated"
    assert not (tmp_path / "changed.txt").exists()
    assert result.artifact_refs == ("changed.txt",)
    assert result.workspace_snapshot_ref.startswith("workspace_snapshot:")


def test_sandbox_runtime_rejects_legacy_workspace_copy_without_operator_opt_in(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.delenv("PRAXIS_ALLOW_LEGACY_WORKSPACE_COPY", raising=False)
    (tmp_path / "seed.txt").write_text("seed", encoding="utf-8")

    runtime = SandboxRuntime()
    runtime._providers = {"fake": _RecordingProvider()}  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="PRAXIS_ALLOW_LEGACY_WORKSPACE_COPY=1"):
        runtime.execute_command(
            provider_name="fake",
            sandbox_session_id="sandbox_session:run.alpha:job.alpha",
            sandbox_group_id="group:run.alpha",
            workdir=str(tmp_path),
            command="echo hi",
            stdin_text="payload",
            env={},
            timeout_seconds=15,
            network_policy="provider_only",
            workspace_materialization="copy",
            execution_transport="cli",
        )


def test_sandbox_runtime_allows_scoped_workspace_copy_without_operator_opt_in(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.delenv("PRAXIS_ALLOW_LEGACY_WORKSPACE_COPY", raising=False)
    (tmp_path / "seed.txt").write_text("seed", encoding="utf-8")

    runtime = SandboxRuntime()
    fake = _RecordingProvider()
    runtime._providers = {"fake": fake}  # type: ignore[attr-defined]

    result = runtime.execute_command(
        provider_name="fake",
        sandbox_session_id="sandbox_session:run.alpha:job.alpha",
        sandbox_group_id="group:run.alpha",
        workdir=str(tmp_path),
        command="echo hi",
        stdin_text="payload",
        env={},
        timeout_seconds=15,
        network_policy="provider_only",
        workspace_materialization="copy",
        execution_transport="cli",
        metadata={
            "execution_bundle": {
                "access_policy": {
                    "resolved_read_scope": ["seed.txt"],
                    "write_scope": ["changed.txt"],
                }
            }
        },
    )

    hydrate_snapshot = fake.calls[1][1]
    assert getattr(hydrate_snapshot, "materialization") == "copy"
    assert getattr(hydrate_snapshot, "path_filter") == ("changed.txt", "seed.txt")
    assert result.workspace_snapshot_ref.startswith("workspace_snapshot:")


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


def test_workspace_snapshot_ref_includes_overlay_files(tmp_path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    (source_root / "seed.txt").write_text("alpha", encoding="utf-8")

    base_ref = sandbox_runtime._workspace_snapshot_ref(str(source_root))
    overlay_ref = sandbox_runtime._workspace_snapshot_ref(
        str(source_root),
        overlay_files=(
            {
                "relative_path": ".gemini/settings.json",
                "content": '{"mcpServers":{"dag-workflow":{"url":"http://mcp.local/mcp","type":"http"}}}',
            },
        ),
    )

    assert overlay_ref != base_ref


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
            "command": "/usr/bin/python3 -m runtime.api_transport_worker --model claude-haiku",
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
            "command": "/usr/bin/python3 -m runtime.api_transport_worker --model claude-haiku",
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


def test_docker_local_hydrate_workspace_reuses_cached_snapshot_archive(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("PRAXIS_SANDBOX_SNAPSHOT_CACHE_DIR", str(tmp_path / "snapshot-cache"))
    monkeypatch.setattr("runtime.sandbox_runtime._docker_available", lambda: True)

    source_root = tmp_path / "source"
    source_root.mkdir()
    (source_root / "seed.txt").write_text("seed", encoding="utf-8")
    snapshot_ref = sandbox_runtime._workspace_snapshot_ref(str(source_root))

    provider = DockerLocalSandboxProvider()
    spec = type(
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

    first_session = provider.create_session(spec)
    first_receipt = provider.hydrate_workspace(
        first_session,
        WorkspaceSnapshot(
            source_root=str(source_root),
            materialization="copy",
            workspace_snapshot_ref=snapshot_ref,
        ),
    )
    assert first_receipt.workspace_snapshot_cache_hit is False
    assert (Path(first_session.workspace_root) / "seed.txt").read_text(encoding="utf-8") == "seed"
    provider.destroy_session(first_session, "completed")

    (source_root / "seed.txt").unlink()

    second_session = provider.create_session(spec)
    second_receipt = provider.hydrate_workspace(
        second_session,
        WorkspaceSnapshot(
            source_root=str(source_root),
            materialization="copy",
            workspace_snapshot_ref=snapshot_ref,
        ),
    )

    assert second_receipt.workspace_snapshot_cache_hit is True
    assert (Path(second_session.workspace_root) / "seed.txt").read_text(encoding="utf-8") == "seed"
    provider.destroy_session(second_session, "completed")


def test_docker_local_hydrate_workspace_applies_overlay_files(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("PRAXIS_SANDBOX_SNAPSHOT_CACHE_DIR", str(tmp_path / "snapshot-cache"))
    monkeypatch.setattr("runtime.sandbox_runtime._docker_available", lambda: True)

    source_root = tmp_path / "source"
    source_root.mkdir()
    (source_root / "seed.txt").write_text("seed", encoding="utf-8")
    overlay_content = '{"mcpServers":{"dag-workflow":{"url":"http://mcp.local/mcp","type":"http"}}}'
    snapshot_ref = sandbox_runtime._workspace_snapshot_ref(
        str(source_root),
        overlay_files=(
            {
                "relative_path": ".gemini/settings.json",
                "content": overlay_content,
            },
        ),
    )

    provider = DockerLocalSandboxProvider()
    spec = type(
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
    session = provider.create_session(spec)
    receipt = provider.hydrate_workspace(
        session,
        WorkspaceSnapshot(
            source_root=str(source_root),
            materialization="copy",
            workspace_snapshot_ref=snapshot_ref,
            overlay_files=(
                {
                    "relative_path": ".gemini/settings.json",
                    "content": overlay_content,
                },
            ),
        ),
    )

    assert receipt.workspace_snapshot_ref == snapshot_ref
    assert (Path(session.workspace_root) / ".gemini" / "settings.json").read_text(encoding="utf-8") == overlay_content
    provider.destroy_session(session, "completed")


def test_docker_local_hydrate_workspace_rejects_unsafe_cached_snapshot_archive(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("PRAXIS_SANDBOX_SNAPSHOT_CACHE_DIR", str(tmp_path / "snapshot-cache"))
    monkeypatch.setattr("runtime.sandbox_runtime._docker_available", lambda: True)

    source_root = tmp_path / "source"
    source_root.mkdir()
    snapshot_ref = "workspace_snapshot:unsafe"
    cache_dir = Path(sandbox_runtime._workspace_snapshot_cache_dir(snapshot_ref))
    cache_dir.mkdir(parents=True)
    archive_path = cache_dir / "workspace.tar.gz"
    metadata_path = cache_dir / "metadata.json"
    metadata_path.write_text(
        json.dumps({"workspace_snapshot_ref": snapshot_ref, "hydrated_files": 1}),
        encoding="utf-8",
    )
    payload = b"owned"
    unsafe_member = tarfile.TarInfo("../pwned.txt")
    unsafe_member.size = len(payload)
    with tarfile.open(archive_path, mode="w:gz") as archive:
        archive.addfile(unsafe_member, io.BytesIO(payload))

    provider = DockerLocalSandboxProvider()
    spec = type(
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
    session = provider.create_session(spec)

    with pytest.raises(RuntimeError, match="unsafe workspace archive member"):
        provider.hydrate_workspace(
            session,
            WorkspaceSnapshot(
                source_root=str(source_root),
                materialization="copy",
                workspace_snapshot_ref=snapshot_ref,
            ),
        )
    assert not (Path(session.workspace_root).parent / "pwned.txt").exists()
    provider.destroy_session(session, "completed")


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


def test_cloudflare_remote_hydrate_workspace_reuses_cached_snapshot_archive(monkeypatch, tmp_path) -> None:
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
            {"hydrated_files": 1, "workspace_snapshot_ref": "workspace_snapshot:test"},
            {"hydrated_files": 1, "workspace_snapshot_ref": "workspace_snapshot:test"},
        ]
    )

    def _fake_urlopen(request, timeout):
        del timeout
        requests.append((request.full_url, json.loads(request.data.decode("utf-8"))))
        return _FakeResponse(next(responses))

    monkeypatch.setenv("PRAXIS_CLOUDFLARE_SANDBOX_URL", "https://sandbox.example")
    monkeypatch.setenv("PRAXIS_SANDBOX_SNAPSHOT_CACHE_DIR", str(tmp_path / "snapshot-cache"))
    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)

    source_root = tmp_path / "source"
    source_root.mkdir()
    (source_root / "seed.txt").write_text("seed", encoding="utf-8")
    snapshot_ref = sandbox_runtime._workspace_snapshot_ref(str(source_root))

    provider = CloudflareRemoteSandboxProvider()
    session = SandboxSession(
        sandbox_session_id="sandbox_session:run.alpha:job.alpha",
        sandbox_group_id="group:run.alpha",
        provider="cloudflare_remote",
        provider_session_id="cf-session",
        workspace_root=str(tmp_path / "local-mirror"),
        network_policy="provider_only",
        workspace_materialization="copy",
        metadata={},
    )

    first_receipt = provider.hydrate_workspace(
        session,
        WorkspaceSnapshot(
            source_root=str(source_root),
            materialization="copy",
            workspace_snapshot_ref=snapshot_ref,
        ),
    )
    assert first_receipt.workspace_snapshot_cache_hit is False

    (source_root / "seed.txt").unlink()

    second_receipt = provider.hydrate_workspace(
        session,
        WorkspaceSnapshot(
            source_root=str(source_root),
            materialization="copy",
            workspace_snapshot_ref=snapshot_ref,
        ),
    )

    assert second_receipt.workspace_snapshot_cache_hit is True
    assert requests[0][1]["archive_base64"] == requests[1][1]["archive_base64"]


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
        lambda **kwargs: (
            "praxis-codex:latest",
            {
                "source": "agent_family",
                "build_error": None,
                "required_image": "praxis-codex:latest",
            },
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
                "metadata": {},
            },
        )()
    )

    try:
        with pytest.raises(RuntimeError, match="thin sandbox image"):
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
    finally:
        provider.destroy_session(session, "failed")


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

    try:
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
    finally:
        provider.destroy_session(session, "completed")


def test_docker_local_exec_mounts_only_provider_auth_files(monkeypatch, tmp_path) -> None:
    docker_cmds: list[list[str]] = []

    _patch_cli_auth_catalog(monkeypatch)
    monkeypatch.setattr("runtime.sandbox_runtime._docker_available", lambda: True)
    monkeypatch.setattr("runtime.sandbox_runtime._docker_image_available", lambda image: True)
    monkeypatch.setattr(
        "runtime.sandbox_runtime.os.path.isfile",
        lambda path: path in {
            f"{AUTH_HOME}/.codex/auth.json",
            f"{AUTH_HOME}/.claude.json",
            f"{AUTH_HOME}/.gemini/oauth_creds.json",
        },
    )
    monkeypatch.setattr("runtime.sandbox_runtime.os.path.expanduser", lambda value: AUTH_HOME if value == "~" else value)
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

    try:
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
        assert f"{AUTH_HOME}/.codex/auth.json:{sandbox_runtime._OPENAI_AUTH_SEED_PATH}:ro" in joined_cmd
        assert f"{AUTH_HOME}/.claude.json:{CONTAINER_HOME}/.claude.json:ro" not in joined_cmd
        assert (
            f"{AUTH_HOME}/.gemini/oauth_creds.json:"
            f"{CONTAINER_HOME}/.gemini/oauth_creds.json:ro"
        ) not in joined_cmd
    finally:
        provider.destroy_session(session, "completed")


def test_docker_local_exec_forwards_anthropic_oauth_token(monkeypatch, tmp_path) -> None:
    docker_cmds: list[list[str]] = []

    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "claude-oauth-token")
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "fallback-token")
    monkeypatch.setattr("runtime.sandbox_runtime._docker_available", lambda: True)
    monkeypatch.setattr("runtime.sandbox_runtime._docker_image_available", lambda image: True)
    monkeypatch.setattr("runtime.sandbox_runtime.os.path.isfile", lambda path: False)
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
                "sandbox_session_id": "sandbox_session:run.alpha:job.claude",
                "sandbox_group_id": "group:run.alpha",
                "network_policy": "disabled",
                "workspace_materialization": "copy",
                "timeout_seconds": 30,
                "metadata": {"provider_slug": "anthropic"},
            },
        )()
    )

    try:
        provider.exec(
            session,
            type(
                "Request",
                (),
                {
                    "command": "claude --print",
                    "stdin_text": "",
                    "env": {"PATH": "/usr/bin:/bin"},
                    "timeout_seconds": 30,
                    "execution_transport": "cli",
                    "image": None,
                },
            )(),
        )

        run_cmd = next(cmd for cmd in docker_cmds if len(cmd) >= 2 and cmd[:2] == ["docker", "run"])
        env_values = [
            value
            for index, value in enumerate(run_cmd)
            if index > 0 and run_cmd[index - 1] == "-e"
        ]
        assert "CLAUDE_CODE_OAUTH_TOKEN=claude-oauth-token" in env_values
        assert "ANTHROPIC_AUTH_TOKEN=fallback-token" not in env_values
    finally:
        provider.destroy_session(session, "completed")


def test_docker_local_autobuilds_thin_image_when_missing(monkeypatch, tmp_path) -> None:
    seen: dict[str, str] = {}

    monkeypatch.delenv("PRAXIS_DOCKER_IMAGE", raising=False)
    monkeypatch.setattr("runtime.sandbox_runtime._docker_available", lambda: True)
    monkeypatch.setattr(
        "runtime.sandbox_runtime.resolve_docker_image",
        lambda **kwargs: (
            "praxis-codex:latest",
            {"source": "agent_family", "build_error": None, "built_default": True},
        ),
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
                "metadata": {"provider_slug": "openai"},
            },
        )()
    )

    try:
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

        assert seen["image"] == "praxis-codex:latest"
        assert result.execution_mode == "docker_local"
    finally:
        provider.destroy_session(session, "completed")


# -----------------------------------------------------------------------------
# Shard materialization: _workspace_file_entries honors path_filter so only the
# declared shard is hydrated. Empty filter preserves legacy full-copy behavior.
# -----------------------------------------------------------------------------


def test_workspace_file_entries_empty_filter_returns_everything(tmp_path) -> None:
    (tmp_path / "a.py").write_text("a", encoding="utf-8")
    (tmp_path / "b.py").write_text("b", encoding="utf-8")
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "c.py").write_text("c", encoding="utf-8")

    entries = sandbox_runtime._workspace_file_entries(str(tmp_path))
    relpaths = {r for r, _ in entries}
    assert relpaths == {"a.py", "b.py", "nested/c.py"}


def test_workspace_file_entries_exact_path_filter(tmp_path) -> None:
    (tmp_path / "a.py").write_text("a", encoding="utf-8")
    (tmp_path / "b.py").write_text("b", encoding="utf-8")

    entries = sandbox_runtime._workspace_file_entries(
        str(tmp_path), path_filter=("a.py",)
    )
    relpaths = {r for r, _ in entries}
    assert relpaths == {"a.py"}


def test_workspace_file_entries_directory_path_filter_includes_descendants(tmp_path) -> None:
    (tmp_path / "Code&DBs" / "Workflow" / "runtime").mkdir(parents=True)
    (tmp_path / "Code&DBs" / "Workflow" / "runtime" / "sandbox_runtime.py").write_text(
        "runtime", encoding="utf-8"
    )
    (tmp_path / "Code&DBs" / "Workflow" / "artifacts").mkdir(parents=True)
    (tmp_path / "Code&DBs" / "Workflow" / "artifacts" / "PLAN.md").write_text(
        "plan", encoding="utf-8"
    )
    (tmp_path / "README.md").write_text("drop", encoding="utf-8")

    entries = sandbox_runtime._workspace_file_entries(
        str(tmp_path),
        path_filter=("Code&DBs/Workflow",),
    )
    relpaths = {r for r, _ in entries}
    assert relpaths == {
        "Code&DBs/Workflow/artifacts/PLAN.md",
        "Code&DBs/Workflow/runtime/sandbox_runtime.py",
    }


def test_workspace_file_entries_fnmatch_glob_filter(tmp_path) -> None:
    (tmp_path / "keep.py").write_text("k", encoding="utf-8")
    (tmp_path / "keep.ts").write_text("k", encoding="utf-8")
    (tmp_path / "drop.md").write_text("d", encoding="utf-8")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "nested.py").write_text("n", encoding="utf-8")

    entries = sandbox_runtime._workspace_file_entries(
        str(tmp_path), path_filter=("*.py", "sub/*.py")
    )
    relpaths = {r for r, _ in entries}
    assert relpaths == {"keep.py", "sub/nested.py"}


def test_workspace_snapshot_ref_differs_across_filters(tmp_path) -> None:
    (tmp_path / "a.py").write_text("a", encoding="utf-8")
    (tmp_path / "b.py").write_text("b", encoding="utf-8")

    full_ref = sandbox_runtime._workspace_snapshot_ref(str(tmp_path))
    filtered_ref = sandbox_runtime._workspace_snapshot_ref(
        str(tmp_path), path_filter=("a.py",)
    )
    assert full_ref != filtered_ref, (
        "different path_filter values must produce different snapshot refs "
        "so the host-side archive cache does not collide on shard boundaries"
    )


def test_execution_shard_paths_extracts_union_from_access_policy() -> None:
    metadata = {
        "execution_bundle": {
            "access_policy": {
                "resolved_read_scope": ["adapters/keychain.py"],
                "write_scope": ["adapters/credentials.py"],
                "test_scope": ["tests/unit/test_keychain.py"],
                "blast_radius": ["registry/provider_execution_registry.py"],
                "declared_read_scope": [],
            }
        }
    }
    paths = sandbox_runtime._execution_shard_paths(metadata)
    assert paths == (
        "adapters/credentials.py",
        "adapters/keychain.py",
        "registry/provider_execution_registry.py",
        "tests/unit/test_keychain.py",
    )


def test_execution_shard_paths_returns_empty_when_bundle_missing() -> None:
    assert sandbox_runtime._execution_shard_paths(None) == ()
    assert sandbox_runtime._execution_shard_paths({}) == ()
    assert sandbox_runtime._execution_shard_paths({"execution_bundle": {}}) == ()
    assert sandbox_runtime._execution_shard_paths(
        {"execution_bundle": {"access_policy": {}}}
    ) == ()


def test_write_workspace_snapshot_archive_writes_only_shard(tmp_path) -> None:
    import tarfile

    (tmp_path / "keep.py").write_text("keep", encoding="utf-8")
    (tmp_path / "drop.py").write_text("drop", encoding="utf-8")
    archive_path = tmp_path / "out.tar.gz"

    hydrated = sandbox_runtime._write_workspace_snapshot_archive(
        str(tmp_path),
        str(archive_path),
        path_filter=("keep.py",),
    )

    assert hydrated == 1
    with tarfile.open(archive_path, mode="r:gz") as archive:
        names = {
            member.name.split("/", 1)[1]
            for member in archive.getmembers()
            if member.isfile()
        }
    assert names == {"keep.py"}, (
        f"archive must contain only the shard files, got {names}"
    )
