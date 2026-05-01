from __future__ import annotations

from pathlib import Path

from runtime.workflow import cli_execution_adapter


def test_execution_workdir_rebases_host_checkout_to_container(
    monkeypatch,
    tmp_path: Path,
) -> None:
    host_root = tmp_path / "host"
    container_root = tmp_path / "container"
    container_root.mkdir()

    monkeypatch.setattr(
        cli_execution_adapter,
        "_translate_host_path_to_container",
        lambda value: str(container_root)
        if value == str(host_root)
        else value,
    )

    assert cli_execution_adapter._execution_workdir({"workdir": str(host_root)}) == str(
        container_root
    )


def test_execution_bundle_merges_context_scope_into_existing_bundle() -> None:
    bundle = cli_execution_adapter._execution_bundle(
        {
            "execution_bundle": {
                "run_id": "run.alpha",
                "job_label": "job.alpha",
                "access_policy": {
                    "write_scope": ["runtime/spec_materializer.py"],
                    "declared_read_scope": [],
                },
            },
            "scope_read": [
                "runtime/operations/commands/provider_availability_refresh.py",
                "runtime/workflow/pipeline_eval.py",
            ],
            "test_scope": ["tests/unit/test_workflow_pipeline_eval.py"],
        }
    )

    assert bundle is not None
    assert bundle["access_policy"]["write_scope"] == ["runtime/spec_materializer.py"]
    assert bundle["access_policy"]["declared_read_scope"] == [
        "runtime/operations/commands/provider_availability_refresh.py",
        "runtime/workflow/pipeline_eval.py",
    ]
    assert bundle["access_policy"]["test_scope"] == [
        "tests/unit/test_workflow_pipeline_eval.py"
    ]
