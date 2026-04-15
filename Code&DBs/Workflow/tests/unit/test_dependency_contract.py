from __future__ import annotations

import tomllib
from pathlib import Path

import runtime.dependency_contract as dependency_contract


REPO_ROOT = Path(__file__).resolve().parents[4]
WORKFLOW_ROOT = REPO_ROOT / "Code&DBs" / "Workflow"


def test_pyproject_and_manifest_point_at_the_same_dependency_contract() -> None:
    pyproject = tomllib.loads((WORKFLOW_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    dynamic = pyproject["project"]["dynamic"]
    dependency_file = pyproject["tool"]["setuptools"]["dynamic"]["dependencies"]["file"]
    manifest_in = (WORKFLOW_ROOT / "MANIFEST.in").read_text(encoding="utf-8")

    assert dynamic == ["dependencies"]
    assert dependency_file == ["requirements.runtime.txt"]
    assert (WORKFLOW_ROOT / dependency_file[0]).is_file()
    assert manifest_in.strip() == "include requirements.runtime.txt"


def test_dependency_truth_report_respects_declared_scopes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    manifest = tmp_path / "requirements.runtime.txt"
    manifest.write_text(
        "\n".join(
            [
                "# scopes: api_server,runtime",
                "alpha-package",
                "sentence-transformers",
                "",
                "# scopes: runtime",
                "google-genai",
                "psycopg2-binary",
            ]
        ),
        encoding="utf-8",
    )

    present_imports = {
        "alpha_package",
        "sentence_transformers",
        "google.genai",
    }

    def _fake_find_spec(name: str):
        return object() if name in present_imports else None

    versions = {
        "alpha-package": "1.0.0",
        "sentence-transformers": "2.0.0",
        "google-genai": "3.0.0",
    }

    monkeypatch.setattr(dependency_contract.importlib.util, "find_spec", _fake_find_spec)
    monkeypatch.setattr(
        dependency_contract.importlib.metadata,
        "version",
        lambda dist: versions[dist],
    )

    api_report = dependency_contract.dependency_truth_report(
        scope="api_server",
        manifest_path=manifest,
    )
    runtime_report = dependency_contract.dependency_truth_report(
        scope="runtime",
        manifest_path=manifest,
    )

    assert api_report["ok"] is True
    assert api_report["required_count"] == 2
    assert api_report["available_count"] == 2
    assert [pkg["distribution"] for pkg in api_report["packages"]] == [
        "alpha-package",
        "sentence-transformers",
    ]
    assert [pkg["import_name"] for pkg in api_report["packages"]] == [
        "alpha_package",
        "sentence_transformers",
    ]

    assert runtime_report["ok"] is False
    assert runtime_report["required_count"] == 4
    assert runtime_report["available_count"] == 3
    assert runtime_report["missing_count"] == 1
    assert [pkg["distribution"] for pkg in runtime_report["packages"]] == [
        "alpha-package",
        "sentence-transformers",
        "google-genai",
        "psycopg2-binary",
    ]
    assert [pkg["available"] for pkg in runtime_report["packages"]] == [
        True,
        True,
        True,
        False,
    ]


def test_default_manifest_exposes_the_api_server_subset(monkeypatch) -> None:
    monkeypatch.setattr(dependency_contract.importlib.util, "find_spec", lambda name: object())
    monkeypatch.setattr(
        dependency_contract.importlib.metadata,
        "version",
        lambda dist: "1.0.0",
    )

    manifest = WORKFLOW_ROOT / "requirements.runtime.txt"
    api_report = dependency_contract.dependency_truth_report(
        scope="api_server",
        manifest_path=manifest,
    )
    worker_report = dependency_contract.dependency_truth_report(
        scope="workflow_worker",
        manifest_path=manifest,
    )
    semantic_backend_report = dependency_contract.dependency_truth_report(
        scope="semantic_backend",
        manifest_path=manifest,
    )
    runtime_report = dependency_contract.dependency_truth_report(
        scope="runtime",
        manifest_path=manifest,
    )

    assert api_report["ok"] is True
    assert api_report["required_count"] == 4
    assert [pkg["distribution"] for pkg in api_report["packages"]] == [
        "asyncpg",
        "pydantic",
        "fastapi",
        "uvicorn",
    ]
    assert worker_report["ok"] is True
    assert worker_report["required_count"] == 7
    assert [pkg["distribution"] for pkg in worker_report["packages"]] == [
        "asyncpg",
        "pydantic",
        "anthropic",
        "google-genai",
        "openai",
        "croniter",
        "psycopg2-binary",
    ]
    assert semantic_backend_report["ok"] is True
    assert semantic_backend_report["required_count"] == 5
    assert [pkg["distribution"] for pkg in semantic_backend_report["packages"]] == [
        "pydantic",
        "fastapi",
        "uvicorn",
        "numpy",
        "sentence-transformers",
    ]
    assert runtime_report["ok"] is True
    assert runtime_report["required_count"] == 11


def test_dependency_truth_report_strips_version_pins_from_distribution_names(
    tmp_path: Path,
    monkeypatch,
) -> None:
    manifest = tmp_path / "requirements.runtime.txt"
    manifest.write_text(
        "\n".join(
            [
                "# scopes: api_server",
                "asyncpg==0.31.0",
                "google-genai==1.70.0",
            ]
        ),
        encoding="utf-8",
    )

    versions = {
        "asyncpg": "0.31.0",
        "google-genai": "1.70.0",
    }

    monkeypatch.setattr(dependency_contract.importlib.util, "find_spec", lambda name: object())
    monkeypatch.setattr(
        dependency_contract.importlib.metadata,
        "version",
        lambda dist: versions[dist],
    )

    report = dependency_contract.dependency_truth_report(
        scope="api_server",
        manifest_path=manifest,
    )

    assert [pkg["requirement"] for pkg in report["packages"]] == [
        "asyncpg==0.31.0",
        "google-genai==1.70.0",
    ]
    assert [pkg["distribution"] for pkg in report["packages"]] == [
        "asyncpg",
        "google-genai",
    ]


def test_requirements_for_scope_returns_exact_requirement_lines(tmp_path: Path) -> None:
    manifest = tmp_path / "requirements.runtime.txt"
    manifest.write_text(
        "\n".join(
            [
                "# scopes: api_server",
                "asyncpg==0.31.0",
                "# scopes: workflow_worker",
                "openai==2.24.0",
            ]
        ),
        encoding="utf-8",
    )

    assert dependency_contract.requirements_for_scope(
        scope="api_server",
        manifest_path=manifest,
    ) == ("asyncpg==0.31.0",)
    assert dependency_contract.requirements_for_scope(
        scope="workflow_worker",
        manifest_path=manifest,
    ) == ("openai==2.24.0",)


def test_dependency_truth_report_uses_manifest_and_import_mapping(
    tmp_path: Path,
    monkeypatch,
) -> None:
    manifest = tmp_path / "requirements.runtime.txt"
    manifest.write_text(
        "\n".join(
            [
                "alpha-package",
                "sentence-transformers",
                "google-genai",
                "psycopg2-binary",
            ]
        ),
        encoding="utf-8",
    )

    present_imports = {
        "alpha_package",
        "sentence_transformers",
        "google.genai",
    }

    def _fake_find_spec(name: str):
        return object() if name in present_imports else None

    versions = {
        "alpha-package": "1.0.0",
        "sentence-transformers": "2.0.0",
        "google-genai": "3.0.0",
    }

    monkeypatch.setattr(dependency_contract.importlib.util, "find_spec", _fake_find_spec)
    monkeypatch.setattr(
        dependency_contract.importlib.metadata,
        "version",
        lambda dist: versions[dist],
    )

    report = dependency_contract.dependency_truth_report(scope="all", manifest_path=manifest)

    assert report["ok"] is False
    assert report["required_count"] == 4
    assert report["available_count"] == 3
    assert report["missing_count"] == 1
    assert [pkg["distribution"] for pkg in report["packages"]] == [
        "alpha-package",
        "sentence-transformers",
        "google-genai",
        "psycopg2-binary",
    ]
    assert [pkg["import_name"] for pkg in report["packages"]] == [
        "alpha_package",
        "sentence_transformers",
        "google.genai",
        "psycopg2",
    ]
    assert [pkg["available"] for pkg in report["packages"]] == [True, True, True, False]
    assert report["missing"] == [report["packages"][3]]
