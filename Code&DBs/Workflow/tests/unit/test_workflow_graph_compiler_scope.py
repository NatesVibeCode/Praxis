from runtime.workflow_graph_compiler import _job_write_scope


def test_job_write_scope_infers_artifact_paths_from_output_contract() -> None:
    job = {
        "label": "pull_crm_snapshot",
        "prompt": (
            "Build artifacts/hubspot_enrichment_20260418/00_crm_snapshot.json. "
            "Use artifacts/hubspot_enrichment_20260418/_deals.json as an intermediate."
        ),
        "outcome_goal": (
            "artifacts/hubspot_enrichment_20260418/00_crm_snapshot.json lists open deals."
        ),
    }

    assert _job_write_scope(job) == [
        "artifacts/hubspot_enrichment_20260418/00_crm_snapshot.json",
        "artifacts/hubspot_enrichment_20260418/_deals.json",
    ]


def test_job_write_scope_does_not_infer_code_paths_from_prompt() -> None:
    job = {
        "label": "code_edit",
        "prompt": "Update Code&DBs/Workflow/runtime/spec_compiler.py",
    }

    assert _job_write_scope(job) == []


def test_explicit_write_scope_wins_over_artifact_inference() -> None:
    job = {
        "label": "bounded",
        "write_scope": ["Code&DBs/Workflow/runtime/example.py"],
        "prompt": "Also write artifacts/should_not_expand/output.json",
    }

    assert _job_write_scope(job) == ["Code&DBs/Workflow/runtime/example.py"]


def test_top_level_write_scope_wins_over_artifact_inference() -> None:
    job = {
        "label": "pull_crm_snapshot",
        "write": ["artifacts/hubspot_enrichment_20260418/"],
        "prompt": (
            "Build artifacts/hubspot_enrichment_20260418/00_crm_snapshot.json. "
            "Use artifacts/hubspot_enrichment_20260418/search_deals.raw.json as an intermediate."
        ),
    }

    assert _job_write_scope(job) == ["artifacts/hubspot_enrichment_20260418/"]
