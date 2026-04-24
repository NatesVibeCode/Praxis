from runtime.canonical_workflows import _apply_progressive_build_step


def test_progressive_build_adds_one_checked_unit_per_pass() -> None:
    prose = (
        "Create a deterministic smoke workflow that takes one text input, "
        "validates it is non-empty, summarizes it in one sentence, and records "
        "an execution receipt. Keep it local and auditable."
    )
    definition: dict = {}

    definition = _apply_progressive_build_step(definition, workflow_id="wf_progressive", body={"prose": prose})
    assert definition["progressive_build"]["last_unit"]["title"] == "Validate input"
    assert [step["id"] for step in definition["draft_flow"]] == ["step:validate-input"]

    definition = _apply_progressive_build_step(definition, workflow_id="wf_progressive", body={"prose": prose})
    assert definition["progressive_build"]["last_unit"]["title"] == "Summarize input"
    assert [step["id"] for step in definition["draft_flow"]] == [
        "step:validate-input",
        "step:summarize-input",
    ]

    definition = _apply_progressive_build_step(definition, workflow_id="wf_progressive", body={"prose": prose})
    progress = definition["progressive_build"]
    assert progress["last_unit"]["title"] == "Record execution receipt"
    assert progress["completion"] == {"accepted": 3, "planned": 3}
    assert [check["state"] for check in progress["checks"]] == ["passed"] * 5
    assert definition["draft_flow"][2]["depends_on"] == ["step:summarize-input"]
