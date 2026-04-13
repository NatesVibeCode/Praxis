from __future__ import annotations

from runtime.definition_compile_kernel import build_definition, split_sentences


def test_split_sentences_breaks_inline_numbered_steps() -> None:
    prose = (
        "I want to be able to 1) capture the application UI, "
        "2) research the API docs with Brave, "
        "3) record the docs and plan the connector, "
        "4) build a basic connector to the common objects."
    )

    sentences = split_sentences(prose)

    assert [sentence for sentence, _, _ in sentences] == [
        "capture the application UI",
        "research the API docs with Brave",
        "record the docs and plan the connector",
        "build a basic connector to the common objects.",
    ]


def test_build_definition_creates_draft_flow_for_inline_numbered_steps() -> None:
    prose = (
        "I want to be able to 1) capture the application UI, "
        "2) research the API docs with Brave, "
        "3) record the docs and plan the connector, "
        "4) build a basic connector to the common objects."
    )

    definition = build_definition(
        source_prose=prose,
        compiled_prose=prose,
        references=[],
        capabilities=[],
        authority="",
        sla={},
    )

    assert [block["summary"] for block in definition["narrative_blocks"]] == [
        "capture the application UI",
        "research the API docs with Brave",
        "record the docs and plan the connector",
        "build a basic connector to the common objects.",
    ]
    assert [step["summary"] for step in definition["draft_flow"]] == [
        "capture the application UI",
        "research the API docs with Brave",
        "record the docs and plan the connector",
        "build a basic connector to the common objects.",
    ]
    assert [step["title"] for step in definition["draft_flow"]] == [
        "capture the application UI",
        "research the API docs with Brave",
        "record the docs and plan the connector",
        "build a basic connector to the common objects",
    ]
    assert definition["draft_flow"][1]["depends_on"] == ["step-001"]
    assert definition["draft_flow"][2]["depends_on"] == ["step-002"]
    assert definition["draft_flow"][3]["depends_on"] == ["step-003"]


def test_build_definition_keeps_long_inline_step_titles_intact() -> None:
    prose = (
        "I want to be able to 1) capture the application UI, "
        "2) research the API docs with Brave, "
        "3) have you come back and record details of those docs and make a plan of how to build a connector, "
        "4) build a basic connector to the common objects."
    )

    definition = build_definition(
        source_prose=prose,
        compiled_prose=prose,
        references=[],
        capabilities=[],
        authority="",
        sla={},
    )

    assert definition["narrative_blocks"][2]["title"] == (
        "have you come back and record details of those docs and make a plan of how to build a connector"
    )
    assert definition["draft_flow"][2]["title"] == (
        "have you come back and record details of those docs and make a plan of how to build a connector"
    )


def test_split_sentences_breaks_need_to_connector_flow_into_steps() -> None:
    prose = (
        "Im going to feed you an application name, "
        "We will need to search the web for that API's docs and store them in the db, "
        "then we need to make a skinny first-pass connector plan, "
        "then that needs to get created and tested until it works."
    )

    sentences = split_sentences(prose)

    assert [sentence for sentence, _, _ in sentences] == [
        "Im going to feed you an application name",
        "We will need to search the web for that API's docs and store them in the db",
        "then we need to make a skinny first-pass connector plan",
        "then that needs to get created and tested until it works.",
    ]
