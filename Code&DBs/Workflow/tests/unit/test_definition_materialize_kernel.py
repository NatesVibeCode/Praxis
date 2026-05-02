from __future__ import annotations

from runtime.definition_materialize_kernel import materialize_definition, build_definition, split_sentences


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
        materialized_prose=prose,
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


def test_materialize_definition_accepts_compact_capability_labels() -> None:
    definition = {
        "source_prose": "Search for docs, then build an integration.",
        "materialized_prose": "Search for docs, then build an integration.",
        "references": [],
        "capabilities": ["search", "integration_build"],
        "authority": "",
        "sla": {},
    }

    materialized = materialize_definition(definition)

    assert [capability["slug"] for capability in materialized["capabilities"]] == [
        "search",
        "integration_build",
    ]


def test_build_definition_keeps_long_inline_step_titles_intact() -> None:
    prose = (
        "I want to be able to 1) capture the application UI, "
        "2) research the API docs with Brave, "
        "3) have you come back and record details of those docs and make a plan of how to build a connector, "
        "4) build a basic connector to the common objects."
    )

    definition = build_definition(
        source_prose=prose,
        materialized_prose=prose,
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


def test_split_sentences_breaks_comma_imperatives_marked_by_then() -> None:
    # Multi-step prose with comma-separated imperatives and a ", then" marker must
    # decompose into one step per clause — not collapse into a single narrative block.
    prose = (
        "Read a transcript, summarize it, use it to create a fan out search, "
        "then use that search to suggest roadmap items."
    )

    sentences = split_sentences(prose)

    assert [sentence for sentence, _, _ in sentences] == [
        "Read a transcript",
        "summarize it",
        "use it to create a fan out search",
        "then use that search to suggest roadmap items.",
    ]


def test_split_sentences_leaves_relative_clauses_unsplit_without_then_marker() -> None:
    # Prose that reads like one assertion with relative clauses must NOT be split
    # on commas — the comma-imperative splitter requires an explicit ", then" marker.
    prose = (
        "Build a workflow that ingests bug reports, routes by severity, "
        "and requires review before closure."
    )

    sentences = split_sentences(prose)

    assert len(sentences) == 1
    assert sentences[0][0] == prose


def test_split_sentences_leaves_noun_lists_unsplit() -> None:
    # Single-word comma lists (noun enumerations) must not trigger imperative split.
    prose = "Apples, oranges, bananas."

    sentences = split_sentences(prose)

    assert len(sentences) == 1
    assert sentences[0][0] == prose
