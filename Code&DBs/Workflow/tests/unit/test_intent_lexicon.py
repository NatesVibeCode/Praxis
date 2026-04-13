from runtime.intent_lexicon import expand_query_terms, text_has_any
from runtime.intent_matcher import IntentMatcher


def test_text_has_any_matches_connector_and_docs_synonyms() -> None:
    prose = "Hook up a skinny adapter from the developer portal and stash the results for later."

    assert text_has_any(prose, "connector")
    assert text_has_any(prose, "api docs")
    assert text_has_any(prose, "persist")
    assert not text_has_any(prose, "oauth")


def test_expand_query_terms_promotes_canonical_synonyms() -> None:
    terms = expand_query_terms(
        "Wire up a first-pass adapter after looking through the developer portal and stashing the findings."
    )

    assert "connector" in terms
    assert "integration" in terms
    assert "api" in terms
    assert "docs" in terms
    assert "store" in terms
    assert "record" in terms


def test_intent_matcher_query_expansion_includes_canonical_terms() -> None:
    query = IntentMatcher._to_or_query(
        "Onboard the app by reading the developer portal, then wire up a bridge and QA it."
    )

    assert "connector" in query
    assert "integration" in query
    assert "api" in query
    assert "docs" in query
    assert "test" in query
    assert "verify" in query
