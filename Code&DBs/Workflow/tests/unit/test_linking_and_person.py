"""Tests for entity_linking and person_identity modules."""

from __future__ import annotations

import math

from memory.entity_linking import (
    CoOccurrence,
    CoOccurrenceDiscovery,
    EntityLinker,
    MentionDetection,
)
from memory.person_identity import (
    NameNormalizer,
    PersonExtractor,
    PersonIdentity,
    PersonIdentityResolver,
    PersonMention,
)


# ── MentionDetection dataclass ──────────────────────────────────────

class TestMentionDetection:
    def test_frozen(self):
        m = MentionDetection("e1", "Foo", "foo", 0, 0.9)
        assert m.entity_id == "e1"
        try:
            m.entity_id = "e2"  # type: ignore[misc]
            assert False, "should be frozen"
        except AttributeError:
            pass

    def test_fields(self):
        m = MentionDetection("e1", "Widget", "widget", 5, 0.75)
        assert m.entity_name == "Widget"
        assert m.mention_text == "widget"
        assert m.position == 5
        assert m.confidence == 0.75


# ── EntityLinker ─────────────────────────────────────────────────────

class TestEntityLinker:
    def test_word_boundary_match(self):
        linker = EntityLinker([("e1", "Python")])
        detections = linker.detect_mentions("I love Python programming")
        assert any(d.entity_id == "e1" and d.confidence == 0.9 for d in detections)

    def test_substring_match_lower_confidence(self):
        linker = EntityLinker([("e1", "graph")])
        detections = linker.detect_mentions("The autograph was nice")
        # "graph" inside "autograph" is a substring-only match.
        substring_hits = [d for d in detections if d.confidence < 0.9]
        assert len(substring_hits) >= 1

    def test_case_insensitive(self):
        linker = EntityLinker([("e1", "Redis")])
        detections = linker.detect_mentions("We deployed redis last week")
        assert any(d.entity_id == "e1" for d in detections)

    def test_no_match(self):
        linker = EntityLinker([("e1", "Kubernetes")])
        assert linker.detect_mentions("We use plain Docker") == []

    def test_link_mentions_deduplicates(self):
        linker = EntityLinker([("e1", "API")])
        links = linker.link_mentions("The API serves our API clients")
        ids = [eid for eid, _, _ in links]
        assert "e1" in ids

    def test_multiple_entities(self):
        linker = EntityLinker([("e1", "Alice"), ("e2", "Bob")])
        links = linker.link_mentions("Alice met Bob at the park")
        ids = {eid for eid, _, _ in links}
        assert ids == {"e1", "e2"}


# ── CoOccurrence dataclass ───────────────────────────────────────────

class TestCoOccurrence:
    def test_frozen(self):
        c = CoOccurrence("a", "b", 3, 1.5, ("ctx",))
        assert c.count == 3
        try:
            c.count = 5  # type: ignore[misc]
            assert False, "should be frozen"
        except AttributeError:
            pass


# ── CoOccurrenceDiscovery ────────────────────────────────────────────

class TestCoOccurrenceDiscovery:
    def test_record_and_compute_pmi(self):
        linker = EntityLinker([("e1", "Alice"), ("e2", "Bob")])
        disco = CoOccurrenceDiscovery()
        disco.record("Alice and Bob worked together", linker)
        disco.record("Alice wrote a report", linker)
        disco.record("Bob reviewed it", linker)
        results = disco.compute_pmi(total_documents=3)
        assert len(results) >= 1
        pair = results[0]
        assert {pair.entity_a_id, pair.entity_b_id} == {"e1", "e2"}
        # PMI should be a finite number.
        assert math.isfinite(pair.pmi_score)

    def test_top_pairs_limit(self):
        linker = EntityLinker([("e1", "X"), ("e2", "Y"), ("e3", "Z")])
        disco = CoOccurrenceDiscovery()
        disco.record("X and Y and Z", linker)
        pairs = disco.top_pairs(limit=2)
        assert len(pairs) <= 2

    def test_empty(self):
        disco = CoOccurrenceDiscovery()
        assert disco.top_pairs() == []


# ── NameNormalizer ───────────────────────────────────────────────────

class TestNameNormalizer:
    def test_strip_title(self):
        n = NameNormalizer()
        assert n.normalize("Dr. Jane Smith") == "Jane Smith"
        assert n.normalize("Mr John Doe") == "John Doe"

    def test_strip_trailing_numbers(self):
        n = NameNormalizer()
        assert n.normalize("Alice Cooper123") == "Alice Cooper"

    def test_title_case_and_whitespace(self):
        n = NameNormalizer()
        assert n.normalize("  bob   jones  ") == "Bob Jones"

    def test_slugify(self):
        n = NameNormalizer()
        assert n.slugify("Jane Smith") == "jane-smith"
        assert n.slugify("O'Brien") == "obrien"


# ── PersonExtractor ──────────────────────────────────────────────────

class TestPersonExtractor:
    def test_at_mention(self):
        ext = PersonExtractor()
        mentions = ext.extract("Ping @john.doe for review")
        assert any(m.channel == "mention" for m in mentions)
        assert any("john" in m.slug for m in mentions)

    def test_email_derived(self):
        ext = PersonExtractor()
        mentions = ext.extract("Contact alice.smith@example.com for details")
        assert any(m.channel == "email" for m in mentions)
        assert any(m.normalized_name == "Alice Smith" for m in mentions)

    def test_capitalized_name(self):
        ext = PersonExtractor()
        mentions = ext.extract("Alice Cooper presented the results")
        assert any(m.normalized_name == "Alice Cooper" for m in mentions)

    def test_no_false_positive_on_lowercase(self):
        ext = PersonExtractor()
        mentions = ext.extract("the quick brown fox jumped")
        assert len(mentions) == 0


# ── PersonIdentityResolver ───────────────────────────────────────────

class TestPersonIdentityResolver:
    def test_resolve_by_slug(self):
        r = PersonIdentityResolver()
        r.register("p1", "Alice Smith", aliases=("asmith",))
        mention = PersonMention("alice smith", "Alice Smith", "alice-smith", None, 0.8)
        identity = r.resolve(mention)
        assert identity is not None
        assert identity.person_id == "p1"

    def test_resolve_by_alias(self):
        r = PersonIdentityResolver()
        r.register("p1", "Robert Johnson", aliases=("Bob Johnson",))
        mention = PersonMention("Bob Johnson", "Bob Johnson", "bob-johnson", None, 0.7)
        identity = r.resolve(mention)
        assert identity is not None
        assert identity.person_id == "p1"

    def test_resolve_no_match(self):
        r = PersonIdentityResolver()
        r.register("p1", "Alice Smith")
        mention = PersonMention("Charlie", "Charlie", "charlie", None, 0.5)
        assert r.resolve(mention) is None

    def test_merge(self):
        r = PersonIdentityResolver()
        r.register("p1", "Alice Smith", aliases=("Ali",), channels=("slack",))
        r.register("p2", "A. Smith", aliases=(), channels=("email",))
        merged = r.merge("p1", "p2")
        assert merged.person_id == "p1"
        assert "A. Smith" in merged.aliases
        assert "email" in merged.channels
        assert "slack" in merged.channels
        # p2 should be gone.
        mention = PersonMention("A. Smith", "A. Smith", "a-smith", None, 0.7)
        result = r.resolve(mention)
        # Should resolve to p1 now via alias.
        assert result is not None
        assert result.person_id == "p1"
