from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class PersonMention:
    raw_name: str
    normalized_name: str
    slug: str
    channel: str | None
    confidence: float


@dataclass(frozen=True)
class PersonIdentity:
    person_id: str
    canonical_name: str
    aliases: tuple[str, ...]
    channels: tuple[str, ...]


_TITLE_PATTERN = re.compile(
    r"^(Mr\.?|Mrs\.?|Ms\.?|Dr\.?|Prof\.?)\s+", re.IGNORECASE
)
_TRAILING_NUMBERS = re.compile(r"\d+$")
_EXTRA_WHITESPACE = re.compile(r"\s+")
_SLUG_DISALLOWED = re.compile(r"[^a-z0-9\-]")

# Person-name patterns.
_AT_MENTION = re.compile(r"@([A-Za-z][A-Za-z0-9_.-]+)")
_EMAIL_NAME = re.compile(r"([a-zA-Z][a-zA-Z0-9_.+-]+)@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
_CAP_NAME = re.compile(
    r"\b([A-Z][a-z]{1,}(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]{1,})\b"
)


class NameNormalizer:
    """Normalize and slugify person names."""

    def normalize(self, raw_name: str) -> str:
        name = raw_name.strip()
        name = _TITLE_PATTERN.sub("", name)
        name = _TRAILING_NUMBERS.sub("", name).strip()
        name = _EXTRA_WHITESPACE.sub(" ", name)
        return name.title()

    def slugify(self, name: str) -> str:
        s = name.lower().strip()
        s = s.replace(" ", "-")
        s = _SLUG_DISALLOWED.sub("", s)
        # Collapse multiple hyphens.
        s = re.sub(r"-{2,}", "-", s)
        return s.strip("-")


class PersonExtractor:
    """Extract person mentions from text."""

    def __init__(self) -> None:
        self._normalizer = NameNormalizer()

    def extract(self, text: str) -> list[PersonMention]:
        mentions: list[PersonMention] = []
        seen_slugs: set[str] = set()

        # 1. @mentions
        for m in _AT_MENTION.finditer(text):
            raw = m.group(1)
            norm = self._normalizer.normalize(raw.replace(".", " ").replace("_", " "))
            slug = self._normalizer.slugify(norm)
            if slug and slug not in seen_slugs:
                seen_slugs.add(slug)
                mentions.append(
                    PersonMention(
                        raw_name=raw,
                        normalized_name=norm,
                        slug=slug,
                        channel="mention",
                        confidence=0.8,
                    )
                )

        # 2. Email-derived names
        for m in _EMAIL_NAME.finditer(text):
            local = m.group(1)
            # Convert first.last to First Last
            parts = re.split(r"[._]", local)
            if len(parts) >= 2:
                name_parts = [p.capitalize() for p in parts if p]
                raw = m.group(0)
                norm = self._normalizer.normalize(" ".join(name_parts))
                slug = self._normalizer.slugify(norm)
                if slug and slug not in seen_slugs:
                    seen_slugs.add(slug)
                    mentions.append(
                        PersonMention(
                            raw_name=raw,
                            normalized_name=norm,
                            slug=slug,
                            channel="email",
                            confidence=0.7,
                        )
                    )

        # 3. Capitalized name patterns (First Last, First M. Last)
        for m in _CAP_NAME.finditer(text):
            raw = m.group(1)
            norm = self._normalizer.normalize(raw)
            slug = self._normalizer.slugify(norm)
            if slug and slug not in seen_slugs:
                seen_slugs.add(slug)
                mentions.append(
                    PersonMention(
                        raw_name=raw,
                        normalized_name=norm,
                        slug=slug,
                        channel=None,
                        confidence=0.6,
                    )
                )

        return mentions


class PersonIdentityResolver:
    """Resolve person mentions to known identities."""

    def __init__(self) -> None:
        self._identities: dict[str, PersonIdentity] = {}
        self._normalizer = NameNormalizer()

    def register(
        self,
        person_id: str,
        canonical_name: str,
        aliases: tuple[str, ...] | list[str] = (),
        channels: tuple[str, ...] | list[str] = (),
    ) -> None:
        self._identities[person_id] = PersonIdentity(
            person_id=person_id,
            canonical_name=canonical_name,
            aliases=tuple(aliases),
            channels=tuple(channels),
        )

    def resolve(self, mention: PersonMention) -> PersonIdentity | None:
        """Match by slug, alias, or fuzzy name."""
        mention_slug = mention.slug
        mention_norm = mention.normalized_name.lower()

        for identity in self._identities.values():
            # Slug match against canonical name.
            canon_slug = self._normalizer.slugify(identity.canonical_name)
            if mention_slug == canon_slug:
                return identity

            # Slug match against aliases.
            for alias in identity.aliases:
                alias_slug = self._normalizer.slugify(alias)
                if mention_slug == alias_slug:
                    return identity

            # Fuzzy: normalized name substring or containment.
            canon_lower = identity.canonical_name.lower()
            if mention_norm == canon_lower:
                return identity
            for alias in identity.aliases:
                if mention_norm == alias.lower():
                    return identity

        return None

    def merge(self, id_a: str, id_b: str) -> PersonIdentity:
        """Merge two identities, keeping all aliases and channels.

        The first id (*id_a*) becomes the surviving identity.  *id_b* is
        removed and its canonical name becomes an alias.
        """
        a = self._identities[id_a]
        b = self._identities[id_b]

        all_aliases = set(a.aliases) | set(b.aliases)
        all_aliases.add(b.canonical_name)
        all_aliases.discard(a.canonical_name)

        all_channels = set(a.channels) | set(b.channels)

        merged = PersonIdentity(
            person_id=id_a,
            canonical_name=a.canonical_name,
            aliases=tuple(sorted(all_aliases)),
            channels=tuple(sorted(all_channels)),
        )
        self._identities[id_a] = merged
        del self._identities[id_b]
        return merged
