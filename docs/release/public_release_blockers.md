# Public Release Blockers

These are private-operator policies intentionally kept on Nate's branch that must be removed or moved behind registry/profile authority before a public build.

- Anthropic direct API is blocked for Nate because this local registry uses Claude through the CLI/OAuth subscription lane. Public Praxis must support users who provide `ANTHROPIC_API_KEY`.
- The source marker is `PUBLIC_RELEASE_REMOVE`; `tests/unit/test_public_release_markers.py` fails if a marker is added or removed without updating the allowlist and reason.
