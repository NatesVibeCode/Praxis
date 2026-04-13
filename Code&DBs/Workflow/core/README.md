# core

Owns:

- domain model
- workflow primitives
- graph concepts
- shared domain logic with minimal IO

Does not own:

- CLI behavior
- API behavior
- raw database access
- environment lookup

Keep this folder small, reusable, and easy to test.
