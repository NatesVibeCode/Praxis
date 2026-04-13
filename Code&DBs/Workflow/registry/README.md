# registry

Owns:

- workspace identity
- config resolution
- path resolution
- resource lookup

Does not own:

- runtime state
- workflow execution
- receipt writing

Nothing else should guess critical paths or configuration.
