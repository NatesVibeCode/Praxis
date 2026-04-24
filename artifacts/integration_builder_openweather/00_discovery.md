# OpenWeather integration discovery

Input: OpenWeather
Normalized slug: openweather
Source auto-builder run: workflow_562b40c61833

## Prior art

- No existing `Code&DBs/Integrations/manifests/openweather.toml` was present before this patched downstream pass.
- Existing manifest analogue: `Code&DBs/Integrations/manifests/hubspot.toml`.
- Existing zero-code manifest example: `Code&DBs/Integrations/manifests/webhook-example.toml`.
- Generated connector registration path exists through `praxis_connector(action='register')`, which introspects `artifacts/connectors/<slug>/client.py`.

## Verdict

Use a custom connector artifact for the first working iteration.

## Reason

OpenWeather API authentication uses the `appid` query parameter. The declarative manifest handler currently resolves a secret and sends it as an `Authorization: Bearer ...` header, so a manifest-only integration would encode the wrong auth placement.

## Logged blockers

- BUG-AA7CA63D observed in `workflow_562b40c61833`: worker sandbox failed with too many open files before the auto-builder could execute the first job.
- Manifest auth placement gap filed from this run: manifest integrations cannot express query-parameter API key auth such as OpenWeather `appid`.
