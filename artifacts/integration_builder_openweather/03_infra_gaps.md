# OpenWeather infrastructure gaps

## Fixed or patched in this iteration

- Patched downstream by generating a custom connector artifact under `artifacts/connectors/openweather`.
- Added mockable validation behavior so the workflow can continue without a live API key.

## Bugs logged

- BUG-AA7CA63D already existed for workflow sandbox file descriptor exhaustion; attached `workflow_562b40c61833` as observed evidence.
- Filed manifest auth placement gap: manifest integrations cannot express query-parameter API key auth such as OpenWeather `appid`.

## Not fixed in runtime

- `runtime.integration_manifest.AuthShape` still lacks explicit auth placement fields.
- `build_manifest_handler` still injects resolved tokens as bearer headers.
- The connector builder template still asks for a manifest-only OpenWeather output even when discovery proves query auth placement is required.
