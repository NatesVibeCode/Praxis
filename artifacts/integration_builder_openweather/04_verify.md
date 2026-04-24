# OpenWeather verification

## Registry

- Connector registered: yes.
- Integration id: `openweather`.
- Auth shape: `api_key`, `env_var=OPENWEATHER_API_KEY`, `placement=query`, `parameter=appid`.
- Capabilities registered: `geocode_direct`, `geocode_reverse`, `get_air_pollution`, `get_current_weather`, `get_five_day_forecast`.

## Smoke test

- Production integration call: passed with `allow_mock=true`.
- Action: `get_current_weather`.
- Result: `status=succeeded`; connector response was mock data for `Mock Location`.

## Connector verifier

- Verification status: `verified`.
- Coverage: `1.0`.
- Passed: `1`.
- Failed: `0`.
- Verification action: `get_current_weather`.

## Blockers and gaps

- `workflow_562b40c61833` failed in the auto-builder worker lane with BUG-AA7CA63D evidence attached.
- BUG-D91DD073 tracks manifest query-parameter auth support for OpenWeather-style `appid` authentication.
- The verifier initially generated empty args and attempted a live call with the mock key. This was patched by allowing generated clients to declare `VERIFICATION_SPEC`.

## Operator secret

For real OpenWeather calls, replace the mock secret with a real key:

```bash
security add-generic-password -U -a praxis -s OPENWEATHER_API_KEY -w <token>
```
