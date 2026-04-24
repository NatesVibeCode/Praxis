# OpenWeather MVP scope

## Selected actions

| Action | Method | URL | Body template | Notes |
| --- | --- | --- | --- | --- |
| get_current_weather | GET | `https://api.openweathermap.org/data/2.5/weather` | no | Smallest useful read path; supports `lat`, `lon`, `units`, and `lang`. |
| get_five_day_forecast | GET | `https://api.openweathermap.org/data/2.5/forecast` | no | Useful forecast action with the same auth shape. |
| geocode_direct | GET | `https://api.openweathermap.org/geo/1.0/direct` | no | Converts app/domain user text into coordinates for weather calls. |
| geocode_reverse | GET | `https://api.openweathermap.org/geo/1.0/reverse` | no | Converts coordinates back into a named place. |
| get_air_pollution | GET | `https://api.openweathermap.org/data/2.5/air_pollution` | no | Adds a distinct data model while preserving the same simple auth. |

## Out of scope - follow-up

- One Call 3.0 is useful but plan-gated; leave it for a second pass after the connector proves auth and basic request construction.
- Weather map tile paths need path parameter interpolation and binary response handling.
- Manifest-only build is out of scope until query-parameter auth placement exists.

## Downstream contract

- Generate `artifacts/connectors/openweather/client.py`.
- Include `OPENWEATHER_API_KEY` handling.
- Allow `allow_mock=true` so validation continues when no key is installed.
- Register through `praxis_connector(action='register', app_slug='openweather')`.
