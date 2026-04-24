# OpenWeather API contract

Sources:

- https://openweathermap.org/api
- https://openweathermap.org/current
- https://openweathermap.org/forecast5
- https://openweathermap.org/api/one-call-3
- https://openweathermap.org/api/geocoding-api
- https://openweathermap.org/appid

## Auth

- Mode: API key.
- Placement: query parameter.
- Parameter name: `appid`.
- Secret name: `OPENWEATHER_API_KEY`.
- Scopes: none in the OAuth sense; product access depends on the OpenWeather account plan.

## Base URLs

- Weather API 2.5: `https://api.openweathermap.org/data/2.5`
- One Call API 3.0: `https://api.openweathermap.org/data/3.0`
- Geocoding API: `https://api.openweathermap.org/geo/1.0`
- Weather map tiles: `https://tile.openweathermap.org/map`

## Endpoint inventory

| Method | Action slug | URL | Body | Description |
| --- | --- | --- | --- | --- |
| GET | get_current_weather | `https://api.openweathermap.org/data/2.5/weather` | n/a | Current weather by coordinates, city, ZIP, or city id. |
| GET | get_five_day_forecast | `https://api.openweathermap.org/data/2.5/forecast` | n/a | Five day / three hour forecast. |
| GET | get_one_call_weather | `https://api.openweathermap.org/data/3.0/onecall` | n/a | Current, minutely, hourly, daily, and alerts by coordinates. |
| GET | get_one_call_timemachine | `https://api.openweathermap.org/data/3.0/onecall/timemachine` | n/a | Historical weather data for a timestamp by coordinates. |
| GET | get_daily_aggregation | `https://api.openweathermap.org/data/3.0/onecall/day_summary` | n/a | Daily aggregation by date and coordinates. |
| GET | get_weather_overview | `https://api.openweathermap.org/data/3.0/onecall/overview` | n/a | Human-readable weather overview by coordinates and date. |
| GET | geocode_direct | `https://api.openweathermap.org/geo/1.0/direct` | n/a | Geocode a location name to coordinates. |
| GET | geocode_reverse | `https://api.openweathermap.org/geo/1.0/reverse` | n/a | Reverse geocode coordinates to place names. |
| GET | get_air_pollution | `https://api.openweathermap.org/data/2.5/air_pollution` | n/a | Air pollution data by coordinates. |
| GET | get_weather_map_tile | `https://tile.openweathermap.org/map/{layer}/{z}/{x}/{y}.png` | n/a | Weather map tile image for a layer and tile coordinates. |

## Structured details

Required common fields:

- `lat`: latitude for coordinate-based endpoints.
- `lon`: longitude for coordinate-based endpoints.
- `appid`: OpenWeather API key, injected from `OPENWEATHER_API_KEY`.
- `units`: optional, one of `standard`, `metric`, `imperial`.
- `lang`: optional language code.

Traceability:

- Auth placement and `appid` are from OpenWeather API call examples and API key guide.
- Current weather and forecast endpoints are from OpenWeather current weather and forecast docs.
- One Call endpoints are from OpenWeather One Call docs.
- Geocoding endpoints are from OpenWeather geocoding docs.

## Gaps

- Declarative manifest auth cannot place the resolved secret into query parameter `appid`.
- Manifest capability paths cannot safely interpolate path segments for map tiles.
- Real validation requires an operator-owned OpenWeather key; early iteration should continue with a mock response when the key is absent.
