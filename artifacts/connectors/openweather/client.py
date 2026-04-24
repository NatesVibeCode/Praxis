"""Generated OpenWeather API connector.

The first iteration is intentionally small: real HTTP when an API key is
available, deterministic mock data when validation must continue without one.
"""

from __future__ import annotations

import json
import os
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class OpenWeatherClient:
    """Client for OpenWeather's query-key APIs."""

    AUTH_SHAPE = {
        "kind": "api_key",
        "env_var": "OPENWEATHER_API_KEY",
        "placement": "query",
        "parameter": "appid",
    }
    VERIFICATION_SPEC = [
        {
            "action": "get_current_weather",
            "args": {"lat": 37.7749, "lon": -122.4194, "units": "metric", "allow_mock": True},
            "expect": {"status": "succeeded", "data_type": "dict", "data_keys": ["status", "source", "response"]},
            "description": "Mock-safe current weather smoke.",
            "skip": False,
        }
    ]

    def __init__(self, base_url: str | None = None) -> None:
        self.base_url = (base_url or "https://api.openweathermap.org").rstrip("/")

    def get_current_weather(
        self,
        lat: float = 37.7749,
        lon: float = -122.4194,
        units: str = "metric",
        lang: str = "en",
        allow_mock: bool = False,
    ) -> dict[str, Any]:
        """Fetch current weather by coordinates."""
        return self._get(
            "/data/2.5/weather",
            {"lat": lat, "lon": lon, "units": units, "lang": lang},
            allow_mock=allow_mock,
            mock_payload={
                "coord": {"lat": lat, "lon": lon},
                "weather": [{"main": "Clear", "description": "mock clear sky"}],
                "main": {"temp": 18.4, "humidity": 62},
                "name": "Mock Location",
            },
        )

    def get_five_day_forecast(
        self,
        lat: float = 37.7749,
        lon: float = -122.4194,
        units: str = "metric",
        allow_mock: bool = False,
    ) -> dict[str, Any]:
        """Fetch the five day forecast by coordinates."""
        return self._get(
            "/data/2.5/forecast",
            {"lat": lat, "lon": lon, "units": units},
            allow_mock=allow_mock,
            mock_payload={
                "city": {"name": "Mock Location", "coord": {"lat": lat, "lon": lon}},
                "list": [{"dt": 0, "main": {"temp": 18.4}, "weather": [{"main": "Clear"}]}],
            },
        )

    def geocode_direct(
        self,
        q: str = "San Francisco,US",
        limit: int = 1,
        allow_mock: bool = False,
    ) -> dict[str, Any]:
        """Resolve a location name to coordinates."""
        return self._get(
            "/geo/1.0/direct",
            {"q": q, "limit": limit},
            allow_mock=allow_mock,
            mock_payload=[{"name": "San Francisco", "lat": 37.7749, "lon": -122.4194, "country": "US"}],
        )

    def geocode_reverse(
        self,
        lat: float = 37.7749,
        lon: float = -122.4194,
        limit: int = 1,
        allow_mock: bool = False,
    ) -> dict[str, Any]:
        """Resolve coordinates to a location name."""
        return self._get(
            "/geo/1.0/reverse",
            {"lat": lat, "lon": lon, "limit": limit},
            allow_mock=allow_mock,
            mock_payload=[{"name": "San Francisco", "lat": lat, "lon": lon, "country": "US"}],
        )

    def get_air_pollution(
        self,
        lat: float = 37.7749,
        lon: float = -122.4194,
        allow_mock: bool = False,
    ) -> dict[str, Any]:
        """Fetch air pollution data by coordinates."""
        return self._get(
            "/data/2.5/air_pollution",
            {"lat": lat, "lon": lon},
            allow_mock=allow_mock,
            mock_payload={"coord": {"lat": lat, "lon": lon}, "list": [{"main": {"aqi": 1}, "components": {}}]},
        )

    def _get(
        self,
        path: str,
        params: dict[str, Any],
        *,
        allow_mock: bool,
        mock_payload: Any,
    ) -> dict[str, Any]:
        if allow_mock or os.environ.get("PRAXIS_INTEGRATION_MOCKS") == "1":
            return {"status": "mocked", "source": "openweather", "response": mock_payload}

        api_key = self._api_key()
        if not api_key:
            raise RuntimeError("OPENWEATHER_API_KEY is required unless allow_mock=true")

        query = {k: v for k, v in params.items() if v is not None}
        query["appid"] = api_key
        url = f"{self.base_url}{path}?{urlencode(query)}"
        request = Request(url, headers={"Accept": "application/json"})
        with urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
        return {"status": "succeeded", "source": "openweather", "response": json.loads(body)}

    def _api_key(self) -> str:
        env_value = os.environ.get("OPENWEATHER_API_KEY", "").strip()
        if env_value:
            return env_value
        try:
            from adapters.keychain import resolve_secret

            return (resolve_secret("OPENWEATHER_API_KEY") or "").strip()
        except Exception:
            return ""
