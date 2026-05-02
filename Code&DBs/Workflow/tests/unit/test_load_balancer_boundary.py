from __future__ import annotations

from typing import Any

from runtime.load_balancer import GlobalLoadBalancer


class _FakeConn:
    def __init__(self) -> None:
        self.closed = False

    async def execute(self, _query: str, *_args: Any) -> Any:
        return "OK"

    async def close(self) -> None:
        self.closed = True


class _FakeProviderConcurrencyRepository:
    def __init__(self, *, acquire_results: list[bool] | None = None) -> None:
        self.acquire_results = list(acquire_results or [True])
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def ensure_schema(self, conn) -> None:
        self.calls.append(("ensure_schema", {"conn": conn}))

    async def ensure_provider(self, conn, *, provider_slug: str) -> None:
        self.calls.append(("ensure_provider", {"conn": conn, "provider_slug": provider_slug}))

    async def ensure_default_providers(self, conn) -> None:
        self.calls.append(("ensure_default_providers", {"conn": conn}))

    async def reap_stale_slots(self, conn, *, provider_slug: str, stale_after_s: float) -> None:
        self.calls.append(
            (
                "reap_stale_slots",
                {
                    "conn": conn,
                    "provider_slug": provider_slug,
                    "stale_after_s": stale_after_s,
                },
            )
        )

    async def try_acquire_slot(self, conn, *, provider_slug: str, cost_weight: float) -> bool:
        self.calls.append(
            (
                "try_acquire_slot",
                {
                    "conn": conn,
                    "provider_slug": provider_slug,
                    "cost_weight": cost_weight,
                },
            )
        )
        return self.acquire_results.pop(0)

    async def release_slot(self, conn, *, provider_slug: str, cost_weight: float) -> None:
        self.calls.append(
            (
                "release_slot",
                {
                    "conn": conn,
                    "provider_slug": provider_slug,
                    "cost_weight": cost_weight,
                },
            )
        )

    async def fetch_slot_status(self, conn) -> dict[str, dict[str, float | int | str]]:
        self.calls.append(("fetch_slot_status", {"conn": conn}))
        return {
            "openai": {
                "provider_slug": "openai",
                "max_concurrent": 4,
                "active_slots": 1.5,
                "cost_weight_default": 1.0,
            }
        }

    async def has_capacity(self, conn, *, provider_slug: str) -> bool:
        self.calls.append(("has_capacity", {"conn": conn, "provider_slug": provider_slug}))
        return False


def test_load_balancer_delegates_bootstrap_and_slot_acquire_to_repository() -> None:
    conn = _FakeConn()
    repository = _FakeProviderConcurrencyRepository(acquire_results=[True])
    balancer = GlobalLoadBalancer(
        "postgresql://example.test/workflow",
        repository=repository,
    )

    async def _connect():
        return conn

    balancer._connect = _connect  # type: ignore[method-assign]

    assert balancer.acquire_slot("openai", cost_weight=2.0, timeout_s=0.1) is True
    assert conn.closed is True
    assert [name for name, _payload in repository.calls] == [
        "ensure_schema",
        "ensure_provider",
        "reap_stale_slots",
        "try_acquire_slot",
    ]


def test_load_balancer_delegates_status_release_and_capacity_queries() -> None:
    conn = _FakeConn()
    repository = _FakeProviderConcurrencyRepository(acquire_results=[True])
    balancer = GlobalLoadBalancer(
        "postgresql://example.test/workflow",
        repository=repository,
    )

    async def _connect():
        return conn

    balancer._connect = _connect  # type: ignore[method-assign]

    status = balancer.slot_status()
    balancer.release_slot("openai", cost_weight=1.0)
    capacity = balancer.has_capacity("openai")

    assert status["openai"].provider_slug == "openai"
    assert status["openai"].current_active == 1.5
    assert capacity is False
    assert [name for name, _payload in repository.calls] == [
        "ensure_schema",
        "ensure_default_providers",
        "fetch_slot_status",
        "ensure_schema",
        "release_slot",
        "ensure_schema",
        "ensure_provider",
        "has_capacity",
    ]


def test_load_balancer_fails_closed_on_db_connection_capacity_error() -> None:
    repository = _FakeProviderConcurrencyRepository(acquire_results=[True])
    balancer = GlobalLoadBalancer(
        "postgresql://example.test/workflow",
        repository=repository,
    )

    async def _connect():
        raise RuntimeError("remaining connection slots are reserved for roles with the SUPERUSER attribute")

    balancer._connect = _connect  # type: ignore[method-assign]

    assert balancer.acquire_slot("openai", cost_weight=1.0, timeout_s=0.1) is False


def test_load_balancer_still_degrades_open_on_generic_db_error() -> None:
    repository = _FakeProviderConcurrencyRepository(acquire_results=[True])
    balancer = GlobalLoadBalancer(
        "postgresql://example.test/workflow",
        repository=repository,
    )

    async def _connect():
        raise RuntimeError("database temporarily unreachable")

    balancer._connect = _connect  # type: ignore[method-assign]

    assert balancer.acquire_slot("openai", cost_weight=1.0, timeout_s=0.1) is True
