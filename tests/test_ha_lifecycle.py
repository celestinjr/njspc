"""Home Assistant config-entry lifecycle tests for njsPC."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from copy import deepcopy
from typing import Any
from unittest.mock import patch

from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import CONF_HOST, CONF_PORT, EVENT_HOMEASSISTANT_STOP
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from pynjspc import ConnectionError, NjsPCEvent, SocketIOEventsInbound
from pytest_homeassistant.common import MockConfigEntry

from custom_components.njspc.const import DOMAIN

EventCallback = Callable[[NjsPCEvent], Awaitable[None] | None]


def _snapshot(
    *,
    circuit_name: str = "Pool",
    circuit_state: bool = False,
) -> dict[str, Any]:
    return {
        "controller": {"status": "ready"},
        "temps": {"air": 82.0},
        "circuits": [
            {
                "id": 1,
                "name": circuit_name,
                "isOn": circuit_state,
            }
        ],
        "pumps": [{"id": 50, "rpm": 1800}],
    }


class FakeClient:
    """Minimal deterministic pynjspc client for HA lifecycle tests."""

    def __init__(
        self,
        *,
        session: object,
        snapshot: dict[str, Any],
        connect_error: Exception | None = None,
    ) -> None:
        self.session = session
        self.snapshot = snapshot
        self.connect_error = connect_error
        self.connected = False
        self.connection_epoch = 0
        self.close_count = 0
        self.callbacks: dict[SocketIOEventsInbound, EventCallback] = {}

    def subscribe(
        self,
        event: SocketIOEventsInbound,
        callback: EventCallback,
    ) -> Callable[[], None]:
        self.callbacks[event] = callback
        removed = False

        def unsubscribe() -> None:
            nonlocal removed
            if removed:
                return
            removed = True
            self.callbacks.pop(event, None)

        return unsubscribe

    async def connect(self) -> None:
        if self.connect_error is not None:
            raise self.connect_error

        self.connected = True
        self.connection_epoch += 1

    async def fetch_full_state(self, **kwargs: Any) -> dict[str, Any]:
        assert kwargs["timeout"] > 0
        return deepcopy(self.snapshot)

    async def close(self) -> None:
        self.close_count += 1
        self.connected = False

    async def emit(
        self,
        event_name: str,
        payload: dict[str, Any],
    ) -> None:
        event_type = next(
            event
            for event in SocketIOEventsInbound
            if event.value == event_name
        )
        callback = self.callbacks[event_type]
        result = callback(
            NjsPCEvent(
                event=event_name,
                payload=payload,
                connection_epoch=self.connection_epoch,
            )
        )
        if result is not None:
            await result


class FakeClientFactory:
    """Construct controller-specific fake clients and retain creation evidence."""

    def __init__(
        self,
        snapshots: dict[str, dict[str, Any]],
        *,
        connect_errors: dict[str, Exception] | None = None,
    ) -> None:
        self.snapshots = snapshots
        self.connect_errors = connect_errors or {}
        self.instances: dict[str, FakeClient] = {}
        self.calls: list[dict[str, Any]] = []

    def __call__(self, **kwargs: Any) -> FakeClient:
        host = kwargs["host"]
        self.calls.append(dict(kwargs))

        client = FakeClient(
            session=kwargs["session"],
            snapshot=deepcopy(self.snapshots[host]),
            connect_error=self.connect_errors.get(host),
        )
        self.instances[host] = client
        return client


def _entry(host: str, port: int = 4200) -> MockConfigEntry:
    return MockConfigEntry(
        domain=DOMAIN,
        title=f"{host}:{port}",
        data={
            CONF_HOST: host,
            CONF_PORT: port,
        },
    )


async def test_setup_injects_ha_session_and_unload_cleans_everything(
    hass: HomeAssistant,
) -> None:
    """A loaded entry owns one runtime and unload tears it down exactly once."""

    host = "controller-a.local"
    entry = _entry(host)
    entry.add_to_hass(hass)

    factory = FakeClientFactory({host: _snapshot()})

    with patch("custom_components.njspc.NjsPCClient", factory):
        assert await hass.config_entries.async_setup(entry.entry_id)

        client = factory.instances[host]
        runtime = entry.runtime_data

        assert entry.state is ConfigEntryState.LOADED
        assert runtime.client is client
        assert runtime.coordinator.ready is True
        assert runtime.coordinator.store.data["circuits"][1]["name"] == "Pool"
        assert client.session is async_get_clientsession(hass)
        assert len(client.callbacks) == len(SocketIOEventsInbound)

        assert await hass.config_entries.async_unload(entry.entry_id)

    assert entry.state is ConfigEntryState.NOT_LOADED
    assert client.close_count == 1
    assert client.callbacks == {}
    assert not hasattr(entry, "runtime_data")


async def test_offline_setup_enters_retry_and_releases_partial_runtime(
    hass: HomeAssistant,
) -> None:
    """A startup transport failure becomes SETUP_RETRY without leaked resources."""

    host = "offline.local"
    entry = _entry(host)
    entry.add_to_hass(hass)

    factory = FakeClientFactory(
        {host: _snapshot()},
        connect_errors={host: ConnectionError("offline")},
    )

    with patch("custom_components.njspc.NjsPCClient", factory):
        assert not await hass.config_entries.async_setup(entry.entry_id)

    client = factory.instances[host]

    assert entry.state is ConfigEntryState.SETUP_RETRY
    assert client.close_count == 1
    assert client.callbacks == {}
    assert not hasattr(entry, "runtime_data")

    assert await hass.config_entries.async_unload(entry.entry_id)
    assert entry.state is ConfigEntryState.NOT_LOADED


async def test_home_assistant_stop_closes_client_and_unload_remains_idempotent(
    hass: HomeAssistant,
) -> None:
    """The HA stop event closes runtime resources before normal unload cleanup."""

    host = "controller-stop.local"
    entry = _entry(host)
    entry.add_to_hass(hass)

    factory = FakeClientFactory({host: _snapshot()})

    with patch("custom_components.njspc.NjsPCClient", factory):
        assert await hass.config_entries.async_setup(entry.entry_id)

        client = factory.instances[host]

        hass.bus.async_fire(EVENT_HOMEASSISTANT_STOP)
        await hass.async_block_till_done()

        assert client.close_count == 1
        assert client.callbacks == {}

        assert await hass.config_entries.async_unload(entry.entry_id)

    assert entry.state is ConfigEntryState.NOT_LOADED
    assert client.close_count == 1


async def test_two_controllers_with_overlapping_equipment_ids_remain_isolated(
    hass: HomeAssistant,
) -> None:
    """Equipment IDs are scoped to their config entry/controller."""

    host_a = "controller-a.local"
    host_b = "controller-b.local"

    entry_a = _entry(host_a)
    entry_b = _entry(host_b)

    entry_a.add_to_hass(hass)
    entry_b.add_to_hass(hass)

    factory = FakeClientFactory(
        {
            host_a: _snapshot(
                circuit_name="Pool A",
                circuit_state=False,
            ),
            host_b: _snapshot(
                circuit_name="Pool B",
                circuit_state=True,
            ),
        }
    )

    with patch("custom_components.njspc.NjsPCClient", factory):
        # The first setup loads the integration domain, and Home Assistant then
        # sets up every enabled config entry for that domain in one gather.
        assert await hass.config_entries.async_setup(entry_a.entry_id)

        assert entry_a.state is ConfigEntryState.LOADED
        assert entry_b.state is ConfigEntryState.LOADED
        assert set(factory.instances) == {host_a, host_b}

        runtime_a = entry_a.runtime_data
        runtime_b = entry_b.runtime_data

        assert runtime_a.coordinator.store.data["circuits"][1]["name"] == "Pool A"
        assert runtime_a.coordinator.store.data["circuits"][1]["isOn"] is False
        assert runtime_b.coordinator.store.data["circuits"][1]["name"] == "Pool B"
        assert runtime_b.coordinator.store.data["circuits"][1]["isOn"] is True

        await factory.instances[host_a].emit(
            "circuit",
            {
                "id": 1,
                "isOn": True,
            },
        )

        assert runtime_a.coordinator.store.data["circuits"][1]["isOn"] is True
        assert runtime_b.coordinator.store.data["circuits"][1]["isOn"] is True
        assert runtime_b.coordinator.store.data["circuits"][1]["name"] == "Pool B"

        assert await hass.config_entries.async_unload(entry_a.entry_id)
        assert await hass.config_entries.async_unload(entry_b.entry_id)

    assert factory.instances[host_a].close_count == 1
    assert factory.instances[host_b].close_count == 1
