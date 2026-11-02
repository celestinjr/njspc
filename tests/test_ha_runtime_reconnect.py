"""End-to-end HA config-entry reload and reconnect foundation tests."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from copy import deepcopy
from typing import Any
from unittest.mock import patch

from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import CONF_HOST, CONF_PORT
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from pynjspc import NjsPCEvent, SocketIOEventsInbound
from pytest_homeassistant.common import MockConfigEntry

from custom_components.njspc.const import DOMAIN

EventCallback = Callable[[NjsPCEvent], Awaitable[None] | None]


def _snapshot(*, on: bool = False) -> dict[str, Any]:
    return {"controller": {"status": "ready"}, "circuits": [{"id": 1, "name": "Pool", "isOn": on}]}


class FakeClient:
    def __init__(self, session: object) -> None:
        self.session = session
        self.connected = False
        self.connection_epoch = 0
        self.snapshot = _snapshot()
        self.fetch_count = 0
        self.close_count = 0
        self.callbacks: dict[SocketIOEventsInbound, EventCallback] = {}

    def subscribe(
        self, event: SocketIOEventsInbound, callback: EventCallback
    ) -> Callable[[], None]:
        self.callbacks[event] = callback

        def remove() -> None:
            self.callbacks.pop(event, None)

        return remove

    async def connect(self) -> None:
        self.connected = True
        self.connection_epoch += 1

    async def fetch_full_state(self, **kwargs: Any) -> dict[str, Any]:
        assert kwargs["timeout"] > 0
        self.fetch_count += 1
        return deepcopy(self.snapshot)

    async def close(self) -> None:
        self.close_count += 1
        self.connected = False

    async def emit(self, event: NjsPCEvent) -> None:
        callback = self.callbacks[SocketIOEventsInbound.CIRCUIT]
        outcome = callback(event)
        if outcome is not None:
            await outcome


class Factory:
    def __init__(self) -> None:
        self.instances: list[FakeClient] = []

    def __call__(self, **kwargs: Any) -> FakeClient:
        client = FakeClient(kwargs["session"])
        self.instances.append(client)
        return client


def _entry() -> MockConfigEntry:
    return MockConfigEntry(
        domain=DOMAIN,
        title="controller.local:4200",
        data={CONF_HOST: "controller.local", CONF_PORT: 4200},
    )


async def test_ha_reload_replaces_runtime_without_resource_leaks(hass: HomeAssistant) -> None:
    """HA reload replaces the client and cancels the old entry-owned monitor."""
    entry = _entry()
    entry.add_to_hass(hass)
    factory = Factory()

    with patch("custom_components.njspc.NjsPCClient", factory):
        assert await hass.config_entries.async_setup(entry.entry_id)
        assert entry.state is ConfigEntryState.LOADED
        first = factory.instances[0]
        first_runtime = entry.runtime_data
        first_task = first_runtime.coordinator._monitor_task
        assert first_task is not None
        assert first.session is async_get_clientsession(hass)

        assert await hass.config_entries.async_reload(entry.entry_id)
        assert entry.state is ConfigEntryState.LOADED
        assert len(factory.instances) == 2
        second = factory.instances[1]
        assert entry.runtime_data is not first_runtime
        assert entry.runtime_data.client is second
        assert entry.runtime_data.coordinator.ready
        assert second.session is first.session
        assert not second.session.closed
        assert first.close_count == 1
        assert first.callbacks == {}
        assert first_task.done()

        second_task = entry.runtime_data.coordinator._monitor_task
        assert second_task is not None
        assert await hass.config_entries.async_unload(entry.entry_id)

    assert second.close_count == 1
    assert second.callbacks == {}
    assert second_task.done()
    assert first.close_count == 1


async def test_ha_reconnect_resync_rejects_stale_events(hass: HomeAssistant) -> None:
    """A loaded HA entry recovers a new epoch via snapshot before state is ready."""
    entry = _entry()
    entry.add_to_hass(hass)
    factory = Factory()

    with patch("custom_components.njspc.NjsPCClient", factory):
        assert await hass.config_entries.async_setup(entry.entry_id)
        runtime = entry.runtime_data
        coordinator = runtime.coordinator
        client = factory.instances[0]
        assert coordinator.ready
        assert client.fetch_count == 1
        assert coordinator.store.connection_epoch == 1

        change = asyncio.Event()
        unsubscribe = coordinator.async_add_listener(change.set)
        try:
            client.connected = False
            await asyncio.wait_for(change.wait(), timeout=3)
            change.clear()
            assert not coordinator.ready
            assert coordinator.store.data["circuits"][1]["isOn"] is False

            ready_again = asyncio.Event()

            def on_update() -> None:
                if coordinator.ready and coordinator.store.connection_epoch == 2:
                    ready_again.set()

            unsub_ready = coordinator.async_add_listener(on_update)
            try:
                client.snapshot = _snapshot(on=True)
                client.connection_epoch += 1
                client.connected = True
                await asyncio.wait_for(ready_again.wait(), timeout=3)
            finally:
                unsub_ready()

            assert coordinator.ready
            assert coordinator.store.connection_epoch == 2
            assert client.fetch_count >= 2
            assert coordinator.store.data["circuits"][1]["isOn"] is True

            await client.emit(
                NjsPCEvent(
                    event="circuit", payload={"id": 1, "isOn": False}, connection_epoch=1
                )
            )
            assert coordinator.store.data["circuits"][1]["isOn"] is True
        finally:
            unsubscribe()
            assert await hass.config_entries.async_unload(entry.entry_id)

    assert client.close_count == 1
    assert client.callbacks == {}
