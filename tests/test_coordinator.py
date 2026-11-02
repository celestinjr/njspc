"""Tests for the push-driven njsPC coordinator lifecycle."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from copy import deepcopy
from typing import Any

from pynjspc import NjsPCEvent, SocketIOEventsInbound

from custom_components.njspc.coordinator import NjspcCoordinator


def _snapshot(*, include_second_circuit: bool = False) -> dict[str, Any]:
    circuits = [{"id": 1, "name": "Pool", "isOn": False}]
    if include_second_circuit:
        circuits.append({"id": 2, "name": "Spa", "isOn": True})
    return {
        "status": {"ready": True},
        "temps": {"air": 82.0},
        "circuits": circuits,
        "pumps": [{"id": 50, "rpm": 1800}],
    }


class FakeClient:
    def __init__(self) -> None:
        self.connected = False
        self.connection_epoch = 0
        self.snapshot = _snapshot()
        self.closed = False
        self.callbacks: dict[
            SocketIOEventsInbound,
            Callable[[NjsPCEvent], Awaitable[None] | None],
        ] = {}

    def subscribe(
        self,
        event: SocketIOEventsInbound,
        callback: Callable[[NjsPCEvent], Awaitable[None] | None],
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
        self.connected = True
        self.connection_epoch += 1

    async def fetch_full_state(self, **kwargs: Any) -> dict[str, Any]:
        if "timeout" in kwargs:
            assert kwargs["timeout"] is None or kwargs["timeout"] > 0
        return deepcopy(self.snapshot)

    async def close(self) -> None:
        self.connected = False
        self.closed = True

    async def emit(
        self,
        event: SocketIOEventsInbound,
        payload: dict[str, Any],
    ) -> None:
        callback = self.callbacks[event]
        result = callback(
            NjsPCEvent(
                event=event.value,
                payload=payload,
                connection_epoch=self.connection_epoch,
            )
        )
        if result is not None:
            await result


class FakeEntry:
    entry_id = "entry-1"

    def __init__(self) -> None:
        self.tasks: list[asyncio.Task[None]] = []

    def async_create_background_task(
        self,
        hass: Any,
        coro: Any,
        *,
        name: str,
        eager_start: bool,
    ) -> asyncio.Task[None]:
        del hass, eager_start
        task = asyncio.create_task(coro, name=name)
        self.tasks.append(task)
        return task


async def _wait_until(predicate: Callable[[], bool], max_wait: float = 1.0) -> None:
    attempts = max(1, int(max_wait / 0.005))
    for _ in range(attempts):
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise TimeoutError("condition did not become true before the test deadline")


async def test_setup_push_update_reconnect_and_close_are_deterministic() -> None:
    client = FakeClient()
    entry = FakeEntry()
    coordinator = NjspcCoordinator(
        object(),
        entry,
        client,  # type: ignore[arg-type]
        monitor_interval=0.01,
        resync_retry_interval=0.01,
    )

    notifications = 0

    def listener() -> None:
        nonlocal notifications
        notifications += 1

    coordinator.async_add_listener(listener)

    await coordinator.async_setup()

    assert coordinator.ready is True
    assert coordinator.store.connection_epoch == 1
    assert len(client.callbacks) == len(SocketIOEventsInbound)

    await client.emit(
        SocketIOEventsInbound.CIRCUIT,
        {"id": 1, "isOn": True},
    )

    assert coordinator.store.data["circuits"][1]["isOn"] is True
    assert notifications >= 2

    client.connected = False
    await _wait_until(lambda: coordinator.ready is False)

    client.snapshot = _snapshot()
    client.snapshot["circuits"][0]["isOn"] = False
    client.connected = True
    client.connection_epoch += 1

    await _wait_until(
        lambda: coordinator.ready
        and coordinator.store.connection_epoch == 2
    )

    assert coordinator.store.data["circuits"][1]["isOn"] is False

    await coordinator.async_close()
    await coordinator.async_close()

    assert client.closed is True
    assert client.callbacks == {}
    assert all(task.done() for task in entry.tasks)


async def test_unknown_inventory_event_converges_via_full_resync() -> None:
    client = FakeClient()
    entry = FakeEntry()
    coordinator = NjspcCoordinator(
        object(),
        entry,
        client,  # type: ignore[arg-type]
        monitor_interval=0.01,
        resync_retry_interval=0.01,
    )

    await coordinator.async_setup()

    client.snapshot = _snapshot(include_second_circuit=True)

    await client.emit(
        SocketIOEventsInbound.CIRCUIT,
        {"id": 2, "isOn": True},
    )

    assert coordinator.ready is False

    await _wait_until(
        lambda: coordinator.ready
        and 2 in coordinator.store.data["circuits"]
    )

    assert coordinator.store.data["circuits"][2]["name"] == "Spa"

    await coordinator.async_close()
