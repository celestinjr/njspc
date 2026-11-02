"""Push-driven controller state reconciliation for njsPC."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from enum import Enum
from typing import Any

from pynjspc import NjsPCClient, NjsPCError, NjsPCEvent, SocketIOEventsInbound

from .const import (
    DEFAULT_MONITOR_INTERVAL,
    DEFAULT_REQUEST_TIMEOUT,
    DEFAULT_RESYNC_RETRY_INTERVAL,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)

EquipmentId = int | str
Listener = Callable[[], None]


class StateStoreError(Exception):
    """Raised when controller state cannot be reconciled safely."""


class EventApplyResult(Enum):
    """Outcome of applying one controller event."""

    APPLIED = "applied"
    QUEUED = "queued"
    STALE = "stale"
    RESYNC_REQUIRED = "resync_required"


_COLLECTION_ALIASES: dict[str, tuple[str, ...]] = {
    "bodies": ("bodies",),
    "pumps": ("pumps",),
    "circuits": ("circuits",),
    "features": ("features",),
    "circuit_groups": ("circuitGroups", "circuit_groups"),
    "light_groups": ("lightGroups", "light_groups"),
    "virtual_circuits": ("virtualCircuits", "virtual_circuits"),
    "chlorinators": ("chlorinators",),
    "chem_controllers": ("chemControllers", "chem_controllers"),
    "filters": ("filters",),
    "schedules": ("schedules",),
}

_SINGLETON_ALIASES: dict[str, tuple[str, ...]] = {
    "controller": ("controller",),
    "temperatures": ("temps", "temperatures"),
}

_EVENT_SECTIONS: dict[str, str] = {
    "body": "bodies",
    "pump": "pumps",
    "pumpExt": "pumps",
    "circuit": "circuits",
    "feature": "features",
    "circuitGroup": "circuit_groups",
    "lightGroup": "light_groups",
    "virtualCircuit": "virtual_circuits",
    "chlorinator": "chlorinators",
    "chemController": "chem_controllers",
    "filter": "filters",
    "schedule": "schedules",
    "controller": "controller",
    "temps": "temperatures",
}

_SINGLETON_SECTIONS = frozenset(_SINGLETON_ALIASES)


class ControllerStateStore:
    """Normalized controller-scoped state with connection-epoch protection."""

    def __init__(self) -> None:
        self.connection_epoch = 0
        self.ready = False
        self._data = self._empty_data()
        self._raw_snapshot: dict[str, Any] = {}
        self._pending_events: list[NjsPCEvent] = []

    @staticmethod
    def _empty_data() -> dict[str, Any]:
        data: dict[str, Any] = {
            "controller": {},
            "temperatures": {},
        }
        for section in _COLLECTION_ALIASES:
            data[section] = {}
        return data

    @property
    def data(self) -> dict[str, Any]:
        """Return a defensive copy of normalized state."""

        return deepcopy(self._data)

    @property
    def raw_snapshot(self) -> dict[str, Any]:
        """Return the most recent raw snapshot, including unknown fields."""

        return deepcopy(self._raw_snapshot)

    def begin_epoch(self, epoch: int) -> bool:
        """Begin a newer connection epoch and mark prior state stale."""

        if epoch <= 0:
            raise StateStoreError(f"connection epoch must be positive, got {epoch}")

        if epoch < self.connection_epoch:
            return False

        if epoch == self.connection_epoch:
            return True

        self.connection_epoch = epoch
        self.ready = False
        self._pending_events.clear()
        return True

    def mark_not_ready(self) -> bool:
        """Mark current state stale, returning whether readiness changed."""

        if not self.ready:
            return False
        self.ready = False
        return True

    @staticmethod
    def _first_present(snapshot: Mapping[str, Any], aliases: tuple[str, ...]) -> Any:
        for alias in aliases:
            if alias in snapshot:
                return snapshot[alias]
        return None

    @staticmethod
    def _normalize_mapping(value: Any, section: str) -> dict[str, Any]:
        if value is None:
            return {}
        if not isinstance(value, Mapping):
            raise StateStoreError(f"{section} must be a mapping")
        return deepcopy(dict(value))

    @staticmethod
    def _normalize_collection(value: Any, section: str) -> dict[EquipmentId, dict[str, Any]]:
        if value is None:
            return {}

        if isinstance(value, Mapping):
            normalized: dict[EquipmentId, dict[str, Any]] = {}
            for key, record in value.items():
                if not isinstance(key, (int, str)):
                    raise StateStoreError(f"{section} contains an unsupported key")
                if not isinstance(record, Mapping):
                    raise StateStoreError(f"{section}[{key!r}] must be a mapping")
                normalized[key] = deepcopy(dict(record))
            return normalized

        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            normalized = {}
            for record in value:
                if not isinstance(record, Mapping):
                    raise StateStoreError(f"{section} contains a non-mapping record")
                equipment_id = record.get("id")
                if not isinstance(equipment_id, (int, str)):
                    raise StateStoreError(f"{section} record is missing a usable id")
                normalized[equipment_id] = deepcopy(dict(record))
            return normalized

        raise StateStoreError(f"{section} must be a mapping or sequence")

    @classmethod
    def _normalize_snapshot(cls, snapshot: Mapping[str, Any]) -> dict[str, Any]:
        normalized = cls._empty_data()

        for section, aliases in _SINGLETON_ALIASES.items():
            normalized[section] = cls._normalize_mapping(
                cls._first_present(snapshot, aliases),
                section,
            )

        for section, aliases in _COLLECTION_ALIASES.items():
            normalized[section] = cls._normalize_collection(
                cls._first_present(snapshot, aliases),
                section,
            )

        return normalized

    def apply_snapshot(self, snapshot: Mapping[str, Any], epoch: int) -> bool:
        """Apply a full snapshot.

        Return True when a second bounded resync is required because a buffered
        event could not be merged safely.
        """

        if epoch < self.connection_epoch:
            return True

        if epoch > self.connection_epoch:
            self.begin_epoch(epoch)
        elif epoch <= 0:
            raise StateStoreError(f"connection epoch must be positive, got {epoch}")

        self._data = self._normalize_snapshot(snapshot)
        self._raw_snapshot = deepcopy(dict(snapshot))
        self.ready = True

        pending = self._pending_events
        self._pending_events = []

        for event in pending:
            result = self._apply_current_event(event)
            if result is EventApplyResult.RESYNC_REQUIRED:
                self.ready = False
                self._pending_events.clear()
                return True

        return False

    def apply_event(self, event: NjsPCEvent) -> EventApplyResult:
        """Apply, queue, or reject an event using its connection epoch."""

        if event.connection_epoch < self.connection_epoch:
            return EventApplyResult.STALE

        if event.connection_epoch > self.connection_epoch:
            self.begin_epoch(event.connection_epoch)
            self._pending_events.append(event)
            return EventApplyResult.QUEUED

        if not self.ready:
            self._pending_events.append(event)
            return EventApplyResult.QUEUED

        result = self._apply_current_event(event)
        if result is EventApplyResult.RESYNC_REQUIRED:
            self.ready = False
        return result

    def _apply_current_event(self, event: NjsPCEvent) -> EventApplyResult:
        section = _EVENT_SECTIONS.get(event.event)
        if section is None:
            return EventApplyResult.RESYNC_REQUIRED

        payload = event.payload
        if not isinstance(payload, Mapping):
            return EventApplyResult.RESYNC_REQUIRED

        if section in _SINGLETON_SECTIONS:
            current = self._data[section]
            if not isinstance(current, dict):
                return EventApplyResult.RESYNC_REQUIRED
            current.update(deepcopy(dict(payload)))
            return EventApplyResult.APPLIED

        equipment_id = payload.get("id")
        if not isinstance(equipment_id, (int, str)):
            return EventApplyResult.RESYNC_REQUIRED

        collection = self._data[section]
        if not isinstance(collection, dict):
            return EventApplyResult.RESYNC_REQUIRED

        current_record = collection.get(equipment_id)
        if not isinstance(current_record, dict):
            return EventApplyResult.RESYNC_REQUIRED

        current_record.update(deepcopy(dict(payload)))
        return EventApplyResult.APPLIED


class NjspcCoordinator:
    """Own subscriptions and converge snapshots plus push events."""

    def __init__(
        self,
        hass: Any,
        entry: Any,
        client: NjsPCClient,
        *,
        monitor_interval: float = DEFAULT_MONITOR_INTERVAL,
        resync_retry_interval: float = DEFAULT_RESYNC_RETRY_INTERVAL,
    ) -> None:
        self.hass = hass
        self.entry = entry
        self.client = client
        self.store = ControllerStateStore()
        self._monitor_interval = monitor_interval
        self._resync_retry_interval = resync_retry_interval
        self._listeners: set[Listener] = set()
        self._unsubscribers: list[Callable[[], None]] = []
        self._monitor_task: asyncio.Task[None] | None = None
        self._resync_lock = asyncio.Lock()
        self._closed = False

    @property
    def ready(self) -> bool:
        """Return whether state is coherent for the current connection epoch."""

        return self.store.ready

    def async_add_listener(self, listener: Listener) -> Callable[[], None]:
        """Register a state-change listener and return an idempotent remover."""

        self._listeners.add(listener)
        removed = False

        def remove_listener() -> None:
            nonlocal removed
            if removed:
                return
            removed = True
            self._listeners.discard(listener)

        return remove_listener

    def _notify_listeners(self) -> None:
        for listener in tuple(self._listeners):
            try:
                listener()
            except Exception:
                _LOGGER.exception("njsPC coordinator listener failed")

    async def async_setup(self) -> None:
        """Subscribe, connect, establish a snapshot, and start epoch monitoring."""

        if self._closed:
            raise StateStoreError("coordinator is closed")

        for event in SocketIOEventsInbound:
            self._unsubscribers.append(
                self.client.subscribe(event, self._async_handle_event)
            )

        try:
            await self.client.connect()
            self.store.begin_epoch(self.client.connection_epoch)
            await self.async_resync()
        except BaseException:
            await self.async_close()
            raise

        self._monitor_task = self.entry.async_create_background_task(
            self.hass,
            self._async_monitor_connection(),
            name=f"{DOMAIN} connection monitor {self.entry.entry_id}",
            eager_start=True,
        )

    async def async_resync(self) -> None:
        """Perform a bounded full-state convergence for the current epoch."""

        async with self._resync_lock:
            for attempt in range(2):
                if self._closed:
                    raise StateStoreError("coordinator is closed")
                if not self.client.connected:
                    raise StateStoreError("controller transport is disconnected")

                epoch_before = self.client.connection_epoch
                snapshot = await self.client.fetch_full_state(
                    timeout=DEFAULT_REQUEST_TIMEOUT
                )
                epoch_after = self.client.connection_epoch

                if (
                    not self.client.connected
                    or epoch_before <= 0
                    or epoch_after != epoch_before
                ):
                    if epoch_after > self.store.connection_epoch:
                        self.store.begin_epoch(epoch_after)
                    continue

                needs_resync = self.store.apply_snapshot(snapshot, epoch_after)
                if not needs_resync:
                    self._notify_listeners()
                    return

                _LOGGER.debug(
                    "Repeating njsPC snapshot after ambiguous buffered event "
                    "(attempt %s)",
                    attempt + 1,
                )

            raise StateStoreError("controller state did not converge after bounded resync")

    async def _async_handle_event(self, event: NjsPCEvent) -> None:
        result = self.store.apply_event(event)

        if result is EventApplyResult.APPLIED:
            self._notify_listeners()
            return

        if result is EventApplyResult.STALE:
            return

        # QUEUED means a baseline is not ready. RESYNC_REQUIRED means an event
        # could not be safely merged. In both cases the monitor performs the
        # bounded full resync rather than inventing state.
        self._notify_listeners()

    async def _async_monitor_connection(self) -> None:
        loop = asyncio.get_running_loop()
        next_resync_at = 0.0

        try:
            while not self._closed:
                await asyncio.sleep(self._monitor_interval)

                if not self.client.connected:
                    if self.store.mark_not_ready():
                        self._notify_listeners()
                    continue

                epoch = self.client.connection_epoch
                if epoch > self.store.connection_epoch:
                    self.store.begin_epoch(epoch)
                    self._notify_listeners()

                if self.store.ready:
                    continue

                now = loop.time()
                if now < next_resync_at:
                    continue

                try:
                    await self.async_resync()
                except (NjsPCError, StateStoreError):
                    _LOGGER.debug(
                        "njsPC state resync failed; will retry",
                        exc_info=True,
                    )
                    next_resync_at = now + self._resync_retry_interval
                else:
                    next_resync_at = 0.0
        except asyncio.CancelledError:
            raise

    async def async_close(self) -> None:
        """Idempotently stop integration-owned work and close the client."""

        if self._closed:
            return

        self._closed = True

        for unsubscribe in self._unsubscribers:
            unsubscribe()
        self._unsubscribers.clear()
        self._listeners.clear()

        monitor_task = self._monitor_task
        self._monitor_task = None

        if monitor_task is not None and monitor_task is not asyncio.current_task():
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

        await self.client.close()
