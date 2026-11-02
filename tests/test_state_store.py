"""Tests for controller-scoped state reconciliation."""

from copy import deepcopy

from pynjspc import NjsPCEvent

from custom_components.njspc.coordinator import (
    ControllerStateStore,
    EventApplyResult,
)


def _snapshot() -> dict:
    return {
        "status": {"ready": True},
        "temps": {"air": 82.0, "solar": 90.0},
        "circuits": [
            {"id": 1, "name": "Pool", "isOn": False, "future": "keep-me"},
        ],
        "pumps": [
            {"id": 50, "name": "Filter Pump", "rpm": 1800},
        ],
        "filters": [
            {"id": 1, "pressure": 12.4},
        ],
        "futureTopLevel": {"preserve": True},
    }


def test_snapshot_normalizes_known_sections_and_preserves_raw_unknown_fields() -> None:
    store = ControllerStateStore()
    snapshot = _snapshot()

    assert store.apply_snapshot(snapshot, epoch=1) is False
    assert store.ready is True
    assert store.connection_epoch == 1
    assert store.data["circuits"][1]["name"] == "Pool"
    assert store.data["pumps"][50]["rpm"] == 1800
    assert store.data["temperatures"]["air"] == 82.0
    assert store.raw_snapshot["futureTopLevel"] == {"preserve": True}

    snapshot["circuits"][0]["name"] = "mutated externally"
    assert store.data["circuits"][1]["name"] == "Pool"


def test_partial_equipment_event_merges_without_losing_existing_fields() -> None:
    store = ControllerStateStore()
    store.apply_snapshot(_snapshot(), epoch=1)

    result = store.apply_event(
        NjsPCEvent(
            event="circuit",
            payload={"id": 1, "isOn": True},
            connection_epoch=1,
        )
    )

    assert result is EventApplyResult.APPLIED
    assert store.data["circuits"][1] == {
        "id": 1,
        "name": "Pool",
        "isOn": True,
        "future": "keep-me",
    }


def test_stale_prior_epoch_event_is_ignored() -> None:
    store = ControllerStateStore()
    store.apply_snapshot(_snapshot(), epoch=2)
    before = deepcopy(store.data)

    result = store.apply_event(
        NjsPCEvent(
            event="circuit",
            payload={"id": 1, "isOn": True},
            connection_epoch=1,
        )
    )

    assert result is EventApplyResult.STALE
    assert store.data == before
    assert store.ready is True


def test_future_epoch_event_is_buffered_then_replayed_over_new_snapshot() -> None:
    store = ControllerStateStore()
    store.apply_snapshot(_snapshot(), epoch=1)

    result = store.apply_event(
        NjsPCEvent(
            event="circuit",
            payload={"id": 1, "isOn": True},
            connection_epoch=2,
        )
    )

    assert result is EventApplyResult.QUEUED
    assert store.connection_epoch == 2
    assert store.ready is False

    snapshot = _snapshot()
    snapshot["circuits"][0]["isOn"] = False

    assert store.apply_snapshot(snapshot, epoch=2) is False
    assert store.ready is True
    assert store.data["circuits"][1]["isOn"] is True


def test_unknown_equipment_event_requires_resync_instead_of_inventing_inventory() -> None:
    store = ControllerStateStore()
    store.apply_snapshot(_snapshot(), epoch=1)

    result = store.apply_event(
        NjsPCEvent(
            event="circuit",
            payload={"id": 99, "isOn": True},
            connection_epoch=1,
        )
    )

    assert result is EventApplyResult.RESYNC_REQUIRED
    assert store.ready is False
    assert 99 not in store.data["circuits"]


def test_singleton_temperature_event_merges_fields() -> None:
    store = ControllerStateStore()
    store.apply_snapshot(_snapshot(), epoch=1)

    result = store.apply_event(
        NjsPCEvent(
            event="temps",
            payload={"air": 83.0},
            connection_epoch=1,
        )
    )

    assert result is EventApplyResult.APPLIED
    assert store.data["temperatures"] == {"air": 83.0, "solar": 90.0}
