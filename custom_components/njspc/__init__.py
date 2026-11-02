"""The njsPC Home Assistant integration."""

from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_HOST, CONF_PORT, EVENT_HOMEASSISTANT_STOP
from homeassistant.core import Event, HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from pynjspc import NjsPCClient, NjsPCError

from .coordinator import NjspcCoordinator, StateStoreError
from .models import NjspcRuntimeData

type NjspcConfigEntry = ConfigEntry[NjspcRuntimeData]


async def async_setup_entry(
    hass: HomeAssistant,
    entry: NjspcConfigEntry,
) -> None:
    """Set up njsPC from a config entry."""

    client = NjsPCClient(
        host=entry.data[CONF_HOST],
        port=entry.data[CONF_PORT],
        session=async_get_clientsession(hass),
    )
    coordinator = NjspcCoordinator(hass, entry, client)

    try:
        await coordinator.async_setup()
    except (NjsPCError, StateStoreError) as err:
        await coordinator.async_close()
        raise ConfigEntryNotReady(
            f"Unable to establish coherent njsPC state for "
            f"{entry.data[CONF_HOST]}:{entry.data[CONF_PORT]}"
        ) from err

    entry.runtime_data = NjspcRuntimeData(
        client=client,
        coordinator=coordinator,
    )

    async def _async_stop(_: Event) -> None:
        await coordinator.async_close()

    entry.async_on_unload(
        hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STOP,
            _async_stop,
        )
    )
    entry.async_on_unload(coordinator.async_close)


async def async_unload_entry(
    hass: HomeAssistant,
    entry: NjspcConfigEntry,
) -> None:
    """Unload an njsPC config entry.

    Home Assistant invokes callbacks registered with ``entry.async_on_unload``
    after this method returns.
    """

    del hass, entry
