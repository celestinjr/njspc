"""Config flow for the njsPC integration."""

from __future__ import annotations

import logging
from typing import Any, override

import probatio
from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.const import CONF_HOST, CONF_PORT
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from pynjspc import NjsPCClient, NjsPCError, ProtocolError

from .const import DEFAULT_PORT, DEFAULT_REQUEST_TIMEOUT, DOMAIN

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = probatio.Schema(
    {
        probatio.Required(CONF_HOST): str,
        probatio.Required(CONF_PORT, default=DEFAULT_PORT): probatio.Coerce(int),
    }
)


def _normalize_input(user_input: dict[str, Any]) -> dict[str, Any]:
    """Normalize connection data before comparison and storage."""

    return {
        CONF_HOST: str(user_input[CONF_HOST]).strip().lower(),
        CONF_PORT: int(user_input[CONF_PORT]),
    }


async def _async_validate_connection(
    hass: HomeAssistant,
    data: dict[str, Any],
) -> str | None:
    """Validate that an endpoint returns an njsPC full-state envelope."""

    client = NjsPCClient(
        host=data[CONF_HOST],
        port=data[CONF_PORT],
        session=async_get_clientsession(hass),
    )

    try:
        await client.fetch_full_state(timeout=DEFAULT_REQUEST_TIMEOUT)
    except ProtocolError:
        return "invalid_response"
    except (NjsPCError, OSError, TimeoutError):
        return "cannot_connect"
    except Exception:
        _LOGGER.exception("Unexpected error validating njsPC endpoint")
        return "unknown"
    finally:
        await client.close()

    return None


class NjspcConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle the njsPC config flow."""

    VERSION = 1

    @override
    async def async_step_user(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Handle manual controller configuration."""

        errors: dict[str, str] = {}

        if user_input is not None:
            data = _normalize_input(user_input)

            if not data[CONF_HOST]:
                errors[CONF_HOST] = "invalid_host"
            elif not 1 <= data[CONF_PORT] <= 65535:
                errors[CONF_PORT] = "invalid_port"
            else:
                endpoint = {
                    CONF_HOST: data[CONF_HOST],
                    CONF_PORT: data[CONF_PORT],
                }
                self._async_abort_entries_match(endpoint)

                if error := await _async_validate_connection(self.hass, data):
                    errors["base"] = error
                else:
                    # Guard against another flow creating the same endpoint
                    # while this flow awaited the connection probe.
                    self._async_abort_entries_match(endpoint)
                    return self.async_create_entry(
                        title=f"{data[CONF_HOST]}:{data[CONF_PORT]}",
                        data=data,
                    )

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_DATA_SCHEMA,
            errors=errors,
        )
