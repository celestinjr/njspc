"""Home Assistant config-flow integration tests for njsPC."""

from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.config_entries import SOURCE_USER
from homeassistant.const import CONF_HOST, CONF_PORT
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant.common import MockConfigEntry

from custom_components.njspc.const import DOMAIN

USER_INPUT = {
    CONF_HOST: " Controller.Local ",
    CONF_PORT: 4200,
}


async def test_user_flow_creates_normalized_entry(hass: HomeAssistant) -> None:
    """A successful manual flow creates one normalized config entry."""

    with (
        patch(
            "custom_components.njspc.config_flow._async_validate_connection",
            new_callable=AsyncMock,
            return_value=None,
        ) as validate,
        patch(
            "custom_components.njspc.async_setup_entry",
            new_callable=AsyncMock,
        ) as setup_entry,
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_USER},
        )

        assert result["type"] is FlowResultType.FORM
        assert result["step_id"] == "user"

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            user_input=USER_INPUT,
        )
        await hass.async_block_till_done()

    assert result["type"] is FlowResultType.CREATE_ENTRY

    entry = result["result"]
    assert entry.domain == DOMAIN
    assert entry.title == "controller.local:4200"
    assert entry.data == {
        CONF_HOST: "controller.local",
        CONF_PORT: 4200,
    }
    assert entry.unique_id is None

    validate.assert_awaited_once()
    setup_entry.assert_awaited_once()


async def test_duplicate_endpoint_aborts_before_connection_probe(
    hass: HomeAssistant,
) -> None:
    """The same normalized host/port cannot be configured twice."""

    MockConfigEntry(
        domain=DOMAIN,
        title="existing",
        data={
            CONF_HOST: "controller.local",
            CONF_PORT: 4200,
        },
    ).add_to_hass(hass)

    with patch(
        "custom_components.njspc.config_flow._async_validate_connection",
        new_callable=AsyncMock,
    ) as validate:
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_USER},
        )

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            user_input=USER_INPUT,
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"
    validate.assert_not_awaited()


@pytest.mark.parametrize(
    ("validation_error", "expected_error"),
    [
        pytest.param("cannot_connect", "cannot_connect", id="transport"),
        pytest.param("invalid_response", "invalid_response", id="protocol"),
    ],
)
async def test_connection_errors_return_to_form(
    hass: HomeAssistant,
    validation_error: str,
    expected_error: str,
) -> None:
    """Connection and protocol failures remain user-correctable flow errors."""

    with patch(
        "custom_components.njspc.config_flow._async_validate_connection",
        new_callable=AsyncMock,
        return_value=validation_error,
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_USER},
        )

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            user_input=USER_INPUT,
        )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "user"
    assert result["errors"] == {"base": expected_error}


async def test_distinct_endpoints_create_distinct_entries(
    hass: HomeAssistant,
) -> None:
    """Host/port are connection data and distinct endpoints may coexist."""

    MockConfigEntry(
        domain=DOMAIN,
        title="first",
        data={
            CONF_HOST: "controller-a.local",
            CONF_PORT: 4200,
        },
    ).add_to_hass(hass)

    with (
        patch(
            "custom_components.njspc.config_flow._async_validate_connection",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch(
            "custom_components.njspc.async_setup_entry",
            new_callable=AsyncMock,
        ),
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_USER},
        )

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            user_input={
                CONF_HOST: "controller-b.local",
                CONF_PORT: 4200,
            },
        )
        await hass.async_block_till_done()

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert len(hass.config_entries.async_entries(DOMAIN)) == 2
