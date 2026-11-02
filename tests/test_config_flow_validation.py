"""Tests for njsPC config-flow connection validation helpers."""

from typing import Any

import pytest
from homeassistant.const import CONF_HOST, CONF_PORT
from pynjspc import NjsPCError, ProtocolError

from custom_components.njspc import config_flow


def test_normalize_input_strips_host_and_coerces_port() -> None:
    assert config_flow._normalize_input(
        {CONF_HOST: " Controller.Local ", CONF_PORT: "4200"}
    ) == {
        CONF_HOST: "controller.local",
        CONF_PORT: 4200,
    }


@pytest.mark.parametrize(
    ("raised", "expected"),
    [
        (NjsPCError("offline"), "cannot_connect"),
        (ProtocolError("unexpected protocol"), "invalid_response"),
    ],
)
async def test_validate_connection_maps_typed_client_errors(
    monkeypatch: pytest.MonkeyPatch,
    raised: Exception,
    expected: str,
) -> None:
    session = object()
    closed = False

    class FakeClient:
        def __init__(self, **kwargs: Any) -> None:
            assert kwargs["host"] == "controller.local"
            assert kwargs["port"] == 4200
            assert kwargs["session"] is session

        async def fetch_full_state(self, **kwargs: Any) -> dict:
            assert kwargs["timeout"] > 0
            raise raised

        async def close(self) -> None:
            nonlocal closed
            closed = True

    monkeypatch.setattr(config_flow, "NjsPCClient", FakeClient)
    monkeypatch.setattr(
        config_flow,
        "async_get_clientsession",
        lambda hass: session,
    )

    result = await config_flow._async_validate_connection(
        object(),
        {CONF_HOST: "controller.local", CONF_PORT: 4200},
    )

    assert result == expected
    assert closed is True


async def test_validate_connection_uses_ha_session_and_closes_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = object()
    closed = False

    class FakeClient:
        def __init__(self, **kwargs: Any) -> None:
            assert kwargs == {
                "host": "controller.local",
                "port": 4200,
                "session": session,
            }

        async def fetch_full_state(self, **kwargs: Any) -> dict:
            assert kwargs["timeout"] > 0
            return {"status": {"ready": True}}

        async def close(self) -> None:
            nonlocal closed
            closed = True

    monkeypatch.setattr(config_flow, "NjsPCClient", FakeClient)
    monkeypatch.setattr(
        config_flow,
        "async_get_clientsession",
        lambda hass: session,
    )

    result = await config_flow._async_validate_connection(
        object(),
        {CONF_HOST: "controller.local", CONF_PORT: 4200},
    )

    assert result is None
    assert closed is True
