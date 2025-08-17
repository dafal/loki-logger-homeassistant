"""Config flow for Loki integration."""
import logging
from typing import Any, Dict, Optional
import voluptuous as vol
from aiohttp import ClientSession, ClientError

from homeassistant import config_entries
from homeassistant.const import (
    CONF_HOST, CONF_PORT, CONF_SSL, CONF_TOKEN, CONF_NAME, CONF_VERIFY_SSL
)
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.entityfilter import FILTER_SCHEMA

from .const import (
    DOMAIN, DEFAULT_HOST, DEFAULT_PORT, DEFAULT_NAME, CONF_FILTER,
    CONF_BATCH_SIZE, CONF_BATCH_TIMEOUT, DEFAULT_BATCH_SIZE, DEFAULT_BATCH_TIMEOUT,
    CONF_ENABLE_HEALTH_CHECK, CONF_HEALTH_CHECK_INTERVAL, 
    DEFAULT_ENABLE_HEALTH_CHECK, DEFAULT_HEALTH_CHECK_INTERVAL,
    CONF_ENABLE_RETRY, CONF_MAX_RETRIES, CONF_RETRY_BASE_DELAY, CONF_RETRY_MAX_DELAY,
    DEFAULT_ENABLE_RETRY, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_BASE_DELAY, DEFAULT_RETRY_MAX_DELAY
)

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = vol.Schema({
    vol.Required(CONF_TOKEN): str,
    vol.Optional(CONF_HOST, default=DEFAULT_HOST): str,
    vol.Optional(CONF_PORT, default=DEFAULT_PORT): int,
    vol.Optional(CONF_SSL, default=False): bool,
    vol.Optional(CONF_VERIFY_SSL, default=True): bool,
    vol.Optional(CONF_NAME, default=DEFAULT_NAME): str,
    vol.Optional(CONF_BATCH_SIZE, default=DEFAULT_BATCH_SIZE): vol.All(int, vol.Range(min=1, max=1000)),
    vol.Optional(CONF_BATCH_TIMEOUT, default=DEFAULT_BATCH_TIMEOUT): vol.All(int, vol.Range(min=1, max=60)),
    vol.Optional(CONF_ENABLE_HEALTH_CHECK, default=DEFAULT_ENABLE_HEALTH_CHECK): bool,
    vol.Optional(CONF_HEALTH_CHECK_INTERVAL, default=DEFAULT_HEALTH_CHECK_INTERVAL): vol.All(int, vol.Range(min=30, max=3600)),
    vol.Optional(CONF_ENABLE_RETRY, default=DEFAULT_ENABLE_RETRY): bool,
    vol.Optional(CONF_MAX_RETRIES, default=DEFAULT_MAX_RETRIES): vol.All(int, vol.Range(min=1, max=10)),
    vol.Optional(CONF_RETRY_BASE_DELAY, default=DEFAULT_RETRY_BASE_DELAY): vol.All(int, vol.Range(min=1, max=30)),
    vol.Optional(CONF_RETRY_MAX_DELAY, default=DEFAULT_RETRY_MAX_DELAY): vol.All(int, vol.Range(min=60, max=3600)),
})


async def validate_input(hass: HomeAssistant, data: dict[str, Any]) -> dict[str, Any]:
    """Validate the user input allows us to connect to Loki."""
    session: ClientSession = async_get_clientsession(hass)
    
    host = data[CONF_HOST]
    port = data[CONF_PORT]
    token = data[CONF_TOKEN]
    use_ssl = data[CONF_SSL]
    verify_ssl = data[CONF_VERIFY_SSL]
    
    protocol = "https" if use_ssl else "http"
    
    # Test connection to Loki - we'll try to hit the ready endpoint
    ready_url = f"{protocol}://{host}:{port}/ready"
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        async with session.get(ready_url, headers=headers, ssl=verify_ssl, timeout=10) as response:
            if response.status not in [200, 404]:  # 404 is ok, means Loki is running but endpoint doesn't exist
                if response.status == 401:
                    raise InvalidAuth
                else:
                    raise CannotConnect
    except ClientError as err:
        _LOGGER.error("Error connecting to Loki: %s", err)
        raise CannotConnect from err
    
    # Return info that you want to store in the config entry.
    return {"title": f"Loki ({host}:{port})"}


class ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Loki."""

    VERSION = 1

    @staticmethod
    def async_get_options_flow(config_entry: config_entries.ConfigEntry):
        """Get the options flow for this handler."""
        return OptionsFlow(config_entry)

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial step."""
        errors: dict[str, str] = {}
        
        if user_input is not None:
            try:
                info = await validate_input(self.hass, user_input)
            except CannotConnect:
                errors["base"] = "cannot_connect"
            except InvalidAuth:
                errors["base"] = "invalid_auth"
            except Exception:  # pylint: disable=broad-except
                _LOGGER.exception("Unexpected exception")
                errors["base"] = "unknown"
            else:
                # Check if already configured
                await self.async_set_unique_id(f"{user_input[CONF_HOST]}:{user_input[CONF_PORT]}")
                self._abort_if_unique_id_configured()
                
                return self.async_create_entry(title=info["title"], data=user_input)

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_DATA_SCHEMA,
            errors=errors,
        )

    async def async_step_import(self, import_config: dict[str, Any]) -> FlowResult:
        """Handle import from configuration.yaml."""
        try:
            info = await validate_input(self.hass, import_config)
        except (CannotConnect, InvalidAuth):
            _LOGGER.error("Cannot import Loki config due to connection issues")
            return self.async_abort(reason="cannot_connect")
        except Exception:  # pylint: disable=broad-except
            _LOGGER.exception("Unexpected exception during import")
            return self.async_abort(reason="unknown")

        # Check if already configured
        await self.async_set_unique_id(f"{import_config[CONF_HOST]}:{import_config[CONF_PORT]}")
        self._abort_if_unique_id_configured()

        return self.async_create_entry(title=info["title"], data=import_config)


class CannotConnect(Exception):
    """Error to indicate we cannot connect."""


class InvalidAuth(Exception):
    """Error to indicate there is invalid auth."""


class OptionsFlow(config_entries.OptionsFlow):
    """Handle options flow for Loki integration."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        """Initialize options flow."""
        super().__init__()
        self._config_entry = config_entry

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """Manage the options."""
        errors: dict[str, str] = {}

        if user_input is not None:
            # Update the config entry
            self.hass.config_entries.async_update_entry(
                self._config_entry,
                data={**self._config_entry.data, **user_input}
            )
            return self.async_create_entry(title="", data={})

        # Create schema with current values as defaults
        current_data = self._config_entry.data
        
        options_schema = vol.Schema({
            vol.Optional(
                CONF_BATCH_SIZE, 
                default=current_data.get(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE)
            ): vol.All(int, vol.Range(min=1, max=1000)),
            vol.Optional(
                CONF_BATCH_TIMEOUT, 
                default=current_data.get(CONF_BATCH_TIMEOUT, DEFAULT_BATCH_TIMEOUT)
            ): vol.All(int, vol.Range(min=1, max=60)),
            vol.Optional(
                CONF_ENABLE_HEALTH_CHECK, 
                default=current_data.get(CONF_ENABLE_HEALTH_CHECK, DEFAULT_ENABLE_HEALTH_CHECK)
            ): bool,
            vol.Optional(
                CONF_HEALTH_CHECK_INTERVAL, 
                default=current_data.get(CONF_HEALTH_CHECK_INTERVAL, DEFAULT_HEALTH_CHECK_INTERVAL)
            ): vol.All(int, vol.Range(min=30, max=3600)),
            vol.Optional(
                CONF_ENABLE_RETRY, 
                default=current_data.get(CONF_ENABLE_RETRY, DEFAULT_ENABLE_RETRY)
            ): bool,
            vol.Optional(
                CONF_MAX_RETRIES, 
                default=current_data.get(CONF_MAX_RETRIES, DEFAULT_MAX_RETRIES)
            ): vol.All(int, vol.Range(min=1, max=10)),
            vol.Optional(
                CONF_RETRY_BASE_DELAY, 
                default=current_data.get(CONF_RETRY_BASE_DELAY, DEFAULT_RETRY_BASE_DELAY)
            ): vol.All(int, vol.Range(min=1, max=30)),
            vol.Optional(
                CONF_RETRY_MAX_DELAY, 
                default=current_data.get(CONF_RETRY_MAX_DELAY, DEFAULT_RETRY_MAX_DELAY)
            ): vol.All(int, vol.Range(min=60, max=3600)),
        })

        return self.async_show_form(
            step_id="init",
            data_schema=options_schema,
            errors=errors,
        )