"""Sensor entities for Loki integration."""
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from homeassistant.components.sensor import SensorEntity, SensorDeviceClass
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import (
    DOMAIN, 
    HEALTH_STATUS_HEALTHY, 
    HEALTH_STATUS_UNHEALTHY, 
    HEALTH_STATUS_UNKNOWN
)

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Loki sensor entities."""
    entry_data = hass.data[DOMAIN][config_entry.entry_id]
    batcher = entry_data["batcher"]
    
    # Only create sensor if health monitoring is enabled
    if batcher.enable_health_check:
        sensors = [
            LokiHealthSensor(config_entry, batcher),
            LokiMetricsSensor(config_entry, batcher),
        ]
        async_add_entities(sensors, True)


class LokiHealthSensor(SensorEntity):
    """Sensor representing Loki connection health status."""

    def __init__(self, config_entry: ConfigEntry, batcher) -> None:
        """Initialize the sensor."""
        self._config_entry = config_entry
        self._batcher = batcher
        self._attr_name = f"Loki {config_entry.data['name']} Connection Health"
        self._attr_unique_id = f"{config_entry.entry_id}_health"
        self._attr_device_class = SensorDeviceClass.ENUM
        self._attr_options = [HEALTH_STATUS_HEALTHY, HEALTH_STATUS_UNHEALTHY, HEALTH_STATUS_UNKNOWN]
        self._attr_entity_category = None  # Make it visible in main dashboard

    @property
    def native_value(self) -> str:
        """Return the state of the sensor."""
        status = self._batcher.health_status
        status_map = {
            HEALTH_STATUS_HEALTHY: "Connected",
            HEALTH_STATUS_UNHEALTHY: "Disconnected", 
            HEALTH_STATUS_UNKNOWN: "Checking"
        }
        return status_map.get(status, status)

    @property
    def icon(self) -> str:
        """Return the icon for the sensor."""
        status = self._batcher.health_status
        if status == HEALTH_STATUS_HEALTHY:
            return "mdi:cloud-check"
        elif status == HEALTH_STATUS_UNHEALTHY:
            return "mdi:cloud-alert"
        else:
            return "mdi:cloud-question"

    @property
    def extra_state_attributes(self) -> Dict[str, Any]:
        """Return additional state attributes."""
        health_info = self._batcher.get_health_info()
        
        attrs = {
            "Last Health Check": (
                datetime.fromtimestamp(health_info["last_health_check"]).strftime("%Y-%m-%d %H:%M:%S")
                if health_info["last_health_check"] else "Never"
            ),
            "Last Successful Send": (
                datetime.fromtimestamp(health_info["last_successful_send"]).strftime("%Y-%m-%d %H:%M:%S")
                if health_info["last_successful_send"] else "Never"
            ),
            "Consecutive Failures": health_info["consecutive_failures"],
            "Health Check Interval (seconds)": health_info["health_check_interval"],
        }
        
        if health_info["time_since_last_success"] is not None:
            seconds = round(health_info["time_since_last_success"])
            if seconds < 60:
                attrs["Time Since Last Success"] = f"{seconds}s"
            elif seconds < 3600:
                attrs["Time Since Last Success"] = f"{seconds//60}m {seconds%60}s"
            else:
                hours = seconds // 3600
                minutes = (seconds % 3600) // 60
                attrs["Time Since Last Success"] = f"{hours}h {minutes}m"
        
        return attrs

    @property
    def available(self) -> bool:
        """Return True if entity is available."""
        return True


class LokiMetricsSensor(SensorEntity):
    """Sensor representing Loki connection metrics."""

    def __init__(self, config_entry: ConfigEntry, batcher) -> None:
        """Initialize the sensor."""
        self._config_entry = config_entry
        self._batcher = batcher
        self._attr_name = f"Loki {config_entry.data['name']} Event Success Rate"
        self._attr_unique_id = f"{config_entry.entry_id}_metrics"
        self._attr_native_unit_of_measurement = "%"
        self._attr_device_class = None
        self._attr_entity_category = None  # Make it visible in main dashboard
        self._attr_suggested_display_precision = 1

    @property
    def native_value(self) -> float:
        """Return the event success rate as the main value."""
        health_info = self._batcher.get_health_info()
        return round(health_info["event_success_rate"], 1)

    @property
    def icon(self) -> str:
        """Return the icon for the sensor."""
        success_rate = self.native_value
        if success_rate >= 95:
            return "mdi:chart-line-variant"
        elif success_rate >= 80:
            return "mdi:chart-line"
        else:
            return "mdi:chart-line-stacked"

    @property
    def extra_state_attributes(self) -> Dict[str, Any]:
        """Return additional state attributes."""
        health_info = self._batcher.get_health_info()
        
        attrs = {
            # Event-based metrics (primary)
            "Total Events Attempted": f"{health_info['total_events_attempted']:,}",
            "Total Events Failed": f"{health_info['total_events_failed']:,}",
            "Event Success Rate (%)": f"{health_info['event_success_rate']:.1f}",
            
            # Request-based metrics (for debugging)
            "Total HTTP Requests": f"{health_info['total_requests']:,}",
            "Total HTTP Failures": f"{health_info['total_request_failures']:,}",
            "HTTP Success Rate (%)": f"{health_info['request_success_rate']:.1f}",
            "Consecutive Failures": health_info["consecutive_failures"],
        }
        
        # Add retry metrics if available
        if "retry_queue_size" in health_info:
            attrs.update({
                "Retry Queue Size": health_info["retry_queue_size"],
                "Events Pending Retry": f"{health_info['retry_queue_events']:,}",
                "Total Retry Attempts": f"{health_info['total_retry_attempts']:,}",
                "Events Recovered via Retry": f"{health_info['successful_retry_events']:,}",
                "Events Lost After Retries": f"{health_info['failed_retry_events']:,}",
                "Retry Success Rate (%)": f"{health_info['retry_success_rate']:.1f}",
            })
        
        return attrs

    @property
    def available(self) -> bool:
        """Return True if entity is available."""
        return True