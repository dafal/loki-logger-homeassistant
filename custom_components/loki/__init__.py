import asyncio
import json
import logging
import random
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from aiohttp import ClientSession, ClientError
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.const import (
    CONF_HOST, CONF_PORT, CONF_SSL, CONF_TOKEN, CONF_NAME,
    CONF_VERIFY_SSL, EVENT_STATE_CHANGED, Platform
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers import config_validation as cv, state as state_helper
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.entityfilter import FILTER_SCHEMA
from homeassistant.helpers.json import JSONEncoder
from homeassistant.helpers.typing import ConfigType

from .const import (
    DOMAIN, CONF_FILTER, CONF_BATCH_SIZE, CONF_BATCH_TIMEOUT,
    CONF_HEALTH_CHECK_INTERVAL, CONF_ENABLE_HEALTH_CHECK,
    CONF_ENABLE_RETRY, CONF_MAX_RETRIES, CONF_RETRY_BASE_DELAY, CONF_RETRY_MAX_DELAY,
    DEFAULT_HOST, DEFAULT_PORT, DEFAULT_SSL, DEFAULT_NAME, 
    DEFAULT_BATCH_SIZE, DEFAULT_BATCH_TIMEOUT, DEFAULT_HEALTH_CHECK_INTERVAL,
    DEFAULT_ENABLE_HEALTH_CHECK, DEFAULT_ENABLE_RETRY, DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_BASE_DELAY, DEFAULT_RETRY_MAX_DELAY,
    HEALTH_STATUS_HEALTHY, HEALTH_STATUS_UNHEALTHY, HEALTH_STATUS_UNKNOWN
)

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [Platform.SENSOR]


@dataclass
class RetryItem:
    """Represents an item in the retry queue."""
    payload: Dict[str, Any]
    attempt_count: int
    next_retry_time: float
    original_timestamp: float
    max_retries: int
    event_count: int  # Number of events in this retry item

CONFIG_SCHEMA = vol.Schema({
    DOMAIN: vol.Schema({
        vol.Required(CONF_TOKEN): cv.string,
        vol.Optional(CONF_HOST, default=DEFAULT_HOST): cv.string,
        vol.Optional(CONF_PORT, default=DEFAULT_PORT): cv.port,
        vol.Optional(CONF_SSL, default=False): cv.boolean,
        vol.Optional(CONF_VERIFY_SSL, default=True): cv.boolean,
        vol.Optional(CONF_NAME, default=DEFAULT_NAME): cv.string,
        vol.Optional(CONF_FILTER, default={}): FILTER_SCHEMA,
        vol.Optional(CONF_BATCH_SIZE, default=DEFAULT_BATCH_SIZE): vol.All(int, vol.Range(min=1, max=1000)),
        vol.Optional(CONF_BATCH_TIMEOUT, default=DEFAULT_BATCH_TIMEOUT): vol.All(int, vol.Range(min=1, max=60)),
        vol.Optional(CONF_ENABLE_HEALTH_CHECK, default=DEFAULT_ENABLE_HEALTH_CHECK): cv.boolean,
        vol.Optional(CONF_HEALTH_CHECK_INTERVAL, default=DEFAULT_HEALTH_CHECK_INTERVAL): vol.All(int, vol.Range(min=30, max=3600)),
        vol.Optional(CONF_ENABLE_RETRY, default=DEFAULT_ENABLE_RETRY): cv.boolean,
        vol.Optional(CONF_MAX_RETRIES, default=DEFAULT_MAX_RETRIES): vol.All(int, vol.Range(min=1, max=10)),
        vol.Optional(CONF_RETRY_BASE_DELAY, default=DEFAULT_RETRY_BASE_DELAY): vol.All(int, vol.Range(min=1, max=30)),
        vol.Optional(CONF_RETRY_MAX_DELAY, default=DEFAULT_RETRY_MAX_DELAY): vol.All(int, vol.Range(min=60, max=3600)),
    })
}, extra=vol.ALLOW_EXTRA)

async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the Loki component from YAML configuration."""
    # Support for YAML configuration
    if DOMAIN in config:
        hass.async_create_task(
            hass.config_entries.flow.async_init(
                DOMAIN, context={"source": config_entries.SOURCE_IMPORT}, data=config[DOMAIN]
            )
        )
    return True


class LokiBatcher:
    """Batches events for efficient Loki sending."""
    
    def __init__(self, session: ClientSession, loki_url: str, headers: Dict[str, str], 
                 verify_ssl: bool, batch_size: int, batch_timeout: int,
                 enable_health_check: bool = True, health_check_interval: int = 60,
                 enable_retry: bool = True, max_retries: int = 5, 
                 retry_base_delay: int = 1, retry_max_delay: int = 300):
        self.session = session
        self.loki_url = loki_url
        self.headers = headers
        self.verify_ssl = verify_ssl
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.batch_queue: deque = deque()
        self.batch_timer: asyncio.TimerHandle = None
        self._lock = asyncio.Lock()
        
        # Health monitoring
        self.enable_health_check = enable_health_check
        self.health_check_interval = health_check_interval
        self.health_timer: asyncio.TimerHandle = None
        self.health_status = HEALTH_STATUS_UNKNOWN
        self.last_health_check = None
        self.last_successful_send = None
        self.consecutive_failures = 0
        
        # Event-based metrics (more meaningful than request-based)
        self.total_events_attempted = 0
        self.total_events_failed = 0
        self.total_requests = 0  # Keep for debugging
        self.total_request_failures = 0  # Keep for debugging
        
        # Retry logic
        self.enable_retry = enable_retry
        self.max_retries = max_retries
        self.retry_base_delay = retry_base_delay
        self.retry_max_delay = retry_max_delay
        self.retry_queue: List[RetryItem] = []
        self.retry_timer: asyncio.TimerHandle = None
        self.total_retry_attempts = 0
        self.successful_retry_events = 0
        self.failed_retry_events = 0
        
        # Start health monitoring if enabled
        if self.enable_health_check:
            self._start_health_monitoring()
            
        # Start retry processor if enabled
        if self.enable_retry:
            self._start_retry_processor()
        
    async def add_event(self, stream_data: Dict[str, Any]) -> None:
        """Add an event to the batch queue."""
        async with self._lock:
            self.batch_queue.append(stream_data)
            
            # If this is the first event, start the timer
            if len(self.batch_queue) == 1:
                self._start_timer()
                
            # If we hit the batch size limit, flush immediately
            if len(self.batch_queue) >= self.batch_size:
                self._cancel_timer()
                await self._flush_batch()
    
    def _start_timer(self) -> None:
        """Start the batch timeout timer."""
        if self.batch_timer:
            self.batch_timer.cancel()
        
        loop = asyncio.get_event_loop()
        self.batch_timer = loop.call_later(
            self.batch_timeout, 
            lambda: asyncio.create_task(self._timer_flush())
        )
    
    def _cancel_timer(self) -> None:
        """Cancel the batch timeout timer."""
        if self.batch_timer:
            self.batch_timer.cancel()
            self.batch_timer = None
    
    async def _timer_flush(self) -> None:
        """Flush batch when timer expires."""
        async with self._lock:
            if self.batch_queue:
                await self._flush_batch()
    
    async def _flush_batch(self) -> None:
        """Send the current batch to Loki."""
        if not self.batch_queue:
            return
            
        # Group events by stream labels for efficiency
        streams_map: Dict[str, List] = {}
        
        while self.batch_queue:
            stream_data = self.batch_queue.popleft()
            stream_key = json.dumps(stream_data["stream"], sort_keys=True)
            
            if stream_key not in streams_map:
                streams_map[stream_key] = {
                    "stream": stream_data["stream"],
                    "values": []
                }
            
            streams_map[stream_key]["values"].extend(stream_data["values"])
        
        # Create the batch payload
        payload = {
            "streams": list(streams_map.values())
        }
        
        # Send to Loki
        await self._send_to_loki(payload)
        
        # Reset timer
        self.batch_timer = None
    
    async def _send_to_loki(self, payload: Dict[str, Any]) -> None:
        """Send payload to Loki."""
        # Calculate total events in this payload
        event_count = sum(len(stream.get("values", [])) for stream in payload.get("streams", []))
        
        self.total_requests += 1
        self.total_events_attempted += event_count
        
        try:
            async with self.session.post(
                self.loki_url, 
                data=json.dumps(payload), 
                headers=self.headers, 
                ssl=self.verify_ssl
            ) as response:
                if response.status != 204:
                    self.total_request_failures += 1
                    self.total_events_failed += event_count
                    self.consecutive_failures += 1
                    _LOGGER.warning(
                        "Failed to push batch to Loki: %s (batch size: %d events)", 
                        await response.text(), 
                        event_count
                    )
                    # Add to retry queue
                    self._add_to_retry_queue(payload, event_count)
                else:
                    self.consecutive_failures = 0
                    self.last_successful_send = time.time()
                    _LOGGER.debug(
                        "Successfully sent batch to Loki (batch size: %d events)", 
                        event_count
                    )
        except ClientError as err:
            self.total_request_failures += 1
            self.total_events_failed += event_count
            self.consecutive_failures += 1
            _LOGGER.error("Loki connection error: %s", err)
            # Add to retry queue
            self._add_to_retry_queue(payload, event_count)
    
    def _start_health_monitoring(self) -> None:
        """Start periodic health checks."""
        loop = asyncio.get_event_loop()
        self.health_timer = loop.call_later(
            self.health_check_interval,
            lambda: asyncio.create_task(self._perform_health_check())
        )
    
    def _cancel_health_timer(self) -> None:
        """Cancel the health check timer."""
        if self.health_timer:
            self.health_timer.cancel()
            self.health_timer = None
    
    async def _perform_health_check(self) -> None:
        """Perform a health check against Loki using the /ready endpoint."""
        self.last_health_check = time.time()
        
        # Construct the proper /ready endpoint URL
        protocol = "https" if "https://" in self.loki_url else "http"
        base_url = self.loki_url.replace("/loki/api/v1/push", "")
        ready_url = f"{base_url}/ready"
        
        try:
            async with self.session.get(
                ready_url, 
                headers={"Authorization": self.headers.get("Authorization", "")}, 
                ssl=self.verify_ssl,
                timeout=10
            ) as response:
                if response.status == 200:
                    # Check if response content is "ready"
                    content = await response.text()
                    if content.strip().lower() == "ready":
                        old_status = self.health_status
                        self.health_status = HEALTH_STATUS_HEALTHY
                        if old_status != HEALTH_STATUS_HEALTHY:
                            _LOGGER.info("Loki health check: HEALTHY (/ready returned 200 with 'ready')")
                    else:
                        old_status = self.health_status
                        self.health_status = HEALTH_STATUS_UNHEALTHY
                        if old_status != HEALTH_STATUS_UNHEALTHY:
                            _LOGGER.warning("Loki health check: UNHEALTHY (/ready returned 200 but content was '%s', expected 'ready')", content.strip())
                else:
                    old_status = self.health_status
                    self.health_status = HEALTH_STATUS_UNHEALTHY
                    if old_status != HEALTH_STATUS_UNHEALTHY:
                        _LOGGER.warning("Loki health check: UNHEALTHY (/ready returned status %d)", response.status)
        except Exception as err:
            old_status = self.health_status
            self.health_status = HEALTH_STATUS_UNHEALTHY
            if old_status != HEALTH_STATUS_UNHEALTHY:
                _LOGGER.warning("Loki health check: UNHEALTHY (error: %s)", err)
        
        # Schedule next health check
        if self.enable_health_check:
            self._start_health_monitoring()
    
    def _start_retry_processor(self) -> None:
        """Start the retry processor."""
        loop = asyncio.get_event_loop()
        self.retry_timer = loop.call_later(
            10,  # Check every 10 seconds
            lambda: asyncio.create_task(self._process_retry_queue())
        )
    
    def _cancel_retry_timer(self) -> None:
        """Cancel the retry timer."""
        if self.retry_timer:
            self.retry_timer.cancel()
            self.retry_timer = None
    
    def _calculate_retry_delay(self, attempt: int) -> float:
        """Calculate retry delay with exponential backoff and jitter."""
        # Base exponential backoff: base_delay * (2 ^ attempt)
        base_delay = self.retry_base_delay * (2 ** attempt)
        
        # Apply jitter (±25% randomization)
        jitter = 0.75 + (random.random() * 0.5)  # Range: 0.75 to 1.25
        delay = base_delay * jitter
        
        # Cap at max delay
        return min(delay, self.retry_max_delay)
    
    def _add_to_retry_queue(self, payload: Dict[str, Any], event_count: int) -> None:
        """Add a failed payload to the retry queue."""
        if not self.enable_retry:
            return
            
        current_time = time.time()
        retry_item = RetryItem(
            payload=payload,
            attempt_count=1,
            next_retry_time=current_time + self._calculate_retry_delay(1),
            original_timestamp=current_time,
            max_retries=self.max_retries,
            event_count=event_count
        )
        
        # Limit retry queue size to prevent memory issues
        if len(self.retry_queue) >= 1000:
            # Remove oldest item and account for lost events
            dropped_item = self.retry_queue.pop(0)
            self.failed_retry_events += dropped_item.event_count
            _LOGGER.warning("Retry queue full, dropping oldest retry item (%d events)", dropped_item.event_count)
        
        self.retry_queue.append(retry_item)
        _LOGGER.debug("Added payload to retry queue (attempt 1/%d, %d events, next retry in %.1fs)", 
                     self.max_retries, event_count, retry_item.next_retry_time - current_time)
    
    async def _process_retry_queue(self) -> None:
        """Process items in the retry queue."""
        if not self.enable_retry or not self.retry_queue:
            if self.enable_retry:
                self._start_retry_processor()
            return
        
        current_time = time.time()
        items_to_process = []
        items_to_keep = []
        
        # Separate items ready for retry from those that need to wait
        for item in self.retry_queue:
            # Remove items that are too old (older than 1 hour)
            if current_time - item.original_timestamp > 3600:
                self.failed_retry_events += item.event_count
                _LOGGER.warning("Dropping retry item older than 1 hour (%d events)", item.event_count)
                continue
                
            if current_time >= item.next_retry_time:
                items_to_process.append(item)
            else:
                items_to_keep.append(item)
        
        # Update retry queue with items that aren't ready yet
        self.retry_queue = items_to_keep
        
        # Process ready items
        for item in items_to_process:
            success = await self._retry_send_to_loki(item)
            if not success:
                # Retry failed, decide whether to retry again or give up
                if item.attempt_count < item.max_retries:
                    item.attempt_count += 1
                    item.next_retry_time = current_time + self._calculate_retry_delay(item.attempt_count)
                    self.retry_queue.append(item)
                    _LOGGER.debug("Retry failed, scheduling attempt %d/%d in %.1fs (%d events)", 
                                 item.attempt_count, item.max_retries, 
                                 item.next_retry_time - current_time, item.event_count)
                else:
                    self.failed_retry_events += item.event_count
                    _LOGGER.warning("Retry failed after %d attempts, giving up on %d events", 
                                   item.max_retries, item.event_count)
        
        # Schedule next retry processing
        if self.enable_retry:
            self._start_retry_processor()
    
    async def _retry_send_to_loki(self, retry_item: RetryItem) -> bool:
        """Attempt to send a retry item to Loki."""
        self.total_retry_attempts += 1
        try:
            async with self.session.post(
                self.loki_url, 
                data=json.dumps(retry_item.payload), 
                headers=self.headers, 
                ssl=self.verify_ssl
            ) as response:
                if response.status == 204:
                    self.successful_retry_events += retry_item.event_count
                    # Remove failed events count since they succeeded on retry
                    self.total_events_failed -= retry_item.event_count
                    _LOGGER.debug("Retry successful (attempt %d/%d, %d events)", 
                                 retry_item.attempt_count, retry_item.max_retries, retry_item.event_count)
                    return True
                else:
                    _LOGGER.debug("Retry failed with status %d (attempt %d/%d, %d events)", 
                                 response.status, retry_item.attempt_count, retry_item.max_retries, retry_item.event_count)
                    return False
        except Exception as err:
            _LOGGER.debug("Retry failed with error: %s (attempt %d/%d, %d events)", 
                         err, retry_item.attempt_count, retry_item.max_retries, retry_item.event_count)
            return False
    
    def get_health_info(self) -> Dict[str, Any]:
        """Get current health status and metrics."""
        now = time.time()
        
        # Calculate event-based success rate
        event_success_rate = (
            (self.total_events_attempted - self.total_events_failed) / self.total_events_attempted * 100
            if self.total_events_attempted > 0 else 0
        )
        
        health_info = {
            "status": self.health_status,
            "last_health_check": self.last_health_check,
            "last_successful_send": self.last_successful_send,
            "consecutive_failures": self.consecutive_failures,
            
            # Event-based metrics (primary)
            "total_events_attempted": self.total_events_attempted,
            "total_events_failed": self.total_events_failed,
            "event_success_rate": event_success_rate,
            
            # Request-based metrics (for debugging)
            "total_requests": self.total_requests,
            "total_request_failures": self.total_request_failures,
            "request_success_rate": (
                (self.total_requests - self.total_request_failures) / self.total_requests * 100
                if self.total_requests > 0 else 0
            ),
            
            "time_since_last_success": (
                now - self.last_successful_send 
                if self.last_successful_send else None
            ),
            "health_check_interval": self.health_check_interval,
        }
        
        # Add retry metrics if retry is enabled
        if self.enable_retry:
            retry_queue_events = sum(item.event_count for item in self.retry_queue)
            retry_success_rate = (
                self.successful_retry_events / (self.successful_retry_events + self.failed_retry_events) * 100
                if (self.successful_retry_events + self.failed_retry_events) > 0 else 0
            )
            
            health_info.update({
                "retry_queue_size": len(self.retry_queue),
                "retry_queue_events": retry_queue_events,
                "total_retry_attempts": self.total_retry_attempts,
                "successful_retry_events": self.successful_retry_events,
                "failed_retry_events": self.failed_retry_events,
                "retry_success_rate": retry_success_rate,
            })
        
        return health_info

    def update_config(self, batch_size: int = None, batch_timeout: int = None, 
                     enable_health_check: bool = None, health_check_interval: int = None,
                     enable_retry: bool = None, max_retries: int = None,
                     retry_base_delay: int = None, retry_max_delay: int = None) -> None:
        """Update batcher configuration."""
        if batch_size is not None:
            self.batch_size = batch_size
        if batch_timeout is not None:
            self.batch_timeout = batch_timeout
        if enable_health_check is not None:
            old_enable = self.enable_health_check
            self.enable_health_check = enable_health_check
            
            # Handle health monitoring state changes
            if enable_health_check and not old_enable:
                # Start health monitoring
                self._start_health_monitoring()
                _LOGGER.info("Health monitoring enabled")
            elif not enable_health_check and old_enable:
                # Stop health monitoring
                self._cancel_health_timer()
                self.health_status = HEALTH_STATUS_UNKNOWN
                _LOGGER.info("Health monitoring disabled")
        
        if health_check_interval is not None:
            self.health_check_interval = health_check_interval
            # Restart health monitoring with new interval if enabled
            if self.enable_health_check:
                self._cancel_health_timer()
                self._start_health_monitoring()
        
        # Handle retry settings
        if enable_retry is not None:
            old_enable_retry = self.enable_retry
            self.enable_retry = enable_retry
            
            if enable_retry and not old_enable_retry:
                # Start retry processing
                self._start_retry_processor()
                _LOGGER.info("Retry logic enabled")
            elif not enable_retry and old_enable_retry:
                # Stop retry processing and clear queue
                self._cancel_retry_timer()
                self.retry_queue.clear()
                _LOGGER.info("Retry logic disabled")
        
        if max_retries is not None:
            self.max_retries = max_retries
        if retry_base_delay is not None:
            self.retry_base_delay = retry_base_delay
        if retry_max_delay is not None:
            self.retry_max_delay = retry_max_delay

    async def flush_and_stop(self) -> None:
        """Flush any remaining events and stop the batcher."""
        async with self._lock:
            self._cancel_timer()
            self._cancel_health_timer()
            self._cancel_retry_timer()
            if self.batch_queue:
                await self._flush_batch()
            # Clear retry queue on shutdown
            self.retry_queue.clear()


async def async_setup_entry(hass: HomeAssistant, entry: config_entries.ConfigEntry) -> bool:
    """Set up Loki from a config entry."""
    conf = entry.data
    host = conf[CONF_HOST]
    port = conf[CONF_PORT]
    token = conf[CONF_TOKEN]
    use_ssl = conf[CONF_SSL]
    verify_ssl = conf[CONF_VERIFY_SSL]
    name = conf[CONF_NAME]
    entity_filter = conf.get(CONF_FILTER, {})
    batch_size = conf.get(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE)
    batch_timeout = conf.get(CONF_BATCH_TIMEOUT, DEFAULT_BATCH_TIMEOUT)
    enable_health_check = conf.get(CONF_ENABLE_HEALTH_CHECK, DEFAULT_ENABLE_HEALTH_CHECK)
    health_check_interval = conf.get(CONF_HEALTH_CHECK_INTERVAL, DEFAULT_HEALTH_CHECK_INTERVAL)
    enable_retry = conf.get(CONF_ENABLE_RETRY, DEFAULT_ENABLE_RETRY)
    max_retries = conf.get(CONF_MAX_RETRIES, DEFAULT_MAX_RETRIES)
    retry_base_delay = conf.get(CONF_RETRY_BASE_DELAY, DEFAULT_RETRY_BASE_DELAY)
    retry_max_delay = conf.get(CONF_RETRY_MAX_DELAY, DEFAULT_RETRY_MAX_DELAY)

    session: ClientSession = async_get_clientsession(hass)

    protocol = "https" if use_ssl else "http"
    loki_url = f"{protocol}://{host}:{port}/loki/api/v1/push"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Initialize the batcher
    batcher = LokiBatcher(
        session, loki_url, headers, verify_ssl, batch_size, batch_timeout,
        enable_health_check, health_check_interval,
        enable_retry, max_retries, retry_base_delay, retry_max_delay
    )

    async def loki_event_listener(event):
        """Send state change events to Loki via batching."""
        state = event.data.get("new_state")
        if state is None:
            return
        
        # Apply entity filter if configured
        if entity_filter and not entity_filter.get(state.entity_id, True):
            return

        try:
            _state = state_helper.state_as_number(state)
        except ValueError:
            _state = state.state

        timestamp_ns = str(int(event.time_fired.timestamp() * 1e9))

        # Log line as JSON (no instance/host here)
        log_line = json.dumps({
            "timestamp": event.time_fired.isoformat(),
            "entity_id": state.entity_id,
            "state": _state,
            "attributes": dict(state.attributes),
            "domain": state.domain,
        }, cls=JSONEncoder)

        # Create stream data for batching
        stream_data = {
            "stream": {
                "entity_id": state.entity_id,
                "domain": state.domain,
                "service_name": "home_assistant",
                "instance": name
            },
            "values": [[timestamp_ns, log_line]]
        }

        # Add to batch
        await batcher.add_event(stream_data)

    # Store the event listener unsubscribe function
    unsubscribe = hass.bus.async_listen(EVENT_STATE_CHANGED, loki_event_listener)
    
    # Store data for later use
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = {
        "unsubscribe": unsubscribe,
        "batcher": batcher,
        "sensors_setup": False,  # Track if sensors are set up
    }

    # Set up sensor platform if health monitoring is enabled
    if enable_health_check:
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
        hass.data[DOMAIN][entry.entry_id]["sensors_setup"] = True

    # Add options update listener
    entry.async_on_unload(entry.add_update_listener(async_options_updated))

    return True


async def async_options_updated(hass: HomeAssistant, entry: config_entries.ConfigEntry) -> None:
    """Handle options update."""
    _LOGGER.debug("Options updated for Loki integration")
    
    if entry.entry_id in hass.data.get(DOMAIN, {}):
        entry_data = hass.data[DOMAIN][entry.entry_id]
        batcher = entry_data["batcher"]
        
        # Update batcher configuration
        new_conf = entry.data
        batcher.update_config(
            batch_size=new_conf.get(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE),
            batch_timeout=new_conf.get(CONF_BATCH_TIMEOUT, DEFAULT_BATCH_TIMEOUT),
            enable_health_check=new_conf.get(CONF_ENABLE_HEALTH_CHECK, DEFAULT_ENABLE_HEALTH_CHECK),
            health_check_interval=new_conf.get(CONF_HEALTH_CHECK_INTERVAL, DEFAULT_HEALTH_CHECK_INTERVAL),
            enable_retry=new_conf.get(CONF_ENABLE_RETRY, DEFAULT_ENABLE_RETRY),
            max_retries=new_conf.get(CONF_MAX_RETRIES, DEFAULT_MAX_RETRIES),
            retry_base_delay=new_conf.get(CONF_RETRY_BASE_DELAY, DEFAULT_RETRY_BASE_DELAY),
            retry_max_delay=new_conf.get(CONF_RETRY_MAX_DELAY, DEFAULT_RETRY_MAX_DELAY),
        )
        
        # Handle sensor platform changes
        current_health_enabled = new_conf.get(CONF_ENABLE_HEALTH_CHECK, DEFAULT_ENABLE_HEALTH_CHECK)
        sensors_currently_setup = entry_data.get("sensors_setup", False)
        
        try:
            if current_health_enabled and not sensors_currently_setup:
                # Set up sensors - they weren't set up before
                await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
                entry_data["sensors_setup"] = True
                _LOGGER.debug("Set up sensor platform after enabling health monitoring")
            elif not current_health_enabled and sensors_currently_setup:
                # Unload sensors - they were set up before
                await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
                entry_data["sensors_setup"] = False
                _LOGGER.debug("Unloaded sensor platform after disabling health monitoring")
        except Exception as err:
            _LOGGER.warning("Error managing sensor platform during options update: %s", err)


async def async_unload_entry(hass: HomeAssistant, entry: config_entries.ConfigEntry) -> bool:
    """Unload a config entry."""
    if DOMAIN in hass.data and entry.entry_id in hass.data[DOMAIN]:
        entry_data = hass.data[DOMAIN][entry.entry_id]
        
        # Unload sensor platform if it was set up
        if entry_data.get("sensors_setup", False):
            await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
        
        # Unsubscribe from events
        entry_data["unsubscribe"]()
        
        # Flush and stop the batcher
        if "batcher" in entry_data:
            await entry_data["batcher"].flush_and_stop()
        
        hass.data[DOMAIN].pop(entry.entry_id)
        
        if not hass.data[DOMAIN]:
            hass.data.pop(DOMAIN)

    return True

