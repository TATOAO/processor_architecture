"""
HTTP and WebSocket client for sending monitoring events to the visualization backend.
"""

import asyncio
import json
import logging
from typing import Any, Dict, Optional
from urllib.parse import urljoin

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    logging.warning("aiohttp not available. Monitoring will be disabled.")

from .models import MonitoringEvent, MonitoringConfig


class MonitoringClient:
    """Client for sending monitoring events to the visualization backend"""
    
    def __init__(self, config: MonitoringConfig):
        self.config = config
        self.session: Optional[Any] = None
        self.websocket: Optional[Any] = None
        self.event_queue: asyncio.Queue = asyncio.Queue(maxsize=config.max_events_buffer)
        self.running = False
        self.logger = logging.getLogger(__name__)
        
        if not AIOHTTP_AVAILABLE:
            self.logger.warning("aiohttp not available. Monitoring client will be disabled.")
    
    async def start(self) -> bool:
        """Start the monitoring client"""
        if not AIOHTTP_AVAILABLE:
            return False
            
        try:
            self.session = aiohttp.ClientSession()
            self.running = True
            
            # Start event processing task
            asyncio.create_task(self._process_events())
            
            # Try to establish WebSocket connection if enabled
            if self.config.enable_websocket:
                asyncio.create_task(self._connect_websocket())
                
            self.logger.info(f"Monitoring client started, connecting to {self.config.api_url}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start monitoring client: {e}")
            return False
    
    async def stop(self):
        """Stop the monitoring client"""
        self.running = False
        
        if self.websocket:
            await self.websocket.close()
            
        if self.session:
            await self.session.close()
    
    async def send_event(self, event: MonitoringEvent) -> bool:
        """Send a monitoring event"""
        if not self.running or not AIOHTTP_AVAILABLE:
            return False
            
        try:
            await self.event_queue.put(event)
            return True
        except asyncio.QueueFull:
            self.logger.warning("Event queue full, dropping event")
            return False
    
    async def _process_events(self):
        """Process events from the queue"""
        while self.running:
            try:
                # Wait for event with timeout
                event = await asyncio.wait_for(self.event_queue.get(), timeout=1.0)
                await self._send_event_http(event)
                
                # Also send via WebSocket if available
                if self.websocket and not self.websocket.closed:
                    await self._send_event_websocket(event)
                    
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Error processing event: {e}")
    
    async def _send_event_http(self, event: MonitoringEvent):
        """Send event via HTTP POST"""
        if not self.session:
            return
            
        try:
            url = urljoin(self.config.api_url, "/api/events")
            async with self.session.post(
                url, 
                json=event.model_dump(),
                timeout=aiohttp.ClientTimeout(total=5)
            ) as response:
                if response.status != 200:
                    self.logger.warning(f"HTTP event send failed: {response.status}")
                    
        except Exception as e:
            self.logger.debug(f"HTTP event send error: {e}")
    
    async def _send_event_websocket(self, event: MonitoringEvent):
        """Send event via WebSocket"""
        if not self.websocket or self.websocket.closed:
            return
            
        try:
            await self.websocket.send_str(event.model_dump_json())
        except Exception as e:
            self.logger.debug(f"WebSocket event send error: {e}")
    
    async def _connect_websocket(self):
        """Establish WebSocket connection"""
        if not self.session:
            return
            
        ws_url = self.config.api_url.replace("http://", "ws://").replace("https://", "wss://")
        ws_url = urljoin(ws_url, f"/ws/{self.config.session_name or 'default'}")
        
        retry_count = 0
        max_retries = 5
        
        while self.running and retry_count < max_retries:
            try:
                self.websocket = await self.session.ws_connect(ws_url)
                self.logger.info(f"WebSocket connected to {ws_url}")
                
                # Keep connection alive
                async for msg in self.websocket:
                    if msg.type == aiohttp.WSMsgType.ERROR:
                        break
                        
            except Exception as e:
                retry_count += 1
                self.logger.debug(f"WebSocket connection failed (attempt {retry_count}): {e}")
                if retry_count < max_retries:
                    await asyncio.sleep(2 ** retry_count)  # Exponential backoff
    
    async def register_session(self, session_info: Dict[str, Any]) -> bool:
        """Register a new monitoring session"""
        if not self.session or not AIOHTTP_AVAILABLE:
            return False
            
        try:
            url = urljoin(self.config.api_url, "/api/sessions")
            async with self.session.post(
                url,
                json=session_info,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                return response.status == 200
                
        except Exception as e:
            self.logger.error(f"Session registration failed: {e}")
            return False