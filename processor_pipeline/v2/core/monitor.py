"""
Comprehensive monitoring and observability system for the v2 processing pipeline.

Provides real-time monitoring, logging, pipe tapping, and performance metrics.
"""

import asyncio
import threading
import uuid
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable, Set
from enum import Enum
from collections import defaultdict, deque

from .interfaces import MonitorInterface, PipeInterface, ProcessorInterface
from .errors import PipelineError


class LogLevel(Enum):
    """Log levels for monitoring events"""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class EventType(Enum):
    """Types of events that can be monitored"""
    PROCESSOR_START = "processor_start"
    PROCESSOR_COMPLETE = "processor_complete"
    PROCESSOR_ERROR = "processor_error"
    PIPE_DATA_FLOW = "pipe_data_flow"
    PIPE_FULL = "pipe_full"
    PIPE_EMPTY = "pipe_empty"
    GRAPH_START = "graph_start"
    GRAPH_COMPLETE = "graph_complete"
    GRAPH_ERROR = "graph_error"
    CHECKPOINT_CREATE = "checkpoint_create"
    CHECKPOINT_RESTORE = "checkpoint_restore"


class MonitorEvent:
    """Represents a monitoring event"""
    
    def __init__(self, event_type: EventType, processor_id: Optional[str] = None,
                 data: Optional[Dict[str, Any]] = None, level: LogLevel = LogLevel.INFO):
        self.event_id = str(uuid.uuid4())
        self.event_type = event_type
        self.processor_id = processor_id
        self.data = data or {}
        self.level = level
        self.timestamp = datetime.now()
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary"""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "processor_id": self.processor_id,
            "data": self.data,
            "level": self.level.value,
            "timestamp": self.timestamp.isoformat()
        }


class PipeTap:
    """Represents a tap on a pipe for monitoring data flow"""
    
    def __init__(self, tap_id: str, pipe: PipeInterface, callback: Callable[[Any], None]):
        self.tap_id = tap_id
        self.pipe = pipe
        self.callback = callback
        self.created_at = datetime.now()
        self.total_data_seen = 0
        self.last_activity = self.created_at
        
    def on_data(self, data: Any) -> None:
        """Called when data flows through the pipe"""
        try:
            self.total_data_seen += 1
            self.last_activity = datetime.now()
            self.callback(data)
        except Exception as e:
            # Don't let tap errors affect the pipeline
            pass
            
    def get_status(self) -> Dict[str, Any]:
        """Get tap status"""
        return {
            "tap_id": self.tap_id,
            "pipe_id": getattr(self.pipe, 'pipe_id', 'unknown'),
            "created_at": self.created_at.isoformat(),
            "last_activity": self.last_activity.isoformat(),
            "total_data_seen": self.total_data_seen
        }


class Monitor:
    """
    Comprehensive monitoring system for pipeline observability.
    
    Provides real-time monitoring, logging, metrics collection, and pipe tapping.
    """
    
    def __init__(self, max_events: int = 10000, log_level: LogLevel = LogLevel.INFO):
        self.max_events = max_events
        self.log_level = log_level
        self.is_monitoring = False
        
        # Event storage
        self.events: deque = deque(maxlen=max_events)
        self.events_lock = threading.RLock()
        
        # Pipe taps
        self.taps: Dict[str, PipeTap] = {}
        self.taps_lock = threading.RLock()
        
        # Processor tracking
        self.processors: Dict[str, ProcessorInterface] = {}
        self.processor_metrics: Dict[str, Dict[str, Any]] = {}
        
        # Performance metrics
        self.metrics = {
            "total_events": 0,
            "events_by_type": defaultdict(int),
            "events_by_processor": defaultdict(int),
            "start_time": None,
            "uptime_seconds": 0
        }
        
        # Logging setup
        self.logger = logging.getLogger(f"monitor_{uuid.uuid4().hex[:8]}")
        self.logger.setLevel(getattr(logging, log_level.value.upper()))
        
        # Event handlers
        self.event_handlers: Dict[EventType, List[Callable]] = defaultdict(list)
        
        # Async components
        self._monitor_task: Optional[asyncio.Task] = None
        self._event_queue: Optional[asyncio.Queue] = None
        
    def start_monitoring(self) -> None:
        """Start the monitoring system"""
        if self.is_monitoring:
            return
            
        self.is_monitoring = True
        self.metrics["start_time"] = datetime.now()
        
        # Start async monitoring task
        if asyncio.get_event_loop().is_running():
            self._event_queue = asyncio.Queue()
            self._monitor_task = asyncio.create_task(self._monitoring_loop())
            
        self.log_event(EventType.GRAPH_START, None, {"message": "Monitoring started"})
        
    def stop_monitoring(self) -> None:
        """Stop the monitoring system"""
        if not self.is_monitoring:
            return
            
        self.is_monitoring = False
        
        # Stop async monitoring task
        if self._monitor_task:
            self._monitor_task.cancel()
            self._monitor_task = None
            
        self.log_event(EventType.GRAPH_COMPLETE, None, {"message": "Monitoring stopped"})
        
    async def _monitoring_loop(self) -> None:
        """Main monitoring loop for async event processing"""
        while self.is_monitoring:
            try:
                # Update metrics
                if self.metrics["start_time"]:
                    self.metrics["uptime_seconds"] = (
                        datetime.now() - self.metrics["start_time"]
                    ).total_seconds()
                    
                # Process any queued events
                if self._event_queue:
                    try:
                        event = await asyncio.wait_for(self._event_queue.get(), timeout=1.0)
                        self._process_event_async(event)
                    except asyncio.TimeoutError:
                        pass
                        
                await asyncio.sleep(0.1)  # Monitoring interval
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                
    def _process_event_async(self, event: MonitorEvent) -> None:
        """Process an event asynchronously"""
        # Call event handlers
        for handler in self.event_handlers.get(event.event_type, []):
            try:
                handler(event)
            except Exception as e:
                self.logger.error(f"Error in event handler: {e}")
                
    def log_event(self, event_type: EventType, processor_id: Optional[str] = None, 
                  data: Optional[Dict[str, Any]] = None, level: LogLevel = LogLevel.INFO) -> None:
        """Log a monitoring event"""
        if not self.is_monitoring:
            return
            
        # Check log level
        if level.value == "debug" and self.log_level != LogLevel.DEBUG:
            return
            
        event = MonitorEvent(event_type, processor_id, data, level)
        
        with self.events_lock:
            self.events.append(event)
            self.metrics["total_events"] += 1
            self.metrics["events_by_type"][event_type.value] += 1
            if processor_id:
                self.metrics["events_by_processor"][processor_id] += 1
                
        # Log to Python logger
        log_message = f"[{event_type.value}] {processor_id or 'SYSTEM'}: {data}"
        getattr(self.logger, level.value)(log_message)
        
        # Queue for async processing
        if self._event_queue:
            try:
                self._event_queue.put_nowait(event)
            except asyncio.QueueFull:
                pass  # Drop event if queue is full
                
    def tap_pipe(self, pipe: PipeInterface, callback: Callable[[Any], None]) -> str:
        """Tap into a pipe to monitor data flow"""
        tap_id = str(uuid.uuid4())
        
        with self.taps_lock:
            tap = PipeTap(tap_id, pipe, callback)
            self.taps[tap_id] = tap
            
            # Add tap to the pipe
            pipe.add_tap(tap_id, tap.on_data)
            
        self.log_event(EventType.PIPE_DATA_FLOW, None, 
                      {"action": "tap_added", "tap_id": tap_id, "pipe_id": getattr(pipe, 'pipe_id', 'unknown')})
        
        return tap_id
        
    def untap_pipe(self, tap_id: str) -> None:
        """Remove a pipe tap"""
        with self.taps_lock:
            if tap_id in self.taps:
                tap = self.taps[tap_id]
                tap.pipe.remove_tap(tap_id)
                del self.taps[tap_id]
                
                self.log_event(EventType.PIPE_DATA_FLOW, None,
                              {"action": "tap_removed", "tap_id": tap_id})
                
    def register_processor(self, processor: ProcessorInterface) -> None:
        """Register a processor for monitoring"""
        self.processors[processor.processor_id] = processor
        
        # Add callbacks to processor
        processor.add_callback("start", lambda p: self.log_event(
            EventType.PROCESSOR_START, p.processor_id, {"state": "started"}
        ))
        processor.add_callback("complete", lambda p: self.log_event(
            EventType.PROCESSOR_COMPLETE, p.processor_id, {"state": "completed"}
        ))
        processor.add_callback("error", lambda p: self.log_event(
            EventType.PROCESSOR_ERROR, p.processor_id, 
            {"state": "error", "error": str(p.last_error) if p.last_error else None}, LogLevel.ERROR
        ))
        
    def get_processor_status(self, processor_id: str) -> Dict[str, Any]:
        """Get current status of a processor"""
        if processor_id not in self.processors:
            return {"error": "Processor not found"}
            
        processor = self.processors[processor_id]
        return processor.get_metrics()
        
    def get_pipe_status(self, pipe: PipeInterface) -> Dict[str, Any]:
        """Get current status of a pipe"""
        return pipe.get_status()
        
    def get_all_processor_statuses(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all registered processors"""
        return {
            processor_id: processor.get_metrics() 
            for processor_id, processor in self.processors.items()
        }
        
    def get_tap_statuses(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all pipe taps"""
        with self.taps_lock:
            return {tap_id: tap.get_status() for tap_id, tap in self.taps.items()}
            
    def get_recent_events(self, count: int = 100, event_type: Optional[EventType] = None,
                         processor_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get recent events with optional filtering"""
        with self.events_lock:
            events = list(self.events)
            
        # Apply filters
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        if processor_id:
            events = [e for e in events if e.processor_id == processor_id]
            
        # Return most recent
        return [e.to_dict() for e in events[-count:]]
        
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary"""
        with self.events_lock:
            recent_events = list(self.events)[-100:]  # Last 100 events
            
        return {
            "monitoring_status": "active" if self.is_monitoring else "inactive",
            "uptime_seconds": self.metrics["uptime_seconds"],
            "total_events": self.metrics["total_events"],
            "events_by_type": dict(self.metrics["events_by_type"]),
            "events_by_processor": dict(self.metrics["events_by_processor"]),
            "active_processors": len(self.processors),
            "active_taps": len(self.taps),
            "recent_event_rate": len(recent_events) / 60 if recent_events else 0,  # events per minute
            "processor_statuses": {
                pid: {"state": p.state.value, "items_processed": p.total_items_processed}
                for pid, p in self.processors.items()
            }
        }
        
    def add_event_handler(self, event_type: EventType, handler: Callable[[MonitorEvent], None]) -> None:
        """Add a custom event handler"""
        self.event_handlers[event_type].append(handler)
        
    def remove_event_handler(self, event_type: EventType, handler: Callable[[MonitorEvent], None]) -> None:
        """Remove a custom event handler"""
        if handler in self.event_handlers[event_type]:
            self.event_handlers[event_type].remove(handler)
            
    def export_events(self, format: str = "json") -> str:
        """Export events for external analysis"""
        with self.events_lock:
            events_data = [event.to_dict() for event in self.events]
            
        if format.lower() == "json":
            return json.dumps(events_data, indent=2)
        else:
            raise ValueError(f"Unsupported export format: {format}")
            
    def clear_events(self) -> None:
        """Clear all stored events"""
        with self.events_lock:
            self.events.clear()
            self.metrics["total_events"] = 0
            self.metrics["events_by_type"].clear()
            self.metrics["events_by_processor"].clear()


class AlertManager:
    """Manages alerts and notifications based on monitoring events"""
    
    def __init__(self, monitor: Monitor):
        self.monitor = monitor
        self.alert_rules: List[Dict[str, Any]] = []
        self.active_alerts: Dict[str, Dict[str, Any]] = {}
        
    def add_alert_rule(self, name: str, condition: Callable[[MonitorEvent], bool],
                      action: Callable[[MonitorEvent], None], cooldown_seconds: int = 60) -> None:
        """Add an alert rule"""
        rule = {
            "name": name,
            "condition": condition,
            "action": action,
            "cooldown_seconds": cooldown_seconds,
            "last_triggered": None
        }
        self.alert_rules.append(rule)
        
        # Add event handler to monitor
        self.monitor.add_event_handler(EventType.PROCESSOR_ERROR, self._check_alerts)
        self.monitor.add_event_handler(EventType.PIPE_FULL, self._check_alerts)
        
    def _check_alerts(self, event: MonitorEvent) -> None:
        """Check if any alert rules are triggered"""
        for rule in self.alert_rules:
            try:
                if rule["condition"](event):
                    # Check cooldown
                    now = datetime.now()
                    if (rule["last_triggered"] is None or 
                        (now - rule["last_triggered"]).total_seconds() >= rule["cooldown_seconds"]):
                        
                        rule["action"](event)
                        rule["last_triggered"] = now
                        
                        # Track active alert
                        alert_id = f"{rule['name']}_{event.event_id}"
                        self.active_alerts[alert_id] = {
                            "rule_name": rule["name"],
                            "event": event.to_dict(),
                            "triggered_at": now.isoformat()
                        }
                        
            except Exception as e:
                self.monitor.logger.error(f"Error checking alert rule {rule['name']}: {e}")
                
    def get_active_alerts(self) -> Dict[str, Dict[str, Any]]:
        """Get currently active alerts"""
        return self.active_alerts.copy()
        
    def clear_alert(self, alert_id: str) -> None:
        """Clear a specific alert"""
        self.active_alerts.pop(alert_id, None) 