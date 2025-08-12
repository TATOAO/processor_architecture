"""
Monitoring hooks that integrate with the existing processor and pipe interfaces.
"""

import asyncio
import time
import uuid
from typing import Any, Dict, Optional, Set
from functools import wraps

from ..new_core.core_interfaces import ProcessorInterface, PipeInterface
from .models import (
    MonitoringEvent, EventType, ProcessorStatus, 
    MonitoringConfig, ProcessorInfo, PipeInfo
)
from .client import MonitoringClient
from .collectors import MetricsCollector, DataCollector, SessionTracker


class MonitoringHooks:
    """Hooks into processor and pipe lifecycle events"""
    
    def __init__(self, config: MonitoringConfig, client: MonitoringClient):
        self.config = config
        self.client = client
        self.session_id = str(uuid.uuid4())
        self.session_name = config.session_name or f"session_{self.session_id[:8]}"
        
        # Collectors
        self.metrics_collector = MetricsCollector()
        self.data_collector = DataCollector() if config.collect_input_data or config.collect_output_data else None
        self.session_tracker = SessionTracker(self.session_id, self.session_name)
        
        # Tracking state
        self.active_processors: Set[str] = set()
        self.processing_start_times: Dict[str, float] = {}
        
        # Background tasks
        self.metrics_task: Optional[asyncio.Task] = None
        self.running = False
    
    async def start(self):
        """Start monitoring hooks"""
        self.running = True
        
        # Start the monitoring client
        await self.client.start()
        
        # Register the session
        session_info = self.session_tracker.get_session_info()
        await self.client.register_session(session_info)
        
        # Send session start event
        await self._send_event(EventType.SESSION_START, {
            "session_id": self.session_id,
            "session_name": self.session_name,
            "config": self.config.model_dump()
        })
        
        # Start metrics collection task
        if self.config.metrics_interval > 0:
            self.metrics_task = asyncio.create_task(self._metrics_collection_loop())
    
    async def stop(self):
        """Stop monitoring hooks"""
        self.running = False
        
        if self.metrics_task:
            self.metrics_task.cancel()
            try:
                await self.metrics_task
            except asyncio.CancelledError:
                pass
        
        # Send session end event
        self.session_tracker.end_session()
        await self._send_event(EventType.SESSION_END, {
            "session_info": self.session_tracker.get_session_info()
        })
        
        await self.client.stop()
    
    def register_processor(self, processor: ProcessorInterface):
        """Register a processor for monitoring"""
        processor_id = processor.processor_id
        processor_type = processor._meta.get("name", type(processor).__name__)
        
        # Add to session tracker
        self.session_tracker.add_processor(
            processor_id, 
            processor_type, 
            processor._meta
        )
        
        # Hook into processor methods
        self._hook_processor_methods(processor)
    
    def register_pipe(self, pipe: PipeInterface, source_processor: str = None, target_processor: str = None):
        """Register a pipe for monitoring"""
        pipe_id = getattr(pipe, '_pipe_id', str(id(pipe)))
        pipe_type = type(pipe).__name__
        
        # Add to session tracker
        self.session_tracker.add_pipe(pipe_id, pipe_type, source_processor, target_processor)
        
        # Hook into pipe methods
        self._hook_pipe_methods(pipe)
    
    def _hook_processor_methods(self, processor: ProcessorInterface):
        """Hook into processor lifecycle methods"""
        original_execute = processor.execute
        original_process = processor.process
        processor_id = processor.processor_id
        
        @wraps(original_execute)
        async def hooked_execute(*args, **kwargs):
            await self.on_processor_start(processor_id, args[0] if args else None)
            try:
                result = await original_execute(*args, **kwargs)
                await self.on_processor_complete(processor_id, result)
                return result
            except Exception as e:
                await self.on_processor_error(processor_id, e)
                raise
        
        @wraps(original_process)
        async def hooked_process(*args, **kwargs):
            start_time = time.time()
            self.processing_start_times[f"{processor_id}_{start_time}"] = start_time
            
            try:
                # Handle both sync and async generators
                result = original_process(*args, **kwargs)
                if hasattr(result, '__aiter__'):
                    # Async generator - this function becomes an async generator
                    async for item in result:
                        if self.data_collector and self.config.collect_output_data:
                            self.data_collector.record_output(processor_id, item, time.time())
                        yield item
                    
                    end_time = time.time()
                    self.metrics_collector.record_processor_complete(processor_id, start_time, end_time)
                elif hasattr(result, '__iter__') and not isinstance(result, (str, bytes)):
                    # Regular generator - this function becomes an async generator
                    for item in result:
                        if self.data_collector and self.config.collect_output_data:
                            self.data_collector.record_output(processor_id, item, time.time())
                        yield item
                    
                    end_time = time.time()
                    self.metrics_collector.record_processor_complete(processor_id, start_time, end_time)
                else:
                    # Regular return value - yield it as a single item
                    if self.data_collector and self.config.collect_output_data:
                        self.data_collector.record_output(processor_id, result, time.time())
                    
                    end_time = time.time()
                    self.metrics_collector.record_processor_complete(processor_id, start_time, end_time)
                    yield result
                    
            except Exception as e:
                self.metrics_collector.record_processor_error(processor_id)
                raise
        
        processor.execute = hooked_execute
        processor.process = hooked_process
    
    def _hook_pipe_methods(self, pipe: PipeInterface):
        """Hook into pipe methods"""
        original_put = pipe.put
        original_get = pipe.get
        pipe_id = getattr(pipe, '_pipe_id', str(id(pipe)))
        
        @wraps(original_put)
        async def hooked_put(data):
            result = await original_put(data)
            await self.on_pipe_put(pipe_id, data)
            return result
        
        @wraps(original_get)
        async def hooked_get(*args, **kwargs):
            result = await original_get(*args, **kwargs)
            await self.on_pipe_get(pipe_id, result)
            return result
        
        pipe.put = hooked_put
        pipe.get = hooked_get
    
    async def on_processor_start(self, processor_id: str, input_data: Any):
        """Called when a processor starts processing"""
        self.active_processors.add(processor_id)
        self.session_tracker.update_processor_status(processor_id, ProcessorStatus.PROCESSING)
        
        start_time = time.time()
        self.metrics_collector.record_processor_start(processor_id, start_time)
        
        if self.data_collector and self.config.collect_input_data:
            self.data_collector.record_input(processor_id, input_data, start_time)
        
        await self._send_event(EventType.PROCESSOR_START, {
            "processor_id": processor_id,
            "input_data_summary": self._summarize_data(input_data) if input_data else None,
            "timestamp": start_time
        })
    
    async def on_processor_complete(self, processor_id: str, result: Any):
        """Called when a processor completes processing"""
        self.active_processors.discard(processor_id)
        self.session_tracker.update_processor_status(processor_id, ProcessorStatus.COMPLETED)
        
        await self._send_event(EventType.PROCESSOR_COMPLETE, {
            "processor_id": processor_id,
            "result_summary": self._summarize_data(result) if result else None,
            "metrics": self.metrics_collector.get_metrics(processor_id).model_dump() if self.metrics_collector.get_metrics(processor_id) else None
        })
    
    async def on_processor_error(self, processor_id: str, error: Exception):
        """Called when a processor encounters an error"""
        self.active_processors.discard(processor_id)
        self.session_tracker.update_processor_status(processor_id, ProcessorStatus.ERROR)
        
        await self._send_event(EventType.PROCESSOR_ERROR, {
            "processor_id": processor_id,
            "error_type": type(error).__name__,
            "error_message": str(error)
        })
    
    async def on_pipe_put(self, pipe_id: str, data: Any):
        """Called when data is put into a pipe"""
        # Update pipe statistics
        if pipe_id in self.session_tracker.pipes:
            # Get current queue size if available
            queue_size = getattr(self.session_tracker.pipes[pipe_id], 'queue_size', 0) + 1
            self.session_tracker.update_pipe_stats(pipe_id, queue_size)
    
    async def on_pipe_get(self, pipe_id: str, data: Any):
        """Called when data is retrieved from a pipe"""
        # Update pipe statistics
        if pipe_id in self.session_tracker.pipes:
            queue_size = max(0, getattr(self.session_tracker.pipes[pipe_id], 'queue_size', 1) - 1)
            self.session_tracker.update_pipe_stats(pipe_id, queue_size)
    
    async def _send_event(self, event_type: EventType, data: Dict[str, Any]):
        """Send a monitoring event"""
        event = MonitoringEvent(
            event_type=event_type,
            session_id=self.session_id,
            data=data
        )
        
        self.session_tracker.record_event(event_type.value, data)
        await self.client.send_event(event)
    
    async def _metrics_collection_loop(self):
        """Background task for periodic metrics collection"""
        while self.running:
            try:
                await asyncio.sleep(self.config.metrics_interval)
                
                # Collect current metrics
                all_metrics = self.metrics_collector.get_all_metrics()
                if all_metrics:
                    await self._send_event(EventType.METRICS_UPDATE, {
                        "metrics": {pid: metrics.model_dump() for pid, metrics in all_metrics.items()},
                        "active_processors": list(self.active_processors),
                        "session_info": self.session_tracker.get_session_info()
                    })
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                # Log error but continue
                pass
    
    def _summarize_data(self, data: Any) -> Dict[str, Any]:
        """Create a summary of data for monitoring"""
        try:
            summary = {
                "type": type(data).__name__,
                "size": len(data) if hasattr(data, '__len__') else None,
            }
            
            if isinstance(data, (str, int, float, bool)):
                summary["value"] = data if len(str(data)) < 100 else f"{str(data)[:100]}..."
            elif isinstance(data, (list, tuple)):
                summary["length"] = len(data)
                summary["sample"] = data[:3] if len(data) > 0 else []
            elif isinstance(data, dict):
                summary["keys"] = list(data.keys())[:10]
                summary["size"] = len(data)
            
            return summary
        except Exception:
            return {"type": type(data).__name__, "error": "Could not summarize"}