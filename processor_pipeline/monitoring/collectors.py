"""
Data collectors for gathering monitoring information from processors and pipes.
"""

import time
from typing import Any, Dict, List, Optional
from collections import defaultdict, deque

from .models import ProcessorMetrics, ProcessorStatus, PipeInfo, ProcessorInfo


class MetricsCollector:
    """Collects and aggregates performance metrics"""
    
    def __init__(self, max_history: int = 100):
        self.max_history = max_history
        self.processor_metrics: Dict[str, ProcessorMetrics] = {}
        self.processing_times: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_history))
        self.throughput_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_history))
        
    def record_processor_start(self, processor_id: str, start_time: float):
        """Record when a processor starts processing"""
        if processor_id not in self.processor_metrics:
            self.processor_metrics[processor_id] = ProcessorMetrics(processor_id=processor_id)
        
        metrics = self.processor_metrics[processor_id]
        metrics.last_activity = start_time
    
    def record_processor_complete(self, processor_id: str, start_time: float, end_time: float):
        """Record when a processor completes processing"""
        processing_time = end_time - start_time
        
        if processor_id not in self.processor_metrics:
            self.processor_metrics[processor_id] = ProcessorMetrics(processor_id=processor_id)
            
        metrics = self.processor_metrics[processor_id]
        metrics.total_processed += 1
        metrics.last_activity = end_time
        
        # Update processing time statistics
        times = self.processing_times[processor_id]
        times.append(processing_time)
        
        if times:
            metrics.processing_time_avg = sum(times) / len(times)
            metrics.processing_time_min = min(times)
            metrics.processing_time_max = max(times)
        
        # Calculate throughput (items per second over last minute)
        current_time = time.time()
        throughput_window = 60.0  # seconds
        
        throughput_times = self.throughput_history[processor_id]
        throughput_times.append(current_time)
        
        # Remove old entries
        cutoff_time = current_time - throughput_window
        while throughput_times and throughput_times[0] < cutoff_time:
            throughput_times.popleft()
            
        metrics.throughput = len(throughput_times) / throughput_window if throughput_times else 0.0
    
    def record_processor_error(self, processor_id: str):
        """Record a processor error"""
        if processor_id not in self.processor_metrics:
            self.processor_metrics[processor_id] = ProcessorMetrics(processor_id=processor_id)
            
        self.processor_metrics[processor_id].error_count += 1
    
    def get_metrics(self, processor_id: str) -> Optional[ProcessorMetrics]:
        """Get metrics for a specific processor"""
        return self.processor_metrics.get(processor_id)
    
    def get_all_metrics(self) -> Dict[str, ProcessorMetrics]:
        """Get metrics for all processors"""
        return self.processor_metrics.copy()


class DataCollector:
    """Collects data samples for debugging and analysis"""
    
    def __init__(self, max_samples: int = 50):
        self.max_samples = max_samples
        self.input_samples: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_samples))
        self.output_samples: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_samples))
    
    def record_input(self, processor_id: str, data: Any, timestamp: float):
        """Record input data for a processor"""
        sample = {
            "timestamp": timestamp,
            "data": self._serialize_data(data),
            "data_type": type(data).__name__
        }
        self.input_samples[processor_id].append(sample)
    
    def record_output(self, processor_id: str, data: Any, timestamp: float):
        """Record output data for a processor"""
        sample = {
            "timestamp": timestamp,
            "data": self._serialize_data(data),
            "data_type": type(data).__name__
        }
        self.output_samples[processor_id].append(sample)
    
    def get_input_samples(self, processor_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent input samples for a processor"""
        samples = list(self.input_samples.get(processor_id, []))
        return samples[-limit:] if samples else []
    
    def get_output_samples(self, processor_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent output samples for a processor"""
        samples = list(self.output_samples.get(processor_id, []))
        return samples[-limit:] if samples else []
    
    def _serialize_data(self, data: Any) -> Any:
        """Serialize data for storage, handling complex types"""
        try:
            # Try to convert to a serializable format
            if hasattr(data, 'model_dump'):  # Pydantic model
                return data.model_dump()
            elif hasattr(data, '__dict__'):  # Regular object
                return str(data)
            elif isinstance(data, (str, int, float, bool, list, dict)):
                return data
            else:
                return str(data)
        except Exception:
            return f"<{type(data).__name__} object>"


class SessionTracker:
    """Tracks session-level information"""
    
    def __init__(self, session_id: str, session_name: str):
        self.session_id = session_id
        self.session_name = session_name
        self.start_time = time.time()
        self.end_time: Optional[float] = None
        
        self.processors: Dict[str, ProcessorInfo] = {}
        self.pipes: Dict[str, PipeInfo] = {}
        self.processor_status: Dict[str, ProcessorStatus] = {}
        
        self.total_events = 0
        self.event_history: deque = deque(maxlen=1000)
    
    def add_processor(self, processor_id: str, processor_type: str, metadata: Dict[str, Any] = None):
        """Add a processor to the session"""
        self.processors[processor_id] = ProcessorInfo(
            processor_id=processor_id,
            processor_type=processor_type,
            metadata=metadata or {}
        )
        self.processor_status[processor_id] = ProcessorStatus.IDLE
    
    def add_pipe(self, pipe_id: str, pipe_type: str, source: str = None, target: str = None):
        """Add a pipe to the session"""
        self.pipes[pipe_id] = PipeInfo(
            pipe_id=pipe_id,
            pipe_type=pipe_type,
            source_processor=source,
            target_processor=target
        )
    
    def update_processor_status(self, processor_id: str, status: ProcessorStatus):
        """Update processor status"""
        if processor_id in self.processors:
            self.processors[processor_id].status = status
            self.processor_status[processor_id] = status
    
    def update_pipe_stats(self, pipe_id: str, queue_size: int, statistics: Dict[str, Any] = None):
        """Update pipe statistics"""
        if pipe_id in self.pipes:
            self.pipes[pipe_id].queue_size = queue_size
            if statistics:
                self.pipes[pipe_id].statistics.update(statistics)
    
    def record_event(self, event_type: str, data: Dict[str, Any]):
        """Record an event in the session history"""
        self.total_events += 1
        self.event_history.append({
            "timestamp": time.time(),
            "event_type": event_type,
            "data": data
        })
    
    def end_session(self):
        """Mark the session as ended"""
        self.end_time = time.time()
    
    def get_session_info(self) -> Dict[str, Any]:
        """Get complete session information"""
        return {
            "session_id": self.session_id,
            "session_name": self.session_name,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration": (self.end_time or time.time()) - self.start_time,
            "processors": list(self.processors.values()),
            "pipes": list(self.pipes.values()),
            "total_events": self.total_events,
            "status": "completed" if self.end_time else "active"
        }