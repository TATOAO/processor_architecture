"""
Data models for monitoring events and metrics.
"""

import time
import uuid
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field


class ProcessorStatus(str, Enum):
    """Status of a processor during execution"""
    IDLE = "idle"
    PROCESSING = "processing" 
    COMPLETED = "completed"
    ERROR = "error"
    WAITING = "waiting"


class EventType(str, Enum):
    """Types of monitoring events"""
    SESSION_START = "session_start"
    SESSION_END = "session_end"
    PROCESSOR_START = "processor_start"
    PROCESSOR_COMPLETE = "processor_complete"
    PROCESSOR_ERROR = "processor_error"
    PIPE_UPDATE = "pipe_update"
    METRICS_UPDATE = "metrics_update"


class MonitoringEvent(BaseModel):
    """Base monitoring event"""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    session_id: str
    timestamp: float = Field(default_factory=time.time)
    data: Dict[str, Any] = Field(default_factory=dict)


class ProcessorInfo(BaseModel):
    """Information about a processor"""
    processor_id: str
    processor_type: str
    status: ProcessorStatus = ProcessorStatus.IDLE
    input_pipe_id: Optional[str] = None
    output_pipe_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class PipeInfo(BaseModel):
    """Information about a pipe"""
    pipe_id: str
    pipe_type: str
    source_processor: Optional[str] = None
    target_processor: Optional[str] = None
    queue_size: int = 0
    max_size: int = -1
    statistics: Dict[str, Any] = Field(default_factory=dict)


class ProcessorMetrics(BaseModel):
    """Performance metrics for a processor"""
    processor_id: str
    total_processed: int = 0
    processing_time_avg: float = 0.0
    processing_time_min: float = 0.0
    processing_time_max: float = 0.0
    error_count: int = 0
    last_activity: Optional[float] = None
    throughput: float = 0.0  # items per second


class SessionInfo(BaseModel):
    """Information about a monitoring session"""
    session_id: str
    session_name: str
    start_time: float = Field(default_factory=time.time)
    end_time: Optional[float] = None
    processors: List[ProcessorInfo] = Field(default_factory=list)
    pipes: List[PipeInfo] = Field(default_factory=list)
    total_events: int = 0
    status: str = "active"


class GraphStructure(BaseModel):
    """Graph structure for visualization"""
    nodes: List[Dict[str, Any]] = Field(default_factory=list)
    edges: List[Dict[str, Any]] = Field(default_factory=list)
    layout: Optional[Dict[str, Any]] = None


class MonitoringConfig(BaseModel):
    """Configuration for monitoring"""
    web_ui_url: str = "http://localhost:3000"
    api_url: str = "http://localhost:8000"
    session_name: Optional[str] = None
    collect_input_data: bool = False
    collect_output_data: bool = False
    metrics_interval: float = 1.0
    max_events_buffer: int = 1000
    enable_websocket: bool = True