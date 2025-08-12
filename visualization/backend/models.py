"""
Data models for the FastAPI backend.
"""

import time
import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class ProcessorStatus(str, Enum):
    """Status of a processor"""
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


class MonitoringEventModel(BaseModel):
    """Monitoring event model for API"""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
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
    created_at: float = Field(default_factory=time.time)
    last_activity: Optional[float] = None


class PipeInfo(BaseModel):
    """Information about a pipe"""
    pipe_id: str
    pipe_type: str
    source_processor: Optional[str] = None
    target_processor: Optional[str] = None
    queue_size: int = 0
    max_size: int = -1
    total_messages: int = 0
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
    throughput: float = 0.0
    created_at: float = Field(default_factory=time.time)


class SessionInfo(BaseModel):
    """Session information for API responses"""
    session_id: str
    session_name: str
    description: Optional[str] = None
    start_time: float = Field(default_factory=time.time)
    end_time: Optional[float] = None
    status: str = "active"
    total_processors: int = 0
    total_events: int = 0
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @property
    def duration(self) -> float:
        """Get session duration in seconds"""
        end = self.end_time or time.time()
        return end - self.start_time
    
    @property
    def created_at_iso(self) -> str:
        """Get creation time in ISO format"""
        return datetime.fromtimestamp(self.start_time).isoformat()


class GraphNode(BaseModel):
    """Node in the execution graph"""
    id: str
    label: str
    processor_type: str
    status: ProcessorStatus = ProcessorStatus.IDLE
    position: Optional[Dict[str, float]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class GraphEdge(BaseModel):
    """Edge in the execution graph"""
    id: str
    source: str
    target: str
    pipe_id: Optional[str] = None
    queue_size: int = 0
    total_messages: int = 0


class GraphStructure(BaseModel):
    """Complete graph structure for visualization"""
    nodes: List[GraphNode] = Field(default_factory=list)
    edges: List[GraphEdge] = Field(default_factory=list)
    layout: Optional[str] = "hierarchical"
    metadata: Dict[str, Any] = Field(default_factory=dict)