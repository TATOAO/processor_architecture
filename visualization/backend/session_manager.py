"""
Session manager for tracking monitoring sessions and their state.
"""

import time
import uuid
from collections import defaultdict, deque
from typing import Dict, List, Optional, Any

from models import (
    SessionInfo, ProcessorInfo, PipeInfo, ProcessorMetrics, 
    MonitoringEventModel, ProcessorStatus, GraphStructure, GraphNode, GraphEdge
)


class MonitoringSession:
    """Represents a single monitoring session"""
    
    def __init__(self, session_id: str, session_name: str, description: str = None, metadata: Dict = None):
        self.session_id = session_id
        self.session_name = session_name
        self.description = description
        self.start_time = time.time()
        self.end_time: Optional[float] = None
        self.status = "active"
        self.metadata = metadata or {}
        
        # Processors and pipes
        self.processors: Dict[str, ProcessorInfo] = {}
        self.pipes: Dict[str, PipeInfo] = {}
        self.processor_metrics: Dict[str, ProcessorMetrics] = {}
        
        # Events
        self.total_events = 0
        self.event_history: deque = deque(maxlen=1000)
        
        # Graph structure
        self._graph_cache: Optional[GraphStructure] = None
        self._graph_dirty = True
    
    def process_event(self, event: MonitoringEventModel):
        """Process a monitoring event and update session state"""
        self.total_events += 1
        self.event_history.append(event)
        
        event_type = event.event_type
        data = event.data
        
        if event_type == "session_start":
            self._handle_session_start(data)
        elif event_type == "session_end":
            self._handle_session_end(data)
        elif event_type == "processor_start":
            self._handle_processor_start(data)
        elif event_type == "processor_complete":
            self._handle_processor_complete(data)
        elif event_type == "processor_error":
            self._handle_processor_error(data)
        elif event_type == "pipe_update":
            self._handle_pipe_update(data)
        elif event_type == "metrics_update":
            self._handle_metrics_update(data)
        
        # Mark graph as dirty for recalculation
        self._graph_dirty = True
    
    def _handle_session_start(self, data: Dict[str, Any]):
        """Handle session start event"""
        if "config" in data:
            self.metadata.update(data["config"])
    
    def _handle_session_end(self, data: Dict[str, Any]):
        """Handle session end event"""
        self.end_time = time.time()
        self.status = "completed"
    
    def _handle_processor_start(self, data: Dict[str, Any]):
        """Handle processor start event"""
        processor_id = data.get("processor_id")
        if processor_id:
            if processor_id not in self.processors:
                self.processors[processor_id] = ProcessorInfo(
                    processor_id=processor_id,
                    processor_type=data.get("processor_type", "Unknown")
                )
            
            self.processors[processor_id].status = ProcessorStatus.PROCESSING
            self.processors[processor_id].last_activity = data.get("timestamp", time.time())
    
    def _handle_processor_complete(self, data: Dict[str, Any]):
        """Handle processor complete event"""
        processor_id = data.get("processor_id")
        if processor_id and processor_id in self.processors:
            self.processors[processor_id].status = ProcessorStatus.COMPLETED
            self.processors[processor_id].last_activity = time.time()
            
            # Update metrics if provided
            if "metrics" in data:
                metrics_data = data["metrics"]
                if processor_id not in self.processor_metrics:
                    self.processor_metrics[processor_id] = ProcessorMetrics(processor_id=processor_id)
                
                # Update metrics fields
                metrics = self.processor_metrics[processor_id]
                for key, value in metrics_data.items():
                    if hasattr(metrics, key):
                        setattr(metrics, key, value)
    
    def _handle_processor_error(self, data: Dict[str, Any]):
        """Handle processor error event"""
        processor_id = data.get("processor_id")
        if processor_id and processor_id in self.processors:
            self.processors[processor_id].status = ProcessorStatus.ERROR
            self.processors[processor_id].last_activity = time.time()
            
            # Update error count in metrics
            if processor_id not in self.processor_metrics:
                self.processor_metrics[processor_id] = ProcessorMetrics(processor_id=processor_id)
            
            self.processor_metrics[processor_id].error_count += 1
    
    def _handle_pipe_update(self, data: Dict[str, Any]):
        """Handle pipe update event"""
        pipe_id = data.get("pipe_id")
        if pipe_id:
            if pipe_id not in self.pipes:
                self.pipes[pipe_id] = PipeInfo(
                    pipe_id=pipe_id,
                    pipe_type=data.get("pipe_type", "Unknown"),
                    source_processor=data.get("source_processor"),
                    target_processor=data.get("target_processor")
                )
            
            pipe = self.pipes[pipe_id]
            pipe.queue_size = data.get("queue_size", pipe.queue_size)
            if "statistics" in data:
                pipe.statistics.update(data["statistics"])
    
    def _handle_metrics_update(self, data: Dict[str, Any]):
        """Handle metrics update event"""
        if "metrics" in data:
            for processor_id, metrics_data in data["metrics"].items():
                if processor_id not in self.processor_metrics:
                    self.processor_metrics[processor_id] = ProcessorMetrics(processor_id=processor_id)
                
                # Update metrics
                metrics = self.processor_metrics[processor_id]
                for key, value in metrics_data.items():
                    if hasattr(metrics, key):
                        setattr(metrics, key, value)
        
        # Update active processor statuses
        if "active_processors" in data:
            active_set = set(data["active_processors"])
            for processor_id, processor in self.processors.items():
                if processor_id in active_set:
                    processor.status = ProcessorStatus.PROCESSING
                elif processor.status == ProcessorStatus.PROCESSING:
                    processor.status = ProcessorStatus.IDLE
    
    def get_processors(self) -> List[ProcessorInfo]:
        """Get all processors in the session"""
        return list(self.processors.values())
    
    def get_processor(self, processor_id: str) -> Optional[ProcessorInfo]:
        """Get a specific processor"""
        return self.processors.get(processor_id)
    
    def get_processor_metrics(self, processor_id: str) -> Optional[ProcessorMetrics]:
        """Get metrics for a specific processor"""
        return self.processor_metrics.get(processor_id)
    
    def get_all_metrics(self) -> Dict[str, ProcessorMetrics]:
        """Get metrics for all processors"""
        return {pid: metrics.model_dump() for pid, metrics in self.processor_metrics.items()}
    
    def get_recent_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent events"""
        events = list(self.event_history)
        return [event.model_dump() for event in events[-limit:]]
    
    def get_graph_structure(self) -> GraphStructure:
        """Get the graph structure for visualization"""
        if self._graph_cache and not self._graph_dirty:
            return self._graph_cache
        
        # Build nodes from processors
        nodes = []
        for processor_id, processor in self.processors.items():
            nodes.append(GraphNode(
                id=processor_id,
                label=processor.processor_type,
                processor_type=processor.processor_type,
                status=processor.status,
                metadata=processor.metadata
            ))
        
        # Build edges from pipes
        edges = []
        for pipe_id, pipe in self.pipes.items():
            if pipe.source_processor and pipe.target_processor:
                edges.append(GraphEdge(
                    id=pipe_id,
                    source=pipe.source_processor,
                    target=pipe.target_processor,
                    pipe_id=pipe_id,
                    queue_size=pipe.queue_size,
                    total_messages=pipe.total_messages
                ))
        
        # Create graph structure
        self._graph_cache = GraphStructure(
            nodes=nodes,
            edges=edges,
            metadata={
                "session_id": self.session_id,
                "total_processors": len(nodes),
                "total_connections": len(edges)
            }
        )
        
        self._graph_dirty = False
        return self._graph_cache
    
    def to_session_info(self) -> SessionInfo:
        """Convert to SessionInfo model"""
        return SessionInfo(
            session_id=self.session_id,
            session_name=self.session_name,
            description=self.description,
            start_time=self.start_time,
            end_time=self.end_time,
            status=self.status,
            total_processors=len(self.processors),
            total_events=self.total_events,
            metadata=self.metadata
        )


class SessionManager:
    """Manages multiple monitoring sessions"""
    
    def __init__(self):
        self.sessions: Dict[str, MonitoringSession] = {}
        self.session_index: Dict[str, str] = {}  # name -> id mapping
    
    def create_session(
        self, 
        session_name: str, 
        session_id: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[Dict] = None
    ) -> MonitoringSession:
        """Create a new monitoring session"""
        if session_id is None:
            session_id = str(uuid.uuid4())
        
        if session_id in self.sessions:
            raise ValueError(f"Session with ID {session_id} already exists")
        
        session = MonitoringSession(session_id, session_name, description, metadata)
        self.sessions[session_id] = session
        self.session_index[session_name] = session_id
        
        return session
    
    def get_session(self, session_id: str) -> Optional[MonitoringSession]:
        """Get a session by ID"""
        return self.sessions.get(session_id)
    
    def get_session_by_name(self, session_name: str) -> Optional[MonitoringSession]:
        """Get a session by name"""
        session_id = self.session_index.get(session_name)
        return self.sessions.get(session_id) if session_id else None
    
    def get_all_sessions(self) -> Dict[str, MonitoringSession]:
        """Get all sessions"""
        return self.sessions.copy()
    
    def delete_session(self, session_id: str) -> bool:
        """Delete a session"""
        if session_id not in self.sessions:
            return False
        
        session = self.sessions[session_id]
        # Remove from name index
        if session.session_name in self.session_index:
            del self.session_index[session.session_name]
        
        # Remove session
        del self.sessions[session_id]
        return True
    
    def cleanup_old_sessions(self, max_age_hours: int = 24):
        """Clean up old completed sessions"""
        current_time = time.time()
        cutoff_time = current_time - (max_age_hours * 3600)
        
        sessions_to_remove = []
        for session_id, session in self.sessions.items():
            if (session.status == "completed" and 
                (session.end_time or session.start_time) < cutoff_time):
                sessions_to_remove.append(session_id)
        
        for session_id in sessions_to_remove:
            self.delete_session(session_id)