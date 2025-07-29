"""
Pipe system for data flow between processors in the v2 processing pipeline
"""

import asyncio
import threading
import uuid
from collections import deque
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime
from enum import Enum

from .interfaces import PipeInterface, PipeType
from .errors import PipeError, TimeoutError


class PipeState(Enum):
    """States a pipe can be in"""
    OPEN = "open"
    CLOSED = "closed"
    FULL = "full"
    ERROR = "error"


class Pipe:
    """
    Core pipe implementation for data flow between processors.
    
    Supports both synchronous and asynchronous operations with monitoring capabilities.
    """
    
    def __init__(self, pipe_id: Optional[str] = None, max_size: int = 1000, 
                 pipe_type: PipeType = PipeType.BIDIRECTIONAL):
        self.pipe_id = pipe_id or str(uuid.uuid4())
        self.max_size = max_size
        self.pipe_type = pipe_type
        self.state = PipeState.OPEN
        
        # Data storage
        self._queue = deque(maxlen=max_size)
        self._async_queue = asyncio.Queue(maxsize=max_size)
        
        # Synchronization
        self._lock = threading.RLock()
        self._async_lock = asyncio.Lock()
        
        # Monitoring
        self._taps: Dict[str, Callable[[Any], None]] = {}
        self._metadata_queue = deque(maxlen=max_size)
        
        # Statistics  
        self.created_at = datetime.now()
        self.total_items_processed = 0
        self.total_bytes_processed = 0
        self.last_activity = self.created_at
        
    def put(self, data: Any, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Put data into the pipe synchronously"""
        if self.state != PipeState.OPEN:
            raise PipeError(f"Cannot put data into {self.state.value} pipe", self.pipe_id, "put")
            
        with self._lock:
            if len(self._queue) >= self.max_size:
                self.state = PipeState.FULL
                raise PipeError(f"Pipe is full (max_size: {self.max_size})", self.pipe_id, "put")
                
            self._queue.append(data)
            self._metadata_queue.append(metadata or {})
            
            # Update statistics
            self.total_items_processed += 1
            self.last_activity = datetime.now()
            
            try:
                self.total_bytes_processed += len(str(data).encode('utf-8'))
            except:
                pass  # Skip if can't calculate bytes
                
            # Notify taps
            self._notify_taps(data)
            
    async def aput(self, data: Any, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Put data into the pipe asynchronously"""
        if self.state != PipeState.OPEN:
            raise PipeError(f"Cannot put data into {self.state.value} pipe", self.pipe_id, "aput")
            
        async with self._async_lock:
            try:
                await self._async_queue.put(data)
                
                # Update statistics
                self.total_items_processed += 1
                self.last_activity = datetime.now()
                
                try:
                    self.total_bytes_processed += len(str(data).encode('utf-8'))
                except:
                    pass
                    
                # Notify taps
                self._notify_taps(data)
                
            except asyncio.QueueFull:
                self.state = PipeState.FULL  
                raise PipeError(f"Async pipe is full (max_size: {self.max_size})", self.pipe_id, "aput")
                
    def get(self, timeout: Optional[float] = None) -> Any:
        """Get data from the pipe synchronously"""
        if self.state == PipeState.CLOSED and self.is_empty():
            raise PipeError("Cannot get data from closed empty pipe", self.pipe_id, "get")
            
        with self._lock:
            if not self._queue:
                if timeout is not None:
                    raise TimeoutError(f"No data available within {timeout}s", timeout, context={"pipe_id": self.pipe_id})
                raise PipeError("No data available in pipe", self.pipe_id, "get")
                
            data = self._queue.popleft()
            self._metadata_queue.popleft()  # Remove corresponding metadata
            
            # Reset full state if we were full
            if self.state == PipeState.FULL:
                self.state = PipeState.OPEN
                
            self.last_activity = datetime.now()
            return data
            
    async def aget(self, timeout: Optional[float] = None) -> Any:
        """Get data from the pipe asynchronously"""
        if self.state == PipeState.CLOSED and await self._async_queue.empty():
            raise PipeError("Cannot get data from closed empty pipe", self.pipe_id, "aget")
            
        try:
            if timeout:
                data = await asyncio.wait_for(self._async_queue.get(), timeout=timeout)
            else:
                data = await self._async_queue.get()
                
            self.last_activity = datetime.now()
            return data
            
        except asyncio.TimeoutError:
            raise TimeoutError(f"No data available within {timeout}s", timeout or 0, context={"pipe_id": self.pipe_id})
            
    def is_empty(self) -> bool:
        """Check if pipe is empty"""
        with self._lock:
            return len(self._queue) == 0
            
    async def ais_empty(self) -> bool:
        """Check if async pipe is empty"""
        return self._async_queue.empty()
        
    def size(self) -> int:
        """Get number of items in pipe"""
        with self._lock:
            return len(self._queue)
            
    async def asize(self) -> int:
        """Get number of items in async pipe"""
        return self._async_queue.qsize()
        
    def close(self) -> None:
        """Close the pipe"""
        self.state = PipeState.CLOSED
        
    def add_tap(self, tap_id: str, callback: Callable[[Any], None]) -> None:
        """Add a monitoring tap to the pipe"""
        self._taps[tap_id] = callback
        
    def remove_tap(self, tap_id: str) -> None:
        """Remove a monitoring tap"""
        self._taps.pop(tap_id, None)
        
    def _notify_taps(self, data: Any) -> None:
        """Notify all taps of new data"""
        for tap_callback in self._taps.values():
            try:
                tap_callback(data)
            except Exception:
                # Don't let tap errors affect pipe operation
                pass
                
    def get_status(self) -> Dict[str, Any]:
        """Get current pipe status"""
        return {
            "pipe_id": self.pipe_id,
            "state": self.state.value,
            "pipe_type": self.pipe_type.value,
            "size": self.size(),
            "max_size": self.max_size,
            "is_empty": self.is_empty(),
            "is_full": self.size() >= self.max_size,
            "created_at": self.created_at.isoformat(),
            "last_activity": self.last_activity.isoformat(),
            "total_items_processed": self.total_items_processed,
            "total_bytes_processed": self.total_bytes_processed,
            "active_taps": len(self._taps)
        }
        
    def get_metrics(self) -> Dict[str, Any]:
        """Get detailed metrics about the pipe"""
        now = datetime.now()
        uptime = (now - self.created_at).total_seconds()
        time_since_activity = (now - self.last_activity).total_seconds()
        
        return {
            "pipe_id": self.pipe_id,
            "uptime_seconds": uptime,
            "time_since_last_activity_seconds": time_since_activity,
            "throughput_items_per_second": self.total_items_processed / uptime if uptime > 0 else 0,
            "throughput_bytes_per_second": self.total_bytes_processed / uptime if uptime > 0 else 0,
            "current_utilization": self.size() / self.max_size if self.max_size > 0 else 0,
            "total_items_processed": self.total_items_processed,
            "total_bytes_processed": self.total_bytes_processed
        }
        
    def clear(self) -> None:
        """Clear all data from the pipe"""
        with self._lock:
            self._queue.clear()
            self._metadata_queue.clear()
            
        # Clear async queue
        while not self._async_queue.empty():
            try:
                self._async_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
                
        if self.state == PipeState.FULL:
            self.state = PipeState.OPEN


class PipeFactory:
    """Factory for creating different types of pipes"""
    
    @staticmethod
    def create_pipe(pipe_type: PipeType = PipeType.BIDIRECTIONAL, 
                   max_size: int = 1000, pipe_id: Optional[str] = None) -> Pipe:
        """Create a standard pipe"""
        return Pipe(pipe_id=pipe_id, max_size=max_size, pipe_type=pipe_type)
        
    @staticmethod
    def create_buffered_pipe(buffer_size: int = 10000, pipe_id: Optional[str] = None) -> Pipe:
        """Create a large buffered pipe for high-throughput scenarios"""
        return Pipe(pipe_id=pipe_id, max_size=buffer_size, pipe_type=PipeType.BIDIRECTIONAL)
        
    @staticmethod
    def create_streaming_pipe(pipe_id: Optional[str] = None) -> Pipe:
        """Create a small pipe optimized for streaming (low latency)"""
        return Pipe(pipe_id=pipe_id, max_size=10, pipe_type=PipeType.BIDIRECTIONAL)


class PipeNetwork:
    """Manages a network of connected pipes"""
    
    def __init__(self):
        self.pipes: Dict[str, Pipe] = {}
        self.connections: Dict[str, List[str]] = {}  # pipe_id -> list of connected pipe_ids
        
    def add_pipe(self, pipe: Pipe) -> None:
        """Add a pipe to the network"""
        self.pipes[pipe.pipe_id] = pipe
        if pipe.pipe_id not in self.connections:
            self.connections[pipe.pipe_id] = []
            
    def connect_pipes(self, from_pipe_id: str, to_pipe_id: str) -> None:
        """Connect two pipes in the network"""
        if from_pipe_id not in self.pipes or to_pipe_id not in self.pipes:
            raise PipeError("Cannot connect non-existent pipes", from_pipe_id, "connect")
            
        self.connections[from_pipe_id].append(to_pipe_id)
        
    def get_network_status(self) -> Dict[str, Any]:
        """Get status of entire pipe network"""
        return {
            "total_pipes": len(self.pipes),
            "total_connections": sum(len(conns) for conns in self.connections.values()),
            "pipe_statuses": {pipe_id: pipe.get_status() for pipe_id, pipe in self.pipes.items()},
            "connections": self.connections
        } 