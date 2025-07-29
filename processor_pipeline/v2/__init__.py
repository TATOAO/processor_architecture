"""
Processor Pipeline v2 - Advanced DAG-based Processing System

A comprehensive pipeline execution system supporting:
- Directed Acyclic Graph (DAG) processing
- Real-time monitoring and debugging
- Advanced error handling and recovery
- Asynchronous and parallel execution
- Dynamic configuration support
"""

from .core.interfaces import (
    ProcessorInterface,
    PipeInterface, 
    MonitorInterface,
    CheckpointInterface
)

from .core.pipe import Pipe, PipeType
from .core.processor import BaseProcessor, AsyncProcessor
from .core.graph import ProcessingGraph, GraphExecutor
from .core.monitor import Monitor, LogLevel
from .core.checkpoint import Checkpoint, CheckpointManager
from .core.errors import (
    PipelineError,
    ProcessorError,
    GraphError,
    ValidationError,
    ExecutionError
)

from .execution.strategies import (
    ErrorStrategy,
    SkipStrategy,
    RetryStrategy,
    HaltStrategy
)

from .config.loader import ConfigLoader, GraphConfig
from .utils.helpers import create_simple_pipeline, visualize_graph

__version__ = "2.0.0"
__all__ = [
    # Core interfaces
    "ProcessorInterface", "PipeInterface", "MonitorInterface", "CheckpointInterface",
    
    # Core components  
    "Pipe", "PipeType", "BaseProcessor", "AsyncProcessor",
    "ProcessingGraph", "GraphExecutor", "Monitor", "LogLevel",
    "Checkpoint", "CheckpointManager",
    
    # Error handling
    "PipelineError", "ProcessorError", "GraphError", "ValidationError", "ExecutionError",
    "ErrorStrategy", "SkipStrategy", "RetryStrategy", "HaltStrategy",
    
    # Configuration and utilities
    "ConfigLoader", "GraphConfig", "create_simple_pipeline", "visualize_graph"
] 