"""
Core components for the v2 processing system
"""

from .interfaces import (
    ProcessorInterface,
    PipeInterface,
    MonitorInterface,
    CheckpointInterface,
    ErrorHandlerInterface,
    GraphInterface,
    ProcessorRegistryInterface,
    ProcessorFactoryInterface,
    ProcessorState,
    PipeType
)

from .registry import (
    ProcessorRegistry,
    ProcessorFactory,
    ProcessorMeta,
    global_registry,
    global_factory
)

__all__ = [
    # Interfaces
    'ProcessorInterface',
    'PipeInterface', 
    'MonitorInterface',
    'CheckpointInterface',
    'ErrorHandlerInterface',
    'GraphInterface',
    'ProcessorRegistryInterface',
    'ProcessorFactoryInterface',
    
    # Enums
    'ProcessorState',
    'PipeType',
    
    # Registry implementations
    'ProcessorRegistry',
    'ProcessorFactory',
    'ProcessorMeta',
    'global_registry',
    'global_factory'
] 