"""
Core interfaces and abstract base classes for the v2 processing system.

These interfaces define the contracts for all major components in the system,
enabling extensibility and clear separation of concerns.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Callable, Type
from enum import Enum


class ProcessorRegistryInterface(ABC):
    """Interface for processor registry management"""
    
    @abstractmethod
    def register_processor(self, name: str, processor_class: Type['ProcessorInterface']) -> None:
        """Register a processor class with a given name"""
        pass
    
    @abstractmethod
    def get_processor_class(self, name: str) -> Type['ProcessorInterface']:
        """Get a processor class by name"""
        pass
    
    @abstractmethod
    def list_registered_processors(self) -> Dict[str, Type['ProcessorInterface']]:
        """Get all registered processor classes"""
        pass
    
    @abstractmethod
    def is_registered(self, name: str) -> bool:
        """Check if a processor with the given name is registered"""
        pass
    
    @abstractmethod
    def unregister_processor(self, name: str) -> None:
        """Unregister a processor by name"""
        pass


class ProcessorFactoryInterface(ABC):
    """Interface for creating processor instances from registry"""
    
    @abstractmethod
    def create_processor(self, name: str, processor_id: str, **kwargs) -> 'ProcessorInterface':
        """Create a processor instance by name with given ID and configuration"""
        pass
    
    @abstractmethod
    def create_processor_from_config(self, config: Dict[str, Any]) -> 'ProcessorInterface':
        """Create a processor instance from configuration dictionary"""
        pass
    
    @abstractmethod
    def get_processor_info(self, name: str) -> Dict[str, Any]:
        """Get information about a registered processor type"""
        pass


class PipeInterface(ABC):
    """Interface for data pipes between processors"""
    
    @abstractmethod
    def put(self, data: Any, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Put data into the pipe"""
        pass
    
    @abstractmethod
    async def aput(self, data: Any, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Asynchronously put data into the pipe"""
        pass
    
    @abstractmethod
    def get(self, timeout: Optional[float] = None) -> Any:
        """Get data from the pipe"""
        pass
    
    @abstractmethod
    async def aget(self, timeout: Optional[float] = None) -> Any:
        """Asynchronously get data from the pipe"""
        pass
    
    @abstractmethod
    def is_empty(self) -> bool:
        """Check if pipe is empty"""
        pass
    
    @abstractmethod
    def size(self) -> int:
        """Get number of items in pipe"""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close the pipe"""
        pass

    
    @abstractmethod
    def statistics(self) -> Dict[str, Any]:
        """Get the statistics of the pipe"""
        """
        Returns:
            Dict[str, Any]: A dictionary containing statistics about the pipe
        """
        pass


class ProcessorInterface(ABC):
    """Interface for processors in the graph"""
    
    @property
    @abstractmethod
    def processor_id(self) -> str:
        """Unique identifier for this processor"""
        pass
    
    @property
    @abstractmethod
    def input_pipes(self) -> Dict[str, PipeInterface]:
        """Dictionary of input pipes by name"""
        pass
    
    @property
    @abstractmethod
    def output_pipes(self) -> Dict[str, PipeInterface]:
        """Dictionary of output pipes by name"""
        pass
    
    @classmethod
    @abstractmethod
    def get_processor_metadata(cls) -> Dict[str, Any]:
        """Get metadata for processor registration (name, input_type, output_type, etc.)"""
        pass
    
    @classmethod
    def get_registry_name(cls) -> Optional[str]:
        """Get the registry name for this processor. Override to provide custom name."""
        metadata = cls.get_processor_metadata()
        return metadata.get("name")
    
    @abstractmethod
    def register_input_pipe(self, name: str, pipe: PipeInterface) -> None:
        """Register an input pipe"""
        pass
    
    @abstractmethod
    def register_output_pipe(self, name: str, pipe: PipeInterface) -> None:
        """Register an output pipe"""
        pass
    
    @abstractmethod
    async def process(self) -> None:
        """Main processing method - reads from input pipes, processes, writes to output pipes"""
        pass
    
    @abstractmethod
    async def initialize(self) -> None:
        """Initialize the processor before processing starts"""
        pass
    
    @abstractmethod
    async def cleanup(self) -> None:
        """Cleanup after processing is complete"""
        pass
    
    @abstractmethod
    def statistics(self) -> Dict[str, Any]:
        """Get performance and status statistics"""
        """
        Returns:
            Dict[str, Any]: A dictionary containing statistics about the processor
        """
        pass


class MonitorInterface(ABC):
    """Interface for monitoring and observability"""
    
    @abstractmethod
    def log_event(self, event_type: str, processor_id: str, data: Dict[str, Any]) -> None:
        """Log an event"""
        pass
    
    @abstractmethod  
    def tap_pipe(self, pipe: PipeInterface, callback: Callable[[Any], None]) -> str:
        """Tap into a pipe to monitor data flow, returns tap ID"""
        pass
    
    @abstractmethod
    def untap_pipe(self, tap_id: str) -> None:
        """Remove a pipe tap"""
        pass
    
    
    @abstractmethod
    def get_pipe_status(self, pipe: PipeInterface) -> Dict[str, Any]:
        """Get current status of a pipe"""
        pass
    
    @abstractmethod
    def start_monitoring(self) -> None:
        """Start the monitoring system"""
        pass
    
    @abstractmethod
    def stop_monitoring(self) -> None:
        """Stop the monitoring system"""
        pass


class CheckpointInterface(ABC):
    """Interface for checkpointing and debugging"""
    
    @abstractmethod
    def create_checkpoint(self, name: str, processor_id: str, data: Any, metadata: Optional[Dict[str, Any]] = None) -> str:
        """Create a checkpoint, returns checkpoint ID"""
        pass
    
    @abstractmethod
    def restore_checkpoint(self, checkpoint_id: str) -> Any:
        """Restore data from a checkpoint"""
        pass
    
    @abstractmethod
    def list_checkpoints(self, processor_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """List available checkpoints"""
        pass
    
    @abstractmethod
    def delete_checkpoint(self, checkpoint_id: str) -> None:
        """Delete a checkpoint"""
        pass
    
    @abstractmethod
    def set_breakpoint(self, processor_id: str, condition: Optional[Callable[[Any], bool]] = None) -> None:
        """Set a breakpoint at a processor"""
        pass
    
    @abstractmethod
    def remove_breakpoint(self, processor_id: str) -> None:
        """Remove a breakpoint"""
        pass

    @abstractmethod
    def get_statistics(self) -> Dict[str, Any]:
        """Get the statistics of the checkpoint manager"""
        """
        Returns:
            Dict[str, Any]: A dictionary containing statistics about the checkpoint manager
        """
        pass


class ErrorHandlerInterface(ABC):
    """Interface for error handling strategies"""
    
    @abstractmethod
    async def handle_error(self, error: Exception, processor_id: str, context: Dict[str, Any]) -> bool:
        """
        Handle an error that occurred during processing.
        
        Returns:
            bool: True if processing should continue, False if it should stop
        """
        pass
    
    @abstractmethod
    def get_strategy_name(self) -> str:
        """Get the name of this error handling strategy"""
        pass


class GraphInterface(ABC):
    """Interface for the processing graph"""
    
    @abstractmethod
    def add_processor(self, processor: ProcessorInterface) -> None:
        """Add a processor to the graph"""
        pass
    
    @abstractmethod
    def connect(self, from_processor: str, from_output: str, to_processor: str, to_input: str) -> None:
        """Connect processors via pipes"""
        pass
    
    @abstractmethod
    def validate(self) -> List[str]:
        """Validate the graph structure, returns list of validation errors"""
        pass
    
    @abstractmethod
    def get_execution_order(self) -> List[List[str]]:
        """Get processors grouped by execution level (topological sort)"""
        pass
    
    @abstractmethod
    async def execute(self, input_data: Dict[str, Any], monitor: Optional[MonitorInterface] = None) -> Dict[str, Any]:
        """Execute the graph"""
        pass
    
    @abstractmethod
    def get_processor(self, processor_id: str) -> ProcessorInterface:
        """Get a processor by ID"""
        pass
    
    @abstractmethod
    def get_all_processors(self) -> Dict[str, ProcessorInterface]:
        """Get all processors in the graph"""
        pass 