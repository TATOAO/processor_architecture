from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Callable
from .core_interfaces import PipeInterface, ProcessorInterface


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