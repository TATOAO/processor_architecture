"""
Core interfaces and abstract base classes for the v2 processing system.

These interfaces define the contracts for all major components in the system,
enabling extensibility and clear separation of concerns.
"""

from abc import ABC, ABCMeta, abstractmethod
from typing import Any, Dict, List, Optional, Callable, Type, AsyncGenerator

class ProcessorMeta(ABCMeta):
    registry: Dict[str, Type["ProcessorInterface"]] = {}

    def __new__(mcs, name, bases, namespace):
        cls = super().__new__(mcs, name, bases, namespace)
        if "meta" in namespace:
            meta = namespace["meta"]
            if "name" in meta:
                mcs.registry[meta["name"]] = cls
        return cls

    @classmethod
    def get(cls, name: str) -> Type["ProcessorInterface"]:
        return cls.registry[name]


class PipeMeta(ABCMeta):
    registry: Dict[str, Type["PipeInterface"]] = {}

    def __new__(mcs, name, bases, namespace):
        cls = super().__new__(mcs, name, bases, namespace)
        if "meta" in namespace:
            meta = namespace["meta"]
            if "name" in meta:
                mcs.registry[meta["name"]] = cls
        return cls

    @classmethod
    def get(cls, name: str) -> Type["PipeInterface"]:
        return cls.registry[name]


class PipeInterface(ABC):
    """Interface for data pipes between processors"""

    @property
    @abstractmethod
    def pipe_id(self) -> str:
        """Unique identifier for this pipe"""
        pass
    
    
    @abstractmethod
    async def put(self, data: Any, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Asynchronously put data into the pipe"""
        pass
    
    @abstractmethod
    async def get(self, timeout: Optional[float] = None) -> Any:
        """Get data from the pipe"""
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
    
    @abstractmethod
    def register_input_pipe(self, pipe: PipeInterface) -> None:
        """Register an input pipe"""
        pass
    
    @abstractmethod
    def register_output_pipe(self, pipe: PipeInterface) -> None:
        """Register an output pipe"""
        pass

    @abstractmethod
    async def execute(self) -> None:
        """Execute the processor"""
        pass
    
    @abstractmethod
    async def process(self, input_data: Any) -> AsyncGenerator[Any, None]:
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

