"""
Core interfaces and abstract base classes for the v2 processing system.

These interfaces define the contracts for all major components in the system,
enabling extensibility and clear separation of concerns.
"""

import inspect
from abc import ABC, ABCMeta, abstractmethod
from typing import Any, Dict, List, Optional, Callable, Type, AsyncGenerator

class ProcessorMeta(ABCMeta):
    registry: Dict[str, Type["ProcessorInterface"]] = {}

    def __new__(mcs, name, bases, namespace):
        cls = super().__new__(mcs, name, bases, namespace)
        if "meta" in namespace:
            meta = namespace["meta"]
            # Store the complete meta information on the class
            cls._meta = meta
            if "name" in meta:
                mcs.registry[meta["name"]] = cls
        
        # Validate process method signature
        if "process" in namespace:
            process_method = namespace["process"]
            if hasattr(process_method, "__code__"):
                sig = inspect.signature(process_method)
                params = list(sig.parameters.values())
                
                # Check if the method accepts *args and **kwargs
                has_var_args = any(param.kind == inspect.Parameter.VAR_POSITIONAL for param in params)
                has_var_kwargs = any(param.kind == inspect.Parameter.VAR_KEYWORD for param in params)
                
                if not (has_var_args and has_var_kwargs):
                    raise TypeError(
                        f"Process method in {name} must accept *args and **kwargs. "
                        f"Current signature: {sig}. "
                        f"Please define it as: async def process(self, input_data: Any, *args, **kwargs) -> AsyncGenerator[Any, None]:"
                    )
        
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

    
    @abstractmethod
    async def put(self, data: Any) -> None:
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
    async def astream(self, input_data: Any) -> AsyncGenerator[Any, None]:
        """Process data without blocking"""
        pass

    @abstractmethod
    async def process(self, input_data: Any, *args, **kwargs) -> AsyncGenerator[Any, None]:
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

