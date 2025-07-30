from abc import ABC, abstractmethod
from typing import Any, Dict, Type
from .core_interfaces import ProcessorInterface


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
