"""
Concrete implementation of processor registry and factory interfaces.

This module provides a working implementation of the registry system
that can be used to register, manage, and create processor instances.
"""

from typing import Any, Dict, Type, Optional
from .interfaces import ProcessorInterface, ProcessorRegistryInterface, ProcessorFactoryInterface


class ProcessorRegistry(ProcessorRegistryInterface):
    """Concrete implementation of processor registry"""
    
    def __init__(self):
        self._registry: Dict[str, Type[ProcessorInterface]] = {}
    
    def register_processor(self, name: str, processor_class: Type[ProcessorInterface]) -> None:
        """Register a processor class with a given name"""
        if not issubclass(processor_class, ProcessorInterface):
            raise ValueError(f"Class {processor_class} must implement ProcessorInterface")
        
        self._registry[name] = processor_class
    
    def get_processor_class(self, name: str) -> Type[ProcessorInterface]:
        """Get a processor class by name"""
        if name not in self._registry:
            raise KeyError(f"Processor '{name}' not found in registry")
        return self._registry[name]
    
    def list_registered_processors(self) -> Dict[str, Type[ProcessorInterface]]:
        """Get all registered processor classes"""
        return self._registry.copy()
    
    def is_registered(self, name: str) -> bool:
        """Check if a processor with the given name is registered"""
        return name in self._registry
    
    def unregister_processor(self, name: str) -> None:
        """Unregister a processor by name"""
        if name in self._registry:
            del self._registry[name]


class ProcessorFactory(ProcessorFactoryInterface):
    """Concrete implementation of processor factory"""
    
    def __init__(self, registry: ProcessorRegistryInterface):
        self.registry = registry
    
    def create_processor(self, name: str, processor_id: str, **kwargs) -> ProcessorInterface:
        """Create a processor instance by name with given ID and configuration"""
        processor_class = self.registry.get_processor_class(name)
        
        # Create instance with processor_id
        instance = processor_class(processor_id=processor_id, **kwargs)
        return instance
    
    def create_processor_from_config(self, config: Dict[str, Any]) -> ProcessorInterface:
        """Create a processor instance from configuration dictionary"""
        if 'name' not in config:
            raise ValueError("Configuration must include 'name' field")
        if 'processor_id' not in config:
            raise ValueError("Configuration must include 'processor_id' field")
        
        name = config.pop('name')
        processor_id = config.pop('processor_id')
        
        return self.create_processor(name, processor_id, **config)
    
    def get_processor_info(self, name: str) -> Dict[str, Any]:
        """Get information about a registered processor type"""
        processor_class = self.registry.get_processor_class(name)
        
        try:
            metadata = processor_class.get_processor_metadata()
        except NotImplementedError:
            metadata = {"name": name}
        
        return {
            "name": name,
            "class": processor_class.__name__,
            "module": processor_class.__module__,
            "metadata": metadata,
            "docstring": processor_class.__doc__
        }


class ProcessorMeta(type):
    """
    Metaclass for automatic processor registration.
    
    Similar to the original ProcessorMeta but designed to work with
    the new registry interface system.
    """
    
    _global_registry: Optional[ProcessorRegistryInterface] = None
    
    @classmethod
    def set_global_registry(cls, registry: ProcessorRegistryInterface):
        """Set the global registry for automatic registration"""
        cls._global_registry = registry
    
    def __new__(mcs, name, bases, namespace):
        cls = super().__new__(mcs, name, bases, namespace)
        
        # Auto-register if the class has a registry name and global registry is set
        if (mcs._global_registry and 
            hasattr(cls, 'get_registry_name') and 
            cls.get_registry_name()):
            
            registry_name = cls.get_registry_name()
            mcs._global_registry.register_processor(registry_name, cls)
        
        return cls


# Global registry instance
global_registry = ProcessorRegistry()
global_factory = ProcessorFactory(global_registry)

# Set the global registry for auto-registration
ProcessorMeta.set_global_registry(global_registry) 