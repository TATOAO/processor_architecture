"""
Example demonstrating how to use the processor registry system.

This example shows:
1. How to create processors that work with the registry
2. Manual registration
3. Automatic registration using metaclass
4. Using the factory to create processor instances
"""

from typing import Any, Dict
from ..core.interfaces import ProcessorInterface, PipeInterface, ProcessorState
from ..core.registry import ProcessorRegistry, ProcessorFactory, ProcessorMeta, global_registry, global_factory


# Example processor with manual registration
class TextUppercaseProcessor(ProcessorInterface):
    """Processor that converts text to uppercase"""
    
    def __init__(self, processor_id: str):
        self._processor_id = processor_id
        self._input_pipes: Dict[str, PipeInterface] = {}
        self._output_pipes: Dict[str, PipeInterface] = {}
        self._state = ProcessorState.IDLE
    
    @property
    def processor_id(self) -> str:
        return self._processor_id
    
    @property
    def input_pipes(self) -> Dict[str, PipeInterface]:
        return self._input_pipes
    
    @property
    def output_pipes(self) -> Dict[str, PipeInterface]:
        return self._output_pipes
    
    @property
    def state(self) -> ProcessorState:
        return self._state
    
    @classmethod
    def get_processor_metadata(cls) -> Dict[str, Any]:
        return {
            "name": "text_uppercase",
            "input_type": str,
            "output_type": str,
            "description": "Converts input text to uppercase"
        }
    
    def register_input_pipe(self, name: str, pipe: PipeInterface) -> None:
        self._input_pipes[name] = pipe
    
    def register_output_pipe(self, name: str, pipe: PipeInterface) -> None:
        self._output_pipes[name] = pipe
    
    async def process(self) -> None:
        self._state = ProcessorState.RUNNING
        # Processing logic would go here
        self._state = ProcessorState.COMPLETED
    
    async def initialize(self) -> None:
        pass
    
    async def cleanup(self) -> None:
        pass
    
    def get_metrics(self) -> Dict[str, Any]:
        return {"processed_items": 0}


# Example processor with automatic registration using metaclass
class TextLowercaseProcessor(ProcessorInterface, metaclass=ProcessorMeta):
    """Processor that converts text to lowercase - auto-registered"""
    
    def __init__(self, processor_id: str):
        self._processor_id = processor_id
        self._input_pipes: Dict[str, PipeInterface] = {}
        self._output_pipes: Dict[str, PipeInterface] = {}
        self._state = ProcessorState.IDLE
    
    @property
    def processor_id(self) -> str:
        return self._processor_id
    
    @property
    def input_pipes(self) -> Dict[str, PipeInterface]:
        return self._input_pipes
    
    @property
    def output_pipes(self) -> Dict[str, PipeInterface]:
        return self._output_pipes
    
    @property
    def state(self) -> ProcessorState:
        return self._state
    
    @classmethod
    def get_processor_metadata(cls) -> Dict[str, Any]:
        return {
            "name": "text_lowercase",
            "input_type": str,
            "output_type": str,
            "description": "Converts input text to lowercase"
        }
    
    def register_input_pipe(self, name: str, pipe: PipeInterface) -> None:
        self._input_pipes[name] = pipe
    
    def register_output_pipe(self, name: str, pipe: PipeInterface) -> None:
        self._output_pipes[name] = pipe
    
    async def process(self) -> None:
        self._state = ProcessorState.RUNNING
        # Processing logic would go here
        self._state = ProcessorState.COMPLETED
    
    async def initialize(self) -> None:
        pass
    
    async def cleanup(self) -> None:
        pass
    
    def get_metrics(self) -> Dict[str, Any]:
        return {"processed_items": 0}


def registry_example():
    """Demonstrate registry usage"""
    
    # Manual registration
    global_registry.register_processor("text_uppercase", TextUppercaseProcessor)
    
    # The TextLowercaseProcessor was automatically registered due to the metaclass
    
    # List all registered processors
    print("Registered processors:")
    for name in global_registry.list_registered_processors():
        print(f"  - {name}")
    
    # Get processor information
    print("\nProcessor info:")
    print(global_factory.get_processor_info("text_uppercase"))
    print(global_factory.get_processor_info("text_lowercase"))
    
    # Create processor instances
    print("\nCreating processor instances:")
    
    # Create using factory
    uppercase_processor = global_factory.create_processor(
        "text_uppercase", 
        "processor_1"
    )
    print(f"Created: {uppercase_processor.processor_id}")
    
    # Create from config
    config = {
        "name": "text_lowercase",
        "processor_id": "processor_2"
    }
    lowercase_processor = global_factory.create_processor_from_config(config)
    print(f"Created: {lowercase_processor.processor_id}")
    
    # Check if processors are registered
    print(f"\nIs 'text_uppercase' registered? {global_registry.is_registered('text_uppercase')}")
    print(f"Is 'nonexistent' registered? {global_registry.is_registered('nonexistent')}")


if __name__ == "__main__":
    registry_example() 