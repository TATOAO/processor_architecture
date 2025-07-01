import datetime
from abc import ABC, ABCMeta, abstractmethod
from typing import Any, Dict, Type, AsyncGenerator

class ProcessorMeta(ABCMeta):
    registry: Dict[str, Type["Processor"]] = {}

    def __new__(mcs, name, bases, namespace):
        cls = super().__new__(mcs, name, bases, namespace)
        if "meta" in namespace:
            meta = namespace["meta"]
            if "name" in meta:
                mcs.registry[meta["name"]] = cls
        return cls

    @classmethod
    def get(cls, name: str) -> Type["Processor"]:
        return cls.registry[name]

# --- Abstract Processor Base ---

class Processor(ABC, metaclass=ProcessorMeta):
    meta: Dict[str, Any] = {
        "name": None,
        "input_type": Any,
        "output_type": Any,
    }

    @abstractmethod
    def process(self, data: Any) -> Any:
        ...

    def get_save_data(self, input_data: Any, output_data: Any, execution_id: str, step_index: int) -> Dict[str, Any]:
        """
        Override this method to define what data should be saved for this processor.
        By default, saves basic information about input/output types and processor name.
        
        Args:
            input_data: The input data passed to this processor
            output_data: The output data produced by this processor
            execution_id: Unique identifier for this pipeline execution
            step_index: Index of this processor in the pipeline (0-based)
            
        Returns:
            Dict that will be JSON serialized and saved
        """
        return {
            "processor_name": self.meta.get("name"),
            "step_index": step_index,
            "execution_id": execution_id,
            "timestamp": datetime.datetime.now().isoformat(),
            "input_type": str(type(input_data).__name__),
            "output_type": str(type(output_data).__name__),
            "input_summary": str(input_data)[:100] if input_data else None,
            "output_summary": str(output_data)[:100] if output_data else None,
        }

    @classmethod
    def get_meta(cls):
        return cls.meta

class AsyncProcessor(ABC, metaclass=ProcessorMeta):
    meta: Dict[str, Any] = {
        "name": None,
        "input_type": Any,
        "output_type": Any,
    }

    @abstractmethod
    async def process(self, data: AsyncGenerator[Any, None]) -> AsyncGenerator[Any, None]:
        ...

    def get_save_data(self, input_data: Any, output_data: Any, execution_id: str, step_index: int) -> Dict[str, Any]:
        """
        Override this method to define what data should be saved for this processor.
        By default, saves basic information about input/output types and processor name.
        
        Args:
            input_data: The input data passed to this processor
            output_data: The output data produced by this processor
            execution_id: Unique identifier for this pipeline execution
            step_index: Index of this processor in the pipeline (0-based)
            
        Returns:
            Dict that will be JSON serialized and saved
        """
        return {
            "processor_name": self.meta.get("name"),
            "step_index": step_index,
            "execution_id": execution_id,
            "timestamp": datetime.datetime.now().isoformat(),
            "input_type": str(type(input_data).__name__),
            "output_type": str(type(output_data).__name__),
            "input_summary": str(input_data)[:100] if input_data else None,
            "output_summary": str(output_data)[:100] if output_data else None,
        }

    @classmethod
    def get_meta(cls):
        return cls.meta
