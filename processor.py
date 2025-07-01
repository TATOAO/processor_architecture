
from abc import ABC, ABCMeta, abstractmethod
from typing import Any, Dict, Type

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

    @classmethod
    def get_meta(cls):
        return cls.meta
