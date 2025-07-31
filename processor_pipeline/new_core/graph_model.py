from .core_interfaces import ProcessorInterface, ProcessorMeta
from pydantic import BaseModel, field_validator

class Node(BaseModel):
    processor_class_name: str
    processor_unique_name: str

    #validation all processors are registered
    @field_validator('processor_class_name')
    def processor_class_name_validator(cls, v):
        if not v:
            raise ValueError("processor_class_name is required")
        if v not in ProcessorMeta.registry.keys():
            raise ValueError(f"processor_class_name {v} is not registered")
        
        # get the processor class
        processor_class = ProcessorMeta.registry[v]
        if not issubclass(processor_class, ProcessorInterface):
            raise ValueError(f"processor_class_name {v} is not a valid processor class")
        return v
    

class Edge(BaseModel):
    source_node_unique_name: str
    target_node_unique_name: str
    edge_unique_name: str


