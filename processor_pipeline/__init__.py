"""
Processor Pipeline Framework

A flexible framework for creating and executing data processing pipelines
with both synchronous and asynchronous support.
"""

from .core.processor import Processor, AsyncProcessor, ProcessorMeta
from .core.pipeline import Pipeline, AsyncPipeline
from .core.helper import (
    generate_execution_id,
    save_to_json,
    default_callback,
    create_execution_summary
)

__version__ = "0.1.0"
__author__ = "TATOAO"

__all__ = [
    # Core processor classes
    "Processor",
    "AsyncProcessor", 
    "ProcessorMeta",
    
    # Pipeline classes
    "Pipeline",
    "AsyncPipeline",
    
    # Helper functions
    "generate_execution_id",
    "save_to_json", 
    "default_callback",
    "create_execution_summary",
]
