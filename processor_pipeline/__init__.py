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
    create_execution_summary,
    # Runtime processor configuration functions
    list_registered_processors,
    create_processor_from_registry,
    create_pipeline_from_config,
    register_processor_class,
    create_dynamic_processor,
    get_pipeline_configuration,
    save_pipeline_configuration,
    load_pipeline_from_file,
    # Code execution functions
    execute_processor_code,
    execute_processor_code_from_file,
    create_processor_template
)


from .new_core import *

__version__ = "1.0.1"
__author__ = "TATOAO"

__all__ = [
    # Core processor classes
    "Processor",
    "AsyncProcessor", 
    "ProcessorMeta",

    "new_core",
    
    # Pipeline classes
    "Pipeline",
    "AsyncPipeline",
    
    # Helper functions
    "generate_execution_id",
    "save_to_json", 
    "default_callback",
    "create_execution_summary",
    
    # Runtime processor configuration
    "list_registered_processors",
    "create_processor_from_registry", 
    "create_pipeline_from_config",
    "register_processor_class",
    "create_dynamic_processor",
    "get_pipeline_configuration",
    "save_pipeline_configuration",
    "load_pipeline_from_file",
    
    # Code execution functions
    "execute_processor_code",
    "execute_processor_code_from_file",
    "create_processor_template"
]
