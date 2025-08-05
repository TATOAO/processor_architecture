"""
Processor Pipeline Framework

A flexible framework for creating and executing data processing pipelines
with both synchronous and asynchronous support.
"""

from .core.processor import Processor, AsyncProcessor as CoreAsyncProcessor, ProcessorMeta
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

# Import new core components
from .new_core import AsyncProcessor as NewAsyncProcessor, GraphBase, AsyncPipe, AsyncPipeline as NewAsyncPipeline
from .new_core.graph_utils import get_root_nodes, get_previous_nodes, get_next_nodes

# For backward compatibility, make the old AsyncProcessor available as the default
# Users can still access the new one via new_core.AsyncProcessor
AsyncProcessor = CoreAsyncProcessor

__version__ = "1.0.1"
__author__ = "TATOAO"

__all__ = [
    # Core processor classes
    "Processor",
    "AsyncProcessor",  # This is the old core AsyncProcessor for backward compatibility
    "CoreAsyncProcessor",  # Explicit access to old core AsyncProcessor
    "NewAsyncProcessor",  # Explicit access to new core AsyncProcessor
    "ProcessorMeta",

    # New core components
    "GraphBase",
    "AsyncPipe",
    "get_root_nodes",
    "get_previous_nodes", 
    "get_next_nodes",
    
    # Pipeline classes
    "Pipeline",
    "AsyncPipeline",  # This is the old core AsyncPipeline for backward compatibility
    "NewAsyncPipeline",  # Explicit access to new graph-based AsyncPipeline
    
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
