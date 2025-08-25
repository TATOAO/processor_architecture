"""
Processor Pipeline Framework

A flexible framework for creating and executing data processing pipelines
with both synchronous and asynchronous support.
"""

from .core.processor import AsyncProcessor, ProcessorMeta
from .core.pipeline import AsyncPipeline
from .core.graph import GraphBase, Edge, Node

# Import new core components
from .core.graph_utils import get_root_nodes, get_previous_nodes, get_next_nodes

# For backward compatibility, make the old AsyncProcessor available as the default
# Users can still access the new one via new_core.AsyncProcessor

# Optional monitoring imports - only available if dependencies are installed
try:
    from .monitoring import enable_monitoring, MonitoringPlugin, MonitoredPipeline
    _monitoring_available = True
except ImportError:
    # Monitoring dependencies not installed
    _monitoring_available = False

__version__ = "1.0.1"
__author__ = "TATOAO"

__all__ = [
    # Core processor classes
    "AsyncProcessor",  # This is the old core AsyncProcessor for backward compatibility
    "ProcessorMeta",

    # New core components
    "GraphBase",
    "AsyncPipe",
    "get_root_nodes",
    "get_previous_nodes", 
    "get_next_nodes",
    "Node",
    "Edge",

    
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

# Add monitoring components to __all__ if available
if _monitoring_available:
    __all__.extend(['enable_monitoring', 'MonitoringPlugin', 'MonitoredPipeline'])
