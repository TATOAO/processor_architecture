import json
import os
import datetime
from typing import Any, Dict, List, Callable, Type, Optional, Union
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --- Utility Functions for Saving ---

def generate_execution_id() -> str:
    """Generate a unique execution ID for pipeline runs."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    return f"{timestamp}"

def save_to_json(data: Dict[str, Any], filepath: str) -> None:
    """
    Save data to a JSON file.
    
    Args:
        data: Dictionary to save as JSON
        filepath: Path where to save the file
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2, default=str)

def default_callback(processor: Any, input_data: Any, output_data: Any, 
                         execution_id: str, step_index: int, *args, **kwargs) -> None:
    """
    Default callback function for saving processor data to JSON files.
    
    Args:
        processor: The processor instance
        input_data: Input data to the processor
        output_data: Output data from the processor
        execution_id: Unique execution identifier
        step_index: Index of the processor in the pipeline
        output_dir: Directory to save the output files
    """
    save_data = processor.get_save_data(input_data, output_data, execution_id, step_index)
    logger.info(f"Callback: log processing data for processor {processor.meta.get('name', 'unknown')} \n{save_data}")

def create_execution_summary(execution_id: str, processors: list, total_time: float, 
                           input_data: Any, final_output: Any, 
                           output_dir: str = "processor_outputs") -> None:
    """
    Create a summary file for the entire pipeline execution.
    
    Args:
        execution_id: Unique execution identifier
        processors: List of processors in the pipeline
        total_time: Total execution time in seconds
        input_data: Initial input to the pipeline
        final_output: Final output from the pipeline
        output_dir: Directory to save the summary file
    """
    summary = {
        "execution_id": execution_id,
        "timestamp": datetime.datetime.now().isoformat(),
        "total_execution_time_seconds": total_time,
        "num_processors": len(processors),
        "processors": [p.meta.get("name") for p in processors],
        "input_summary": str(input_data)[:200] if input_data else None,
        "output_summary": str(final_output)[:200] if final_output else None,
    }
    
    filename = f"execution_summary_{execution_id}.json"
    filepath = os.path.join(output_dir, filename)
    save_to_json(summary, filepath)

# --- Runtime Processor Creation and Configuration ---

def list_registered_processors() -> Dict[str, Dict[str, Any]]:
    """
    List all registered processors with their metadata.
    
    Returns:
        Dictionary mapping processor names to their metadata
    """
    from .processor import ProcessorMeta
    
    processors_info = {}
    for name, processor_class in ProcessorMeta.registry.items():
        meta = processor_class.get_meta()
        processors_info[name] = {
            "class_name": processor_class.__name__,
            "input_type": str(meta.get("input_type", "Any")),
            "output_type": str(meta.get("output_type", "Any")),
            "description": getattr(processor_class, "__doc__", "No description available")
        }
    
    return processors_info

def create_processor_from_registry(name: str, **kwargs) -> Any:
    """
    Create a processor instance from the registry.
    
    Args:
        name: Name of the registered processor
        **kwargs: Arguments to pass to the processor constructor
        
    Returns:
        Processor instance
        
    Raises:
        KeyError: If processor name is not found in registry
    """
    from .processor import ProcessorMeta
    
    if name not in ProcessorMeta.registry:
        available = list(ProcessorMeta.registry.keys())
        raise KeyError(f"Processor '{name}' not found. Available processors: {available}")
    
    processor_class = ProcessorMeta.registry[name]
    return processor_class(**kwargs)

def execute_processor_code(code: str, globals_dict: Optional[Dict[str, Any]] = None, 
                          safe_mode: bool = True) -> List[str]:
    """
    Execute a piece of code and automatically register any processors defined in it.
    
    Args:
        code: String containing Python code that defines processor classes
        globals_dict: Optional dictionary of global variables to make available to the code
        safe_mode: If True, restricts dangerous operations (recommended for user input)
        
    Returns:
        List of processor names that were newly registered
        
    Example:
        code = '''
class MyCustomProcessor(Processor):
    meta = {
        "name": "my_custom_processor",
        "input_type": str,
        "output_type": str,
    }
    
    def process(self, data):
        return f"Processed: {data}"
        '''
        
        registered = execute_processor_code(code)
        print(f"Registered processors: {registered}")
    """
    from .processor import ProcessorMeta, Processor, AsyncProcessor
    
    # Track processors before execution
    processors_before = set(ProcessorMeta.registry.keys())
    
    # Prepare execution environment
    if safe_mode:
        # Safe builtins - include necessary functions for class creation but exclude dangerous ones
        safe_builtins = {
            # Basic types and functions
            'len': len, 'str': str, 'int': int, 'float': float, 'bool': bool,
            'list': list, 'dict': dict, 'tuple': tuple, 'set': set,
            'range': range, 'enumerate': enumerate, 'zip': zip,
            'print': print, 'isinstance': isinstance, 'hasattr': hasattr,
            'getattr': getattr, 'setattr': setattr, 'type': type,
            'object': object, 'super': super,
            # Mathematical and utility functions
            'sum': sum, 'min': min, 'max': max, 'abs': abs, 'round': round,
            'sorted': sorted, 'reversed': reversed, 'all': all, 'any': any,
            # Essential for class creation and imports
            '__build_class__': __builtins__['__build_class__'],
            '__import__': __import__,
            '__name__': '__main__',
            # Type hints
            'Any': Any, 'List': List, 'Dict': Dict, 'Optional': Optional, 'Union': Union,
            'Callable': Callable,
        }
        exec_globals = {'__builtins__': safe_builtins}
    else:
        exec_globals = {'__builtins__': __builtins__}
    
    exec_globals.update({
        'Processor': Processor,
        'AsyncProcessor': AsyncProcessor,
        'ProcessorMeta': ProcessorMeta,
        'Any': Any,
        'List': List,
        'Dict': Dict,
        'Optional': Optional,
        'Union': Union,
        'Callable': Callable,
        'asyncio': __import__('asyncio') if not safe_mode else None,
        'datetime': __import__('datetime') if not safe_mode else None,
        'json': __import__('json') if not safe_mode else None,
        'os': __import__('os') if not safe_mode else None,
        'sys': __import__('sys') if not safe_mode else None,
    })
    
    # Add user-provided globals
    if globals_dict:
        exec_globals.update(globals_dict)
    
    # Create local scope
    exec_locals = {}
    
    try:
        # Execute the code
        exec(code, exec_globals, exec_locals)
        
        # Find newly registered processors
        processors_after = set(ProcessorMeta.registry.keys())
        new_processors = processors_after - processors_before
        
        # Log registration
        for processor_name in new_processors:
            logger.info(f"Registered processor via code execution: {processor_name}")
        
        return list(new_processors)
        
    except Exception as e:
        logger.error(f"Error executing processor code: {str(e)}")
        raise RuntimeError(f"Failed to execute processor code: {str(e)}") from e

def execute_processor_code_from_file(filepath: str, safe_mode: bool = True) -> List[str]:
    """
    Execute processor code from a file and register any processors defined in it.
    
    Args:
        filepath: Path to the Python file containing processor definitions
        safe_mode: If True, restricts dangerous operations
        
    Returns:
        List of processor names that were newly registered
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            code = f.read()
        return execute_processor_code(code, safe_mode=safe_mode)
    except FileNotFoundError:
        raise FileNotFoundError(f"Processor file not found: {filepath}")
    except Exception as e:
        raise RuntimeError(f"Failed to load processor file {filepath}: {str(e)}") from e

def create_processor_template(name: str, input_type: str = "Any", output_type: str = "Any", 
                             is_async: bool = False) -> str:
    """
    Generate a template for creating a new processor.
    
    Args:
        name: Name for the processor
        input_type: Input type for the processor
        output_type: Output type for the processor
        is_async: Whether to create an async processor template
        
    Returns:
        String containing a processor class template
    """
    base_class = "AsyncProcessor" if is_async else "Processor"
    process_method = "async def process" if is_async else "def process"
    
    template = f'''class {name.title().replace('_', '')}Processor({base_class}):
    """
    TODO: Add description for your processor
    """
    
    meta = {{
        "name": "{name}",
        "input_type": {input_type},
        "output_type": {output_type},
    }}
    
    def __init__(self, **kwargs):
        super().__init__()
        # TODO: Initialize any parameters here
        pass
    
    {process_method}(self, data):
        """
        Process the input data.
        
        Args:
            data: Input data to process
            
        Returns:
            Processed output data
        """
        # TODO: Implement your processing logic here
        return data
    
    def get_save_data(self, input_data, output_data, execution_id, step_index):
        """Override to customize what data gets saved for this processor"""
        base_data = super().get_save_data(input_data, output_data, execution_id, step_index)
        # TODO: Add any custom save data here
        return base_data
'''
    
    return template

def create_pipeline_from_config(processor_configs: List[Dict[str, Any]], pipeline_type: str = "sync") -> Any:
    """
    Create a pipeline from a list of processor configurations.
    
    Args:
        processor_configs: List of dictionaries, each containing:
            - "name": processor name from registry
            - "params": optional dictionary of parameters for processor constructor
        pipeline_type: "sync" for Pipeline or "async" for AsyncPipeline
        
    Returns:
        Pipeline instance
        
    Example:
        configs = [
            {"name": "txt_to_text_processor"},
            {"name": "text_chunker_processor", "params": {"window_size": 300, "overlap": 50}}
        ]
        pipeline = create_pipeline_from_config(configs, "async")
    """
    from .pipeline import Pipeline, AsyncPipeline
    
    processors = []
    for config in processor_configs:
        name = config["name"]
        params = config.get("params", {})
        processor = create_processor_from_registry(name, **params)
        processors.append(processor)
    
    if pipeline_type.lower() == "async":
        return AsyncPipeline(processors)
    else:
        return Pipeline(processors)

def register_processor_class(processor_class: Type, force: bool = False) -> None:
    """
    Manually register a processor class in the registry.
    
    Args:
        processor_class: The processor class to register
        force: If True, overwrite existing registration
        
    Raises:
        ValueError: If processor doesn't have proper meta or name already exists
    """
    from .processor import ProcessorMeta
    
    if not hasattr(processor_class, 'meta') or not isinstance(processor_class.meta, dict):
        raise ValueError("Processor class must have a 'meta' dictionary")
    
    name = processor_class.meta.get("name")
    if not name:
        raise ValueError("Processor meta must contain a 'name' field")
    
    if name in ProcessorMeta.registry and not force:
        raise ValueError(f"Processor '{name}' already registered. Use force=True to overwrite.")
    
    ProcessorMeta.registry[name] = processor_class
    logger.info(f"Registered processor: {name}")

def create_dynamic_processor(name: str, 
                           process_func: Callable[[Any], Any],
                           input_type: Any = Any,
                           output_type: Any = Any,
                           is_async: bool = False,
                           description: str = "") -> Type:
    """
    Create a new processor class dynamically at runtime.
    
    Args:
        name: Name for the new processor
        process_func: Function that performs the processing
        input_type: Expected input type
        output_type: Expected output type  
        is_async: Whether this is an async processor
        description: Description of the processor
        
    Returns:
        The new processor class
        
    Example:
        # Create a simple synchronous processor
        def upper_case_func(data):
            return data.upper()
        
        UpperCaseProcessor = create_dynamic_processor(
            name="upper_case_processor",
            process_func=upper_case_func,
            input_type=str,
            output_type=str,
            description="Converts text to uppercase"
        )
        
        # The processor is automatically registered and can be used
        processor = create_processor_from_registry("upper_case_processor")
    """
    from .processor import Processor, AsyncProcessor
    
    base_class = AsyncProcessor if is_async else Processor
    
    class DynamicProcessor(base_class):
        meta = {
            "name": name,
            "input_type": input_type,
            "output_type": output_type,
        }
        
        def __init__(self, **kwargs):
            super().__init__()
            self._process_func = process_func
            self._kwargs = kwargs
        
        if is_async:
            async def process(self, data):
                if hasattr(process_func, '__call__'):
                    return await process_func(data, **self._kwargs)
                else:
                    raise ValueError("process_func must be callable for async processor")
        else:
            def process(self, data):
                return process_func(data, **self._kwargs)
    
    # Set docstring
    DynamicProcessor.__doc__ = description or f"Dynamically created processor: {name}"
    
    # Register the processor
    register_processor_class(DynamicProcessor, force=True)
    
    return DynamicProcessor

def get_pipeline_configuration(pipeline) -> List[Dict[str, Any]]:
    """
    Get the configuration of an existing pipeline.
    
    Args:
        pipeline: Pipeline instance
        
    Returns:
        List of processor configurations that can be used with create_pipeline_from_config
    """
    config = []
    for processor in pipeline.processors:
        processor_config = {
            "name": processor.meta.get("name"),
            "class_name": processor.__class__.__name__,
            "input_type": str(processor.meta.get("input_type", "Any")),
            "output_type": str(processor.meta.get("output_type", "Any")),
        }
        config.append(processor_config)
    
    return config

def save_pipeline_configuration(pipeline, filepath: str) -> None:
    """
    Save a pipeline configuration to a JSON file.
    
    Args:
        pipeline: Pipeline instance
        filepath: Path to save the configuration
    """
    config = get_pipeline_configuration(pipeline)
    save_to_json(config, filepath)

def load_pipeline_from_file(filepath: str, pipeline_type: str = "sync") -> Any:
    """
    Load a pipeline from a configuration file.
    
    Args:
        filepath: Path to the configuration file
        pipeline_type: "sync" for Pipeline or "async" for AsyncPipeline
        
    Returns:
        Pipeline instance
    """
    with open(filepath, 'r') as f:
        processor_configs = json.load(f)
    
    return create_pipeline_from_config(processor_configs, pipeline_type)