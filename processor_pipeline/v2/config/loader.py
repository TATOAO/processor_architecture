"""
Configuration loading system for the v2 processing pipeline.

Supports loading pipeline configurations from YAML, JSON, and Python dictionaries.
"""

import json
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass

from ..core.graph import ProcessingGraph
from ..core.processor import BaseProcessor, SimpleProcessor
from ..core.monitor import Monitor, LogLevel
from ..core.checkpoint import CheckpointManager
from ..execution.strategies import (
    ErrorStrategy, SkipStrategy, RetryStrategy, HaltStrategy, 
    CompositeStrategy, create_default_error_strategy
)
from ..core.errors import ConfigurationError


@dataclass
class ProcessorConfig:
    """Configuration for a single processor"""
    processor_id: str
    processor_type: str
    parameters: Dict[str, Any]
    input_pipes: List[str]
    output_pipes: List[str]


@dataclass
class ConnectionConfig:
    """Configuration for a connection between processors"""
    from_processor: str
    from_output: str
    to_processor: str
    to_input: str


@dataclass
class GraphConfig:
    """Complete graph configuration"""
    graph_id: Optional[str]
    processors: List[ProcessorConfig]
    connections: List[ConnectionConfig]
    error_strategy: Dict[str, Any]
    monitoring: Dict[str, Any]
    checkpoints: Dict[str, Any]


class ConfigLoader:
    """
    Loads and parses pipeline configurations from various sources.
    """
    
    def __init__(self):
        self.processor_factories = {}
        self._register_built_in_factories()
        
    def _register_built_in_factories(self) -> None:
        """Register built-in processor factories"""
        # Simple function-based processor
        self.processor_factories["simple"] = self._create_simple_processor
        
        # Example processors (you can extend this)
        self.processor_factories["text_transformer"] = self._create_text_transformer
        self.processor_factories["data_filter"] = self._create_data_filter
        self.processor_factories["aggregator"] = self._create_aggregator
        
    def register_processor_factory(self, processor_type: str, factory_func) -> None:
        """Register a custom processor factory"""
        self.processor_factories[processor_type] = factory_func
        
    def load_from_file(self, config_path: Union[str, Path]) -> GraphConfig:
        """Load configuration from a file (YAML or JSON)"""
        config_path = Path(config_path)
        
        if not config_path.exists():
            raise ConfigurationError(f"Configuration file not found: {config_path}")
            
        try:
            with open(config_path, 'r') as f:
                if config_path.suffix.lower() in ['.yaml', '.yml']:
                    config_data = yaml.safe_load(f)
                elif config_path.suffix.lower() == '.json':
                    config_data = json.load(f)
                else:
                    raise ConfigurationError(f"Unsupported file format: {config_path.suffix}")
                    
            return self.load_from_dict(config_data)
            
        except Exception as e:
            raise ConfigurationError(f"Failed to load configuration from {config_path}: {e}")
            
    def load_from_dict(self, config_data: Dict[str, Any]) -> GraphConfig:
        """Load configuration from a dictionary"""
        try:
            # Parse processors
            processors = []
            for proc_config in config_data.get("processors", []):
                processors.append(ProcessorConfig(
                    processor_id=proc_config["processor_id"],
                    processor_type=proc_config["processor_type"],
                    parameters=proc_config.get("parameters", {}),
                    input_pipes=proc_config.get("input_pipes", []),
                    output_pipes=proc_config.get("output_pipes", [])
                ))
                
            # Parse connections
            connections = []
            for conn_config in config_data.get("connections", []):
                connections.append(ConnectionConfig(
                    from_processor=conn_config["from_processor"],
                    from_output=conn_config["from_output"],
                    to_processor=conn_config["to_processor"],
                    to_input=conn_config["to_input"]
                ))
                
            return GraphConfig(
                graph_id=config_data.get("graph_id"),
                processors=processors,
                connections=connections,
                error_strategy=config_data.get("error_strategy", {}),
                monitoring=config_data.get("monitoring", {}),
                checkpoints=config_data.get("checkpoints", {})
            )
            
        except Exception as e:
            raise ConfigurationError(f"Failed to parse configuration: {e}")
            
    def create_graph_from_config(self, config: GraphConfig) -> ProcessingGraph:
        """Create a ProcessingGraph from configuration"""
        # Create error strategy
        error_strategy = self._create_error_strategy(config.error_strategy)
        
        # Create graph
        graph = ProcessingGraph(config.graph_id, error_strategy)
        
        # Create processors
        processors = {}
        for proc_config in config.processors:
            processor = self._create_processor(proc_config)
            processors[proc_config.processor_id] = processor
            graph.add_processor(processor)
            
        # Create connections
        for conn_config in config.connections:
            graph.connect(
                conn_config.from_processor,
                conn_config.from_output,
                conn_config.to_processor,
                conn_config.to_input
            )
            
        return graph
        
    def _create_processor(self, config: ProcessorConfig) -> BaseProcessor:
        """Create a processor from configuration"""
        if config.processor_type not in self.processor_factories:
            raise ConfigurationError(f"Unknown processor type: {config.processor_type}")
            
        factory = self.processor_factories[config.processor_type]
        processor = factory(config)
        
        # Create default pipes if specified
        if config.input_pipes or config.output_pipes:
            processor.create_default_pipes(
                config.input_pipes or None,
                config.output_pipes or None
            )
            
        return processor
        
    def _create_error_strategy(self, config: Dict[str, Any]) -> ErrorStrategy:
        """Create error strategy from configuration"""
        if not config:
            return create_default_error_strategy()
            
        strategy_type = config.get("type", "default")
        
        if strategy_type == "skip":
            return SkipStrategy(
                max_skips=config.get("max_skips"),
                skip_callback=None  # TODO: Support callback configuration
            )
        elif strategy_type == "retry":
            return RetryStrategy(
                max_retries=config.get("max_retries", 3),
                initial_delay=config.get("initial_delay", 1.0),
                max_delay=config.get("max_delay", 60.0),
                backoff_multiplier=config.get("backoff_multiplier", 2.0)
            )
        elif strategy_type == "halt":
            return HaltStrategy(
                cleanup_timeout=config.get("cleanup_timeout", 30.0)
            )
        elif strategy_type == "composite":
            strategies = []
            for strategy_config in config.get("strategies", []):
                strategies.append(self._create_error_strategy(strategy_config))
            return CompositeStrategy(
                strategies,
                config.get("strategy_order", "sequential")
            )
        else:
            return create_default_error_strategy()
            
    def create_monitor_from_config(self, config: Dict[str, Any]) -> Optional[Monitor]:
        """Create monitor from configuration"""
        if not config.get("enabled", True):
            return None
            
        log_level = LogLevel(config.get("log_level", "info"))
        max_events = config.get("max_events", 10000)
        
        return Monitor(max_events=max_events, log_level=log_level)
        
    def create_checkpoint_manager_from_config(self, config: Dict[str, Any]) -> Optional[CheckpointManager]:
        """Create checkpoint manager from configuration"""
        if not config.get("enabled", False):
            return None
            
        storage_path = config.get("storage_path")
        max_checkpoints = config.get("max_checkpoints", 1000)
        
        return CheckpointManager(storage_path=storage_path, max_checkpoints=max_checkpoints)
        
    # Built-in processor factories
    def _create_simple_processor(self, config: ProcessorConfig) -> SimpleProcessor:
        """Create a simple function-based processor"""
        func_name = config.parameters.get("function")
        if not func_name:
            raise ConfigurationError("Simple processor requires 'function' parameter")
            
        # For demo purposes, create some basic functions
        if func_name == "uppercase":
            def process_func(input_data):
                for key, value in input_data.items():
                    if isinstance(value, str):
                        return value.upper()
                return str(input_data).upper()
        elif func_name == "multiply":
            multiplier = config.parameters.get("multiplier", 2)
            def process_func(input_data):
                for key, value in input_data.items():
                    if isinstance(value, (int, float)):
                        return value * multiplier
                return input_data
        else:
            def process_func(input_data):
                return input_data  # Pass-through
                
        return SimpleProcessor(process_func, config.processor_id, **config.parameters)
        
    def _create_text_transformer(self, config: ProcessorConfig) -> SimpleProcessor:
        """Create a text transformation processor"""
        transformation = config.parameters.get("transformation", "none")
        
        async def process_func(input_data):
            text = None
            for key, value in input_data.items():
                if isinstance(value, str):
                    text = value
                    break
                    
            if text is None:
                text = str(input_data)
                
            if transformation == "uppercase":
                return text.upper()
            elif transformation == "lowercase":
                return text.lower()
            elif transformation == "reverse":
                return text[::-1]
            elif transformation == "length":
                return len(text)
            else:
                return text
                
        return SimpleProcessor(process_func, config.processor_id, **config.parameters)
        
    def _create_data_filter(self, config: ProcessorConfig) -> SimpleProcessor:
        """Create a data filtering processor"""
        filter_condition = config.parameters.get("condition", "all")
        
        async def process_func(input_data):
            # Simple filtering logic
            if filter_condition == "numbers_only":
                for key, value in input_data.items():
                    if isinstance(value, (int, float)):
                        return value
                return None
            elif filter_condition == "strings_only":
                for key, value in input_data.items():
                    if isinstance(value, str):
                        return value
                return None
            else:
                return input_data  # Pass all data
                
        return SimpleProcessor(process_func, config.processor_id, **config.parameters)
        
    def _create_aggregator(self, config: ProcessorConfig) -> SimpleProcessor:
        """Create an aggregation processor"""
        operation = config.parameters.get("operation", "count")
        
        def __init__(self):
            self.collected_data = []
            
        async def process_func(input_data):
            # Simple aggregation - in real implementation, this would be more sophisticated
            if operation == "count":
                return len(str(input_data))
            elif operation == "sum":
                total = 0
                for key, value in input_data.items():
                    if isinstance(value, (int, float)):
                        total += value
                return total
            else:
                return input_data
                
        return SimpleProcessor(process_func, config.processor_id, **config.parameters)


def load_graph_from_file(config_path: Union[str, Path]) -> ProcessingGraph:
    """Convenience function to load a graph from a configuration file"""
    loader = ConfigLoader()
    config = loader.load_from_file(config_path)
    return loader.create_graph_from_config(config)


def create_example_config() -> Dict[str, Any]:
    """Create an example configuration for demonstration"""
    return {
        "graph_id": "example_processing_graph",
        "processors": [
            {
                "processor_id": "text_input",
                "processor_type": "simple",
                "parameters": {"function": "passthrough"},
                "input_pipes": ["input"],
                "output_pipes": ["output"]
            },
            {
                "processor_id": "text_transformer",
                "processor_type": "text_transformer",
                "parameters": {"transformation": "uppercase"},
                "input_pipes": ["input"],
                "output_pipes": ["output"]
            },
            {
                "processor_id": "text_filter",
                "processor_type": "data_filter",
                "parameters": {"condition": "strings_only"},
                "input_pipes": ["input"],
                "output_pipes": ["output"]
            }
        ],
        "connections": [
            {
                "from_processor": "text_input",
                "from_output": "output",
                "to_processor": "text_transformer",
                "to_input": "input"
            },
            {
                "from_processor": "text_transformer",
                "from_output": "output",
                "to_processor": "text_filter",
                "to_input": "input"
            }
        ],
        "error_strategy": {
            "type": "composite",
            "strategy_order": "sequential",
            "strategies": [
                {
                    "type": "retry",
                    "max_retries": 2,
                    "initial_delay": 0.5
                },
                {
                    "type": "skip",
                    "max_skips": 5
                }
            ]
        },
        "monitoring": {
            "enabled": True,
            "log_level": "info",
            "max_events": 1000
        },
        "checkpoints": {
            "enabled": True,
            "storage_path": "./checkpoints",
            "max_checkpoints": 100
        }
    } 