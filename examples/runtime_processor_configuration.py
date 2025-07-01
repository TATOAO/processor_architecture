"""
Example demonstrating runtime processor creation and configuration.

This example shows how to:
1. List registered processors
2. Create processors from registry
3. Configure pipelines dynamically
4. Create new processors at runtime
5. Save and load pipeline configurations
"""

import asyncio
import os
import sys

# Add the processor_pipeline to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from processor_pipeline.core.helper import (
    list_registered_processors,
    create_processor_from_registry,
    create_pipeline_from_config,
    create_dynamic_processor,
    save_pipeline_configuration,
    load_pipeline_from_file,
    get_pipeline_configuration
)
from processor_pipeline.core.processor import Processor, AsyncProcessor
from typing import Any

# First, let's import the example processors to register them
from doc_chunker_embedding import TxtToTextProcessor, TextChunkerProcessor, VectorGeneratorProcessor

def demonstrate_listing_processors():
    """Demonstrate listing all registered processors"""
    print("=== Available Registered Processors ===")
    processors = list_registered_processors()
    
    for name, info in processors.items():
        print(f"\nProcessor: {name}")
        print(f"  Class: {info['class_name']}")
        print(f"  Input Type: {info['input_type']}")
        print(f"  Output Type: {info['output_type']}")
        print(f"  Description: {info['description']}")

def demonstrate_creating_from_registry():
    """Demonstrate creating processors from registry"""
    print("\n=== Creating Processors from Registry ===")
    
    # Create processors with default parameters
    txt_processor = create_processor_from_registry("txt_to_text_processor")
    print(f"Created: {txt_processor.__class__.__name__}")
    
    # Create processors with custom parameters
    chunker = create_processor_from_registry(
        "text_chunker_processor", 
        window_size=300, 
        overlap=50
    )
    print(f"Created: {chunker.__class__.__name__} with window_size=300, overlap=50")
    
    vector_gen = create_processor_from_registry("vector_generator_processor")
    print(f"Created: {vector_gen.__class__.__name__}")

def demonstrate_pipeline_from_config():
    """Demonstrate creating pipelines from configuration"""
    print("\n=== Creating Pipeline from Configuration ===")
    
    # Define pipeline configuration
    pipeline_config = [
        {"name": "txt_to_text_processor"},
        {"name": "text_chunker_processor", "params": {"window_size": 200, "overlap": 30}},
        {"name": "vector_generator_processor"}
    ]
    
    # Create async pipeline
    pipeline = create_pipeline_from_config(pipeline_config, "async")
    print(f"Created AsyncPipeline with {len(pipeline.processors)} processors")
    
    # Show pipeline configuration
    config = get_pipeline_configuration(pipeline)
    print("Pipeline configuration:")
    for i, proc_config in enumerate(config):
        print(f"  {i+1}. {proc_config['name']} ({proc_config['class_name']})")

def demonstrate_dynamic_processors():
    """Demonstrate creating processors dynamically at runtime"""
    print("\n=== Creating Dynamic Processors ===")
    
    # Create a simple text processor
    def reverse_text_func(data):
        """Reverse the input text"""
        return data[::-1]
    
    ReverseTextProcessor = create_dynamic_processor(
        name="reverse_text_processor",
        process_func=reverse_text_func,
        input_type=str,
        output_type=str,
        description="Reverses input text"
    )
    print("Created reverse_text_processor")
    
    # Create a list processor
    def count_items_func(data):
        """Count items in a list"""
        return {"count": len(data), "items": data}
    
    CountItemsProcessor = create_dynamic_processor(
        name="count_items_processor",
        process_func=count_items_func,
        input_type=list,
        output_type=dict,
        description="Counts items in a list"
    )
    print("Created count_items_processor")
    
    # Create an async processor
    async def async_upper_func(data):
        """Convert text to uppercase with delay"""
        await asyncio.sleep(0.1)  # Simulate async work
        if hasattr(data, '__iter__') and not isinstance(data, str):
            # Handle async generator input
            result = []
            async for item in data:
                result.append(item.upper())
            return result
        else:
            return data.upper()
    
    AsyncUpperProcessor = create_dynamic_processor(
        name="async_upper_processor",
        process_func=async_upper_func,
        input_type=str,
        output_type=str,
        is_async=True,
        description="Converts text to uppercase asynchronously"
    )
    print("Created async_upper_processor")

def demonstrate_using_dynamic_processors():
    """Demonstrate using the dynamically created processors"""
    print("\n=== Using Dynamic Processors ===")
    
    # Test reverse text processor
    reverse_processor = create_processor_from_registry("reverse_text_processor")
    result = reverse_processor.process("Hello World")
    print(f"Reverse text: 'Hello World' -> '{result}'")
    
    # Test count items processor
    count_processor = create_processor_from_registry("count_items_processor")
    result = count_processor.process(["apple", "banana", "cherry"])
    print(f"Count items: {result}")

async def demonstrate_async_dynamic_pipeline():
    """Demonstrate using dynamic processors in an async pipeline"""
    print("\n=== Dynamic Async Pipeline ===")
    
    # Create a pipeline with dynamic processors
    dynamic_config = [
        {"name": "reverse_text_processor"},
        {"name": "async_upper_processor"}
    ]
    
    dynamic_pipeline = create_pipeline_from_config(dynamic_config, "async")
    
    # Run the pipeline
    result = await dynamic_pipeline.run(["hello world", "python rocks"])
    print(f"Pipeline result: {result}")

def demonstrate_save_load_config():
    """Demonstrate saving and loading pipeline configurations"""
    print("\n=== Save/Load Pipeline Configuration ===")
    
    # Create a pipeline
    config = [
        {"name": "txt_to_text_processor"},
        {"name": "text_chunker_processor", "params": {"window_size": 400}},
        {"name": "reverse_text_processor"},  # Our dynamic processor
    ]
    
    pipeline = create_pipeline_from_config(config, "sync")
    
    # Save configuration
    config_file = "my_pipeline_config.json"
    save_pipeline_configuration(pipeline, config_file)
    print(f"Saved pipeline configuration to {config_file}")
    
    # Load configuration
    loaded_pipeline = load_pipeline_from_file(config_file, "sync")
    print(f"Loaded pipeline with {len(loaded_pipeline.processors)} processors")
    
    # Show loaded configuration
    loaded_config = get_pipeline_configuration(loaded_pipeline)
    print("Loaded pipeline processors:")
    for i, proc_config in enumerate(loaded_config):
        print(f"  {i+1}. {proc_config['name']}")
    
    # Cleanup
    if os.path.exists(config_file):
        os.remove(config_file)
        print(f"Cleaned up {config_file}")

async def main():
    """Main demonstration function"""
    print("Runtime Processor Configuration Demo")
    print("=" * 50)
    
    # Demonstrate all features
    demonstrate_listing_processors()
    demonstrate_creating_from_registry()
    demonstrate_pipeline_from_config()
    demonstrate_dynamic_processors()
    demonstrate_using_dynamic_processors()
    await demonstrate_async_dynamic_pipeline()
    demonstrate_save_load_config()
    
    print("\n" + "=" * 50)
    print("Demo completed! You can now:")
    print("1. List available processors: list_registered_processors()")
    print("2. Create processors by name: create_processor_from_registry('name')")
    print("3. Build pipelines from config: create_pipeline_from_config(config)")
    print("4. Create new processors: create_dynamic_processor()")
    print("5. Save/load pipeline configs: save_pipeline_configuration()")

if __name__ == "__main__":
    asyncio.run(main()) 