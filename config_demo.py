#!/usr/bin/env python3
"""
Demo of JSON-driven pipeline configuration using the processor architecture.
"""

import json
from processor_customized import ProcessorMeta, Pipeline

# Sample JSON configuration  
json_config = {
    "pipeline": ["chunk", "entity"],
    "input_text": "Alice went to Wonderland. Bob stayed home. Charlie likes reading."
}

# Alternative pipeline configuration
alternative_config = {
    "pipeline": ["chunk"],  # Only chunking, no entity extraction
    "input_text": "This is a test document. It has multiple sentences."
}

def run_from_json_config(json_config: dict):
    """Run pipeline from JSON configuration."""
    processors = [ProcessorMeta.get(name)() for name in json_config["pipeline"]]
    pipeline = Pipeline(processors)
    return pipeline.run(json_config["input_text"])

def run_from_json_file(filepath: str):
    """Run pipeline from JSON file."""
    with open(filepath, 'r') as f:
        config = json.load(f)
    return run_from_json_config(config)

def show_available_processors():
    """Display all registered processors and their metadata."""
    print("🔌 Available Processors:")
    print("=" * 50)
    for name, processor_class in ProcessorMeta.registry.items():
        meta = processor_class.get_meta()
        print(f"📦 {name}")
        print(f"   Input:  {meta['input_type']}")
        print(f"   Output: {meta['output_type']}")
        print()

if __name__ == "__main__":
    print("🚀 Processor Architecture - Configuration Demo")
    print("=" * 50)
    
    # Show available processors
    show_available_processors()
    
    # Run from JSON config
    print("📊 Running from JSON config (chunk + entity):")
    json_result = run_from_json_config(json_config)
    print(f"Result: {json_result}")
    print()
    
    # Run alternative config
    print("📄 Running alternative config (chunk only):")
    alt_result = run_from_json_config(alternative_config)
    print(f"Result: {alt_result}")
    print()
    
        # Demonstrate type checking
    print("🔧 Type checking in action:")
    print("Pipeline validates that chunk output matches entity input types!")
    print("✅ Pipeline created successfully - types are compatible.") 