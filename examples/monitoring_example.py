"""
Example demonstrating how to use the monitoring and visualization system.

This example shows:
1. How to enable monitoring on a pipeline with minimal code changes
2. Different monitoring configurations
3. How to use the MonitoringPlugin directly
4. Integration with existing pipelines
"""

import asyncio
import time
from typing import Any, AsyncGenerator

# Import the processor pipeline components
from processor_pipeline.new_core import AsyncProcessor, AsyncPipeline

# Import monitoring components
from processor_pipeline.monitoring import (
    enable_monitoring, 
    MonitoringPlugin, 
    MonitoredPipeline,
    MonitoringConfig
)


class TextProcessor(AsyncProcessor):
    """Example processor that processes text data"""
    
    meta = {
        "name": "TextProcessor",
        "input_type": str,
        "output_type": str,
    }
    
    def __init__(self, delay: float = 0.1, **kwargs):
        super().__init__(**kwargs)
        self.delay = delay
    
    async def process(self, data: str, *args, **kwargs) -> AsyncGenerator[str, None]:
        """Process text by converting to uppercase with a delay"""
        await asyncio.sleep(self.delay)  # Simulate processing time
        yield data.upper()


class WordCountProcessor(AsyncProcessor):
    """Example processor that counts words"""
    
    meta = {
        "name": "WordCountProcessor", 
        "input_type": str,
        "output_type": dict,
    }
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    async def process(self, data: str, *args, **kwargs) -> AsyncGenerator[dict, None]:
        """Count words in the text"""
        await asyncio.sleep(0.05)  # Simulate processing time
        words = data.split()
        yield {
            "text": data,
            "word_count": len(words),
            "character_count": len(data),
            "unique_words": len(set(word.lower() for word in words))
        }


class DataFormatterProcessor(AsyncProcessor):
    """Example processor that formats the final output"""
    
    meta = {
        "name": "DataFormatterProcessor",
        "input_type": dict,
        "output_type": str,
    }
    
    async def process(self, data: dict, *args, **kwargs) -> AsyncGenerator[str, None]:
        """Format the data as a readable string"""
        await asyncio.sleep(0.02)  # Simulate processing time
        formatted = (
            f"Text: '{data['text']}' | "
            f"Words: {data['word_count']} | "
            f"Characters: {data['character_count']} | "
            f"Unique: {data['unique_words']}"
        )
        yield formatted


async def example_basic_monitoring():
    """
    Example 1: Basic monitoring with minimal code changes
    """
    print("\n=== Example 1: Basic Monitoring ===")
    
    # Create a regular pipeline
    pipeline = AsyncPipeline([
        TextProcessor(delay=0.2),
        WordCountProcessor(),
        DataFormatterProcessor(),
    ])
    
    # Enable monitoring with one line - original code unchanged!
    monitored_pipeline = enable_monitoring(
        pipeline,
        web_ui_url="http://localhost:3000",
        session_name="basic_example"
    )
    
    # Use the pipeline normally
    sample_data = [
        "Hello world",
        "This is a test",
        "Monitoring makes debugging easier",
        "Real-time visualization is powerful"
    ]
    
    print("Processing data with monitoring enabled...")
    results = []
    async for result in monitored_pipeline.astream(sample_data):
        print(f"Result: {result}")
        results.append(result)
    
    print(f"Processed {len(results)} items")
    print(f"Session ID: {monitored_pipeline.get_session_id()}")
    
    # Keep running for a bit to see the data in the web UI
    print("Check the web UI at http://localhost:3000")
    await asyncio.sleep(2)
    
    # Stop monitoring
    await monitored_pipeline.stop_monitoring()


async def example_custom_config():
    """
    Example 2: Custom monitoring configuration
    """
    print("\n=== Example 2: Custom Configuration ===")
    
    # Create custom monitoring configuration
    config = MonitoringConfig(
        web_ui_url="http://localhost:3000",
        session_name="custom_config_example",
        collect_input_data=True,   # Collect input data samples
        collect_output_data=True,  # Collect output data samples
        metrics_interval=0.5,      # Update metrics every 0.5 seconds
        max_events_buffer=500      # Keep up to 500 events in buffer
    )
    
    # Create processors
    processors = [
        TextProcessor(delay=0.1),
        WordCountProcessor(),
        DataFormatterProcessor(),
    ]
    
    # Use MonitoringPlugin directly for more control
    monitored_pipeline = MonitoringPlugin(
        AsyncPipeline(processors),
        config=config
    )
    
    # Process data
    sample_data = ["Custom configuration example", "With detailed monitoring"]
    
    print("Processing with custom configuration...")
    async for result in monitored_pipeline.astream(sample_data):
        print(f"Result: {result}")
    
    # Get session information
    session_info = monitored_pipeline.get_session_info()
    print(f"Session: {session_info['session_name']}")
    print(f"Total events: {session_info['total_events']}")
    
    await monitored_pipeline.stop_monitoring()


async def example_monitored_pipeline():
    """
    Example 3: Using MonitoredPipeline convenience class
    """
    print("\n=== Example 3: MonitoredPipeline Class ===")
    
    # Create a monitored pipeline directly
    pipeline = MonitoredPipeline(
        processors=[
            TextProcessor(delay=0.15),
            WordCountProcessor(), 
            DataFormatterProcessor(),
        ],
        session_name="monitored_pipeline_example",
        web_ui_url="http://localhost:3000"
    )
    
    # Process data
    sample_data = [
        "MonitoredPipeline is convenient",
        "It combines pipeline creation with monitoring"
    ]
    
    print("Processing with MonitoredPipeline...")
    results = []
    async for result in pipeline.astream(sample_data):
        print(f"Result: {result}")
        results.append(result)
    
    # Get metrics
    metrics = pipeline.get_metrics()
    print(f"Collected metrics for {len(metrics)} processors")
    
    await pipeline.stop_monitoring()


async def example_error_handling():
    """
    Example 4: Monitoring with error handling
    """
    print("\n=== Example 4: Error Handling Monitoring ===")
    
    class ErrorProneProcessor(AsyncProcessor):
        """Processor that sometimes fails"""
        
        meta = {
            "name": "ErrorProneProcessor",
            "input_type": str,
            "output_type": str,
        }
        
        def __init__(self, error_rate: float = 0.3, **kwargs):
            super().__init__(**kwargs)
            self.error_rate = error_rate
            self.processed_count = 0
        
        async def process(self, data: str, *args, **kwargs) -> AsyncGenerator[str, None]:
            self.processed_count += 1
            
            # Simulate random errors
            if self.processed_count % 3 == 0:  # Every 3rd item fails
                raise ValueError(f"Simulated error processing: {data}")
            
            await asyncio.sleep(0.1)
            yield f"Processed: {data}"
    
    # Create pipeline with error-prone processor
    pipeline = AsyncPipeline([
        ErrorProneProcessor(),
        DataFormatterProcessor(),
    ])
    
    monitored_pipeline = enable_monitoring(
        pipeline,
        session_name="error_handling_example"
    )
    
    # Process data that will cause some errors
    sample_data = [
        "Item 1", "Item 2", "Item 3",  # Item 3 will fail
        "Item 4", "Item 5", "Item 6",  # Item 6 will fail
        "Item 7", "Item 8", "Item 9",  # Item 9 will fail
    ]
    
    print("Processing data with simulated errors...")
    successful_results = []
    
    try:
        async for result in monitored_pipeline.astream(sample_data):
            print(f"Success: {result}")
            successful_results.append(result)
    except Exception as e:
        print(f"Pipeline error: {e}")
    
    print(f"Successfully processed {len(successful_results)} items")
    print("Check the web UI to see error events and metrics")
    
    await asyncio.sleep(1)
    await monitored_pipeline.stop_monitoring()


async def example_context_manager():
    """
    Example 5: Using monitoring with context manager
    """
    print("\n=== Example 5: Context Manager Usage ===")
    
    # Use monitoring as a context manager for automatic cleanup
    async with enable_monitoring(
        AsyncPipeline([
            TextProcessor(delay=0.1),
            WordCountProcessor(),
        ]),
        session_name="context_manager_example"
    ) as monitored_pipeline:
        
        sample_data = ["Context manager example", "Automatic cleanup"]
        
        print("Processing with context manager...")
        async for result in monitored_pipeline.astream(sample_data):
            print(f"Result: {result}")
        
        print("Context manager will automatically stop monitoring")


async def main():
    """Run all examples"""
    print("Processor Pipeline Monitoring Examples")
    print("=====================================")
    print("\n📊 Make sure to start the visualization server:")
    print("   cd visualization/backend && python main.py")
    print("   cd visualization/frontend && npm run dev")
    print("   Then open http://localhost:3000")
    print("\nRunning examples...")
    
    try:
        await example_basic_monitoring()
        await asyncio.sleep(1)
        
        await example_custom_config()
        await asyncio.sleep(1)
        
        await example_monitored_pipeline()
        await asyncio.sleep(1)
        
        await example_error_handling()
        await asyncio.sleep(1)
        
        await example_context_manager()
        
    except KeyboardInterrupt:
        print("\n\nExamples interrupted by user")
    except Exception as e:
        print(f"\nError running examples: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n✅ Examples completed!")
    print("🌐 Visit http://localhost:3000 to see the monitoring dashboard")


if __name__ == "__main__":
    asyncio.run(main())