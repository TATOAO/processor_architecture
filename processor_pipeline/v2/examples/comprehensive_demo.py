"""
Comprehensive demonstration of the v2 processing pipeline system.

This example showcases:
- DAG-based processing with multiple processors
- Real-time monitoring and logging
- Error handling with different strategies
- Checkpointing and debugging
- Configuration-based pipeline creation
- Pipe tapping for observability
"""

import asyncio
import json
from pathlib import Path

# Import v2 components
from processor_pipeline.v2 import (
    ProcessingGraph, GraphExecutor, BaseProcessor, SimpleProcessor,
    Monitor, CheckpointManager, Pipe, PipeFactory,
    SkipStrategy, RetryStrategy, HaltStrategy, CompositeStrategy,
    ConfigLoader, create_simple_pipeline, visualize_graph, run_simple_pipeline
)


class TextProcessor(BaseProcessor):
    """Custom text processing processor"""
    
    async def process_data(self, input_data):
        """Process text data with various transformations"""
        for key, value in input_data.items():
            if isinstance(value, str):
                # Apply transformations based on processor config
                transformation = self.config.get('transformation', 'uppercase')
                
                if transformation == 'uppercase':
                    return value.upper()
                elif transformation == 'lowercase':
                    return value.lower()
                elif transformation == 'reverse':
                    return value[::-1]
                elif transformation == 'length':
                    return len(value)
                else:
                    return value
        
        return str(input_data)


class NumberProcessor(BaseProcessor):
    """Custom number processing processor"""
    
    async def process_data(self, input_data):
        """Process numeric data"""
        for key, value in input_data.items():
            if isinstance(value, (int, float)):
                operation = self.config.get('operation', 'double')
                
                if operation == 'double':
                    return value * 2
                elif operation == 'square':
                    return value ** 2
                elif operation == 'sqrt':
                    return value ** 0.5
                else:
                    return value
                    
        # Try to convert string to number
        for key, value in input_data.items():
            if isinstance(value, str) and value.isdigit():
                return float(value) * 2
                
        return 0


class AggregatorProcessor(BaseProcessor):
    """Aggregates data from multiple inputs"""
    
    def __init__(self, processor_id=None, **kwargs):
        super().__init__(processor_id, **kwargs)
        self.collected_data = []
        
    async def process_data(self, input_data):
        """Aggregate multiple inputs"""
        # Collect data from all input pipes
        aggregated = {}
        
        for pipe_name, data in input_data.items():
            aggregated[pipe_name] = data
            
        # Perform aggregation based on config
        agg_type = self.config.get('aggregation', 'combine')
        
        if agg_type == 'combine':
            return f"Combined: {json.dumps(aggregated)}"
        elif agg_type == 'sum' and all(isinstance(v, (int, float)) for v in aggregated.values()):
            return sum(aggregated.values())
        elif agg_type == 'count':
            return len(aggregated)
        else:
            return aggregated


async def demonstrate_basic_dag():
    """Demonstrate basic DAG construction and execution"""
    print("🔧 Creating a Basic DAG Processing Pipeline")
    print("=" * 50)
    
    # Create a processing graph
    graph = ProcessingGraph("basic_dag_demo")
    
    # Create processors
    text_proc = TextProcessor("text_processor", transformation='uppercase')
    text_proc.create_default_pipes(["input"], ["output"])
    
    number_proc = NumberProcessor("number_processor", operation='square')
    number_proc.create_default_pipes(["input"], ["output"])
    
    aggregator = AggregatorProcessor("aggregator", aggregation='combine')
    aggregator.create_default_pipes(["text_input", "number_input"], ["output"])
    
    # Add processors to graph
    graph.add_processor(text_proc)
    graph.add_processor(number_proc)
    graph.add_processor(aggregator)
    
    # Create connections
    graph.connect("text_processor", "output", "aggregator", "text_input")
    graph.connect("number_processor", "output", "aggregator", "number_input")
    
    # Validate the graph
    print("Validating graph...")
    errors = graph.validate()
    if errors:
        print(f"❌ Validation errors: {errors}")
        return
    else:
        print("✅ Graph validation passed!")
        
    # Visualize the graph
    print("\nGraph Visualization:")
    print(visualize_graph(graph))
    
    # Execute the graph
    print("\nExecuting the graph...")
    monitor = Monitor()
    executor = GraphExecutor(graph, monitor)
    
    input_data = {
        "text_processor": "hello world",
        "number_processor": 5
    }
    
    results = await executor.execute(input_data, monitor)
    print(f"✅ Execution completed!")
    print(f"Results: {results}")
    
    # Show monitoring metrics
    print("\nMonitoring Summary:")
    metrics = monitor.get_metrics_summary()
    print(f"Total events: {metrics['total_events']}")
    print(f"Active processors: {metrics['active_processors']}")
    
    return graph, results


async def demonstrate_error_handling():
    """Demonstrate different error handling strategies"""
    print("\n🛡️ Demonstrating Error Handling Strategies")
    print("=" * 50)
    
    # Create a processor that sometimes fails
    class FaultyProcessor(BaseProcessor):
        def __init__(self, processor_id=None, **kwargs):
            super().__init__(processor_id, **kwargs)
            self.call_count = 0
            
        async def process_data(self, input_data):
            self.call_count += 1
            
            # Fail on first two calls, succeed on third
            if self.call_count <= 2:
                raise Exception(f"Simulated failure #{self.call_count}")
            else:
                return f"Success after {self.call_count} attempts!"
    
    # Test different error strategies
    strategies = [
        ("Skip Strategy", SkipStrategy(max_skips=3)),
        ("Retry Strategy", RetryStrategy(max_retries=3, initial_delay=0.1)),
        ("Composite Strategy", CompositeStrategy([
            RetryStrategy(max_retries=2, initial_delay=0.1),
            SkipStrategy(max_skips=1)
        ]))
    ]
    
    for strategy_name, strategy in strategies:
        print(f"\nTesting {strategy_name}:")
        
        # Create graph with error strategy
        graph = ProcessingGraph("error_demo", error_strategy=strategy)
        
        faulty_proc = FaultyProcessor("faulty_processor")
        faulty_proc.create_default_pipes(["input"], ["output"])
        graph.add_processor(faulty_proc)
        
        monitor = Monitor()
        executor = GraphExecutor(graph, monitor)
        
        try:
            results = await executor.execute({"faulty_processor": "test_data"}, monitor)
            print(f"  ✅ Strategy succeeded: {results}")
        except Exception as e:
            print(f"  ❌ Strategy failed: {e}")
            
        # Reset processor for next test
        faulty_proc.call_count = 0


async def demonstrate_monitoring_and_checkpoints():
    """Demonstrate monitoring and checkpointing capabilities"""
    print("\n📊 Demonstrating Monitoring and Checkpointing")
    print("=" * 50)
    
    # Create a simple pipeline
    pipeline = create_simple_pipeline([
        ("step1", lambda data: f"Step1: {data}"),
        ("step2", lambda data: f"Step2: {data}"),
        ("step3", lambda data: f"Step3: {data}")
    ], "monitoring_demo")
    
    # Create monitor and checkpoint manager
    monitor = Monitor(log_level="DEBUG")
    checkpoint_manager = CheckpointManager("./demo_checkpoints")
    
    # Create a tap to monitor data flow
    def data_tap_callback(data):
        print(f"  📡 Data tap captured: {data}")
        
    # Set up monitoring
    monitor.start_monitoring()
    
    # Add pipe taps
    for processor in pipeline.get_all_processors().values():
        for pipe_name, pipe in processor.output_pipes.items():
            tap_id = monitor.tap_pipe(pipe, data_tap_callback)
            print(f"Added tap {tap_id} to {processor.processor_id}:{pipe_name}")
    
    # Execute with monitoring
    executor = GraphExecutor(pipeline, monitor)
    
    print("\nExecuting pipeline with monitoring...")
    results = await executor.execute({"step1": "Hello World"}, monitor)
    print(f"Results: {results}")
    
    # Create checkpoints
    print("\nCreating checkpoints...")
    for processor in pipeline.get_all_processors().values():
        checkpoint_id = checkpoint_manager.create_checkpoint(
            f"demo_checkpoint_{processor.processor_id}",
            processor.processor_id,
            {"state": "demo", "metrics": processor.get_metrics()}
        )
        print(f"Created checkpoint {checkpoint_id} for {processor.processor_id}")
    
    # Show monitoring results
    print("\nMonitoring Results:")
    recent_events = monitor.get_recent_events(count=10)
    for event in recent_events[-3:]:  # Show last 3 events
        print(f"  {event['timestamp']}: {event['event_type']} - {event['data']}")
    
    # Show checkpoint information
    print("\nCheckpoint Information:")
    checkpoints = checkpoint_manager.list_checkpoints()
    for cp in checkpoints[:3]:  # Show first 3 checkpoints
        print(f"  {cp['checkpoint_id']}: {cp['name']} ({cp['size_bytes']} bytes)")
    
    monitor.stop_monitoring()


async def demonstrate_configuration_system():
    """Demonstrate configuration-based pipeline creation"""
    print("\n⚙️ Demonstrating Configuration System")
    print("=" * 50)
    
    # Create example configuration
    config_data = {
        "graph_id": "config_demo_pipeline",
        "processors": [
            {
                "processor_id": "input_processor",
                "processor_type": "simple",
                "parameters": {"function": "passthrough"},
                "input_pipes": ["input"],
                "output_pipes": ["output"]
            },
            {
                "processor_id": "transform_processor",
                "processor_type": "text_transformer",
                "parameters": {"transformation": "uppercase"},
                "input_pipes": ["input"],
                "output_pipes": ["output"]
            },
            {
                "processor_id": "filter_processor",
                "processor_type": "data_filter",
                "parameters": {"condition": "strings_only"},
                "input_pipes": ["input"],
                "output_pipes": ["output"]
            }
        ],
        "connections": [
            {
                "from_processor": "input_processor",
                "from_output": "output",
                "to_processor": "transform_processor",
                "to_input": "input"
            },
            {
                "from_processor": "transform_processor",
                "from_output": "output",
                "to_processor": "filter_processor",
                "to_input": "input"
            }
        ],
        "error_strategy": {
            "type": "retry",
            "max_retries": 2,
            "initial_delay": 0.1
        },
        "monitoring": {
            "enabled": True,
            "log_level": "info"
        }
    }
    
    # Save configuration to file
    config_path = Path("demo_config.json")
    with open(config_path, 'w') as f:
        json.dump(config_data, f, indent=2)
    
    print(f"Created configuration file: {config_path}")
    
    # Load configuration and create graph
    loader = ConfigLoader()
    config = loader.load_from_dict(config_data)
    graph = loader.create_graph_from_config(config)
    
    print(f"Created graph from configuration: {graph.graph_id}")
    print(f"Processors: {len(graph.nodes)}")
    print(f"Connections: {len(graph.connections)}")
    
    # Execute the configured graph
    monitor = loader.create_monitor_from_config(config_data.get("monitoring", {}))
    executor = GraphExecutor(graph, monitor)
    
    print("\nExecuting configured pipeline...")
    results = await executor.execute({"input_processor": "hello configuration world"}, monitor)
    print(f"Results: {results}")
    
    # Clean up
    config_path.unlink()


async def demonstrate_complex_dag():
    """Demonstrate a complex DAG with multiple branches and aggregation"""
    print("\n🌐 Demonstrating Complex DAG Processing")
    print("=" * 50)
    
    # Create a complex processing graph
    graph = ProcessingGraph("complex_dag_demo")
    
    # Source processor
    source = SimpleProcessor(lambda data: data, "source")
    source.create_default_pipes(["input"], ["text_out", "number_out"])
    
    # Text processing branch
    text_upper = TextProcessor("text_upper", transformation='uppercase')
    text_upper.create_default_pipes(["input"], ["output"])
    
    text_reverse = TextProcessor("text_reverse", transformation='reverse')
    text_reverse.create_default_pipes(["input"], ["output"])
    
    # Number processing branch
    number_square = NumberProcessor("number_square", operation='square')
    number_square.create_default_pipes(["input"], ["output"])
    
    number_double = NumberProcessor("number_double", operation='double')
    number_double.create_default_pipes(["input"], ["output"])
    
    # Aggregators
    text_agg = AggregatorProcessor("text_aggregator", aggregation='combine')
    text_agg.create_default_pipes(["upper", "reverse"], ["output"])
    
    number_agg = AggregatorProcessor("number_aggregator", aggregation='sum')
    number_agg.create_default_pipes(["square", "double"], ["output"])
    
    # Final aggregator
    final_agg = AggregatorProcessor("final_aggregator", aggregation='combine')
    final_agg.create_default_pipes(["text", "numbers"], ["output"])
    
    # Add all processors
    processors = [source, text_upper, text_reverse, number_square, number_double,
                 text_agg, number_agg, final_agg]
    for proc in processors:
        graph.add_processor(proc)
    
    # Create connections
    connections = [
        ("source", "text_out", "text_upper", "input"),
        ("source", "text_out", "text_reverse", "input"),
        ("source", "number_out", "number_square", "input"),
        ("source", "number_out", "number_double", "input"),
        ("text_upper", "output", "text_aggregator", "upper"),
        ("text_reverse", "output", "text_aggregator", "reverse"),
        ("number_square", "output", "number_aggregator", "square"),
        ("number_double", "output", "number_aggregator", "double"),
        ("text_aggregator", "output", "final_aggregator", "text"),
        ("number_aggregator", "output", "final_aggregator", "numbers")
    ]
    
    for from_proc, from_out, to_proc, to_in in connections:
        graph.connect(from_proc, from_out, to_proc, to_in)
    
    # Validate and visualize
    errors = graph.validate()
    if errors:
        print(f"❌ Validation errors: {errors}")
        return
    
    print("Complex DAG Structure:")
    print(visualize_graph(graph))
    
    # Execute with monitoring
    monitor = Monitor()
    executor = GraphExecutor(graph, monitor)
    
    input_data = {"source": {"text": "Hello", "number": 5}}
    
    print("\nExecuting complex DAG...")
    results = await executor.execute(input_data, monitor)
    print(f"✅ Complex DAG execution completed!")
    print(f"Final results: {results}")
    
    # Show execution statistics
    print(f"\nExecution Statistics:")
    print(f"Total processors: {len(graph.nodes)}")
    print(f"Execution levels: {len(graph.execution_order)}")
    for level, procs in enumerate(graph.execution_order):
        print(f"  Level {level}: {', '.join(procs)}")


async def main():
    """Run all demonstrations"""
    print("🚀 Processor Pipeline v2 - Comprehensive Demonstration")
    print("=" * 60)
    
    try:
        # Run all demonstrations
        await demonstrate_basic_dag()
        await demonstrate_error_handling()
        await demonstrate_monitoring_and_checkpoints()
        await demonstrate_configuration_system()
        await demonstrate_complex_dag()
        
        print("\n🎉 All demonstrations completed successfully!")
        print("\nKey features demonstrated:")
        print("✅ DAG-based processing with topological sorting")
        print("✅ Real-time monitoring and pipe tapping")
        print("✅ Comprehensive error handling strategies")
        print("✅ Checkpointing and debugging capabilities")
        print("✅ Configuration-based pipeline creation")
        print("✅ Complex multi-branch processing")
        print("✅ Async execution with parallel processing")
        
    except Exception as e:
        print(f"❌ Error during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main()) 