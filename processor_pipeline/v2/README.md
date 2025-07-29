# Processor Pipeline v2 - Advanced DAG Processing System

A comprehensive pipeline execution system supporting Directed Acyclic Graph (DAG) processing, real-time monitoring, advanced error handling, and dynamic configuration.

## 🚀 Key Features

### Core Functionality
- **DAG-Based Processing**: Create complex processing graphs with multiple branches and dependencies
- **Pipe System**: Explicit data flow channels between processors with monitoring capabilities
- **Async Execution**: Full asynchronous processing with parallel execution support
- **Topological Sorting**: Automatic dependency resolution and execution ordering

### Debugging & Observability
- **Real-time Monitoring**: Comprehensive monitoring system with event logging and metrics
- **Pipe Tapping**: Monitor data flow through any pipe in real-time
- **Checkpointing**: Capture and restore processor states for debugging
- **Breakpoints**: Set conditional breakpoints for step-by-step debugging

### Error Handling
- **Multiple Strategies**: Skip, retry, halt, circuit breaker patterns
- **Configurable Recovery**: Customizable error handling per processor or graph
- **Error Propagation**: Structured error tracking and reporting
- **Composite Strategies**: Combine multiple error handling approaches

### Configuration & Extensibility
- **YAML/JSON Config**: Define pipelines declaratively
- **Dynamic Creation**: Create processors and graphs at runtime
- **Custom Processors**: Easy extension with custom processing logic
- **Factory Pattern**: Pluggable processor creation system

## 📦 Installation

```bash
# Install the package (assuming it's in your Python path)
pip install processor-pipeline
```

## 🏃 Quick Start

### Simple Linear Pipeline

```python
import asyncio
from processor_pipeline.v2 import create_simple_pipeline, run_simple_pipeline

# Create a simple pipeline
pipeline = create_simple_pipeline([
    ("step1", lambda data: data.upper()),
    ("step2", lambda data: data.strip()),
    ("step3", lambda data: f"Processed: {data}")
])

# Run the pipeline
async def main():
    results = await run_simple_pipeline(pipeline, "  hello world  ")
    print(results)  # {'step3:output': 'Processed: HELLO WORLD'}

asyncio.run(main())
```

### DAG Processing

```python
from processor_pipeline.v2 import ProcessingGraph, GraphExecutor, SimpleProcessor

# Create a DAG
graph = ProcessingGraph("my_dag")

# Create processors
text_proc = SimpleProcessor(lambda data: data["text"].upper(), "text_processor")
text_proc.create_default_pipes(["input"], ["output"])

number_proc = SimpleProcessor(lambda data: data["number"] * 2, "number_processor")  
number_proc.create_default_pipes(["input"], ["output"])

# Create aggregator
def aggregate(data):
    return f"Text: {data.get('text_input', '')}, Number: {data.get('number_input', '')}"

agg_proc = SimpleProcessor(aggregate, "aggregator")
agg_proc.create_default_pipes(["text_input", "number_input"], ["output"])

# Add to graph and connect
for proc in [text_proc, number_proc, agg_proc]:
    graph.add_processor(proc)

graph.connect("text_processor", "output", "aggregator", "text_input")
graph.connect("number_processor", "output", "aggregator", "number_input")

# Execute
async def main():
    executor = GraphExecutor(graph)
    results = await executor.execute({
        "text_processor": {"text": "hello"},
        "number_processor": {"number": 5}
    })
    print(results)

asyncio.run(main())
```

## 🔧 Advanced Usage

### Custom Processors

```python
from processor_pipeline.v2 import BaseProcessor

class MyCustomProcessor(BaseProcessor):
    async def process_data(self, input_data):
        # Your custom processing logic
        result = self.transform_data(input_data)
        return result
    
    def transform_data(self, data):
        # Custom transformation
        return data
```

### Error Handling

```python
from processor_pipeline.v2 import RetryStrategy, SkipStrategy, CompositeStrategy

# Retry then skip strategy
error_strategy = CompositeStrategy([
    RetryStrategy(max_retries=3, initial_delay=1.0),
    SkipStrategy(max_skips=5)
], strategy_order="sequential")

graph = ProcessingGraph("my_graph", error_strategy=error_strategy)
```

### Monitoring and Checkpoints

```python
from processor_pipeline.v2 import Monitor, CheckpointManager

# Set up monitoring
monitor = Monitor(log_level="DEBUG")

# Set up checkpointing
checkpoint_manager = CheckpointManager("./checkpoints")

# Add pipe taps for monitoring
tap_id = monitor.tap_pipe(pipe, lambda data: print(f"Data: {data}"))

# Create checkpoints
checkpoint_id = checkpoint_manager.create_checkpoint(
    "my_checkpoint", processor_id, data
)

# Restore from checkpoint
restored_data = checkpoint_manager.restore_checkpoint(checkpoint_id)
```

### Configuration-Based Pipelines

```yaml
# pipeline_config.yaml
graph_id: my_configured_pipeline
processors:
  - processor_id: input_proc
    processor_type: simple
    parameters:
      function: passthrough
    input_pipes: [input]
    output_pipes: [output]
  
  - processor_id: transform_proc
    processor_type: text_transformer
    parameters:
      transformation: uppercase
    input_pipes: [input]
    output_pipes: [output]

connections:
  - from_processor: input_proc
    from_output: output
    to_processor: transform_proc
    to_input: input

error_strategy:
  type: retry
  max_retries: 3
  initial_delay: 0.5
```

```python
from processor_pipeline.v2 import ConfigLoader

# Load from configuration
loader = ConfigLoader()
config = loader.load_from_file("pipeline_config.yaml")
graph = loader.create_graph_from_config(config)
```

## 🏗️ Architecture

The v2 system is built around several key components:

### Core Components
- **ProcessingGraph**: DAG container managing processors and connections
- **BaseProcessor**: Abstract base class for all processors
- **Pipe**: Data flow channels between processors
- **GraphExecutor**: Executes graphs with monitoring and error handling

### Supporting Systems
- **Monitor**: Real-time monitoring and observability
- **CheckpointManager**: State capture and debugging
- **ErrorStrategy**: Configurable error handling
- **ConfigLoader**: Dynamic pipeline creation from configuration

### Execution Flow
1. **Graph Construction**: Add processors and create connections
2. **Validation**: Check for cycles, missing connections, etc.
3. **Topological Sort**: Determine execution order
4. **Parallel Execution**: Execute processors level by level
5. **Monitoring**: Real-time event logging and metrics
6. **Error Handling**: Apply configured error strategies

## 📊 Performance Features

- **Parallel Processing**: Concurrent execution of independent processors
- **Async Operations**: Non-blocking I/O throughout the system
- **Memory Efficient**: Streaming data through pipes
- **Configurable Concurrency**: Control parallelism levels
- **Performance Metrics**: Built-in timing and throughput monitoring

## 🔍 Debugging Features

- **Breakpoints**: Set conditional breakpoints on processors
- **Checkpoints**: Capture state at any point in processing
- **Pipe Tapping**: Monitor data flow in real-time
- **Event Logging**: Comprehensive execution logs
- **Graph Visualization**: Text and Mermaid diagram generation

## 🛡️ Error Handling Strategies

### Built-in Strategies
- **SkipStrategy**: Skip failed items and continue
- **RetryStrategy**: Retry with exponential backoff
- **HaltStrategy**: Stop processing on error
- **CircuitBreakerStrategy**: Fail fast when error rate is high
- **CompositeStrategy**: Combine multiple strategies

### Custom Strategies
```python
from processor_pipeline.v2 import ErrorStrategy

class MyErrorStrategy(ErrorStrategy):
    async def handle_error(self, error, processor_id, context):
        # Your custom error handling logic
        return True  # Continue processing
```

## 📈 Monitoring and Observability

### Event Types
- Processor start/complete/error
- Pipe data flow events  
- Graph execution events
- Checkpoint creation/restoration

### Metrics
- Throughput (items/second)
- Processing times
- Error rates
- Pipe utilization
- Memory usage

### Alerting
```python
from processor_pipeline.v2 import AlertManager

alert_manager = AlertManager(monitor)
alert_manager.add_alert_rule(
    "high_error_rate",
    condition=lambda event: event.event_type == EventType.PROCESSOR_ERROR,
    action=lambda event: print(f"Alert: {event}")
)
```

## 🔧 Extension Points

### Custom Processor Types
```python
# Register custom processor factory
loader = ConfigLoader()
loader.register_processor_factory("my_type", my_factory_function)
```

### Custom Error Strategies
```python
# Use in graph configuration
graph = ProcessingGraph(error_strategy=MyCustomStrategy())
```

### Custom Monitoring
```python
# Add custom event handlers
monitor.add_event_handler(EventType.PROCESSOR_START, my_handler)
```

## 📝 Best Practices

1. **Design for Failure**: Use appropriate error handling strategies
2. **Monitor Everything**: Set up comprehensive monitoring
3. **Test Thoroughly**: Validate graphs before production
4. **Use Checkpoints**: Capture state for debugging complex flows
5. **Configure Declaratively**: Use YAML/JSON for pipeline definitions
6. **Profile Performance**: Use built-in profiling tools
7. **Handle Backpressure**: Monitor pipe utilization

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

MIT License - see LICENSE file for details.

## 🆚 v1 vs v2 Comparison

| Feature | v1 | v2 |
|---------|----|----|
| Architecture | Linear pipelines | DAG-based graphs |
| Execution | Sequential | Parallel async |
| Monitoring | Basic logging | Real-time monitoring |
| Error Handling | Simple retry | Multiple strategies |
| Debugging | Limited | Checkpoints & breakpoints |
| Configuration | Code-based | YAML/JSON support |
| Pipe System | Implicit | Explicit with monitoring |
| Scalability | Limited | High-performance async |

## 📚 Examples

See the `examples/` directory for comprehensive demonstrations:
- `comprehensive_demo.py`: Full feature demonstration
- Basic usage examples
- Configuration file examples
- Custom processor examples
