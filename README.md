# Processor Pipeline

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A flexible and efficient Python framework for creating and executing data processing pipelines with async support, automatic type checking, and concurrent processing capabilities.

## Features

- **🚀 Async-First Design**: Built for high-performance async processing with concurrent execution
- **🔧 Type Safety**: Automatic type compatibility checking between pipeline stages
- **📊 Monitoring & Logging**: Built-in callback system for monitoring, logging, and debugging
- **⚡ Concurrent Processing**: Semaphore-controlled concurrency for optimal resource usage
- **🔄 Streaming Support**: Process data as streams for memory-efficient handling of large datasets
- **🎯 Simple API**: Easy-to-use interface for both sync and async processors
- **📝 Extensible**: Create custom processors by extending base classes

## Installation

```bash
pip install processor-pipeline
```

For development:

```bash
git clone https://github.com/yourusername/processor-pipeline.git
cd processor-pipeline
pip install -e .[dev]
```

## Quick Start

### Basic Async Pipeline

```python
from processor_pipeline import AsyncPipeline, AsyncProcessor
import asyncio
from typing import AsyncGenerator, Any

class TextProcessor(AsyncProcessor):
    meta = {
        "name": "text_processor",
        "input_type": str,
        "output_type": str,
    }
  
    async def process(self, data: AsyncGenerator[Any, None]) -> AsyncGenerator[str, None]:
        async for text in data:
            # Process the text (uppercase example)
            yield text.upper()

class LengthProcessor(AsyncProcessor):
    meta = {
        "name": "length_processor", 
        "input_type": str,
        "output_type": int,
    }
  
    async def process(self, data: AsyncGenerator[Any, None]) -> AsyncGenerator[int, None]:
        async for text in data:
            # Return text length
            yield len(text)

async def main():
    # Create pipeline
    pipeline = AsyncPipeline([
        TextProcessor(),
        LengthProcessor()
    ])
  
    # Run pipeline
    results = await pipeline.run("hello world")
    print(results)  # [11] (length of "HELLO WORLD")

asyncio.run(main())
```

### Document Processing Example

Here's a more complex example that processes documents by chunking text and generating vectors:

```python
from processor_pipeline import AsyncPipeline, AsyncProcessor
import asyncio
import random
from typing import AsyncGenerator, Any, Dict

class TextChunkerProcessor(AsyncProcessor):
    meta = {
        "name": "text_chunker_processor",
        "input_type": str,
        "output_type": list,
    }
  
    def __init__(self, window_size: int = 500, overlap: int = 100):
        self.window_size = window_size
        self.overlap = overlap
  
    async def process(self, data: AsyncGenerator[Any, None]) -> AsyncGenerator[list, None]:
        async for text in data:
            chunks = self._chunk_text(text, self.window_size, self.overlap)
            yield chunks
  
    def _chunk_text(self, text: str, window_size: int, overlap: int) -> list:
        chunks = []
        start = 0
        text_length = len(text)
      
        while start < text_length:
            end = min(start + window_size, text_length)
            chunk = text[start:end]
            chunks.append(chunk)
            start += window_size - overlap
            if end == text_length:
                break
      
        return chunks

class VectorGeneratorProcessor(AsyncProcessor):
    meta = {
        "name": "vector_generator_processor",
        "input_type": list,
        "output_type": dict,
    }
  
    async def process(self, data: AsyncGenerator[Any, None]) -> AsyncGenerator[dict, None]:
        async for chunks in data:
            async for vector_result in self._generate_vectors_for_chunks(chunks):
                yield vector_result
  
    async def _generate_vectors_for_chunks(self, chunks: list) -> AsyncGenerator[dict, None]:
        # Create coroutines for all chunks
        chunk_tasks = [
            self._process_single_chunk(i, chunk, len(chunks))
            for i, chunk in enumerate(chunks)
        ]
      
        # Process chunks as they complete using asyncio.as_completed
        for coro in asyncio.as_completed(chunk_tasks):
            result = await coro
            yield result
  
    async def _process_single_chunk(self, chunk_index: int, chunk: str, total_chunks: int) -> dict:
        # Simulate async processing (e.g., API call)
        await asyncio.sleep(random.random() * 0.1)  # Random delay
      
        # Generate vector (5 dimensions for example)
        vector = [random.random() for _ in range(5)]
      
        return {
            "chunk_index": chunk_index,
            "chunk": chunk,
            "vector": vector,
            "processing_time": 0.1
        }

# Custom callback for logging
def log_callback(processor, input_data, output_data, execution_id, step_index):
    print(f"Step {step_index}: {processor.meta['name']} completed")
    if hasattr(output_data, '__len__'):
        print(f"  Output size: {len(output_data)}")

async def main():
    # Sample text
    text = "Your long document text here..." * 100
  
    # Create pipeline
    pipeline = AsyncPipeline([
        TextChunkerProcessor(window_size=200, overlap=50),
        VectorGeneratorProcessor()
    ])
  
    # Run with callback
    results = await pipeline.run(text, callback=log_callback)
  
    print(f"Generated {len(results)} chunk-vector pairs")

asyncio.run(main())
```

## API Reference

### AsyncPipeline

The main pipeline class for async processing.

```python
AsyncPipeline(processors: List[AsyncProcessor], max_concurrent_tasks: int = 10)
```

**Parameters:**

- `processors`: List of async processors to chain together
- `max_concurrent_tasks`: Maximum number of concurrent tasks (default: 10)

**Methods:**

#### `async run(input_data, execution_id=None, callback=None, *args, **kwargs)`

Execute the pipeline with the given input data.

**Parameters:**

- `input_data`: Input data for the pipeline
- `execution_id`: Optional execution ID for tracking (auto-generated if not provided)
- `callback`: Optional callback function called after each processor step
- `*args, **kwargs`: Additional arguments passed to callback

**Returns:** List of final outputs from the pipeline

### AsyncProcessor

Base class for creating async processors.

```python
class MyProcessor(AsyncProcessor):
    meta = {
        "name": "my_processor",
        "input_type": str,   # Expected input type
        "output_type": int,  # Output type
    }
  
    async def process(self, data: AsyncGenerator[Any, None]) -> AsyncGenerator[Any, None]:
        async for item in data:
            # Process item
            yield processed_item
```

**Required Methods:**

- `process(data)`: Async generator that processes input data stream

**Optional Methods:**

- `get_save_data(input_data, output_data, execution_id, step_index)`: Customize logged data

### Callbacks

Callbacks are called after each processor step and receive:

```python
def callback(processor, input_data, output_data, execution_id, step_index):
    # processor: The processor instance that just ran
    # input_data: Input data for this processor
    # output_data: Output data from this processor  
    # execution_id: Unique execution identifier
    # step_index: Step number in pipeline (0-based)
    pass
```

## Advanced Features

### Type Checking

The pipeline automatically validates that processor input/output types are compatible:

```python
class StringProcessor(AsyncProcessor):
    meta = {
        "name": "string_proc",
        "input_type": str,
        "output_type": str,
    }

class NumberProcessor(AsyncProcessor):
    meta = {
        "name": "number_proc", 
        "input_type": int,  # ❌ Incompatible with StringProcessor output
        "output_type": int,
    }

# This will raise TypeError during pipeline creation
pipeline = AsyncPipeline([StringProcessor(), NumberProcessor()])
```

### Concurrent Processing Control

Control concurrency with the `max_concurrent_tasks` parameter:

```python
# Low concurrency for rate-limited APIs
pipeline = AsyncPipeline(processors, max_concurrent_tasks=5)

# High concurrency for CPU-bound tasks
pipeline = AsyncPipeline(processors, max_concurrent_tasks=50)
```

### Custom Data Logging

Override `get_save_data()` to customize what gets logged:

```python
class CustomProcessor(AsyncProcessor):
    def get_save_data(self, input_data, output_data, execution_id, step_index):
        base_data = super().get_save_data(input_data, output_data, execution_id, step_index)
        base_data.update({
            "custom_metric": len(output_data),
            "processing_details": {"status": "success"}
        })
        return base_data
```

## Examples

The `examples/` directory contains complete working examples:

- **`doc_chunker_embedding.py`**: Document processing pipeline with text chunking and vector generation

Run examples:

```bash
python examples/doc_chunker_embedding.py
```

## Development

### Setup Development Environment

```bash
git clone https://github.com/yourusername/processor-pipeline.git
cd processor-pipeline

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install in development mode
pip install -e .[dev]
```

### Running Tests

```bash
pytest
```

### Code Formatting

```bash
black processor_pipeline/
isort processor_pipeline/
```

### Type Checking

```bash
mypy processor_pipeline/
```

## Requirements

- Python 3.8+
- No external runtime dependencies (pure Python)

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Changelog

### 0.1.0 (Current)

- Initial release
- Async pipeline support
- Type checking
- Callback system
- Concurrent processing
- Streaming data support

## Support

- 📧 Email: tatoao@126.com
- 🐛 Issues: [GitHub Issues](https://github.com/yourusername/processor-pipeline/issues)
- 📖 Documentation: [Read the Docs](https://processor-pipeline.readthedocs.io)
