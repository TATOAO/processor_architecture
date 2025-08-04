"""
Test and example for the new centralized pipe hub system.

This demonstrates how processors register with the hub and how data flows
through the centralized pipe management system.
"""

import asyncio
from typing import Any, AsyncGenerator
from .core_interfaces import ProcessorInterface, ProcessorMeta
from .pipe_hub import PipeHub
from .pipe import BufferPipe


class SimpleProcessor(ProcessorInterface, metaclass=ProcessorMeta):
    """A simple processor for testing the pipe hub"""
    
    meta = {
        "name": "simple_processor",
        "output_strategy": "asap",
        "max_concurrent": 5
    }
    
    def __init__(self, processor_id: str, pipe_hub: PipeHub, multiplier: int = 2):
        self.processor_id = processor_id
        self.pipe_hub = pipe_hub
        self.multiplier = multiplier
        self._running = False
    
    async def process(self, input_data: Any, *args, **kwargs) -> AsyncGenerator[Any, None]:
        """Process input data by multiplying by the multiplier"""
        result = input_data * self.multiplier
        yield result
    
    async def execute(self) -> None:
        """Execute the processor - read from input pipe, process, write to output pipe"""
        self._running = True
        
        # Get pipes from hub
        input_pipe = await self.pipe_hub.get_merged_input_pipe(self.processor_id)
        output_pipe = await self.pipe_hub.get_output_pipe(self.processor_id)
        
        if not input_pipe or not output_pipe:
            raise ValueError(f"Processor {self.processor_id} not properly registered with hub")
        
        try:
            async for message_id, data in input_pipe:
                if not self._running:
                    break
                
                # Process the data
                async for result in self.process(data):
                    await output_pipe.put(result)
                    
        except Exception as e:
            print(f"Error in processor {self.processor_id}: {e}")
        finally:
            output_pipe.close()
    
    async def astream(self, input_data: Any) -> AsyncGenerator[Any, None]:
        """Not implemented in this simple example"""
        raise NotImplementedError()
    
    async def peek_astream(self, observer_id: str = None) -> AsyncGenerator[Any, None]:
        """Not implemented in this simple example"""
        raise NotImplementedError()
    
    async def initialize(self) -> None:
        """Initialize the processor"""
        pass
    
    async def cleanup(self) -> None:
        """Cleanup the processor"""
        self._running = False
    
    def statistics(self) -> dict:
        """Get processor statistics"""
        return {
            "processor_id": self.processor_id,
            "multiplier": self.multiplier,
            "running": self._running
        }


async def test_pipe_hub_basic():
    """Test basic pipe hub functionality"""
    print("Testing basic pipe hub functionality...")
    
    # Create hub
    hub = PipeHub("test_hub")
    
    # Create pipes
    input_pipe_1 = BufferPipe("input_1")
    input_pipe_2 = BufferPipe("input_2")
    output_pipe = BufferPipe("output")
    
    # Register pipes with hub
    await hub.register_pipe("input_1", input_pipe_1)
    await hub.register_pipe("input_2", input_pipe_2)
    await hub.register_pipe("output", output_pipe)
    
    # Create and register processor
    processor = SimpleProcessor("proc_1", hub, multiplier=3)
    await hub.register_processor("proc_1", ["input_1", "input_2"], "output")
    
    # Start hub
    await hub.start_hub()
    
    # Start processor
    processor_task = asyncio.create_task(processor.execute())
    
    # Send some test data
    await input_pipe_1.put(10)
    await input_pipe_2.put(20)
    await input_pipe_1.put(5)
    
    # Close input pipes to signal end
    input_pipe_1.close()
    input_pipe_2.close()
    
    # Wait a bit for processing
    await asyncio.sleep(0.1)
    
    # Stop processor
    await processor.cleanup()
    processor_task.cancel()
    
    # Collect results
    results = []
    while not output_pipe.is_empty():
        _, data = await output_pipe.get()
        results.append(data)
    
    print(f"Input data: [10, 20, 5]")
    print(f"Results (multiplied by 3): {results}")
    print(f"Hub statistics: {hub.hub_statistics()}")
    
    # Stop hub
    await hub.stop_hub()
    
    return results


async def test_pipe_hub_single_input():
    """Test pipe hub with single input pipe"""
    print("\nTesting pipe hub with single input...")
    
    # Create hub
    hub = PipeHub("test_hub_single")
    
    # Create pipes
    input_pipe = BufferPipe("single_input")
    output_pipe = BufferPipe("single_output")
    
    # Register pipes with hub
    await hub.register_pipe("single_input", input_pipe)
    await hub.register_pipe("single_output", output_pipe)
    
    # Create and register processor
    processor = SimpleProcessor("proc_single", hub, multiplier=5)
    await hub.register_processor("proc_single", ["single_input"], "single_output")
    
    # Start hub
    await hub.start_hub()
    
    # Start processor
    processor_task = asyncio.create_task(processor.execute())
    
    # Send test data
    test_data = [1, 2, 3, 4]
    for data in test_data:
        await input_pipe.put(data)
    
    # Close input pipe
    input_pipe.close()
    
    # Wait for processing
    await asyncio.sleep(0.1)
    
    # Stop processor
    await processor.cleanup()
    processor_task.cancel()
    
    # Collect results
    results = []
    while not output_pipe.is_empty():
        _, data = await output_pipe.get()
        results.append(data)
    
    print(f"Input data: {test_data}")
    print(f"Results (multiplied by 5): {results}")
    
    # Stop hub
    await hub.stop_hub()
    
    return results


async def main():
    """Run all tests"""
    print("=" * 50)
    print("Testing New Centralized Pipe Hub System")
    print("=" * 50)
    
    # Test basic functionality with multiple inputs
    results1 = await test_pipe_hub_basic()
    
    # Test single input functionality
    results2 = await test_pipe_hub_single_input()
    
    print("\n" + "=" * 50)
    print("All tests completed successfully!")
    print(f"Multi-input test results: {results1}")
    print(f"Single-input test results: {results2}")


if __name__ == "__main__":
    asyncio.run(main())