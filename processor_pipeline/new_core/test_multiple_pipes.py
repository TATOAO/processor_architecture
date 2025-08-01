"""
Test for multiple input/output pipe registration functionality
"""
import asyncio
import pytest
from typing import Any, AsyncGenerator
from .processor import AsyncProcessor
from .pipe import AsyncPipe, BufferPipe


class TestProcessor(AsyncProcessor):
    """Simple test processor that doubles input values"""
    
    meta = {
        "name": "TestProcessor",
        "output_strategy": "ordered",
        "max_concurrent": 5
    }
    
    async def process(self, input_data: Any, *args, **kwargs) -> AsyncGenerator[Any, None]:
        yield input_data * 2


async def test_multiple_input_pipes():
    """Test that multiple input pipes are merged correctly"""
    
    # Create the processor
    main_input_pipe = AsyncPipe(pipe_id="main_input")
    main_output_pipe = AsyncPipe(pipe_id="main_output") 
    processor = TestProcessor(
        processor_id="test_processor",
        input_pipe=main_input_pipe,
        output_pipe=main_output_pipe
    )
    
    # Create additional input pipes
    input_pipe1 = AsyncPipe(pipe_id="input1")
    input_pipe2 = AsyncPipe(pipe_id="input2")
    
    # Register additional input pipes
    processor.register_input_pipe(input_pipe1)
    processor.register_input_pipe(input_pipe2)
    
    # Put data into different input pipes
    await main_input_pipe.put(10)  # Should become 20
    await input_pipe1.put(5)       # Should become 10
    await input_pipe2.put(3)       # Should become 6
    
    # Close all input pipes
    await main_input_pipe.close()
    await input_pipe1.close()
    await input_pipe2.close()
    
    # Execute processor
    results = await processor.execute()
    
    # Collect all results
    result_values = []
    async for message_id, data in main_output_pipe:
        result_values.append(data)
    
    # Should have processed all inputs
    assert len(result_values) == 3
    assert set(result_values) == {20, 10, 6}  # Order may vary due to async processing
    
    print("✓ Multiple input pipes test passed")


async def test_multiple_output_pipes():
    """Test that output is broadcasted to multiple output pipes"""
    
    # Create the processor  
    main_input_pipe = AsyncPipe(pipe_id="main_input")
    main_output_pipe = AsyncPipe(pipe_id="main_output")
    processor = TestProcessor(
        processor_id="test_processor",
        input_pipe=main_input_pipe,
        output_pipe=main_output_pipe
    )
    
    # Create additional output pipes
    output_pipe1 = AsyncPipe(pipe_id="output1")
    output_pipe2 = AsyncPipe(pipe_id="output2")
    
    # Register additional output pipes
    processor.register_output_pipe(output_pipe1)
    processor.register_output_pipe(output_pipe2)
    
    # Put data into main input pipe
    await main_input_pipe.put(7)  # Should become 14
    await main_input_pipe.close()
    
    # Execute processor
    execution_task = asyncio.create_task(processor.execute())
    
    # Collect results from all output pipes
    main_results = []
    output1_results = []
    output2_results = []
    
    async def collect_from_pipe(pipe, results_list, pipe_name):
        async for message_id, data in pipe:
            results_list.append(data)
            print(f"Received {data} from {pipe_name}")
    
    # Start collection tasks
    collect_tasks = [
        asyncio.create_task(collect_from_pipe(main_output_pipe, main_results, "main")),
        asyncio.create_task(collect_from_pipe(output_pipe1, output1_results, "output1")), 
        asyncio.create_task(collect_from_pipe(output_pipe2, output2_results, "output2"))
    ]
    
    # Wait for execution to complete
    await execution_task
    
    # Wait a bit for broadcasting to complete
    await asyncio.sleep(0.1)
    
    # Cancel collection tasks
    for task in collect_tasks:
        task.cancel()
    await asyncio.gather(*collect_tasks, return_exceptions=True)
    
    # Verify results
    assert main_results == [14], f"Main results: {main_results}"
    assert output1_results == [14], f"Output1 results: {output1_results}"
    assert output2_results == [14], f"Output2 results: {output2_results}"
    
    print("✓ Multiple output pipes test passed")


async def test_dynamic_registration():
    """Test that pipes can be registered dynamically after processor starts"""
    
    # Create the processor
    main_input_pipe = AsyncPipe(pipe_id="main_input")
    main_output_pipe = AsyncPipe(pipe_id="main_output")
    processor = TestProcessor(
        processor_id="test_processor", 
        input_pipe=main_input_pipe,
        output_pipe=main_output_pipe
    )
    
    # Start processor execution in background
    async def run_processor():
        # Put some initial data
        await main_input_pipe.put(1)
        
        # Wait and register a new input pipe dynamically
        await asyncio.sleep(0.1)
        dynamic_input = AsyncPipe(pipe_id="dynamic_input")
        processor.register_input_pipe(dynamic_input)
        await dynamic_input.put(2)
        await dynamic_input.close()
        
        # Wait and close main input
        await asyncio.sleep(0.1)
        await main_input_pipe.close()
    
    # Run both tasks concurrently
    execution_task = asyncio.create_task(processor.execute())
    input_task = asyncio.create_task(run_processor())
    
    # Wait for both to complete
    await asyncio.gather(execution_task, input_task)
    
    # Collect results
    results = []
    async for message_id, data in main_output_pipe:
        results.append(data)
    
    # Should have processed both inputs
    assert len(results) == 2
    assert set(results) == {2, 4}  # 1*2=2, 2*2=4
    
    print("✓ Dynamic registration test passed")


async def test_statistics():
    """Test that statistics include pipe registration info"""
    
    processor = TestProcessor(
        processor_id="test_processor",
        input_pipe=AsyncPipe(),
        output_pipe=AsyncPipe()
    )
    
    # Initially no registered pipes
    stats = processor.statistics()
    assert stats['registered_input_pipes'] == 0
    assert stats['registered_output_pipes'] == 0
    assert stats['merger_broadcaster_running'] == False
    
    # Register some pipes
    processor.register_input_pipe(AsyncPipe())
    processor.register_output_pipe(AsyncPipe())
    processor.register_output_pipe(AsyncPipe())
    
    # Check updated stats
    stats = processor.statistics()
    assert stats['registered_input_pipes'] == 1
    assert stats['registered_output_pipes'] == 2
    
    print("✓ Statistics test passed")


async def main():
    """Run all tests"""
    print("Testing multiple pipe registration functionality...\n")
    
    await test_multiple_input_pipes()
    await test_multiple_output_pipes() 
    await test_dynamic_registration()
    await test_statistics()
    
    print("\n✅ All tests passed!")


if __name__ == "__main__":
    asyncio.run(main())