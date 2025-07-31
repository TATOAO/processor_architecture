import asyncio
import logging
from typing import AsyncGenerator
from processor_pipeline.new_core.processor import AsyncProcessor
from processor_pipeline.new_core.pipe import AsyncPipe


class TestProcessor(AsyncProcessor):
    """Test processor that outputs numbers 1-5 with delays"""
    meta = {
        "name": "TestProcessor",
        "input_pipe_type": "AsyncPipe",
        "output_pipe_type": "AsyncPipe",
        "output_strategy": "asap",
    }

    async def process(self, input_data: int, *args, **kwargs) -> AsyncGenerator[int, None]:
        """Process by simply outputting the number"""
        await asyncio.sleep(0.1)  # Small delay to simulate processing
        print(f"Processing: {input_data}")
        yield input_data


async def test_multiple_peek_consumers():
    """Test that multiple consumers can peek at the same data without interfering"""
    print("\n=== Testing Multiple Peek Consumers ===")
    
    # Create processor
    input_pipe = AsyncPipe(pipe_id="test_input")
    output_pipe = AsyncPipe(pipe_id="test_output")
    processor = TestProcessor(
        processor_id="test_processor", 
        input_pipe=input_pipe, 
        output_pipe=output_pipe
    )
    
    # Data to process
    test_data = [1, 2, 3, 4, 5]
    
    # Results storage
    peek_consumer_1_results = []
    peek_consumer_2_results = []
    main_consumer_results = []
    
    async def feed_data():
        """Feed data to the processor"""
        await asyncio.sleep(0.2)  # Let consumers start first
        for data in test_data:
            await input_pipe.put(data)
        await input_pipe.close()
    
    async def peek_consumer_1():
        """First peek consumer"""
        print("Peek Consumer 1 starting...")
        async for data in processor.peek_astream("peek_1"):
            print(f"Peek Consumer 1 got: {data}")
            peek_consumer_1_results.append(data)
        print("Peek Consumer 1 finished")
    
    async def peek_consumer_2():
        """Second peek consumer"""
        print("Peek Consumer 2 starting...")
        async for data in processor.peek_astream("peek_2"):
            print(f"Peek Consumer 2 got: {data}")
            peek_consumer_2_results.append(data)
        print("Peek Consumer 2 finished")
    
    async def main_consumer():
        """Main consumer using regular astream"""
        await asyncio.sleep(0.1)  # Start slightly after peek consumers
        print("Main Consumer starting...")
        async for data in processor.astream():
            print(f"Main Consumer got: {data}")
            main_consumer_results.append(data)
        print("Main Consumer finished")
    
    # Run all consumers and data feeder concurrently
    await asyncio.gather(
        feed_data(),
        peek_consumer_1(),
        peek_consumer_2(),
        main_consumer()
    )
    
    # Verify results
    print(f"\nPeek Consumer 1 results: {peek_consumer_1_results}")
    print(f"Peek Consumer 2 results: {peek_consumer_2_results}")
    print(f"Main Consumer results: {main_consumer_results}")
    
    # All consumers should see the same data
    assert peek_consumer_1_results == test_data, f"Peek Consumer 1 incomplete: {peek_consumer_1_results} != {test_data}"
    assert peek_consumer_2_results == test_data, f"Peek Consumer 2 incomplete: {peek_consumer_2_results} != {test_data}"
    assert main_consumer_results == test_data, f"Main Consumer incomplete: {main_consumer_results} != {test_data}"
    
    print("✅ All consumers received complete data without interference!")


async def test_observer_cleanup():
    """Test that observers are properly cleaned up"""
    print("\n=== Testing Observer Cleanup ===")
    
    pipe = AsyncPipe(pipe_id="cleanup_test")
    
    # Register some observers
    observer_1 = await pipe.register_observer("obs_1")
    observer_2 = await pipe.register_observer("obs_2")
    
    print(f"Registered observers: {list(pipe._observers.keys())}")
    assert len(pipe._observers) == 2
    
    # Unregister one
    await pipe.unregister_observer(observer_1)
    print(f"After unregistering obs_1: {list(pipe._observers.keys())}")
    assert len(pipe._observers) == 1
    assert "obs_2" in pipe._observers
    
    # Use peek_aiter with auto-cleanup
    async def peek_user():
        result = []
        async for data in pipe.peek_aiter():
            result.append(data)
            if len(result) >= 2:  # Only take first 2 items
                break
        return result
    
    # Feed some data
    async def feed_cleanup_data():
        await asyncio.sleep(0.1)
        await pipe.put("data1")
        await pipe.put("data2")
        await pipe.put("data3")
        await pipe.close()
    
    results = await asyncio.gather(
        peek_user(),
        feed_cleanup_data()
    )
    
    peek_results = results[0]
    print(f"Peek results: {peek_results}")
    
    # The auto-generated observer should be cleaned up
    print(f"Observers after auto-cleanup: {list(pipe._observers.keys())}")
    # Should only have obs_2 left
    assert len(pipe._observers) == 1
    assert "obs_2" in pipe._observers
    
    print("✅ Observer cleanup working correctly!")


async def main():
    """Run all tests"""
    logging.basicConfig(level=logging.INFO)
    
    await test_multiple_peek_consumers()
    await test_observer_cleanup()
    
    print("\n🎉 All tests passed!")


if __name__ == "__main__":
    asyncio.run(main())