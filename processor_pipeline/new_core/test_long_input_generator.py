#!/usr/bin/env python3
"""
Test script to demonstrate the issue with the current processor logic.

The problem: When the input pipe keeps sending data, the async for loop never ends,
so the processor never reaches the point where it sends data to the output pipe.
"""

import asyncio
import time
import random
import sys
from typing import Any, AsyncGenerator
from processor_pipeline.new_core.pipe import AsyncPipe
from processor_pipeline.new_core.processor import AsyncProcessor, OutputStrategy
from processor_pipeline.new_core.core_interfaces import ProcessorMeta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestProcessor(AsyncProcessor, metaclass=ProcessorMeta):
    """
    A simple test processor that adds 1 to each input number.
    """
    meta = {
        "name": "test_processor",
        "output_strategy": "ordered",
        "max_concurrent": 100
    }
    
    async def process(self, input_data: Any, *args, **kwargs) -> AsyncGenerator[Any, None]:
        """Process the input data by adding 1 to it."""

        chunk_size = 2

        totle_time = 0
        for i in range(0, len(input_data), chunk_size):
            # s = random.random()

            # if input_data[0] == "X":
            #     s = 0.3
            # if input_data[0] == "O":
            #     s = 0.7
            # raise Exception("test")

            s = random.random()
            totle_time += s
            await asyncio.sleep(s)
            self.logger.warning(f"{TestProcessor.__name__} processing time: {s} seconds")
            yield input_data[i:i+chunk_size]
        
        self.logger.warning(f"{TestProcessor.__name__} total processing {input_data} time: {totle_time} seconds")


async def stream_terminal_input(input_pipe: AsyncPipe):
    """
    Stream terminal input into the input pipe.
    Reads lines from stdin and puts them into the pipe.
    """
    try:
        while True:
            # Read a line from stdin
            line = await asyncio.get_event_loop().run_in_executor(None, sys.stdin.readline)
            
            if line.strip() == "stop":  # Exit on "stop" command
                break
                
            line = line.strip()
            if line:  # Only process non-empty lines
                try:
                    # Try to convert to number, otherwise use as string
                    data = int(line) if line.isdigit() else line
                    await input_pipe.put(data)
                    logger.info(f"Put data into input pipe: {data}")
                except ValueError:
                    # If conversion fails, use the raw string
                    await input_pipe.put(line)
                    logger.info(f"Put data into input pipe: {line}")
                    
    except KeyboardInterrupt:
        logger.info("Terminal input interrupted by user")
    finally:
        # Close the input pipe to signal end of input
        await input_pipe.close()
        logger.info("Terminal input stream closed")

async def fixed_input_stream(input_pipe: AsyncPipe):
    """
    Stream fixed input into the input pipe.
    """
    for i in range(5):
        input_data = f"{i}" * 10
        print(f"Putting data into input pipe: {input_data}")
        await input_pipe.put(input_data)
    await input_pipe.put(None)
    await input_pipe.close()


async def monitor_output(output_pipe: AsyncPipe):
    """
    Monitor the output pipe and print the data.
    """
    observer_id = await output_pipe.register_observer()
    async for message_id, data in output_pipe.peek_aiter(observer_id):
        print(f"monitor output: {data}")


async def main():
    input_pipe = AsyncPipe(pipe_id="input_pipe_test_long_input")
    output_pipe = AsyncPipe(pipe_id="output_pipe_test_long_input")

    processor = TestProcessor(input_pipe=input_pipe, output_pipe=output_pipe)

    # Create tasks for both terminal input streaming and processor output
    # terminal_task = asyncio.create_task(stream_terminal_input(input_pipe))
    start_time = time.time()
    terminal_task = await fixed_input_stream(input_pipe)
    monitor_task = asyncio.create_task(monitor_output(output_pipe))
    processor_task = asyncio.create_task(processor.execute())
    
    # Wait for both tasks to complete
    monitor, result = await asyncio.gather(monitor_task, processor_task)
    print(result)

    end_time = time.time()
    print(f"Time taken to put data into input pipe: {end_time - start_time} seconds")

async def main_test_astream():
    start_time = time.time()
    input_pipe = AsyncPipe(pipe_id="input_pipe_test_long_input")
    output_pipe = AsyncPipe(pipe_id="output_pipe_test_long_input")

    processor = TestProcessor(input_pipe=input_pipe, output_pipe=output_pipe)

    async def main_stream():
        async for item in processor.astream():
            print('xxxxxxx', item)
    
    terminal_task = asyncio.create_task(fixed_input_stream(input_pipe))
    main_stream_task = asyncio.create_task(main_stream())
    monitor_task = asyncio.create_task(monitor_output(output_pipe))

    await asyncio.gather(main_stream_task, terminal_task)
    end_time = time.time()
    print(f"Time taken to put data into input pipe: {end_time - start_time} seconds")
        


# python -m processor_pipeline.new_core.test_long_input_generator
if __name__ == "__main__":
    print("Enter numbers or text (press Ctrl+D or Ctrl+C to exit):")
    # asyncio.run(main())
    asyncio.run(main_test_astream())
