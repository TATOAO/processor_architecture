from typing import AsyncGenerator
from .processor import AsyncProcessor
import asyncio
import hashlib
import random

class ChunkerProcessor(AsyncProcessor):
    meta = {
        "name": "ChunkerProcessor",
        "input_pipe_type": "AsyncPipe",
        "output_pipe_type": "AsyncPipe",
        "output_strategy": "ordered",
    }

    async def process(self, input_data:str, *args, **kwargs) -> AsyncGenerator[str, None]:
        chunk_size = 2
        for i in range(0, len(input_data), chunk_size):
            s = random.random()

            if input_data[0] == "X":
                s = 0.3
            if input_data[0] == "O":
                s = 0.7

            await asyncio.sleep(s)
            print("sleeping for", s, "seconds")
            yield input_data[i:i+chunk_size]

class HasherProcessor(AsyncProcessor):
    meta = {
        "name": "HasherProcessor",
        "input_pipe_type": "AsyncPipe",
        "output_pipe_type": "AsyncPipe",
        "output_strategy": "asap",
    }

    async def process(self, input_data:str, *args, **kwargs) -> [str, None]:
        hashed = hashlib.sha256(input_data.encode()).hexdigest()
        print("input_data", input_data, "hashed", hashed)
        return hashed


# python -m processor_pipeline.new_core.test_graph
if __name__ == "__main__":
    from .pipe import AsyncPipe
    input_pipe = AsyncPipe(pipe_id="input_pipe_test1")
    output_pipe = AsyncPipe(pipe_id="output_pipe_test1")
    hasher_pipe = AsyncPipe(pipe_id="hasher_pipe_test1")

    import time
    import logging
    logging.basicConfig(level=logging.DEBUG)

    async def main():
        start_time = time.time()
        await input_pipe.put("X" * 10)
        await input_pipe.put("O" * 10)
        await input_pipe.put(None)  # Signal end of input

        chunker_processor = ChunkerProcessor(input_pipe=input_pipe, output_pipe=output_pipe)
        hasher_processor = HasherProcessor(input_pipe=output_pipe, output_pipe=hasher_pipe)
        
        # Start both processors concurrently
        chunker_task = asyncio.create_task(chunker_processor.execute())
        
        # Stream results from the hasher processor
        async for data in hasher_processor.astream():
            print('final data', data)
        
        # Wait for chunker to complete
        await chunker_task

        end_time = time.time()
        print(f"Time taken: {end_time - start_time} seconds")

    import asyncio
    asyncio.run(main())
