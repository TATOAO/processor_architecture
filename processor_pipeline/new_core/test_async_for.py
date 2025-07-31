from typing import AsyncGenerator
from .processor import AsyncProcessor
import asyncio
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


# python -m processor_pipeline.new_core.test_async_for
if __name__ == "__main__":
    from .pipe import AsyncPipe
    input_pipe = AsyncPipe(pipe_id="input_pipe_test1")
    output_pipe = AsyncPipe(pipe_id="output_pipe_test1")

    import time
    import logging
    logging.basicConfig(level=logging.DEBUG)

    async def main():
        start_time = time.time()
        await input_pipe.put("X" * 10)
        await input_pipe.put("O" * 10)
        await input_pipe.put(None)

        chunker_processor = ChunkerProcessor(input_pipe=input_pipe, output_pipe=output_pipe)
        # async for data in chunker_processor.astream():
        #     print(data)

        result = await chunker_processor.execute()
        print(result)



        end_time = time.time()
        print(f"Time taken: {end_time - start_time} seconds")

    import asyncio
    asyncio.run(main())
