import asyncio
from pydantic import computed_field
from typing import Any, List
from .processor import AsyncProcessor
from .pipe import AsyncPipe
import random


class TestProcessor(AsyncProcessor):
    """
    Test processor.
    """
    def __init__(self, processor_id: str, input_pipe: AsyncPipe, output_pipe: AsyncPipe):
        super().__init__(processor_id, input_pipe, output_pipe)

    async def process(self, data: Any) -> Any:
        # await asyncio.sleep(random.randint(1, 10))

        self.logger.warning(f"{TestProcessor.__name__} processing data: {data}")
        s = random.randint(1, 10)
        self.logger.warning(f"{TestProcessor.__name__} processing time: {s} seconds")
        # await asyncio.sleep(1)
        await asyncio.sleep(s)

        return data * 2


# python -m processor_pipeline.new_core.test
if __name__ == "__main__":
    input_pipe = AsyncPipe(pipe_id="input_pipe_test1")
    output_pipe = AsyncPipe(pipe_id="output_pipe_test1")

    import time
    async def main():
        processor = TestProcessor("processor_id_test1", input_pipe, output_pipe)
        start_time = time.time()
        await input_pipe.put(1)
        await input_pipe.put(2)
        await input_pipe.put(3)
        await input_pipe.put(4)
        await input_pipe.put(5)
        await input_pipe.put(None)

        # result = await processor.execute()
        # print(result)

        async for data in processor.astream():
            print(data)


        end_time = time.time()
        print(f"Time taken: {end_time - start_time} seconds")

    asyncio.run(main())