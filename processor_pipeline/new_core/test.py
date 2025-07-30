import asyncio
from pydantic import computed_field
from typing import Any, List, Tuple
from .processor import AsyncProcessor
from .pipe import AsyncPipe, BufferPipe
import random


class TestProcessor(AsyncProcessor):
    """
    Test processor.
    """

    async def process(self, data: Any, *args, **kwargs) -> Any:
        # await asyncio.sleep(random.randint(1, 10))

        print(f"Processing data: {data}")
        s = random.random() * data * 2
        self.logger.warning(f"{TestProcessor.__name__} processing time: {s} seconds")
        # await asyncio.sleep(1)
        await asyncio.sleep(s)

        if data == 2:
            await asyncio.sleep(0.5)
            raise Exception("Test exception")


        try:
            pass
            # print(await self.input_pipe.peek(5))
        except Exception as e:
            print(e)

        return data * 2


# python -m processor_pipeline.new_core.test
if __name__ == "__main__":
    input_pipe = AsyncPipe(pipe_id="input_pipe_test1")
    output_pipe = AsyncPipe(pipe_id="output_pipe_test1")

    import time
    async def main():
        processor = TestProcessor("processor_id_test1", input_pipe, output_pipe, output_strategy="ordered", max_concurrent=10)
        # max_concurent is not going to work since all task are io bounded
        await input_pipe.put(1)
        await input_pipe.put(2)
        await input_pipe.put(3)
        await input_pipe.put(4)
        await input_pipe.put(5)
        await input_pipe.put(None)

        # result = await processor.execute()
        # print(result)

        start_time = time.time()
        async for data in processor.astream():
            print(data)


        end_time = time.time()
        print(f"Time taken: {end_time - start_time} seconds")

    asyncio.run(main())