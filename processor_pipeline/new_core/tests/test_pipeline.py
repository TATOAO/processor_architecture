

from typing import Any, AsyncGenerator
from processor_pipeline.new_core import AsyncProcessor
import asyncio

class FakeProcessor(AsyncProcessor):
    meta = {
        "name": "FakeProcessor",
        "input_type": Any,
        "output_type": Any,
    }

    async def process(self, data: AsyncGenerator[Any, None], *args, **kwargs) -> AsyncGenerator[Any, None]:
        await asyncio.sleep(0.3)
        yield data

class FakeProcessor2(AsyncProcessor):
    meta = {
        "name": "FakeProcessor2",
        "input_type": Any,
        "output_type": Any,
    }
    
    async def process(self, data: AsyncGenerator[Any, None], *args, **kwargs) -> AsyncGenerator[Any, None]:
        for ch in data:
            await asyncio.sleep(0.3)
            yield str(ch)
            

class FakeProcessor3(AsyncProcessor):
    meta = {
        "name": "FakeProcessor3",
        "input_type": Any,
        "output_type": Any,
    }
   
    async def process(self, data: AsyncGenerator[Any, None], *args, **kwargs) -> AsyncGenerator[Any, None]:
        await asyncio.sleep(0.1)
        yield data



# python -m processor_pipeline.new_core.tests.test_pipeline
if __name__ == "__main__":
    async def main():

        from processor_pipeline.new_core.pipeline import AsyncPipeline
        pipeline = AsyncPipeline([
            FakeProcessor(),
            FakeProcessor2(),
            FakeProcessor3(),
        ])
        
        # Initialize the pipeline
        # await pipeline.initialize()

        # all_results = []
        # async for item in pipeline.astream(["asdjfioajwef", "28903urf203f902390f"]):
            # all_results.append(item)
            # print('--', item)
        all_results = await pipeline.execute(["asdjfioajwef", "28903urf203f902390f"])
        print(all_results)


    import asyncio
    result = asyncio.run(main())
