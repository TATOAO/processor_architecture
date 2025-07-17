from processor_pipeline import AsyncProcessor, AsyncPipeline
from typing import Any, AsyncGenerator
import asyncio

class FakeProcessor(AsyncProcessor):
    meta = {
        "name": "FakeProcessor",
        "input_type": Any,
        "output_type": Any,
    }

    async def process(self, data: AsyncGenerator[Any, None]) -> AsyncGenerator[Any, None]:
        async for item in data:
            await asyncio.sleep(0.3)
            yield item

class FakeProcessor2(AsyncProcessor):
    meta = {
        "name": "FakeProcessor2",
        "input_type": Any,
        "output_type": Any,
    }
    
    async def process(self, data: AsyncGenerator[Any, None]) -> AsyncGenerator[Any, None]:
        async for item in data:
            
            print("FakeProcessor2: ", item)
            await asyncio.sleep(0.3)
            yield len(str(item))
            

class FakeProcessor3(AsyncProcessor):
    meta = {
        "name": "FakeProcessor3",
        "input_type": Any,
        "output_type": Any,
    }
    
    async def process(self, data: AsyncGenerator[Any, None]) -> AsyncGenerator[Any, None]:
        async for item in data:
            print("FakeProcessor3: ", item)
            await asyncio.sleep(0.1)
            yield len(str(item))


pipeline1 = AsyncPipeline([
    FakeProcessor(),
    FakeProcessor2(),
])


pipeline2 = AsyncPipeline([
    FakeProcessor3(),
])


pipeline3 = AsyncPipeline([
    pipeline1,
    pipeline2,
])



# python -m examples.nested_pipeline_example
if __name__ == "__main__":
    async def main():
        result = await pipeline3.run(["asdjfioajwef", "28903urf203f902390f"])
        print(result)

    asyncio.run(main())