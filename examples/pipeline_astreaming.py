from typing import Any, AsyncGenerator
from processor_pipeline import AsyncProcessor
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
            for ch in item:
                await asyncio.sleep(0.3)
                yield len(str(ch))
            

class FakeProcessor3(AsyncProcessor):
    meta = {
        "name": "FakeProcessor3",
        "input_type": Any,
        "output_type": Any,
    }
    
    async def process(self, data: AsyncGenerator[Any, None]) -> AsyncGenerator[Any, None]:
        async for item in data:
            await asyncio.sleep(0.1)
            yield len(str(item))



# python -m examples.pipeline_astreaming
if __name__ == "__main__":
    async def main():

        from processor_pipeline import AsyncPipeline
        pipeline = AsyncPipeline([
            FakeProcessor(),
            FakeProcessor2(),
            FakeProcessor3(),
        ])

        async for item in pipeline.astream(["asdjfioajwef", "28903urf203f902390f"]):
            print(item)
            print('--')


    import asyncio
    result = asyncio.run(main())