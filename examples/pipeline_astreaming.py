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
            for ch in item:
                await asyncio.sleep(0.3)
                yield ch

# python -m examples.pipeline_astreaming
if __name__ == "__main__":
    async def main():

        from processor_pipeline import AsyncPipeline
        pipeline = AsyncPipeline([
            FakeProcessor(),
        ])

        async for item in pipeline.astream("asdjfioajwef"):
            print(item)
            print('--')

    import asyncio
    result = asyncio.run(main())