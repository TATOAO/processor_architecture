from typing import AsyncGenerator, Any
from processor_pipeline import AsyncPipeline, AsyncProcessor


class MyProcessor(AsyncProcessor):
    async def process(self, data: AsyncGenerator[str, None]) -> AsyncGenerator[str, None]:
        
        print(data)
        async for item in data:
            chunk_size = 10
            i = 0
            while True:
                if i > len(item):
                    break
                yield item[i:i+chunk_size]
                i += chunk_size
        

# python -m examples.single_processor_pipeline
if __name__ == "__main__":
    import asyncio
    async def main():
        pipeline = AsyncPipeline([MyProcessor()])
        results = await pipeline.run(input_data="123456789091029301920391293019230192039")
        print(results)
    asyncio.run(main())









