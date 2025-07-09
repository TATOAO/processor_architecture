from typing import AsyncGenerator, Any
from processor_pipeline import AsyncPipeline, AsyncProcessor
import asyncio


class MyProcessor(AsyncProcessor):
    async def process(self, data: AsyncGenerator[str, None]) -> AsyncGenerator[str, None]:
        
        print(data)
        async for item in data:
            chunk_size = 10
            i = 0
            while True:
                await asyncio.sleep(0.5)
                if i > len(item):
                    break
                yield item[i:i+chunk_size]
                i += chunk_size
    
    async def after_process(self, input_data: Any, output_data: Any, execution_id: str, step_index: int, *args, **kwargs) -> None:
        print("close processor")



class SecondProcessor(AsyncProcessor):
    async def process(self, data: AsyncGenerator[str, None]) -> AsyncGenerator[str, None]:
        async for item in data:
            yield str(len(item))
    
    async def after_process(self, input_data: Any, output_data: Any, execution_id: str, step_index: int, *args, **kwargs) -> None:
        print("close second processor")



# python -m examples.single_processor_pipeline
if __name__ == "__main__":
    import asyncio
    async def main():
        pipeline = AsyncPipeline([MyProcessor(), SecondProcessor()])
        results = await pipeline.run(input_data="123456789091029301920391293019230192039")
        print(results)
    asyncio.run(main())









