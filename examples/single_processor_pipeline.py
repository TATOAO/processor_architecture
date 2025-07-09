from typing import AsyncGenerator, Any
from processor_pipeline import AsyncPipeline, AsyncProcessor
import asyncio


# log with timestamp
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



class MyProcessor(AsyncProcessor):
    async def process(self, data: AsyncGenerator[str, None]) -> AsyncGenerator[str, None]:
        logger.info(f"MyProcessor: {data}")
        async for item in data:
            chunk_size = 10
            i = 0
            while True:
                await asyncio.sleep(0.5)
                if i > len(item):
                    break

                logger.info(f"MyProcessor: {item[i:i+chunk_size]}")
                yield item[i:i+chunk_size]
                i += chunk_size
    
    async def after_process(self, input_data: Any, output_data: Any, execution_id: str, step_index: int, *args, **kwargs) -> None:
        logger.info(f"MyProcessor: {input_data} - {output_data} - {execution_id} - {step_index}")



class SecondProcessor(AsyncProcessor):
    async def process(self, data: AsyncGenerator[str, None]) -> AsyncGenerator[str, None]:
        async for item in data:
            logger.info(f"SecondProcessor: {item}")
            await asyncio.sleep(0.1)
            logger.info(f"SecondProcessor: {len(item)}")
            yield str(len(item))
    
    async def after_process(self, input_data: Any, output_data: Any, execution_id: str, step_index: int, *args, **kwargs) -> None:
        logger.info(f"SecondProcessor: {input_data} - {output_data} - {execution_id} - {step_index}")

class ThirdProcessor(AsyncProcessor):
    async def process(self, data: AsyncGenerator[str, None]) -> AsyncGenerator[str, None]:
        async for item in data:
            logger.info(f"ThirdProcessor: {item}")
            await asyncio.sleep(0.1)
            logger.info(f"ThirdProcessor: {len(item)}")
            yield str(len(item))
    
    async def after_process(self, input_data: Any, output_data: Any, execution_id: str, step_index: int, *args, **kwargs) -> None:
        logger.info(f"ThirdProcessor: {input_data} - {output_data} - {execution_id} - {step_index}")


# python -m examples.single_processor_pipeline
if __name__ == "__main__":
    import asyncio
    import time
    async def main():

        start_time = time.time()

        pipeline = AsyncPipeline([MyProcessor(), SecondProcessor(), ThirdProcessor()])
        results = await pipeline.run(input_data="123456789091029301920391293019230192039")

        end_time = time.time()
        print(results)
        print(f"Time taken: {end_time - start_time} seconds")

    asyncio.run(main())









