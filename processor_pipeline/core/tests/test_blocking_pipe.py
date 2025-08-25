
import asyncio
import hashlib
import random
from loguru import logger
from typing import AsyncGenerator
from processor_pipeline.new_core.processor import AsyncProcessor
from processor_pipeline.new_core.graph import GraphBase
from processor_pipeline.new_core.graph_model import Node, Edge

class ChunkerProcessor(AsyncProcessor):
    meta = {
        "name": "ChunkerProcessor",
        "output_strategy": "ordered",
    }

    async def process(self, input_data:str, *args, **kwargs) -> AsyncGenerator[str, None]:
        chunk_size = 2
        for i in range(0, len(input_data), chunk_size):

            if input_data[0] == "X":
                s = i
            if input_data[0] == "O":
                s = 0.1 + i

            await asyncio.sleep(s)
            logger.info(f"sleeping for {s} seconds")
            yield input_data[i:i+chunk_size]

class WaitCompleteProcessor(AsyncProcessor):
    meta = {
        "name": "WaitCompleteProcessor",
        "output_strategy": "ordered",
        "input_pipe_type": "BlockingPipe",
    }

    async def process(self, input_data:str, *args, **kwargs) -> str:

        logger.info(f"WaitCompleteProcessor input_data: {input_data}")

        await asyncio.sleep(5)
        logger.info(f"WaitCompleteProcessor done")
        yield "done"



# python -m processor_pipeline.new_core.tests.test_blocking_pipe
if __name__ == "__main__":
    import time

    async def main():
        start_time = time.time()

        graph = GraphBase(
            nodes=[
                Node(
                    processor_class_name="ChunkerProcessor", 
                    processor_unique_name="chunker_processor_1"),
                Node(
                    processor_class_name="WaitCompleteProcessor", 
                    processor_unique_name="wait_complete_processor_1"),
            ], edges=[
                Edge(source_node_unique_name="chunker_processor_1", 
                    target_node_unique_name="wait_complete_processor_1", 
                    edge_unique_name="edge_1"),
            ], processor_id = "graph_1"
        )

        graph.initialize()


        async def monitor_pipe_hasher_input_task():
            async for data in graph.processor_pipes["wait_complete_processor_1"].input_pipe.peek_aiter():
                logger.info(f"[hasher processor input] monitor data: {data}")


        task = asyncio.create_task(monitor_pipe_hasher_input_task())


        async def monitor_pipe_hasher_output_task():
            async for data in graph.processor_pipes["wait_complete_processor_1"].output_pipe.peek_aiter():
                logger.info(f"[hasher processor output] monitor data: {data}")

        task2 = asyncio.create_task(monitor_pipe_hasher_output_task())


        def init_generator_1():
            return [
                "X" * 10,
                "O" * 10,
            ]

        async def init_generator_2():
            yield "XXXXXXXXXX"
            yield "OOOOOOOOOO"

        async for data in graph.astream(init_generator_2()):
            logger.info(f'final and final result: {data}')


        await asyncio.gather(task, task2)

        end_time = time.time()
        logger.info(f"Time taken: {end_time - start_time} seconds")


    import asyncio
    asyncio.run(main())
