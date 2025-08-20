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
            else:
                s = 0.1

            logger.info(f"input_data: {input_data}, i: {i}, s: {s}")
            await asyncio.sleep(s)
            logger.info(f"sleeping for {s} seconds")
            yield input_data[i:i+chunk_size]
        
        logger.warning("ChunkerProcessor completed")

class HasherProcessor(AsyncProcessor):
    meta = {
        "name": "HasherProcessor",
        "output_strategy": "asap",
    }

    async def process(self, input_data:str, *args, **kwargs) -> str:

        # hashed = hashlib.sha256(input_data.encode()).hexdigest()
        # logger.info(f"input_data: {input_data}, hashed: {hashed}")
        logger.info(f"HasherProcessor 111 input_data: {input_data}")
        hashed = input_data[0]

        for i in range(5):
            await asyncio.sleep(1)
            logger.info(f"HasherProcessor sleeping for 0.1 seconds")
            yield i
        logger.warning("HasherProcessor1 completed")

class HasherProcessor2(AsyncProcessor):
    meta = {
        "name": "HasherProcessor2",
        "output_strategy": "asap",
    }

    async def process(self, input_data:str, *args, **kwargs) -> str:

        # hashed = hashlib.sha256(input_data.encode()).hexdigest()
        # logger.info(f"input_data: {input_data}, hashed: {hashed}")
        logger.info(f"HasherProcessor 222 input_data: {input_data}")

        for i in "abcde":
            await asyncio.sleep(1)
            logger.info(f"HasherProcessor sleeping for 0.1 seconds")
            yield i
        logger.warning("HasherProcessor2 completed")


# python -m processor_pipeline.new_core.tests.test_graph
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
                    processor_class_name="HasherProcessor", 
                    processor_unique_name="hasher_processor_1"),
                Node(
                    processor_class_name="HasherProcessor2", 
                    processor_unique_name="hasher_processor_2"),

            ], edges=[
                Edge(source_node_unique_name="chunker_processor_1", 
                    target_node_unique_name="hasher_processor_1", 
                    edge_unique_name="edge_1"),
                Edge(source_node_unique_name="chunker_processor_1", 
                    target_node_unique_name="hasher_processor_2", 
                    edge_unique_name="edge_2"),
            ], processor_id = "graph_1"
        )

        graph.initialize()

        async def monitor_pipe_intake_task():
            async for data in graph.processor_pipes["hasher_processor_1"].input_pipe.peek_aiter():
                logger.info(f"[111111111111111111  hasher 1 processor input] monitor data: {data}")

        # async def monitor_pipe_hasher_input_task():
        #     async for data in graph.processor_pipes["hasher_processor_1"].input_pipe.peek_aiter():
        #         logger.info(f"[111111111111111111  hasher 1 processor input] monitor data: {data}")



        # async def monitor_pipe_hasher_input_task2():
        #     async for data in graph.processor_pipes["hasher_processor_2"].input_pipe.peek_aiter():
        #         logger.info(f"[22222222222222222 hasher 2 processor input] monitor data: {data}")


        # async def monitor_pipe_chunk_output_task():
        #     async for data in graph.processor_pipes["chunker_processor_1"].output_pipe.peek_aiter():
        #         logger.info(f"[3333333333333 chunker processor output] monitor data: {data}")


        # task1 = asyncio.create_task(monitor_pipe_hasher_input_task())
        # task2 = asyncio.create_task(monitor_pipe_chunk_output_task())
        # task3 = asyncio.create_task(monitor_pipe_hasher_input_task2())


        def init_generator_1():
            return [
                "X" * 10,
                "O" * 10,
            ]

        async def init_generator_2():
            yield "XXXXXXXXXX"
            yield "OOOOOOOOOO"

        async def init_generator_3():
            yield "1234"


        async for data in graph.astream(init_generator_3()):
            logger.info(f'xxxxxxxxxxxxxxxx: {data}')


        # await asyncio.gather(task1, task2, task3)

        end_time = time.time()
        logger.info(f"Time taken: {end_time - start_time} seconds")


    asyncio.run(main())
