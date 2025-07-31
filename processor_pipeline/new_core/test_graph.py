import asyncio
import hashlib
import random
from typing import AsyncGenerator
from processor_pipeline.new_core.processor import AsyncProcessor
from processor_pipeline.new_core.graph import GraphBase
from processor_pipeline.new_core.graph_model import Node, Edge

class ChunkerProcessor(AsyncProcessor):
    meta = {
        "name": "ChunkerProcessor",
        "input_pipe_type": "AsyncPipe",
        "output_pipe_type": "AsyncPipe",
        "output_strategy": "ordered",
    }

    async def process(self, input_data:str, *args, **kwargs) -> AsyncGenerator[str, None]:
        chunk_size = 2
        for i in range(0, len(input_data), chunk_size):
            s = random.random()

            if input_data[0] == "X":
                s = 0.3
            if input_data[0] == "O":
                s = 0.7

            await asyncio.sleep(s)
            print("sleeping for", s, "seconds")
            yield input_data[i:i+chunk_size]

class HasherProcessor(AsyncProcessor):
    meta = {
        "name": "HasherProcessor",
        "input_pipe_type": "AsyncPipe",
        "output_pipe_type": "AsyncPipe",
        "output_strategy": "asap",
    }

    async def process(self, input_data:str, *args, **kwargs) -> [str, None]:
        hashed = hashlib.sha256(input_data.encode()).hexdigest()
        print("input_data", input_data, "hashed", hashed)
        return hashed


# python -m processor_pipeline.new_core.test_graph
if __name__ == "__main__":
    from .pipe import AsyncPipe

    import time
    import logging
    logging.basicConfig(level=logging.DEBUG)

    async def main():
        start_time = time.time()

        graph = GraphBase(nodes=[
            Node(processor_class_name="ChunkerProcessor", processor_unique_name="chunker_processor_1"),
            Node(processor_class_name="HasherProcessor", processor_unique_name="hasher_processor_1"),
        ], edges=[
            Edge(source_node_unique_name="chunker_processor_1", 
                target_node_unique_name="hasher_processor_1", 
                edge_unique_name="edge_1"),
        ])

        def init_generator_1():
            return [
                "X" * 10,
                "O" * 10,
            ]

        def init_generator_2():
            for i in range(10):
                yield "X" * 10
                yield "O" * 10
                yield "X" * 10


        async for data in graph.astream(init_generator_1):
            print('result', data)

        end_time = time.time()
        print(f"Time taken: {end_time - start_time} seconds")

    import asyncio
    asyncio.run(main())
