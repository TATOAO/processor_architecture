import asyncio
from typing import AsyncGenerator
from loguru import logger
from processor_pipeline.new_core import AsyncProcessor, GraphBase, Node, Edge

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

class HasherProcessor(AsyncProcessor):
    meta = {
        "name": "HasherProcessor",
        "output_strategy": "ordered",
    }

    async def process(self, input_data:str, *args, **kwargs) -> str:

        # hashed = hashlib.sha256(input_data.encode()).hexdigest()
        # logger.info(f"input_data: {input_data}, hashed: {hashed}")
        logger.info(f"HasherProcessor input_data: {input_data}")
        hashed = input_data[0]

        for i in range(5):
            await asyncio.sleep(0.1)
            logger.info(f"HasherProcessor sleeping for 0.1 seconds")
            yield i

demo_graph = GraphBase(
    nodes=[
        Node(
            processor_class_name="ChunkerProcessor", 
            processor_unique_name="chunker_processor_1"),
        Node(
            processor_class_name="HasherProcessor", 
            processor_unique_name="hasher_processor_1"),
    ], edges=[
        Edge(source_node_unique_name="chunker_processor_1", 
            target_node_unique_name="hasher_processor_1", 
            edge_unique_name="edge_1"),
    ], processor_id = "graph_1"
)

# Only initialize when an event loop is running; safe to import module otherwise
try:
    asyncio.get_running_loop()
    demo_graph.initialize()
except RuntimeError:
    # No running event loop at import time; caller can initialize later
    pass
