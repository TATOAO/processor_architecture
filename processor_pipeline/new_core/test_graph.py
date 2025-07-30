from typing import AsyncGenerator
from .processor import AsyncProcessor

class ChunkerProcessor(AsyncProcessor):
    meta = {
        "name": "ChunkerProcessor",
        "input_pipe_type": "AsyncPipe",
        "output_pipe_type": "AsyncPipe",
        "output_strategy": "asap",
    }

    async def process(self, input_data:str, *args, **kwargs) -> AsyncGenerator[str, None]:
        chunk_size = 10
        for i in range(0, len(input_data), chunk_size):
            yield input_data[i:i+chunk_size]


# python -m processor_pipeline.new_core.test_graph
if __name__ == "__main__":
    from .pipe import AsyncPipe
    input_pipe = AsyncPipe(pipe_id="input_pipe_test1")
    output_pipe = AsyncPipe(pipe_id="output_pipe_test1")

    async def main():
        await input_pipe.put("Hello, world! " * 100)
        await input_pipe.put("hhhhhhhhhhhh " * 100)

        chunker_processor = ChunkerProcessor(input_pipe=input_pipe, output_pipe=output_pipe)
        async for data in chunker_processor.astream():
            print(data)

    import asyncio
    asyncio.run(main())
