from .processor import AsyncProcessor
from .pipe import AsyncPipe

class TestProcessor(AsyncProcessor):
    """
    Test processor.
    """
    def __init__(self, processor_id: str, input_pipe: AsyncPipe, output_pipe: AsyncPipe):
        super().__init__(processor_id, input_pipe, output_pipe)

    async def process(self, data: Any) -> None:
        async with self.semaphore:
        yield data


if __name__ == "__main__":
    input_pipe = AsyncPipe()
    output_pipe = AsyncPipe()

    async def main():
        processor = TestProcessor("test", input_pipe, output_pipe)
        await processor.execute()

    asyncio.run(main())