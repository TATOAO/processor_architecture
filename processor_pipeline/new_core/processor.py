import asyncio
from .core_interfaces import ProcessorInterface, PipeInterface


class AsyncProcessor(ProcessorInterface):
    """
    Processor implementation.

    Attributes:
        processor_id (str): The unique identifier for the processor.
        input_pipe (PipeInterface): The input pipe for the processor.
        output_pipe (PipeInterface): The output pipe for the processor.
    """
    def __init__(self, processor_id: str, input_pipe: PipeInterface, output_pipe: PipeInterface, max_concurrent: int = 10):
        self.processor_id = processor_id
        self.input_pipe = input_pipe
        self.output_pipe = output_pipe
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def execute(self) -> None:
        """
        Execute the processor. A non blocking method that will run processor.process() whenever input pipe
        has data.

        """

        tasks = []
        while True:
            data = await self.input_pipe.get()
            task = asyncio.create_task(self.process(data))
            tasks.append(task)

            await self.process(data)
            await self.output_pipe.put(data)

            if await self.input_pipe.is_empty():
                break


    async def initialize(self) -> None:
        pass
    
    async def cleanup(self) -> None:
        pass
    
    async def process(self, input_data: Any) -> AsyncGenerator[Any, None]:
        pass