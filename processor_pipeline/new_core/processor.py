import asyncio
from typing import Any, AsyncGenerator, Dict, List
from logging import Logger
from .core_interfaces import ProcessorInterface, PipeInterface

from pydantic import BaseModel, computed_field


class ProcessorStatistics(BaseModel):
    """
    Statistics about the processor.
    """
    historic_process_count: int = 0
    historic_process_time: List[float] = []

    @computed_field
    def mean_process_time(self) -> float:
        return sum(self.historic_process_time) / len(self.historic_process_time)

    
    @computed_field
    def process_time_percentile_min_max(self) -> List[float]:
        return [
            min(self.historic_process_time),
            max(self.historic_process_time)
        ]


class AsyncProcessor(ProcessorInterface):
    """
    Processor implementation.

    Attributes:
        processor_id (str): The unique identifier for the processor.
        input_pipe (PipeInterface): The input pipe for the processor.
        output_pipe (PipeInterface): The output pipe for the processor.
    """
    def __init__(self, processor_id: str, input_pipe: PipeInterface, output_pipe: PipeInterface, logger: Logger = Logger("AsyncProcessor"), max_concurrent: int = 10):
        self.processor_id = processor_id
        self.input_pipe = input_pipe
        self.output_pipe = output_pipe
        self.semaphore = asyncio.Semaphore(max_concurrent)

        self.tasks = []
        self.statistics = ProcessorStatistics()

        # format logger
        self.logger = logger
    

    async def astream(self) -> AsyncGenerator[Any, None]:
        """
        Process data without blocking.
        """

        main_processing_task = asyncio.create_task(self.execute())
        async for data in self.output_pipe:
            # yield data
            yield self.tasks

        await main_processing_task

    async def execute(self) -> List[Any]:
        """
        Execute the processor. A non blocking method that will run processor.process() whenever input pipe
        has data.

        push data to output pipe as soon as it is processed

        """

        async with self.semaphore:
            async for data in self.input_pipe:
                task = asyncio.create_task(self.process_without_blocking(data))
                self.tasks.append(task) 

            result = await asyncio.gather(*self.tasks)
            # tell the output pipe that we are done
            await self.output_pipe.close()

            return result

    
    async def process_without_blocking(self, input_data: Any) -> None:
        """
        Process data without blocking.
        """
        async with self.semaphore:
            result = await self.process(input_data)
            await self.output_pipe.put(result)
            return result
    

    # async def output_strategy(self, strategy: str) -> None:
    #     """
    #     Output strategy.
    #     """
    #     if strategy == "blocking":
    #         await self.output_pipe.put(result)
    #     elif strategy == "non_blocking":
    #         pass
    #     else:
    #         raise ValueError(f"Invalid output strategy: {strategy}")


    async def initialize(self) -> None:
        pass
    
    async def cleanup(self) -> None:
        pass
    
    async def process(self, input_data: Any) -> Any:
        pass


    def register_input_pipe(self, pipe: PipeInterface) -> None:
        self.input_pipe = pipe

    def register_output_pipe(self, pipe: PipeInterface) -> None:
        self.output_pipe = pipe
    
    def statistics(self) -> Dict[str, Any]:
        return self.statistics.model_dump()