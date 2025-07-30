import asyncio
from typing import Any, AsyncGenerator, Dict, List
from logging import Logger
from enum import Enum
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

class OutputStrategy(Enum):
    """
    Output strategy.
    """
    ASAP = "asap"
    ORDERED = "ordered"
    
    @classmethod
    def from_string(cls, value: str) -> "OutputStrategy":
        """Create an OutputStrategy from a string value."""
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"Invalid output strategy: {value}")

class AsyncProcessor(ProcessorInterface):
    """
    Processor implementation.

    Attributes:
        processor_id (str): The unique identifier for the processor.
        input_pipe (PipeInterface): The input pipe for the processor.
        output_pipe (PipeInterface): The output pipe for the processor.
    """
    def __init__(self, processor_id: str, 
            input_pipe: PipeInterface, 
            output_pipe: PipeInterface, 
            output_strategy: str = "asap",
            logger: Logger = Logger("AsyncProcessor"), 
            max_concurrent: int = 10):

        self.processor_id = processor_id
        self.input_pipe = input_pipe
        self.output_pipe = output_pipe
        self.semaphore = asyncio.Semaphore(max_concurrent)

        # output strategy type
        self.output_strategy = OutputStrategy.from_string(output_strategy)

    
        # tasks
        self.tasks = []

        # statistics
        self.statistics = ProcessorStatistics()

        # format logger
        self.logger = logger
    

    async def astream(self) -> AsyncGenerator[Any, None]:
        """
        Process data without blocking.
        """

        main_processing_task = asyncio.create_task(self.execute())
        async for data in self.output_pipe:
            yield data

        await main_processing_task

    async def execute(self) -> List[Any]:
        """
        Execute the processor. A non blocking method that will run processor.process() whenever input pipe
        has data.

        push data to output pipe as soon as it is processed

        """

        async with self.semaphore:
            async for data in self.input_pipe:
                task = asyncio.create_task(self.process(data))
                self.tasks.append(task) 


            if self.output_strategy == OutputStrategy.ASAP:
                for task in asyncio.as_completed(self.tasks):
                    result = await task
                    await self.output_pipe.put(result)

            elif self.output_strategy == OutputStrategy.ORDERED:
                for task in self.tasks:
                    result = await task
                    await self.output_pipe.put(result)


            # tell the output pipe that we are done
            await self.output_pipe.close()

            return await asyncio.gather(*self.tasks)

    
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