import uuid
import asyncio
import traceback
import inspect
from enum import Enum
from collections import deque, defaultdict
from logging import Logger
from typing import Any, AsyncGenerator, Dict, List, Optional, Set
from .core_interfaces import ProcessorInterface, PipeInterface, ProcessorMeta
from .pipe import BufferPipe
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

class AsyncProcessor(ProcessorInterface, metaclass=ProcessorMeta):
    """
    Processor implementation.

    Attributes:
        processor_id (str): The unique identifier for the processor.
        input_pipe (PipeInterface): The input pipe for the processor.
        output_pipe (PipeInterface): The output pipe for the processor.
    """
    def __init__(self, 
            processor_id: Optional[str] = None, 
            output_strategy: str = None,
            logger: Logger = None, 
            max_concurrent: int = None):

        # Main workflow pipes - these are the only pipes that connect to the actual processing workflow
        self.input_pipe = None
        self.output_pipe = None
        
        # Use meta values as defaults if available
        meta = getattr(self.__class__, '_meta', {})

        self.processor_id = processor_id
        if self.processor_id is None:
            self.processor_id = f"{meta['name']}_{str(uuid.uuid4())}"
        
        # Set defaults from meta or fallback values
        if output_strategy is None:
            output_strategy = meta.get('output_strategy', 'ordered')
        if logger is None:
            logger = Logger(meta.get('name', 'AsyncProcessor'))
        if max_concurrent is None:
            max_concurrent = meta.get('max_concurrent', 10)
            
        self.semaphore = asyncio.Semaphore(max_concurrent)

        # output strategy type
        self.output_strategy = OutputStrategy.from_string(output_strategy)

        # statistics
        self._statistics = ProcessorStatistics()

        # format logger
        self.logger = logger
    

    async def astream(self) -> AsyncGenerator[Any, None]:
        """
        Process data without blocking.
        """


        main_processing_task = asyncio.create_task(self.execute())
        async for message_id, data in self.output_pipe:
            yield data

        await main_processing_task


    async def peek_astream(self, observer_id: Optional[str] = None) -> AsyncGenerator[Any, None]:
        """
        Process data without blocking and without consuming from the output pipe.
        This allows multiple consumers to "peek" at the data without interfering 
        with the main data flow.
        
        Args:
            observer_id: Optional observer ID for tracking. If None, a new one is generated.
            
        Yields:
            Any: Data from the processor's output pipe without consuming it
        """
        # Start the processing task
        main_processing_task = asyncio.create_task(self.execute())
        
        # Get data through the peek mechanism
        async for message_id, data in self.output_pipe.peek_aiter(observer_id):
            yield data

        await main_processing_task

    
    async def execute_output_asap(self) -> List[Any]:
        """
        Execute processor that outputs as soon as it is processed.
        Handles both async generators and regular async functions.
        """
        tasks = []
        
        async def process_task(data: Any, message_id: str) -> Any:
            process_result = self.process(data, message_id=message_id)
            
            # Check if the process method returns an async generator
            if inspect.isasyncgen(process_result):
                results = []
                # Handle async generator case
                async for item in process_result:
                    await self.output_pipe.put(item)
                    results.append(item)
                return results
            else:
                # Handle regular async function case
                result = await process_result
                await self.output_pipe.put(result)
                return result

        async with self.semaphore:
            try:
                async for message_id, data in self.input_pipe:
                    task = asyncio.create_task(process_task(data, message_id=message_id))
                    tasks.append(task)

                result = await asyncio.gather(*tasks)
            except Exception as e:
                self.logger.error(f"Error in processor {self.processor_id}: {e}")
                self.logger.error(traceback.format_exc())
                raise e
            finally:
                await self.output_pipe.close()
                return result

    async def execute_output_ordered(self) -> Any:
        """
        Execute processor that outputs in order.
        """
        tasks = []
        results = []

        async with self.semaphore:
            try:
                async for message_id, data in self.input_pipe:
                    task = asyncio.create_task(self.process(data, message_id=message_id))
                    tasks.append(task)

                for task in tasks:
                    result = await task
                    await self.output_pipe.put(result)
                    results.append(result)

            except Exception as e:
                self.logger.error(f"Error in processor {self.processor_id}: {e}")
                self.logger.error(traceback.format_exc())
                raise e
            finally:
                await self.output_pipe.close()
                await self.input_pipe.close()
                return results       # return result


    async def execute_output_ordered_async_generator(self) -> Any:
        """
        Execute processor that outputs in order.
        """
        task_ids = []
        results = []
        task_queue_dict = defaultdict(asyncio.Queue)


        async def process_task(data: Any, message_id: str) -> Any:
            """
            may be having trouble if message is not unique?
            """
            process_result = self.process(data, message_id=message_id)
            # if inspect.isasyncgen(process_result):
            async for item in process_result:
                await task_queue_dict[message_id].put(item)
            
            task_queue_dict[message_id].put_nowait(None)
        
        async def output_from_task_queue_with_order():
            result_list_of_list= []
            for message_id in task_ids:
                result = []
                while True:
                    item = await task_queue_dict[message_id].get()
                    if item is None:
                        break
                    result.append(item)
                    await self.output_pipe.put(item)
                
                result_list_of_list.append(result)
            return result_list_of_list

        async with self.semaphore:
            try:
                async for message_id, data in self.input_pipe:
                    task_ids.append(message_id)
                    task = asyncio.create_task(process_task(data, message_id=message_id))
                results = await output_from_task_queue_with_order()

            except Exception as e:
                self.logger.error(f"Error in processor {self.processor_id}: {e}")
                self.logger.error(traceback.format_exc())
                raise e
            finally:
                await self.output_pipe.close()
                return results



    async def execute(self) -> List[Any]:
        """
        Execute the processor. A non blocking method that will run processor.process() whenever input pipe
        has data.

        push data to output pipe as soon as it is processed

        """
        
        if self.output_strategy == OutputStrategy.ASAP:
            return await self.execute_output_asap()
        elif self.output_strategy == OutputStrategy.ORDERED:
            if inspect.isasyncgenfunction(self.process):
                return await self.execute_output_ordered_async_generator()
            else:
                return await self.execute_output_ordered()
        else:
            raise ValueError(f"Invalid output strategy: {self.output_strategy}")


    async def initialize(self) -> None:
        """Initialize the processor and start merger/broadcaster tasks if there are registered pipes"""
        pass
    
    async def cleanup(self) -> None:
        """Cleanup the processor and stop merger/broadcaster tasks"""
        await self._stop_merger_broadcaster()

    def register_input_pipe(self, pipe: PipeInterface) -> None:
        """
        Register an input pipe for the processor.
        Args:
            pipe: The input pipe to register for the processor
        """
        self.input_pipe = pipe
        
    def register_output_pipe(self, pipe: PipeInterface) -> None:
        """
        Register an output pipe for the processor.
        Args:
            pipe: The output pipe to register for the processor
        """
        self.output_pipe = pipe

    
    def statistics(self) -> Dict[str, Any]:
        """Get performance and status statistics for the processor"""
        stats = self._statistics.model_dump()
        # Add pipe registration info
        stats['registered_input_pipes'] = len(self._registered_input_pipes)
        stats['registered_output_pipes'] = len(self._registered_output_pipes)
        stats['merger_broadcaster_running'] = self._merger_broadcaster_running
        return stats
    
