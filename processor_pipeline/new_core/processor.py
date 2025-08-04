import uuid
import asyncio
import traceback
import inspect
from enum import Enum
from collections import deque, defaultdict
from typing import Any, AsyncGenerator, Dict, List, Optional, Set
from .core_interfaces import ProcessorInterface, PipeInterface, ProcessorMeta
from .pipe import BufferPipe
from pydantic import BaseModel, computed_field
from threading import Thread
from loguru import logger

# Configure loguru for better formatting and control
logger.remove()  # Remove default handler
logger.add(
    "logs/processor_pipeline.log",
    rotation="10 MB",
    retention="7 days",
    level="INFO",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    backtrace=True,
    diagnose=True
)
logger.add(
    lambda msg: print(msg, end=""),
    level="DEBUG",
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> - <level>{message}</level>",
    colorize=True
)


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
        
        if max_concurrent is None:
            max_concurrent = meta.get('max_concurrent', 10)
            
        self.semaphore = asyncio.Semaphore(max_concurrent)

        # output strategy type
        self.output_strategy = OutputStrategy.from_string(output_strategy)

        # statistics
        self._statistics = ProcessorStatistics()

        self.background_tasks: List[asyncio.Task] = []
        
        # Debug logging for initialization
        self.logger = logger
        self.logger.debug(f"Initialized processor {self.processor_id} with output_strategy={self.output_strategy.value}, max_concurrent={max_concurrent}")
        self.logger.debug(f"Processor meta: {meta}")

    async def astream(self, data: Any) -> AsyncGenerator[Any, None]:
        """
        Process data without blocking.
        """
        self.logger.info(f"Starting astream processing for {self.processor_id}")

        main_processing_task = asyncio.create_task(self.execute(data))
        
        message_count = 0
        async for message_id, data in self.output_pipe:
            message_count += 1
            yield data

        self.logger.info(f"Completed astream processing for {self.processor_id}, yielded {message_count} messages")
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
        if observer_id is None:
            observer_id = f"observer_{str(uuid.uuid4())[:8]}"
        
        self.logger.info(f"Starting peek_astream for {self.processor_id} with observer_id={observer_id}")
        
        # Start the processing task
        main_processing_task = asyncio.create_task(self.execute())
        
        # Get data through the peek mechanism
        message_count = 0
        async for message_id, data in self.output_pipe.peek_aiter(observer_id):
            message_count += 1
            yield data

        self.logger.info(f"Completed peek_astream for {self.processor_id}, peeked {message_count} messages")
        await main_processing_task

    async def execute_output_asap(self) -> List[Any]:
        """
        Execute processor that outputs as soon as it is processed.
        Handles both async generators and regular async functions.
        """
        self.logger.info(f"Starting execute_output_asap for {self.processor_id}")
        tasks = []
        task_count = 0
        
        async def process_task(data: Any, message_id: str) -> Any:
            nonlocal task_count
            task_count += 1
            current_task_num = task_count
            
            start_time = asyncio.get_event_loop().time()
            
            try:
                process_result = self.process(data, message_id=message_id)
                
                # Check if the process method returns an async generator
                if inspect.isasyncgen(process_result):
                    results = []
                    item_count = 0
                    # Handle async generator case
                    async for item in process_result:
                        item_count += 1
                        await self.output_pipe.put(item)
                        results.append(item)
                    self.logger.debug(f"Task {current_task_num} completed async generator with {item_count} items")
                    return results
                else:
                    # Handle regular async function case
                    result = await process_result
                    await self.output_pipe.put(result)
                    return result
                    
            except Exception as e:
                self.logger.error(f"Task {current_task_num} failed: {e}")
                self.logger.error(f"Traceback: {traceback.format_exc()}")
                raise e
            finally:
                end_time = asyncio.get_event_loop().time()
                processing_time = end_time - start_time
                self.logger.debug(f"Task {current_task_num} completed in {processing_time:.4f}s")

        async with self.semaphore:
            try:
                input_count = 0
                async for message_id, data in self.input_pipe:
                    input_count += 1
                    task = asyncio.create_task(process_task(data, message_id=message_id))
                    tasks.append(task)

                self.logger.info(f"Processing {len(tasks)} tasks concurrently")
                final_results = await asyncio.gather(*tasks)
                self.logger.info(f"All {len(tasks)} tasks completed successfully")
                
            except Exception as e:
                self.logger.error(f"Error in processor {self.processor_id}: {e}")
                self.logger.error(traceback.format_exc())
                raise e
            finally:
                await self.output_pipe.close()
                self.logger.info(f"execute_output_asap completed for {self.processor_id}")
                return final_results

    async def execute_output_ordered(self) -> Any:
        """
        Execute processor that outputs in order.
        """
        self.logger.info(f"Starting execute_output_ordered for {self.processor_id}")
        tasks = []
        results = []

        async with self.semaphore:
            try:
                input_count = 0
                async for message_id, data in self.input_pipe:
                    input_count += 1
                    task = asyncio.create_task(self.process(data, message_id=message_id))
                    tasks.append(task)

                self.logger.info(f"Executing {len(tasks)} tasks in order")
                
                for i, task in enumerate(tasks):
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
                self.logger.info(f"execute_output_ordered completed for {self.processor_id} with {len(results)} results")
                return results

    async def execute_output_ordered_async_generator(self) -> Any:
        """
        Execute processor that outputs in order.
        """
        self.logger.info(f"Starting execute_output_ordered_async_generator for {self.processor_id}")
        task_ids = []
        results = []
        task_queue_dict = defaultdict(asyncio.Queue)

        async def process_task(data: Any, message_id: str) -> Any:
            """
            may be having trouble if message is not unique?
            """
            process_result = self.process(data, message_id=message_id)
            # if inspect.isasyncgen(process_result):
            item_count = 0
            async for item in process_result:
                item_count += 1
                await task_queue_dict[message_id].put(item)
            
            self.logger.debug(f"Async generator completed for message_id={message_id}, sent {item_count} items")
            task_queue_dict[message_id].put_nowait(None)
        
        async def output_from_task_queue_with_order():
            result_list_of_list= []
            for message_id in task_ids:
                result = []
                item_count = 0
                while True:
                    item = await task_queue_dict[message_id].get()
                    if item is None:
                        break
                    item_count += 1
                    result.append(item)
                    await self.output_pipe.put(item)
                
                self.logger.debug(f"Completed queue for message_id={message_id} with {item_count} items")
                result_list_of_list.append(result)
            return result_list_of_list

        async with self.semaphore:
            try:
                input_count = 0
                async for message_id, data in self.input_pipe:
                    input_count += 1
                    task_ids.append(message_id)
                    task = asyncio.create_task(process_task(data, message_id=message_id))
                
                self.logger.info(f"Starting ordered output for {len(task_ids)} tasks")
                results = await output_from_task_queue_with_order()

            except Exception as e:
                self.logger.error(f"Error in processor {self.processor_id}: {e}")
                self.logger.error(traceback.format_exc())
                raise e
            finally:
                await self.output_pipe.close()
                self.logger.info(f"execute_output_ordered_async_generator completed for {self.processor_id}")
                return results

    async def execute(self, data: Any = None) -> List[Any]:
        """
        Execute the processor. A non blocking method that will run processor.process() whenever input pipe
        has data.

        push data to output pipe as soon as it is processed

        """
        self.logger.info(f"Starting execute for {self.processor_id} with output strategy: {self.output_strategy.value}")
        
        # Determine intake method based on data type
        if data is AsyncGenerator or inspect.isasyncgenfunction(data) or hasattr(data, '__aiter__'):
            generator = data
            async def intake_method():
                item_count = 0
                async for item in generator:
                    item_count += 1
                    await self.input_pipe.put(item)
                self.logger.debug(f"Async intake completed, sent {item_count} items")
                await self.input_pipe.put(None)
            intake_task = asyncio.create_task(intake_method())
        elif hasattr(data, '__iter__'):
            async def intake_method():
                item_count = 0
                for item in data:
                    item_count += 1
                    await self.input_pipe.put(item)
                self.logger.debug(f"Sync intake completed, sent {item_count} items")
                await self.input_pipe.put(None)
            intake_task = asyncio.create_task(intake_method())
        elif data is None:
            intake_task = asyncio.create_task(asyncio.sleep(0))
        
        # Create main processing task based on output strategy
        if self.output_strategy == OutputStrategy.ASAP:
            main_task = asyncio.create_task(self.execute_output_asap())
        elif self.output_strategy == OutputStrategy.ORDERED:
            if inspect.isasyncgenfunction(self.process):
                main_task = asyncio.create_task(self.execute_output_ordered_async_generator())
            else:
                main_task = asyncio.create_task(self.execute_output_ordered())
        else:
            raise ValueError(f"Invalid output strategy: {self.output_strategy}")
        
        _, result, *_ = await asyncio.gather(intake_task, main_task, *self.background_tasks)
        self.logger.info(f"Execute completed for {self.processor_id}")
        return result

    async def initialize(self) -> None:
        """Initialize the processor and start merger/broadcaster tasks if there are registered pipes"""
        self.logger.debug(f"Initializing processor {self.processor_id}")
        pass
    
    async def cleanup(self) -> None:
        """Cleanup the processor"""
        self.logger.debug(f"Cleaning up processor {self.processor_id}")
        pass

    def register_input_pipe(self, pipe: PipeInterface) -> None:
        """
        Register an input pipe for the processor.
        Args:
            pipe: The input pipe to register for the processor
        """
        self.logger.debug(f"Registering input pipe {type(pipe).__name__} for {self.processor_id}")
        self.input_pipe = pipe
        
    def register_output_pipe(self, pipe: PipeInterface) -> None:
        """
        Register an output pipe for the processor.
        Args:
            pipe: The output pipe to register for the processor
        """
        self.logger.debug(f"Registering output pipe {type(pipe).__name__} for {self.processor_id}")
        self.output_pipe = pipe

    def statistics(self) -> Dict[str, Any]:
        """Get performance and status statistics for the processor"""
        self.logger.debug(f"Generating statistics for {self.processor_id}")
        stats = self._statistics.model_dump()
        # Add pipe registration info
        stats['registered_input_pipes'] = len(self._registered_input_pipes)
        stats['registered_output_pipes'] = len(self._registered_output_pipes)
        stats['merger_broadcaster_running'] = self._merger_broadcaster_running
        self.logger.debug(f"Statistics for {self.processor_id}: {stats}")
        return stats
    
