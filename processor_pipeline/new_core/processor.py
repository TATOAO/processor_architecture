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
from .logger import logger


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

        # global dict to store things across all processors
        self.session = {}

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
        
        # Get pipe types from meta (defaults are now set in ProcessorMeta)
        self.input_pipe_type = meta["input_pipe_type"]
        self.output_pipe_type = meta["output_pipe_type"]
        self.background_tasks: List[asyncio.Task] = []

            
        self.semaphore = asyncio.Semaphore(max_concurrent)

        # output strategy type
        self.output_strategy = OutputStrategy.from_string(output_strategy)

        # statistics
        self._statistics = ProcessorStatistics()

        
        # Debug logging for initialization
        self.logger = logger.bind(object_name=self.processor_id)
        self.logger.debug(f"Initialized processor {self.processor_id} with output_strategy={self.output_strategy.value}, max_concurrent={max_concurrent}")
        self.logger.debug(f"Processor meta: {meta}")

    def bind_session_id(self, session_id: str) -> None:
        """Bind a session_id to this processor and its pipes for contextual logging."""
        # Track in shared session dict
        try:
            self.session["session_id"] = session_id
        except Exception:
            self.session = {"session_id": session_id}

        # Bind logger for this processor
        self.logger = self.logger.bind(session_id=session_id, object_name=self.processor_id)

        # Bind logger for pipes if available
        if getattr(self, "input_pipe", None) is not None and getattr(self.input_pipe, "logger", None) is not None:
            try:
                self.input_pipe.logger = self.input_pipe.logger.bind(session_id=session_id, object_name=self.input_pipe._pipe_id)
            except Exception:
                pass
        if getattr(self, "output_pipe", None) is not None and getattr(self.output_pipe, "logger", None) is not None:
            try:
                self.output_pipe.logger = self.output_pipe.logger.bind(session_id=session_id, object_name=self.output_pipe._pipe_id)
            except Exception:
                pass

    async def astream(self, data: Any, session_id: Optional[str] = None, *args, **kwargs) -> AsyncGenerator[Any, None]:
        """
        Process data without blocking.
        """
        if session_id is None:
            # Reuse existing session or generate a new one
            session_id = (self.session.get("session_id") if isinstance(self.session, dict) else None) or str(uuid.uuid4())
        self.bind_session_id(session_id)
        self.logger.info(f"Starting astream processing for {self.processor_id}")

        main_processing_task = asyncio.create_task(self.execute(data, session_id=session_id))
        
        message_count = 0
        async for message_id, data in self.output_pipe:
            message_count += 1
            yield data

        self.logger.info(f"Completed astream processing for {self.processor_id}, yielded {message_count} messages")
        await main_processing_task

    async def peek_astream(self, observer_id: Optional[str] = None, session_id: Optional[str] = None, *args, **kwargs) -> AsyncGenerator[Any, None]:
        """
        Process data without blocking and without consuming from the output pipe.
        This allows multiple consumers to "peek" at the data without interfering 
        with the main data flow.
        
        Args:
            observer_id: Optional observer ID for tracking. If None, a new one is generated.
            
        Yields:
            Any: Data from the processor's output pipe without consuming it
        """
        if session_id is None:
            session_id = (self.session.get("session_id") if isinstance(self.session, dict) else None) or str(uuid.uuid4())
        self.bind_session_id(session_id)

        if observer_id is None:
            observer_id = f"observer_{str(uuid.uuid4())[:8]}"
        
        self.logger.info(f"Starting peek_astream for {self.processor_id} with observer_id={observer_id}")
        
        # Start the processing task
        main_processing_task = asyncio.create_task(self.execute(session_id=session_id))
        
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
                short_id = (message_id or "").split("_")[0]
                # Bind per-input suffix for this task's logs
                with logger.contextualize(sid_suffix=f":{short_id}"):
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
            finally:
                end_time = asyncio.get_event_loop().time()
                processing_time = end_time - start_time
                self.logger.debug(f"Task {current_task_num} completed in {processing_time:.4f}s")

        async with self.semaphore:
            final_results = []
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
        task_queue = asyncio.Queue()


        async def process_task(data: Any, message_id: str) -> Any:
            self.logger.debug(f"processor {self.processor_id} start process for message_id={message_id}, data={data}")
            short_id = (message_id or "").split("_")[0]
            async def run_with_context():
                with logger.contextualize(sid_suffix=f":{short_id}"):
                    return await self.process(data, message_id=message_id)
            task = asyncio.create_task(run_with_context())
            task_queue.put_nowait(task)
        
        async def output_task() -> Any:
            self.logger.debug(f"processor {self.processor_id} start output")
            while True:
                task = await task_queue.get()
                if task is None:
                    break
                item = await task
                await self.output_pipe.put(item)
                results.append(item)

        async with self.semaphore:
            try:
                output_task = asyncio.create_task(output_task())
                async for message_id, data in self.input_pipe:
                    self.logger.debug(f"processor {self.processor_id} start process for message_id={message_id}, data={data}")
                    tasks.append(asyncio.create_task(process_task(data, message_id=message_id)))

                self.logger.info(f"Executing {len(tasks)} tasks in order")
                task_queue.put_nowait(None)
                
                await asyncio.gather(*tasks, output_task)

            except Exception as e:
                self.logger.error(f"Error in processor {self.processor_id}: {e}")
                self.logger.error(traceback.format_exc())
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
        task_queue_dict = defaultdict(asyncio.Queue)
        output_task_message_id_queue = asyncio.Queue()
        results = []

        async def process_task(data: Any, message_id: str) -> Any:
            """
            may be having trouble if message is not unique?
            """
            short_id = (message_id or "").split("_")[0]
            with logger.contextualize(sid_suffix=f":{short_id}"):
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
            while True:
                message_id = await output_task_message_id_queue.get()
                if message_id is None:
                    break
                result = []
                item_count = 0
                short_id = (message_id or "").split("_")[0]
                with logger.contextualize(sid_suffix=f":{short_id}"):
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
                task_allocated = []
                input_count = 0
                output_task = asyncio.create_task(output_from_task_queue_with_order())
                async for message_id, data in self.input_pipe:
                    input_count += 1
                    task = asyncio.create_task(process_task(data, message_id=message_id))
                    output_task_message_id_queue.put_nowait(message_id)
                    task_allocated.append(task)
                output_task_message_id_queue.put_nowait(None)
                
                self.logger.info(f"Starting ordered output for {len(task_ids)} tasks")
                results, *_ = await asyncio.gather(output_task, *task_allocated)

            except Exception as e:
                self.logger.error(f"Error in processor {self.processor_id}: {e}")
                self.logger.error(traceback.format_exc())
            finally:
                await self.output_pipe.close()
                self.logger.info(f"execute_output_ordered_async_generator completed for {self.processor_id}")
                return results

    async def execute(self, data: Any = None, session_id: Optional[str] = None, *args, **kwargs) -> List[Any]:
        """
        Execute the processor. A non blocking method that will run processor.process() whenever input pipe
        has data.

        push data to output pipe as soon as it is processed

        """
        if session_id is None:
            session_id = (self.session.get("session_id") if isinstance(self.session, dict) else None) or str(uuid.uuid4())
        self.bind_session_id(session_id)

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
        elif isinstance(data, list):
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
        else:
            # a one time intake input
            task1 = asyncio.create_task(self.input_pipe.put(data))
            task2 = asyncio.create_task(self.input_pipe.put(None))
            intake_task = asyncio.gather(task1, task2)
        
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
        
        try:
            _, result = await asyncio.gather(intake_task, main_task)
            self.logger.info(f"Execute completed for {self.processor_id}")
        
        except Exception as e:
            self.logger.error(f"Error in processor {self.processor_id}: {e}")
            self.logger.error(traceback.format_exc())
            raise e
        finally:
            await self.output_pipe.put(None)
            try:
                await asyncio.gather(*self.background_tasks)
            except Exception as e:
                self.logger.error(f"Error in processor {self.processor_id}: {e}")
                self.logger.error(traceback.format_exc())
                raise e

        return result
    
    def add_background_task(self, task: asyncio.Task) -> None:
        """
        Add a background task to the processor.
        """
        self.background_tasks.append(task)

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
    
