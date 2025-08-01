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
            input_pipe: PipeInterface = ..., 
            output_pipe: PipeInterface = ..., 
            output_strategy: str = None,
            logger: Logger = None, 
            max_concurrent: int = None):

        # Main workflow pipes - these are the only pipes that connect to the actual processing workflow
        self.input_pipe = input_pipe
        self.output_pipe = output_pipe
        
        # Registered pipes for merging inputs and broadcasting outputs
        self._registered_input_pipes: Set[PipeInterface] = set()
        self._registered_output_pipes: Set[PipeInterface] = set()
        self._pipe_registration_lock = asyncio.Lock()
        
        # Background tasks for input merging and output broadcasting
        self._input_merger_task: Optional[asyncio.Task] = None
        self._output_broadcaster_task: Optional[asyncio.Task] = None
        self._merger_broadcaster_running = False
        
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

    async def _input_merger(self) -> None:
        """
        Background task that merges input from all registered input pipes into the main input pipe.
        This runs continuously and handles dynamic registration of new input pipes.
        """
        merger_tasks = set()
        
        async def merge_from_pipe(pipe: PipeInterface) -> None:
            """Merge data from a single input pipe into the main input pipe"""
            try:
                async for message_id, data in pipe:
                    await self.input_pipe.put(data)
            except Exception as e:
                self.logger.error(f"Error in input merger for pipe: {e}")
                self.logger.error(traceback.format_exc())
        
        try:
            while self._merger_broadcaster_running:
                # Check for new registered input pipes
                async with self._pipe_registration_lock:
                    current_pipes = self._registered_input_pipes.copy()
                
                # Start merger tasks for any new pipes
                for pipe in current_pipes:
                    # Check if we already have a task for this pipe
                    if not any(task.get_name() == f"merger_{id(pipe)}" for task in merger_tasks if not task.done()):
                        task = asyncio.create_task(merge_from_pipe(pipe))
                        task.set_name(f"merger_{id(pipe)}")
                        merger_tasks.add(task)
                
                # Clean up completed tasks
                merger_tasks = {task for task in merger_tasks if not task.done()}
                
                # Wait a bit before checking for new pipes
                await asyncio.sleep(0.1)
                
        except asyncio.CancelledError:
            self.logger.debug("Input merger task cancelled")
        except Exception as e:
            self.logger.error(f"Error in input merger: {e}")
            self.logger.error(traceback.format_exc())
        finally:
            # Cancel all merger tasks
            for task in merger_tasks:
                if not task.done():
                    task.cancel()
            # Wait for all tasks to complete
            if merger_tasks:
                await asyncio.gather(*merger_tasks, return_exceptions=True)

    async def _output_broadcaster(self) -> None:
        """
        Background task that broadcasts output from the main output pipe to all registered output pipes.
        This runs continuously and handles dynamic registration of new output pipes.
        """
        try:
            # Use the peek mechanism to avoid consuming from the main output pipe
            observer_id = await self.output_pipe.register_observer("broadcaster")
            
            async for message_id, data in self.output_pipe.peek_aiter(observer_id):
                # Broadcast to all registered output pipes
                async with self._pipe_registration_lock:
                    broadcast_tasks = []
                    for pipe in self._registered_output_pipes:
                        broadcast_tasks.append(asyncio.create_task(pipe.put(data)))
                    
                    if broadcast_tasks:
                        await asyncio.gather(*broadcast_tasks, return_exceptions=True)
                        
        except asyncio.CancelledError:
            self.logger.debug("Output broadcaster task cancelled")
        except Exception as e:
            self.logger.error(f"Error in output broadcaster: {e}")
            self.logger.error(traceback.format_exc())

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
        # Start merger/broadcaster tasks if needed
        await self._start_merger_broadcaster_if_needed()
        
        try:
            if self.output_strategy == OutputStrategy.ASAP:
                return await self.execute_output_asap()
            elif self.output_strategy == OutputStrategy.ORDERED:
                if inspect.isasyncgenfunction(self.process):
                    return await self.execute_output_ordered_async_generator()
                else:
                    return await self.execute_output_ordered()
            else:
                raise ValueError(f"Invalid output strategy: {self.output_strategy}")
        finally:
            # Ensure cleanup happens even if execution fails
            await self._stop_merger_broadcaster()


    
    async def initialize(self) -> None:
        """Initialize the processor and start merger/broadcaster tasks if there are registered pipes"""
        await self._start_merger_broadcaster_if_needed()
    
    async def cleanup(self) -> None:
        """Cleanup the processor and stop merger/broadcaster tasks"""
        await self._stop_merger_broadcaster()

    async def _start_merger_broadcaster_if_needed(self) -> None:
        """Start merger and broadcaster tasks if they aren't already running and there are registered pipes"""
        if self._merger_broadcaster_running:
            return
            
        async with self._pipe_registration_lock:
            has_registered_pipes = len(self._registered_input_pipes) > 0 or len(self._registered_output_pipes) > 0
        
        if has_registered_pipes:
            await self._start_merger_broadcaster()

    async def _start_merger_broadcaster(self) -> None:
        """Start the merger and broadcaster background tasks"""
        if self._merger_broadcaster_running:
            return
            
        self._merger_broadcaster_running = True
        
        # Start input merger task
        if self._input_merger_task is None or self._input_merger_task.done():
            self._input_merger_task = asyncio.create_task(self._input_merger())
            self._input_merger_task.set_name(f"input_merger_{self.processor_id}")
        
        # Start output broadcaster task  
        if self._output_broadcaster_task is None or self._output_broadcaster_task.done():
            self._output_broadcaster_task = asyncio.create_task(self._output_broadcaster())
            self._output_broadcaster_task.set_name(f"output_broadcaster_{self.processor_id}")
            
        self.logger.debug(f"Started merger/broadcaster tasks for processor {self.processor_id}")

    async def _stop_merger_broadcaster(self) -> None:
        """Stop the merger and broadcaster background tasks"""
        if not self._merger_broadcaster_running:
            return
            
        self._merger_broadcaster_running = False
        
        # Cancel and wait for tasks to complete
        tasks_to_cancel = []
        if self._input_merger_task and not self._input_merger_task.done():
            tasks_to_cancel.append(self._input_merger_task)
        if self._output_broadcaster_task and not self._output_broadcaster_task.done():
            tasks_to_cancel.append(self._output_broadcaster_task)
            
        for task in tasks_to_cancel:
            task.cancel()
            
        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            
        self.logger.debug(f"Stopped merger/broadcaster tasks for processor {self.processor_id}")

    def register_input_pipe(self, pipe: PipeInterface) -> None:
        """
        Register an input pipe for merging. This pipe's data will be merged into the main input pipe.
        Can be called dynamically even after the processor has started running.
        
        Args:
            pipe: The input pipe to register for merging
        """
        # If this is the first input pipe and we don't have a main input pipe, make it the main one
        if self.input_pipe is ... and not self._registered_input_pipes:
            self.input_pipe = pipe
            return
            
        # Add to registered input pipes for merging
        async def _register():
            async with self._pipe_registration_lock:
                self._registered_input_pipes.add(pipe)
            # Start merger/broadcaster if not already running
            await self._start_merger_broadcaster_if_needed()
        
        # If we're in an async context, run immediately, otherwise store for later
        try:
            loop = asyncio.get_running_loop()
            asyncio.create_task(_register())
        except RuntimeError:
            # No event loop running, store for later - merger will pick it up when started
            self._registered_input_pipes.add(pipe)

    def register_output_pipe(self, pipe: PipeInterface) -> None:
        """
        Register an output pipe for broadcasting. Data from the main output pipe will be copied to this pipe.
        Can be called dynamically even after the processor has started running.
        
        Args:
            pipe: The output pipe to register for broadcasting
        """
        # If this is the first output pipe and we don't have a main output pipe, make it the main one
        if self.output_pipe is ... and not self._registered_output_pipes:
            self.output_pipe = pipe
            return
            
        # Add to registered output pipes for broadcasting
        async def _register():
            async with self._pipe_registration_lock:
                self._registered_output_pipes.add(pipe)
            # Start merger/broadcaster if not already running
            await self._start_merger_broadcaster_if_needed()
        
        # If we're in an async context, run immediately, otherwise store for later
        try:
            loop = asyncio.get_running_loop()
            asyncio.create_task(_register())
        except RuntimeError:
            # No event loop running, store for later - broadcaster will pick it up when started
            self._registered_output_pipes.add(pipe)

    def unregister_input_pipe(self, pipe: PipeInterface) -> None:
        """
        Unregister an input pipe from merging.
        
        Args:
            pipe: The input pipe to unregister
        """
        async def _unregister():
            async with self._pipe_registration_lock:
                self._registered_input_pipes.discard(pipe)
        
        try:
            loop = asyncio.get_running_loop()
            asyncio.create_task(_unregister())
        except RuntimeError:
            # No event loop running, remove directly
            self._registered_input_pipes.discard(pipe)

    def unregister_output_pipe(self, pipe: PipeInterface) -> None:
        """
        Unregister an output pipe from broadcasting.
        
        Args:
            pipe: The output pipe to unregister
        """
        async def _unregister():
            async with self._pipe_registration_lock:
                self._registered_output_pipes.discard(pipe)
        
        try:
            loop = asyncio.get_running_loop()
            asyncio.create_task(_unregister())
        except RuntimeError:
            # No event loop running, remove directly
            self._registered_output_pipes.discard(pipe)
    
    def statistics(self) -> Dict[str, Any]:
        """Get performance and status statistics for the processor"""
        stats = self._statistics.model_dump()
        # Add pipe registration info
        stats['registered_input_pipes'] = len(self._registered_input_pipes)
        stats['registered_output_pipes'] = len(self._registered_output_pipes)
        stats['merger_broadcaster_running'] = self._merger_broadcaster_running
        return stats
    
