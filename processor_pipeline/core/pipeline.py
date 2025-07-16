from typing import List, Any, get_origin, get_args, Annotated, AsyncGenerator, Optional, Callable, Union
from .helper import generate_execution_id, default_callback
from .processor import Processor, AsyncProcessor
import asyncio

# --- Type Checking Utility ---
def match_types(output_type: Any, input_type: Any) -> bool:
    def resolve(t):
        if get_origin(t) is Annotated:
            return get_args(t)[0]
        return t

    resolved_output = resolve(output_type)
    resolved_input = resolve(input_type)

    # If input_type is a Union, check if output_type matches any of its types
    if get_origin(resolved_input) is Union:
        input_args = get_args(resolved_input)
        return any(resolved_output == resolve(arg) for arg in input_args)
    else:
        return resolved_output == resolved_input

class Pipeline:
    def __init__(self, processors: List[Processor]):
        self.processors = processors
        self._check_compatibility()

    def _check_compatibility(self):
        for i in range(len(self.processors) - 1):
            p1, p2 = self.processors[i], self.processors[i + 1]
            out_type = p1.get_meta()["output_type"]
            in_type = p2.get_meta()["input_type"]
            if not match_types(out_type, in_type):
                raise TypeError(
                    f"Incompatible types: '{p1.meta['name']}' outputs {out_type}, "
                    f"but '{p2.meta['name']}' expects {in_type}"
                )

    def run(self, data: Any, execution_id: Optional[str] = None, 
            callback: Optional[Callable] = None, *args, **kwargs) -> Any:
        """
        Run the pipeline with optional callback and execution tracking.
        
        Args:
            data: Input data for the pipeline
            execution_id: Optional execution ID (auto-generated if not provided)
            callback: Optional callback function called after each processor
                     Signature: callback(processor, input_data, output_data, execution_id, step_index)
            output_dir: Directory for saving outputs (used by default callback)
            
        Returns:
            Final output from the pipeline
        """
        if execution_id is None:
            execution_id = generate_execution_id()
        
        if callback is None:
            # Use default callback that saves to JSON
            callback =  default_callback
        
        for step_index, processor in enumerate(self.processors):
            input_data = data
            data = processor.process(data)
            # Call the callback function (per item, if data is a list)
            if isinstance(data, list):
                for item in data:
                    callback(processor, input_data, item, execution_id, step_index, *args, **kwargs)
            else:
                callback(processor, input_data, data, execution_id, step_index, *args, **kwargs)
            # Call after_process ONCE after the processor is done
            processor.after_process(input_data, data, execution_id, step_index, *args, **kwargs)
        
        return data


class AsyncPipeline(Pipeline):
    def __init__(self, processors: List[AsyncProcessor], max_concurrent_tasks: int = 10):
        super().__init__(processors)
        self.max_concurrent_tasks = max_concurrent_tasks
        self._semaphore = asyncio.Semaphore(max_concurrent_tasks)

    async def run(self, input_data: Any, execution_id: Optional[str] = None,
                  callback: Optional[Callable] = None, *args, **kwargs) -> List[Any]:
        """
        Run the async pipeline with optional callback and execution tracking.
        
        Args:
            input_data: Input data for the pipeline
            execution_id: Optional execution ID (auto-generated if not provided)
            callback: Optional callback function called after each processor step
                     Signature: callback(processor, input_data, output_data, execution_id, step_index)
            output_dir: Directory for saving outputs (used by default callback)
            
        Returns:
            List of final outputs from the pipeline
        """
        if execution_id is None:
            execution_id = generate_execution_id()
        
        if callback is None:
            # Use default callback that saves to JSON
            callback = default_callback
        
        if not self.processors:
            return [input_data]
        
        # For true streaming, we'll use a pipe mechanism with queues
        return await self._run_streaming_pipeline(input_data, execution_id, callback, *args, **kwargs)

    async def _run_streaming_pipeline(self, input_data: Any, execution_id: str, 
                                    callback: Callable, *args, **kwargs) -> List[Any]:
        """
        Run the pipeline with true streaming using asyncio.Queue for real-time data flow.
        """
        if len(self.processors) == 1:
            # Single processor case
            stream = self._to_async_stream([input_data])
            results = []
            async for item in self.processors[0].process(stream):
                results.append(item)
                if callback:
                    callback(self.processors[0], input_data, item, execution_id, 0, *args, **kwargs)
            await self.processors[0].after_process(input_data, results, execution_id, 0, *args, **kwargs)
            return results
        
        # Multiple processors - create streaming pipes
        queues = [asyncio.Queue() for _ in range(len(self.processors) + 1)]
        final_results = []
        
        # Start all processor tasks
        tasks = []
        for i, processor in enumerate(self.processors):
            task = asyncio.create_task(
                self._run_processor_with_queue(
                    processor, queues[i], queues[i + 1], 
                    input_data, execution_id, i, callback, *args, **kwargs
                )
            )
            tasks.append(task)
        
        # Feed initial data to first queue
        await queues[0].put(input_data)
        await queues[0].put(None)  # Sentinel to indicate end
        
        # Wait for all processors to complete
        await asyncio.gather(*tasks)
        
        # Collect final results from last queue
        while not queues[-1].empty():
            item = await queues[-1].get()
            if item is not None:
                final_results.append(item)
        
        return final_results

    async def _run_processor_with_queue(self, processor: AsyncProcessor, 
                                      input_queue: asyncio.Queue, output_queue: asyncio.Queue,
                                      input_data: Any, execution_id: str, step_index: int,
                                      callback: Callable, *args, **kwargs):
        """
        Run a single processor with input and output queues for streaming.
        """
        # Create input stream from queue
        async def input_stream():
            while True:
                item = await input_queue.get()
                if item is None:  # Sentinel
                    break
                yield item
        
        # Process items and put them in output queue
        processed_items = []
        async for processed_item in processor.process(input_stream()):
            processed_items.append(processed_item)
            await output_queue.put(processed_item)
            if callback:
                callback(processor, input_data, processed_item, execution_id, step_index, *args, **kwargs)
        
        # Call after_process
        await processor.after_process(input_data, processed_items, execution_id, step_index, *args, **kwargs)
        
        # Put sentinel in output queue
        await output_queue.put(None)

    async def _to_async_stream(self, items: List[Any]) -> AsyncGenerator[Any, None]:
        for item in items:
            yield item

    async def _create_streaming_pipe(self, input_data: Any) -> AsyncGenerator[Any, None]:
        """
        Create a streaming pipe that allows immediate flow between processors.
        This is a more advanced implementation for true streaming.
        """
        # For now, we'll use the simpler approach above
        # This method can be enhanced later for more complex streaming scenarios
        pass

    async def astream(self, input_data: Any, execution_id: Optional[str] = None,
                     callback: Optional[Callable] = None, *args, **kwargs) -> AsyncGenerator[Any, None]:
        """
        Async generator that yields each item as soon as it is produced by the last processor.
        Args:
            input_data: Input data for the pipeline
            execution_id: Optional execution ID (auto-generated if not provided)
            callback: Optional callback function called after each processor step
                     Signature: callback(processor, input_data, output_data, execution_id, step_index)
            output_dir: Directory for saving outputs (used by default callback)
        Yields:
            Each item as soon as it is produced by the last processor.
        """
        if execution_id is None:
            execution_id = generate_execution_id()
        if callback is None:
            callback = default_callback
        if not self.processors:
            return
        if len(self.processors) == 1:
            # Single processor: yield each item as processed
            stream = self._to_async_stream([input_data])
            async for item in self.processors[0].process(stream):
                callback(self.processors[0], input_data, item, execution_id, 0, *args, **kwargs)
                yield item
            await self.processors[0].after_process(input_data, None, execution_id, 0, *args, **kwargs)
            return
        # Multiple processors: use queues to connect them
        queues = [asyncio.Queue() for _ in range(len(self.processors) + 1)]
        # Start all processor tasks
        tasks = []
        for i, processor in enumerate(self.processors):
            async def run_proc(proc, in_q, out_q, step_idx):
                async def input_stream():
                    while True:
                        item = await in_q.get()
                        if item is None:
                            break
                        yield item
                processed_items = []
                async for processed_item in proc.process(input_stream()):
                    processed_items.append(processed_item)
                    await out_q.put(processed_item)
                    callback(proc, None, processed_item, execution_id, step_idx, *args, **kwargs)
                await proc.after_process(None, processed_items, execution_id, step_idx, *args, **kwargs)
                await out_q.put(None)
            tasks.append(asyncio.create_task(run_proc(processor, queues[i], queues[i+1], i)))
        # Feed initial data to first queue
        await queues[0].put(input_data)
        await queues[0].put(None)
        # Yield from the last queue as soon as items are available
        while True:
            item = await queues[-1].get()
            if item is None:
                break
            yield item
        # Wait for all processors to finish
        await asyncio.gather(*tasks)