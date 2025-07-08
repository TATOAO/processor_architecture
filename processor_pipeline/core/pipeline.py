from typing import List, Any, get_origin, get_args, Annotated, AsyncGenerator, Optional, Callable
from .helper import generate_execution_id, default_callback
from .processor import Processor, AsyncProcessor
import asyncio

# --- Type Checking Utility ---
def match_types(output_type: Any, input_type: Any) -> bool:
    def resolve(t):
        if get_origin(t) is Annotated:
            return get_args(t)[0]
        return t

    return resolve(output_type) == resolve(input_type)

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
        
        # Create the initial stream
        stream = self._to_async_stream([input_data])
        
        # If only one processor, process normally
        if len(self.processors) == 1:
            results = []
            input_items = [input_data]  # Track input for callback
            step_index = 0
            async for item in self.processors[0].process(stream):
                results.append(item)
                # Call callback for each output item
                callback(self.processors[0], input_items, item, execution_id, step_index, *args, **kwargs)
            # Call after_process ONCE after all items are processed
            await self.processors[0].after_process(input_items, results, execution_id, step_index, *args, **kwargs)
            return results
        
        # For multiple processors, use concurrent streaming
        results = await self._run_concurrent_streaming_with_callbacks(stream, execution_id, callback, *args, **kwargs)
        return results
    
    async def _run_concurrent_streaming_with_callbacks(self, initial_stream: AsyncGenerator[Any, None], 
                                                      execution_id: str, callback: Optional[Callable],
                                                      *args, **kwargs) -> List[Any]:
        """Run with concurrent streaming and callback support"""
        # Process through first processor with callbacks
        first_processor = self.processors[0]
        remaining_processors = self.processors[1:]
        
        # Collect intermediate results and call callbacks
        intermediate_results = []
        step_index = 0
        async for item in first_processor.process(initial_stream):
            intermediate_results.append(item)
            if callback:
                callback(first_processor, None, item, execution_id, step_index, *args, **kwargs)
        
        # Create tasks for each item produced by the first processor
        tasks = []
        for item in intermediate_results:
            task = asyncio.create_task(
                self._process_item_with_semaphore_and_callbacks(
                    item, remaining_processors, execution_id, callback, step_index + 1
                )
            )
            tasks.append(task)
        
        # Wait for all tasks to complete
        results = []
        if tasks:
            completed_results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in completed_results:
                if isinstance(result, Exception):
                    raise result
                if isinstance(result, list):
                    results.extend(result)
                else:
                    results.append(result)
        
        # Call after_process ONCE after all downstream tasks are complete
        await first_processor.after_process(None, intermediate_results, execution_id, step_index, *args, **kwargs)
        
        return results
    
    async def _process_item_with_semaphore_and_callbacks(self, item: Any, processors: List[AsyncProcessor], 
                                                        execution_id: str, callback: Optional[Callable],
                                                        start_step_index: int, *args, **kwargs) -> List[Any]:
        """Process an item through processors with semaphore-controlled concurrency and callbacks"""
        async with self._semaphore:
            return await self._process_through_remaining_processors_with_callbacks(
                item, processors, execution_id, callback, start_step_index, 
                *args, **kwargs
            )
    
    async def _process_through_remaining_processors_with_callbacks(self, item: Any, processors: List[AsyncProcessor],
                                                                  execution_id: str, callback: Optional[Callable],
                                                                  start_step_index: int, *args, **kwargs) -> List[Any]:
        """Process an item through the remaining processors with callbacks"""
        if not processors:
            return [item]
        
        # Create a stream with this single item
        stream = self._to_async_stream([item])
        current_input = item
        
        # Process through remaining processors
        for i, processor in enumerate(processors):
            step_index = start_step_index + i
            current_stream_items = []
            # Collect all items from current stream
            async for stream_item in stream:
                current_stream_items.append(stream_item)
            # Process through current processor
            stream = self._to_async_stream(current_stream_items)
            processed_items = []
            async for processed_item in processor.process(stream):
                processed_items.append(processed_item)
                if callback:
                    callback(processor, current_input, processed_item, execution_id, step_index, *args, **kwargs)
            # Update stream and input for next iteration
            stream = self._to_async_stream(processed_items)
            # Call after_process ONCE after all downstream processing is done
            # But only after all further processors have finished
            # So, defer after_process until after all downstream processing is done
            current_input = processed_items
        # After all processors are done, call after_process for the last one
        # (This is only for the last processor in the chain)
        if processors:
            await processors[-1].after_process(current_input, current_input, execution_id, start_step_index + len(processors) - 1, *args, **kwargs)
        # Collect final results
        results = []
        async for result in stream:
            results.append(result)
        return results

    async def _to_async_stream(self, items: List[Any]) -> AsyncGenerator[Any, None]:
        for item in items:
            yield item