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
        
        # For multiple processors, process sequentially to ensure proper after_process timing
        current_stream = stream
        current_input = [input_data]
        
        for step_index, processor in enumerate(self.processors):
            # Collect all items from current stream
            current_items = []
            async for item in current_stream:
                current_items.append(item)
            
            if not current_items:
                break
            
            # Process through current processor
            processor_stream = self._to_async_stream(current_items)
            processed_items = []
            
            async for processed_item in processor.process(processor_stream):
                processed_items.append(processed_item)
                # Call callback for each output item
                if callback:
                    callback(processor, current_input, processed_item, execution_id, step_index, *args, **kwargs)
            
            # Call after_process ONCE after all items are processed by this processor
            await processor.after_process(current_input, processed_items, execution_id, step_index, *args, **kwargs)
            
            # Update for next iteration
            current_stream = self._to_async_stream(processed_items)
            current_input = processed_items
        
        return current_input

    async def _to_async_stream(self, items: List[Any]) -> AsyncGenerator[Any, None]:
        for item in items:
            yield item