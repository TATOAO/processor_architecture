from typing import List, Any, get_origin, get_args, Annotated, AsyncGenerator
from processor import Processor, AsyncProcessor
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

    def run(self, data: Any) -> Any:
        for processor in self.processors:
            data = processor.process(data)
        return data


class AsyncPipeline(Pipeline):
    def __init__(self, processors: List[AsyncProcessor], max_concurrent_tasks: int = 10):
        super().__init__(processors)
        self.max_concurrent_tasks = max_concurrent_tasks
        self._semaphore = asyncio.Semaphore(max_concurrent_tasks)

    async def run(self, input_data: Any) -> List[Any]:
        if not self.processors:
            return [input_data]
        
        # Create the initial stream
        stream = self._to_async_stream([input_data])
        
        # If only one processor, process normally
        if len(self.processors) == 1:
            results = []
            async for item in self.processors[0].process(stream):
                results.append(item)
            return results
        
        # For multiple processors, use concurrent streaming
        return await self._run_concurrent_streaming(stream)
    
    async def _run_concurrent_streaming(self, initial_stream: AsyncGenerator[Any, None]) -> List[Any]:
        """Run with true concurrent streaming - each intermediate result spawns a new task"""
        # Process through first processor
        first_processor = self.processors[0]
        remaining_processors = self.processors[1:]
        
        # Create tasks for each item produced by the first processor
        tasks = []
        async for item in first_processor.process(initial_stream):
            # Create a task to process this item through remaining processors
            # Use semaphore to limit concurrent tasks
            task = asyncio.create_task(
                self._process_item_with_semaphore(item, remaining_processors)
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
        
        return results
    
    async def _process_item_with_semaphore(self, item: Any, processors: List[AsyncProcessor]) -> List[Any]:
        """Process an item through processors with semaphore-controlled concurrency"""
        async with self._semaphore:
            return await self._process_through_remaining_processors(item, processors)
    
    async def _process_through_remaining_processors(self, item: Any, processors: List[AsyncProcessor]) -> List[Any]:
        """Process an item through the remaining processors"""
        if not processors:
            return [item]
        
        # Create a stream with this single item
        stream = self._to_async_stream([item])
        
        # Process through remaining processors
        for processor in processors:
            stream = processor.process(stream)
        
        # Collect results
        results = []
        async for result in stream:
            results.append(result)
        
        return results

    async def _to_async_stream(self, items: List[Any]) -> AsyncGenerator[Any, None]:
        for item in items:
            yield item