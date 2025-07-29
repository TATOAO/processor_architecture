"""
Processor implementations for the v2 processing system.

Provides base classes for creating processors that work with the pipe-based DAG system.
"""

import asyncio
import uuid
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Callable, AsyncGenerator
from datetime import datetime

from .interfaces import ProcessorInterface, ProcessorState, PipeInterface
from .pipe import Pipe, PipeFactory, PipeType
from .errors import ProcessorError, PipeError


class BaseProcessor(ProcessorInterface):
    """
    Base processor implementation with pipe management and lifecycle hooks.
    
    Subclasses should implement the process_data method to define their processing logic.
    """
    
    def __init__(self, processor_id: Optional[str] = None, **kwargs):
        self._processor_id = processor_id or str(uuid.uuid4())
        self._input_pipes: Dict[str, PipeInterface] = {}
        self._output_pipes: Dict[str, PipeInterface] = {}
        self._state = ProcessorState.IDLE
        self._state_lock = threading.RLock()
        
        # Metrics and monitoring
        self.created_at = datetime.now()
        self.processing_start_time: Optional[datetime] = None
        self.processing_end_time: Optional[datetime] = None
        self.total_items_processed = 0
        self.total_processing_time = 0.0
        self.error_count = 0
        self.last_error: Optional[Exception] = None
        
        # Configuration
        self.config = kwargs
        self.max_retries = kwargs.get('max_retries', 3)
        self.timeout = kwargs.get('timeout', None)
        
        # Callbacks and hooks
        self.on_start_callbacks: List[Callable] = []
        self.on_complete_callbacks: List[Callable] = []
        self.on_error_callbacks: List[Callable] = []
        
    @property
    def processor_id(self) -> str:
        return self._processor_id
        
    @property
    def input_pipes(self) -> Dict[str, PipeInterface]:
        return self._input_pipes
        
    @property
    def output_pipes(self) -> Dict[str, PipeInterface]:
        return self._output_pipes
        
    @property
    def state(self) -> ProcessorState:
        with self._state_lock:
            return self._state
            
    def _set_state(self, state: ProcessorState) -> None:
        """Thread-safe state change"""
        with self._state_lock:
            old_state = self._state
            self._state = state
            self._on_state_change(old_state, state)
            
    def _on_state_change(self, old_state: ProcessorState, new_state: ProcessorState) -> None:
        """Called when processor state changes"""
        if new_state == ProcessorState.RUNNING:
            self.processing_start_time = datetime.now()
            for callback in self.on_start_callbacks:
                try:
                    callback(self)
                except Exception:
                    pass  # Don't let callback errors affect processing
                    
        elif new_state == ProcessorState.COMPLETED:
            self.processing_end_time = datetime.now()
            if self.processing_start_time:
                self.total_processing_time += (self.processing_end_time - self.processing_start_time).total_seconds()
            for callback in self.on_complete_callbacks:
                try:
                    callback(self)
                except Exception:
                    pass
                    
        elif new_state == ProcessorState.ERROR:
            for callback in self.on_error_callbacks:
                try:
                    callback(self)
                except Exception:
                    pass
                    
    def register_input_pipe(self, name: str, pipe: PipeInterface) -> None:
        """Register an input pipe"""
        self._input_pipes[name] = pipe
        
    def register_output_pipe(self, name: str, pipe: PipeInterface) -> None:
        """Register an output pipe"""
        self._output_pipes[name] = pipe
        
    def create_default_pipes(self, input_pipe_names: List[str] = None, 
                           output_pipe_names: List[str] = None) -> None:
        """Create default input and output pipes"""
        if input_pipe_names is None:
            input_pipe_names = ["input"]
        if output_pipe_names is None:
            output_pipe_names = ["output"]
            
        for name in input_pipe_names:
            pipe = PipeFactory.create_pipe(PipeType.INPUT)
            self.register_input_pipe(name, pipe)
            
        for name in output_pipe_names:
            pipe = PipeFactory.create_pipe(PipeType.OUTPUT)
            self.register_output_pipe(name, pipe)
            
    async def initialize(self) -> None:
        """Initialize the processor before processing starts"""
        self._set_state(ProcessorState.IDLE)
        await self._initialize_impl()
        
    async def _initialize_impl(self) -> None:
        """Override this method for custom initialization logic"""
        pass
        
    async def process(self) -> None:
        """Main processing method - reads from input pipes, processes, writes to output pipes"""
        try:
            self._set_state(ProcessorState.RUNNING)
            
            # Process data from input pipes
            await self._process_loop()
            
            self._set_state(ProcessorState.COMPLETED)
            
        except Exception as e:
            self.error_count += 1
            self.last_error = e
            self._set_state(ProcessorState.ERROR)
            raise ProcessorError(
                f"Error in processor {self.processor_id}: {str(e)}",
                self.processor_id,
                context={"config": self.config},
                cause=e
            )
            
    async def _process_loop(self) -> None:
        """Main processing loop - override for custom behavior"""
        if not self._input_pipes:
            # No input pipes - process once
            await self._process_single()
            return
            
        # Process data from all input pipes
        while True:
            # Check if all input pipes are empty and closed
            all_closed_and_empty = True
            for pipe in self._input_pipes.values():
                if not pipe.is_empty() or pipe.state != "closed":
                    all_closed_and_empty = False
                    break
                    
            if all_closed_and_empty:
                break
                
            # Get data from input pipes
            input_data = {}
            has_data = False
            
            for name, pipe in self._input_pipes.items():
                try:
                    if not pipe.is_empty():
                        data = await pipe.aget(timeout=0.1)  # Short timeout
                        input_data[name] = data
                        has_data = True
                except:
                    # No data available from this pipe
                    continue
                    
            if has_data:
                # Process the data
                output_data = await self.process_data(input_data)
                self.total_items_processed += 1
                
                # Send to output pipes
                await self._send_output(output_data)
            else:
                # No data available, small delay to avoid busy waiting
                await asyncio.sleep(0.01)
                
    async def _process_single(self) -> None:
        """Process once without input data"""
        output_data = await self.process_data({})
        self.total_items_processed += 1
        await self._send_output(output_data)
        
    async def _send_output(self, output_data: Any) -> None:
        """Send output data to output pipes"""
        if not self._output_pipes:
            return
            
        if isinstance(output_data, dict):
            # Send to specific pipes by name
            for name, data in output_data.items():
                if name in self._output_pipes:
                    await self._output_pipes[name].aput(data)
        else:
            # Send to all output pipes
            for pipe in self._output_pipes.values():
                await pipe.aput(output_data)
                
    @abstractmethod
    async def process_data(self, input_data: Dict[str, Any]) -> Any:
        """
        Process input data and return output data.
        
        Args:
            input_data: Dictionary of data from input pipes (pipe_name -> data)
            
        Returns:
            Output data to send to output pipes
        """
        pass
        
    async def cleanup(self) -> None:
        """Cleanup after processing is complete"""
        # Close all output pipes
        for pipe in self._output_pipes.values():
            pipe.close()
            
        await self._cleanup_impl()
        
    async def _cleanup_impl(self) -> None:
        """Override this method for custom cleanup logic"""
        pass
        
    def get_metrics(self) -> Dict[str, Any]:
        """Get performance and status metrics"""
        now = datetime.now()
        uptime = (now - self.created_at).total_seconds()
        
        metrics = {
            "processor_id": self.processor_id,
            "state": self.state.value,
            "uptime_seconds": uptime,
            "total_items_processed": self.total_items_processed,
            "total_processing_time_seconds": self.total_processing_time,
            "error_count": self.error_count,
            "last_error": str(self.last_error) if self.last_error else None,
            "average_processing_time": (
                self.total_processing_time / self.total_items_processed 
                if self.total_items_processed > 0 else 0
            ),
            "throughput_items_per_second": (
                self.total_items_processed / uptime if uptime > 0 else 0
            ),
            "input_pipes": {name: pipe.get_status() for name, pipe in self._input_pipes.items()},
            "output_pipes": {name: pipe.get_status() for name, pipe in self._output_pipes.items()},
            "config": self.config
        }
        
        if self.processing_start_time:
            metrics["processing_start_time"] = self.processing_start_time.isoformat()
        if self.processing_end_time:
            metrics["processing_end_time"] = self.processing_end_time.isoformat()
            
        return metrics
        
    def add_callback(self, event: str, callback: Callable) -> None:
        """Add event callback"""
        if event == "start":
            self.on_start_callbacks.append(callback)
        elif event == "complete":
            self.on_complete_callbacks.append(callback)
        elif event == "error":
            self.on_error_callbacks.append(callback)


class AsyncProcessor(BaseProcessor):
    """
    Async processor that can handle streaming data and parallel processing.
    """
    
    def __init__(self, processor_id: Optional[str] = None, max_concurrent: int = 10, **kwargs):
        super().__init__(processor_id, **kwargs)
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._active_tasks: List[asyncio.Task] = []
        
    async def _process_loop(self) -> None:
        """Enhanced processing loop with concurrency control"""
        if not self._input_pipes:
            await self._process_single()
            return
            
        # Create tasks for concurrent processing
        tasks = []
        
        while True:
            # Check if all input pipes are empty and closed
            all_closed_and_empty = True
            for pipe in self._input_pipes.values():
                if not pipe.is_empty() or pipe.state != "closed":
                    all_closed_and_empty = False
                    break
                    
            if all_closed_and_empty and not tasks:
                break
                
            # Process available data concurrently
            input_data = {}
            has_data = False
            
            for name, pipe in self._input_pipes.items():
                try:
                    if not pipe.is_empty():
                        data = await pipe.aget(timeout=0.1)
                        input_data[name] = data
                        has_data = True
                except:
                    continue
                    
            if has_data and len(tasks) < self.max_concurrent:
                # Create new processing task
                task = asyncio.create_task(self._process_concurrent(input_data))
                tasks.append(task)
                
            # Clean up completed tasks
            tasks = [task for task in tasks if not task.done()]
            
            # Small delay if no data or at max concurrency
            if not has_data or len(tasks) >= self.max_concurrent:
                await asyncio.sleep(0.01)
                
        # Wait for remaining tasks to complete
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            
    async def _process_concurrent(self, input_data: Dict[str, Any]) -> None:
        """Process data with concurrency control"""
        async with self._semaphore:
            try:
                output_data = await self.process_data(input_data)
                self.total_items_processed += 1
                await self._send_output(output_data)
            except Exception as e:
                self.error_count += 1
                self.last_error = e
                raise


class SimpleProcessor(BaseProcessor):
    """
    Simple processor that takes a processing function.
    Useful for creating quick processors without subclassing.
    """
    
    def __init__(self, process_func: Callable, processor_id: Optional[str] = None, **kwargs):
        super().__init__(processor_id, **kwargs)
        self.process_func = process_func
        
    async def process_data(self, input_data: Dict[str, Any]) -> Any:
        """Process data using the provided function"""
        if asyncio.iscoroutinefunction(self.process_func):
            return await self.process_func(input_data)
        else:
            return self.process_func(input_data)


class GeneratorProcessor(BaseProcessor):
    """
    Processor that yields multiple outputs from a single input.
    """
    
    async def _process_loop(self) -> None:
        """Process loop that handles generator outputs"""
        if not self._input_pipes:
            async for output in self._process_generator({}):
                await self._send_output(output)
            return
            
        while True:
            all_closed_and_empty = True
            for pipe in self._input_pipes.values():
                if not pipe.is_empty() or pipe.state != "closed":
                    all_closed_and_empty = False
                    break
                    
            if all_closed_and_empty:
                break
                
            input_data = {}
            has_data = False
            
            for name, pipe in self._input_pipes.items():
                try:
                    if not pipe.is_empty():
                        data = await pipe.aget(timeout=0.1)
                        input_data[name] = data
                        has_data = True
                except:
                    continue
                    
            if has_data:
                async for output in self._process_generator(input_data):
                    await self._send_output(output)
                    self.total_items_processed += 1
            else:
                await asyncio.sleep(0.01)
                
    async def _process_generator(self, input_data: Dict[str, Any]) -> AsyncGenerator[Any, None]:
        """Override this to implement generator processing"""
        # Default implementation - just pass through
        if input_data:
            for key, value in input_data.items():
                yield value
                
    async def process_data(self, input_data: Dict[str, Any]) -> Any:
        """Not used in generator processor"""
        return None 