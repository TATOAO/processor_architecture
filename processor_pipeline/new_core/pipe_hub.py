"""
Centralized pipe hub for managing all pipe connections and data flow.

This module provides a centralized hub that manages merging and broadcasting
of data between processors, eliminating the need for individual processors
to handle their own pipe management.
"""

import asyncio
import traceback
import uuid
from typing import Any, Dict, List, Optional, Set
from collections import defaultdict
from loguru import logger
from .core_interfaces import PipeHubInterface, PipeInterface
from .pipe import BufferPipe


class ProcessorRegistration:
    """Information about a registered processor"""
    
    def __init__(self, processor_id: str, input_pipes: List[str], output_pipe: str):
        self.processor_id = processor_id
        self.input_pipes = input_pipes
        self.output_pipe = output_pipe
        self.merged_input_pipe: Optional[PipeInterface] = None
        self.merger_tasks: Set[asyncio.Task] = set()
        self.broadcaster_tasks: Set[asyncio.Task] = set()


class PipeHub(PipeHubInterface):
    """
    Centralized pipe hub that manages all pipe connections and data flow.
    
    The hub handles:
    - Merging multiple input pipes for processors
    - Broadcasting output from processors to multiple destinations
    - Managing all background tasks for data flow
    - Providing centralized statistics and monitoring
    """
    
    def __init__(self, hub_id: Optional[str] = None, logger=None):
        self.hub_id = hub_id or f"pipe_hub_{str(uuid.uuid4())[:8]}"
        self.logger = logger or logger.bind(name=f"PipeHub-{self.hub_id}")
        
        # Registry of named pipes
        self._pipes: Dict[str, PipeInterface] = {}
        
        # Registry of processors and their pipe configurations
        self._processors: Dict[str, ProcessorRegistration] = {}
        
        # Hub state
        self._running = False
        self._hub_lock = asyncio.Lock()
        
        # Background tasks for managing data flow
        self._management_tasks: Set[asyncio.Task] = set()
        
        # Statistics
        self._stats = {
            'processors_registered': 0,
            'pipes_registered': 0,
            'merger_tasks_active': 0,
            'broadcaster_tasks_active': 0,
            'hub_running': False
        }
    
    async def register_pipe(self, pipe_name: str, pipe: PipeInterface) -> None:
        """Register a named pipe in the hub"""
        async with self._hub_lock:
            self._pipes[pipe_name] = pipe
            self._stats['pipes_registered'] = len(self._pipes)
            self.logger.debug(f"Registered pipe: {pipe_name}")
    
    async def unregister_pipe(self, pipe_name: str) -> None:
        """Unregister a named pipe from the hub"""
        async with self._hub_lock:
            if pipe_name in self._pipes:
                pipe = self._pipes.pop(pipe_name)
                pipe.close()
                self._stats['pipes_registered'] = len(self._pipes)
                self.logger.debug(f"Unregistered pipe: {pipe_name}")
    
    def get_pipe(self, pipe_name: str) -> Optional[PipeInterface]:
        """Get a pipe by name"""
        return self._pipes.get(pipe_name)
    
    async def register_processor(self, 
                               processor_id: str,
                               input_pipes: List[str], 
                               output_pipe: str) -> None:
        """Register a processor with its input and output pipe names"""
        async with self._hub_lock:
            # Create processor registration
            registration = ProcessorRegistration(processor_id, input_pipes, output_pipe)
            
            # Create merged input pipe for the processor if it has multiple inputs
            if len(input_pipes) > 1:
                registration.merged_input_pipe = BufferPipe(f"{processor_id}_merged_input")
                await self.register_pipe(f"{processor_id}_merged_input", registration.merged_input_pipe)
            elif len(input_pipes) == 1:
                # For single input, use the pipe directly
                registration.merged_input_pipe = self._pipes.get(input_pipes[0])
            
            self._processors[processor_id] = registration
            self._stats['processors_registered'] = len(self._processors)
            
            # Start merger and broadcaster tasks if hub is running
            if self._running:
                await self._start_processor_tasks(registration)
            
            self.logger.debug(f"Registered processor: {processor_id} with inputs: {input_pipes}, output: {output_pipe}")
    
    async def unregister_processor(self, processor_id: str) -> None:
        """Unregister a processor from the hub"""
        async with self._hub_lock:
            if processor_id not in self._processors:
                return
            
            registration = self._processors.pop(processor_id)
            
            # Stop all tasks for this processor
            await self._stop_processor_tasks(registration)
            
            # Clean up merged input pipe if it was created
            if len(registration.input_pipes) > 1 and registration.merged_input_pipe:
                await self.unregister_pipe(f"{processor_id}_merged_input")
            
            self._stats['processors_registered'] = len(self._processors)
            self.logger.debug(f"Unregistered processor: {processor_id}")
    
    async def get_merged_input_pipe(self, processor_id: str) -> Optional[PipeInterface]:
        """Get the merged input pipe for a processor"""
        registration = self._processors.get(processor_id)
        return registration.merged_input_pipe if registration else None
    
    async def get_output_pipe(self, processor_id: str) -> Optional[PipeInterface]:
        """Get the output pipe for a processor"""
        registration = self._processors.get(processor_id)
        if registration:
            return self._pipes.get(registration.output_pipe)
        return None
    
    async def start_hub(self) -> None:
        """Start the hub and all its background tasks"""
        async with self._hub_lock:
            if self._running:
                return
            
            self._running = True
            self._stats['hub_running'] = True
            
            # Start tasks for all registered processors
            for registration in self._processors.values():
                await self._start_processor_tasks(registration)
            
            self.logger.info(f"PipeHub {self.hub_id} started")
    
    async def stop_hub(self) -> None:
        """Stop the hub and clean up all background tasks"""
        async with self._hub_lock:
            if not self._running:
                return
            
            self._running = False
            self._stats['hub_running'] = False
            
            # Stop all processor tasks
            for registration in self._processors.values():
                await self._stop_processor_tasks(registration)
            
            # Cancel any remaining management tasks
            for task in self._management_tasks:
                if not task.done():
                    task.cancel()
            
            if self._management_tasks:
                await asyncio.gather(*self._management_tasks, return_exceptions=True)
            
            self._management_tasks.clear()
            self.logger.info(f"PipeHub {self.hub_id} stopped")
    
    async def _start_processor_tasks(self, registration: ProcessorRegistration) -> None:
        """Start merger and broadcaster tasks for a processor"""
        # Start input merger tasks if processor has multiple inputs
        if len(registration.input_pipes) > 1 and registration.merged_input_pipe:
            for input_pipe_name in registration.input_pipes:
                input_pipe = self._pipes.get(input_pipe_name)
                if input_pipe:
                    task = asyncio.create_task(
                        self._merge_input_pipe(input_pipe, registration.merged_input_pipe, 
                                             f"{registration.processor_id}_{input_pipe_name}")
                    )
                    registration.merger_tasks.add(task)
                    self._management_tasks.add(task)
        
        # Start broadcaster task for output if there are multiple consumers
        # This will be implemented when we have multiple consumers for a single output
        # For now, processors write directly to their output pipe
        
        self._update_task_stats()
    
    async def _stop_processor_tasks(self, registration: ProcessorRegistration) -> None:
        """Stop all tasks for a processor"""
        # Cancel merger tasks
        for task in registration.merger_tasks:
            if not task.done():
                task.cancel()
        
        if registration.merger_tasks:
            await asyncio.gather(*registration.merger_tasks, return_exceptions=True)
        
        # Cancel broadcaster tasks
        for task in registration.broadcaster_tasks:
            if not task.done():
                task.cancel()
        
        if registration.broadcaster_tasks:
            await asyncio.gather(*registration.broadcaster_tasks, return_exceptions=True)
        
        # Remove from management tasks
        self._management_tasks -= registration.merger_tasks
        self._management_tasks -= registration.broadcaster_tasks
        
        registration.merger_tasks.clear()
        registration.broadcaster_tasks.clear()
        
        self._update_task_stats()
    
    async def _merge_input_pipe(self, source_pipe: PipeInterface, 
                              target_pipe: PipeInterface, 
                              merger_id: str) -> None:
        """Merge data from a source pipe into a target pipe"""
        try:
            self.logger.debug(f"Starting input merger: {merger_id}")
            async for message_id, data in source_pipe:
                await target_pipe.put(data)
        except asyncio.CancelledError:
            self.logger.debug(f"Input merger cancelled: {merger_id}")
        except Exception as e:
            self.logger.error(f"Error in input merger {merger_id}: {e}")
            self.logger.error(traceback.format_exc())
    
    async def _broadcast_output_pipe(self, source_pipe: PipeInterface, 
                                   target_pipes: List[PipeInterface], 
                                   broadcaster_id: str) -> None:
        """Broadcast data from a source pipe to multiple target pipes"""
        try:
            self.logger.debug(f"Starting output broadcaster: {broadcaster_id}")
            # Use peek mechanism to avoid consuming from source
            observer_id = await source_pipe.register_observer(f"broadcaster_{broadcaster_id}")
            
            async for message_id, data in source_pipe.peek_aiter(observer_id):
                # Broadcast to all target pipes
                broadcast_tasks = []
                for target_pipe in target_pipes:
                    broadcast_tasks.append(asyncio.create_task(target_pipe.put(data)))
                
                if broadcast_tasks:
                    await asyncio.gather(*broadcast_tasks, return_exceptions=True)
                    
        except asyncio.CancelledError:
            self.logger.debug(f"Output broadcaster cancelled: {broadcaster_id}")
        except Exception as e:
            self.logger.error(f"Error in output broadcaster {broadcaster_id}: {e}")
            self.logger.error(traceback.format_exc())
        finally:
            # Clean up observer
            try:
                await source_pipe.unregister_observer(observer_id)
            except:
                pass
    
    def _update_task_stats(self) -> None:
        """Update task statistics"""
        merger_count = sum(len(reg.merger_tasks) for reg in self._processors.values())
        broadcaster_count = sum(len(reg.broadcaster_tasks) for reg in self._processors.values())
        
        self._stats['merger_tasks_active'] = merger_count
        self._stats['broadcaster_tasks_active'] = broadcaster_count
    
    def hub_statistics(self) -> Dict[str, Any]:
        """Get hub statistics including all registered processors and pipes"""
        # Update task stats
        self._update_task_stats()
        
        # Build detailed statistics
        stats = self._stats.copy()
        stats.update({
            'hub_id': self.hub_id,
            'processors': {
                proc_id: {
                    'input_pipes': reg.input_pipes,
                    'output_pipe': reg.output_pipe,
                    'has_merged_input': reg.merged_input_pipe is not None,
                    'merger_tasks': len(reg.merger_tasks),
                    'broadcaster_tasks': len(reg.broadcaster_tasks)
                }
                for proc_id, reg in self._processors.items()
            },
            'pipes': {
                pipe_name: {
                    'size': pipe.size(),
                    'is_empty': pipe.is_empty(),
                    'statistics': pipe.statistics()
                }
                for pipe_name, pipe in self._pipes.items()
            }
        })
        
        return stats