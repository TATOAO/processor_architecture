"""
DAG execution engine for the v2 processing pipeline.

Provides graph construction, validation, topological sorting, and parallel execution.
"""

import asyncio
import uuid
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Set, Tuple
from datetime import datetime

from .interfaces import GraphInterface, ProcessorInterface, MonitorInterface, PipeInterface
from .pipe import Pipe, PipeFactory, PipeType
from .errors import GraphError, ValidationError, ExecutionError
from .monitor import Monitor, EventType, LogLevel
from ..execution.strategies import ErrorStrategy, create_default_error_strategy


class GraphNode:
    """Represents a node in the processing graph"""
    
    def __init__(self, processor: ProcessorInterface):
        self.processor = processor
        self.dependencies: Set[str] = set()  # Processor IDs this node depends on
        self.dependents: Set[str] = set()    # Processor IDs that depend on this node
        self.execution_level = -1            # Level in topological ordering
        
    def add_dependency(self, dependency_id: str) -> None:
        """Add a dependency (this node depends on dependency_id)"""
        self.dependencies.add(dependency_id)
        
    def add_dependent(self, dependent_id: str) -> None:
        """Add a dependent (dependent_id depends on this node)"""
        self.dependents.add(dependent_id)
        
    def remove_dependency(self, dependency_id: str) -> None:
        """Remove a dependency"""
        self.dependencies.discard(dependency_id)
        
    def remove_dependent(self, dependent_id: str) -> None:
        """Remove a dependent"""
        self.dependents.discard(dependent_id)


class ProcessingGraph:
    """
    DAG-based processing graph that manages processors and their connections.
    
    Provides graph construction, validation, and execution capabilities.
    """
    
    def __init__(self, graph_id: Optional[str] = None, error_strategy: Optional[ErrorStrategy] = None):
        self.graph_id = graph_id or str(uuid.uuid4())
        self.nodes: Dict[str, GraphNode] = {}
        self.connections: List[Tuple[str, str, str, str]] = []  # (from_proc, from_output, to_proc, to_input)
        self.pipes: Dict[str, PipeInterface] = {}  # Connection ID -> Pipe
        self.error_strategy = error_strategy or create_default_error_strategy()
        
        # Execution state
        self.is_validated = False
        self.execution_order: List[List[str]] = []  # List of levels, each containing processor IDs
        self.created_at = datetime.now()
        
        # Statistics
        self.total_executions = 0
        self.successful_executions = 0
        self.failed_executions = 0
        
    def add_processor(self, processor: ProcessorInterface) -> None:
        """Add a processor to the graph"""
        if processor.processor_id in self.nodes:
            raise GraphError(f"Processor {processor.processor_id} already exists in graph")
            
        self.nodes[processor.processor_id] = GraphNode(processor)
        self.is_validated = False  # Need to revalidate
        
    def connect(self, from_processor: str, from_output: str, to_processor: str, to_input: str) -> None:
        """Connect processors via pipes"""
        # Validate processors exist
        if from_processor not in self.nodes:
            raise GraphError(f"Source processor {from_processor} not found in graph")
        if to_processor not in self.nodes:
            raise GraphError(f"Target processor {to_processor} not found in graph")
            
        # Create connection
        connection = (from_processor, from_output, to_processor, to_input)
        self.connections.append(connection)
        
        # Update dependencies
        self.nodes[to_processor].add_dependency(from_processor)
        self.nodes[from_processor].add_dependent(to_processor)
        
        # Create pipe for this connection
        connection_id = f"{from_processor}:{from_output}->{to_processor}:{to_input}"
        pipe = PipeFactory.create_pipe(PipeType.BIDIRECTIONAL)
        self.pipes[connection_id] = pipe
        
        # Register pipe with processors
        from_node = self.nodes[from_processor]
        to_node = self.nodes[to_processor]
        
        from_node.processor.register_output_pipe(from_output, pipe)
        to_node.processor.register_input_pipe(to_input, pipe)
        
        self.is_validated = False  # Need to revalidate
        
    def remove_processor(self, processor_id: str) -> None:
        """Remove a processor and all its connections"""
        if processor_id not in self.nodes:
            return
            
        # Remove all connections involving this processor
        self.connections = [
            conn for conn in self.connections 
            if conn[0] != processor_id and conn[2] != processor_id
        ]
        
        # Update dependencies
        node = self.nodes[processor_id]
        for dep_id in node.dependencies:
            if dep_id in self.nodes:
                self.nodes[dep_id].remove_dependent(processor_id)
                
        for dep_id in node.dependents:
            if dep_id in self.nodes:
                self.nodes[dep_id].remove_dependency(processor_id)
                
        # Remove node
        del self.nodes[processor_id]
        self.is_validated = False
        
    def validate(self) -> List[str]:
        """Validate the graph structure"""
        errors = []
        
        # Check for cycles
        if self._has_cycle():
            errors.append("Graph contains cycles")
            
        # Check for isolated nodes (no connections)
        isolated_nodes = self._find_isolated_nodes()
        if isolated_nodes:
            errors.append(f"Isolated nodes found: {isolated_nodes}")
            
        # Validate pipe connections
        pipe_errors = self._validate_pipe_connections()
        errors.extend(pipe_errors)
        
        # Check for missing input/output pipes
        missing_pipe_errors = self._validate_processor_pipes()
        errors.extend(missing_pipe_errors)
        
        if not errors:
            self.is_validated = True
            self._compute_execution_order()
            
        return errors
        
    def _has_cycle(self) -> bool:
        """Check if graph has cycles using DFS"""
        white = set(self.nodes.keys())  # Unvisited
        gray = set()   # Currently being processed
        black = set()  # Completely processed
        
        def dfs(node_id: str) -> bool:
            if node_id in gray:
                return True  # Back edge found - cycle detected
            if node_id in black:
                return False
                
            gray.add(node_id)
            white.discard(node_id)
            
            # Visit dependencies
            for dep_id in self.nodes[node_id].dependencies:
                if dfs(dep_id):
                    return True
                    
            gray.remove(node_id)
            black.add(node_id)
            return False
            
        while white:
            if dfs(white.pop()):
                return True
        return False
        
    def _find_isolated_nodes(self) -> List[str]:
        """Find nodes with no connections"""
        isolated = []
        for node_id, node in self.nodes.items():
            if not node.dependencies and not node.dependents:
                isolated.append(node_id)
        return isolated
        
    def _validate_pipe_connections(self) -> List[str]:
        """Validate that all pipe connections are properly set up"""
        errors = []
        
        for from_proc, from_output, to_proc, to_input in self.connections:
            connection_id = f"{from_proc}:{from_output}->{to_proc}:{to_input}"
            
            if connection_id not in self.pipes:
                errors.append(f"Missing pipe for connection {connection_id}")
                continue
                
            # Check if processors have the pipes registered
            from_node = self.nodes.get(from_proc)
            to_node = self.nodes.get(to_proc)
            
            if from_node and from_output not in from_node.processor.output_pipes:
                errors.append(f"Output pipe '{from_output}' not registered in processor {from_proc}")
                
            if to_node and to_input not in to_node.processor.input_pipes:
                errors.append(f"Input pipe '{to_input}' not registered in processor {to_proc}")
                
        return errors
        
    def _validate_processor_pipes(self) -> List[str]:
        """Validate that processors have required pipes"""
        errors = []
        
        for node_id, node in self.nodes.items():
            processor = node.processor
            
            # Check if processor has input pipes when it has dependencies
            if node.dependencies and not processor.input_pipes:
                errors.append(f"Processor {node_id} has dependencies but no input pipes")
                
            # Check if processor has output pipes when it has dependents
            if node.dependents and not processor.output_pipes:
                errors.append(f"Processor {node_id} has dependents but no output pipes")
                
        return errors
        
    def _compute_execution_order(self) -> None:
        """Compute topological ordering for execution"""
        # Kahn's algorithm for topological sorting
        in_degree = {node_id: len(node.dependencies) for node_id, node in self.nodes.items()}
        queue = deque([node_id for node_id, degree in in_degree.items() if degree == 0])
        
        self.execution_order = []
        current_level = 0
        
        while queue:
            current_level_nodes = list(queue)
            self.execution_order.append(current_level_nodes)
            queue.clear()
            
            # Set execution level for nodes
            for node_id in current_level_nodes:
                self.nodes[node_id].execution_level = current_level
                
                # Reduce in-degree of dependents
                for dependent_id in self.nodes[node_id].dependents:
                    in_degree[dependent_id] -= 1
                    if in_degree[dependent_id] == 0:
                        queue.append(dependent_id)
                        
            current_level += 1
            
    def get_execution_order(self) -> List[List[str]]:
        """Get processors grouped by execution level"""
        if not self.is_validated:
            validation_errors = self.validate()
            if validation_errors:
                raise ValidationError("Graph validation failed", validation_errors)
                
        return self.execution_order.copy()
        
    def get_processor(self, processor_id: str) -> ProcessorInterface:
        """Get a processor by ID"""
        if processor_id not in self.nodes:
            raise GraphError(f"Processor {processor_id} not found in graph")
        return self.nodes[processor_id].processor
        
    def get_all_processors(self) -> Dict[str, ProcessorInterface]:
        """Get all processors in the graph"""
        return {node_id: node.processor for node_id, node in self.nodes.items()}
        
    def get_graph_info(self) -> Dict[str, Any]:
        """Get information about the graph structure"""
        return {
            "graph_id": self.graph_id,
            "total_processors": len(self.nodes),
            "total_connections": len(self.connections),
            "is_validated": self.is_validated,
            "execution_levels": len(self.execution_order) if self.is_validated else 0,
            "total_executions": self.total_executions,
            "successful_executions": self.successful_executions,
            "failed_executions": self.failed_executions,
            "created_at": self.created_at.isoformat(),
            "processors": list(self.nodes.keys()),
            "connections": [
                {"from": f"{conn[0]}:{conn[1]}", "to": f"{conn[2]}:{conn[3]}"}
                for conn in self.connections
            ]
        }


class GraphExecutor:
    """
    Executes processing graphs with monitoring and error handling.
    """
    
    def __init__(self, graph: ProcessingGraph, monitor: Optional[Monitor] = None):
        self.graph = graph
        self.monitor = monitor or Monitor()
        self.execution_id: Optional[str] = None
        self.is_running = False
        self.execution_tasks: Dict[str, asyncio.Task] = {}
        
    async def execute(self, input_data: Dict[str, Any], 
                     monitor: Optional[MonitorInterface] = None) -> Dict[str, Any]:
        """Execute the graph with input data"""
        if self.is_running:
            raise ExecutionError("Graph is already running", [])
            
        # Validate graph first
        validation_errors = self.graph.validate()
        if validation_errors:
            raise ValidationError("Graph validation failed before execution", validation_errors)
            
        self.execution_id = str(uuid.uuid4())
        self.is_running = True
        
        # Use provided monitor or default
        if monitor:
            self.monitor = monitor
            
        self.monitor.start_monitoring()
        
        try:
            # Register all processors with monitor
            for processor in self.graph.get_all_processors().values():
                self.monitor.register_processor(processor)
                
            self.monitor.log_event(EventType.GRAPH_START, None, {
                "execution_id": self.execution_id,
                "graph_id": self.graph.graph_id,
                "total_processors": len(self.graph.nodes)
            })
            
            # Initialize processors
            await self._initialize_processors()
            
            # Feed input data to source processors
            await self._feed_input_data(input_data)
            
            # Execute graph level by level
            result = await self._execute_levels()
            
            # Cleanup processors
            await self._cleanup_processors()
            
            self.graph.successful_executions += 1
            self.monitor.log_event(EventType.GRAPH_COMPLETE, None, {
                "execution_id": self.execution_id,
                "success": True
            })
            
            return result
            
        except Exception as e:
            self.graph.failed_executions += 1
            failed_processors = [
                task_id for task_id, task in self.execution_tasks.items()
                if task.done() and task.exception()
            ]
            
            self.monitor.log_event(EventType.GRAPH_ERROR, None, {
                "execution_id": self.execution_id,
                "error": str(e),
                "failed_processors": failed_processors
            }, LogLevel.ERROR)
            
            raise ExecutionError(f"Graph execution failed: {e}", failed_processors, cause=e)
            
        finally:
            self.is_running = False
            self.graph.total_executions += 1
            self.monitor.stop_monitoring()
            
            # Cancel any remaining tasks
            for task in self.execution_tasks.values():
                if not task.done():
                    task.cancel()
                    
    async def _initialize_processors(self) -> None:
        """Initialize all processors in the graph"""
        for processor in self.graph.get_all_processors().values():
            await processor.initialize()
            
    async def _feed_input_data(self, input_data: Dict[str, Any]) -> None:
        """Feed input data to source processors (those with no dependencies)"""
        execution_order = self.graph.get_execution_order()
        if not execution_order:
            return
            
        source_processors = execution_order[0]  # First level has no dependencies
        
        for processor_id in source_processors:
            processor = self.graph.get_processor(processor_id)
            
            # Feed data to processor's input pipes
            for pipe_name, pipe in processor.input_pipes.items():
                if pipe_name in input_data:
                    await pipe.aput(input_data[pipe_name])
                    
            # If processor has no input pipes, it's a source processor
            if not processor.input_pipes:
                # Create a default input pipe and feed data
                default_pipe = PipeFactory.create_pipe()
                processor.register_input_pipe("input", default_pipe)
                
                # Feed all input data or specific data for this processor
                processor_data = input_data.get(processor_id, input_data)
                await default_pipe.aput(processor_data)
                
    async def _execute_levels(self) -> Dict[str, Any]:
        """Execute processors level by level"""
        execution_order = self.graph.get_execution_order()
        final_results = {}
        
        for level, processor_ids in enumerate(execution_order):
            self.monitor.log_event(EventType.PROCESSOR_START, None, {
                "level": level,
                "processors": processor_ids
            })
            
            # Execute all processors in this level concurrently
            level_tasks = []
            for processor_id in processor_ids:
                processor = self.graph.get_processor(processor_id)
                task = asyncio.create_task(self._execute_processor_with_error_handling(processor))
                self.execution_tasks[processor_id] = task
                level_tasks.append(task)
                
            # Wait for all processors in this level to complete
            results = await asyncio.gather(*level_tasks, return_exceptions=True)
            
            # Check for errors
            for i, result in enumerate(results):
                processor_id = processor_ids[i]
                if isinstance(result, Exception):
                    # Handle error using strategy
                    continue_processing = await self.graph.error_strategy.handle_error(
                        result, processor_id, {"level": level}
                    )
                    if not continue_processing:
                        raise ExecutionError(f"Processing halted due to error in {processor_id}", [processor_id])
                        
            # Collect final results from processors with no dependents (sink processors)
            for processor_id in processor_ids:
                node = self.graph.nodes[processor_id]
                if not node.dependents:  # Sink processor
                    processor = node.processor
                    # Collect output from output pipes
                    for pipe_name, pipe in processor.output_pipes.items():
                        if not pipe.is_empty():
                            try:
                                data = await pipe.aget(timeout=0.1)
                                final_results[f"{processor_id}:{pipe_name}"] = data
                            except:
                                pass  # No data available
                                
        return final_results
        
    async def _execute_processor_with_error_handling(self, processor: ProcessorInterface) -> None:
        """Execute a processor with error handling"""
        try:
            await processor.process()
        except Exception as e:
            # Let the error bubble up to be handled by the level executor
            raise ProcessorError(f"Error in processor {processor.processor_id}", 
                               processor.processor_id, cause=e)
                               
    async def _cleanup_processors(self) -> None:
        """Cleanup all processors after execution"""
        cleanup_tasks = []
        for processor in self.graph.get_all_processors().values():
            task = asyncio.create_task(processor.cleanup())
            cleanup_tasks.append(task)
            
        # Wait for all cleanup tasks
        await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        
    def get_execution_status(self) -> Dict[str, Any]:
        """Get current execution status"""
        return {
            "execution_id": self.execution_id,
            "is_running": self.is_running,
            "graph_id": self.graph.graph_id,
            "active_tasks": len([t for t in self.execution_tasks.values() if not t.done()]),
            "completed_tasks": len([t for t in self.execution_tasks.values() if t.done()]),
            "failed_tasks": len([
                t for t in self.execution_tasks.values() 
                if t.done() and t.exception()
            ])
        }
        
    async def stop_execution(self) -> None:
        """Stop current execution"""
        if not self.is_running:
            return
            
        # Cancel all running tasks
        for task in self.execution_tasks.values():
            if not task.done():
                task.cancel()
                
        # Wait for tasks to be cancelled
        await asyncio.gather(*self.execution_tasks.values(), return_exceptions=True)
        
        self.is_running = False
        self.monitor.log_event(EventType.GRAPH_COMPLETE, None, {
            "execution_id": self.execution_id,
            "stopped_by_user": True
        }) 