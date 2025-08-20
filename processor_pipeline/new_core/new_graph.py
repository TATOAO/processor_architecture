import uuid
import traceback
import asyncio
from typing import List, Dict, Any, Optional, Set, Union
from .processor import AsyncProcessor
from .graph_model import Node, Edge
from .core_interfaces import PipeInterface, PipeMeta, ProcessorInterface
from pydantic import BaseModel, ConfigDict
from .graph_utils import (
    build_nx_graph, 
    get_previous_nodes, 
    get_next_nodes, 
    get_root_nodes, 
    get_leaf_nodes, 
    is_acyclic
)


class ProcessorDirectPipe(BaseModel):
    """Direct pipes for each processor"""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    input_pipe: PipeInterface
    output_pipe: PipeInterface


class EdgeTask(BaseModel):
    """Represents a single data transfer task between nodes (1-to-1 or 1-to-many)"""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    edge: Edge
    source_pipe: PipeInterface
    target_pipe: Union[PipeInterface, List[PipeInterface]]  # Single pipe or list of pipes for fan-out
    task: Optional[asyncio.Task] = None


class NodeCloseTracker(BaseModel):
    """Tracks closing state for a node"""
    node_name: str
    total_upstream_sources: int
    closed_upstream_sources: int = 0
    input_pipe_closed: bool = False
    
    def can_close_input(self) -> bool:
        """Returns True if all upstream sources have closed"""
        return self.closed_upstream_sources >= self.total_upstream_sources
    
    def mark_upstream_closed(self) -> bool:
        """Mark one upstream source as closed. Returns True if input can now be closed."""
        self.closed_upstream_sources += 1
        return self.can_close_input()


class GraphBase(AsyncProcessor):
    """
    Redesigned GraphBase with proper pipe handling and closing logic.
    
    Key improvements:
    1. Edge-based tasks instead of node-based tasks
    2. Proper pipe closing with reference counting
    3. No race conditions or duplicate tasks
    4. Clear separation of concerns
    """
    
    meta = {
        "name": "GraphBase",
        "input_pipe_type": "BufferPipe",
        "output_pipe_type": "BufferPipe",
    }

    def __init__(self, nodes: List[Node], edges: List[Edge], **kwargs):
        super().__init__(**kwargs)

        if "processor_id" not in kwargs:
            unique_id = uuid.uuid4()
            self.processor_id = f"graph_{unique_id}"
        else:
            self.processor_id = kwargs["processor_id"]

        self.nodes: List[Node] = nodes
        self.edges: List[Edge] = edges
        self.nodes_map: Dict[str, Node] = {node.processor_unique_name: node for node in self.nodes}
        self.edges_map: Dict[str, Edge] = {edge.edge_unique_name: edge for edge in self.edges}
        self.nx_graph = build_nx_graph(self.nodes, self.edges)

        # Core data structures
        self.processor_pipes: Dict[str, ProcessorDirectPipe] = {}
        self.processors: Dict[str, ProcessorInterface] = {}
        self.edge_tasks: List[EdgeTask] = []
        self.close_trackers: Dict[str, NodeCloseTracker] = {}
        
        # Initialization state
        self.node_init_variables: Dict[str, Any] = {}
        
        # Validate graph structure
        if not is_acyclic(self.nx_graph):
            raise ValueError("The graph must be acyclic")
        
        root_nodes = get_root_nodes(self.nx_graph)
        if len(root_nodes) != 1:
            raise ValueError("The graph must have exactly one root node")

    def initialize(self):
        """Initialize the graph with proper pipe connections and task management"""
        
        # Step 1: Initialize all processors and their pipes
        for node in self.nodes:
            self._initialize_node(node)
        
        # Step 2: Create graph's own input/output pipes
        self._create_graph_pipes()
        
        # Step 3: Initialize close tracking for each node
        self._initialize_close_trackers()
        
        # Step 4: Create edge tasks for data transfer
        self._create_edge_tasks()
        
        # Step 5: Create special tasks for graph input/output
        self._create_graph_io_tasks()
        
        self.logger.info(f"Graph initialized: {self.processor_id} with {len(self.edge_tasks)} edge tasks")

    def _initialize_node(self, node: Node):
        """Initialize a single node with its processor and pipes"""
        
        # Get initialization variables
        init_kwargs = self.node_init_variables.get(node.processor_unique_name, {})
        
        # Create pipes based on processor meta
        meta = getattr(node.processor_class, '_meta', {})
        input_pipe_type = meta["input_pipe_type"]
        output_pipe_type = meta["output_pipe_type"]
        
        input_pipe = PipeMeta.registry[input_pipe_type](
            pipe_id=f"input_pipe_{node.processor_unique_name}"
        )
        output_pipe = PipeMeta.registry[output_pipe_type](
            pipe_id=f"output_pipe_{node.processor_unique_name}"
        )
        
        self.processor_pipes[node.processor_unique_name] = ProcessorDirectPipe(
            input_pipe=input_pipe,
            output_pipe=output_pipe
        )
        
        # Initialize processor
        processor = node.processor_class(**init_kwargs)
        processor.register_input_pipe(input_pipe)
        processor.register_output_pipe(output_pipe)
        processor.session = self.session
        
        self.processors[node.processor_unique_name] = processor

    def _create_graph_pipes(self):
        """Create the graph's own input and output pipes"""
        
        # Graph output pipe
        output_pipe_type = self.meta["output_pipe_type"]
        graph_output_pipe = PipeMeta.registry[output_pipe_type](
            pipe_id=f"output_pipe_{self.processor_id}"
        )
        self.register_output_pipe(graph_output_pipe)
        
        # Graph input pipe
        input_pipe_type = self.input_pipe_type
        graph_input_pipe = PipeMeta.registry[input_pipe_type](
            pipe_id=f"input_pipe_{self.processor_id}"
        )
        self.register_input_pipe(graph_input_pipe)

    def _initialize_close_trackers(self):
        """Initialize close tracking for each node"""
        
        for node in self.nodes:
            upstream_nodes = get_previous_nodes(self.nx_graph, node.processor_unique_name)
            
            self.close_trackers[node.processor_unique_name] = NodeCloseTracker(
                node_name=node.processor_unique_name,
                total_upstream_sources=len(upstream_nodes)
            )

    def _create_edge_tasks(self):
        """Create tasks for data transfer, handling fan-out properly"""
        
        # Group edges by source node to handle fan-out
        source_to_edges = {}
        for edge in self.edges:
            source = edge.source_node_unique_name
            if source not in source_to_edges:
                source_to_edges[source] = []
            source_to_edges[source].append(edge)
        
        # Create tasks based on fan-out patterns
        for source_node, edges in source_to_edges.items():
            if len(edges) == 1:
                # Simple 1-to-1 connection
                edge = edges[0]
                source_pipe = self.processor_pipes[edge.source_node_unique_name].output_pipe
                target_pipe = self.processor_pipes[edge.target_node_unique_name].input_pipe
                
                edge_task = EdgeTask(
                    edge=edge,
                    source_pipe=source_pipe,
                    target_pipe=target_pipe
                )
                self.edge_tasks.append(edge_task)
            else:
                # Fan-out: 1-to-many connection
                source_pipe = self.processor_pipes[source_node].output_pipe
                target_pipes = [self.processor_pipes[edge.target_node_unique_name].input_pipe for edge in edges]
                
                # Create a single fan-out task for all targets
                fan_out_edge = Edge(
                    source_node_unique_name=source_node,
                    target_node_unique_name=f"fanout_{'_'.join([e.target_node_unique_name for e in edges])}",
                    edge_unique_name=f"fanout_{source_node}"
                )
                
                edge_task = EdgeTask(
                    edge=fan_out_edge,
                    source_pipe=source_pipe,
                    target_pipe=target_pipes  # Pass list of target pipes
                )
                self.edge_tasks.append(edge_task)

    def _create_graph_io_tasks(self):
        """Create tasks for graph input/output connections"""
        
        # Connect graph input to root node
        root_node = get_root_nodes(self.nx_graph)[0]
        root_input_pipe = self.processor_pipes[root_node.processor_unique_name].input_pipe
        
        graph_input_task = EdgeTask(
            edge=Edge(
                source_node_unique_name="graph_input",
                target_node_unique_name=root_node.processor_unique_name,
                edge_unique_name="graph_to_root"
            ),
            source_pipe=self.input_pipe,
            target_pipe=root_input_pipe
        )
        self.edge_tasks.append(graph_input_task)
        
        # Connect leaf nodes to graph output
        leaf_nodes = get_leaf_nodes(self.nx_graph)
        for leaf_node in leaf_nodes:
            leaf_output_pipe = self.processor_pipes[leaf_node.processor_unique_name].output_pipe
            
            leaf_output_task = EdgeTask(
                edge=Edge(
                    source_node_unique_name=leaf_node.processor_unique_name,
                    target_node_unique_name="graph_output",
                    edge_unique_name=f"{leaf_node.processor_unique_name}_to_graph"
                ),
                source_pipe=leaf_output_pipe,
                target_pipe=self.output_pipe
            )
            self.edge_tasks.append(leaf_output_task)

    async def _execute_edge_task(self, edge_task: EdgeTask):
        """Execute a single edge task with proper closing logic (handles both 1-to-1 and 1-to-many)"""
        
        source_pipe = edge_task.source_pipe
        target_pipe = edge_task.target_pipe
        edge = edge_task.edge
        
        try:
            self.logger.info(f"Starting edge task: {edge.source_node_unique_name} -> {edge.target_node_unique_name}")
            
            # Handle both single pipe and fan-out scenarios
            if isinstance(target_pipe, list):
                # Fan-out: 1-to-many
                async for message_id, data in source_pipe:
                    if data is None:
                        break
                    # Send data to ALL target pipes
                    for pipe in target_pipe:
                        await pipe.put(data)
                        self.logger.debug(f"Fan-out data from {source_pipe._pipe_id} to {pipe._pipe_id}: {data}")
            else:
                # Simple 1-to-1 transfer
                async for message_id, data in source_pipe:
                    if data is None:
                        break
                    await target_pipe.put(data)
                    self.logger.debug(f"Transferred data from {source_pipe._pipe_id} to {target_pipe._pipe_id}: {data}")
            
            self.logger.info(f"Edge task completed: {edge.source_node_unique_name} -> {edge.target_node_unique_name}")
            
        except Exception as e:
            self.logger.error(f"Error in edge task {edge.source_node_unique_name} -> {edge.target_node_unique_name}: {e}")
            self.logger.error(traceback.format_exc())
        finally:
            # Handle closing logic
            await self._handle_edge_completion(edge_task)

    async def _handle_edge_completion(self, edge_task: EdgeTask):
        """Handle proper closing when an edge task completes"""
        
        target_node_name = edge_task.edge.target_node_unique_name
        target_pipe = edge_task.target_pipe
        
        # Handle closing for different scenarios
        if target_node_name == "graph_output":
            # Closing graph output - no special logic needed
            return
        
        if edge_task.edge.source_node_unique_name == "graph_input":
            # Closing graph input connection - no special logic needed
            return
        
        if isinstance(target_pipe, list):
            # Fan-out scenario: signal end-of-stream to all target pipes
            for pipe in target_pipe:
                await pipe.put(None)
                # Extract node name from pipe_id (format: "input_pipe_{node_name}")
                node_name = pipe._pipe_id.replace("input_pipe_", "")
                if node_name in self.close_trackers:
                    tracker = self.close_trackers[node_name]
                    if tracker.mark_upstream_closed() and not tracker.input_pipe_closed:
                        tracker.input_pipe_closed = True
                        self.logger.info(f"Closed input pipe for {node_name} - all upstream sources completed")
        else:
            # Single target: mark one upstream source as closed for the target node
            if target_node_name in self.close_trackers:
                tracker = self.close_trackers[target_node_name]
                
                if tracker.mark_upstream_closed() and not tracker.input_pipe_closed:
                    # All upstream sources have closed, now close the input pipe
                    await target_pipe.put(None)  # Signal end-of-stream
                    tracker.input_pipe_closed = True
                    self.logger.info(f"Closed input pipe for {target_node_name} - all upstream sources completed")

    async def execute(self, data: Any, session_id: Optional[str] = None, *args, **kwargs) -> Any:
        """Execute the graph with proper task coordination"""
        
        # Bind session ID
        if session_id is None:
            session_id = str(uuid.uuid4())
        
        self.bind_session_id(session_id)
        for processor in self.processors.values():
            try:
                processor.bind_session_id(session_id)
            except Exception:
                try:
                    processor.session["session_id"] = session_id
                except Exception:
                    pass

        # Start all edge tasks
        edge_tasks = []
        for edge_task in self.edge_tasks:
            task = asyncio.create_task(
                self._execute_edge_task(edge_task),
                name=f"edge_{edge_task.edge.source_node_unique_name}_to_{edge_task.edge.target_node_unique_name}"
            )
            edge_task.task = task
            edge_tasks.append(task)
        
        # Start all processor tasks
        processor_tasks = []
        root_node = get_root_nodes(self.nx_graph)[0]
        
        for node in self.nodes:
            if node.processor_unique_name == root_node.processor_unique_name:
                task = asyncio.create_task(
                    self.processors[node.processor_unique_name].execute(data, session_id=session_id, *args, **kwargs),
                    name=f"processor_{node.processor_unique_name}"
                )
            else:
                task = asyncio.create_task(
                    self.processors[node.processor_unique_name].execute(session_id=session_id),
                    name=f"processor_{node.processor_unique_name}"
                )
            processor_tasks.append(task)

        try:
            # Wait for all processor tasks to complete first
            await asyncio.gather(*processor_tasks)
            
            # Close the graph input pipe to signal end of input
            await self.input_pipe.close()
            
            # Now wait for all edge tasks to complete
            await asyncio.gather(*edge_tasks)
            
        except Exception as e:
            self.logger.error(f"Error executing graph: {e}")
            self.logger.error(traceback.format_exc())
            raise
        finally:
            # Ensure all pipes are properly closed
            await self._cleanup_pipes()

    async def _cleanup_pipes(self):
        """Ensure all pipes are properly closed"""
        try:
            # Close graph output pipe properly
            await self.output_pipe.close()
        except Exception as e:
            self.logger.warning(f"Error closing graph output pipe: {e}")

    async def process(self, data: Any, *args, **kwargs) -> Any:
        """Process method - not used in GraphBase"""
        passear
        pass
