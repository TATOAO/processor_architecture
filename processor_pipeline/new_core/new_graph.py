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
            self.add_node(node)
        
        # Step 2: Add edges message passing tasks
        for edge in self.edges:
            self.add_edge(edge)
        
        # Step 3: Create graph's own input/output pipes
        self._create_graph_pipes()

        # Step 4: Graph edge tasks
        self.add_graph_tasks()
    
    def add_graph_tasks(self):
                
        # Connect graph input to root node
        root_node = get_root_nodes(self.nx_graph)[0]
        root_input_pipe = self.processor_pipes[root_node.processor_unique_name].input_pipe
        
        async def graph_input_task():
            async for data in self.input_pipe.peek_aiter():
                await root_input_pipe.put(data)
            await root_input_pipe.put(None)
        
        task = asyncio.create_task(
            graph_input_task(),
            name=f"graph_input_task_{root_node.processor_unique_name}"
        )
        self.add_background_task(task)
        
        # Connect leaf nodes to graph output
        leaf_nodes = get_leaf_nodes(self.nx_graph)
        for leaf_node in leaf_nodes:
            leaf_output_pipe = self.processor_pipes[leaf_node.processor_unique_name].output_pipe
            
            async def leaf_output_task(pipe=leaf_output_pipe, node_name=leaf_node.processor_unique_name):
                async for message_id, data in pipe.peek_aiter():
                    await self.output_pipe.put(data)

            task = asyncio.create_task(
                leaf_output_task(),
                name=f"leaf_output_task_{leaf_node.processor_unique_name}"
            )
            self.add_background_task(task)
        
        
    
    def add_edge(self, edge: Edge):
        """Add an edge to the graph"""
        source_node = self.nodes_map[edge.source_node_unique_name]
        target_node = self.nodes_map[edge.target_node_unique_name]

        source_pipe = self.processor_pipes[source_node.processor_unique_name].output_pipe
        target_pipe = self.processor_pipes[target_node.processor_unique_name].input_pipe

        async def communication_task():
            async for message_id, data in source_pipe.peek_aiter():
                await target_pipe.put(data)
            await target_pipe.put(None)
        
        task = asyncio.create_task(
            communication_task(),
            name=f"communication_task_{source_node.processor_unique_name}_to_{target_node.processor_unique_name}"
        )
        self.processors[source_node.processor_unique_name].add_background_task(task)
        
        

    def add_node(self, node: Node):
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

            await asyncio.gather(*self.background_tasks)
            
            
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
        pass
