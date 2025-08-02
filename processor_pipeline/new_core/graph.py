import networkx as nx
import asyncio
from typing import List, Dict, Any, Optional, Set, AsyncGenerator, Generator
from .processor import AsyncProcessor
from .pipe import AsyncPipe
from .graph_model import Node, Edge
from .core_interfaces import PipeInterface, PipeMeta, ProcessorMeta, ProcessorInterface
from pydantic import BaseModel, field_validator
from .graph_utils import (
    build_nx_graph, 
    traverse_graph_generator_dfs,
    get_previous_nodes, 
    get_next_nodes_names, 
    get_root_nodes, 
    get_leaf_nodes, 
    get_node_paths, 
    is_acyclic, 
    get_topological_sort, 
    get_node_depth, 
    get_nodes_at_depth, 
    get_subgraph_from_node, 
    get_execution_levels, 
    get_parallel_execution_opportunities, 
    get_graph_statistics,
)


class ProcessorDirectPipe(BaseModel):
    input_pipe: PipeInterface
    output_pipe: PipeInterface

class GraphStatistics(BaseModel):
    """Statistics about the graph"""
    total_nodes: int = 0
    total_edges: int = 0
    total_processors: int = 0
    total_pipes: int = 0

class GraphBase(AsyncProcessor):
    meta = {
        "name": "GraphBase",
        "input_pipe_type": "BufferPipe",
        "output_pipe_type": "BufferPipe",
    }

    def __init__(self, nodes: List[Node], edges: List[Edge], **kwargs):
        super().__init__(**kwargs)
        self._statistics = GraphStatistics()

        self.nodes: List[Node] = nodes
        self.edges: List[Edge] = edges
        self.nodes_map: Dict[str, Node] = {node.processor_unique_name: node for node in self.nodes}
        self.edges_map: Dict[str, Edge] = {edge.edge_unique_name: edge for edge in self.edges}
        self.nx_graph = build_nx_graph(self.nodes, self.edges)

        self.processor_pipes: Dict[str, ProcessorDirectPipe] = {}
        self.processors: Dict[str, ProcessorInterface] = {}


    async def initialize(self):

        for node in self.nodes:
            self.initialize_node(node)

        connecting_tasks = []
        for node in self.nodes:
            # merge input pipes
            precious_nodes = get_previous_nodes(self.nx_graph, node.processor_unique_name)
            previous_input_pipes = [self.processor_pipes[node.processor_unique_name].output_pipe for node in precious_nodes]
            task = asyncio.create_task(self._fan_in_pipes(previous_input_pipes, self.processor_pipes[node.processor_unique_name].input_pipe))
            connecting_tasks.append(task)

            # merge output pipes
            next_nodes = get_next_nodes_names(self.nx_graph, node.processor_unique_name)
            next_output_pipes = [self.processor_pipes[node.processor_unique_name].output_pipe for node in next_nodes]
            task = asyncio.create_task(self._fan_out_pipes(self.processor_pipes[node.processor_unique_name].output_pipe, next_output_pipes))
            connecting_tasks.append(task)
        
        root_node = get_root_nodes(self.nx_graph)[0]
        self.register_input_pipe(self.processor_pipes[root_node.processor_unique_name].input_pipe)

        this_output_pipe_type = self.meta["output_pipe_type"]
        the_output_pipe = PipeMeta.registry[this_output_pipe_type](pipe_id=f"output_pipe_{self.processor_unique_name}")
        self.register_output_pipe(the_output_pipe)

        leaf_nodes = get_leaf_nodes(self.nx_graph)
        leaf_output_pipes = [self.processor_pipes[node.processor_unique_name].output_pipe for node in leaf_nodes]
        task = asyncio.create_task(self._fan_out_pipes(the_output_pipe, leaf_output_pipes))
        connecting_tasks.append(task)

        await asyncio.gather(*connecting_tasks)


    def initialize_node(self, node: Node):
        input_pipe_type = node.processor_class.meta["input_pipe_type"]
        input_pipe_class = PipeMeta.registry[input_pipe_type]
        output_pipe_type = node.processor_class.meta["output_pipe_type"]
        output_pipe_class = PipeMeta.registry[output_pipe_type]
        self.processor_pipes[node.processor_unique_name] = ProcessorDirectPipe(
            input_pipe=input_pipe_class(pipe_id=f"input_pipe_{node.processor_unique_name}"),
            output_pipe=output_pipe_class(pipe_id=f"output_pipe_{node.processor_unique_name}")
        )

        # initialize processor
        processor = node.processor_class()
        processor.register_input_pipe(self.processor_pipes[node.processor_unique_name].input_pipe)
        processor.register_output_pipe(self.processor_pipes[node.processor_unique_name].output_pipe)
        self.processors[node.processor_unique_name] = processor


    async def _fan_in_pipes(self, input_pipes: List[PipeInterface], output_pipe: PipeInterface):
        """Merge multiple input pipes into a single output pipe using asyncio.as_completed"""
        async def read_pipe_task(pipe):
            """Read all data from a single pipe"""
            async for data in pipe:
                if data is None:
                    break
                await output_pipe.put(data)
        
        # Create tasks for reading from each input pipe
        tasks = [asyncio.create_task(read_pipe_task(pipe)) for pipe in input_pipes]
        
        # Wait for all tasks to complete
        await asyncio.gather(*tasks)
        
        # Close the output pipe when all inputs are exhausted
        await output_pipe.close()
    

    async def _fan_out_pipes(self, source_pipe: PipeInterface, output_pipes: List[PipeInterface]):
        """
        Fan out data from a single source pipe to multiple output pipes. Copy the data from the source pipe to each output pipe.
        """
        async for data in source_pipe:
            async for pipe in output_pipes:
                await pipe.put(data)
        
        await source_pipe.close()

    
    async def astream(self, input_data) -> AsyncGenerator[Any, None]:
        pass

