import networkx as nx
import asyncio
from typing import List, Dict, Any, Optional, Set, AsyncGenerator, Generator
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


class GraphBase():
    def __init__(self, nodes: List[Node], edges: List[Edge]):
        self.nodes: List[Node] = nodes
        self.edges: List[Edge] = edges
        self.nodes_map: Dict[str, Node] = {node.processor_unique_name: node for node in self.nodes}
        self.edges_map: Dict[str, Edge] = {edge.edge_unique_name: edge for edge in self.edges}
        self.nx_graph = build_nx_graph(self.nodes, self.edges)

        self.pipes = {}
        self.processors_map = {}

        ## 
        self.leaf_nodes = get_leaf_nodes(self.nx_graph)

        self.initialize()


    def initialize_processors(self) -> Dict[str, ProcessorInterface]:
        """Initialize all processors in the graph"""
        processors = {}
        for node in self.nodes:
            processor_class = ProcessorMeta.registry[node.processor_class_name]
            # Create processor instance and store it
            processors[node.processor_unique_name] = processor_class()
        return processors

    def initialize(self):
        """Initialize all processors in the graph:
        1. create pipes
        2. instance all processors
        3. connecting pipes
            
        4. start all the async task for all processors execute function
        """
        from .pipe import AsyncPipe
        from .graph_utils import initialize_processors
        
        self.root_nodes = get_root_nodes(self.nx_graph)
        self.leaf_nodes = get_leaf_nodes(self.nx_graph)
        # only one root node is allowed
        if len(self.root_nodes) != 1:
            raise ValueError("Graph must have exactly one root node")
        
        self.root_node = self.root_nodes[0]


        for edge in self.edges:
            edge.source_node_pipe_id = f"output_pipe_{edge.source_node_unique_name}"
            edge.target_node_pipe_id = f"input_pipe_{edge.target_node_unique_name}"
        

        for node in self.nodes:
            if node.processor_unique_name == self.root_node:
                continue

            # previosu_node has output deegree 1, this node has input deegree 1, so we can share the pipe
            if len(previous_nodes) == 1:
                previous_node = previous_nodes[0]

                shared_pipe_type = node.processor_class.meta["input_pipe_type"]
                pipe_class = PipeMeta.registry[shared_pipe_type]

                shared_pipe = pipe_class()
                shared_pipe.pipe_id = f"shared_pipe_{node.processor_unique_name}"
                self.pipes[shared_pipe.pipe_id] = shared_pipe




            elif len(get_previous_nodes(self.nx_graph, node.processor_unique_name)) > 1:
                edge = self.edges_map[f"edge_{node.processor_unique_name}"]
                edge.source_node_pipe_id = f"output_pipe_{edge.source_node_unique_name}"
                edge.target_node_pipe_id = f"input_pipe_{edge.target_node_unique_name}"






        # Step 3: Connect pipes to processors
        for node_name in traverse_graph_generator_dfs(self.nx_graph, self.root_node):
            node = self.nodes_map[node_name]
            processor_class_name = node.processor_class_name
            processor_class = ProcessorMeta.registry[processor_class_name]
            input_pipepipe_type = processor_class.meta["input_pipe_type"]
            output_pipe_type = processor_class.meta["output_pipe_type"]


            if input_pipe_type == "AsyncPipe":
                input_pipe = AsyncPipe(pipe_id=f"input_pipe_{node_name}")
            elif input_pipe_type == "GeneratorPipe":

            processor = processor_class()

            # ProcessorMeta.registry[processor_class_name]
            # processor = processor_class()

            


            # Get incoming and outgoing edges for this node
            incoming_edges = [edge for edge in self.edges if edge.target_node_unique_name == node_name]
            outgoing_edges = [edge for edge in self.edges if edge.source_node_unique_name == node_name]
            
            # Register input pipes (N to 1 case)
            if len(incoming_edges) == 0:
                # Root node - no input pipes needed
                pass
            elif len(incoming_edges) == 1:
                # Simple 1-to-1 connection
                input_pipe = self.pipes[incoming_edges[0].edge_unique_name]
                processor.register_input_pipe(input_pipe)
            elif len(incoming_edges) > 1:
                # N-to-1 connection: create a connector pipe
                connector_pipe = AsyncPipe(pipe_id=f"connector_{node_name}")
                processor.register_input_pipe(connector_pipe)
                
                # Start async task to merge multiple inputs into connector pipe
                input_pipes = [self.pipes[edge.edge_unique_name] for edge in incoming_edges]
                asyncio.create_task(self._merge_pipes(input_pipes, connector_pipe))
            
            # Register output pipes (1 to N case)
            if len(outgoing_edges) == 0:
                # Leaf node - no output pipes needed
                pass
            elif len(outgoing_edges) == 1:
                # Simple 1-to-1 connection
                output_pipe = self.pipes[outgoing_edges[0].edge_unique_name]
                processor.register_output_pipe(output_pipe)
            elif len(outgoing_edges) > 1:
                # 1-to-N connection: processors will duplicate output to all pipes
                for edge in outgoing_edges:
                    output_pipe = self.pipes[edge.edge_unique_name]
                    processor.register_output_pipe(output_pipe)

        # Step 4: Start all processor execution tasks
        self.processor_tasks = []
        for node_name in traverse_graph_generator_dfs(self.nx_graph, self.root_node):
            processor = self.processors[node_name]
            task = asyncio.create_task(processor.execute())
            self.processor_tasks.append(task)

    async def _merge_pipes(self, input_pipes: List[PipeInterface], output_pipe: PipeInterface):
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

    
    async def astream(self, input_data) -> AsyncGenerator[Any, None]:
        pass

