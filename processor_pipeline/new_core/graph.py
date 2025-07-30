import networkx as nx
from typing import List, Dict, Any, Optional, Set
from .core_interfaces import PipeInterface, PipeMeta, ProcessorMeta, ProcessorInterface
from pydantic import BaseModel, field_validator
from .graph_utils import (
    build_nx_graph, 
    get_previous_nodes, 
    get_next_nodes, 
    get_root_nodes, 
    get_leaf_nodes, 
    get_node_paths, 
    is_acyclic, 
    get_topological_sort, 
    get_node_depth, 
    get_nodes_at_depth, 
    get_subgraph_from_node, 
    add_node_to_graph, 
    add_edge_to_graph, 
    get_execution_levels, 
    get_parallel_execution_opportunities, 
    get_graph_statistics,
)

class Node(BaseModel):
    processor_class_name: str
    processor_unique_name: str

    #validation all processors are registered
    @field_validator('processor_class_name', pre=True)
    def processor_class_name_validator(cls, v):
        if not v:
            raise ValueError("processor_class_name is required")
        if v not in ProcessorMeta.registry.keys():
            raise ValueError(f"processor_class_name {v} is not registered")
        
        # get the processor class
        processor_class = ProcessorMeta.registry[v]
        if not issubclass(processor_class, ProcessorInterface):
            raise ValueError(f"processor_class_name {v} is not a valid processor class")
        return v
    

class Edge(BaseModel):
    source_node_unique_name: str
    target_node_unique_name: str
    edge_unique_name: str


class Graph(BaseModel):
    nodes: List[Node]
    edges: List[Edge]


class GraphBase():
    def __init__(self, graph: Graph):
        self.graph = graph
        self.nx_graph = nx.DiGraph()
        self.processors = {}
        self.nx_graph = build_nx_graph(self.graph)
    
    def initialize(self):
        """Initialize all processors in the graph"""
        for node in self.graph.nodes:
            processor_class = ProcessorMeta.registry[node.processor_class_name]

            previous_nodes = get_previous_nodes(self.nx_graph, node.processor_unique_name)
            # build the input pipe
            if len(previous_nodes) == 1:
                previous_node = previous_nodes[0]
                previous_node_data = self.nx_graph.nodes[previous_node]['data']
                previous_node_processor = self.processors[previous_node_data.processor_unique_name]
                
                previous_output_pipe_type = previous_node_processor.meta.get('output_pipe_type')
                previous_output_pipe_class = PipeMeta.registry[previous_output_pipe_type]
                previous_output_pipe = previous_output_pipe_class()

                this_input_pipe_type = node.meta.get('input_pipe_type')
                this_input_pipe_class = PipeMeta.registry[this_input_pipe_type]
                this_input_pipe = this_input_pipe_class()

                if this_input_pipe_type != previous_output_pipe_type:
                    # matching the output pipe to the input pipe = convert the output pipe to the input pipe
                    # this is a conversion pipe
                    
                    input_pipe = this_input_pipe_class()
                else:
                    # using the same pipe
                    input_pipe = previous_output_pipe
            
            # multiple previous nodes
            else:
                pass






            # Create processor instance and store it
            self.processors[node.processor_unique_name] = processor_class()
    
    