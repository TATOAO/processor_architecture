import uuid
import traceback
import networkx as nx
import asyncio
from typing import List, Dict, Any, Optional, Set, AsyncGenerator, Generator
from .processor import AsyncProcessor
from .pipe import AsyncPipe
from .graph_model import Node, Edge
from .core_interfaces import PipeInterface, PipeMeta, ProcessorMeta, ProcessorInterface
from pydantic import BaseModel, field_validator, ConfigDict
from .graph_utils import (
    build_nx_graph, 
    traverse_graph_generator_dfs,
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
    get_execution_levels, 
    get_parallel_execution_opportunities, 
    get_graph_statistics,
)


class ProcessorDirectPipe(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
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


        if "processor_id" not in kwargs:
            unique_id = uuid.uuid4()
            self.processor_id = f"graph_{unique_id}"
        else:
            self.processor_id = kwargs["processor_id"]

        self._statistics = GraphStatistics()

        self.nodes: List[Node] = nodes
        self.edges: List[Edge] = edges
        self.nodes_map: Dict[str, Node] = {node.processor_unique_name: node for node in self.nodes}
        self.edges_map: Dict[str, Edge] = {edge.edge_unique_name: edge for edge in self.edges}
        self.nx_graph = build_nx_graph(self.nodes, self.edges)

        self.processor_pipes: Dict[str, ProcessorDirectPipe] = {}
        self.processors: Dict[str, ProcessorInterface] = {}


    async def dynamic_fan_in_pipes_task(self, node: Node):
        """
        Fan in pipes for a node.
        """
        precious_nodes = get_previous_nodes(self.nx_graph, node.processor_unique_name)

        if len(precious_nodes) == 0:
            return

        previous_input_pipes = [
            self.processor_pipes[previous_node.processor_unique_name].output_pipe 
            for previous_node in precious_nodes
            ]
        task = asyncio.create_task(
            self._fan_in_pipes(
                previous_input_pipes, 
                self.processor_pipes[node.processor_unique_name].input_pipe), 
            name=f"fan_in_pipes_task_{node.processor_unique_name}_to_{'='.join([previous_node.processor_unique_name for previous_node in precious_nodes])}"
        )

        await task

    async def dynamic_fan_out_pipes_task(self, node: Node):
        """
        Fan out pipes for a node.
        """

        next_nodes = get_next_nodes(self.nx_graph, node.processor_unique_name)

        if len(next_nodes) > 0:
            next_input_pipes = [
                self.processor_pipes[next_node.processor_unique_name].input_pipe
                for next_node in next_nodes
            ]

            task = asyncio.create_task(
                self._fan_out_pipes(
                    self.processor_pipes[node.processor_unique_name].\
                        output_pipe, next_input_pipes
                ),
                name=f"fan_out_pipes_task_{node.processor_unique_name}_to_{'='.join([next_node.processor_unique_name for next_node in next_nodes])}"
            )
            await task
        else:
            await asyncio.sleep(0)
            


    async def add_processor_into_graph(self, node: Node):
        self.initialize_node(node)
        # merge input pipes
        self.background_tasks.append(self.dynamic_fan_in_pipes_task(node))
        # merge output pipes
        self.background_tasks.append(self.dynamic_fan_out_pipes_task(node))


    def initialize(self):

        for node in self.nodes:
            self.initialize_node(node)

        # Get output pipe type from meta (defaults are now set in ProcessorMeta)
        this_output_pipe_type = self.meta["output_pipe_type"]
        the_output_pipe = PipeMeta.registry[this_output_pipe_type](
            pipe_id=f"output_pipe_{self.processor_id}"
            )

        self.register_output_pipe(the_output_pipe)


        graph_input_pipe_type = self.input_pipe_type
        graph_input_pipe = PipeMeta.registry[graph_input_pipe_type](
            pipe_id=f"input_pipe_{self.processor_id}"
        )
        self.register_input_pipe(graph_input_pipe)




        for node in self.nodes:
            # merge input pipes
            self.background_tasks.append(self.dynamic_fan_in_pipes_task(node))

            # merge output pipes
            self.background_tasks.append(self.dynamic_fan_out_pipes_task(node))

        
        root_node = get_root_nodes(self.nx_graph)[0]
        
        # Create graph's own input pipe (separate from root node's input pipe)
        graph_pipe_into_root_input_pipe_task = asyncio.create_task(
            self._fan_out_pipes(
                self.input_pipe, 
                [self.processor_pipes[root_node.processor_unique_name].input_pipe]
            ),
            name="graph_pipe_into_root_input_pipe_task"
        )


        # create the task for all leaf nodes
        leaf_nodes = get_leaf_nodes(self.nx_graph)
        leaf_nodes_output_pipes = [
            self.processor_pipes[node.processor_unique_name].output_pipe
            for node in leaf_nodes
        ]
        leaf_nodes_output_pipe_task = asyncio.create_task(
            self._fan_in_pipes(leaf_nodes_output_pipes, self.output_pipe),
            name="leaf_nodes_output_pipe_task"
        )

        self.background_tasks.insert(0, graph_pipe_into_root_input_pipe_task)
        self.background_tasks.insert(0, leaf_nodes_output_pipe_task)
        self.logger.info(f"Graph initialized: {self.processor_id}, {self.background_tasks}")



    def initialize_node(self, node: Node):
        # Get pipe types from meta (defaults are now set in ProcessorMeta)
        meta = getattr(node.processor_class, '_meta', {})
        input_pipe_type = meta["input_pipe_type"]
        input_pipe_class = PipeMeta.registry[input_pipe_type]
        output_pipe_type = meta["output_pipe_type"]
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

        processor.session = self.session


    async def _fan_in_pipes(self, input_pipes: List[PipeInterface], output_pipe: PipeInterface):

        """Merge multiple input pipes into a single output pipe using asyncio.as_completed"""
        async def read_pipe_task(pipe):
            """Read all data from a single pipe"""
            # import ipdb; ipdb.set_trace()
            async for message_id, data in pipe:
                self.logger.info(f"Fan in pipe: {data}: from {pipe._pipe_id} to {output_pipe._pipe_id}")
                if data is None:
                    break
                await output_pipe.put(data)
        
        try:
            # Create tasks for reading from each input pipe
            tasks = [asyncio.create_task(read_pipe_task(pipe), name=f"read_pipe_task_{pipe._pipe_id}") for pipe in input_pipes]
            # Wait for all tasks to complete
            await asyncio.gather(*tasks)
        except Exception as e:
            # import ipdb; ipdb.set_trace()
            self.logger.error(f"Error fanning in pipes: {e}")
            self.logger.error(traceback.format_exc())
        finally:
<<<<<<< Updated upstream
            # Close the output pipe when all inputs are exhausted
            await output_pipe.close()
=======
            # Signal end-of-stream to output pipe so pipes like BlockingPipe can flush
            # import ipdb; ipdb.set_trace()
            await output_pipe.put(None)
>>>>>>> Stashed changes
    

    async def _fan_out_pipes(self, source_pipe: PipeInterface, output_pipes: List[PipeInterface]):
        """
        Fan out data from a single source pipe to multiple output pipes. Copy the data from the source pipe to each output pipe.
        """
        try:
            # import ipdb; ipdb.set_trace()
            async for message_id, data in source_pipe:
                for pipe in output_pipes:
                    self.logger.info(f"Fan out pipe: {data}: from {source_pipe._pipe_id} to {pipe._pipe_id}")
                await pipe.put(data)
        except Exception as e:
            self.logger.error(f"Error fanning out pipes: {e}")
            self.logger.error(traceback.format_exc())
        finally:
            # Propagate end-of-stream to all downstream pipes
            # import ipdb; ipdb.set_trace()
            for pipe in output_pipes:
                try:
                    await pipe.put(None)
                except Exception as e:
                    self.logger.error(f"Error signaling end-of-stream to pipe {getattr(pipe, '_pipe_id', 'unknown')}: {e}")

    
    async def validate_graph(self):
        """
        Validate the graph.
        """
        if not is_acyclic(self.nx_graph):
            raise ValueError("The graph is not acyclic")

        root_nodes = get_root_nodes(self.nx_graph)
        if len(root_nodes) != 1:
            raise ValueError("The graph should have exactly one root node")


    async def execute(self, data: Any, *args, **kwargs) -> Any:
        """
        Execute the graph.
        """
        tasks = []
        root_node = get_root_nodes(self.nx_graph)[0]
        caught_exception = None
        try:
            for node in self.nodes:
                self.logger.info(f"Graph starting processors: {node.processor_unique_name}")

                if node.processor_unique_name == root_node.processor_unique_name:
                    task = asyncio.create_task(
                        self.processors[node.processor_unique_name].execute(data, session_id=session_id, *args, **kwargs), 
                        name=f"execute_task_{node.processor_unique_name}")
                else:
                    task = asyncio.create_task(
                        self.processors[node.processor_unique_name].execute(session_id=session_id), 
                        name=f"execute_task_{node.processor_unique_name}")
                tasks.append(task)

            # Wait for all processors to complete
            await asyncio.gather(*self.background_tasks)
            await asyncio.gather(*tasks)


        except Exception as e:
            self.logger.error(f"Error executing graph: {e}")
            self.logger.error(traceback.format_exc())
            caught_exception = e
        finally:
            await self.input_pipe.close()
            await self.output_pipe.close()
            if caught_exception:
                raise caught_exception


    async def process(self, data: Any, *args, **kwargs) -> Any:
        """
        Process data.
        """
        pass