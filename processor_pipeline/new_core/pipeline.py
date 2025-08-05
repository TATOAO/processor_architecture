import asyncio
from .graph import GraphBase, ProcessorDirectPipe, get_root_nodes
from .graph_model import Node, Edge
from typing import List, Dict
from .core_interfaces import ProcessorInterface, PipeMeta
from .pipe import AsyncPipe

class AsyncPipeline(GraphBase):
    meta = {
        "input_pipe_type": "AsyncPipe",
        "output_pipe_type": "AsyncPipe",
    }

    def __init__(self, processors: List[ProcessorInterface], **kwargs):

        nodes = []
        for processor in processors:
            nodes.append(Node(processor_class_name=processor.__class__.__name__, processor_unique_name=processor.processor_id))

        edges = []
        for processor in processors:
            edges.append(Edge(source_node_unique_name=processor.__class__.__name__, target_node_unique_name=processor.processor_id))  

        super().__init__(nodes, edges, **kwargs)

        self.processors: Dict[str, ProcessorInterface] = {processor.processor_id: processor for processor in processors}

    async def initialize(self):
        """
        skip the processor initialization
        """


        for node in self.nodes:
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
            processor = self.processors[node.processor_unique_name]
            processor.register_input_pipe(self.processor_pipes[node.processor_unique_name].input_pipe)
            processor.register_output_pipe(self.processor_pipes[node.processor_unique_name].output_pipe)



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
            task = asyncio.create_task(self.dynamic_fan_in_pipes_task(node))
            self.background_tasks.append(task)

            # merge output pipes
            task = asyncio.create_task(self.dynamic_fan_out_pipes_task(node))
            self.background_tasks.append(task)
        
        root_node = get_root_nodes(self.nx_graph)[0]
        
        # Create graph's own input pipe (separate from root node's input pipe)
        graph_pipe_into_root_input_pipe_task = asyncio.create_task(
            self._fan_out_pipes(
                self.input_pipe, 
                [self.processor_pipes[root_node.processor_unique_name].input_pipe]
            )
        )
        self.background_tasks.insert(0, graph_pipe_into_root_input_pipe_task)
        self.logger.info(f"Graph initialized: {self.processor_id}, {self.background_tasks}")
