import asyncio
from .graph import GraphBase, ProcessorDirectPipe, get_root_nodes
from .graph_model import Node, Edge
from typing import List, Dict
from .core_interfaces import ProcessorInterface, PipeMeta
from .pipe import AsyncPipe

class AsyncPipeline(GraphBase):
    meta = {
        "name": "AsyncPipeline",
        "input_pipe_type": "AsyncPipe",
        "output_pipe_type": "AsyncPipe",
    }

    def __init__(self, processors: List[ProcessorInterface], **kwargs):
        # Create nodes from processor instances
        nodes = []
        for processor in processors:
            # Get the processor class name from the meta
            processor_class_name = processor._meta["name"]
            nodes.append(Node(
                processor_class_name=processor_class_name, 
                processor_unique_name=processor.processor_id
            ))

        # Create sequential edges connecting processors in a pipeline
        edges = []
        for i in range(len(processors) - 1):
            source_processor = processors[i]
            target_processor = processors[i + 1]
            edges.append(Edge(
                source_node_unique_name=source_processor.processor_id,
                target_node_unique_name=target_processor.processor_id,
                edge_unique_name=f"{source_processor.processor_id}_to_{target_processor.processor_id}"
            ))

        super().__init__(nodes, edges, **kwargs)

        # Store processor instances directly
        self.processors: Dict[str, ProcessorInterface] = {processor.processor_id: processor for processor in processors}

        self.initialize()
        

    def initialize(self):
        """
        Initialize the pipeline by setting up pipes for each processor instance
        and connecting them in sequence.
        """
        # Initialize pipes for each processor using their existing instances
        for node in self.nodes:
            processor = self.processors[node.processor_unique_name]
            meta = processor._meta
            
            input_pipe_type = meta["input_pipe_type"]
            input_pipe_class = PipeMeta.registry[input_pipe_type]
            output_pipe_type = meta["output_pipe_type"]
            output_pipe_class = PipeMeta.registry[output_pipe_type]
            
            self.processor_pipes[node.processor_unique_name] = ProcessorDirectPipe(
                input_pipe=input_pipe_class(pipe_id=f"input_pipe_{node.processor_unique_name}"),
                output_pipe=output_pipe_class(pipe_id=f"output_pipe_{node.processor_unique_name}")
            )

            # Register pipes with the processor instance
            processor.register_input_pipe(self.processor_pipes[node.processor_unique_name].input_pipe)
            processor.register_output_pipe(self.processor_pipes[node.processor_unique_name].output_pipe)
            
            # Share session across all processors
            processor.session = self.session
            # If a session_id exists already, bind it for consistent logging at init-time
            try:
                existing_session_id = None
                if isinstance(self.session, dict):
                    existing_session_id = self.session.get("session_id")
                if existing_session_id:
                    processor.bind_session_id(existing_session_id)  # type: ignore[attr-defined]
            except Exception:
                pass

        # Create the pipeline's own input and output pipes
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

        # Set up pipe connections for the sequential pipeline
        for node in self.nodes:
            # Only create fan-out tasks - each node pushes data to its successors
            # Fan-in would cause racing issues where multiple consumers compete for the same data
            # Fan-out ensures each downstream node gets an identical copy of the data
            task = asyncio.create_task(self.dynamic_fan_out_pipes_task(node))
            self.background_tasks.append(task)
        
        # Connect pipeline input to the first processor
        root_node = get_root_nodes(self.nx_graph)[0]
        graph_pipe_into_root_input_pipe_task = asyncio.create_task(
            self._fan_out_pipes(
                self.input_pipe, 
                [self.processor_pipes[root_node.processor_unique_name].input_pipe]
            )
        )
        self.background_tasks.insert(0, graph_pipe_into_root_input_pipe_task)
        self.logger.info(f"Pipeline initialized: {self.processor_id}, {len(self.background_tasks)} background tasks")
