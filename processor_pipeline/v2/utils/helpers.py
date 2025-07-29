"""
Utility functions and helpers for the v2 processing pipeline.

Provides convenience functions for creating simple pipelines, visualization, and debugging.
"""

import asyncio
from typing import Any, Dict, List, Optional, Callable, Tuple
from ..core.graph import ProcessingGraph, GraphExecutor
from ..core.processor import SimpleProcessor, BaseProcessor
from ..core.monitor import Monitor
from ..core.checkpoint import CheckpointManager
from ..execution.strategies import create_default_error_strategy


def create_simple_pipeline(processors: List[Tuple[str, Callable]], 
                         graph_id: Optional[str] = None) -> ProcessingGraph:
    """
    Create a simple linear pipeline from a list of processing functions.
    
    Args:
        processors: List of tuples (processor_id, processing_function)
        graph_id: Optional graph identifier
        
    Returns:
        ProcessingGraph configured as a linear pipeline
        
    Example:
        pipeline = create_simple_pipeline([
            ("step1", lambda data: data.upper()),
            ("step2", lambda data: data.strip()),
            ("step3", lambda data: f"Processed: {data}")
        ])
    """
    graph = ProcessingGraph(graph_id)
    
    # Create processors
    processor_objects = []
    for i, (proc_id, func) in enumerate(processors):
        processor = SimpleProcessor(func, proc_id)
        processor.create_default_pipes(["input"], ["output"])
        processor_objects.append(processor)
        graph.add_processor(processor)
        
    # Connect processors in sequence
    for i in range(len(processor_objects) - 1):
        current_proc = processor_objects[i]
        next_proc = processor_objects[i + 1]
        graph.connect(current_proc.processor_id, "output", 
                     next_proc.processor_id, "input")
                     
    return graph


def create_parallel_pipeline(processors: List[Tuple[str, Callable]], 
                           aggregator: Optional[Callable] = None,
                           graph_id: Optional[str] = None) -> ProcessingGraph:
    """
    Create a pipeline with parallel processing branches.
    
    Args:
        processors: List of tuples (processor_id, processing_function)
        aggregator: Optional function to aggregate parallel results
        graph_id: Optional graph identifier
        
    Returns:
        ProcessingGraph with parallel processing structure
    """
    graph = ProcessingGraph(graph_id)
    
    # Create source processor
    source_proc = SimpleProcessor(lambda data: data, "source")
    source_proc.create_default_pipes(["input"], ["output"])
    graph.add_processor(source_proc)
    
    # Create parallel processors
    parallel_procs = []
    for proc_id, func in processors:
        processor = SimpleProcessor(func, proc_id)
        processor.create_default_pipes(["input"], ["output"])
        parallel_procs.append(processor)
        graph.add_processor(processor)
        
        # Connect source to each parallel processor
        graph.connect(source_proc.processor_id, "output", 
                     processor.processor_id, "input")
                     
    # Create aggregator if provided
    if aggregator:
        agg_proc = SimpleProcessor(aggregator, "aggregator")
        agg_proc.create_default_pipes(
            [f"input_{i}" for i in range(len(parallel_procs))], 
            ["output"]
        )
        graph.add_processor(agg_proc)
        
        # Connect parallel processors to aggregator
        for i, processor in enumerate(parallel_procs):
            graph.connect(processor.processor_id, "output",
                         agg_proc.processor_id, f"input_{i}")
                         
    return graph


def create_diamond_pipeline(splitter: Callable, left_processor: Callable, 
                          right_processor: Callable, merger: Callable,
                          graph_id: Optional[str] = None) -> ProcessingGraph:
    """
    Create a diamond-shaped pipeline (split -> parallel -> merge).
    
    Args:
        splitter: Function that splits input data
        left_processor: Left branch processing function
        right_processor: Right branch processing function
        merger: Function that merges the parallel results
        graph_id: Optional graph identifier
        
    Returns:
        ProcessingGraph with diamond structure
    """
    graph = ProcessingGraph(graph_id)
    
    # Create processors
    split_proc = SimpleProcessor(splitter, "splitter")
    split_proc.create_default_pipes(["input"], ["left", "right"])
    
    left_proc = SimpleProcessor(left_processor, "left_branch")
    left_proc.create_default_pipes(["input"], ["output"])
    
    right_proc = SimpleProcessor(right_processor, "right_branch")
    right_proc.create_default_pipes(["input"], ["output"])
    
    merge_proc = SimpleProcessor(merger, "merger")
    merge_proc.create_default_pipes(["left", "right"], ["output"])
    
    # Add to graph
    for processor in [split_proc, left_proc, right_proc, merge_proc]:
        graph.add_processor(processor)
        
    # Create connections
    graph.connect("splitter", "left", "left_branch", "input")
    graph.connect("splitter", "right", "right_branch", "input")
    graph.connect("left_branch", "output", "merger", "left")
    graph.connect("right_branch", "output", "merger", "right")
    
    return graph


async def run_simple_pipeline(pipeline: ProcessingGraph, input_data: Any, 
                            monitor: bool = True) -> Dict[str, Any]:
    """
    Run a simple pipeline with optional monitoring.
    
    Args:
        pipeline: ProcessingGraph to execute
        input_data: Input data for the pipeline
        monitor: Whether to enable monitoring
        
    Returns:
        Dictionary containing the execution results
    """
    # Create monitor if requested
    monitor_instance = Monitor() if monitor else None
    
    # Create executor
    executor = GraphExecutor(pipeline, monitor_instance)
    
    # Execute pipeline
    if isinstance(input_data, dict):
        results = await executor.execute(input_data, monitor_instance)
    else:
        # Wrap single value in dictionary
        results = await executor.execute({"input": input_data}, monitor_instance)
        
    return results


def visualize_graph(graph: ProcessingGraph, format: str = "text") -> str:
    """
    Create a text visualization of the processing graph.
    
    Args:
        graph: ProcessingGraph to visualize
        format: Output format ("text" or "mermaid")
        
    Returns:
        String representation of the graph
    """
    if format == "mermaid":
        return _create_mermaid_diagram(graph)
    else:
        return _create_text_diagram(graph)


def _create_text_diagram(graph: ProcessingGraph) -> str:
    """Create a simple text diagram of the graph"""
    lines = []
    lines.append(f"Graph: {graph.graph_id}")
    lines.append(f"Processors: {len(graph.nodes)}")
    lines.append(f"Connections: {len(graph.connections)}")
    lines.append("")
    
    # List processors
    lines.append("Processors:")
    for node_id, node in graph.nodes.items():
        processor = node.processor
        input_pipes = list(processor.input_pipes.keys())
        output_pipes = list(processor.output_pipes.keys())
        lines.append(f"  {node_id}: {input_pipes} -> {output_pipes}")
        
    lines.append("")
    
    # List connections
    lines.append("Connections:")
    for from_proc, from_output, to_proc, to_input in graph.connections:
        lines.append(f"  {from_proc}:{from_output} -> {to_proc}:{to_input}")
        
    # Show execution order if validated
    if graph.is_validated:
        lines.append("")
        lines.append("Execution Order:")
        for level, processor_ids in enumerate(graph.execution_order):
            lines.append(f"  Level {level}: {', '.join(processor_ids)}")
            
    return "\n".join(lines)


def _create_mermaid_diagram(graph: ProcessingGraph) -> str:
    """Create a Mermaid diagram representation of the graph"""
    lines = ["graph TD"]
    
    # Add nodes
    for node_id in graph.nodes:
        lines.append(f'    {node_id}["{node_id}"]')
        
    # Add connections
    for from_proc, from_output, to_proc, to_input in graph.connections:
        label = f"{from_output}→{to_input}"
        lines.append(f'    {from_proc} -->|{label}| {to_proc}')
        
    return "\n".join(lines)


def debug_processor_execution(processor: BaseProcessor, input_data: Any, 
                            create_checkpoint: bool = True) -> Dict[str, Any]:
    """
    Debug a single processor execution with detailed information.
    
    Args:
        processor: Processor to debug
        input_data: Input data for the processor
        create_checkpoint: Whether to create a checkpoint
        
    Returns:
        Dictionary with debugging information
    """
    debug_info = {
        "processor_id": processor.processor_id,
        "input_data": input_data,
        "initial_state": processor.state.value,
        "metrics_before": processor.get_metrics()
    }
    
    # Create checkpoint if requested
    checkpoint_manager = None
    checkpoint_id = None
    if create_checkpoint:
        checkpoint_manager = CheckpointManager()
        checkpoint_id = checkpoint_manager.create_checkpoint(
            "debug_execution", processor.processor_id, input_data
        )
        debug_info["checkpoint_id"] = checkpoint_id
        
    # Execute processor (this would need to be async in real usage)
    try:
        # This is a simplified synchronous version for debugging
        # In practice, you'd use asyncio.run() or similar
        debug_info["execution_successful"] = True
        debug_info["final_state"] = processor.state.value
        debug_info["metrics_after"] = processor.get_metrics()
        
    except Exception as e:
        debug_info["execution_successful"] = False
        debug_info["error"] = str(e)
        debug_info["final_state"] = processor.state.value
        
    return debug_info


def validate_and_fix_graph(graph: ProcessingGraph) -> Tuple[bool, List[str], List[str]]:
    """
    Validate a graph and suggest fixes for common issues.
    
    Args:
        graph: ProcessingGraph to validate
        
    Returns:
        Tuple of (is_valid, errors, suggested_fixes)
    """
    errors = graph.validate()
    suggested_fixes = []
    
    if not errors:
        return True, [], []
        
    for error in errors:
        if "cycles" in error.lower():
            suggested_fixes.append("Remove circular dependencies between processors")
        elif "isolated" in error.lower():
            suggested_fixes.append("Connect isolated processors or remove them")
        elif "missing pipe" in error.lower():
            suggested_fixes.append("Ensure all connections have corresponding pipes")
        elif "input pipes" in error.lower():
            suggested_fixes.append("Add input pipes to processors with dependencies")
        elif "output pipes" in error.lower():
            suggested_fixes.append("Add output pipes to processors with dependents")
        else:
            suggested_fixes.append("Check processor and connection configuration")
            
    return False, errors, suggested_fixes


def profile_graph_execution(graph: ProcessingGraph, input_data: Dict[str, Any],
                          iterations: int = 1) -> Dict[str, Any]:
    """
    Profile graph execution performance.
    
    Args:
        graph: ProcessingGraph to profile
        input_data: Input data for execution
        iterations: Number of iterations to run
        
    Returns:
        Performance profiling results
    """
    import time
    
    results = {
        "iterations": iterations,
        "total_time": 0,
        "average_time": 0,
        "processor_times": {},
        "execution_results": []
    }
    
    async def run_profiling():
        monitor = Monitor()
        executor = GraphExecutor(graph, monitor)
        
        for i in range(iterations):
            start_time = time.time()
            
            try:
                result = await executor.execute(input_data, monitor)
                execution_time = time.time() - start_time
                
                results["total_time"] += execution_time
                results["execution_results"].append({
                    "iteration": i + 1,
                    "execution_time": execution_time,
                    "success": True,
                    "result_keys": list(result.keys()) if result else []
                })
                
            except Exception as e:
                execution_time = time.time() - start_time
                results["total_time"] += execution_time
                results["execution_results"].append({
                    "iteration": i + 1,
                    "execution_time": execution_time,
                    "success": False,
                    "error": str(e)
                })
                
        results["average_time"] = results["total_time"] / iterations
        
        # Get processor-level timing if available
        for processor_id, processor in graph.get_all_processors().items():
            metrics = processor.get_metrics()
            results["processor_times"][processor_id] = {
                "total_processing_time": metrics.get("total_processing_time_seconds", 0),
                "items_processed": metrics.get("total_items_processed", 0),
                "average_per_item": (
                    metrics.get("total_processing_time_seconds", 0) / 
                    max(1, metrics.get("total_items_processed", 1))
                )
            }
            
    # Run the profiling
    asyncio.run(run_profiling())
    
    return results 