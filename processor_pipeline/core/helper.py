import json
import os
import datetime
from typing import Any, Dict
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --- Utility Functions for Saving ---

def generate_execution_id() -> str:
    """Generate a unique execution ID for pipeline runs."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    return f"{timestamp}"

def save_to_json(data: Dict[str, Any], filepath: str) -> None:
    """
    Save data to a JSON file.
    
    Args:
        data: Dictionary to save as JSON
        filepath: Path where to save the file
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2, default=str)

def default_callback(processor: Any, input_data: Any, output_data: Any, 
                         execution_id: str, step_index: int, *args, **kwargs) -> None:
    """
    Default callback function for saving processor data to JSON files.
    
    Args:
        processor: The processor instance
        input_data: Input data to the processor
        output_data: Output data from the processor
        execution_id: Unique execution identifier
        step_index: Index of the processor in the pipeline
        output_dir: Directory to save the output files
    """
    save_data = processor.get_save_data(input_data, output_data, execution_id, step_index)
    logger.info(f"Callback: log processing data for processor {processor.meta.get('name', 'unknown')} \n{save_data}")

def create_execution_summary(execution_id: str, processors: list, total_time: float, 
                           input_data: Any, final_output: Any, 
                           output_dir: str = "processor_outputs") -> None:
    """
    Create a summary file for the entire pipeline execution.
    
    Args:
        execution_id: Unique execution identifier
        processors: List of processors in the pipeline
        total_time: Total execution time in seconds
        input_data: Initial input to the pipeline
        final_output: Final output from the pipeline
        output_dir: Directory to save the summary file
    """
    summary = {
        "execution_id": execution_id,
        "timestamp": datetime.datetime.now().isoformat(),
        "total_execution_time_seconds": total_time,
        "num_processors": len(processors),
        "processors": [p.meta.get("name") for p in processors],
        "input_summary": str(input_data)[:200] if input_data else None,
        "output_summary": str(final_output)[:200] if final_output else None,
    }
    
    filename = f"execution_summary_{execution_id}.json"
    filepath = os.path.join(output_dir, filename)
    save_to_json(summary, filepath)