"""
Comprehensive error handling system for the v2 processing pipeline
"""

from typing import Optional, Dict, Any, List
from datetime import datetime


class PipelineError(Exception):
    """Base exception for all pipeline-related errors"""
    
    def __init__(self, message: str, processor_id: Optional[str] = None, 
                 context: Optional[Dict[str, Any]] = None, cause: Optional[Exception] = None):
        super().__init__(message)
        self.message = message
        self.processor_id = processor_id
        self.context = context or {}
        self.cause = cause
        self.timestamp = datetime.now()
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for logging/serialization"""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "processor_id": self.processor_id,
            "context": self.context,
            "timestamp": self.timestamp.isoformat(),
            "cause": str(self.cause) if self.cause else None
        }


class ProcessorError(PipelineError):
    """Error that occurred within a processor"""
    
    def __init__(self, message: str, processor_id: str, input_data: Any = None, 
                 context: Optional[Dict[str, Any]] = None, cause: Optional[Exception] = None):
        super().__init__(message, processor_id, context, cause)
        self.input_data = input_data
        
    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["input_data_type"] = str(type(self.input_data).__name__) if self.input_data is not None else None
        data["input_data_summary"] = str(self.input_data)[:200] if self.input_data is not None else None
        return data


class GraphError(PipelineError):
    """Error related to graph structure or execution"""
    
    def __init__(self, message: str, graph_issues: Optional[List[str]] = None, 
                 context: Optional[Dict[str, Any]] = None, cause: Optional[Exception] = None):
        super().__init__(message, None, context, cause)
        self.graph_issues = graph_issues or []
        
    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["graph_issues"] = self.graph_issues
        return data


class ValidationError(PipelineError):
    """Error during graph or component validation"""
    
    def __init__(self, message: str, validation_errors: List[str], 
                 context: Optional[Dict[str, Any]] = None):
        super().__init__(message, None, context)
        self.validation_errors = validation_errors
        
    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["validation_errors"] = self.validation_errors
        return data


class ExecutionError(PipelineError):
    """Error during graph execution"""
    
    def __init__(self, message: str, failed_processors: List[str], 
                 processor_id: Optional[str] = None, context: Optional[Dict[str, Any]] = None,
                 cause: Optional[Exception] = None):
        super().__init__(message, processor_id, context, cause)
        self.failed_processors = failed_processors
        
    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["failed_processors"] = self.failed_processors
        return data


class PipeError(PipelineError):
    """Error related to pipe operations"""
    
    def __init__(self, message: str, pipe_id: str, operation: str,
                 context: Optional[Dict[str, Any]] = None, cause: Optional[Exception] = None):
        super().__init__(message, None, context, cause)
        self.pipe_id = pipe_id
        self.operation = operation
        
    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["pipe_id"] = self.pipe_id
        data["operation"] = self.operation
        return data


class TimeoutError(PipelineError):
    """Error when an operation times out"""
    
    def __init__(self, message: str, timeout_duration: float, 
                 processor_id: Optional[str] = None, context: Optional[Dict[str, Any]] = None):
        super().__init__(message, processor_id, context)
        self.timeout_duration = timeout_duration
        
    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["timeout_duration"] = self.timeout_duration
        return data


class ConfigurationError(PipelineError):
    """Error in configuration or setup"""
    
    def __init__(self, message: str, config_section: Optional[str] = None,
                 context: Optional[Dict[str, Any]] = None):
        super().__init__(message, None, context)
        self.config_section = config_section
        
    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["config_section"] = self.config_section
        return data


class ErrorTracker:
    """Tracks and analyzes errors across the pipeline"""
    
    def __init__(self):
        self.errors: List[PipelineError] = []
        self.error_counts: Dict[str, int] = {}
        
    def record_error(self, error: PipelineError) -> None:
        """Record an error for tracking and analysis"""
        self.errors.append(error)
        error_type = error.__class__.__name__
        self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1
        
    def get_error_summary(self) -> Dict[str, Any]:
        """Get a summary of all recorded errors"""
        processor_errors = {}
        for error in self.errors:
            if error.processor_id:
                if error.processor_id not in processor_errors:
                    processor_errors[error.processor_id] = []
                processor_errors[error.processor_id].append(error.to_dict())
                
        return {
            "total_errors": len(self.errors),
            "error_counts_by_type": self.error_counts,
            "errors_by_processor": processor_errors,
            "recent_errors": [e.to_dict() for e in self.errors[-10:]]  # Last 10 errors
        }
        
    def clear_errors(self) -> None:
        """Clear all recorded errors"""
        self.errors.clear()
        self.error_counts.clear()
        
    def get_processor_error_rate(self, processor_id: str) -> float:
        """Get error rate for a specific processor"""
        processor_errors = [e for e in self.errors if e.processor_id == processor_id]
        if not processor_errors:
            return 0.0
        
        # Simple calculation - could be enhanced with time windows
        return len(processor_errors) / len(self.errors)
        
    def has_recurring_errors(self, processor_id: str, threshold: int = 3) -> bool:
        """Check if a processor has recurring errors above threshold"""
        processor_errors = [e for e in self.errors if e.processor_id == processor_id]
        return len(processor_errors) >= threshold 