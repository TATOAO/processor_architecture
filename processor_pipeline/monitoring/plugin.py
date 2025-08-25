"""
Main monitoring plugin that wraps processors and pipelines for easy monitoring.
"""

import asyncio
import uuid
from typing import Any, AsyncGenerator, List, Optional, Union

from ..core.core_interfaces import ProcessorInterface
from ..core.pipeline import AsyncPipeline
from ..core.graph import GraphBase
from .models import MonitoringConfig
from .client import MonitoringClient
from .hooks import MonitoringHooks


class MonitoringPlugin:
    """
    Main monitoring plugin that wraps processors/pipelines to enable monitoring.
    
    This plugin acts as a transparent wrapper that adds monitoring capabilities
    without changing the original API.
    """
    
    def __init__(
        self, 
        target: Union[ProcessorInterface, AsyncPipeline, GraphBase],
        config: Optional[Union[MonitoringConfig, dict]] = None,
        **kwargs
    ):
        """
        Initialize monitoring plugin.
        
        Args:
            target: The processor, pipeline, or graph to monitor
            config: Monitoring configuration or dict of config parameters
            **kwargs: Additional config parameters
        """
        self.target = target
        
        # Handle config
        if isinstance(config, dict):
            config.update(kwargs)
            self.config = MonitoringConfig(**config)
        elif config is None:
            self.config = MonitoringConfig(**kwargs)
        else:
            self.config = config
        
        # Set session name if not provided
        if self.config.session_name is None:
            if hasattr(target, 'processor_id'):
                self.config.session_name = f"monitor_{target.processor_id}"
            else:
                self.config.session_name = f"monitor_{str(uuid.uuid4())[:8]}"
        
        # Initialize monitoring components
        self.client = MonitoringClient(self.config)
        self.hooks = MonitoringHooks(self.config, self.client)
        
        # Setup monitoring
        self._setup_monitoring()
        
        # Track if monitoring is started
        self._monitoring_started = False
    
    def _setup_monitoring(self):
        """Setup monitoring hooks for the target"""
        if isinstance(self.target, (AsyncPipeline, GraphBase)):
            # Monitor all processors in the pipeline/graph
            for processor_id, processor in self.target.processors.items():
                self.hooks.register_processor(processor)
                
                # Also monitor pipes if accessible
                if hasattr(self.target, 'processor_pipes') and processor_id in self.target.processor_pipes:
                    pipes = self.target.processor_pipes[processor_id]
                    if hasattr(pipes, 'input_pipe'):
                        self.hooks.register_pipe(pipes.input_pipe, target_processor=processor_id)
                    if hasattr(pipes, 'output_pipe'):
                        self.hooks.register_pipe(pipes.output_pipe, source_processor=processor_id)
                        
        elif isinstance(self.target, ProcessorInterface):
            # Monitor single processor
            self.hooks.register_processor(self.target)
    
    async def _ensure_monitoring_started(self):
        """Ensure monitoring is started before processing"""
        if not self._monitoring_started:
            await self.hooks.start()
            self._monitoring_started = True
    
    async def execute(self, data: Any = None) -> Any:
        """Execute the target with monitoring"""
        await self._ensure_monitoring_started()
        
        try:
            result = await self.target.execute(data)
            return result
        finally:
            # Keep monitoring running for potential future executions
            pass
    
    async def astream(self, data: Any) -> AsyncGenerator[Any, None]:
        """Stream data through the target with monitoring"""
        await self._ensure_monitoring_started()
        
        async for item in self.target.astream(data):
            yield item
    
    async def peek_astream(self, observer_id: Optional[str] = None) -> AsyncGenerator[Any, None]:
        """Peek stream with monitoring"""
        await self._ensure_monitoring_started()
        
        if hasattr(self.target, 'peek_astream'):
            async for item in self.target.peek_astream(observer_id):
                yield item
        else:
            raise AttributeError(f"{type(self.target).__name__} does not support peek_astream")
    
    def __getattr__(self, name: str) -> Any:
        """Delegate attribute access to the target"""
        return getattr(self.target, name)
    
    async def stop_monitoring(self):
        """Stop monitoring and cleanup"""
        if self._monitoring_started:
            await self.hooks.stop()
            self._monitoring_started = False
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self._ensure_monitoring_started()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop_monitoring()
    
    def get_session_id(self) -> str:
        """Get the monitoring session ID"""
        return self.hooks.session_id
    
    def get_session_info(self) -> dict:
        """Get current session information"""
        return self.hooks.session_tracker.get_session_info()
    
    def get_metrics(self) -> dict:
        """Get current metrics"""
        return {
            pid: metrics.model_dump() 
            for pid, metrics in self.hooks.metrics_collector.get_all_metrics().items()
        }


def enable_monitoring(
    target: Union[ProcessorInterface, AsyncPipeline, GraphBase],
    web_ui_url: str = "http://localhost:3000",
    **kwargs
) -> MonitoringPlugin:
    """
    Convenience function to enable monitoring on a processor/pipeline.
    
    Args:
        target: The processor, pipeline, or graph to monitor
        web_ui_url: URL of the web UI
        **kwargs: Additional monitoring configuration
    
    Returns:
        MonitoringPlugin: The wrapped target with monitoring enabled
    
    Example:
        pipeline = AsyncPipeline([...])
        monitored = enable_monitoring(pipeline, web_ui_url="http://localhost:3000")
        
        async for result in monitored.astream(data):
            print(result)
    """
    config = MonitoringConfig(web_ui_url=web_ui_url, **kwargs)
    return MonitoringPlugin(target, config)


class MonitoredPipeline(MonitoringPlugin):
    """
    Convenience class for monitored pipelines.
    
    This is just an alias for MonitoringPlugin with a more descriptive name
    when used specifically with pipelines.
    """
    
    def __init__(
        self, 
        processors: List[ProcessorInterface],
        config: Optional[Union[MonitoringConfig, dict]] = None,
        **kwargs
    ):
        """
        Create a monitored pipeline from a list of processors.
        
        Args:
            processors: List of processors to create pipeline from
            config: Monitoring configuration
            **kwargs: Additional config parameters
        """
        pipeline = AsyncPipeline(processors)
        super().__init__(pipeline, config, **kwargs)