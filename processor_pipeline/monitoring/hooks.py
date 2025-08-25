"""
Monitoring hooks that integrate with the existing processor and pipe interfaces.
"""

import asyncio
import time
import uuid
from typing import Any, Dict, Optional, Set
from functools import wraps

from ..core.core_interfaces import ProcessorInterface, PipeInterface
from .models import (
    MonitoringEvent, EventType, ProcessorStatus, 
    MonitoringConfig, ProcessorInfo, PipeInfo
)
from .client import MonitoringClient
from .collectors import MetricsCollector, DataCollector, SessionTracker


class MonitoringHooks:
    """Hooks into processor and pipe lifecycle events"""
    
    def __init__(self, config: MonitoringConfig, client: MonitoringClient):
        self.config = config
        self.client = client
        self.session_id = str(uuid.uuid4())
        self.session_name = config.session_name or f"session_{self.session_id[:8]}"
        
        # Collectors
        self.metrics_collector = MetricsCollector()
        self.data_collector = DataCollector() if config.collect_input_data or config.collect_output_data else None
        self.session_tracker = SessionTracker(self.session_id, self.session_name)
        
        # Tracking state
        self.active_processors: Set[str] = set()
        self.processing_start_times: Dict[str, float] = {}
        
        # Background tasks
        self.metrics_task: Optional[asyncio.Task] = None
        self.running = False
    
    async def start(self):
        """Start monitoring hooks"""
        self.running = True
        
        # Start the monitoring client
        await self.client.start()
        
        # Register the session
        session_info = self.session_tracker.get_session_info()
        await self.client.register_session(session_info)
        
        # Send session start event
        await self._send_event(EventType.SESSION_START, {
            "session_id": self.session_id,
            "session_name": self.session_name,
            "config": self.config.model_dump()
        })
        
        # Start metrics collection task
        if self.config.metrics_interval > 0:
            self.metrics_task = asyncio.create_task(self._metrics_collection_loop())
    
    async def stop(self):
        """Stop monitoring hooks"""
        self.running = False
        
        if self.metrics_task:
            self.metrics_task.cancel()
            try:
                await self.metrics_task
            except asyncio.CancelledError:
                pass
        
        # Send session end event
        self.session_tracker.end_session()
        await self._send_event(EventType.SESSION_END, {
            "session_info": self.session_tracker.get_session_info()
        })
        
        await self.client.stop()
    
    def register_processor(self, processor: ProcessorInterface):
        """Register a processor for monitoring"""
        processor_id = processor.processor_id
        processor_type = processor._meta.get("name", type(processor).__name__)
        
        # Add to session tracker
        self.session_tracker.add_processor(
            processor_id, 
            processor_type, 
            processor._meta
        )
        
        # Hook into processor methods
        self._hook_processor_methods(processor)
    
    def register_pipe(self, pipe: PipeInterface, source_processor: str = None, target_processor: str = None):
        """Register a pipe for monitoring"""
        pipe_id = getattr(pipe, '_pipe_id', str(id(pipe)))
        pipe_type = type(pipe).__name__
        
        # Add to session tracker
        self.session_tracker.add_pipe(pipe_id, pipe_type, source_processor, target_processor)
        
        # Hook into pipe methods
        self._hook_pipe_methods(pipe)
    
    def _hook_processor_methods(self, processor: ProcessorInterface):
        """Hook into processor lifecycle methods"""
        original_execute = processor.execute
        original_process = processor.process
        processor_id = processor.processor_id
        
        @wraps(original_execute)
        async def hooked_execute(*args, **kwargs):
            await self.on_processor_start(processor_id, args[0] if args else None)
            try:
                result = await original_execute(*args, **kwargs)
                await self.on_processor_complete(processor_id, result)
                return result
            except Exception as e:
                await self.on_processor_error(processor_id, e)
                raise
        
        @wraps(original_process)
        async def hooked_process(*args, **kwargs):
            start_time = time.time()
            self.processing_start_times[f"{processor_id}_{start_time}"] = start_time
            
            try:
                # Handle both sync and async generators
                result = original_process(*args, **kwargs)
                if hasattr(result, '__aiter__'):
                    # Async generator - this function becomes an async generator
                    async for item in result:
                        if self.data_collector and self.config.collect_output_data:
                            self.data_collector.record_output(processor_id, item, time.time())
                        yield item
                    
                    end_time = time.time()
                    self.metrics_collector.record_processor_complete(processor_id, start_time, end_time)
                elif hasattr(result, '__iter__') and not isinstance(result, (str, bytes)):
                    # Regular generator - this function becomes an async generator
                    for item in result:
                        if self.data_collector and self.config.collect_output_data:
                            self.data_collector.record_output(processor_id, item, time.time())
                        yield item
                    
                    end_time = time.time()
                    self.metrics_collector.record_processor_complete(processor_id, start_time, end_time)
                else:
                    # Regular return value - yield it as a single item
                    if self.data_collector and self.config.collect_output_data:
                        self.data_collector.record_output(processor_id, result, time.time())
                    
                    end_time = time.time()
                    self.metrics_collector.record_processor_complete(processor_id, start_time, end_time)
                    yield result
                    
            except Exception as e:
                self.metrics_collector.record_processor_error(processor_id)
                raise
        
        processor.execute = hooked_execute
        processor.process = hooked_process
    
    def _hook_pipe_methods(self, pipe: PipeInterface):
        """Hook into pipe methods"""
        original_put = pipe.put
        original_get = pipe.get
        pipe_id = getattr(pipe, '_pipe_id', str(id(pipe)))
        
        @wraps(original_put)
        async def hooked_put(data):
            result = await original_put(data)
            await self.on_pipe_put(pipe_id, data)
            return result
        
        @wraps(original_get)
        async def hooked_get(*args, **kwargs):
            result = await original_get(*args, **kwargs)
            await self.on_pipe_get(pipe_id, result)
            return result
        
        pipe.put = hooked_put
        pipe.get = hooked_get
    
    async def on_processor_start(self, processor_id: str, input_data: Any):
        """Called when a processor starts processing"""
        self.active_processors.add(processor_id)
        self.session_tracker.update_processor_status(processor_id, ProcessorStatus.PROCESSING)
        
        start_time = time.time()
        self.metrics_collector.record_processor_start(processor_id, start_time)
        
        if self.data_collector and self.config.collect_input_data:
            self.data_collector.record_input(processor_id, input_data, start_time)
        
        await self._send_event(EventType.PROCESSOR_START, {
            "processor_id": processor_id,
            "input_data_summary": self._summarize_data(input_data) if input_data else None,
            "timestamp": start_time
        })
    
    async def on_processor_complete(self, processor_id: str, result: Any):
        """Called when a processor completes processing"""
        self.active_processors.discard(processor_id)
        self.session_tracker.update_processor_status(processor_id, ProcessorStatus.COMPLETED)
        
        await self._send_event(EventType.PROCESSOR_COMPLETE, {
            "processor_id": processor_id,
            "result_summary": self._summarize_data(result) if result else None,
            "metrics": self.metrics_collector.get_metrics(processor_id).model_dump() if self.metrics_collector.get_metrics(processor_id) else None
        })
    
    async def on_processor_error(self, processor_id: str, error: Exception):
        """Called when a processor encounters an error"""
        self.active_processors.discard(processor_id)
        self.session_tracker.update_processor_status(processor_id, ProcessorStatus.ERROR)
        
        await self._send_event(EventType.PROCESSOR_ERROR, {
            "processor_id": processor_id,
            "error_type": type(error).__name__,
            "error_message": str(error)
        })
    
    async def on_pipe_put(self, pipe_id: str, data: Any):
        """Called when data is put into a pipe"""
        # Update pipe statistics
        if pipe_id in self.session_tracker.pipes:
            # Get current queue size if available
            queue_size = getattr(self.session_tracker.pipes[pipe_id], 'queue_size', 0) + 1
            self.session_tracker.update_pipe_stats(pipe_id, queue_size)
    
    async def on_pipe_get(self, pipe_id: str, data: Any):
        """Called when data is retrieved from a pipe"""
        # Update pipe statistics
        if pipe_id in self.session_tracker.pipes:
            queue_size = max(0, getattr(self.session_tracker.pipes[pipe_id], 'queue_size', 1) - 1)
            self.session_tracker.update_pipe_stats(pipe_id, queue_size)
    
    async def _send_event(self, event_type: EventType, data: Dict[str, Any]):
        """Send a monitoring event"""
        event = MonitoringEvent(
            event_type=event_type,
            session_id=self.session_id,
            data=data
        )
        
        self.session_tracker.record_event(event_type.value, data)
        await self.client.send_event(event)
    
    async def _metrics_collection_loop(self):
        """Background task for periodic metrics collection"""
        while self.running:
            try:
                await asyncio.sleep(self.config.metrics_interval)
                
                # Collect current metrics
                all_metrics = self.metrics_collector.get_all_metrics()
                if all_metrics:
                    await self._send_event(EventType.METRICS_UPDATE, {
                        "metrics": {pid: metrics.model_dump() for pid, metrics in all_metrics.items()},
                        "active_processors": list(self.active_processors),
                        "session_info": self.session_tracker.get_session_info()
                    })
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                # Log error but continue
                pass
    
    def _summarize_data(self, data: Any) -> Dict[str, Any]:
        """Create a summary of data for monitoring"""
        try:
            summary = {
                "type": type(data).__name__,
                "size": len(data) if hasattr(data, '__len__') else None,
            }
            
            if isinstance(data, (str, int, float, bool)):
                summary["value"] = data if len(str(data)) < 100 else f"{str(data)[:100]}..."
            elif isinstance(data, (list, tuple)):
                summary["length"] = len(data)
                summary["sample"] = data[:3] if len(data) > 0 else []
            elif isinstance(data, dict):
                summary["keys"] = list(data.keys())[:10]
                summary["size"] = len(data)
            
            return summary
        except Exception:
            return {"type": type(data).__name__, "error": "Could not summarize"}


# python -m processor_pipeline.monitoring.hooks
if __name__ == "__main__":
    import asyncio
    from ..core.graph import GraphBase, Node, Edge
    from ..core.processor import AsyncProcessor

    class MyProcessor(AsyncProcessor):
        meta = {
            "name": "MyProcessor",
            "description": "My Processor that calculates string length",
            "version": "1.0.0"
        }
        async def process(self, data: Any, *args, **kwargs) -> Any:
            """Process input data and yield its length twice"""
            print(f"MyProcessor processing: {data}")
            await asyncio.sleep(5)  # Simulate some processing time
            length = len(str(data))
            yield length
            await asyncio.sleep(5)  # Simulate some processing time
            yield length
    
    class MyProcessor2(AsyncProcessor):
        meta = {
            "name": "MyProcessor2", 
            "description": "My Processor that processes numbers",
            "version": "1.0.0"
        }
        async def process(self, data: Any, *args, **kwargs) -> Any:
            """Process numeric input and yield first value and doubled value"""
            print(f"MyProcessor2 processing: {data}")
            await asyncio.sleep(5)  # Simulate some processing time
            yield data
            await asyncio.sleep(5)  # Simulate some processing time
            yield data * 2
    

    async def run_monitoring_example():
        """Complete example showing how monitoring hooks work"""
        print("🔧 Setting up processors and graph...")
        
        # Create nodes and edges for the graph
        nodes = [
            Node(
                processor_class_name="MyProcessor",
                processor_unique_name="MyProcessor",
            ),
            Node(
                processor_class_name="MyProcessor2",
                processor_unique_name="MyProcessor2",
            ),
        ]

        edges = [
            Edge(
                source_node_unique_name="MyProcessor",
                target_node_unique_name="MyProcessor2",
                edge_unique_name="MyProcessor_to_MyProcessor2",
            ),
        ]

        # Create the graph
        graph = GraphBase(nodes=nodes, edges=edges)
        graph.initialize()
        print(f"✅ Created graph with {len(graph.processors)} processors")
        
        # Setup monitoring configuration
        config = MonitoringConfig(
            session_name="hooks_example_session",
            collect_input_data=True,
            collect_output_data=True,
            metrics_interval=2.0,  # Collect metrics every 2 seconds
            web_ui_url="http://localhost:3000",
            api_url="http://localhost:8000"
        )
        
        # Create monitoring client and hooks
        client = MonitoringClient(config)
        monitoring_hooks = MonitoringHooks(config, client)
        
        print("🔗 Registering processors and pipes for monitoring...")
        
        # Register all processors for monitoring
        for processor_id, processor in graph.processors.items():
            monitoring_hooks.register_processor(processor)
            print(f"  📊 Registered processor: {processor_id}")

        # Register all pipes for monitoring
        for processor_id, pipes in graph.processor_pipes.items():
            if hasattr(pipes, 'input_pipe') and pipes.input_pipe:
                monitoring_hooks.register_pipe(
                    pipes.input_pipe, 
                    target_processor=processor_id
                )
                print(f"  🔄 Registered input pipe for: {processor_id}")
            if hasattr(pipes, 'output_pipe') and pipes.output_pipe:
                monitoring_hooks.register_pipe(
                    pipes.output_pipe,
                    source_processor=processor_id
                )
                print(f"  🔄 Registered output pipe for: {processor_id}")

        try:
            print("\n🚀 Starting monitoring...")
            await monitoring_hooks.start()
            print(f"📊 Monitoring session started: {monitoring_hooks.session_name}")
            print(f"🆔 Session ID: {monitoring_hooks.session_id}")
            
            # Test data to process
            test_inputs = [
                "Hello World!",
                "Python Programming", 
                "Monitoring Hooks Demo",
                [1, 2, 3, 4, 5]
            ]
            
            print("\n🔄 Processing test data through the graph...")
            
            # Execute the graph with test data
            results = []
            async for result in graph.astream(test_inputs):
                results.append(result)
                print(f"📤 Output: {result}")
            
            print(f"✅ Completed processing, got {len(results)} results")
            
            # Small delay between tests
            await asyncio.sleep(0.5)
                    
            
            # Let metrics collection run for a bit
            print("\n⏳ Letting metrics collection run...")
            await asyncio.sleep(3)
            
            # Show some collected metrics
            print("\n📈 Current Metrics:")
            all_metrics = monitoring_hooks.metrics_collector.get_all_metrics()
            for processor_id, metrics in all_metrics.items():
                print(f"  {processor_id}:")
                print(f"    - Total processed: {metrics.total_processed}")
                print(f"    - Avg processing time: {metrics.processing_time_avg:.3f}s")
                print(f"    - Throughput: {metrics.throughput:.2f} items/sec")
                print(f"    - Error count: {metrics.error_count}")
            
            # Show session info
            print("\n📊 Session Info:")
            session_info = monitoring_hooks.session_tracker.get_session_info()
            print(f"  - Duration: {session_info['duration']:.2f}s")
            print(f"  - Total events: {session_info['total_events']}")
            print(f"  - Processors: {len(session_info['processors'])}")
            print(f"  - Pipes: {len(session_info['pipes'])}")
            
        except Exception as e:
            print(f"❌ Error during monitoring: {e}")
            
        finally:
            print("\n🛑 Stopping monitoring...")
            await monitoring_hooks.stop()
            print("✅ Monitoring stopped successfully")

    # Register the processor classes so they can be found by the graph
    import sys
    current_module = sys.modules[__name__]
    setattr(current_module, 'MyProcessor', MyProcessor)
    setattr(current_module, 'MyProcessor2', MyProcessor2)
    
    # Run the complete example
    print("🎯 Starting Monitoring Hooks Example")
    print("=" * 50)
    asyncio.run(run_monitoring_example())
    print("=" * 50)
    print("🎉 Example completed!")