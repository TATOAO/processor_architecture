"""
Monitoring module for processor pipeline visualization.

This module provides optional monitoring capabilities for processor pipelines,
enabling real-time visualization and performance tracking through a web interface.
"""

from .plugin import MonitoringPlugin, enable_monitoring, MonitoredPipeline
from .hooks import MonitoringHooks
from .client import MonitoringClient
from .collectors import DataCollector, MetricsCollector
from .models import MonitoringConfig

__all__ = [
    "MonitoringPlugin",
    "enable_monitoring", 
    "MonitoredPipeline",
    "MonitoringHooks",
    "MonitoringClient",
    "DataCollector",
    "MetricsCollector",
    "MonitoringConfig"
]