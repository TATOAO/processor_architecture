# Processor Pipeline Visualization System

A real-time monitoring and visualization system for processor pipelines, built with FastAPI and Next.js.

## 🎯 Features

- **Real-time Monitoring**: Live updates via WebSocket connections
- **Interactive Graph Visualization**: See your pipeline structure and data flow
- **Performance Metrics**: Track throughput, processing times, and error rates
- **Event Logging**: Detailed event history with filtering and search
- **Zero-friction Integration**: Add monitoring with just one line of code
- **Session Management**: Monitor multiple pipelines simultaneously
- **Modern Web UI**: Responsive design with dark/light modes

## 🚀 Quick Start

### 1. Start the Visualization Server

```bash
# Option A: Using Docker (Recommended)
cd visualization
docker-compose up

# Option B: Manual Setup
# Backend
cd visualization/backend
pip install -r requirements.txt
python main.py

# Frontend (in another terminal)
cd visualization/frontend
npm install
npm run dev
```

The web interface will be available at http://localhost:3000

### 2. Enable Monitoring in Your Pipeline

```python
from processor_pipeline.monitoring import enable_monitoring

# Your existing pipeline
pipeline = AsyncPipeline([...])

# Enable monitoring with one line
monitored_pipeline = enable_monitoring(pipeline)

# Use normally - monitoring happens automatically
async for result in monitored_pipeline.astream(data):
    print(result)
```

### 3. View Real-time Monitoring

Open http://localhost:3000 to see:
- Live pipeline graph with processor status
- Real-time performance metrics
- Event log with detailed information
- Session management and history

## 📖 Usage Examples

### Basic Monitoring

```python
from processor_pipeline import AsyncPipeline
from processor_pipeline.monitoring import enable_monitoring

# Create your pipeline
pipeline = AsyncPipeline([
    TextProcessor(),
    DataProcessor(),
    OutputProcessor(),
])

# Enable monitoring
monitored = enable_monitoring(
    pipeline,
    session_name="my_pipeline",
    web_ui_url="http://localhost:3000"
)

# Process data with monitoring
async for result in monitored.astream(input_data):
    print(result)
```

### Custom Configuration

```python
from processor_pipeline.monitoring import MonitoringConfig, MonitoringPlugin

config = MonitoringConfig(
    session_name="advanced_pipeline",
    collect_input_data=True,      # Log input samples
    collect_output_data=True,     # Log output samples
    metrics_interval=0.5,         # Update metrics every 0.5s
    max_events_buffer=1000        # Keep 1000 events in memory
)

monitored_pipeline = MonitoringPlugin(pipeline, config=config)
```

### Context Manager Usage

```python
# Automatic cleanup when done
async with enable_monitoring(pipeline) as monitored:
    async for result in monitored.astream(data):
        process(result)
# Monitoring automatically stops here
```

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Your Pipeline │───▶│ Monitoring      │───▶│ Visualization   │
│                 │    │ Plugin          │    │ Dashboard       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │ FastAPI Backend │
                       │ + WebSocket     │
                       └─────────────────┘
```

### Components

- **Monitoring Plugin**: Hooks into processor lifecycle with minimal overhead
- **FastAPI Backend**: RESTful API and WebSocket server for real-time updates
- **Next.js Frontend**: Modern React-based dashboard with real-time visualization
- **Session Management**: Track multiple pipeline executions
- **Event System**: Comprehensive logging of pipeline events

## 🔧 Configuration

### Monitoring Options

| Option | Default | Description |
|--------|---------|-------------|
| `web_ui_url` | `"http://localhost:3000"` | Web interface URL |
| `api_url` | `"http://localhost:8000"` | Backend API URL |
| `session_name` | Auto-generated | Custom session name |
| `collect_input_data` | `False` | Log input data samples |
| `collect_output_data` | `False` | Log output data samples |
| `metrics_interval` | `1.0` | Metrics update frequency (seconds) |
| `max_events_buffer` | `1000` | Maximum events to keep in memory |

### Environment Variables

```bash
# Backend
PYTHONPATH=/app

# Frontend
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_WS_URL=ws://localhost:8000
```

## 📊 Web Interface

### Dashboard Features

1. **Session Overview**: List of all monitoring sessions
2. **Pipeline Graph**: Interactive visualization of processor connections
3. **Real-time Metrics**: Performance charts and statistics
4. **Event Log**: Detailed event history with filtering
5. **Processor Details**: Individual processor information and status

### Graph Visualization

- **Processor Nodes**: Color-coded by status (idle, processing, completed, error)
- **Data Flow**: Animated connections showing data movement
- **Queue Sizes**: Visual indicators of pipe queue states
- **Interactive**: Click nodes for detailed information

### Metrics Dashboard

- **Throughput Charts**: Items processed per second
- **Processing Time**: Min/max/average processing times
- **Error Rates**: Success/failure statistics
- **Status Distribution**: Processor status breakdown

## 🔍 Monitoring Events

The system tracks these event types:

- `session_start`: Monitoring session begins
- `session_end`: Monitoring session ends
- `processor_start`: Processor begins processing
- `processor_complete`: Processor completes processing
- `processor_error`: Processor encounters an error
- `pipe_update`: Pipe queue size changes
- `metrics_update`: Performance metrics update

## 🐳 Docker Deployment

```bash
# Development
docker-compose up

# Production
docker-compose -f docker-compose.prod.yml up -d
```

## 🤝 Integration with External Projects

### Option 1: Wrapper Approach (Recommended)

```python
# In your existing project
from processor_pipeline.monitoring import enable_monitoring

# Wrap your existing pipeline
monitored_pipeline = enable_monitoring(your_pipeline)
```

### Option 2: Direct Integration

```python
# Import monitoring components
from processor_pipeline.monitoring import MonitoringPlugin, MonitoringConfig

# Configure monitoring
config = MonitoringConfig(session_name="my_project")
plugin = MonitoringPlugin(your_pipeline, config)
```

### Option 3: Conditional Monitoring

```python
# Enable monitoring only in development/debug mode
if DEBUG_MODE:
    from processor_pipeline.monitoring import enable_monitoring
    pipeline = enable_monitoring(pipeline)
```

## 🔧 Development

### Backend Development

```bash
cd visualization/backend
pip install -r requirements.txt
uvicorn main:app --reload
```

### Frontend Development

```bash
cd visualization/frontend
npm install
npm run dev
```

### Running Tests

```bash
# Backend tests
cd visualization/backend
python -m pytest

# Frontend tests  
cd visualization/frontend
npm test
```

## 📝 License

MIT License - see LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 🆘 Troubleshooting

### Common Issues

**Q: Monitoring not showing up in web interface**
- Check that both backend (port 8000) and frontend (port 3000) are running
- Verify WebSocket connection in browser dev tools
- Ensure firewall allows connections to ports 3000 and 8000

**Q: High memory usage with monitoring**
- Reduce `max_events_buffer` in monitoring config
- Set `collect_input_data` and `collect_output_data` to `False`
- Decrease `metrics_interval` to reduce update frequency

**Q: Performance impact**
- Monitoring has minimal overhead when disabled
- Use `collect_input_data=False` in production
- Consider running visualization server on separate machine

### Getting Help

- Check the [examples](../examples/monitoring_example.py) for usage patterns
- Review the [API documentation](http://localhost:8000/docs) when backend is running
- Open an issue for bugs or feature requests