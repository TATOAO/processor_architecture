"""
FastAPI backend for processor pipeline monitoring and visualization.
"""

import asyncio
import json
import logging
from datetime import datetime
import os
from typing import Dict, List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from models import SessionInfo, MonitoringEventModel, ProcessorMetrics, ProcessorInfo, PipeInfo
from session_manager import SessionManager, MonitoringSession
from websocket_manager import WebSocketManager
from typing import Any

# Demo graph/processors
import sys
import importlib.util
from pathlib import Path
try:
    # Ensure repository root and backend dir are on sys.path
    CURRENT_DIR = Path(__file__).resolve().parent  # .../visualization/backend
    REPO_ROOT = CURRENT_DIR.parent.parent          # repo root
    for p in (str(REPO_ROOT), str(CURRENT_DIR)):
        if p not in sys.path:
            sys.path.insert(0, p)

    # Try package-relative import first
    try:
        from .processors import demo_graph as DEMO_GRAPH_MODULE_INSTANCE  # type: ignore
        from . import processors as demo_processors_module  # type: ignore
    except Exception:
        # Fallback: absolute import when running from backend cwd
        try:
            from processors import demo_graph as DEMO_GRAPH_MODULE_INSTANCE  # type: ignore
            import processors as demo_processors_module  # type: ignore
        except Exception:
            # Last resort: import from file path
            proc_path = CURRENT_DIR / "processors.py"
            spec = importlib.util.spec_from_file_location("visualization_backend_processors", proc_path)
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                DEMO_GRAPH_MODULE_INSTANCE = getattr(mod, "demo_graph", None)
                demo_processors_module = mod
            else:
                DEMO_GRAPH_MODULE_INSTANCE = None
                demo_processors_module = None
except Exception as e:
    DEMO_GRAPH_MODULE_INSTANCE = None
    demo_processors_module = None
    logger = logging.getLogger(__name__)
    logger.warning(f"Failed to import demo processors/graph: {e}")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global managers
session_manager = SessionManager()
websocket_manager = WebSocketManager()
DEMO_SESSION_ID: Optional[str] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management"""
    logger.info("Starting Processor Pipeline Monitor API")
    # Initialize a demo session so the frontend can display a graph immediately
    global DEMO_SESSION_ID
    try:
        if demo_processors_module and DEMO_GRAPH_MODULE_INSTANCE:
            demo_session = session_manager.create_session(
                session_name="Demo Graph",
                description="Auto-created demo session for visualization",
                metadata={"demo": True}
            )
            DEMO_SESSION_ID = demo_session.session_id

            # Populate processors/pipes into the session from the demo graph so /graph endpoint works
            _populate_session_from_demo_graph(demo_session)
            logger.info(f"Initialized demo session with id={DEMO_SESSION_ID}")
    except Exception as e:
        logger.warning(f"Could not initialize demo session: {e}")
    yield
    logger.info("Shutting down Processor Pipeline Monitor API")


# Create FastAPI app
app = FastAPI(
    title="Processor Pipeline Monitor",
    description="Real-time monitoring and visualization for processor pipelines",
    version="1.0.0",
    lifespan=lifespan
)
# Helper functions for demo graph/session
def _populate_session_from_demo_graph(session: MonitoringSession):
    """Populate a MonitoringSession's processors and pipes based on the demo graph structure."""
    try:
        if not demo_processors_module or not DEMO_GRAPH_MODULE_INSTANCE:
            return
        # Add processors
        session.processors.clear()
        for node in DEMO_GRAPH_MODULE_INSTANCE.nodes:
            session.processors[node.processor_unique_name] = ProcessorInfo(
                processor_id=node.processor_unique_name,
                processor_type=node.processor_class_name,
            )
        # Add pipes from edges
        session.pipes.clear()
        for edge in DEMO_GRAPH_MODULE_INSTANCE.edges:
            session.pipes[edge.edge_unique_name] = PipeInfo(
                pipe_id=edge.edge_unique_name,
                pipe_type="BufferPipe",
                source_processor=edge.source_node_unique_name,
                target_processor=edge.target_node_unique_name,
            )
        # Mark graph dirty so it rebuilds next time requested
        session._graph_dirty = True
    except Exception as e:
        logger.error(f"Error populating session from demo graph: {e}")


async def _broadcast_event(session_id: str, event_type: str, data: dict):
    """Create MonitoringEventModel, update session, and broadcast to WebSocket clients."""
    event = MonitoringEventModel(event_type=event_type, session_id=session_id, data=data)
    session = session_manager.get_session(session_id)
    if session:
        session.process_event(event)
    await websocket_manager.broadcast_to_session(session_id, {
        "type": "event",
        "event": event.model_dump(),
    })


async def _run_demo_pipeline(session_id: str, input_data: Any):
    """Run the demo pipeline (astream) and emit monitoring events."""
    if not demo_processors_module or not DEMO_GRAPH_MODULE_INSTANCE:
        logger.warning("Demo graph module not available; cannot run demo pipeline")
        return

    # Ensure session exists and has processors/pipes populated
    session = session_manager.get_session(session_id)
    if not session:
        session = session_manager.create_session(session_name=f"Demo Graph {session_id}")
    _populate_session_from_demo_graph(session)

    # Start events
    await _broadcast_event(session_id, "session_start", {"config": {"demo": True}})
    for node in DEMO_GRAPH_MODULE_INSTANCE.nodes:
        await _broadcast_event(session_id, "processor_start", {
            "processor_id": node.processor_unique_name,
            "processor_type": node.processor_class_name,
        })

    # Build a fresh graph instance for each run
    try:
        GraphClass = getattr(demo_processors_module, "GraphBase")
        new_graph = GraphClass(
            nodes=DEMO_GRAPH_MODULE_INSTANCE.nodes,
            edges=DEMO_GRAPH_MODULE_INSTANCE.edges,
            processor_id=f"graph_{session_id}"
        )
        new_graph.initialize()

        # Consume the astream to completion
        async for _ in new_graph.astream(input_data):
            # Optional: could broadcast intermediate pipe/metrics updates here
            pass

        # Mark processors complete
        for node in DEMO_GRAPH_MODULE_INSTANCE.nodes:
            await _broadcast_event(session_id, "processor_complete", {
                "processor_id": node.processor_unique_name,
                "metrics": {}
            })

        await _broadcast_event(session_id, "session_end", {})
    except Exception as e:
        logger.error(f"Error running demo pipeline: {e}")
        # Emit error event on all nodes
        for node in DEMO_GRAPH_MODULE_INSTANCE.nodes:
            await _broadcast_event(session_id, "processor_error", {
                "processor_id": node.processor_unique_name,
                "error": str(e)
            })

# Add CORS middleware
# Allow overriding via ALLOWED_ORIGINS env var (comma-separated)
allowed_origins_env = os.getenv("ALLOWED_ORIGINS")
if allowed_origins_env:
    allowed_origins = [origin.strip() for origin in allowed_origins_env.split(",") if origin.strip()]
else:
    allowed_origins = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3001",
    ]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# API Models
class CreateSessionRequest(BaseModel):
    session_name: str
    description: Optional[str] = None
    metadata: Optional[Dict] = None


class EventRequest(BaseModel):
    session_id: str
    event_type: str
    processor_id: Optional[str] = None
    data: Dict


# Root endpoint
@app.get("/")
async def root():
    return {
        "message": "Processor Pipeline Monitor API",
        "version": "1.0.0",
        "docs": "/docs",
        "websocket": "/ws/{session_id}"
    }


# Health check
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "active_sessions": len(session_manager.get_all_sessions()),
        "active_websockets": websocket_manager.get_connection_count()
    }


# Session management endpoints
@app.get("/api/sessions", response_model=List[SessionInfo])
async def get_sessions():
    """Get all active monitoring sessions"""
    sessions = session_manager.get_all_sessions()
    return [session.to_session_info() for session in sessions.values()]


@app.post("/api/sessions", response_model=SessionInfo)
async def create_session(request: CreateSessionRequest):
    """Create a new monitoring session"""
    try:
        session = session_manager.create_session(
            session_name=request.session_name,
            description=request.description,
            metadata=request.metadata or {}
        )
        logger.info(f"Created session: {session.session_id}")
        return session.to_session_info()
    except Exception as e:
        logger.error(f"Failed to create session: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/sessions/{session_id}", response_model=SessionInfo)
async def get_session(session_id: str):
    """Get a specific session"""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session.to_session_info()


@app.delete("/api/sessions/{session_id}")
async def delete_session(session_id: str):
    """Delete a session"""
    success = session_manager.delete_session(session_id)
    if not success:
        raise HTTPException(status_code=404, detail="Session not found")
    
    # Disconnect all WebSocket connections for this session
    await websocket_manager.disconnect_session(session_id)
    
    return {"message": f"Session {session_id} deleted"}


# Demo helper endpoints
@app.get("/api/demo/session")
async def get_demo_session():
    """Return the demo session id (creating if needed)."""
    global DEMO_SESSION_ID
    if DEMO_SESSION_ID is None:
        # Create on-demand if not created during startup
        session = session_manager.create_session(
            session_name="Demo Graph",
            description="On-demand demo session",
            metadata={"demo": True}
        )
        DEMO_SESSION_ID = session.session_id
        _populate_session_from_demo_graph(session)
    return {"session_id": DEMO_SESSION_ID}


@app.get("/api/demo/graph")
async def get_demo_graph_for_graphcomponent():
    """Return demo graph in GraphComponent.tsx expected format."""
    if not DEMO_GRAPH_MODULE_INSTANCE:
        raise HTTPException(status_code=404, detail="Demo graph not available")
    nodes = [
        {
            "processor_class_name": n.processor_class_name,
            "processor_unique_name": n.processor_unique_name,
        }
        for n in DEMO_GRAPH_MODULE_INSTANCE.nodes
    ]
    edges = [
        {
            "source_node_unique_name": e.source_node_unique_name,
            "target_node_unique_name": e.target_node_unique_name,
            "edge_unique_name": e.edge_unique_name,
        }
        for e in DEMO_GRAPH_MODULE_INSTANCE.edges
    ]
    return {
        "nodes": nodes,
        "edges": edges,
        "processor_id": getattr(DEMO_GRAPH_MODULE_INSTANCE, "processor_id", "graph_demo"),
    }


# Processor endpoints
@app.get("/api/sessions/{session_id}/processors")
async def get_session_processors(session_id: str):
    """Get all processors in a session"""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session.get_processors()


@app.get("/api/sessions/{session_id}/processors/{processor_id}")
async def get_processor_info(session_id: str, processor_id: str):
    """Get detailed information about a specific processor"""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    processor = session.get_processor(processor_id)
    if not processor:
        raise HTTPException(status_code=404, detail="Processor not found")
    
    return processor


@app.get("/api/sessions/{session_id}/processors/{processor_id}/metrics")
async def get_processor_metrics(session_id: str, processor_id: str):
    """Get metrics for a specific processor"""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    metrics = session.get_processor_metrics(processor_id)
    if metrics is None:
        raise HTTPException(status_code=404, detail="Processor metrics not found")
    
    return metrics


# Graph structure endpoints
@app.get("/api/sessions/{session_id}/graph")
async def get_session_graph(session_id: str):
    """Get the execution graph structure for a session"""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    return session.get_graph_structure()


class RunRequest(BaseModel):
    input_data: Any


@app.post("/api/sessions/{session_id}/run")
async def run_demo_session(session_id: str, request: RunRequest):
    """Start running the demo pipeline for a session with provided input_data."""
    # Kick off as background task
    asyncio.create_task(_run_demo_pipeline(session_id, request.input_data))
    return {"status": "started"}


# Event handling endpoints
@app.post("/api/events")
async def receive_event(event_data: dict):
    """Receive a monitoring event from a processor"""
    try:
        # Parse the event
        event = MonitoringEventModel(**event_data)
        
        # Get or create session
        session = session_manager.get_session(event.session_id)
        if not session:
            # Auto-create session if it doesn't exist
            session = session_manager.create_session(
                session_id=event.session_id,
                session_name=f"auto_session_{event.session_id[:8]}"
            )
        
        # Process the event
        session.process_event(event)
        
        # Broadcast to WebSocket connections
        await websocket_manager.broadcast_to_session(event.session_id, {
            "type": "event",
            "event": event.model_dump()
        })
        
        return {"status": "received"}
        
    except Exception as e:
        logger.error(f"Error processing event: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/sessions/{session_id}/events")
async def get_session_events(session_id: str, limit: int = 100):
    """Get recent events for a session"""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    events = session.get_recent_events(limit)
    return events


# WebSocket endpoint
@app.websocket("/ws/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    """WebSocket endpoint for real-time updates"""
    await websocket_manager.connect(websocket, session_id)
    
    try:
        # Send initial session data
        session = session_manager.get_session(session_id)
        if session:
            await websocket.send_json({
                "type": "session_info",
                "data": session.to_session_info().model_dump()
            })
        
        # Keep connection alive and handle incoming messages
        while True:
            try:
                # Wait for messages (for potential client requests)
                message = await websocket.receive_text()
                data = json.loads(message)
                
                # Handle client requests
                if data.get("type") == "get_metrics":
                    if session:
                        metrics = session.get_all_metrics()
                        await websocket.send_json({
                            "type": "metrics_update",
                            "data": metrics
                        })
                elif data.get("type") == "start_pipeline":
                    # Optional input_data, default to a simple string
                    input_data = data.get("input_data", "XOXXOOXX")
                    asyncio.create_task(_run_demo_pipeline(session_id, input_data))
                    await websocket.send_json({"type": "ack", "message": "pipeline_started"})
                
            except WebSocketDisconnect:
                break
            except json.JSONDecodeError:
                await websocket.send_json({
                    "type": "error",
                    "message": "Invalid JSON message"
                })
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                await websocket.send_json({
                    "type": "error", 
                    "message": str(e)
                })
                
    except WebSocketDisconnect:
        pass
    finally:
        websocket_manager.disconnect(websocket, session_id)


# Statistics endpoint
@app.get("/api/stats")
async def get_system_stats():
    """Get system-wide statistics"""
    sessions = session_manager.get_all_sessions()
    
    total_processors = sum(len(session.processors) for session in sessions.values())
    total_events = sum(session.total_events for session in sessions.values())
    
    return {
        "total_sessions": len(sessions),
        "active_sessions": len([s for s in sessions.values() if s.status == "active"]),
        "total_processors": total_processors,
        "total_events": total_events,
        "active_websockets": websocket_manager.get_connection_count(),
        "uptime": "N/A"  # Could add uptime tracking
    }


# uvicorn main:app --reload
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)