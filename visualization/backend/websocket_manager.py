"""
WebSocket manager for real-time communication with frontend clients.
"""

import asyncio
import json
import logging
from typing import Dict, List, Set
from collections import defaultdict

from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


class WebSocketManager:
    """Manages WebSocket connections for real-time updates"""
    
    def __init__(self):
        # Active connections by session
        self.connections: Dict[str, Set[WebSocket]] = defaultdict(set)
        # Connection metadata
        self.connection_info: Dict[WebSocket, Dict] = {}
        # Total connection count
        self.total_connections = 0
    
    async def connect(self, websocket: WebSocket, session_id: str):
        """Accept a new WebSocket connection"""
        await websocket.accept()
        
        self.connections[session_id].add(websocket)
        self.connection_info[websocket] = {
            "session_id": session_id,
            "connected_at": asyncio.get_event_loop().time(),
            "messages_sent": 0
        }
        self.total_connections += 1
        
        logger.info(f"WebSocket connected for session {session_id}. Total connections: {self.total_connections}")
    
    def disconnect(self, websocket: WebSocket, session_id: str):
        """Remove a WebSocket connection"""
        if websocket in self.connections[session_id]:
            self.connections[session_id].remove(websocket)
            
        if websocket in self.connection_info:
            del self.connection_info[websocket]
            
        self.total_connections -= 1
        
        # Clean up empty session sets
        if not self.connections[session_id]:
            del self.connections[session_id]
        
        logger.info(f"WebSocket disconnected for session {session_id}. Total connections: {self.total_connections}")
    
    async def disconnect_session(self, session_id: str):
        """Disconnect all WebSocket connections for a session"""
        if session_id in self.connections:
            connections = self.connections[session_id].copy()
            for websocket in connections:
                try:
                    await websocket.close(code=1000, reason="Session ended")
                except Exception as e:
                    logger.warning(f"Error closing WebSocket: {e}")
                finally:
                    self.disconnect(websocket, session_id)
    
    async def broadcast_to_session(self, session_id: str, message: dict):
        """Broadcast a message to all connections in a session"""
        if session_id not in self.connections:
            return
        
        connections = self.connections[session_id].copy()
        if not connections:
            return
        
        # Send to all connections concurrently
        tasks = []
        for websocket in connections:
            tasks.append(self._send_safe(websocket, message, session_id))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def broadcast_to_all(self, message: dict):
        """Broadcast a message to all connections"""
        all_connections = []
        for session_connections in self.connections.values():
            all_connections.extend(session_connections)
        
        if not all_connections:
            return
        
        tasks = []
        for websocket in all_connections:
            session_id = self.connection_info.get(websocket, {}).get("session_id", "unknown")
            tasks.append(self._send_safe(websocket, message, session_id))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def send_to_connection(self, websocket: WebSocket, message: dict):
        """Send a message to a specific connection"""
        session_id = self.connection_info.get(websocket, {}).get("session_id", "unknown")
        await self._send_safe(websocket, message, session_id)
    
    async def _send_safe(self, websocket: WebSocket, message: dict, session_id: str):
        """Safely send a message to a WebSocket connection"""
        try:
            await websocket.send_json(message)
            
            # Update message count
            if websocket in self.connection_info:
                self.connection_info[websocket]["messages_sent"] += 1
                
        except WebSocketDisconnect:
            # Connection was closed
            self.disconnect(websocket, session_id)
        except Exception as e:
            logger.warning(f"Error sending WebSocket message to session {session_id}: {e}")
            # Try to close the connection
            try:
                await websocket.close(code=1011, reason="Internal error")
            except:
                pass
            finally:
                self.disconnect(websocket, session_id)
    
    def get_connection_count(self) -> int:
        """Get total number of active connections"""
        return self.total_connections
    
    def get_session_connection_count(self, session_id: str) -> int:
        """Get number of connections for a specific session"""
        return len(self.connections.get(session_id, set()))
    
    def get_connection_stats(self) -> dict:
        """Get connection statistics"""
        session_stats = {}
        for session_id, connections in self.connections.items():
            session_stats[session_id] = {
                "connection_count": len(connections),
                "total_messages": sum(
                    self.connection_info.get(ws, {}).get("messages_sent", 0)
                    for ws in connections
                )
            }
        
        return {
            "total_connections": self.total_connections,
            "active_sessions": len(self.connections),
            "session_stats": session_stats
        }