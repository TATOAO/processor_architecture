"""
Checkpointing and debugging system for the v2 processing pipeline.

Provides state capture, restoration, breakpoints, and debugging capabilities.
"""

import uuid
import pickle
import json
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable, Set
from pathlib import Path

from .interfaces import CheckpointInterface
from .errors import PipelineError


class Checkpoint:
    """Represents a checkpoint with captured state and metadata"""
    
    def __init__(self, checkpoint_id: str, name: str, processor_id: str, 
                 data: Any, metadata: Optional[Dict[str, Any]] = None):
        self.checkpoint_id = checkpoint_id
        self.name = name
        self.processor_id = processor_id
        self.data = data
        self.metadata = metadata or {}
        self.created_at = datetime.now()
        self.size_bytes = self._calculate_size()
        
    def _calculate_size(self) -> int:
        """Calculate approximate size of checkpoint data"""
        try:
            return len(pickle.dumps(self.data))
        except:
            return len(str(self.data).encode('utf-8'))
            
    def to_dict(self) -> Dict[str, Any]:
        """Convert checkpoint to dictionary (without data for efficiency)"""
        return {
            "checkpoint_id": self.checkpoint_id,
            "name": self.name,
            "processor_id": self.processor_id,
            "created_at": self.created_at.isoformat(),
            "size_bytes": self.size_bytes,
            "metadata": self.metadata,
            "data_type": str(type(self.data).__name__)
        }


class Breakpoint:
    """Represents a breakpoint for debugging"""
    
    def __init__(self, processor_id: str, condition: Optional[Callable[[Any], bool]] = None,
                 hit_count: int = 0, enabled: bool = True):
        self.breakpoint_id = str(uuid.uuid4())
        self.processor_id = processor_id
        self.condition = condition
        self.hit_count = hit_count
        self.enabled = enabled
        self.created_at = datetime.now()
        self.total_hits = 0
        self.last_hit: Optional[datetime] = None
        
    def should_break(self, data: Any) -> bool:
        """Check if breakpoint should trigger"""
        if not self.enabled:
            return False
            
        if self.condition and not self.condition(data):
            return False
            
        self.total_hits += 1
        self.last_hit = datetime.now()
        
        # Check hit count (0 means break every time)
        if self.hit_count == 0:
            return True
        else:
            return self.total_hits <= self.hit_count
            
    def to_dict(self) -> Dict[str, Any]:
        """Convert breakpoint to dictionary"""
        return {
            "breakpoint_id": self.breakpoint_id,
            "processor_id": self.processor_id,
            "enabled": self.enabled,
            "hit_count": self.hit_count,
            "total_hits": self.total_hits,
            "created_at": self.created_at.isoformat(),
            "last_hit": self.last_hit.isoformat() if self.last_hit else None,
            "has_condition": self.condition is not None
        }


class CheckpointManager:
    """
    Manages checkpoints and debugging for the processing pipeline.
    
    Provides state capture, restoration, breakpoints, and debugging tools.
    """
    
    def __init__(self, storage_path: Optional[str] = None, max_checkpoints: int = 1000):
        self.storage_path = Path(storage_path) if storage_path else Path("./checkpoints")
        self.max_checkpoints = max_checkpoints
        
        # In-memory storage
        self.checkpoints: Dict[str, Checkpoint] = {}
        self.breakpoints: Dict[str, Breakpoint] = {}
        
        # Thread safety
        self.checkpoints_lock = threading.RLock()
        self.breakpoints_lock = threading.RLock()
        
        # Statistics
        self.total_checkpoints_created = 0
        self.total_checkpoints_restored = 0
        self.total_breakpoints_hit = 0
        
        # Ensure storage directory exists
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        # Load existing checkpoints
        self._load_checkpoints()
        
    def create_checkpoint(self, name: str, processor_id: str, data: Any, 
                         metadata: Optional[Dict[str, Any]] = None, 
                         persist: bool = True) -> str:
        """Create a checkpoint with captured state"""
        checkpoint_id = str(uuid.uuid4())
        
        # Add system metadata
        system_metadata = {
            "created_by": "system",
            "data_size": len(str(data)) if data else 0,
            "persist": persist
        }
        if metadata:
            system_metadata.update(metadata)
            
        checkpoint = Checkpoint(checkpoint_id, name, processor_id, data, system_metadata)
        
        with self.checkpoints_lock:
            # Clean up old checkpoints if needed
            if len(self.checkpoints) >= self.max_checkpoints:
                self._cleanup_old_checkpoints()
                
            self.checkpoints[checkpoint_id] = checkpoint
            self.total_checkpoints_created += 1
            
        # Persist to disk if requested
        if persist:
            self._save_checkpoint(checkpoint)
            
        return checkpoint_id
        
    def restore_checkpoint(self, checkpoint_id: str) -> Any:
        """Restore data from a checkpoint"""
        with self.checkpoints_lock:
            if checkpoint_id not in self.checkpoints:
                # Try loading from disk
                if not self._load_checkpoint(checkpoint_id):
                    raise PipelineError(f"Checkpoint {checkpoint_id} not found")
                    
            checkpoint = self.checkpoints[checkpoint_id]
            self.total_checkpoints_restored += 1
            return checkpoint.data
            
    def list_checkpoints(self, processor_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """List available checkpoints"""
        with self.checkpoints_lock:
            checkpoints = list(self.checkpoints.values())
            
        if processor_id:
            checkpoints = [cp for cp in checkpoints if cp.processor_id == processor_id]
            
        # Sort by creation time (newest first)
        checkpoints.sort(key=lambda cp: cp.created_at, reverse=True)
        
        return [cp.to_dict() for cp in checkpoints]
        
    def delete_checkpoint(self, checkpoint_id: str) -> None:
        """Delete a checkpoint"""
        with self.checkpoints_lock:
            if checkpoint_id in self.checkpoints:
                checkpoint = self.checkpoints[checkpoint_id]
                del self.checkpoints[checkpoint_id]
                
                # Delete from disk
                checkpoint_file = self.storage_path / f"{checkpoint_id}.pkl"
                if checkpoint_file.exists():
                    checkpoint_file.unlink()
                    
    def set_breakpoint(self, processor_id: str, condition: Optional[Callable[[Any], bool]] = None,
                      hit_count: int = 0) -> str:
        """Set a breakpoint at a processor"""
        with self.breakpoints_lock:
            # Remove existing breakpoint for this processor
            existing = [bp for bp in self.breakpoints.values() if bp.processor_id == processor_id]
            for bp in existing:
                del self.breakpoints[bp.breakpoint_id]
                
            # Create new breakpoint
            breakpoint = Breakpoint(processor_id, condition, hit_count)
            self.breakpoints[breakpoint.breakpoint_id] = breakpoint
            
            return breakpoint.breakpoint_id
            
    def remove_breakpoint(self, processor_id: str) -> None:
        """Remove breakpoint from a processor"""
        with self.breakpoints_lock:
            to_remove = [
                bp_id for bp_id, bp in self.breakpoints.items() 
                if bp.processor_id == processor_id
            ]
            for bp_id in to_remove:
                del self.breakpoints[bp_id]
                
    def check_breakpoint(self, processor_id: str, data: Any) -> bool:
        """Check if a breakpoint should trigger"""
        with self.breakpoints_lock:
            for breakpoint in self.breakpoints.values():
                if (breakpoint.processor_id == processor_id and 
                    breakpoint.should_break(data)):
                    self.total_breakpoints_hit += 1
                    return True
        return False
        
    def list_breakpoints(self) -> List[Dict[str, Any]]:
        """List all active breakpoints"""
        with self.breakpoints_lock:
            return [bp.to_dict() for bp in self.breakpoints.values()]
            
    def enable_breakpoint(self, breakpoint_id: str) -> None:
        """Enable a breakpoint"""
        with self.breakpoints_lock:
            if breakpoint_id in self.breakpoints:
                self.breakpoints[breakpoint_id].enabled = True
                
    def disable_breakpoint(self, breakpoint_id: str) -> None:
        """Disable a breakpoint"""
        with self.breakpoints_lock:
            if breakpoint_id in self.breakpoints:
                self.breakpoints[breakpoint_id].enabled = False
                
    def get_checkpoint_data_preview(self, checkpoint_id: str, max_length: int = 500) -> str:
        """Get a preview of checkpoint data"""
        with self.checkpoints_lock:
            if checkpoint_id not in self.checkpoints:
                return "Checkpoint not found"
                
            data = self.checkpoints[checkpoint_id].data
            data_str = str(data)
            
            if len(data_str) <= max_length:
                return data_str
            else:
                return data_str[:max_length] + "... (truncated)"
                
    def create_debug_session(self, processor_id: str) -> "DebugSession":
        """Create a debug session for a processor"""
        return DebugSession(self, processor_id)
        
    def get_statistics(self) -> Dict[str, Any]:
        """Get checkpoint manager statistics"""
        with self.checkpoints_lock, self.breakpoints_lock:
            return {
                "total_checkpoints": len(self.checkpoints),
                "total_breakpoints": len(self.breakpoints),
                "total_checkpoints_created": self.total_checkpoints_created,
                "total_checkpoints_restored": self.total_checkpoints_restored,
                "total_breakpoints_hit": self.total_breakpoints_hit,
                "storage_path": str(self.storage_path),
                "active_breakpoints": len([bp for bp in self.breakpoints.values() if bp.enabled]),
                "checkpoint_size_bytes": sum(cp.size_bytes for cp in self.checkpoints.values())
            }
            
    def _cleanup_old_checkpoints(self) -> None:
        """Remove oldest checkpoints to maintain limit"""
        # Sort by creation time and remove oldest
        sorted_checkpoints = sorted(
            self.checkpoints.items(), 
            key=lambda x: x[1].created_at
        )
        
        # Remove oldest 10% to make room
        to_remove = int(self.max_checkpoints * 0.1)
        for checkpoint_id, checkpoint in sorted_checkpoints[:to_remove]:
            del self.checkpoints[checkpoint_id]
            
            # Delete from disk
            checkpoint_file = self.storage_path / f"{checkpoint_id}.pkl"
            if checkpoint_file.exists():
                checkpoint_file.unlink()
                
    def _save_checkpoint(self, checkpoint: Checkpoint) -> None:
        """Save checkpoint to disk"""
        try:
            checkpoint_file = self.storage_path / f"{checkpoint.checkpoint_id}.pkl"
            with open(checkpoint_file, 'wb') as f:
                pickle.dump({
                    'checkpoint': checkpoint,
                    'metadata': checkpoint.to_dict()
                }, f)
                
            # Also save metadata as JSON for easy browsing
            metadata_file = self.storage_path / f"{checkpoint.checkpoint_id}.json"
            with open(metadata_file, 'w') as f:
                json.dump(checkpoint.to_dict(), f, indent=2)
                
        except Exception as e:
            # Don't fail if we can't persist
            pass
            
    def _load_checkpoint(self, checkpoint_id: str) -> bool:
        """Load checkpoint from disk"""
        try:
            checkpoint_file = self.storage_path / f"{checkpoint_id}.pkl"
            if not checkpoint_file.exists():
                return False
                
            with open(checkpoint_file, 'rb') as f:
                data = pickle.load(f)
                checkpoint = data['checkpoint']
                self.checkpoints[checkpoint_id] = checkpoint
                return True
                
        except Exception:
            return False
            
    def _load_checkpoints(self) -> None:
        """Load all checkpoints from disk on startup"""
        if not self.storage_path.exists():
            return
            
        for checkpoint_file in self.storage_path.glob("*.pkl"):
            checkpoint_id = checkpoint_file.stem
            self._load_checkpoint(checkpoint_id)


class DebugSession:
    """Debug session for interactive debugging of a processor"""
    
    def __init__(self, checkpoint_manager: CheckpointManager, processor_id: str):
        self.checkpoint_manager = checkpoint_manager
        self.processor_id = processor_id
        self.session_id = str(uuid.uuid4())
        self.created_at = datetime.now()
        self.step_count = 0
        
    def step(self) -> None:
        """Step to next breakpoint"""
        self.step_count += 1
        
    def create_checkpoint(self, name: str, data: Any) -> str:
        """Create a checkpoint in this debug session"""
        session_name = f"debug_{self.session_id}_{name}"
        return self.checkpoint_manager.create_checkpoint(
            session_name, 
            self.processor_id, 
            data,
            {"debug_session": self.session_id, "step": self.step_count}
        )
        
    def get_session_checkpoints(self) -> List[Dict[str, Any]]:
        """Get all checkpoints created in this session"""
        all_checkpoints = self.checkpoint_manager.list_checkpoints(self.processor_id)
        return [
            cp for cp in all_checkpoints 
            if cp.get("metadata", {}).get("debug_session") == self.session_id
        ] 