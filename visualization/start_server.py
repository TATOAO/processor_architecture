#!/usr/bin/env python3
"""
Startup script for the processor pipeline visualization server.

This script starts both the FastAPI backend and can optionally start the 
Next.js frontend in development mode.
"""

import os
import sys
import subprocess
import time
import signal
import threading
from pathlib import Path

def check_dependencies():
    """Check if required dependencies are installed"""
    try:
        import fastapi
        import uvicorn
        import aiohttp
        print("✅ Backend dependencies found")
        return True
    except ImportError as e:
        print(f"❌ Missing backend dependencies: {e}")
        print("Install with: pip install processor-pipeline[monitoring]")
        return False

def start_backend():
    """Start the FastAPI backend server"""
    backend_dir = Path(__file__).parent / "backend"
    os.chdir(backend_dir)
    
    print("🚀 Starting FastAPI backend server...")
    cmd = [sys.executable, "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
    return subprocess.Popen(cmd)

def start_frontend():
    """Start the Next.js frontend server"""
    frontend_dir = Path(__file__).parent / "frontend"
    
    # Check if node_modules exists
    if not (frontend_dir / "node_modules").exists():
        print("📦 Installing frontend dependencies...")
        os.chdir(frontend_dir)
        subprocess.run(["npm", "install"], check=True)
    
    print("🎨 Starting Next.js frontend server...")
    os.chdir(frontend_dir)
    cmd = ["npm", "run", "dev"]
    return subprocess.Popen(cmd)

def check_ports():
    """Check if required ports are available"""
    import socket
    
    def is_port_open(port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(('localhost', port)) != 0
    
    if not is_port_open(8000):
        print("⚠️  Port 8000 is already in use")
        return False
    
    if not is_port_open(3000):
        print("⚠️  Port 3000 is already in use")
        return False
    
    return True

def main():
    """Main startup function"""
    print("🔧 Processor Pipeline Visualization Server")
    print("=" * 50)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Check ports
    if not check_ports():
        print("\n💡 You can still run the backend only on port 8000")
        response = input("Continue with backend only? (y/N): ")
        if response.lower() != 'y':
            sys.exit(1)
    
    processes = []
    
    try:
        # Start backend
        backend_process = start_backend()
        processes.append(backend_process)
        
        # Wait a moment for backend to start
        time.sleep(2)
        
        # Check if we should start frontend
        frontend_dir = Path(__file__).parent / "frontend"
        if frontend_dir.exists() and (frontend_dir / "package.json").exists():
            try:
                frontend_process = start_frontend()
                processes.append(frontend_process)
            except (subprocess.CalledProcessError, FileNotFoundError):
                print("⚠️  Could not start frontend. Make sure Node.js and npm are installed.")
                print("   You can still use the backend API at http://localhost:8000")
        else:
            print("⚠️  Frontend not found. Running backend only.")
        
        print("\n✅ Server started successfully!")
        print("📊 Backend API: http://localhost:8000")
        print("📊 API Documentation: http://localhost:8000/docs")
        if len(processes) > 1:
            print("🎨 Frontend UI: http://localhost:3000")
        
        print("\n🔍 Monitoring your pipelines:")
        print("   from processor_pipeline.monitoring import enable_monitoring")
        print("   monitored = enable_monitoring(your_pipeline)")
        
        print("\n📝 Press Ctrl+C to stop the servers")
        
        # Wait for processes
        while True:
            time.sleep(1)
            # Check if any process has died
            for i, process in enumerate(processes):
                if process.poll() is not None:
                    print(f"\n❌ Process {i} has stopped unexpectedly")
                    break
    
    except KeyboardInterrupt:
        print("\n🛑 Shutting down servers...")
    
    finally:
        # Clean up processes
        for process in processes:
            try:
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        
        print("✅ Servers stopped")

if __name__ == "__main__":
    main()