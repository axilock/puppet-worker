"""
Worker implementation for the distributed task processing system.
FIXED VERSION - keeps all original functionality while solving pickling issues.
"""
import logging
import os
import sys
import time
import uuid
import socket
import threading
import multiprocessing
import signal
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse

import psutil
from celery import Celery

from config import (
    WORKER_HEARTBEAT_INTERVAL, WORKER_PLUGIN_CHECK_INTERVAL, WORKER_METRICS_INTERVAL
)
from models import Worker, WorkerStatus, WorkerResources, WorkerMetrics
from api_client import ApiClientPuppet
from plugin_manager import PluginManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_single_worker_process(server_url: str, worker_index: int):
    """
    Module-level function to run a single worker process.
    This avoids pickling issues by creating all objects fresh in the new process.
    
    Args:
        server_url: URL of the central server
        worker_index: Index of this worker for identification
    """
    # Set up signal handlers for graceful shutdown
    stop_event = threading.Event()
    
    def signal_handler(signum, frame):
        logger.info(f"Worker {worker_index} received signal {signum}, shutting down...")
        stop_event.set()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    worker = None
    try:
        logger.info(f"Starting worker process {worker_index}")
        
        # Create worker process - all objects are created fresh in this process
        worker = WorkerProcess(server_url)
        
        # Actually start the worker (this was missing in original!)
        worker.start()
        
        # Keep worker running until stop signal
        while not stop_event.is_set() and not worker.stop_event.is_set():
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info(f"Worker {worker_index} interrupted")
    except Exception as e:
        logger.error(f"Worker {worker_index} failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if worker:
            try:
                worker.stop()
            except Exception as e:
                logger.error(f"Error stopping worker {worker_index}: {e}")
        
        logger.info(f"Worker {worker_index} process ended")


class WorkerProcess:
    """
    Represents a worker process that processes tasks from the queue.
    
    This class manages the lifecycle of a single worker process, including
    registration, heartbeats, metrics reporting, and plugin updates.
    """
    
    def __init__(self, server_url: str):
        """
        Initialize the worker process.
        
        Args:
            server_url: URL of the central server
        """
        self.server_url = server_url
        self.worker_id = str(uuid.uuid4())
        self.hostname = socket.gethostname()
        
        try:
            self.ip = socket.gethostbyname(self.hostname)
        except socket.gaierror:
            self.ip = "127.0.0.1"
        
        # Parse server URL safely
        self._parse_server_url()
        # Initialize API client and plugin manager
        self.api_client = ApiClientPuppet(self.server_url)
        self.plugin_manager = PluginManager(worker_id=self.worker_id)
        
        # Worker state
        self.status = WorkerStatus.IDLE.value
        self.active_tasks = []
        self.tasks_processed = 0
        self.tasks_succeeded = 0
        self.tasks_failed = 0
        self.stop_event = threading.Event()
        self.celery_worker_process = None
        self.celery_app = None  # Store reference to Celery app
        
        # Initialize with empty queue list - will get queues from server during registration
        self.queues = []
    
    def _parse_server_url(self):
        """Parse server URL safely to avoid the original fragile parsing."""
        try:
            if '://' in self.server_url:
                # Full URL format
                parsed = urlparse(self.server_url)
                self.server_host = parsed.hostname or "localhost"
                self.server_port = parsed.port or 8000
            else:
                # Simple host:port format
                parts = self.server_url.split(':')
                self.server_host = parts[0] if parts[0] else "localhost"
                self.server_port = int(parts[1]) if len(parts) > 1 else 8000
        except Exception as e:
            logger.error(f"Failed to parse server URL '{self.server_url}': {e}")
            self.server_host = "localhost"
            self.server_port = 8000
    
    def start(self):
        """Start the worker process."""
        logger.info(f"Starting worker {self.worker_id} on {self.hostname} ({self.ip})")
        
        # Register with the central server
        self._register() # register this work to webserver
        
        # Start background threads
        self._start_heartbeat_thread()
        self._start_plugin_update_thread()
        self._start_metrics_thread()

        # Start the Celery worker with the retrieved queues
        self._start_celery_worker()
        
    
    def stop(self):
        """Stop the worker process."""
        logger.info(f"Stopping worker {self.worker_id}")
        self.stop_event.set()
        
        # Stop Celery worker process if it exists
        if self.celery_worker_process and self.celery_worker_process.is_alive():
            logger.info("Terminating Celery worker process...")
            self.celery_worker_process.terminate()
            
            # Wait for graceful shutdown
            self.celery_worker_process.join(timeout=10)
            
            # Force kill if still alive
            if self.celery_worker_process.is_alive():
                logger.warning("Force killing Celery worker process...")
                self.celery_worker_process.kill()
                self.celery_worker_process.join()
        
        # Update status to offline
        self.status = WorkerStatus.OFFLINE.value
        resources = self._get_resources()
        try:
            self.api_client.send_heartbeat(self.worker_id, self.status, resources.dict())
        except Exception as e:
            logger.error(f"Failed to update status to offline: {e}")
    
    def _register(self):
        """Register the worker with the central server and start the Celery worker."""
        resources = self._get_resources()
        
        worker = Worker(
            id=self.worker_id,
            hostname=self.hostname,
            ip=self.ip,
            status=WorkerStatus.IDLE.value,
            resources=resources,
            active_tasks=[],
            plugins={},
            queues=self.queues,
            registered_at=str(datetime.utcnow())
        )
        
        try:
            # Register with the central server
            self.api_client.register_worker(worker.dict())
            logger.info(f"Worker {self.worker_id} registered successfully")
            
            # Get all plugins from the server and extract their queues
            logger.info("Fetching plugins from server...")
            self.queues = self.plugin_manager.update_plugins()
            logger.info("Plugins fetched successfully")
            
            # Get queue configuration from server
            if not self.queues:
                logger.error("No queues assigned by server. Exiting.")
                sys.exit(1)
            
                
        except Exception as e:
            logger.error(f"Failed to register worker: {e}")
            raise
    
    def _get_resources(self) -> WorkerResources:
        """Get the current resource usage."""
        try:
            memory = psutil.virtual_memory()
            return WorkerResources(
                ram_total=memory.total / (1024 * 1024 * 1024),  # GB
                ram_available=memory.available / (1024 * 1024 * 1024),  # GB
                cpu_usage=psutil.cpu_percent()
            )
        except Exception as e:
            logger.error(f"Failed to get system resources: {e}")
            # Return default values in case of error
            return WorkerResources(ram_total=0, ram_available=0, cpu_usage=0)
    
    def _start_heartbeat_thread(self):
        """Start the heartbeat thread."""
        def heartbeat_loop():
            while not self.stop_event.is_set():
                try:
                    resources = self._get_resources()
                    self.api_client.send_heartbeat(self.worker_id, self.status, resources.dict())
                    logger.debug(f"Sent heartbeat for worker {self.worker_id}")
                    
                except Exception as e:
                    logger.error(f"Failed to send heartbeat: {e}")
                
                # Sleep until next heartbeat or stop event
                self.stop_event.wait(WORKER_HEARTBEAT_INTERVAL)
        
        thread = threading.Thread(target=heartbeat_loop, daemon=True)
        thread.start()
        logger.info("Heartbeat thread started")
    
   
        
    def _update_celery_queues(self, new_queues):
        """
        Update Celery worker queues without restarting the worker.
        
        This method uses Celery's control API to dynamically add or remove queues
        from a running worker, which is much more efficient than restarting the
        entire worker process.
        
        Args:
            new_queues: List of new queues to consume from
        """
        if not new_queues:
            logger.error("No queues available to consume from. Exiting.")
            sys.exit(1)
            
        # Get current queues
        old_queues = self.queues.copy()
        
        # Update self.queues
        self.queues = new_queues
        
        logger.info(f"Updating Celery worker queues: {old_queues} -> {new_queues}")
        
        # Get worker name
        worker_name = f"worker-{self.worker_id}@{self.hostname}"
        
        # Find queues to add and remove
        queues_to_add = [q for q in new_queues if q not in old_queues]
        queues_to_remove = [q for q in old_queues if q not in new_queues]
        
        # Add new queues
        if queues_to_add:
            logger.info(f"Adding queues: {queues_to_add}")
            for queue in queues_to_add:
                try:
                    self.celery_app.control.add_consumer(
                        queue=queue,
                        destination=[worker_name]
                    )
                    logger.info(f"Added queue: {queue}")
                except Exception as e:
                    logger.error(f"Failed to add queue {queue}: {e}")
        
        # Remove old queues
        if queues_to_remove:
            logger.info(f"Removing queues: {queues_to_remove}")
            for queue in queues_to_remove:
                try:
                    self.celery_app.control.cancel_consumer(
                        queue=queue,
                        destination=[worker_name]
                    )
                    logger.info(f"Removed queue: {queue}")
                except Exception as e:
                    logger.error(f"Failed to remove queue {queue}: {e}")
        
        logger.info(f"Celery worker queues updated to: {new_queues}")
    
    def _start_plugin_update_thread(self):
        """Start the plugin update thread."""
        def plugin_update_loop():
            while not self.stop_event.is_set():
                try:
                    all_plugin_queues = self.plugin_manager.update_plugins()
                    logger.info("Plugins updated")
                    
                    # Check if there are any queues from plugins
                    if all_plugin_queues:
                        # Convert to list if it's not already (since it might be a set)
                        all_plugin_queues_list = list(all_plugin_queues)
                        
                        # Always update self.queues to be the complete list of all plugin queues
                        if set(all_plugin_queues_list) != set(self.queues):
                            logger.info(f"Queue configuration changed, updating Celery worker queues")
                            # Update Celery worker queues without restarting
                            self._update_celery_queues(all_plugin_queues_list)
                except Exception as e:
                    logger.error(f"Failed to update plugins: {e}")
                
                # Sleep until next update check or stop event
                self.stop_event.wait(WORKER_PLUGIN_CHECK_INTERVAL)
        
        thread = threading.Thread(target=plugin_update_loop, daemon=True)
        thread.start()
        logger.info("Plugin update thread started")
    
    def _start_metrics_thread(self):
        """Start the metrics reporting thread."""
        def metrics_loop():
            while not self.stop_event.is_set():
                try:
                    metrics = WorkerMetrics(
                        worker_id=self.worker_id,
                        hostname=self.hostname,
                        timestamp=str(datetime.utcnow()),
                        ram_usage_percent=psutil.virtual_memory().percent,
                        cpu_usage_percent=psutil.cpu_percent(),
                        tasks_processed=self.tasks_processed,
                        tasks_succeeded=self.tasks_succeeded,
                        tasks_failed=self.tasks_failed,
                        active_task_count=len(self.active_tasks)
                    )
                    self.api_client.submit_metrics(metrics)
                    logger.debug(f"Submitted metrics for worker {self.worker_id}")
                except Exception as e:
                    logger.error(f"worker.py: Failed to submit metrics: {e}")
                
                # Sleep until next metrics report or stop event
                self.stop_event.wait(WORKER_METRICS_INTERVAL)
        
        thread = threading.Thread(target=metrics_loop, daemon=True)
        thread.start()
        logger.info("Metrics thread started")
    
    def _start_celery_worker(self):
        """Start the Celery worker process."""
        # Check if there are any queues to consume from
        if not self.queues:
            logger.error("No queues available to consume from. Exiting.")
            sys.exit(1)
            
        logger.info(f"Starting Celery worker with queues: {self.queues}")
        
        # Create Celery app using local configuration
        from celery_config import create_celery_app
        self.celery_app = create_celery_app(self.queues)
        
        # Start worker in a new process
        worker_name = f"worker-{self.worker_id}"
        argv = [
            'worker',
            '--loglevel=info',
            f'--hostname={worker_name}@{self.hostname}',
            f'--queues={",".join(self.queues)}',
            '--without-gossip',  # Disable gossip for better performance
            '--without-mingle',  # Disable mingle for better performance
            '--concurrency=1',   # One task at a time per worker process
        ]
        
        # Start the worker process
        process = multiprocessing.Process(
            target=self.celery_app.worker_main,
            args=(argv,)
        )
        process.start()
        
        # Store the reference to the worker process
        self.celery_worker_process = process
        
        logger.info(f"Celery worker started with queues: {self.queues}")


class WorkerManager:
    """
    Manages multiple worker processes.
    
    This class is responsible for starting and stopping multiple worker processes
    based on the specified count.
    """
    
    def __init__(self, server_url: str):
        """
        Initialize the worker manager.
        
        Args:
            server_url: URL of the central server
        """
        self.server_url = server_url
        self.worker_processes = []
    
    def start_workers(self, count: int):
        """
        Start the specified number of worker processes.
        
        Args:
            count: Number of worker processes to start
        """
        logger.info(f"Starting {count} worker processes")
        
        for i in range(count):
            try:
                # Create a new worker process using the module-level function
                # This avoids pickling issues by not trying to pickle complex objects
                process = multiprocessing.Process(
                    target=run_single_worker_process,  # Module-level function
                    args=(self.server_url, i),
                    name=f"worker-{i}"
                )
                process.start()
                self.worker_processes.append(process)
                logger.info(f"Started worker process {i} (PID: {process.pid})")
                
            except Exception as e:
                logger.error(f"Failed to start worker process {i}: {e}")
        
        logger.info(f"Started {len(self.worker_processes)} worker processes")
    
    def stop_workers(self):
        """Stop all worker processes."""
        logger.info(f"Stopping {len(self.worker_processes)} worker processes")
        
        # Terminate all processes
        for i, process in enumerate(self.worker_processes):
            if process.is_alive():
                logger.info(f"Terminating worker process {i} (PID: {process.pid})")
                process.terminate()
        
        # Wait for all processes to terminate gracefully
        for i, process in enumerate(self.worker_processes):
            try:
                process.join(timeout=10)
                if process.is_alive():
                    logger.warning(f"Force killing worker process {i}")
                    process.kill()
                    process.join()
                logger.info(f"Worker process {i} stopped")
            except Exception as e:
                logger.error(f"Error stopping worker process {i}: {e}")
        
        self.worker_processes = []
        logger.info("All worker processes stopped")


def main():
    """Main entry point for the worker."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Start worker processes')
    parser.add_argument('--server', required=True, help='Central server URL (host:port)')
    parser.add_argument('--workers', type=int, default=1, help='Number of worker processes to start')
    
    args = parser.parse_args()
    
    # Start worker manager
    manager = WorkerManager(args.server)
    
    try:
        # Start workers with no predefined queues - all queues will be fetched from server
        manager.start_workers(args.workers)
        
        # Keep the main process running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Interrupted, stopping workers")
    finally:
        manager.stop_workers()


if __name__ == "__main__":
    main()