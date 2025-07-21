"""
API client for the worker to communicate with the central server.
"""
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, Any, Optional, List

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from requests.exceptions import RequestException

from config import API_HOST, AUTH_USERNAME, AUTH_PASSWORD
from models import TaskStatus, TaskResult, WorkerMetrics

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ApiClientPuppet:
    """Client for interacting with the central server API."""
    
    def __init__(self, host: str = API_HOST):
        """Initialize the API client."""
        self.base_url = f"{host}/api"
        self.session = requests.Session()
        self.auth = (AUTH_USERNAME, AUTH_PASSWORD)
    
    def _make_request(self, method: str, endpoint: str, data: Optional[Dict[str, Any]] = None,
                     params: Optional[Dict[str, Any]] = None, retries: int = 3) -> Dict[str, Any]:
        """Make a request to the API with retry logic."""
        url = f"{self.base_url}/{endpoint}"
        
        for attempt in range(retries):
            try:
                if method.lower() == 'get':
                    response = self.session.get(url, params=params, auth=self.auth, timeout=10)
                elif method.lower() == 'post':
                    response = self.session.post(url, json=data, auth=self.auth, timeout=10)
                elif method.lower() == 'put':
                    response = self.session.put(url, json=data, auth=self.auth, timeout=10)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                response.raise_for_status()
                return response.json()
                
            except RequestException as e:
                logger.error(f"API request failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt == retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def register_worker(self, worker_data: Dict[str, Any]) -> str:
        """Register a worker with the central server."""
        try:
            response = self._make_request('post', 'workers/register', data=worker_data)
            return response.get('worker_id')
        except Exception as e:
            logger.error(f"Failed to register worker: {e}")
            raise
    
    def send_heartbeat(self, worker_id: str, status: str, resources: Dict[str, Any]) -> bool:
        """Send a heartbeat to the central server."""
        try:
            self._make_request('post', f'workers/{worker_id}/heartbeat', data={
                'status': status,
                'resources': resources
            })
            return True
        except Exception as e:
            logger.error(f"Failed to send heartbeat: {e}")
            return False
    
    def update_task_status(self, task_id: str, status: TaskStatus, 
                          worker_id: Optional[str] = None, 
                          started_at: Optional[datetime] = None) -> bool:
        """Update the status of a task."""
        try:
            data = {'status': status}
            if worker_id:
                data['worker_id'] = worker_id
            if started_at:
                data['started_at'] = started_at
            
            self._make_request('post', f'tasks/{task_id}/status', data=data)
            return True
        except Exception as e:
            logger.error(f"Failed to update task status: {e}")
            return False
    
    def submit_task_result(self, result: TaskResult) -> bool:
        """Submit the result of a completed task."""
        try:
            self._make_request('post', 'results', data=result.dict())
            return True
        except Exception as e:
            logger.error(f"Failed to submit task result: {e}")
            return False
    
    def get_pending_tasks(self, task_type: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """Get pending tasks from the central server."""
        try:
            params = {'status': 'pending', 'limit': limit}
            if task_type:
                params['type'] = task_type
            
            return self._make_request('get', 'tasks', params=params)
        except Exception as e:
            logger.error(f"Failed to get pending tasks: {e}")
            return []
    
    def get_plugin(self, plugin_id: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get a plugin from the central server."""
        try:
            params = {}
            if version:
                params['version'] = version
            
            return self._make_request('get', f'plugins/{plugin_id}', params=params)
        except Exception as e:
            logger.error(f"Failed to get plugin {plugin_id}: {e}")
            return None
    
    def get_all_plugins(self) -> List[Dict[str, Any]]:
        """Get all plugins from the central server."""
        try:
            return self._make_request('get', 'plugins')
        except Exception as e:
            logger.error(f"Failed to get plugins: {e}")
            return []
    
    def get_worker(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """Get worker information from the central server."""
        try:
            return self._make_request('get', f'workers/{worker_id}')
        except Exception as e:
            logger.error(f"Failed to get worker {worker_id}: {e}")
            return None
    
    def submit_metrics(self, metrics: WorkerMetrics) -> bool:
        """Submit worker metrics to the central server."""
        try:
            self._make_request('post', 'metrics/worker', data=metrics.dict())
            return True
        except Exception as e:
            logger.error(f"api_clinet:Failed to submit metrics: {e}")
            return False
