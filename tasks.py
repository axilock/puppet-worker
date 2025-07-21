"""
Celery tasks for the worker component.
"""
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional

from celery import Task, shared_task
from celery.exceptions import SoftTimeLimitExceeded

from config import TASK_RETRY_LIMIT, TASK_TIMEOUT
from models import TaskStatus, TaskResult
from plugin_manager import PluginManager
from api_client import ApiClientPuppet
from celery.signals import task_unknown


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TaskProcessor(Task):
    """Base task class with error handling and retry logic."""
    
    # Retry settings
    max_retries = TASK_RETRY_LIMIT
    default_retry_delay = 60  # 1 minute
    
    # Plugins and API client
    _plugin_manager = None
    _api_client = None
    
    @property
    def plugin_manager(self) -> PluginManager:
        """Get or create the plugin manager."""
        if self._plugin_manager is None:
            self._plugin_manager = PluginManager(worker_id="UNKNOWN")
        return self._plugin_manager
    
    @property
    def api_client(self) -> ApiClientPuppet:
        """Get or create the API client."""
        if self._api_client is None:
            self._api_client = ApiClientPuppet()
        return self._api_client
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Handle task failure."""
        task_data = kwargs.get('task_data', {})
        task_id = task_data.get('id')
        worker_id = kwargs.get('worker_id')
        
        if task_id:
            # Update task status in the database
            if self.request.retries >= self.max_retries:
                status = TaskStatus.MANUAL_REVIEW
                logger.error(f"Task {task_id} failed after {self.max_retries} retries, marking for manual review")
            else:
                status = TaskStatus.FAILED
                logger.error(f"Task {task_id} failed, will retry ({self.request.retries + 1}/{self.max_retries})")
            
            # Submit task result
            result = TaskResult(
                task_id=task_id,
                success=False,
                error=str(exc),
                processing_time=time.time() - kwargs.get('start_time', time.time()),
                worker_id="None",
                completed_at=str(datetime.utcnow())
            )
            
            try:
                self.api_client.submit_task_result(result)
            except Exception as e:
                logger.error(f"Failed to submit task result: {e}")


@task_unknown.connect
def task_unknown_handler(sender=None, name=None, id=None, message=None, exc=None, traceback=None, **kwargs):
    """
    Handle unknown/unregistered tasks by logging and optionally requeuing.
    """
    
    
    logger.error(f"Unknown task received: {name} with id: {id}")
    
    # Extract task details from the message
    if message and hasattr(message, 'body'):
        try:
            import json
            task_data = json.loads(message.body)
            logger.info(f"Task data: {task_data}")
            
            # You could implement custom requeuing logic here
            # For example, send to a dead letter queue or retry mechanism
            
        except Exception as e:
            logger.error(f"Failed to parse task message: {e}")

@shared_task(bind=True, base=TaskProcessor, time_limit=TASK_TIMEOUT, name='tasks.process_task')
def process_task(self, task_data: Dict[str, Any], worker_id: str, task_type: str) -> Dict[str, Any]:
    """
    Process a task using the appropriate plugin.
    
    Args:
        task_data: The task data from the database
        worker_id: The ID of the worker processing this task
        task_type: The type of task (domain, repository, etc.)
    
    Returns:
        The result of the task processing
    """
    start_time = time.time()
    task_id = task_data.get('id')
    plugin_id = task_data.get('plugin_id')
    plugin_parameters = task_data.get('plugin_parameters', {})
    input_data = task_data.get('input_data', {})
    
    logger.info(f"Processing task {task_id} of type {task_type} with plugin {plugin_id}")
    
    try:
        # Update task status to in_process
        self.api_client.update_task_status(
            task_id=task_id,
            status=TaskStatus.IN_PROCESS.value,
            worker_id=worker_id,
            started_at=str(datetime.utcnow())
        )
        
        # Get the plugin
        plugin = self.plugin_manager.get_plugin(plugin_id)
        if not plugin:
            raise ValueError(f"Plugin {plugin_id} not found")
        
        # Run the plugin
        result = plugin.run(input_data, plugin_parameters)
        
        # Calculate processing time
        processing_time = time.time() - start_time
        
        # Submit task result
        task_result = TaskResult(
            task_id=task_id,
            success=True,
            data=result,
            processing_time=processing_time,
            worker_id=plugin.worker_id,
            completed_at=str(datetime.utcnow())
        )
        
        self.api_client.submit_task_result(task_result)
        
        logger.info(f"Task {task_id} completed successfully in {processing_time:.2f} seconds")
        return result
        
    except SoftTimeLimitExceeded:
        logger.error(f"Task {task_id} timed out after {TASK_TIMEOUT} seconds")
        raise
        
    except Exception as e:
        logger.error(f"Error processing task {task_id}: {e}")
        # This will trigger on_failure which will handle retries
        raise
