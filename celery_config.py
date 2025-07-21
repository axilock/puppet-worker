"""
Celery configuration for worker nodes in the distributed task processing system.
"""
import os
import sys
from celery import Celery

import tasks

from config import get_broker_url

# Module-level function that can be pickled
def route_task(task_args, task_kwargs, options, available_queues=None):
    """
    Route tasks to the appropriate queue based on task_type.
    
    Args:
        task_args: Task arguments
        task_kwargs: Task keyword arguments
        options: Task options
        available_queues: List of available queues
        
    Returns:
        Queue name or None
    """
    task_type = task_kwargs.get('task_type')
    
    # Check if the task_type is in our available queues
    if task_type in (available_queues or []):
        return task_type
    
    # Default to the first available queue or 'default' if none available
    return available_queues[0] if available_queues else 'default'

def create_celery_app(queues):
    """
    Create a Celery app configured for worker nodes.
    
    Args:
        queues: List of queue names to consume from (optional)
        
    Returns:
        Configured Celery app instance
    """
    # Create Celery app
    app = Celery('distributed_task_system')
    
    # Store available queues for task routing
    available_queues = queues or []
    
    # Configure Celery
    # Create a partial function with available_queues bound
    from functools import partial
    bound_route_task = partial(route_task, available_queues=available_queues)
    
    app.conf.update(
        broker_url=get_broker_url(),
        result_backend=get_broker_url(),
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone='UTC',
        enable_utc=True,
        task_routes={
            'tasks.process_task': {
                'queue': bound_route_task
            }
        },
        worker_prefetch_multiplier=1,  # Fetch one task at a time
        task_acks_late=True,  # Acknowledge task after it's completed
        task_reject_on_worker_lost=True,  # Reject task if worker is lost
        task_time_limit=1800,  # 30 minutes
    )
    
    
    # Print registered tasks for debugging
    print("Registered tasks:", app.tasks.keys())  
    
    return app
