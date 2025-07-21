"""
Configuration settings for the worker component.
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

# API Server configuration (central server)
API_HOST = os.getenv('API_HOST', 'localhost')
AUTH_USERNAME = os.getenv('AUTH_USERNAME', 'admin')
AUTH_PASSWORD = os.getenv('AUTH_PASSWORD', 'securepassword')

# Worker configuration
WORKER_HEARTBEAT_INTERVAL = int(os.getenv('WORKER_HEARTBEAT_INTERVAL', 60))  # seconds  tell worker status (idle/processing)
WORKER_PLUGIN_CHECK_INTERVAL = int(os.getenv('WORKER_PLUGIN_CHECK_INTERVAL', 600))  # seconds (10min update) [after how long will it check for new plugins]
WORKER_METRICS_INTERVAL = int(os.getenv('WORKER_METRICS_INTERVAL', 1800))  # seconds  (30min) [send computer resource data to server ]

# Task configuration
TASK_RETRY_LIMIT = int(os.getenv('TASK_RETRY_LIMIT', 3))
TASK_TIMEOUT = int(os.getenv('TASK_TIMEOUT', 1800))  # 30 minutes in seconds

# Plugin configuration
PLUGIN_CACHE_DIR = os.getenv('PLUGIN_CACHE_DIR', '/tmp/task_plugins')

# No hardcoded queue names - all queues come from the server

# Broker URL for Celery
def get_broker_url():
    """Generate the broker URL for Celery based on Redis configuration."""
    auth = f":{REDIS_PASSWORD}@" if REDIS_PASSWORD else ""
    return f"redis://{auth}{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
