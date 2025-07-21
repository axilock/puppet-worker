# Distributed Task Processing System - Worker Component

This is the worker component of the distributed task processing system. It can be run independently from the central server, allowing for easy deployment on multiple machines.

## Overview

The worker component:

- Connects to a central server to receive tasks
- Processes tasks using Docker-based plugins
- Automatically updates plugins from the central server
- Dynamically configures itself to handle new task types
- Reports metrics and status back to the central server

## Requirements

- Python 3.9+
- Redis (accessible from the worker)
- Docker
- Required Python packages (see `setup.py`)

## Installation

### Install from source

```bash
# Clone the repository
git clone https://github.com/0xsapra/puppet-worker.git

cd puppet-worker
```


## Configuration

Rename `.env.example` file to `.env` with desired configration

The worker can be configured using environment variables or a `.env` file:

- `REDIS_HOST`: Redis host (default: localhost)
- `REDIS_PORT`: Redis port (default: 6379)
- `REDIS_DB`: Redis database number (default: 0)
- `REDIS_PASSWORD`: Redis password (default: None)
- `API_HOST`: Central server API host (default: localhost)
- `API_PORT`: Central server API port (default: 8000)
- `WORKER_HEARTBEAT_INTERVAL`: Interval for sending heartbeats (default: 60 seconds)
- `WORKER_PLUGIN_CHECK_INTERVAL`: Interval for checking for plugin updates (default: 300 seconds)
- `WORKER_METRICS_INTERVAL`: Interval for sending metrics (default: 60 seconds)
- `PLUGIN_CACHE_DIR`: Directory to cache plugin data (default: /tmp/task_plugins)

## Running the Worker

```bash
# Start a worker with 4 processes
chmod +x run_worker.sh

./run_worker.sh
```

The worker will automatically fetch queue configuration from the server. No manual queue specification is needed or allowed.

