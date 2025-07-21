#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
else
  echo "Error: .env file not found. Please create one based on .env.example"
  exit 1
fi

# Override WORKER_COUNT if provided as command-line argument
if [ ! -z "$1" ]; then
  WORKER_COUNT=$1
fi

# Set environment variables for the worker
export REDIS_HOST=$REDIS_HOST
export REDIS_PORT=$REDIS_PORT
export REDIS_PASSWORD=$REDIS_PASSWORD
export API_HOST=$API_HOST

echo "Starting worker with server at localhost:$API_PORT and Redis at localhost:$REDIS_PORT"
echo "Number of workers: $WORKER_COUNT"

# Run the worker
python -m worker --server $API_HOST --workers $WORKER_COUNT
