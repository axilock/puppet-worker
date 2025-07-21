"""
Plugin manager for the worker component.

Handles downloading, managing, and executing Docker-based plugins.
"""
import logging
import os
import sys
import json
import tempfile
import shutil
from typing import Dict, Any, Optional, List

import docker
from docker.errors import DockerException, ImageNotFound

from config import PLUGIN_CACHE_DIR
from api_client import ApiClientPuppet

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Plugin:
    """Represents a Docker-based plugin."""
    
    def __init__(self, plugin_id: str, version: str, docker_image: str, 
                docker_client: docker.DockerClient,  worker_id: str):
        """Initialize the plugin."""
        self.id = plugin_id
        self.version = version
        self.docker_image = docker_image
        self.docker_client = docker_client
        self.worker_id = worker_id
    
    def run(self, input_data: Dict[str, Any], parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run the plugin with the given input data and parameters.
        
        Args:
            input_data: The input data for the plugin
            parameters: The parameters for the plugin
        
        Returns:
            The result of the plugin execution
        """
        # Create temporary directories for input and output
        
        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = os.path.join(temp_dir, 'input')
            output_dir = os.path.join(temp_dir, 'output')
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(output_dir, exist_ok=True)
            
            # Write input data and parameters to files
            with open(os.path.join(input_dir, 'input.json'), 'w') as f:
                json.dump(input_data, f)
            
            with open(os.path.join(input_dir, 'parameters.json'), 'w') as f:
                json.dump(parameters, f)

            # Run the Docker container
            try:
                logger.info(f"Running plugin {self.id}:{self.version} with Docker image {self.docker_image}")
                
                container = self.docker_client.containers.run(
                    image=self.docker_image,
                    detach=True,
                    volumes={
                        input_dir: {'bind': '/tmp/input', 'mode': 'ro'},
                        output_dir: {'bind': '/tmp/output', 'mode': 'rw'}
                    },
                    environment={
                        'PLUGIN_ID': self.id,
                        'PLUGIN_VERSION': self.version
                    },
                    # mem_limit='512m',
                    cpu_count=1
                )
                
                # Wait for the container to finish
                result = container.wait()
                exit_code = result['StatusCode']
                
                # Get logs
                logs = container.logs().decode('utf-8')
                
                # Clean up the container
                container.remove()
                
                if exit_code != 0:
                    logger.error(f"Plugin {self.id} failed with exit code {exit_code}")
                    raise RuntimeError(f"Plugin failed with exit code {exit_code}: {logs}")
                
                # Read the output
                try:
                    with open(os.path.join(output_dir, 'result.json'), 'r') as f:
                        result = json.load(f)
                    return result
                except (FileNotFoundError, json.JSONDecodeError) as e:
                    logger.error(f"Failed to read plugin output: {e}")
                    raise RuntimeError(f"Plugin did not produce valid output: {e}")
                
            except DockerException as e:
                logger.error(f"Docker error running plugin {self.id}: {e}")
                raise


class PluginManager:
    """Manages Docker-based plugins for the worker."""
    
    def __init__(self, worker_id: Optional[str] = None, cache_dir: str = PLUGIN_CACHE_DIR):
        """
        Initialize the plugin manager.
        
        Args:
            worker_id: ID of the worker this plugin manager belongs to
            cache_dir: Directory to cache plugin data
        """
        self.worker_id = worker_id
        self.cache_dir = cache_dir
        
        self.api_client = ApiClientPuppet()
        self.docker_client = self._init_docker_client()
        self.plugins: Dict[str, Plugin] = {}
        
        # Create cache directory if it doesn't exist
        os.makedirs(self.cache_dir, exist_ok=True)
    
    def _init_docker_client(self):
        """
        Initialize Docker client with support for various Docker setups.
        Handles Docker Desktop, Rancher Desktop, and remote Docker.
        """
        print("🐳 Initializing Docker client...")
        
        # Method 1: Try standard docker.from_env() first
        try:
            client = docker.from_env()
            client.ping()
            logger.info("✅ Connected to Docker via docker.from_env()")
            return client
        except Exception as e:
            print(f"⚠️  docker.from_env() failed: {e}")
        
        # Method 2: Try Rancher Desktop specific paths
        rancher_paths = [
            os.path.expanduser('~/.rd/docker.sock'),
            os.path.expanduser('~/.lima/rancher-desktop/sock/docker.sock'),
            os.path.expanduser('~/.lima/docker/sock/docker.sock'),
            os.path.expanduser('~/.docker/run/docker.sock')
        ]
        
        for socket_path in rancher_paths:
            if os.path.exists(socket_path):
                try:
                    client = docker.DockerClient(base_url=f'unix://{socket_path}')
                    client.ping()
                    print(f"✅ Docker connected via Rancher socket: {socket_path}")
                    return client
                except Exception as e:
                    print(f"❌ Socket {socket_path} failed: {e}")
            
        
        # Method 3: Try standard Docker Desktop paths
        standard_paths = [
            '/var/run/docker.sock',
            '/usr/local/var/run/docker.sock'
        ]
        
        print("🐋 Trying standard Docker socket paths...")
        for socket_path in standard_paths:
            if os.path.exists(socket_path):
                try:
                    client = docker.DockerClient(base_url=f'unix://{socket_path}')
                    client.ping()
                    print(f"✅ Docker connected via standard socket: {socket_path}")
                    return client
                except Exception as e:
                    print(f"❌ Socket {socket_path} failed: {e}")
        
        
        return None

    def _is_running_in_container(self) -> bool:
        """Check if we're running inside a Docker container."""
        try:
            # Check for .dockerenv file
            if os.path.exists('/.dockerenv'):
                return True
            
            # Check cgroup for container indicators
            with open('/proc/1/cgroup', 'r') as f:
                content = f.read()
                return 'docker' in content or 'containerd' in content
        except:
            return False
    
    def has_docker(self) -> bool:
        """Check if Docker client is available."""
        return self.docker_client is not None

    
    def get_plugin(self, plugin_id: str, version: Optional[str] = None) -> Optional[Plugin]:
        """
        Get a plugin by ID and optionally version.
        
        If the plugin is not in the cache, it will be downloaded from the central server.
        If no version is specified, the latest version will be used.
        
        Args:
            plugin_id: The ID of the plugin
            version: The version of the plugin (optional)
        
        Returns:
            The plugin, or None if it could not be found or downloaded
        """
        # Check if we already have this plugin in memory
        cache_key = f"{plugin_id}:{version}" if version else plugin_id
        if cache_key in self.plugins:
            return self.plugins[cache_key]
        
        # Get plugin metadata from the central server
        plugin_data = self.api_client.get_plugin(plugin_id, version)
        if not plugin_data:
            logger.error(f"Plugin {plugin_id} not found on central server")
            return None
        
        # Extract plugin information
        plugin_id = plugin_data['id']
        plugin_version = plugin_data['version']
        docker_image = plugin_data['docker_image']
        
        # Check if we have the Docker image
        try:
            self.docker_client.images.get(docker_image)
            logger.info(f"Plugin {plugin_id}:{plugin_version} already available locally")
        except ImageNotFound:
            # Pull the Docker image
            logger.info(f"Pulling Docker image {docker_image} for plugin {plugin_id}:{plugin_version}")
            try:
                self.docker_client.images.pull(docker_image)
            except DockerException as e:
                logger.error(f"Failed to pull Docker image {docker_image}: {e}")
                return None
        
        # Create and cache the plugin
        plugin = Plugin(plugin_id, plugin_version, docker_image, self.docker_client,  self.worker_id)
        self.plugins[cache_key] = plugin
        return plugin
    
    def update_plugins(self) -> List[str]:
        """
        Update all plugins from the central server.
        
        Returns:
            list of queues needed for all plugins
        """
        try:
            # Get all plugins from the central server
            plugins = self.api_client.get_all_plugins()
            
            # Track all queues from plugins
            all_plugin_queues = set()
            
            # Update each plugin
            for plugin_data in plugins:
                plugin_id = plugin_data['id']
                plugin_version = plugin_data['version']
                docker_image = plugin_data['docker_image']
                plugin_queue = plugin_data.get('queue')

                if plugin_queue:
                    all_plugin_queues.add(plugin_queue)
                
                # Check if the Docker image already exists locally
                logger.info(f"Updating plugin {plugin_id} to version {plugin_version}")
                try:
                    # Try to get the image to see if it exists locally
                    try:
                        self.docker_client.images.get(docker_image)
                        logger.info(f"Docker image {docker_image} already exists locally, skipping pull")
                    except ImageNotFound:
                        # Image doesn't exist locally, so pull it
                        logger.info(f"Pulling Docker image {docker_image}")
                        self.docker_client.images.pull(docker_image)
                    
                    # Update the plugin in the cache
                    cache_key = f"{plugin_id}:{plugin_version}"
                    self.plugins[cache_key] = Plugin(
                        plugin_id, plugin_version, docker_image, self.docker_client, self.worker_id
                    )
                    
                    # Also update the latest version reference
                    self.plugins[plugin_id] = self.plugins[cache_key]
                    
                except DockerException as e:
                    logger.error(f"Failed to update plugin {plugin_id}: {e}")
            
            # Update worker's queue configuration if needed
            if all_plugin_queues:
                self._update_worker_queues(list(all_plugin_queues))
            
            # TODO:  cleanup_old_plugins remove docker and stuff
            return all_plugin_queues
            
        except Exception as e:
            logger.error(f"Failed to update plugins: {e}")
            return []
    
    def _update_worker_queues(self, queues: List[str]) -> None:
        """
        Update the worker's queue configuration based on plugin requirements.
        
        Args:
            queues: List of queue names required by plugins
        """
        if not self.worker_id:
            logger.warning("Cannot update worker queues: worker_id not set")
            return
            
        try:
            # Get current worker information
            worker = self.api_client.get_worker(self.worker_id)
            
            if worker:
                # Update worker's queue configuration
                current_queues = worker.get('queues', [])
                
                # Add any new queues that aren't already in the worker's configuration
                new_queues = list(set(current_queues) | set(queues))
                
                if set(new_queues) != set(current_queues):
                    logger.info(f"Updating worker queues: {current_queues} -> {new_queues}")
                    
                    # Update the worker's queue configuration via API
                    try:
                        # Make a POST request to update the worker's queues
                        response = self.api_client._make_request(
                            'post', 
                            f'workers/{self.worker_id}/queues',
                            data=new_queues
                        )
                        logger.info(f"Worker queues updated successfully: {response}")
                    except Exception as e:
                        logger.error(f"Failed to update worker queues via API: {e}")
        except Exception as e:
            logger.error(f"Failed to update worker queues: {e}")
    
    def cleanup_old_plugins(self) -> None:
        """Clean up old plugin versions to free up disk space."""
        # This is a placeholder for future implementation
        # Could remove old Docker images that are no longer needed
        pass
