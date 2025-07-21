"""
Setup script for the worker component of the distributed task processing system.
"""
from setuptools import setup, find_packages

setup(
    name="distributed-task-worker",
    version="1.0.0",
    description="Worker component for the distributed task processing system",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    install_requires=[
        "celery>=5.2.0",
        "redis>=4.0.0",
        "requests>=2.25.0",
        "pydantic>=1.8.0",
        "docker>=5.0.0",
        "psutil>=5.8.0",
        "python-dotenv>=0.19.0",
    ],
    entry_points={
        "console_scripts": [
            "task-worker=worker.worker:main",
        ],
    },
    python_requires=">=3.9",
)
