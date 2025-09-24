"""
Instrumentation package for different components.
Contains auto-instrumentation for various libraries and frameworks.
"""
from .database import DatabaseInstrumentation
from .redis import RedisInstrumentation
from .celery import CeleryInstrumentation

__all__ = [
    "DatabaseInstrumentation",
    "RedisInstrumentation",
    "CeleryInstrumentation",
]
