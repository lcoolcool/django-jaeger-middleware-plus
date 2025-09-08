"""
Custom exceptions for the tracing package.
"""


class TracingError(Exception):
    """Base exception for tracing-related errors."""
    pass


class TracingConfigError(TracingError):
    """Exception raised for configuration errors."""
    pass


class TracingInitializationError(TracingError):
    """Exception raised when tracer initialization fails."""
    pass


class SpanNotFoundError(TracingError):
    """Exception raised when trying to access a span that doesn't exist."""
    pass
