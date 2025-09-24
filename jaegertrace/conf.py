#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

__all__ = [
    "get_tracer_config",
    "get_tracing_config",
    "get_service_name",
    "is_component_enabled",
]


# Default tracer configuration for Jaeger
DEFAULT_TRACER_CONFIG = {
    'sampler': {
        'type': 'const',
        'param': 1,
    },
    'local_agent': {
        'reporting_host': 'localhost',
        'reporting_port': 6831,
    },
    'trace_id_header': 'trace-id',
    'baggage_header_prefix': 'jaegertrace-',
    "logging": True,
}

# Default tracing configuration for different components
DEFAULT_TRACING_CONFIG = {
    "http_requests": {
        "ignore_urls": [],
        "max_tag_value_length": 1024,
    },
    "database": {
        "enabled": True,
        "ignore_sqls": [],
        "max_query_length": 1000,
    },
    "redis": {
        "enabled": True,
        "ignore_commands": [],
        "max_command_length": 500,
    },
    "celery": {
        "enabled": True,
    },
}


def get_tracer_config() -> dict:
    """Get tracer configuration from Django settings with defaults."""
    config = getattr(settings, "TRACER_CONFIG", {})
    # Merge with defaults
    merged_config = DEFAULT_TRACER_CONFIG.copy()
    merged_config.update(config)
    return merged_config


def get_tracing_config() -> dict:
    """Get tracing configuration from Django settings with defaults."""
    config = getattr(settings, "TRACING_CONFIG", {})
    # Deep merge with defaults
    merged_config = {}
    for key, default_value in DEFAULT_TRACING_CONFIG.items():
        if key in config:
            if isinstance(default_value, dict):
                merged_config[key] = {**default_value, **config[key]}
            else:
                merged_config[key] = config[key]
        else:
            merged_config[key] = default_value
    return merged_config


def get_service_name() -> str:
    """Get service name for tracing."""
    service_name = getattr(settings, "TRACING_SERVICE_NAME", None)
    if not service_name:
        # Try to get from other common settings
        wsgi_application = getattr(settings, "WSGI_APPLICATION", None)
        if not wsgi_application:
            raise ImproperlyConfigured(
                "TRACING_SERVICE_NAME or WSGI_APPLICATION must be set in Django settings"
            )
        service_name = wsgi_application.split(".")[0]
    return service_name


def is_component_enabled(component_name) -> bool:
    """Check if a specific tracing component is enabled."""
    config = get_tracing_config()
    component_config = config.get(component_name, {})
    return component_config.get("enabled", False)
