#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from django.conf import settings

if not settings.configured:
    settings.configure()

# tracer config
DEFAULT_TRACER_CONFIG = {
    'sampler': {
        'type': 'const',
        'param': 1,
    },
    'local_agent': {
        'reporting_host': 'localhost',
        'reporting_port': '5775',
    },
    'trace_id_header': 'trace-id',
    'baggage_header_prefix': 'jaegertrace-',
}
TRACER_CONFIG = getattr(settings, 'TRACER_CONFIG', DEFAULT_TRACER_CONFIG)  # type: dict


# tracing config
DEFAULT_TRACING_CONFIG = {}
"""
example:

TRACING_CONFIG = {
    "http_requests": {
        "enabled": True,
        "trace_headers": True,
        "ignore_urls": ["/health", "/metrics"]
    },
    "database": {
        "enabled": True,
        "slow_query_threshold": 100,  # ms
        "log_sql": True
    },
    "redis": {
        "enabled": True,
        "log_commands": ["GET", "SET", "HGET", "HSET"],
        "ignore_commands": ["PING"]
    },
    "celery": {
        "enabled": True,
        "trace_task_args": False,  # 是否追踪任务参数
        "trace_result": False
    },
    "rocketmq": {
        "enabled": True,
        "trace_message_body": False,
        "max_message_size": 1024
    }
}
"""
TRACING_CONFIG = getattr(settings, "TRACING_CONFIG", DEFAULT_TRACING_CONFIG)  # type: dict
