# Django Distributed Tracing

A comprehensive Django middleware package for distributed tracing with Jaeger, supporting HTTP requests, Database queries, Redis operations, Celery tasks, and RocketMQ messaging.

I read Jaeger - Distributed Tracing System on [github](https://github.com/jaegertracing/jaeger-client-python) and make it plus.

[![Pypi Version](https://badge.fury.io/py/django-nacos-microservice.svg)](https://badge.fury.io/py/django-jaeger-middleware-plus)

## Features

- **HTTP Request Tracing**: Automatic tracing of incoming HTTP requests and outgoing HTTP calls
- **Database Query Tracing**: Track Django ORM queries with performance metrics
- **Redis Operation Tracing**: Monitor Redis commands and operations
- **Celery Task Tracing**: Distributed tracing across Celery task queues
- **Configurable Components**: Enable/disable specific tracing components
- **Performance Monitoring**: Track slow queries, long-running requests, and bottlenecks
- **Error Tracking**: Automatic error logging and span tagging

## Installation

```bash
pip install django-jaeger-middleware-plus
```

## Quick Start

### 1. Add to Django Settings

```python
# settings.py

INSTALLED_APPS = [
    # ... other apps
    'jaegertrace',
]

MIDDLEWARE = [
    'jaegertrace.middleware.TraceMiddleware',
    # ... other middleware
]

# Required: Service name for tracing
TRACING_SERVICE_NAME = "my-django-service"
```

## Configuration Reference

### Tracer Configuration

```python
TRACER_CONFIG = {
    "sampler": {
        "type": "const",        # const, probabilistic, rate_limiting
        "param": 1,             # Sample rate (0.0 to 1.0)
    },
    "local_agent": {
        "reporting_host": "localhost",
        "reporting_port": 6832,
    },
    "trace_id_header": "trace-id",
    "baggage_header_prefix": "jaeger-",
    "logging": True,
}
```

### Component Configuration

```python
# settings.py

TRACING_CONFIG = {
    "http_requests": {
        "ignore_urls": ["/health", "/metrics", "/favicon.ico"],  # URLs to skip
        "max_tag_value_length": 1024,  # Max length for tag values, default 1024
    },
    "database": {
        "enabled": True,
        "ignore_sqls": ["SHOW TABLES", "DESCRIBE"],  # SQL commands to skip, default ["SHOW TABLES", "DESCRIBE"]
        "max_query_length": 1000,  # Truncate long queries, default 1000
    },
    "redis": {
        "enabled": True,
        "ignore_commands": ["PING", "INFO"],  # Redis commands to skip
        "max_command_length": 500,  # Truncate long commands, default 500
    },
    "celery": {
        "enabled": True,
        "ignore_tasks": [],  # Celery tasks to skip
    }
}
```

## Usage

### 1. Using the Traced HTTP Client

```python
from jaegertrace.httpclient import HttpClient

# Create a traced HTTP client
client = HttpClient()

# Make requests - automatically traced
response = client.get("/users/123")
response = client.post("/users", json={"name": "John"})

```

## Production Considerations

### Sampling

In production, consider using probabilistic sampling to reduce overhead:

```python
TRACER_CONFIG = {
    "sampler": {
        "type": "probabilistic",
        "param": 0.1,  # Sample 10% of traces
    }
}
```

### Performance Impact

- Database query tracing adds minimal overhead (~1-2ms per query)
- HTTP request tracing adds ~5-10ms per request
- Redis tracing adds ~1ms per operation
- Consider disabling SQL logging in production

### Resource Usage

- Each span consumes ~1KB of memory
- Jaeger agent buffers traces locally before sending
- Monitor memory usage with high-throughput applications

## Troubleshooting

### Common Issues

1. **Traces not appearing in Jaeger**
   - Check Jaeger agent connectivity
   - Verify sampling configuration
   - Check service name configuration

2. **High memory usage**
   - Reduce sampling rate
   - Disable detailed logging (SQL, message bodies)
   - Check for span leaks (unfinished spans)

3. **Performance degradation**
   - Tune slow query thresholds
   - Disable non-essential component tracing
   - Use asynchronous reporting

## Debug Mode
Enable debug logging to troubleshoot issues:
```python
# settings.py

LOGGING = {
    'loggers': {
        'django_tracing': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': False,
        },
    },
}
```

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Contact
Email me with any questions: <zhaishuaishuai001@gmail.com>.