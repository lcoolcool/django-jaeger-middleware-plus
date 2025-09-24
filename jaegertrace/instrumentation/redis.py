"""
Redis instrumentation for Redis operations.
Automatically traces Redis commands with performance metrics.
"""
import logging

from opentracing import logs
from opentracing.ext import tags

from .utils import tracing_enabled
from ..conf import is_component_enabled, get_tracing_config

logger = logging.getLogger(__name__)


def redis_name(func, *args, **kwargs):
    return "REDIS"


def redis_span_processor(func, span, *args, **kwargs):
    span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)
    span.set_tag(tags.COMPONENT, "redis")
    span.set_tag(tags.DATABASE_TYPE, "redis")

    # Add SQL statement (optionally truncated)
    max_length = get_tracing_config().get("database", {}).get("max_query_length", 1000)
    statement = args[1] + " " + args[2]
    span.set_tag(logs.MESSAGE, statement[:max_length])
    span.set_tag(tags.DATABASE_STATEMENT, statement[:max_length])


def redis_need_ignore(func, *args, **kwargs):
    ignore_commands = get_tracing_config().get("redis", {}).get("ignore_commands", [])
    return any(
        ignore_command.upper() in args[0].upper()
        for ignore_command in ignore_commands
    )


class RedisInstrumentation:
    """Redis instrumentation manager."""

    @classmethod
    def install(cls):
        """Install Redis instrumentation."""
        if not is_component_enabled("redis"):
            return

        try:
            import redis
            from redis.client import Redis

            Redis.execute_command = tracing_enabled(Redis.execute_command, redis_name, redis_span_processor, redis_need_ignore)
            logger.info("Redis instrumentation installed")
        except ImportError:
            logger.warning("Redis package not found, skipping Redis instrumentation")
