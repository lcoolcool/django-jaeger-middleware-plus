"""
Redis instrumentation for Redis operations.
Automatically traces Redis commands with performance metrics.
"""
import logging
import time
from typing import Any, List, Optional

from opentracing.ext import tags

from ..conf import is_component_enabled, get_tracing_config
from ..initial_tracer import initialize_global_tracer
from ..request_context import get_current_span

logger = logging.getLogger(__name__)


class TracingRedisConnection:
    """Redis connection wrapper that adds tracing."""

    def __init__(self, connection):
        self._connection = connection
        self._tracer = initialize_global_tracer()
        self._config = get_tracing_config().get("redis", {})

    def __getattr__(self, name):
        """Delegate to original connection for unknown attributes."""
        return getattr(self._connection, name)

    def _should_trace_command(self, command_name: str) -> bool:
        """Determine if the Redis command should be traced."""
        if not is_component_enabled("redis"):
            return False

        # Check ignore list
        ignore_commands = self._config.get("ignore_commands", [])
        if command_name.upper() in [cmd.upper() for cmd in ignore_commands]:
            return False

        # Check log list (if specified, only log these commands)
        log_commands = self._config.get("log_commands", [])
        if log_commands:
            return command_name.upper() in [cmd.upper() for cmd in log_commands]

        return True

    def _create_span(self, command_name: str, args: tuple = None):
        """Create tracing span for Redis command."""
        parent_span = get_current_span()
        operation_name = f"REDIS {command_name.upper()}"

        span = self._tracer.start_span(
            operation_name=operation_name,
            child_of=parent_span
        )

        # Set standard Redis tags
        span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)
        span.set_tag(tags.COMPONENT, "redis")
        span.set_tag(tags.DATABASE_TYPE, "redis")
        span.set_tag("db.redis.command", command_name.upper())

        # Add connection information
        connection_kwargs = getattr(self._connection, "connection_kwargs", {})
        if "host" in connection_kwargs:
            span.set_tag(tags.PEER_HOST_IPV4, connection_kwargs["host"])
        if "port" in connection_kwargs:
            span.set_tag(tags.PEER_PORT, connection_kwargs["port"])
        if "db" in connection_kwargs:
            span.set_tag("db.redis.database_index", connection_kwargs["db"])

        # Add arguments count
        if args:
            span.set_tag("db.redis.args_count", len(args))

            # Add key information for certain commands
            if command_name.upper() in ["GET", "SET", "DEL", "EXISTS", "HGET", "HSET"] and args:
                key = str(args[0])
                max_length = self._config.get("max_value_length", 500)
                span.set_tag("db.redis.key", key[:max_length])

        return span

    def execute_command(self, *args, **kwargs):
        """Execute Redis command with tracing."""
        if not args:
            return self._connection.execute_command(*args, **kwargs)

        command_name = str(args[0])

        if not self._should_trace_command(command_name):
            return self._connection.execute_command(*args, **kwargs)

        span = self._create_span(command_name, args[1:])
        start_time = time.time()

        try:
            result = self._connection.execute_command(*args, **kwargs)

            # Calculate command duration
            duration_ms = (time.time() - start_time) * 1000
            span.set_tag("db.redis.duration_ms", round(duration_ms, 2))

            # Add result information
            if result is not None:
                if isinstance(result, (list, tuple)):
                    span.set_tag("db.redis.result_length", len(result))
                elif isinstance(result, (str, bytes)):
                    max_length = self._config.get("max_value_length", 500)
                    result_str = str(result)[:max_length]
                    span.set_tag("db.redis.result_size", len(result_str))

            return result

        except Exception as e:
            span.set_tag(tags.ERROR, True)
            span.log_kv({
                "event": "error",
                "error.kind": e.__class__.__name__,
                "error.object": str(e),
                "message": str(e),
            })
            raise
        finally:
            span.finish()


class RedisInstrumentation:
    """Redis instrumentation manager."""

    _original_connection_class = None

    @classmethod
    def install(cls):
        """Install Redis instrumentation."""
        if not is_component_enabled("redis"):
            return

        try:
            import redis
            from redis.connection import Connection

            # Store original connection class
            cls._original_connection_class = Connection

            class TracedConnection(Connection):
                """Redis connection with tracing support."""

                def __init__(self, *args, **kwargs):
                    super().__init__(*args, **kwargs)
                    self._tracing_wrapper = None

                def connect(self):
                    """Connect and wrap with tracing."""
                    result = super().connect()
                    if not self._tracing_wrapper:
                        self._tracing_wrapper = TracingRedisConnection(self)
                    return result

                def execute_command(self, *args, **kwargs):
                    """Execute command with tracing."""
                    if self._tracing_wrapper:
                        return self._tracing_wrapper.execute_command(*args, **kwargs)
                    return super().execute_command(*args, **kwargs)

            # Replace Redis connection class
            redis.connection.Connection = TracedConnection
            redis.Connection = TracedConnection

            logger.info("Redis instrumentation installed")

        except ImportError:
            logger.warning("Redis package not found, skipping Redis instrumentation")

    @classmethod
    def uninstall(cls):
        """Uninstall Redis instrumentation."""
        if cls._original_connection_class:
            try:
                import redis
                redis.connection.Connection = cls._original_connection_class
                redis.Connection = cls._original_connection_class
                logger.info("Redis instrumentation uninstalled")
            except ImportError:
                pass