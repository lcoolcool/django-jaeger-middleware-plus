"""
Redis instrumentation for Redis operations.
Automatically traces Redis commands with performance metrics.
"""
import logging
import time

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

    def _should_ignore_tracing(self, command_name: str) -> bool:
        """Determine if the Redis command should be traced."""
        if not is_component_enabled("redis"):
            return True

        ignore_commands = self._config.get("ignore_commands", [])

        return any(
            ignore_command.upper() in command_name.upper()
            for ignore_command in ignore_commands
        )


    def _create_span(self, command_name: str, args: tuple = None):
        """Create tracing span for Redis command."""
        parent_span = get_current_span()
        operation_name = "REDIS"

        span = self._tracer.start_span(
            operation_name=operation_name,
            child_of=parent_span
        )

        # Set standard Redis tags
        span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)
        span.set_tag(tags.COMPONENT, "redis")
        span.set_tag(tags.DATABASE_TYPE, "redis")

        # Add connection information
        connection_kwargs = getattr(self._connection, "connection_kwargs", {})
        if "host" in connection_kwargs:
            span.set_tag(tags.PEER_HOST_IPV4, connection_kwargs["host"])
        if "port" in connection_kwargs:
            span.set_tag(tags.PEER_PORT, connection_kwargs["port"])
        if "db" in connection_kwargs:
            span.set_tag("db.redis.database_index", connection_kwargs["db"])

        statement = command_name.upper()
        if args:
            statement += " " + str(args[0])

        # Add statement (optionally truncated)
        if self._config.get("log_command", False):
            max_length = self._config.get("max_command_length", 500)
            span.set_tag(tags.DATABASE_STATEMENT, statement[:max_length])

        return span

    def send_command(self, *args, **kwargs):
        """Execute Redis command with tracing."""
        if not args:
            return self._connection.send_command(*args, **kwargs)

        command_name = str(args[0])

        if self._should_ignore_tracing(command_name):
            return self._connection.send_command(*args, **kwargs)

        span = self._create_span(command_name, args[1:])
        start_time = time.time()

        try:
            result = self._connection.send_command(*args, **kwargs)

            # Calculate command duration
            duration_ms = (time.time() - start_time) * 1000
            span.set_tag("db.redis.duration_ms", round(duration_ms, 2))

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

    @classmethod
    def install(cls):
        """Install Redis instrumentation."""
        if not is_component_enabled("redis"):
            return

        try:
            import redis
            from redis.connection import Connection

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

                def send_command(self, *args, **kwargs):
                    """Execute command with tracing."""
                    if self._tracing_wrapper:
                        return self._tracing_wrapper.send_command(*args, **kwargs)
                    return super().send_command(*args, **kwargs)

            # Replace Redis connection class
            redis.connection.Connection = TracedConnection
            redis.Connection = TracedConnection

            logger.info("Redis instrumentation installed")

        except ImportError:
            logger.warning("Redis package not found, skipping Redis instrumentation")
