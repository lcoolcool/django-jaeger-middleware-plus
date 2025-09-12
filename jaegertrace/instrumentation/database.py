"""
Database instrumentation for Django ORM queries.
Automatically traces database operations with query details and performance metrics.
"""
import logging
import time

from django.db.backends.utils import CursorWrapper
from opentracing.ext import tags

from ..conf import is_component_enabled, get_tracing_config
from ..initial_tracer import initialize_global_tracer
from ..request_context import get_current_span

logger = logging.getLogger(__name__)


class TracingCursorWrapper(CursorWrapper):
    """Database cursor wrapper that adds tracing to SQL queries."""

    def __init__(self, cursor, db):
        super().__init__(cursor, db)
        self._tracer = initialize_global_tracer()
        self._config = get_tracing_config().get("database", {})

    def _should_ignore(self, sql: str) -> bool:
        """Check if the query should be ignored based on configuration."""
        if not is_component_enabled("database"):
            return True

        ignore_sqls = self._config.get("ignore_sqls", [])

        return any(
            ignore_sql in sql.upper()
            for ignore_sql in ignore_sqls
        )

    def _create_span(self, sql: str, params=None):
        """Create tracing span for database query."""
        parent_span = get_current_span()
        operation_name = "DB_QUERY"

        span = self._tracer.start_span(
            operation_name=operation_name,
            child_of=parent_span
        )

        # Set standard database tags
        span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)
        span.set_tag(tags.COMPONENT, "django.db")
        span.set_tag(tags.DATABASE_TYPE, self.db.vendor)
        span.set_tag(tags.DATABASE_INSTANCE, self.db.settings_dict.get("NAME", ""))
        span.set_tag(tags.DATABASE_USER, self.db.settings_dict.get("USER", ""))
        span.set_tag(tags.PEER_HOST_IPV4, self.db.settings_dict.get("HOST", ""))
        span.set_tag(tags.PEER_PORT, self.db.settings_dict.get("PORT", ""))

        # Add SQL statement (optionally truncated)
        if self._config.get("log_sql", False):
            max_length = self._config.get("max_query_length", 1000)
            span.set_tag(tags.DATABASE_STATEMENT, sql[:max_length])

        return span

    def execute(self, sql, params=None):
        """Execute SQL with tracing."""
        if self._should_ignore(sql):
            return super().execute(sql, params)

        span = self._create_span(sql, params)
        start_time = time.time()

        try:
            result = super().execute(sql, params)

            # Calculate query duration
            duration_ms = (time.time() - start_time) * 1000
            span.set_tag("db.duration_ms", round(duration_ms, 2))

            # Check for slow queries
            slow_threshold = self._config.get("slow_query_threshold", 100)
            if duration_ms > slow_threshold:
                span.set_tag("db.slow_query", True)
                span.log_kv({
                    "event": "slow_query",
                    "duration_ms": duration_ms,
                    "threshold_ms": slow_threshold,
                })

            # Add row count if available
            if hasattr(self.cursor, "rowcount") and self.cursor.rowcount >= 0:
                span.set_tag("db.rows_affected", self.cursor.rowcount)

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

    def executemany(self, sql, param_list):
        """Execute many SQL statements with tracing."""
        if self._should_ignore(sql):
            return super().execute(sql, param_list)

        span = self._create_span(sql, param_list)
        start_time = time.time()

        try:
            result = super().executemany(sql, param_list)

            # Calculate query duration
            duration_ms = (time.time() - start_time) * 1000
            span.set_tag("db.duration_ms", round(duration_ms, 2))

            # Add row count if available
            if hasattr(self.cursor, "rowcount") and self.cursor.rowcount >= 0:
                span.set_tag("db.rows_affected", self.cursor.rowcount)

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


class DatabaseInstrumentation:
    """Database instrumentation manager."""

    @classmethod
    def install(cls):
        """Install database instrumentation."""
        if not is_component_enabled("database"):
            return

        from django.db.backends.base.base import BaseDatabaseWrapper

        def traced_make_cursor(self, cursor):
            """Create traced cursor wrapper."""
            return TracingCursorWrapper(cursor, self)

        # Monkey patch the make_cursor method
        BaseDatabaseWrapper.make_cursor = traced_make_cursor
        logger.info("Database instrumentation installed")
