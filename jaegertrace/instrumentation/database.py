"""
Database instrumentation for Django ORM queries.
Automatically traces database operations with query details and performance metrics.
"""
import logging
import opentracing

from opentracing.ext import tags

from ..conf import is_component_enabled, get_tracing_config, get_service_name
from ..request_context import get_current_span

logger = logging.getLogger(__name__)


def span_processor(sql, span):
    # Set standard database tags
    span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)
    span.set_tag(tags.COMPONENT, "django.db")
    span.set_tag(tags.DATABASE_TYPE, "sql")
    span.set_tag(tags.DATABASE_INSTANCE, get_service_name())

    # Add SQL statement (optionally truncated)
    max_length = get_tracing_config().get("database", {}).get("max_query_length", 1000)
    span.set_tag(tags.DATABASE_STATEMENT, sql[:max_length])



class DatabaseInstrumentation:
    """Database instrumentation manager."""

    @classmethod
    def install(cls):
        """Install database instrumentation."""
        if not is_component_enabled("database"):
            return

        from django.db.backends.utils import CursorWrapper

        def traced_make_cursor(func):
            def inner(*args, **kwargs):
                parent_span = get_current_span()
                tracer = opentracing.global_tracer()
                config = get_tracing_config().get("database", {})

                sql = args[1]

                # if database tracing is disabled, return the original function
                if not config.get('enabled', True):
                    return func(*args, **kwargs)

                # if sql is in ignore_sqls, return the original function
                ignore_sqls = config.get("ignore_sqls", [])
                if not any(ignore_sql in sql.upper() for ignore_sql in ignore_sqls):
                    return func(*args, **kwargs)


                with tracer.start_span('DB_QUERY', child_of=parent_span) as span:
                    span_processor(sql, span)
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        span.set_tag('error', True)
                        span.log_kv({'event': 'error', 'error.object': e})
                        raise

            return inner

        CursorWrapper.execute = traced_make_cursor(CursorWrapper.execute)
        logger.info("Database instrumentation installed")
