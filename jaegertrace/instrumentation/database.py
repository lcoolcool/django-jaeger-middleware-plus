"""
Database instrumentation for Django ORM queries.
Automatically traces database operations with query details and performance metrics.
"""
import logging

from opentracing.ext import tags

from ..conf import is_component_enabled, get_tracing_config, get_service_name
from .utils import tracing_enabled

logger = logging.getLogger(__name__)


def db_name(func, *args, **kwargs):
    return "DB_QUERY"


def db_span_processor(func, span, *args, **kwargs):
    span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)
    span.set_tag(tags.COMPONENT, "django.db")
    span.set_tag(tags.DATABASE_TYPE, "sql")
    span.set_tag(tags.DATABASE_INSTANCE, get_service_name())

    # Add SQL statement (optionally truncated)
    max_length = get_tracing_config().get("database", {}).get("max_query_length", 1000)
    span.set_tag(tags.DATABASE_STATEMENT, args[0][:max_length])


def db_need_ignore(func, *args, **kwargs):
    sql = args[0]  # SQL query
    ignore_sqls = get_tracing_config().get("database", {}).get("ignore_sqls", [])
    return any(
        ignore_sql.upper() in sql.upper()
        for ignore_sql in ignore_sqls
    )


class DatabaseInstrumentation:
    """Database instrumentation manager."""

    @classmethod
    def install(cls):
        """Install database instrumentation."""
        if not is_component_enabled("database"):
            return

        from django.db.backends.utils import CursorWrapper

        CursorWrapper.execute = tracing_enabled(CursorWrapper.execute, db_name, db_span_processor, db_need_ignore)
        logger.info("Database instrumentation installed")
