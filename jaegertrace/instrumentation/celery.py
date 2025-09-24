"""
Celery instrumentation for distributed task tracing.
Automatically traces Celery task execution and message passing.
"""
import logging

from opentracing.ext import tags

from ..conf import is_component_enabled
from .utils import tracing_enabled

logger = logging.getLogger(__name__)


def celery_name(func, *args, **kwargs):
    return "CELERY"


def celery_span_processor(func, span, *args, **kwargs):
    span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)
    span.set_tag(tags.COMPONENT, "celery")
    span.set_tag('task.name', getattr(args[0], "name", ""))


class CeleryInstrumentation:
    """Celery instrumentation manager."""

    @classmethod
    def install(cls):
        """Install Celery instrumentation."""
        if not is_component_enabled("celery"):
            return

        try:
            from celery.app.task import Task
            Task.apply_async = tracing_enabled(Task.apply_async, celery_name, celery_span_processor)
            logger.info("Celery instrumentation installed")
        except ImportError:
            logger.warning("Celery package not found, skipping Celery instrumentation")
