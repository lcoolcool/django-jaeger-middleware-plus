"""
Celery instrumentation for distributed task tracing.
Automatically traces Celery task execution and message passing.
"""
import logging
import time

from opentracing import Format
from opentracing.ext import tags

from ..conf import is_component_enabled, get_tracing_config
from ..initial_tracer import initialize_global_tracer
from ..request_context import get_current_span, span_in_context, span_out_context

logger = logging.getLogger(__name__)


class CeleryInstrumentation:
    """Celery instrumentation manager."""

    @classmethod
    def install(cls):
        """Install Celery instrumentation."""
        if not is_component_enabled("celery"):
            return

        try:
            from celery.app.task import Task

            # Monkey patch Task.apply_async for trace injection
            original_apply_async = Task.apply_async

            def traced_apply_async(self, args=None, kwargs=None, **options):
                """Apply async with tracing context injection."""
                return cls._inject_trace_context(self, original_apply_async, args, kwargs, **options)

            Task.apply_async = traced_apply_async

            logger.info("Celery instrumentation installed")

        except ImportError:
            logger.warning("Celery package not found, skipping Celery instrumentation")

    @classmethod
    def _should_ignore_tracing(cls, task_name: str) -> bool:
        """Determine if task should be traced."""
        if not is_component_enabled("celery"):
            return True

        config = get_tracing_config().get("celery", {})
        ignore_tasks = config.get("ignore_tasks", [])

        return task_name not in ignore_tasks

    @classmethod
    def _inject_trace_context(cls, task, original_method, args=None, kwargs=None, **options):
        """Inject tracing context into task headers."""
        if cls._should_ignore_tracing(task.name):
            return original_method(task, args, kwargs, **options)

        tracer = initialize_global_tracer()
        parent_span = get_current_span()
        operation_name = "CELERY"

        if parent_span:
            # Create span for task publishing
            span = tracer.start_span(
                operation_name=operation_name,
                child_of=parent_span
            )

            span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_PRODUCER)
            span.set_tag(tags.COMPONENT, "celery")
            span.set_tag("celery.task_name", task.name)
            span.set_tag("celery.task_id", options.get("task_id", ""))

            # Inject trace context into task headers
            headers = options.get("headers", {})
            carrier = {}

            try:
                tracer.inject(
                    span_context=span.context,
                    format=Format.TEXT_MAP,
                    carrier=carrier
                )

                # Add tracing headers to request
                for key, value in carrier.items():
                    headers[key] = value

            except Exception as e:
                logger.debug(f"Failed to inject trace context: {e}")

            try:
                return original_method(task, args, kwargs, **options)
            finally:
                span.finish()

        return original_method(task, args, kwargs, **options)
