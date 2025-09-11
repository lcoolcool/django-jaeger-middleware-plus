"""
Celery instrumentation for distributed task tracing.
Automatically traces Celery task execution and message passing.
"""

import logging
import time
from typing import Any, Dict, Optional

from opentracing import Format
from opentracing.ext import tags

from ..conf import is_component_enabled, get_tracing_config
from ..initial_tracer import initialize_global_tracer
from ..request_context import get_current_span, span_in_context, span_out_context

logger = logging.getLogger(__name__)


class CeleryInstrumentation:
    """Celery instrumentation manager."""

    _installed = False

    @classmethod
    def install(cls):
        """Install Celery instrumentation."""
        if cls._installed or not is_component_enabled("celery"):
            return

        try:
            from celery import signals
            from celery.app.task import Task

            # Connect to Celery signals
            signals.task_prerun.connect(cls._task_prerun_handler)
            signals.task_postrun.connect(cls._task_postrun_handler)
            signals.task_failure.connect(cls._task_failure_handler)
            signals.task_retry.connect(cls._task_retry_handler)
            signals.before_task_publish.connect(cls._before_task_publish_handler)
            signals.task_sent.connect(cls._task_sent_handler)

            # Monkey patch Task.apply_async for trace injection
            original_apply_async = Task.apply_async

            def traced_apply_async(self, args=None, kwargs=None, **options):
                """Apply async with tracing context injection."""
                return cls._inject_trace_context(self, original_apply_async, args, kwargs, **options)

            Task.apply_async = traced_apply_async

            cls._installed = True
            logger.info("Celery instrumentation installed")

        except ImportError:
            logger.warning("Celery package not found, skipping Celery instrumentation")

    @classmethod
    def _should_trace_task(cls, task_name: str) -> bool:
        """Determine if task should be traced."""
        config = get_tracing_config().get("celery", {})
        ignore_tasks = config.get("ignore_tasks", [])
        return task_name not in ignore_tasks

    @classmethod
    def _inject_trace_context(cls, task, original_method, args=None, kwargs=None, **options):
        """Inject tracing context into task headers."""
        if not cls._should_trace_task(task.name):
            return original_method(task, args, kwargs, **options)

        tracer = initialize_global_tracer()
        current_span = get_current_span()

        if current_span:
            # Create span for task publishing
            span = tracer.start_span(
                operation_name=f"PUBLISH {task.name}",
                child_of=current_span
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
                headers.update(carrier)
                options["headers"] = headers
            except Exception as e:
                logger.debug(f"Failed to inject trace context: {e}")

            try:
                return original_method(task, args, kwargs, **options)
            finally:
                span.finish()

        return original_method(task, args, kwargs, **options)

    @classmethod
    def _task_prerun_handler(cls, sender=None, task_id=None, task=None, args=None, kwargs=None, **kwds):
        """Handle task prerun signal."""
        if not cls._should_trace_task(task.name):
            return

        tracer = initialize_global_tracer()
        config = get_tracing_config().get("celery", {})

        # Extract parent context from headers
        headers = getattr(task.request, "headers", {})
        parent_context = None

        if headers:
            try:
                parent_context = tracer.extract(
                    format=Format.TEXT_MAP,
                    carrier=headers
                )
            except Exception as e:
                logger.debug(f"Failed to extract trace context: {e}")

        # Create span for task execution
        span = tracer.start_span(
            operation_name=f"EXECUTE {task.name}",
            child_of=parent_context
        )

        span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_CONSUMER)
        span.set_tag(tags.COMPONENT, "celery")
        span.set_tag("celery.task_name", task.name)
        span.set_tag("celery.task_id", task_id)
        span.set_tag("celery.worker", getattr(task.request, "hostname", ""))

        # Add task arguments if configured
        if config.get("trace_task_args", False):
            if args:
                span.set_tag("celery.args_count", len(args))
            if kwargs:
                span.set_tag("celery.kwargs_count", len(kwargs))

        # Store span in task request for later access
        setattr(task.request, "_tracing_span", span)
        span_in_context(span)

        # Add start time for duration calculation
        setattr(task.request, "_tracing_start_time", time.time())

    @classmethod
    def _task_postrun_handler(cls, sender=None, task_id=None, task=None, args=None, kwargs=None, retval=None,
                              state=None, **kwds):
        """Handle task postrun signal."""
        span = getattr(task.request, "_tracing_span", None)
        if not span:
            return

        config = get_tracing_config().get("celery", {})

        try:
            # Calculate task duration
            start_time = getattr(task.request, "_tracing_start_time", None)
            if start_time:
                duration_ms = (time.time() - start_time) * 1000
                span.set_tag("celery.duration_ms", round(duration_ms, 2))

            # Add task state
            span.set_tag("celery.state", state or "SUCCESS")

            # Add result information if configured
            if config.get("trace_result", False) and retval is not None:
                span.set_tag("celery.result_type", type(retval).__name__)

        except Exception as e:
            logger.error(f"Error in task postrun handler: {e}")
        finally:
            span.finish()
            span_out_context()

    @classmethod
    def _task_failure_handler(cls, sender=None, task_id=None, exception=None, traceback=None, einfo=None, **kwds):
        """Handle task failure signal."""
        task = sender
        span = getattr(task.request, "_tracing_span", None) if task else None

        if span:
            span.set_tag(tags.ERROR, True)
            span.set_tag("celery.state", "FAILURE")

            if exception:
                span.log_kv({
                    "event": "error",
                    "error.kind": exception.__class__.__name__,
                    "error.object": str(exception),
                    "message": str(exception),
                })

    @classmethod
    def _task_retry_handler(cls, sender=None, task_id=None, reason=None, einfo=None, **kwds):
        """Handle task retry signal."""
        task = sender
        span = getattr(task.request, "_tracing_span", None) if task else None

        if span:
            span.set_tag("celery.retry", True)
            span.set_tag("celery.state", "RETRY")

            if reason:
                span.log_kv({
                    "event": "retry",
                    "reason": str(reason),
                })

    @classmethod
    def _before_task_publish_handler(cls, sender=None, body=None, exchange=None, routing_key=None, headers=None,
                                     properties=None, declare=None, retry_policy=None, **kwds):
        """Handle before task publish signal."""
        # Additional logging for task publishing
        pass

    @classmethod
    def _task_sent_handler(cls, sender=None, task_id=None, task=None, args=None, kwargs=None, eta=None, **kwds):
        """Handle task sent signal."""
        # Additional logging for task sending
        pass
