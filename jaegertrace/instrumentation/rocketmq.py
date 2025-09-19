"""
RocketMQ instrumentation for message queue tracing.
Automatically traces RocketMQ message production and consumption.
"""
import logging

from opentracing import Format
from opentracing.ext import tags

from ..conf import is_component_enabled, get_tracing_config
from ..initial_tracer import initialize_global_tracer
from ..request_context import get_current_span, span_in_context

logger = logging.getLogger(__name__)


class RocketMQInstrumentation:
    """RocketMQ instrumentation manager."""

    @classmethod
    def install(cls):
        """Install RocketMQ instrumentation."""
        if not is_component_enabled("rocketmq"):
            return

        try:
            # Try to import RocketMQ client
            from rocketmq.client import Producer, PushConsumer

            # Monkey patch Producer
            cls._patch_producer(Producer)

            # Monkey patch PushConsumer
            cls._patch_consumer(PushConsumer)

            logger.info("RocketMQ instrumentation installed")

        except ImportError:
            logger.warning("RocketMQ package not found, skipping RocketMQ instrumentation")

    @classmethod
    def _should_ignore_tracing(cls, topic: str) -> bool:
        """Determine if topic should be traced."""
        if not is_component_enabled("rocketmq"):
            return True

        config = get_tracing_config().get("rocketmq", {})
        ignore_topics = config.get("ignore_topics", [])

        return topic not in ignore_topics

    @classmethod
    def _patch_producer(cls, producer_class):
        """Patch RocketMQ Producer class."""
        original_send_sync = producer_class.send_sync
        original_send_oneway = producer_class.send_oneway

        def traced_send_sync(self, msg, timeout=3000):
            """Send message synchronously with tracing."""
            return cls._trace_send_message(self, original_send_sync, msg, "send_sync", timeout=timeout)

        def traced_send_oneway(self, msg):
            """Send message oneway with tracing."""
            return cls._trace_send_message(self, original_send_oneway, msg, "send_oneway")

        # Replace methods
        producer_class.send_sync = traced_send_sync
        producer_class.send_oneway = traced_send_oneway

    @classmethod
    def _patch_consumer(cls, consumer_class):
        """Patch RocketMQ Consumer class."""
        original_subscribe = consumer_class.subscribe

        def traced_subscribe(self, topic, callback, expression="*"):
            """Subscribe to topic with tracing wrapper."""
            def traced_callback(msg):
                return cls._trace_consume_message(msg, callback)

            return original_subscribe(self, topic, traced_callback, expression)

        consumer_class.subscribe = traced_subscribe

    @classmethod
    def _trace_send_message(cls, producer, original_method, msg, operation, **kwargs):
        """Trace message sending operations."""
        topic = msg.topic

        if cls._should_ignore_tracing(topic):
            return original_method(producer, msg, **kwargs)

        tracer = initialize_global_tracer()
        config = get_tracing_config().get("rocketmq", {})
        current_span = get_current_span()

        # Create span for message production
        span = tracer.start_span(
            operation_name=f"ROCKETMQ",
            child_of=current_span
        )

        span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_PRODUCER)
        span.set_tag(tags.COMPONENT, "rocketmq")
        span.set_tag(tags.MESSAGE_BUS_DESTINATION, topic)
        span.set_tag("rocketmq.operation", operation)
        span.set_tag("rocketmq.topic", topic)

        # Add message tags
        if hasattr(msg, "tags") and msg.tags:
            span.set_tag("rocketmq.tags", msg.tags)

        if hasattr(msg, "keys") and msg.keys:
            span.set_tag("rocketmq.keys", msg.keys)

        # Add message body
        if hasattr(msg, "body") and msg.body and config.get("trace_message_body", False):
            body_content = msg.body
            span.set_tag("rocketmq.body", str(body_content))

        # Inject tracing context into message properties
        try:
            carrier = {}
            tracer.inject(
                span_context=span.context,
                format=Format.TEXT_MAP,
                carrier=carrier
            )

            # Add trace context to message properties
            if not hasattr(msg, "properties"):
                msg.properties = {}
            if msg.properties is None:
                msg.properties = {}

            msg.properties.update(carrier)

        except Exception as e:
            logger.debug(f"Failed to inject trace context into RocketMQ message: {e}")

        try:
            result = original_method(producer, msg, **kwargs)

            # For synchronous operations, finish span immediately
            if operation == "send_sync":
                if hasattr(result, "status"):
                    if result.status == 0:  # Success
                        span.set_tag("rocketmq.send_status", "success")
                        if hasattr(result, "msg_id"):
                            span.set_tag("rocketmq.msg_id", result.msg_id)
                    else:
                        span.set_tag("rocketmq.send_status", "failed")
                        span.set_tag(tags.ERROR, True)
                span.finish()
            elif operation == "send_oneway":
                span.set_tag("rocketmq.send_status", "oneway")
                span.finish()

            return result

        except Exception as e:
            span.set_tag(tags.ERROR, True)
            span.log_kv({
                "event": "error",
                "error.kind": e.__class__.__name__,
                "error.object": str(e),
                "message": str(e),
            })
            span.finish()
            raise

    @classmethod
    def _trace_consume_message(cls, msg, original_callback):
        """Trace message consumption."""
        topic = msg.topic

        if cls._should_ignore_tracing(topic):
            return original_callback(msg)

        tracer = initialize_global_tracer()
        config = get_tracing_config().get("rocketmq", {})

        # Extract parent context from message properties
        parent_context = None
        if hasattr(msg, "properties") and msg.properties:
            try:
                parent_context = tracer.extract(
                    format=Format.TEXT_MAP,
                    carrier=msg.properties
                )
            except Exception as e:
                logger.debug(f"Failed to extract trace context from RocketMQ message: {e}")

        # Create span for message consumption
        span = tracer.start_span(
            operation_name=f"ROCKETMQ",
            child_of=parent_context
        )

        span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_CONSUMER)
        span.set_tag(tags.COMPONENT, "rocketmq")
        span.set_tag(tags.MESSAGE_BUS_DESTINATION, topic)
        span.set_tag("rocketmq.topic", topic)

        # Add message information
        if hasattr(msg, "msg_id"):
            span.set_tag("rocketmq.msg_id", msg.msg_id)

        if hasattr(msg, "tags") and msg.tags:
            span.set_tag("rocketmq.tags", msg.tags)

        if hasattr(msg, "keys") and msg.keys:
            span.set_tag("rocketmq.keys", msg.keys)

        if hasattr(msg, "queue_id"):
            span.set_tag("rocketmq.queue_id", msg.queue_id)

        if hasattr(msg, "born_timestamp"):
            span.set_tag("rocketmq.born_timestamp", msg.born_timestamp)

        # Add message body
        if hasattr(msg, "body") and msg.body and config.get("trace_message_body", False):
            body_content = msg.body
            span.set_tag("rocketmq.body", str(body_content))

        try:
            with span_in_context(span):
                result = original_callback(msg)

            span.set_tag("rocketmq.consume_status", "success")

            return result

        except Exception as e:
            span.set_tag(tags.ERROR, True)
            span.set_tag("rocketmq.consume_status", "failed")
            span.log_kv({
                "event": "error",
                "error.kind": e.__class__.__name__,
                "error.object": str(e),
                "message": str(e),
            })
            raise
        finally:
            span.finish()
