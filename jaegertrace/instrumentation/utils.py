import opentracing

from jaeger_client import codecs
from jaeger_client.constants import TRACE_ID_HEADER
from opentracing import tags

from ..initial_tracer import initialize_global_tracer


def gen_name(func, *args, **kwargs):
    return func.__name__


def no_op(func, span, *args, **kwargs):
    pass


def enabled_tracing(func, name_generator=gen_name, span_processor=no_op):
    def _call(*args, **kwargs):
        tracer, span_ctx, span_tags = None, None, None
        tracing = initialize_global_tracer()
        try:
            tracer = opentracing.tracer
            # 生成上下文
            span_ctx = tracer.extract(opentracing.Format.HTTP_HEADERS, {TRACE_ID_HEADER: tracing.trace_id})
            span_tags = {tags.SPAN_KIND: tags.SPAN_KIND_RPC_SERVER}
        except:
            pass
        with opentracing.tracer.start_span(name_generator(func, *args, **kwargs), child_of=span_ctx, tags=span_tags) as span:
            try:
                span_processor(func, span, *args, **kwargs)
                previous_trace_id = tracing.trace_id
                tracing.trace_id = codecs.span_context_to_string(
                    trace_id=span.trace_id, span_id=span.span_id,
                    parent_id=span.parent_id, flags=span.flags)
            except:
                previous_trace_id = None
            try:
                return func(*args, **kwargs)  # actual call
            finally:
                tracing.trace_id = previous_trace_id

    return _call
