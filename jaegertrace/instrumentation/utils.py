import opentracing

from ..request_context import get_current_span


def gen_name(func, *args, **kwargs):
    return func.__name__


def no_op(func, span, *args, **kwargs):
    pass


def no_ignore(func, *args, **kwargs) -> bool:
    return False


def tracing_enabled(func, operation_name=gen_name, span_processor=no_op, need_ignore=no_ignore):
    def call(*args, **kwargs):

        # ignore this function
        if need_ignore(func, *args, **kwargs):
            return func(*args, **kwargs)

        parent_span = get_current_span()
        tracer = opentracing.global_tracer()

        with tracer.start_span(operation_name(func, *args, **kwargs), child_of=parent_span) as span:
            span_processor(func, span, *args, **kwargs)
            try:
                return func(*args, **kwargs)
            except Exception as e:
                span.set_tag('error', True)
                span.log_kv({'event': 'error', 'error.object': e})
                raise

    return call
