#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import conf
import opentracing.tracer

from django.conf import settings
from jaeger_client import Config


def initialize_global_tracer():
    _config = Config(
        config=conf.TRACER_CONFIG,
        service_name=settings.TRACE_SERVICE_NAME,
        validate=True,
    )
    if _config.initialized():
        tracer = opentracing.tracer
    else:
        try:
            # use uwsgi
            # multi-processing, fork(2) issues see:
            # https://github.com/jaegertracing/jaeger-client-python/issues/60
            # https://github.com/jaegertracing/jaeger-client-python/issues/31
            from uwsgidecorators import postfork
            @postfork
            def post_fork_initialize_jaeger():
                _config.initialize_tracer()

            tracer = opentracing.tracer
        except ImportError:
            # use gunicorn etc.
            tracer = _config.initialize_tracer()
    return tracer
