#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import conf
import opentracing.tracer

from django.conf import settings
from jaeger_client import Config


def initialize_global_tracer():
    _config = Config(
        config=conf.TRACE_CONFIG,
        service_name=settings.TRACE_SERVICE_NAME,
        validate=True,
    )
    if _config.initialized():
        tracer = opentracing.tracer
    else:
        tracer = _config.initialize_tracer()
    return tracer
