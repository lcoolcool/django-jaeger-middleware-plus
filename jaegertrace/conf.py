#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from django.conf import settings

if not settings.configured:
    settings.configure()

SAMPLE_TYPE = 'const'
if hasattr(settings, 'TRACE_SAMPLE_TYPE'):
    SAMPLE_TYPE = settings.TRACE_SAMPLE_TYPE

SAMPLE_PARAM = 1
if hasattr(settings, 'TRACE_SAMPLE_PARAM'):
    SAMPLE_PARAM = settings.TRACE_SAMPLE_PARAM

TRACE_ID_HEADER = 'trace-id'
if hasattr(settings, 'TRACE_ID_HEADER'):
    TRACE_ID_HEADER = settings.TRACE_ID_HEADER

BAGGAGE_HEADER_PREFIX = 'jaegertrace-'
if hasattr(settings, 'TRACE_BAGGAGE_HEADER_PREFIX'):
    BAGGAGE_HEADER_PREFIX = settings.TRACE_BAGGAGE_HEADER_PREFIX

if not hasattr(settings, 'TRACE_SERVICE_NAME'):
    settings.TRACE_SERVICE_NAME = settings.WSGI_APPLICATION.split(".")[0]

REPORTING_HOST = 'localhost'
if hasattr(settings, 'JAEGER_REPORTING_HOST'):
    REPORTING_HOST = settings.JAEGER_REPORTING_HOST

TRACE_CONFIG = {
    'sampler': {
        'type': SAMPLE_TYPE,
        'param': SAMPLE_PARAM,
    },
    'local_agent': {
        'reporting_host': REPORTING_HOST,
    },
    'trace_id_header': TRACE_ID_HEADER,
    'baggage_header_prefix': BAGGAGE_HEADER_PREFIX,
}

TRACING_PLUS_CONFIG = getattr(settings, "TRACING_CONFIG", {})  # type: dict

"""
example：

TRACING_CONFIG = {
    "sql": True,
    "redis": True,
    "celery": True,
    "mongo": True,
    "mns": True,
    "rocketmq": True,
}
"""
